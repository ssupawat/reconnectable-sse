package main

// SSE-over-POST backend with cross-instance resume via Redis Streams.
//
// Per stream two goroutines run, communicating only through Redis:
//   1. runEventGenerator  — does the "work", XADDs events to the stream
//   2. runSSEResponse     — XREADs the stream and writes SSE to the client
//
// Stream identity comes from the client: each request sends an X-Stream-Id
// header (client-generated, e.g. a UUID). The server uses it directly as
// the Redis stream key, so:
//   - same X-Stream-Id + same X-Last-Event-Id  →  resume
//   - same X-Stream-Id, no X-Last-Event-Id     →  read from start
//   - different X-Stream-Id                    →  different stream
//
// On client disconnect, runSSEResponse returns. The event generator keeps
// running under context.Background(), so a reconnect on any instance can
// resume by reading the same Redis stream at the X-Last-Event-Id offset.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	streamKeyPrefix = "sse:stream:"
	streamTTL       = time.Hour
	heartbeatPeriod = 5 * time.Second
	tokensPerStream = 20
	tokenDelay      = 300 * time.Millisecond
)

var rdb *redis.Client

func main() {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb = redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("redis ping %s: %v", addr, err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/stream", handleStream)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	instance := os.Getenv("INSTANCE_ID")
	log.Printf("api starting instance=%s redis=%s", instance, addr)
	log.Fatal(http.ListenAndServe(":8080", mux))
}

// handleStream routes the request and starts the two stream goroutines.
//
// Headers:
//   X-Stream-Id         required; client-generated id (e.g. a UUID). Used
//                       directly as the Redis stream key.
//   X-Last-Event-Id     resume offset; absent → read from start
//   X-Test-Drop-After N (test only) close the SSE response after N events
//                       to simulate a connection drop at the nginx layer
func handleStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	instance := os.Getenv("INSTANCE_ID")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	// Headers must be set before the first Write.
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")

	streamID := r.Header.Get("X-Stream-Id")
	if streamID == "" {
		http.Error(w, "X-Stream-Id required", http.StatusBadRequest)
		return
	}
	key := streamKeyPrefix + streamID

	body, _ := io.ReadAll(r.Body)

	lastEventID := r.Header.Get("X-Last-Event-Id")
	if lastEventID == "" {
		lastEventID = "0"
	}

	dropAfter := 0
	if v := r.Header.Get("X-Test-Drop-After"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			dropAfter = n
		}
	}

	// First Write triggers 200 OK and flushes the response head.
	meta := map[string]string{"stream_id": streamID}
	metaJSON, _ := json.Marshal(meta)
	fmt.Fprintf(w, "event: meta\ndata: %s\n\n", metaJSON)
	flusher.Flush()

	// Start the generator only the first time we see this stream id.
	exists, _ := rdb.Exists(r.Context(), key).Result()
	if exists == 0 {
		genCtx, cancelGen := context.WithCancel(context.Background())
		go func() {
			defer cancelGen()
			runEventGenerator(genCtx, streamID, string(body))
		}()
		log.Printf("[%s] new stream_id=%s (generator starting)", instance, streamID)
	} else {
		log.Printf("[%s] seen stream_id=%s, resuming from %s", instance, streamID, lastEventID)
	}

	runSSEResponse(r.Context(), streamID, lastEventID, dropAfter, w, flusher)
}

// runEventGenerator simulates work and XADDs events to the Redis stream.
func runEventGenerator(ctx context.Context, streamID, prompt string) {
	key := streamKeyPrefix + streamID
	tokens := generateTokens(prompt)

	ttlSet := false
	for i, tok := range tokens {
		if ctx.Err() != nil {
			return
		}

		payload, _ := json.Marshal(map[string]any{
			"type":  "token",
			"index": i,
			"text":  tok,
		})
		if err := rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: key,
			Values: map[string]any{"event": "token", "data": string(payload)},
		}).Err(); err != nil {
			log.Printf("[generator %s] xadd: %v", streamID, err)
			return
		}

		if !ttlSet {
			_ = rdb.Expire(ctx, key, streamTTL).Err()
			ttlSet = true
		}

		time.Sleep(tokenDelay)
	}

	if err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: key,
		Values: map[string]any{"event": "done", "data": `{"reason":"complete"}`},
	}).Err(); err != nil {
		log.Printf("[generator %s] xadd done: %v", streamID, err)
		return
	}
	log.Printf("[generator %s] complete (%d tokens)", streamID, len(tokens))
}

// runSSEResponse XREADs from the stream and writes SSE events to the client.
// Exits when:
//   - a `done` event is sent, OR
//   - the request context is cancelled (client disconnect), OR
//   - dropAfter > 0 and that many events have been sent (test drop), OR
//   - the stream does not exist on a resume (sends `error` then returns).
func runSSEResponse(ctx context.Context, streamID, lastEventID string, dropAfter int, w http.ResponseWriter, flusher http.Flusher) {
	key := streamKeyPrefix + streamID

	// For resumes, verify the stream still exists. For new requests
	// (lastID=="0") we skip the check — the stream may not exist yet but
	// XREAD with BLOCK will wait for the generator's first XADD to create it.
	if lastEventID != "0" {
		n, err := rdb.Exists(ctx, key).Result()
		if err != nil {
			log.Printf("[sse %s] exists check: %v", streamID, err)
			return
		}
		if n == 0 {
			fmt.Fprintf(w, "event: error\ndata: {\"error\":\"stream_not_found\"}\n\n")
			flusher.Flush()
			return
		}
	}

	readID := lastEventID
	if readID == "0" {
		readID = "0-0"
	}

	sent := 0
	for {
		streams, err := rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{key, readID},
			Count:   100,
			Block:   heartbeatPeriod,
		}).Result()

		if err == redis.Nil {
			if _, werr := fmt.Fprintf(w, ": heartbeat\n\n"); werr != nil {
				return
			}
			flusher.Flush()
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				log.Printf("[sse %s] client disconnected", streamID)
				return
			}
			log.Printf("[sse %s] xread: %v", streamID, err)
			return
		}

		for _, s := range streams {
			for _, msg := range s.Messages {
				readID = msg.ID
				evType, _ := msg.Values["event"].(string)
				data, _ := msg.Values["data"].(string)

				if _, werr := fmt.Fprintf(w, "id: %s\nevent: %s\ndata: %s\n\n", msg.ID, evType, data); werr != nil {
					return
				}
				flusher.Flush()
				sent++

				if evType == "done" {
					return
				}
				if dropAfter > 0 && sent >= dropAfter {
					log.Printf("[sse %s] test drop after %d events", streamID, sent)
					return
				}
			}
		}
	}
}

func generateTokens(prompt string) []string {
	base := fmt.Sprintf("You asked: %q. Simulated streaming response, one token at a time.", prompt)
	words := strings.Fields(base)
	out := make([]string, 0, tokensPerStream)
	for i := 0; i < tokensPerStream; i++ {
		out = append(out, words[i%len(words)])
	}
	return out
}
