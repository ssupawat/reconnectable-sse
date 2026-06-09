// SSE-over-POST backend. Stream state lives in Redis; any api pod can
// serve a resume. The two goroutines per stream coordinate through
// Redis only: runEventGenerator does the work and XADDs events,
// runSSEResponse XREADs and writes them as SSE.
//
// Stream identity is the X-Stream-Id header. Same id + X-Last-Event-Id
// resumes the stream; same id, no offset, reads from start; different
// id is a fresh stream.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
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

	log.Printf("api starting instance=%s redis=%s", os.Getenv("INSTANCE_ID"), addr)
	log.Fatal(http.ListenAndServe(":8080", mux))
}

func handleStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	streamID := r.Header.Get("X-Stream-Id")
	if streamID == "" {
		http.Error(w, "X-Stream-Id required", http.StatusBadRequest)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	setSSEHeaders(w)
	writeMeta(w, flusher, streamID)

	lastEventID := r.Header.Get("X-Last-Event-Id")
	if lastEventID == "" {
		lastEventID = "0"
	}

	key := streamKeyPrefix + streamID
	body, _ := io.ReadAll(r.Body)

	// First request for this stream starts the generator; resumes
	// just read from the existing stream.
	if !streamExists(r.Context(), key) {
		go runEventGenerator(streamID, string(body))
	}

	runSSEResponse(r.Context(), streamID, lastEventID, w, flusher)
}

func runEventGenerator(streamID, prompt string) {
	key := streamKeyPrefix + streamID
	tokens := generateTokens(prompt)

	for i, tok := range tokens {
		payload, _ := json.Marshal(map[string]any{"index": i, "text": tok})
		if err := rdb.XAdd(context.Background(), &redis.XAddArgs{
			Stream: key,
			Values: map[string]any{"event": "token", "data": string(payload)},
		}).Err(); err != nil {
			log.Printf("[generator %s] xadd: %v", streamID, err)
			return
		}
		if i == 0 {
			// Set TTL once the key actually exists.
			rdb.Expire(context.Background(), key, streamTTL)
		}
		time.Sleep(tokenDelay)
	}

	if err := rdb.XAdd(context.Background(), &redis.XAddArgs{
		Stream: key,
		Values: map[string]any{"event": "done", "data": `{"reason":"complete"}`},
	}).Err(); err != nil {
		log.Printf("[generator %s] xadd done: %v", streamID, err)
		return
	}
	log.Printf("[generator %s] complete (%d tokens)", streamID, len(tokens))
}

func runSSEResponse(ctx context.Context, streamID, lastEventID string, w http.ResponseWriter, flusher http.Flusher) {
	key := streamKeyPrefix + streamID

	// For resumes, fail fast if the stream has expired. New requests
	// skip this and rely on XREAD BLOCK to wait for the first XADD.
	if lastEventID != "0" && !streamExists(ctx, key) {
		writeError(w, flusher, "stream_not_found")
		return
	}

	readID := lastEventID
	if readID == "0" {
		readID = "0-0"
	}

	for {
		streams, err := rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{key, readID},
			Count:   100,
			Block:   heartbeatPeriod,
		}).Result()

		if err == redis.Nil {
			writeHeartbeat(w, flusher)
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				log.Printf("[sse %s] client disconnected", streamID)
			} else {
				log.Printf("[sse %s] xread: %v", streamID, err)
			}
			return
		}

		for _, s := range streams {
			for _, msg := range s.Messages {
				readID = msg.ID
				evType, _ := msg.Values["event"].(string)
				data, _ := msg.Values["data"].(string)
				writeEvent(w, flusher, msg.ID, evType, data)
				if evType == "done" {
					return
				}
			}
		}
	}
}

func streamExists(ctx context.Context, key string) bool {
	n, _ := rdb.Exists(ctx, key).Result()
	return n > 0
}

func setSSEHeaders(w http.ResponseWriter) {
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
}

func writeMeta(w http.ResponseWriter, flusher http.Flusher, streamID string) {
	payload, _ := json.Marshal(map[string]string{"stream_id": streamID})
	fmt.Fprintf(w, "event: meta\ndata: %s\n\n", payload)
	flusher.Flush()
}

func writeEvent(w http.ResponseWriter, flusher http.Flusher, id, event, data string) {
	fmt.Fprintf(w, "id: %s\nevent: %s\ndata: %s\n\n", id, event, data)
	flusher.Flush()
}

func writeHeartbeat(w http.ResponseWriter, flusher http.Flusher) {
	fmt.Fprintf(w, ": heartbeat\n\n")
	flusher.Flush()
}

func writeError(w http.ResponseWriter, flusher http.Flusher, err string) {
	fmt.Fprintf(w, "event: error\ndata: {\"error\":%q}\n\n", err)
	flusher.Flush()
}

func generateTokens(prompt string) []string {
	base := fmt.Sprintf("You asked: %q. Simulated streaming response, one token at a time.", prompt)
	words := strings.Fields(base)
	out := make([]string, tokensPerStream)
	for i := range out {
		out[i] = words[i%len(words)]
	}
	return out
}
