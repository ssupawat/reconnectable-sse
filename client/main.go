// Test client for the SSE-over-POST backend.
//
// Phase 1: POST /v1/stream with X-Test-Drop-After: 3, read events until
//          the server closes the connection. The close is initiated by the
//          server and propagates through nginx — the client experiences it
//          as a network-level drop, not a clean shutdown it requested.
// Phase 2: POST /v1/stream with the same X-Stream-Id and X-Last-Event-Id
//          of the last event from phase 1, read events to `done`.
//
// Both phases use the SAME X-Stream-Id, so the second request resumes the
// same stream that phase 1 started.
package main

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	targetURL   = "http://localhost:8080/v1/stream"
	readTimeout = 30 * time.Second
	promptText  = "Hello from the reconnectable SSE client"
	dropAfter   = 3
)

type sseEvent struct {
	ID    string
	Event string
	Data  string
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ERR:", err)
		os.Exit(1)
	}
}

func run() error {
	streamID := uuid.NewString()
	body := fmt.Sprintf(`{"prompt":%q}`, promptText)
	fmt.Printf("stream_id : %s\n", streamID)
	fmt.Printf("body      : %s\n\n", body)

	// Phase 1: server drops the connection after 3 events.
	fmt.Println("=== Phase 1: connect (server will drop after 3 events) ===")
	lastID, err := connect(streamID, body, "", dropAfter, true)
	if err != nil {
		return fmt.Errorf("phase 1: %w", err)
	}
	if lastID == "" {
		return fmt.Errorf("phase 1: no events received")
	}
	fmt.Printf("\n→ last_event_id = %s\n", lastID)
	fmt.Println("  (connection terminated by server; not a client-initiated close)")

	time.Sleep(500 * time.Millisecond)

	// Phase 2: same stream_id, with X-Last-Event-Id, read to done.
	fmt.Println("\n=== Phase 2: reconnect with same stream_id, read to done ===")
	if _, err := connect(streamID, body, lastID, 1000, false); err != nil {
		return fmt.Errorf("phase 2: %w", err)
	}

	fmt.Println("\n(tail `podman-compose logs -f api-1 api-2` to see which pod handled each phase)")
	return nil
}

// connect performs one POST, reads SSE events, and returns the last event id
// read. If expectDrop is true, the server is told to close the connection
// after maxEvents events; an EOF before `done` is treated as the expected drop.
func connect(streamID, body, lastID string, maxEvents int, expectDrop bool) (string, error) {
	req, err := http.NewRequest("POST", targetURL, strings.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Stream-Id", streamID)
	if lastID != "" {
		req.Header.Set("X-Last-Event-Id", lastID)
	}
	if expectDrop {
		req.Header.Set("X-Test-Drop-After", fmt.Sprintf("%d", maxEvents))
	}

	httpClient := &http.Client{Timeout: readTimeout}
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	fmt.Printf("status=%d\n", resp.StatusCode)

	var capturedLastID string
	sawDone := false
	count := 0
	scanner := bufio.NewScanner(resp.Body)
	for {
		ev, err := readSSE(scanner)
		if err == io.EOF {
			break
		}
		if err != nil {
			return capturedLastID, err
		}

		if ev.ID != "" {
			capturedLastID = ev.ID
		}
		if ev.Event == "done" {
			sawDone = true
		}

		count++
		fmt.Printf("  [#%02d] id=%-22s event=%-6s data=%s\n", count, ev.ID, ev.Event, ev.Data)

		if ev.Event == "done" || ev.Event == "error" {
			return capturedLastID, nil
		}
	}

	// EOF reached.
	if expectDrop && !sawDone {
		// Expected: server closed without sending `done`.
		return capturedLastID, nil
	}
	if !sawDone {
		return capturedLastID, fmt.Errorf("connection ended without `done` event")
	}
	return capturedLastID, nil
}

func readSSE(scanner *bufio.Scanner) (*sseEvent, error) {
	var ev sseEvent
	hasField := false
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if hasField {
				return &ev, nil
			}
			continue
		}
		hasField = true
		switch {
		case strings.HasPrefix(line, "id: "):
			ev.ID = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "event: "):
			ev.Event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			ev.Data = strings.TrimPrefix(line, "data: ")
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return nil, io.EOF
}
