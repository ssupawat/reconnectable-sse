// Test client for the SSE-over-POST backend.
//
// Phase 1: POST /v1/stream, read 3 events, then close the connection
//          from the client side. This simulates a network drop — the
//          server has no idea who closed the connection.
// Phase 2: POST /v1/stream with the same X-Stream-Id + X-Last-Event-Id
//          from phase 1, read events to `done`.
//
// Both phases use the same X-Stream-Id, so phase 2 resumes the stream
// that phase 1 started.
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
	disconnectAfter = 3
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

	fmt.Println("=== Phase 1: connect, read 3 events, close connection ===")
	lastID, err := connect(streamID, body, "", disconnectAfter)
	if err != nil {
		return fmt.Errorf("phase 1: %w", err)
	}
	if lastID == "" {
		return fmt.Errorf("phase 1: no events received")
	}
	fmt.Printf("\n→ last_event_id = %s\n", lastID)

	time.Sleep(500 * time.Millisecond)

	fmt.Println("\n=== Phase 2: reconnect, read to done ===")
	if _, err := connect(streamID, body, lastID, 0); err != nil {
		return fmt.Errorf("phase 2: %w", err)
	}

	fmt.Println("\n(tail `podman-compose logs -f api-1 api-2` to see which pod handled each phase)")
	return nil
}

// connect performs one POST and reads SSE events. If disconnectAfter > 0,
// the connection is closed after that many events; otherwise it reads to
// `done`. Returns the last event id seen.
func connect(streamID, body, lastID string, disconnectAfter int) (string, error) {
	req, err := http.NewRequest("POST", targetURL, strings.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Stream-Id", streamID)
	if lastID != "" {
		req.Header.Set("X-Last-Event-Id", lastID)
	}

	httpClient := &http.Client{Timeout: readTimeout}
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	fmt.Printf("status=%d\n", resp.StatusCode)

	var capturedLastID string
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

		count++
		fmt.Printf("  [#%02d] id=%-22s event=%-6s data=%s\n", count, ev.ID, ev.Event, ev.Data)

		if ev.Event == "done" || ev.Event == "error" {
			return capturedLastID, nil
		}
		if disconnectAfter > 0 && count >= disconnectAfter {
			fmt.Println("  -- closing connection --")
			return capturedLastID, nil
		}
	}

	if capturedLastID == "" {
		return "", fmt.Errorf("connection ended without any events")
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
