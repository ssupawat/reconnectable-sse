# reconnectable-sse

Multi-instance SSE-over-POST backend with cross-pod stream resume via Redis Streams.

## Design

```
                                 ┌──────────────────────────────┐
Client ── POST /v1/stream ──▶   │           nginx              │
                                 │       (round-robin)          │
                                 └──────────┬───────────────────┘
                                            │
                          ┌─────────────────┴─────────────────┐
                          ▼                                   ▼
                   ┌─────────────┐                     ┌─────────────┐
                   │   api-1     │                     │   api-2     │
                   │             │                     │             │
                   │  goroutine 1: runSSEResponse     │  goroutine 1: runSSEResponse
                   │  goroutine 2: runEventGenerator  │  goroutine 2: runEventGenerator
                   └──────┬──────┘                     └──────┬──────┘
                          │         XADD / XREAD            │
                          └──────────────┬───────────────────┘
                                         ▼
                                 ┌───────────────┐
                                 │     redis     │
                                 │ sse:stream:*  │
                                 └───────────────┘
```

The two goroutines per stream communicate **only** through the Redis stream:

| Goroutine           | Reads from     | Writes to        | Lifetime                  |
| ------------------- | -------------- | ---------------- | ------------------------- |
| `runEventGenerator` | (does the work)| Redis (`XADD`)   | `context.Background()`    |
| `runSSEResponse`    | Redis (`XREAD`)| HTTP response    | request `context.Context` |

The generator is detached — a client disconnect never stops the work.

### Stream identity = hash(body)

The stream id is `sha256(request_body)`. The same body always maps to the
same stream, so the client does not need to track an id. To resume, the
client simply re-sends the same body with the `X-Last-Event-Id` header.

```
request body "X"  →  stream id a1b2...  →  redis key sse:stream:a1b2...
request body "Y"  →  stream id c3d4...  →  redis key sse:stream:c3d4...
```

### Generator lifecycle

The first time an api instance sees a given stream id, it starts the
generator. Subsequent requests (resumes) for the same id do not start a new
generator — they just consume from the existing Redis stream.

## Endpoints

### `POST /v1/stream`

**Headers**

| Header               | Required | Meaning                                                    |
| -------------------- | -------- | ---------------------------------------------------------- |
| `X-Last-Event-Id`    | no       | Resume offset (Redis stream id). Absent → read from start  |
| `X-Test-Drop-After`  | no       | Test only: close the SSE response after N events to        |
|                      |          | simulate a connection drop at the nginx layer              |

**Body**

The body is opaque to the server; it is hashed to derive the stream id.
The example uses JSON with a `prompt` field, but any body works.

**Response (SSE)**

```
event: meta
data: {"stream_id":"<sha256-of-body>"}

event: token
data: {"type":"token","index":0,"text":"You"}

id: 1700000000000-0
event: token
data: {"type":"token","index":1,"text":"asked:"}

...

event: done
data: {"reason":"complete"}
```

If a resume references a stream that has expired or never existed, the
server sends `event: error` with `{"error":"stream_not_found"}` and closes.

> `X-Last-Event-Id` (not the standard `Last-Event-ID`) is used because the
> standard header is only auto-sent by the browser's `EventSource` on GET
> reconnects. With POST the client must send it explicitly. The stream id
> is implicit in the body, so no separate `X-Stream-Id` header is needed.

### `GET /healthz`

Returns `200 ok`.

## Quickstart

```bash
go mod tidy
podman-compose up --build -d
go run ./client
```

The demo:

1. Sends a body with `X-Test-Drop-After: 3`, reads events, and waits for
   the server-initiated close (not a client-initiated close).
2. Sends the **same body** with `X-Last-Event-Id` of the last received
   event, reads events to `done`.

Run `podman-compose logs -f api-1 api-2` in another terminal to see which
pod handled each phase — a different pod for phase 2 proves cross-instance
resume.

## Cleanup

```bash
podman-compose down -v
```

## Layout

```
reconnectable-sse/
├── podman-compose.yml        # 4 containers: nginx, api-1, api-2, redis
├── Containerfile             # multi-stage Go build → distroless
├── go.mod
├── main.go                   # api server: handler + 2 goroutines
├── client/
│   └── main.go               # test client (POST → server drops → POST resume)
├── nginx/
│   └── default.conf          # round-robin upstream + SSE-safe proxy
└── README.md
```
