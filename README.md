# reconnectable-sse

Multi-instance SSE-over-POST backend with cross-pod stream resume via Redis Streams.

## What this PoC proves

A browser can lose its SSE connection mid-response and, on retry with the
same `X-Stream-Id` + last event id, pick up exactly where it left off —
regardless of which api pod it lands on. State lives in Redis, not in
process. The two api pods are interchangeable.

Designed for **browser auto-reconnect** (not user-driven "click reconnect"):
the window between disconnect and reconnect is seconds, the buffer is tiny,
and the 1h TTL on the Redis key is the only safety net needed for
abandoned sessions.

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
                   │ runSSEResponse   runEventGenerator │ runSSEResponse   runEventGenerator
                   └──────┬──────┘                     └──────┬──────┘
                          │         XADD / XREAD            │
                          └──────────────┬───────────────────┘
                                         ▼
                                 ┌───────────────┐
                                 │     redis     │
                                 │ sse:stream:*  │
                                 └───────────────┘
```

Per stream, two goroutines run, communicating **only** through Redis:

| Goroutine           | Reads from     | Writes to        | Lifetime                  |
| ------------------- | -------------- | ---------------- | ------------------------- |
| `runEventGenerator` | (does the work)| Redis (`XADD`)   | `context.Background()`    |
| `runSSEResponse`    | Redis (`XREAD`)| HTTP response    | request `context.Context` |

`runEventGenerator` is detached — a client disconnect never stops the work.
On a reconnect (any pod), `runSSEResponse` reads from the offset in
`X-Last-Event-Id` and forwards new events to the client.

### Stream identity

The client sends an `X-Stream-Id` header (any opaque string, e.g. a UUID).
The server uses it directly as the Redis stream key.

```
X-Stream-Id: a1b2-...   →  redis key sse:stream:a1b2-...
X-Stream-Id: c3d4-...   →  redis key sse:stream:c3d4-...
```

The same `X-Stream-Id` across reconnects resumes the same stream. A
different `X-Stream-Id` is a different stream, fully isolated.

The generator is started only the first time an api instance sees a given
`X-Stream-Id`. Resumes on subsequent requests just consume from the
existing Redis stream.

## Endpoints

### `POST /v1/stream`

**Headers**

| Header              | Required | Meaning                                                     |
| ------------------- | -------- | ----------------------------------------------------------- |
| `X-Stream-Id`       | **yes**  | Client-generated stream id (any opaque string)              |
| `X-Last-Event-Id`   | no       | Resume offset (Redis stream id). Absent → read from start   |

**Body** is the work payload (chat prompt, messages, anything). It does not
affect stream identity.

**Response (SSE)**

```
event: meta
data: {"stream_id":"<echoes X-Stream-Id>"}

event: token
data: {"type":"token","index":0,"text":"You"}

id: 1700000000000-0
event: token
data: {"type":"token","index":1,"text":"asked:"}

...

event: done
data: {"reason":"complete"}
```

A resume of a stream that has expired (TTL > 1h) gets
`event: error` with `{"error":"stream_not_found"}` and the connection
closes.

> `X-Stream-Id` and `X-Last-Event-Id` are custom (not the standard
> `Last-Event-ID`) because the standard header is only auto-sent by the
> browser's `EventSource` on **GET** reconnects. With **POST** the
> client code must send both explicitly.

### `GET /healthz`

Returns `200 ok`.

## Quickstart

```bash
go mod tidy
podman-compose up --build -d
go run ./client
```

The demo:

1. Generates a UUID for `X-Stream-Id`, sends a body, reads 3 events,
   then closes the connection from the client side (simulates a drop).
2. Sends the **same `X-Stream-Id`** with `X-Last-Event-Id` of the last
   received event, reads events to `done`.

In another terminal, `podman-compose logs -f api-1 api-2` — a different
pod handling phase 2 proves cross-instance resume.

```bash
# tear down
podman-compose down -v
```

## Not covered (intentionally out of scope)

- **CORS** — assumes same-origin
- **Auth** — `X-Stream-Id` is trusted as-is
- **Multi-turn conversation** — one request, one stream; new chat = new `X-Stream-Id`
- **TLS / HTTP/2** — plain HTTP/1.1 via nginx
- **Browser-side reconnect loop** — the demo is a Go client; a real product writes the JS

## Layout

```
reconnectable-sse/
├── podman-compose.yml        # 4 containers: nginx, api-1, api-2, redis
├── Containerfile             # multi-stage Go build → distroless
├── go.mod
├── main.go                   # api server: handler + 2 goroutines
├── client/
│   └── main.go               # test client: POST → server drops → POST resume
├── nginx/
│   └── default.conf          # round-robin upstream + SSE-safe proxy
├── Makefile                  # up / down / logs / demo
└── README.md
```
