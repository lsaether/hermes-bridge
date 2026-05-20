# Roadmap

Status legend: `[ ]` not started · `[~]` in progress · `[x]` done

---

## v0 — single-client byte relay ✅

- [x] FastAPI/uvicorn server, `/healthz` + `/acp` WebSocket
- [x] `ACPSubprocess` manager (spawn, NDJSON stdio, graceful shutdown)
- [x] Pure byte-level relay (no JSON parsing on the bridge)
- [x] CLI entry point + smoke test script
- [x] End-to-end verified: `initialize` round-trips through bridge → hermes-acp v0.14.0

---

## v0.5 — session multiplexing

**Goal:** N clients can subscribe to the same live Hermes session. Phone + desktop client both watch the same turn stream in real time, either can drive.

**Architectural shift:** the bridge stops being a pure byte relay and becomes a JSON-RPC-aware proxy. We still treat ACP-specific payloads as opaque, but we parse the JSON-RPC envelope (id, method) to route correctly.

**Sizing estimate:** 3–5 focused days, broken into 7 chunks. Each chunk is a self-contained commit/PR.

### Chunk 1 — session routing (½ day)

WS endpoint accepts `?session=<id>` query param. Introduce a `SessionRegistry` that maps session IDs to `SessionState` objects holding the subprocess. Each new session ID spawns its own `hermes-acp`. Still one subscriber per session for now — second connection to a live session is rejected.

**DoD:** Two WS connections to `?session=foo` — first succeeds, second is rejected with a clear close code. Each unique `session` value spawns its own subprocess.

### Chunk 2 — notification fan-out (½–1 day)

`SessionState` grows a subscriber set. Multiple WS connections to the same session ID are now allowed. **Notifications** (JSON-RPC frames with no `id` field — token deltas, tool progress, status updates) broadcast to every subscriber. Request/response still passes through to the first subscriber as a placeholder.

**DoD:** Two clients on the same session — one sends a prompt, *both* see the streaming token deltas in real time. This is the moment the "watch on both screens" UX works.

### Chunk 3 — request ID translation (1–2 days)

Per-subscriber ID translation table. Outgoing requests get their `id` rewritten to a session-unique bridge ID; incoming responses get rewritten back and routed to the originating subscriber. Bridge intercepts `initialize` from subsequent clients and replays a cached response (hermes-acp only accepts one initialize per session).

**DoD:** Two clients each call `initialize` successfully. Client A calls a request method and gets the response; client B doesn't see A's response. Client A sees the streaming notifications from its own turn.

### Chunk 4 — agent-initiated request routing (½–1 day)

Track "driving client" per session: whichever subscriber sent the most recent request. Server-initiated requests from hermes-acp (e.g., tool authorization prompts) route to the driving client. If the driving client disconnects mid-request, route to next subscriber or fail-fast.

**DoD:** Tool authorization request reaches the client that initiated the turn, not other subscribers.

### Chunk 5 — concurrent turn serialization (½ day)

Session has explicit state: `idle` | `in_turn`. New prompt while `in_turn` is **rejected** (v0.5 picks rejection over queueing for simplicity); other subscribers are notified via a synthetic `session/busy` event so their UI can disable the composer.

**Decision to make:** reject vs. queue. v0.5 = reject. Revisit in v1.0 if usage shows pain.

**DoD:** Two clients send simultaneous prompts; one succeeds, the other gets a busy notification. All subscribers see the session-busy state.

### Chunk 6 — reconnect grace + lifecycle polish (½ day)

When the last subscriber disconnects, don't kill the subprocess immediately — start a 30s TTL timer. New subscriber within TTL cancels the timer. After TTL, subprocess terminates and session is removed from the registry. Configurable via `--session-ttl-seconds`.

**DoD:** Connect → disconnect → reconnect within 30s → same subprocess (verify by PID). Disconnect → wait 30s → subprocess gone.

### Chunk 7 — tests, docs, migration notes (½–1 day)

- pytest-asyncio test suite covering the new multiplex behavior (registry, fan-out, ID translation, busy state, TTL).
- README architecture diagram updated to show multi-subscriber.
- Migration note: clients now **must** specify `?session=<id>`. The old "connect-and-go" implicit-session behavior is removed.

**DoD:** `pytest` passes. README reflects v0.5 architecture.

---

## v1.0 — future scope

Not committed. Captured here so we don't lose ideas and so v0.5 stays focused.

### Likely v1.0 features

- **Replay buffer per session.** New subscriber to a mid-turn session gets the last N events on attach, so the phone reconnecting doesn't stare at a blank screen until the next token.
- **Backpressure.** Per-client bounded send queues. Slow clients can't stall fan-out to fast clients; on queue overflow, either drop the slow client or drop oldest events.
- **Subprocess crash recovery.** Auto-restart `hermes-acp` on crash, reload session from state.db, notify subscribers via a synthetic `session/restored` event.
- **Auth.** Per-client tokens (separate from Tailscale tailnet trust). Needed if the bridge ever serves a shared tailnet or moves to a non-Tailscale transport.
- **Session discovery API.** `GET /sessions` returns active sessions with subscriber counts. Powers a phone-side "session picker" without needing direct DB access.
- **Concurrent turn handling — queue mode.** Optional `--turn-policy={reject,queue}` to enqueue prompts instead of rejecting them. Useful if real usage shows people frequently double-send.
- **Multi-session per subprocess.** If hermes-acp can handle multiple sessions in one process (via `session/new` + `session/load`), share a subprocess across session IDs to save memory. Requires deeper ACP session-routing on the bridge.
- **Metrics endpoint.** Prometheus-style `/metrics`: active sessions, subscribers per session, request rate, subprocess restart count, fan-out latency p50/p99.
- **Persistent always-on subprocess.** Untangle subprocess lifecycle from client connections entirely — bridge launches one `hermes-acp` per known session at boot, clients attach/detach freely. The "Hermes is always running, you just attach" model.
- **Session sharing URLs.** Generate one-time links that another device uses to attach to a session — useful for ad-hoc handoff between phone and desktop without needing a session picker.
- **Recording / playback.** Capture the event stream to disk; replay for debugging, sharing, or building eval datasets.

### Companion projects (parallel tracks, not blockers)

- **Phone client cutover.** Port the existing PWA viewer prototype to consume `ws://bridge/acp?session=<id>` instead of DB-polling + subprocess-runner. The frontend (composer, voice, attach gate) is largely reusable; the data layer changes completely.
- **Desktop ACP TUI client.** Small Textual or Rust/ratatui client that speaks ACP over WebSocket. Connects to the bridge, renders messages/tool events/deltas, sends prompts. ~300–600 lines. Removes the need to use Zed for desktop access.

### Explicitly out of scope

- **Changes to hermes-agent itself.** Bridge must stay a pure consumer of public ACP. If hermes-agent doesn't expose something we need, work around it or wait for upstream — don't fork.
- **Embedding a full terminal.** This was an explicit non-goal of the original viewer prototype; it's also a non-goal here.
- **Cross-machine federation.** v1.0 is still localhost-per-host (with Tailscale for transport). One bridge per machine, no inter-bridge routing.
