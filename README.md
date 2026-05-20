# hermes-bridge

> A tiny adapter that lets your phone (or any web client) attach to a local Hermes agent the same way Zed does — over [ACP](https://agentclientprotocol.com), just transported over WebSocket instead of stdio.

WebSocket bridge to a [hermes-agent](https://github.com/NousResearch/hermes-agent) ACP stdio server. Connects multiple mobile/web clients to a local Hermes agent without forking Hermes or scraping its session DB. Phone + desktop client can attach to the same session and both watch the stream in real time.

## Architecture

```
   phone client          desktop ACP client
        │                       │
        └────── WSS (Tailscale) ┘
                    │
                    ▼
       ┌─────────────────────────┐
       │     hermes-bridge       │   JSON-RPC-aware proxy:
       │   (FastAPI + uvicorn)   │     • per-session subprocess
       │                         │     • per-subscriber id translation
       │                         │     • initialize cache + replay
       │                         │     • turn serialization
       │                         │     • reconnect TTL grace
       └────────────┬────────────┘
                    │ NDJSON / JSON-RPC over stdio
                    ▼
            hermes-acp (one subprocess per session)
                    │
                    └─ ~/.hermes/state.db (persistent session storage)
```

Each unique `?session=<id>` value maps to one `hermes-acp` subprocess. Multiple WebSocket subscribers per session are supported: notifications (token streams, tool events) fan out to every subscriber; requests/responses route per-subscriber via bridge-side id translation; agent-initiated requests (tool authorization, terminal commands) go to whichever subscriber most recently issued a client request.

## Install

```bash
cd ~/Code/hermes-bridge
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

`hermes-acp` must be installed and on PATH. The bridge defaults to launching `hermes-acp`; override with `--hermes-acp-cmd`.

## Run

```bash
hermes-bridge --host 127.0.0.1 --port 8765
```

CLI options:

| Flag | Default | Purpose |
|---|---|---|
| `--host` | `127.0.0.1` | Bind address |
| `--port` | `8765` | Bind port |
| `--hermes-acp-cmd` | `hermes-acp` | Command to spawn the ACP subprocess (parsed via shlex) |
| `--session-ttl-seconds` | `30` | Seconds to preserve an idle session's subprocess after the last subscriber disconnects. Reconnects within the window reuse the live subprocess (cache + active turn preserved). `0` = immediate teardown. |
| `--log-level` | `info` | `debug` / `info` / `warning` / `error` |

## Client contract

Clients connect to `ws://<host>:<port>/acp?session=<id>`.

Session IDs must match `^[A-Za-z0-9_-]{1,128}$`. Missing or invalid → close code **4400**.

The protocol on the wire is unmodified [ACP](https://agentclientprotocol.com) JSON-RPC 2.0, one message per WebSocket text frame.

### Bridge-synthetic notifications

The bridge injects a small number of out-of-band notifications outside the ACP method namespace:

- `bridge/session_busy` — broadcast when a `session/prompt` is rejected because another turn is already in flight. The rejected client also gets a JSON-RPC error response with code `-32001`.

## Test client

```bash
python scripts/test_client.py --session smoketest
```

Sends an `initialize` request and prints incoming events.

## Run the test suite

```bash
pip install -e ".[dev]"
pytest
```

The suite spins up the bridge against an in-tree fake ACP (`tests/fake_acp.py`) so it runs deterministically without LLM credits.

## Security

Bind to `127.0.0.1` by default. For phone access, expose via Tailscale Serve HTTPS rather than opening a port to the public internet. There is no auth layer in v0.5 — the trust boundary is the tailnet.

## Migration from v0

v0 accepted any WebSocket connection to `/acp` and spawned one subprocess per connection. v0.5 requires `?session=<id>` and reuses subprocesses across multiple subscribers of the same session.

If you previously connected to `ws://host:port/acp`, change it to `ws://host:port/acp?session=<any-stable-id>`. Pick the session id deterministically so reconnects (within the TTL) preserve agent state.

## Status

v0.5. Multi-subscriber sessions with id translation, initialize caching, agent-request routing to the driving client, turn serialization, and TTL reconnect grace. No auth. See `ROADMAP.md` for v1.0 ideas (replay buffer, backpressure, crash recovery, session discovery, persistent always-on subprocesses).
