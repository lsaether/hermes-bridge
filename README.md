# hermes-bridge

> A tiny adapter that lets your phone (or any web client) attach to a local Hermes agent the same way Zed does — over [ACP](https://agentclientprotocol.com), just transported over WebSocket instead of stdio.

WebSocket bridge to a [hermes-agent](https://github.com/NousResearch/hermes-agent) ACP stdio server. Connects mobile/web clients to a local Hermes agent without forking Hermes or scraping its session DB.

## Architecture

```
phone (WSS)  ─────────────┐
                          │
                  Tailscale tailnet
                          │
                          ▼
                  hermes-bridge (FastAPI + uvicorn)
                     │           │
                     │  WS ↔ NDJSON relay (no semantic awareness)
                     ▼
                  hermes-acp (stdio JSON-RPC subprocess)
                     │
                     └─ Hermes session state in ~/.hermes/state.db
```

The bridge spawns `hermes-acp` as a child process and relays newline-delimited JSON-RPC messages bidirectionally between the WebSocket and the subprocess's stdio. The bridge has no semantic awareness of ACP — it forwards bytes. This keeps the bridge decoupled from upstream Hermes version churn.

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

Then connect a WebSocket client to `ws://127.0.0.1:8765/acp`. Each connection spawns its own `hermes-acp` subprocess for v0; session multiplexing is a future task.

## Test client

```bash
python scripts/test_client.py
```

Sends an `initialize` request and prints incoming events.

## Security

Bind to `127.0.0.1` by default. For phone access, expose via Tailscale Serve HTTPS rather than opening a port to the public internet.

## Status

v0. Bridge is a pure byte-level relay. No auth, no session multiplexing, no reconnect handling.
