"""ws-acp-shim: imitates acpx's ``<bin> exec '<prompt>' --approve-all`` semantics
over a WebSocket ACP transport.

The `flowforgelab/hermes-agent` "Generalized ACP client" patch spawns a CLI like:

    acpx claude exec '<prompt text>' --approve-all

and reads NDJSON `session/update` notifications from the subprocess's stdout
until it exits. The agent-side ACP protocol negotiation (initialize,
session/new, session/prompt, response collection) is handled inside acpx.

This shim provides the same surface, but talks to a WebSocket ACP server
(e.g. hermes-bridge) instead of spawning a local acpx process. Use it by
setting:

    export HERMES_ACP_HERMES_COMMAND="ws-acp-shim ws://127.0.0.1:8765/acp?session=desktop"
    hermes --tui --provider hermes-acp

The patch will then call:

    ws-acp-shim ws://... exec '<prompt>' --approve-all

and read the resulting NDJSON stream as if it were acpx output.

Known limitation: agent-initiated requests (tool authorization,
permission/request) are rejected with JSON-RPC -32601. The ``--approve-all``
flag is accepted (for argv compatibility) but only causes the shim to send
an empty-result response to every agent-initiated request, which most
agents interpret as a generic OK. Use prompts that don't require tool
authorization for v0 reliability.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed


# JSON-RPC ids the shim uses for its own outgoing requests. Kept small &
# distinct from anything an agent is likely to allocate for itself.
_ID_INITIALIZE = 1
_ID_SESSION_NEW = 2
_ID_SESSION_PROMPT = 3


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="ws-acp-shim",
        description="Imitate acpx's `exec` stdio semantics over a WebSocket ACP transport.",
    )
    p.add_argument(
        "url",
        help="WebSocket URL of the ACP server (e.g. ws://127.0.0.1:8765/acp?session=desktop)",
    )
    p.add_argument(
        "subcommand",
        nargs="?",
        default="exec",
        help="Currently always 'exec' (the only acpx subcommand the patch invokes).",
    )
    p.add_argument(
        "prompt",
        nargs="?",
        default="",
        help="The user prompt to send via session/prompt.",
    )
    p.add_argument(
        "--approve-all",
        action="store_true",
        help="Auto-respond to agent-initiated requests with an empty result. "
        "Most agents interpret this as a generic OK; if your prompt triggers "
        "complex tool flows, expect them to misbehave.",
    )
    return p.parse_args(argv)


async def _send(ws: Any, msg: dict[str, Any]) -> None:
    await ws.send(json.dumps(msg))


def _emit_event(raw_text: str) -> None:
    """Print a single NDJSON line to stdout. The patch's stdout reader expects
    one JSON event per line."""
    sys.stdout.write(raw_text.rstrip("\n") + "\n")
    sys.stdout.flush()


async def _handle_agent_request(ws: Any, msg: dict[str, Any], approve_all: bool) -> None:
    """Reply to a server-initiated request so the agent doesn't stall."""
    req_id = msg.get("id")
    method = msg.get("method", "?")
    if approve_all:
        # Empty-result placeholder. Works for many ACP request types; for
        # permission-shaped requests the agent may still misinterpret.
        print(
            f"ws-acp-shim: auto-approving agent request {method!r}", file=sys.stderr
        )
        await _send(ws, {"jsonrpc": "2.0", "id": req_id, "result": {}})
    else:
        print(
            f"ws-acp-shim: rejecting agent request {method!r} (use --approve-all to auto-respond)",
            file=sys.stderr,
        )
        await _send(
            ws,
            {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {
                    "code": -32601,
                    "message": "ws-acp-shim does not handle agent-initiated requests in v0",
                },
            },
        )


async def _await_response(ws: Any, expected_id: int, approve_all: bool) -> dict[str, Any]:
    """Drain frames until a response with the given id arrives.

    Notifications encountered along the way are forwarded to stdout (so any
    pre-prompt streaming the agent emits is visible to the patch's reader).
    Agent-initiated requests are answered per ``approve_all``.
    """
    while True:
        raw = await ws.recv()
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if not isinstance(msg, dict):
            continue
        msg_id = msg.get("id")
        method = msg.get("method")

        if method and msg_id is None:
            # Notification — forward to stdout.
            _emit_event(raw)
            continue
        if method and msg_id is not None:
            await _handle_agent_request(ws, msg, approve_all)
            continue
        if msg_id == expected_id:
            return msg
        # Other responses (rare during handshake) — log and drop.
        print(f"ws-acp-shim: unexpected response id={msg_id}", file=sys.stderr)


async def run(url: str, prompt: str, approve_all: bool) -> int:
    try:
        ws = await websockets.connect(url)
    except Exception as exc:
        print(f"ws-acp-shim: failed to connect to {url}: {exc}", file=sys.stderr)
        return 2

    try:
        # initialize
        await _send(
            ws,
            {
                "jsonrpc": "2.0",
                "id": _ID_INITIALIZE,
                "method": "initialize",
                "params": {
                    "protocolVersion": 1,
                    "clientInfo": {"name": "ws-acp-shim", "version": "0.1.0"},
                    "clientCapabilities": {},
                },
            },
        )
        init_resp = await _await_response(ws, _ID_INITIALIZE, approve_all)
        if "error" in init_resp:
            print(f"ws-acp-shim: initialize failed: {init_resp['error']}", file=sys.stderr)
            return 3

        # session/new
        await _send(
            ws,
            {
                "jsonrpc": "2.0",
                "id": _ID_SESSION_NEW,
                "method": "session/new",
                "params": {
                    "cwd": str(Path.cwd()),
                    "mcpServers": [],
                },
            },
        )
        new_resp = await _await_response(ws, _ID_SESSION_NEW, approve_all)
        if "error" in new_resp:
            print(f"ws-acp-shim: session/new failed: {new_resp['error']}", file=sys.stderr)
            return 4
        result = new_resp.get("result") or {}
        session_id = result.get("sessionId")
        if not session_id:
            print(
                f"ws-acp-shim: session/new returned no sessionId: {result}",
                file=sys.stderr,
            )
            return 4

        # session/prompt
        await _send(
            ws,
            {
                "jsonrpc": "2.0",
                "id": _ID_SESSION_PROMPT,
                "method": "session/prompt",
                "params": {
                    "sessionId": session_id,
                    "prompt": [{"type": "text", "text": prompt}],
                },
            },
        )
        prompt_resp = await _await_response(ws, _ID_SESSION_PROMPT, approve_all)
        if "error" in prompt_resp:
            print(
                f"ws-acp-shim: session/prompt failed: {prompt_resp['error']}",
                file=sys.stderr,
            )
            return 5

        # All notifications were streamed inside _await_response. Exit clean.
        return 0
    except ConnectionClosed as exc:
        print(f"ws-acp-shim: connection closed mid-turn: {exc}", file=sys.stderr)
        return 6
    except Exception as exc:
        print(f"ws-acp-shim: unexpected error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 7
    finally:
        try:
            await ws.close()
        except Exception:
            pass


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    if args.subcommand != "exec":
        print(
            f"ws-acp-shim: only the 'exec' subcommand is supported (got {args.subcommand!r})",
            file=sys.stderr,
        )
        sys.exit(2)
    if not args.prompt:
        print("ws-acp-shim: prompt must be non-empty", file=sys.stderr)
        sys.exit(2)
    sys.exit(asyncio.run(run(args.url, args.prompt, args.approve_all)))


if __name__ == "__main__":
    main()
