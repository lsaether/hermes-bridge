"""Minimal smoke test for the hermes-bridge WebSocket relay.

Connects to ws://localhost:8765/acp, sends an ACP `initialize` request, and
prints every response for `--timeout` seconds. Verifies that the bridge spawns
hermes-acp and round-trips JSON-RPC successfully.

Run after starting the bridge:
    hermes-bridge --host 127.0.0.1 --port 8765

Then:
    python scripts/test_client.py
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import sys

import websockets


async def run(url: str, timeout: float) -> int:
    print(f"Connecting to {url}...", file=sys.stderr)
    async with websockets.connect(url) as ws:
        print("Connected. Sending initialize...", file=sys.stderr)
        init = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": 1,
                "clientInfo": {"name": "hermes-bridge-smoketest", "version": "0.1.0"},
                "clientCapabilities": {},
            },
        }
        await ws.send(json.dumps(init))

        # Read for `timeout` seconds, printing each frame.
        async def reader() -> None:
            async for msg in ws:
                try:
                    parsed = json.loads(msg)
                    print(json.dumps(parsed, indent=2))
                except json.JSONDecodeError:
                    print(f"<non-json> {msg!r}")

        task = asyncio.create_task(reader())
        try:
            await asyncio.wait_for(task, timeout=timeout)
        except asyncio.TimeoutError:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        print(f"\nDone (read for {timeout}s).", file=sys.stderr)
    return 0


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="ws://127.0.0.1:8765/acp")
    p.add_argument("--timeout", type=float, default=5.0)
    args = p.parse_args()
    sys.exit(asyncio.run(run(args.url, args.timeout)))


if __name__ == "__main__":
    main()
