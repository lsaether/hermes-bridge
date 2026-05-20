"""Bidirectional NDJSON relay between a WebSocket and a hermes-acp subprocess.

One subprocess per WebSocket connection. The bridge does not parse JSON-RPC
bodies; it only ensures one NDJSON line maps to exactly one WebSocket text
frame and vice versa.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from fastapi import WebSocket, WebSocketDisconnect

from .acp_client import ACPSubprocess

logger = logging.getLogger(__name__)


@dataclass
class RelayConfig:
    """Settings for spawning the ACP subprocess per connection."""

    command: str = "hermes-acp"
    args: tuple[str, ...] = ()


async def relay(ws: WebSocket, config: RelayConfig) -> None:
    """Accept the WebSocket and relay until either end closes."""
    await ws.accept()
    client = ws.client.host if ws.client else "?"
    logger.info("WS connected from %s", client)

    proc = ACPSubprocess(command=config.command, args=list(config.args))
    try:
        await proc.start()
    except FileNotFoundError:
        await ws.close(code=1011, reason=f"command not found: {config.command}")
        logger.error("Cannot spawn %s — not on PATH", config.command)
        return
    except Exception as exc:
        await ws.close(code=1011, reason=f"failed to spawn ACP: {exc}")
        logger.exception("Failed to spawn ACP subprocess")
        return

    ws_to_proc = asyncio.create_task(_pump_ws_to_proc(ws, proc), name="ws->proc")
    proc_to_ws = asyncio.create_task(_pump_proc_to_ws(proc, ws), name="proc->ws")

    try:
        # When either direction ends (WS disconnect or subprocess EOF), tear down.
        done, pending = await asyncio.wait(
            {ws_to_proc, proc_to_ws}, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        for task in pending:
            try:
                await task
            except asyncio.CancelledError:
                pass
    finally:
        rc = await proc.stop()
        logger.info("WS closed for %s; ACP exit=%s", client, rc)
        try:
            await ws.close()
        except RuntimeError:
            pass  # already closed


async def _pump_ws_to_proc(ws: WebSocket, proc: ACPSubprocess) -> None:
    """Read text frames from WS and write each as one NDJSON line to subprocess."""
    try:
        while True:
            msg = await ws.receive_text()
            # Normalise: strip newlines inside the payload (NDJSON requires one line per frame).
            # The client shouldn't be sending embedded newlines, but be defensive.
            normalised = msg.replace("\n", "").replace("\r", "")
            await proc.send_line(normalised.encode("utf-8"))
    except WebSocketDisconnect:
        logger.debug("WS disconnected (ws->proc direction)")
    except Exception:
        logger.exception("ws->proc relay error")


async def _pump_proc_to_ws(proc: ACPSubprocess, ws: WebSocket) -> None:
    """Read NDJSON lines from subprocess stdout and forward each as a WS text frame."""
    try:
        async for line in proc.lines():
            if not line:
                continue
            await ws.send_text(line.decode("utf-8", errors="replace"))
    except WebSocketDisconnect:
        logger.debug("WS disconnected (proc->ws direction)")
    except Exception:
        logger.exception("proc->ws relay error")
