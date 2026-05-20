"""Bidirectional NDJSON relay between a WebSocket and a hermes-acp subprocess.

In v0.5 chunk 1 each session still has at most one subscriber, but the
subprocess lifecycle is owned by `SessionRegistry` (not by the relay function)
so chunk 2 can attach additional subscribers without restructuring this file.
"""

from __future__ import annotations

import asyncio
import logging

from fastapi import WebSocket, WebSocketDisconnect

from .acp_client import ACPSubprocess
from .session_registry import (
    SessionConflict,
    SessionRegistry,
    is_valid_session_id,
)

logger = logging.getLogger(__name__)

# Application-specific WS close codes (4000-4999 range is reserved for app use).
WS_CLOSE_INVALID_SESSION = 4400
WS_CLOSE_SESSION_IN_USE = 4409
WS_CLOSE_ACP_SPAWN_FAILED = 1011


async def relay(ws: WebSocket, session_id: str | None, registry: SessionRegistry) -> None:
    """Accept the WebSocket and relay NDJSON frames until either end closes."""
    await ws.accept()
    client = ws.client.host if ws.client else "?"

    if not is_valid_session_id(session_id):
        logger.info("rejected connection from %s: invalid session id %r", client, session_id)
        await ws.close(code=WS_CLOSE_INVALID_SESSION, reason="invalid or missing session id")
        return
    assert session_id is not None  # narrow for type checkers

    try:
        state = await registry.attach(session_id, ws)
    except SessionConflict:
        logger.info("rejected duplicate connection to session %s from %s", session_id, client)
        await ws.close(code=WS_CLOSE_SESSION_IN_USE, reason=f"session in use: {session_id}")
        return
    except FileNotFoundError:
        await ws.close(code=WS_CLOSE_ACP_SPAWN_FAILED, reason="hermes-acp command not found")
        return
    except Exception as exc:
        logger.exception("failed to spawn ACP subprocess for session %s", session_id)
        await ws.close(code=WS_CLOSE_ACP_SPAWN_FAILED, reason=f"failed to spawn ACP: {exc}")
        return

    logger.info("ws %s attached to session %s", client, session_id)

    ws_to_proc = asyncio.create_task(_pump_ws_to_proc(ws, state.proc), name="ws->proc")
    proc_to_ws = asyncio.create_task(_pump_proc_to_ws(state.proc, ws), name="proc->ws")

    try:
        _done, pending = await asyncio.wait(
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
        await registry.detach(session_id, ws)
        logger.info("ws %s detached from session %s", client, session_id)
        try:
            await ws.close()
        except RuntimeError:
            pass  # already closed


async def _pump_ws_to_proc(ws: WebSocket, proc: ACPSubprocess) -> None:
    """Read text frames from WS and write each as one NDJSON line to subprocess."""
    try:
        while True:
            msg = await ws.receive_text()
            normalised = msg.replace("\n", "").replace("\r", "")
            await proc.send_line(normalised.encode("utf-8"))
    except WebSocketDisconnect:
        logger.debug("ws disconnected (ws->proc direction)")
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
        logger.debug("ws disconnected (proc->ws direction)")
    except Exception:
        logger.exception("proc->ws relay error")
