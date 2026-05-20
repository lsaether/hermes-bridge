"""WebSocket relay: pumps incoming WS frames into the session's hermes-acp stdin.

The subprocess-to-subscriber direction is now owned by SessionState's dispatcher
(see session_registry._dispatch_subprocess_output), so this module only handles
the WS → subprocess direction. Multiple WS subscribers per session share the
subprocess; concurrent stdin writes are serialised inside ACPSubprocess.
"""

from __future__ import annotations

import asyncio
import logging

from fastapi import WebSocket, WebSocketDisconnect

from .acp_client import ACPSubprocess
from .session_registry import SessionRegistry, is_valid_session_id

logger = logging.getLogger(__name__)

# Application-specific WS close codes (4000-4999 range is reserved for app use).
WS_CLOSE_INVALID_SESSION = 4400
WS_CLOSE_ACP_SPAWN_FAILED = 1011


async def relay(ws: WebSocket, session_id: str | None, registry: SessionRegistry) -> None:
    """Accept the WebSocket, attach to a session, and pump WS frames to subprocess stdin
    until either side closes. Subprocess output is dispatched by the session's reader."""
    await ws.accept()
    client = ws.client.host if ws.client else "?"

    if not is_valid_session_id(session_id):
        logger.info("rejected connection from %s: invalid session id %r", client, session_id)
        await ws.close(code=WS_CLOSE_INVALID_SESSION, reason="invalid or missing session id")
        return
    assert session_id is not None

    try:
        state = await registry.attach(session_id, ws)
    except FileNotFoundError:
        await ws.close(code=WS_CLOSE_ACP_SPAWN_FAILED, reason="hermes-acp command not found")
        return
    except Exception as exc:
        logger.exception("failed to spawn ACP subprocess for session %s", session_id)
        await ws.close(code=WS_CLOSE_ACP_SPAWN_FAILED, reason=f"failed to spawn ACP: {exc}")
        return

    logger.info("ws %s attached to session %s", client, session_id)

    try:
        await _pump_ws_to_proc(ws, state.proc)
    finally:
        await registry.detach(session_id, ws)
        logger.info("ws %s detached from session %s", client, session_id)
        try:
            await ws.close()
        except RuntimeError:
            pass  # already closed (e.g. subprocess EOF closed it from the dispatcher)


async def _pump_ws_to_proc(ws: WebSocket, proc: ACPSubprocess) -> None:
    """Read text frames from the WebSocket and write each as one NDJSON line to subprocess stdin."""
    try:
        while True:
            msg = await ws.receive_text()
            normalised = msg.replace("\n", "").replace("\r", "")
            await proc.send_line(normalised.encode("utf-8"))
    except WebSocketDisconnect:
        logger.debug("ws disconnected (ws->proc direction)")
    except Exception:
        logger.exception("ws->proc relay error")
