"""Session registry: maps a client-supplied session_id to a hermes-acp subprocess.

Chunk 2: each session may have multiple subscribers. The subprocess reader is
owned by SessionState (started on first attach) and fans out JSON-RPC frames:
  - notifications (no `id` field): broadcast to all subscribers
  - requests/responses (have `id`): chunk 2 placeholder — send to the first
    subscriber; chunk 3 will introduce per-subscriber ID translation and
    proper routing.

Chunk 6 will replace immediate subprocess teardown on last-subscriber-leave
with a TTL grace period.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any

from .acp_client import ACPSubprocess

logger = logging.getLogger(__name__)

SESSION_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,128}$")


def is_valid_session_id(session_id: str | None) -> bool:
    """Validate session_id format. Returns False for None, empty, or non-matching strings."""
    if not session_id:
        return False
    return SESSION_ID_PATTERN.match(session_id) is not None


@dataclass
class SessionState:
    """Per-session state shared across subscribers."""

    session_id: str
    proc: ACPSubprocess
    subscribers: list[Any] = field(default_factory=list)  # opaque WS handles, attachment order
    reader_task: asyncio.Task[None] | None = None


class SessionRegistry:
    def __init__(self, acp_command: str, acp_args: tuple[str, ...]) -> None:
        self._acp_command = acp_command
        self._acp_args = acp_args
        self._sessions: dict[str, SessionState] = {}
        self._lock = asyncio.Lock()

    async def attach(self, session_id: str, subscriber: Any) -> SessionState:
        """Attach a subscriber to a session. Spawns subprocess + dispatcher if new."""
        async with self._lock:
            state = self._sessions.get(session_id)
            if state is None:
                proc = ACPSubprocess(command=self._acp_command, args=list(self._acp_args))
                await proc.start()
                state = SessionState(session_id=session_id, proc=proc, subscribers=[subscriber])
                self._sessions[session_id] = state
                state.reader_task = asyncio.create_task(
                    _dispatch_subprocess_output(state),
                    name=f"session-reader-{session_id}",
                )
                logger.info("created session %s (first subscriber)", session_id)
                return state

            if subscriber not in state.subscribers:
                state.subscribers.append(subscriber)
                logger.info(
                    "added subscriber to session %s (count=%d)",
                    session_id,
                    len(state.subscribers),
                )
            return state

    async def detach(self, session_id: str, subscriber: Any) -> None:
        """Remove a subscriber. Tears down the session if it was the last one."""
        state_to_stop: SessionState | None = None
        async with self._lock:
            state = self._sessions.get(session_id)
            if state is None:
                return
            state.subscribers = [s for s in state.subscribers if s is not subscriber]
            logger.info(
                "removed subscriber from session %s (remaining=%d)",
                session_id,
                len(state.subscribers),
            )
            if state.subscribers:
                return
            self._sessions.pop(session_id, None)
            state_to_stop = state

        if state_to_stop is not None:
            await _shutdown_session(state_to_stop)

    async def shutdown(self) -> None:
        """Stop all subprocesses + dispatchers. Called on server shutdown."""
        async with self._lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()
        for state in sessions:
            await _shutdown_session(state)

    def active_session_count(self) -> int:
        return len(self._sessions)

    def subscriber_count(self, session_id: str) -> int:
        state = self._sessions.get(session_id)
        return len(state.subscribers) if state else 0


async def _shutdown_session(state: SessionState) -> None:
    """Cancel the reader task and stop the subprocess. Idempotent."""
    if state.reader_task is not None and not state.reader_task.done():
        state.reader_task.cancel()
        try:
            await state.reader_task
        except (asyncio.CancelledError, Exception):
            pass
    try:
        await state.proc.stop()
    except Exception:
        logger.exception("error stopping subprocess for session %s", state.session_id)


async def _dispatch_subprocess_output(state: SessionState) -> None:
    """Read NDJSON lines from the subprocess and fan them out to subscribers.

    Chunk 2 routing:
      - Notifications (dict without `id`): broadcast to every subscriber.
      - Requests/responses (have `id`): send to subscribers[0] as a placeholder.
        Chunk 3 will rewrite this with ID translation.
      - Non-JSON lines: broadcast as opaque text (defensive — shouldn't happen
        from a healthy hermes-acp, but stray prints to stdout are possible).
    """
    try:
        async for raw in state.proc.lines():
            if not raw:
                continue
            text = raw.decode("utf-8", errors="replace")

            parsed: Any
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                logger.debug(
                    "non-json line from session %s: %r", state.session_id, text[:120]
                )
                await _broadcast(state, text)
                continue

            if isinstance(parsed, dict) and "id" not in parsed:
                await _broadcast(state, text)
            else:
                # Request or response — chunk 2 placeholder. Chunk 3 will route by id.
                if state.subscribers:
                    await _send_one(state.subscribers[0], text)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("dispatcher error for session %s", state.session_id)
    finally:
        # Subprocess ended (EOF or crash). Close any remaining subscribers so
        # their relay tasks unwind and call detach.
        if state.subscribers:
            logger.info(
                "subprocess ended for session %s; closing %d subscriber(s)",
                state.session_id,
                len(state.subscribers),
            )
        for ws in list(state.subscribers):
            try:
                await ws.close(code=1011, reason="acp subprocess ended")
            except Exception:
                pass


async def _broadcast(state: SessionState, text: str) -> None:
    """Send `text` to every current subscriber. Snapshot the list to tolerate
    concurrent mutations (a subscriber detaching mid-broadcast)."""
    for ws in list(state.subscribers):
        await _send_one(ws, text)


async def _send_one(ws: Any, text: str) -> None:
    try:
        await ws.send_text(text)
    except Exception:
        logger.debug("failed to send to subscriber; ignoring", exc_info=True)
