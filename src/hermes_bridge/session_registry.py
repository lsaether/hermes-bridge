"""Session registry: maps a client-supplied session_id to a hermes-acp subprocess.

In v0.5 chunk 1, each session has at most one subscriber. Chunk 2 widens this
to N subscribers. Chunk 6 adds a TTL-based subprocess lifetime so reconnects
don't kill the agent.
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass

from .acp_client import ACPSubprocess

logger = logging.getLogger(__name__)

SESSION_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,128}$")


def is_valid_session_id(session_id: str | None) -> bool:
    """Validate session_id format. Returns False for None, empty, or non-matching strings."""
    if not session_id:
        return False
    return SESSION_ID_PATTERN.match(session_id) is not None


class SessionConflict(Exception):
    """Raised when a session already has a subscriber (chunk 1 rule)."""

    def __init__(self, session_id: str) -> None:
        super().__init__(session_id)
        self.session_id = session_id


@dataclass
class SessionState:
    """Per-session state. In chunk 1 there is at most one subscriber per session.

    Chunk 2 will widen `subscriber` into a set of subscribers and add fan-out logic.
    """

    session_id: str
    proc: ACPSubprocess
    subscriber: object | None = None  # opaque WS handle


class SessionRegistry:
    def __init__(self, acp_command: str, acp_args: tuple[str, ...]) -> None:
        self._acp_command = acp_command
        self._acp_args = acp_args
        self._sessions: dict[str, SessionState] = {}
        self._lock = asyncio.Lock()

    async def attach(self, session_id: str, subscriber: object) -> SessionState:
        """Attach a subscriber to a session. Spawns the ACP subprocess if the session is new.

        Raises:
            SessionConflict: if a different subscriber is already attached.
        """
        async with self._lock:
            state = self._sessions.get(session_id)
            if state is None:
                proc = ACPSubprocess(command=self._acp_command, args=list(self._acp_args))
                await proc.start()
                state = SessionState(session_id=session_id, proc=proc, subscriber=subscriber)
                self._sessions[session_id] = state
                logger.info("created session %s", session_id)
                return state

            if state.subscriber is not None and state.subscriber is not subscriber:
                raise SessionConflict(session_id)

            state.subscriber = subscriber
            return state

    async def detach(self, session_id: str, subscriber: object) -> None:
        """Subscriber disconnected. In chunk 1 this also terminates the subprocess.

        Chunk 6 will replace immediate teardown with a TTL grace period.
        """
        async with self._lock:
            state = self._sessions.get(session_id)
            if state is None or state.subscriber is not subscriber:
                return
            state.subscriber = None
            self._sessions.pop(session_id, None)

        try:
            await state.proc.stop()
            logger.info("terminated session %s", session_id)
        except Exception:
            logger.exception("error stopping subprocess for session %s", session_id)

    async def shutdown(self) -> None:
        """Stop all subprocesses. Called on server shutdown."""
        async with self._lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()
        for state in sessions:
            try:
                await state.proc.stop()
            except Exception:
                logger.exception("error stopping subprocess for session %s", state.session_id)

    def active_session_count(self) -> int:
        return len(self._sessions)
