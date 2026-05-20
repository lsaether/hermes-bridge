"""Session registry: maps a client-supplied session_id to a hermes-acp subprocess.

Chunk 3 introduces JSON-RPC-aware routing:

  - Client → subprocess requests get their `id` rewritten to a session-unique
    bridge_id. The mapping (bridge_id → (subscriber, original_id)) is stored in
    SessionState.pending_requests.

  - Subprocess → client responses are matched against pending_requests, get
    their id rewritten back to the originating subscriber's original_id, and
    are sent only to that subscriber.

  - The first `initialize` request is forwarded normally; its response is
    cached. Subsequent `initialize` requests from new subscribers are
    intercepted at the bridge and answered with the cached result.

  - Notifications (frames without `id`) broadcast to all subscribers.

  - Agent-initiated requests (frames with `id` AND `method`) route to
    subscribers[0] as a chunk-3 placeholder; chunk 4 will route them to the
    "driving" client.

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
    if not session_id:
        return False
    return SESSION_ID_PATTERN.match(session_id) is not None


@dataclass
class SessionState:
    """Per-session state shared across subscribers."""

    session_id: str
    proc: ACPSubprocess
    subscribers: list[Any] = field(default_factory=list)
    reader_task: asyncio.Task[None] | None = None

    # JSON-RPC translation state (chunk 3).
    next_bridge_id: int = 1
    pending_requests: dict[int, tuple[Any, Any]] = field(default_factory=dict)
    # ^ bridge_id -> (subscriber, original_client_id)

    # Cached initialize handshake (chunk 3). pending_initialize_bridge_id tracks
    # the in-flight first initialize; once the response arrives, its `result` is
    # stored in cached_initialize_result and replayed for subsequent clients.
    pending_initialize_bridge_id: int | None = None
    cached_initialize_result: Any | None = None

    # Driving subscriber (chunk 4): whoever most recently issued a substantive
    # request (anything except initialize). Agent-initiated requests from the
    # subprocess (tool authorization, terminal commands, ...) route to this
    # subscriber so the right user is asked. Falls back to subscribers[0] if
    # the driving subscriber has disconnected.
    driving_subscriber: Any | None = None

    # Turn serialization (chunk 5). When a session/prompt is forwarded, the
    # bridge_id is recorded here; further session/prompt requests are rejected
    # with -32001 until the response arrives (or the subprocess restarts).
    # Cleared in handle_inbound when the matching response comes back. Not
    # cleared on subscriber detach — the subprocess is still processing the
    # turn, and clearing here would let another subscriber start a second
    # concurrent turn that hermes-acp can't handle.
    active_turn_bridge_id: int | None = None

    async def handle_outbound(self, subscriber: Any, raw: str) -> None:
        """A subscriber sent `raw` to the subprocess. Translate, intercept, or pass through."""
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            await self.proc.send_line(raw.encode("utf-8"))
            return

        if not isinstance(parsed, dict):
            await self.proc.send_line(raw.encode("utf-8"))
            return

        msg_id = parsed.get("id")
        method = parsed.get("method")

        # Notification from the client (rare in ACP but possible). Pass through.
        if msg_id is None and method:
            await self.proc.send_line(raw.encode("utf-8"))
            return

        # Client response to an agent-initiated request. Chunk 3 passes through;
        # chunk 4 will track agent-id ownership.
        if msg_id is not None and not method:
            await self.proc.send_line(raw.encode("utf-8"))
            return

        # Client request to the agent.
        if msg_id is not None and method:
            # Initialize interception: serve cached result for subscribers after the first.
            if method == "initialize" and self.cached_initialize_result is not None:
                response = {
                    "jsonrpc": "2.0",
                    "id": msg_id,
                    "result": self.cached_initialize_result,
                }
                logger.debug(
                    "intercepted initialize for session %s; replayed cached response",
                    self.session_id,
                )
                await _send_one(subscriber, json.dumps(response))
                return

            # Turn serialization: reject session/prompt while another turn is active.
            if method == "session/prompt" and self.active_turn_bridge_id is not None:
                logger.info(
                    "session %s: rejecting session/prompt (turn in progress, bridge_id=%s)",
                    self.session_id, self.active_turn_bridge_id,
                )
                error_response = {
                    "jsonrpc": "2.0",
                    "id": msg_id,
                    "error": {
                        "code": -32001,
                        "message": "session busy: another turn is in progress",
                    },
                }
                await _send_one(subscriber, json.dumps(error_response))
                busy_notif = json.dumps({
                    "jsonrpc": "2.0",
                    "method": "bridge/session_busy",
                    "params": {
                        "rejected_method": method,
                        "rejected_client_id": msg_id,
                    },
                })
                await _broadcast(self, busy_notif)
                return

            # Allocate a bridge id and forward.
            bridge_id = self.next_bridge_id
            self.next_bridge_id += 1
            self.pending_requests[bridge_id] = (subscriber, msg_id)
            if method == "initialize" and self.cached_initialize_result is None:
                self.pending_initialize_bridge_id = bridge_id
            else:
                # Substantive (non-initialize) request — this subscriber is now driving.
                self.driving_subscriber = subscriber
            if method == "session/prompt":
                self.active_turn_bridge_id = bridge_id

            parsed["id"] = bridge_id
            await self.proc.send_line(json.dumps(parsed).encode("utf-8"))
            return

        # Fallthrough: weird shape (no id, no method). Pass through.
        await self.proc.send_line(raw.encode("utf-8"))

    async def handle_inbound(self, raw: str) -> None:
        """The subprocess emitted `raw`. Classify and route to subscriber(s)."""
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            logger.debug("non-json line from session %s: %r", self.session_id, raw[:120])
            await _broadcast(self, raw)
            return

        if not isinstance(parsed, dict):
            await _broadcast(self, raw)
            return

        msg_id = parsed.get("id")
        method = parsed.get("method")

        # Notification from the agent — broadcast.
        if msg_id is None:
            await _broadcast(self, raw)
            return

        # Agent-initiated request — route to the driving subscriber.
        if msg_id is not None and method:
            target = self.driving_subscriber
            if target is None or target not in self.subscribers:
                target = self.subscribers[0] if self.subscribers else None
            if target is None:
                logger.warning(
                    "session %s: agent-initiated request with no subscribers; dropped",
                    self.session_id,
                )
                return
            await _send_one(target, raw)
            return

        # Response to a bridge-forwarded request.
        if msg_id is not None and not method:
            entry = self.pending_requests.pop(msg_id, None)
            if entry is None:
                logger.warning(
                    "response for unknown bridge id %s in session %s",
                    msg_id, self.session_id,
                )
                return
            subscriber, original_id = entry

            # If this was the initial initialize, cache its result for replay.
            if (
                msg_id == self.pending_initialize_bridge_id
                and self.cached_initialize_result is None
                and "result" in parsed
            ):
                self.cached_initialize_result = parsed["result"]
                logger.info(
                    "cached initialize result for session %s; future subscribers will replay it",
                    self.session_id,
                )
            self.pending_initialize_bridge_id = None  # initialize handshake done either way

            # If this response completes the active turn, clear the busy state.
            if msg_id == self.active_turn_bridge_id:
                self.active_turn_bridge_id = None
                logger.debug(
                    "session %s: active turn (bridge_id=%s) complete", self.session_id, msg_id,
                )

            parsed["id"] = original_id
            await _send_one(subscriber, json.dumps(parsed))
            return

        # Unrecognised — broadcast.
        await _broadcast(self, raw)


class SessionRegistry:
    def __init__(self, acp_command: str, acp_args: tuple[str, ...]) -> None:
        self._acp_command = acp_command
        self._acp_args = acp_args
        self._sessions: dict[str, SessionState] = {}
        self._lock = asyncio.Lock()

    async def attach(self, session_id: str, subscriber: Any) -> SessionState:
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
                    session_id, len(state.subscribers),
                )
            return state

    async def detach(self, session_id: str, subscriber: Any) -> None:
        state_to_stop: SessionState | None = None
        async with self._lock:
            state = self._sessions.get(session_id)
            if state is None:
                return
            state.subscribers = [s for s in state.subscribers if s is not subscriber]
            # Drop any pending requests this subscriber had in flight — responses
            # for them would now have nowhere to route.
            state.pending_requests = {
                bid: entry
                for bid, entry in state.pending_requests.items()
                if entry[0] is not subscriber
            }
            # If the leaving subscriber was driving, clear it. Future agent-
            # initiated requests fall back to subscribers[0] until someone else
            # issues a substantive request.
            if state.driving_subscriber is subscriber:
                state.driving_subscriber = None
            logger.info(
                "removed subscriber from session %s (remaining=%d)",
                session_id, len(state.subscribers),
            )
            if state.subscribers:
                return
            self._sessions.pop(session_id, None)
            state_to_stop = state

        if state_to_stop is not None:
            await _shutdown_session(state_to_stop)

    async def shutdown(self) -> None:
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
    try:
        async for raw in state.proc.lines():
            if not raw:
                continue
            await state.handle_inbound(raw.decode("utf-8", errors="replace"))
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("dispatcher error for session %s", state.session_id)
    finally:
        if state.subscribers:
            logger.info(
                "subprocess ended for session %s; closing %d subscriber(s)",
                state.session_id, len(state.subscribers),
            )
        for ws in list(state.subscribers):
            try:
                await ws.close(code=1011, reason="acp subprocess ended")
            except Exception:
                pass


async def _broadcast(state: SessionState, text: str) -> None:
    for ws in list(state.subscribers):
        await _send_one(ws, text)


async def _send_one(ws: Any, text: str) -> None:
    try:
        await ws.send_text(text)
    except Exception:
        logger.debug("failed to send to subscriber; ignoring", exc_info=True)
