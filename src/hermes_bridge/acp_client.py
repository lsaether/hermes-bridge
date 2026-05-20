"""Manages a hermes-acp subprocess and exposes its NDJSON stdio as async streams.

The ACP protocol is JSON-RPC 2.0 framed as newline-delimited JSON over stdio
(see acp/connection.py:62 in the upstream `agent-client-protocol` package).
This module is intentionally protocol-agnostic: it reads and writes complete
lines without parsing JSON. The bridge passes those lines through to a
WebSocket client unchanged.
"""

from __future__ import annotations

import asyncio
import logging
import os
import shlex
import signal
from collections.abc import AsyncIterator

logger = logging.getLogger(__name__)


class ACPSubprocess:
    """Spawn a hermes-acp subprocess and stream its NDJSON stdio."""

    def __init__(
        self,
        command: str = "hermes-acp",
        args: list[str] | None = None,
        env: dict[str, str] | None = None,
    ) -> None:
        self._command = command
        self._args = list(args or [])
        self._env = {**os.environ, **(env or {})}
        self._proc: asyncio.subprocess.Process | None = None

    async def start(self) -> None:
        if self._proc is not None:
            raise RuntimeError("ACPSubprocess already started")

        logger.info("Spawning ACP subprocess: %s %s", self._command, shlex.join(self._args))
        self._proc = await asyncio.create_subprocess_exec(
            self._command,
            *self._args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=self._env,
        )
        # Drain stderr to our own stderr so Hermes logs are visible to the operator.
        asyncio.create_task(self._drain_stderr(), name="acp-stderr-drain")

    async def _drain_stderr(self) -> None:
        assert self._proc is not None and self._proc.stderr is not None
        while True:
            raw = await self._proc.stderr.readline()
            if not raw:
                return
            try:
                text = raw.rstrip(b"\n").decode("utf-8", errors="replace")
            except Exception:
                text = repr(raw)
            logger.info("[hermes-acp] %s", text)

    async def send_line(self, line: bytes) -> None:
        """Write a single NDJSON message to the subprocess stdin.

        The caller is responsible for ensuring `line` contains no embedded newlines.
        A trailing newline is appended if not present.
        """
        if self._proc is None or self._proc.stdin is None:
            raise RuntimeError("ACPSubprocess not started")
        if not line.endswith(b"\n"):
            line = line + b"\n"
        self._proc.stdin.write(line)
        await self._proc.stdin.drain()

    async def lines(self) -> AsyncIterator[bytes]:
        """Yield each line from the subprocess stdout (without the trailing newline)."""
        if self._proc is None or self._proc.stdout is None:
            raise RuntimeError("ACPSubprocess not started")
        while True:
            raw = await self._proc.stdout.readline()
            if not raw:
                return  # subprocess closed stdout
            yield raw.rstrip(b"\n")

    async def stop(self, timeout: float = 5.0) -> int | None:
        """Terminate the subprocess gracefully, then forcefully if needed."""
        if self._proc is None:
            return None
        if self._proc.returncode is not None:
            return self._proc.returncode

        try:
            if self._proc.stdin and not self._proc.stdin.is_closing():
                self._proc.stdin.close()
        except Exception:
            logger.debug("Error closing subprocess stdin", exc_info=True)

        try:
            self._proc.send_signal(signal.SIGTERM)
        except ProcessLookupError:
            return self._proc.returncode

        try:
            return await asyncio.wait_for(self._proc.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("ACP subprocess did not exit after SIGTERM; sending SIGKILL")
            try:
                self._proc.kill()
            except ProcessLookupError:
                pass
            return await self._proc.wait()

    @property
    def returncode(self) -> int | None:
        return self._proc.returncode if self._proc else None
