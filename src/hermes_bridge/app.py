"""FastAPI app exposing the ACP relay over WebSocket."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass

from fastapi import FastAPI, WebSocket

from . import __version__
from .session_registry import SessionRegistry
from .ws_relay import relay

logger = logging.getLogger(__name__)


@dataclass
class AppConfig:
    """Settings for the bridge app."""

    acp_command: str = "hermes-acp"
    acp_args: tuple[str, ...] = ()
    session_ttl_seconds: float = 30.0


def create_app(config: AppConfig) -> FastAPI:
    registry = SessionRegistry(
        acp_command=config.acp_command,
        acp_args=config.acp_args,
        session_ttl_seconds=config.session_ttl_seconds,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        try:
            yield
        finally:
            logger.info("shutting down: stopping %d active session(s)", registry.active_session_count())
            await registry.shutdown()

    app = FastAPI(title="hermes-bridge", version=__version__, lifespan=lifespan)

    @app.get("/healthz")
    async def healthz() -> dict[str, object]:
        return {
            "status": "ok",
            "version": __version__,
            "activeSessions": registry.active_session_count(),
        }

    @app.websocket("/acp")
    async def acp_endpoint(ws: WebSocket, session: str | None = None) -> None:
        await relay(ws, session, registry)

    return app
