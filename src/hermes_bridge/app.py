"""FastAPI app exposing the ACP relay over WebSocket."""

from __future__ import annotations

from fastapi import FastAPI, WebSocket

from . import __version__
from .ws_relay import RelayConfig, relay


def create_app(config: RelayConfig) -> FastAPI:
    app = FastAPI(title="hermes-bridge", version=__version__)

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok", "version": __version__}

    @app.websocket("/acp")
    async def acp_endpoint(ws: WebSocket) -> None:
        await relay(ws, config)

    return app
