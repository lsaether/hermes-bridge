"""CLI entry point: `hermes-bridge [--host ...] [--port ...] [--hermes-acp-cmd ...]`."""

from __future__ import annotations

import argparse
import logging
import shlex
import sys

import uvicorn

from . import __version__
from .app import AppConfig, create_app


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="hermes-bridge",
        description="WebSocket bridge to a hermes-acp stdio subprocess.",
    )
    p.add_argument("--host", default="127.0.0.1", help="Interface to bind (default: 127.0.0.1)")
    p.add_argument("--port", type=int, default=8765, help="Port to bind (default: 8765)")
    p.add_argument(
        "--hermes-acp-cmd",
        default="hermes-acp",
        help="Command to spawn for the ACP subprocess (default: hermes-acp). "
        "May include extra args, parsed via shlex.",
    )
    p.add_argument(
        "--session-ttl-seconds",
        type=float,
        default=30.0,
        help="Seconds to keep an idle session's subprocess alive after the last "
        "subscriber disconnects, so reconnects don't lose state. Set to 0 for "
        "immediate teardown. (default: 30)",
    )
    p.add_argument(
        "--log-level",
        default="info",
        choices=("debug", "info", "warning", "error"),
    )
    return p.parse_args(argv)


LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOG_DATEFMT = "%H:%M:%S"


def _uvicorn_log_config(level: str) -> dict:
    """Mirror our basicConfig format onto uvicorn's loggers so startup output
    isn't a Frankenstein of two different formatters."""
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {"format": LOG_FORMAT, "datefmt": LOG_DATEFMT},
        },
        "handlers": {
            "default": {
                "formatter": "default",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stderr",
            },
        },
        "loggers": {
            "uvicorn": {"handlers": ["default"], "level": level.upper(), "propagate": False},
            "uvicorn.error": {"handlers": ["default"], "level": level.upper(), "propagate": False},
            "uvicorn.access": {"handlers": ["default"], "level": level.upper(), "propagate": False},
        },
    }


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    logging.basicConfig(
        level=args.log_level.upper(),
        format=LOG_FORMAT,
        datefmt=LOG_DATEFMT,
        stream=sys.stderr,
    )

    parts = shlex.split(args.hermes_acp_cmd)
    if not parts:
        print("--hermes-acp-cmd must not be empty", file=sys.stderr)
        sys.exit(2)
    config = AppConfig(
        acp_command=parts[0],
        acp_args=tuple(parts[1:]),
        session_ttl_seconds=args.session_ttl_seconds,
    )
    app = create_app(config)

    logger = logging.getLogger("hermes_bridge")
    logger.info("hermes-bridge %s starting", __version__)
    logger.info("listening on %s:%d (ws endpoint: /acp?session=<id>)", args.host, args.port)
    logger.info("acp command: %s", args.hermes_acp_cmd)
    if args.session_ttl_seconds <= 0:
        logger.info("session ttl: 0s (immediate teardown on last disconnect)")
    else:
        logger.info("session ttl: %.0fs", args.session_ttl_seconds)

    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        log_level=args.log_level,
        access_log=False,
        log_config=_uvicorn_log_config(args.log_level),
    )


if __name__ == "__main__":
    main()
