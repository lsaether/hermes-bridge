"""CLI entry point: `hermes-bridge [--host ...] [--port ...] [--hermes-acp-cmd ...]`."""

from __future__ import annotations

import argparse
import logging
import shlex
import sys

import uvicorn

from .app import create_app
from .ws_relay import RelayConfig


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
        "--log-level",
        default="info",
        choices=("debug", "info", "warning", "error"),
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )

    parts = shlex.split(args.hermes_acp_cmd)
    if not parts:
        print("--hermes-acp-cmd must not be empty", file=sys.stderr)
        sys.exit(2)
    config = RelayConfig(command=parts[0], args=tuple(parts[1:]))
    app = create_app(config)

    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        log_level=args.log_level,
        access_log=False,
    )


if __name__ == "__main__":
    main()
