"""pytest fixtures for v0.5 integration tests.

Each test spins up a `hermes-bridge` subprocess wired to the in-repo
`tests/fake_acp.py` fake ACP server. The fake is deterministic and avoids
real LLM calls.
"""

from __future__ import annotations

import contextlib
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx

TESTS_DIR = Path(__file__).resolve().parent
FAKE_ACP_PATH = TESTS_DIR / "fake_acp.py"


def _find_free_port() -> int:
    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@contextlib.contextmanager
def run_bridge(ttl_seconds: float = 30.0):
    """Spawn a bridge subprocess; yield connection info; tear down on exit."""
    port = _find_free_port()
    base_url = f"http://127.0.0.1:{port}"
    ws_url = f"ws://127.0.0.1:{port}/acp"

    proc = subprocess.Popen(
        [
            sys.executable, "-m", "hermes_bridge",
            "--host", "127.0.0.1",
            "--port", str(port),
            "--hermes-acp-cmd", f"{sys.executable} {FAKE_ACP_PATH}",
            "--session-ttl-seconds", str(ttl_seconds),
            "--log-level", "warning",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    deadline = time.monotonic() + 5.0
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            r = httpx.get(f"{base_url}/healthz", timeout=0.5)
            if r.status_code == 200:
                break
        except Exception as exc:
            last_err = exc
        time.sleep(0.1)
    else:
        proc.terminate()
        try:
            _, err = proc.communicate(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()
            _, err = proc.communicate()
        raise RuntimeError(
            f"bridge failed to start within 5s (last_err={last_err}); stderr=\n{err.decode()}"
        )

    try:
        yield {"port": port, "base_url": base_url, "ws_url": ws_url}
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def healthz(base_url: str) -> dict:
    return httpx.get(f"{base_url}/healthz", timeout=1.0).json()
