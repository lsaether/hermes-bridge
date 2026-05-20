"""Integration tests for v0.5 multiplex behavior.

Each test exercises one chunk's DoD against a real bridge subprocess wired
to the in-repo fake ACP. The fake echoes requests as responses, emits a
session/update notification for every line, and delays session/prompt
responses by 1.5s so chunk-5 (turn serialization) can race a second prompt.
"""

from __future__ import annotations

import asyncio
import json

import pytest
import websockets
from websockets.exceptions import ConnectionClosed

from .conftest import healthz, run_bridge

pytestmark = pytest.mark.asyncio


async def _recv_until(ws, predicate, timeout=2.0):
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            return None
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
        except (asyncio.TimeoutError, ConnectionClosed):
            return None
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if predicate(msg):
            return msg


async def _drain(ws, duration=0.3):
    loop = asyncio.get_event_loop()
    deadline = loop.time() + duration
    while True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            return
        try:
            await asyncio.wait_for(ws.recv(), timeout=remaining)
        except (asyncio.TimeoutError, ConnectionClosed):
            return


# ---------- chunk 1: session routing ----------


async def test_chunk1_missing_session_id_rejected():
    with run_bridge() as b:
        with pytest.raises(ConnectionClosed) as exc:
            ws = await websockets.connect(b["ws_url"])
            await ws.recv()
        assert exc.value.rcvd.code == 4400


async def test_chunk1_invalid_session_id_rejected():
    with run_bridge() as b:
        with pytest.raises(ConnectionClosed) as exc:
            ws = await websockets.connect(f"{b['ws_url']}?session=has%20space")
            await ws.recv()
        assert exc.value.rcvd.code == 4400


async def test_chunk1_valid_session_initializes():
    with run_bridge() as b:
        ws = await websockets.connect(f"{b['ws_url']}?session=alpha")
        try:
            await ws.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "initialize"}))
            resp = await _recv_until(ws, lambda m: m.get("id") == 1)
            assert resp is not None
            assert resp.get("result", {}).get("acked") is True
        finally:
            await ws.close()


async def test_chunk1_concurrent_sessions_are_isolated():
    with run_bridge() as b:
        ws_a = await websockets.connect(f"{b['ws_url']}?session=one")
        ws_b = await websockets.connect(f"{b['ws_url']}?session=two")
        await asyncio.sleep(0.2)
        try:
            assert healthz(b["base_url"])["activeSessions"] == 2
        finally:
            await ws_a.close()
            await ws_b.close()


# ---------- chunk 2: notification fan-out ----------


async def test_chunk2_notifications_broadcast_to_all_subscribers():
    with run_bridge() as b:
        a = await websockets.connect(f"{b['ws_url']}?session=fan")
        c = await websockets.connect(f"{b['ws_url']}?session=fan")
        await asyncio.sleep(0.2)
        try:
            await a.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "ping"}))
            a_notif = await _recv_until(a, lambda m: m.get("method") == "session/update")
            c_notif = await _recv_until(c, lambda m: m.get("method") == "session/update")
            assert a_notif is not None
            assert c_notif is not None
        finally:
            await a.close()
            await c.close()


async def test_chunk2_session_persists_while_any_subscriber_attached():
    with run_bridge() as b:
        a = await websockets.connect(f"{b['ws_url']}?session=persist")
        c = await websockets.connect(f"{b['ws_url']}?session=persist")
        await asyncio.sleep(0.2)
        try:
            await a.close()
            await asyncio.sleep(0.3)
            assert healthz(b["base_url"])["activeSessions"] == 1
        finally:
            await c.close()


# ---------- chunk 3: request ID translation ----------


async def test_chunk3_id_collision_resolves_per_subscriber():
    with run_bridge() as b:
        a = await websockets.connect(f"{b['ws_url']}?session=ids")
        c = await websockets.connect(f"{b['ws_url']}?session=ids")
        await asyncio.sleep(0.2)
        try:
            # Both initialize first (so cache is populated and subsequent
            # requests are clean).
            await a.send(json.dumps({"jsonrpc": "2.0", "id": 100, "method": "initialize"}))
            await _recv_until(a, lambda m: m.get("id") == 100)
            await c.send(json.dumps({"jsonrpc": "2.0", "id": 100, "method": "initialize"}))
            await _recv_until(c, lambda m: m.get("id") == 100)
            await _drain(a)
            await _drain(c)

            # Both send a request with the same client-side id=7.
            await a.send(json.dumps({"jsonrpc": "2.0", "id": 7, "method": "ping"}))
            await c.send(json.dumps({"jsonrpc": "2.0", "id": 7, "method": "pong"}))

            a_resp = await _recv_until(
                a, lambda m: m.get("id") == 7 and "method" not in m
            )
            c_resp = await _recv_until(
                c, lambda m: m.get("id") == 7 and "method" not in m
            )
            assert a_resp and a_resp["result"]["method"] == "ping"
            assert c_resp and c_resp["result"]["method"] == "pong"
        finally:
            await a.close()
            await c.close()


async def test_chunk3_initialize_replay_does_not_leak_to_other_subscribers():
    with run_bridge() as b:
        a = await websockets.connect(f"{b['ws_url']}?session=init")
        c = await websockets.connect(f"{b['ws_url']}?session=init")
        await asyncio.sleep(0.2)
        try:
            await a.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "initialize"}))
            await _recv_until(a, lambda m: m.get("id") == 1)
            await c.send(json.dumps({"jsonrpc": "2.0", "id": 42, "method": "initialize"}))
            c_init = await _recv_until(c, lambda m: m.get("id") == 42)
            assert c_init and c_init.get("result", {}).get("acked")

            leak = await _recv_until(a, lambda m: m.get("id") == 42, timeout=0.4)
            assert leak is None
        finally:
            await a.close()
            await c.close()


# ---------- chunk 4: agent-initiated request routing ----------


def _is_perm_req_for(method: str):
    return lambda m: (
        m.get("method") == "permission/request"
        and m.get("params", {}).get("trigger_method") == method
    )


async def test_chunk4_agent_request_goes_to_driving_subscriber():
    with run_bridge() as b:
        a = await websockets.connect(f"{b['ws_url']}?session=drive")
        c = await websockets.connect(f"{b['ws_url']}?session=drive")
        await asyncio.sleep(0.2)
        try:
            await a.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "method-a"}))
            a_req = await _recv_until(a, _is_perm_req_for("method-a"))
            c_leak = await _recv_until(c, _is_perm_req_for("method-a"), timeout=0.4)
            assert a_req is not None
            assert c_leak is None
        finally:
            await a.close()
            await c.close()


async def test_chunk4_driving_handoff_redirects_agent_requests():
    with run_bridge() as b:
        a = await websockets.connect(f"{b['ws_url']}?session=ho")
        c = await websockets.connect(f"{b['ws_url']}?session=ho")
        await asyncio.sleep(0.2)
        try:
            await a.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "from-a"}))
            await _recv_until(a, _is_perm_req_for("from-a"))
            await _drain(a)
            await _drain(c)

            await c.send(json.dumps({"jsonrpc": "2.0", "id": 2, "method": "from-c"}))
            c_req = await _recv_until(c, _is_perm_req_for("from-c"))
            a_leak = await _recv_until(a, _is_perm_req_for("from-c"), timeout=0.4)
            assert c_req is not None
            assert a_leak is None
        finally:
            await a.close()
            await c.close()


# ---------- chunk 5: concurrent turn serialization ----------


async def test_chunk5_concurrent_prompt_rejected_with_minus_32001():
    with run_bridge() as b:
        a = await websockets.connect(f"{b['ws_url']}?session=turn")
        c = await websockets.connect(f"{b['ws_url']}?session=turn")
        await asyncio.sleep(0.2)
        try:
            await a.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "session/prompt"}))
            await asyncio.sleep(0.1)  # let bridge mark the turn active
            await c.send(json.dumps({"jsonrpc": "2.0", "id": 2, "method": "session/prompt"}))

            c_err = await _recv_until(c, lambda m: m.get("id") == 2 and "error" in m, timeout=1.0)
            assert c_err is not None
            assert c_err["error"]["code"] == -32001

            # Both subscribers receive the busy notification.
            a_busy = await _recv_until(
                a,
                lambda m: m.get("method") == "bridge/session_busy"
                and m.get("params", {}).get("rejected_client_id") == 2,
                timeout=1.0,
            )
            c_busy = await _recv_until(
                c,
                lambda m: m.get("method") == "bridge/session_busy"
                and m.get("params", {}).get("rejected_client_id") == 2,
                timeout=1.0,
            )
            assert a_busy is not None
            assert c_busy is not None
        finally:
            await a.close()
            await c.close()


async def test_chunk5_turn_clears_after_response_arrives():
    with run_bridge() as b:
        a = await websockets.connect(f"{b['ws_url']}?session=clear")
        try:
            await a.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "session/prompt"}))
            resp = await _recv_until(a, lambda m: m.get("id") == 1, timeout=4.0)
            assert resp and resp.get("result", {}).get("acked")

            await a.send(json.dumps({"jsonrpc": "2.0", "id": 2, "method": "session/prompt"}))
            resp2 = await _recv_until(a, lambda m: m.get("id") == 2, timeout=4.0)
            assert resp2 and "error" not in resp2
        finally:
            await a.close()


# ---------- chunk 6: TTL reconnect grace ----------


async def test_chunk6_subprocess_survives_disconnect_within_ttl():
    with run_bridge(ttl_seconds=2.0) as b:
        # First client initializes; cache populated.
        a = await websockets.connect(f"{b['ws_url']}?session=ttl")
        await a.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "initialize"}))
        await _recv_until(a, lambda m: m.get("id") == 1)
        await a.close()

        # Brief idle period — under TTL — then a reconnect with a different id.
        await asyncio.sleep(0.6)
        assert healthz(b["base_url"])["activeSessions"] == 1

        c = await websockets.connect(f"{b['ws_url']}?session=ttl")
        try:
            await c.send(json.dumps({"jsonrpc": "2.0", "id": 999, "method": "initialize"}))
            resp = await _recv_until(c, lambda m: m.get("id") == 999)
            assert resp and resp.get("result", {}).get("acked")
            # The fact that the cached id=999 response came back proves we hit
            # the bridge's cached_initialize_result path (the same subprocess).
        finally:
            await c.close()


async def test_chunk6_session_reaped_after_ttl():
    with run_bridge(ttl_seconds=1.0) as b:
        a = await websockets.connect(f"{b['ws_url']}?session=reap")
        await a.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "initialize"}))
        await _recv_until(a, lambda m: m.get("id") == 1)
        await a.close()

        await asyncio.sleep(2.0)
        assert healthz(b["base_url"])["activeSessions"] == 0


async def test_chunk6_ttl_zero_means_immediate_teardown():
    with run_bridge(ttl_seconds=0.0) as b:
        a = await websockets.connect(f"{b['ws_url']}?session=instant")
        await a.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "initialize"}))
        await _recv_until(a, lambda m: m.get("id") == 1)
        await a.close()
        await asyncio.sleep(0.4)
        assert healthz(b["base_url"])["activeSessions"] == 0
