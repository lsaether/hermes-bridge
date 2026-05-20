"""Tiny fake ACP subprocess for chunk 2-5 + v0.5.1 testing.

For each client message we receive, we emit:
  1) A session/update notification.
  2) If the message is a request (had id+method):
       - For "session/new": return a fresh UUID sessionId. The bridge's
         v0.5.1 session-resolution feature should ensure only the first
         subscriber's session/new reaches us per bridge session.
       - For "session/prompt": delay 1.5s before sending the response so the
         test can race a second prompt against it. No agent-initiated request.
       - For everything else: immediate response + an agent-initiated
         permission/request.
"""

import json
import sys
import time
import uuid

AGENT_ID_BASE = 1000
PROMPT_DELAY_S = 1.5


def main() -> None:
    print("fake_acp: ready", file=sys.stderr, flush=True)
    agent_id = AGENT_ID_BASE

    for line in sys.stdin:
        stripped = line.strip()
        if not stripped:
            continue

        print(
            json.dumps(
                {"jsonrpc": "2.0", "method": "session/update", "params": {"echo": stripped[:120]}}
            ),
            flush=True,
        )

        try:
            msg = json.loads(stripped)
        except json.JSONDecodeError:
            continue

        if not isinstance(msg, dict) or "id" not in msg:
            continue

        method = msg.get("method")

        if method == "session/new":
            print(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": msg["id"],
                        "result": {"sessionId": str(uuid.uuid4())},
                    }
                ),
                flush=True,
            )
            continue

        if method == "session/prompt":
            time.sleep(PROMPT_DELAY_S)
            print(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": msg["id"],
                        "result": {"acked": True, "method": method},
                    }
                ),
                flush=True,
            )
            continue

        # Normal echo response.
        print(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": msg["id"],
                    "result": {"acked": True, "method": method},
                }
            ),
            flush=True,
        )

        if method:
            print(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": agent_id,
                        "method": "permission/request",
                        "params": {
                            "trigger_method": method,
                            "trigger_bridge_id": msg["id"],
                        },
                    }
                ),
                flush=True,
            )
            agent_id += 1


if __name__ == "__main__":
    main()
