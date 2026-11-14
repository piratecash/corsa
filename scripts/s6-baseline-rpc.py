#!/usr/bin/env python3
"""Collect a bounded, read-only S6 pilot through authenticated localhost RPC."""

import argparse
import base64
from datetime import datetime, timezone
from http.client import HTTPConnection, HTTPException
import json
import os
import time

COMMANDS = ("getBaselineClock", "getNodeStatus", "fetchRouteSummary", "getNodeStatus", "getBaselineClock")
MAX_RESPONSE = 1024 * 1024


def read(port, authorization, command):
    started = datetime.now(timezone.utc).isoformat()
    result = {"command": command, "client_started_at": started}
    connection = HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        connection.request("POST", "/rpc/v1/exec", json.dumps({"command": command}),
                           {"Authorization": authorization, "Content-Type": "application/json"})
        with connection.getresponse() as response:
            result["http_status"] = response.status
            raw = response.read(MAX_RESPONSE + 1)
            if len(raw) > MAX_RESPONSE:
                result["state"] = "oversize"
            else:
                body = json.loads(raw)
                if type(body) is not dict:
                    result["state"] = "unexpected_json"
                else:
                    result["state"] = "observed" if response.status == 200 and "error" not in body else "rpc_error"
                    result["body"] = body
    except (OSError, HTTPException, ValueError):
        result["state"] = "incomplete"
    finally:
        connection.close()
    result["client_finished_at"] = datetime.now(timezone.utc).isoformat()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=46464)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("invalid local RPC port")
    username, password = (os.environ.get(key) for key in ("CORSA_RPC_USERNAME", "CORSA_RPC_PASSWORD"))
    if not username or not password:
        parser.error("set CORSA_RPC_USERNAME and CORSA_RPC_PASSWORD")
    authorization = "Basic " + base64.b64encode(f"{username}:{password}".encode()).decode()
    os.umask(0o077)
    complete = True
    with open(args.output, "x", encoding="utf-8") as output:
        start = time.monotonic()
        for index in range(16):
            time.sleep(max(0, start + 60 * index - time.monotonic()))
            rows = [read(args.port, authorization, command) for command in COMMANDS]
            complete &= all(row["state"] == "observed" and
                            (row["command"] != "getBaselineClock" or row["body"].get("status") == "sample")
                            for row in rows)
            output.write(json.dumps({"index": index, "rpc": rows}) + "\n")
            output.flush()
    return 0 if complete else 2


if __name__ == "__main__":
    raise SystemExit(main())
