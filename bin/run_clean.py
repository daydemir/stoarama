#!/usr/bin/env python3
"""Run one command in a new process group with a minimal environment."""

import json
import os
import signal
import subprocess
import sys
import time


def main() -> int:
    argv = json.loads(os.environ["STOARAMA_VERIFY_ARGV_JSON"])
    env = json.loads(os.environ["STOARAMA_VERIFY_ENV_JSON"])
    if not isinstance(argv, list) or not argv or not all(isinstance(item, str) for item in argv):
        return 97
    if not isinstance(env, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in env.items()):
        return 98
    child = subprocess.Popen(argv, env=env, start_new_session=True)
    pgid_file = os.environ.get("STOARAMA_VERIFY_PGID_FILE")
    if pgid_file:
        with open(pgid_file, "x", encoding="ascii") as handle:
            handle.write(f"{child.pid}\n")

    def forward(signum, _frame):
        try:
            os.killpg(child.pid, signum)
        except ProcessLookupError:
            return
        deadline = time.monotonic() + 1.0
        while child.poll() is None and time.monotonic() < deadline:
            time.sleep(0.02)
        if child.poll() is None:
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

    for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(signum, forward)
    return child.wait()


if __name__ == "__main__":
    raise SystemExit(main())
