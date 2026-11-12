#!/usr/bin/env python3
"""Jelly-only apply wrapper. Never starts/stops jobs or changes credentials."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess


def check_repair_stopped(keys):
    if not any(key.startswith("repaired/") for key in keys):
        return
    # A loaded idle job can restart. Require bootout, not merely a missing PID.
    # Capture launchctl output: it can contain environment values; never print it.
    domain = "gui/{}".format(os.getuid())
    domain_state = subprocess.run(["/bin/launchctl", "print", domain], capture_output=True)
    if domain_state.returncode:
        raise RuntimeError("launchd domain unavailable; repair stop state unknown")
    job = subprocess.run(["/bin/launchctl", "print", domain + "/com.stoarama.repair"], capture_output=True)
    if job.returncode != 113 or b"Could not find service" not in job.stderr:
        raise RuntimeError("repair job must be booted out; stopped state unproven")


def run():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stoaramactl", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("args", nargs=argparse.REMAINDER)
    opts = parser.parse_args()
    if socket.gethostname().split(".")[0] != "jellys-mac-mini":
        raise RuntimeError("apply wrapper must run on Jelly")
    body = Path(opts.manifest).read_bytes()
    if hashlib.sha256(body).hexdigest() != opts.expected_sha256:
        raise RuntimeError("manifest differs from reviewed digest")
    records = [json.loads(line) for line in body.splitlines()]
    keys = [record["object_key"] for record in records[1:]]
    check_repair_stopped(keys)
    # The Go CLI independently refuses every repaired/ key, even when stopped,
    # and every key overlapping other ungated writers. No local state waives it.
    extra = opts.args[1:] if opts.args[:1] == ["--"] else opts.args
    if any(arg.split("=", 1)[0] in ("--manifest", "--expected-sha256") for arg in extra):
        raise RuntimeError("duplicate manifest options refused")
    os.execv(opts.stoaramactl, [opts.stoaramactl, "r2-retire", "apply", "--manifest", opts.manifest,
                              "--expected-sha256", opts.expected_sha256] + extra)


if __name__ == "__main__":
    try:
        run()
    except (RuntimeError, OSError, ValueError, KeyError):
        # Neither subprocess output, supplied CLI arguments nor manifest bytes
        # are safe error detail: they may contain URLs or credential material.
        raise SystemExit("Jelly R2 retirement apply preflight refused") from None
