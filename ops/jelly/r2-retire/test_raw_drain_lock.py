#!/usr/bin/env python3
"""Two-session proof for raw-drain-lock.sql against a real PostgreSQL.

Local scratch database only; never point this at production:

    scripts/test-db run -- python3 ops/jelly/r2-retire/test_raw_drain_lock.py -v

The holder is a real second session whose marker arrives only after
pg_advisory_lock returns, so no startup polling is needed and there is no window
to race. The holder confirms it unlocked the key before the next receipt.

CI runs this file without a database, so it skips when
STOARAMA_TEST_DATABASE_URL is unset. With a database provided, a missing psql is
a failure, not a skip: a silently skipped proof is not a proof.
"""
import os
from pathlib import Path
import select
import shutil
import subprocess
import time
import unittest
from contextlib import contextmanager

HELPER = Path(__file__).with_name("raw-drain-lock.sql")
KEY = 7343226011173      # classid 1709, objid 3126902309
CONTROL = 7347520978469  # same objid 3126902309, classid 1710

# Bound every step that could otherwise hang: the TCP/auth handshake, the lock
# acquisition, and the wait for the holder's marker line.
CONNECT_TIMEOUT = "10"
HOLDER_PGOPTIONS = "-c lock_timeout=15s -c statement_timeout=30s"
MARKER_TIMEOUT = 30


@unittest.skipUnless(os.environ.get("STOARAMA_TEST_DATABASE_URL"),
                     "needs scripts/test-db run and a scratch database")
class RawDrainLockTests(unittest.TestCase):
    def setUp(self):
        # PATH only, like every other repo script; no per-machine fallbacks.
        self.psql = shutil.which("psql")
        if self.psql is None:
            self.fail("psql not found on PATH, but a test database was provided")
        self.url = os.environ["STOARAMA_TEST_DATABASE_URL"]

    def receipt(self, label=None):
        """Read the helper with psql -f from a session that holds nothing."""
        command = [self.psql, "-XqAt", "-v", "ON_ERROR_STOP=1"]
        if label is not None:
            command += ["-v", "lock_label=" + label]
        result = subprocess.run(command + [self.url, "-f", str(HELPER)],
                                capture_output=True, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout.strip()

    def marker_line(self, proc):
        """Wait at most MARKER_TIMEOUT for the holder's first nonblank line.

        A broken fixture -- unreachable server, refused lock, dying session --
        ends the process and yields EOF here instead of blocking forever.
        """
        deadline = time.monotonic() + MARKER_TIMEOUT
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return ""
            readable, _, _ = select.select([proc.stdout.fileno()], [], [], remaining)
            if not readable:
                return ""
            line = proc.stdout.readline()
            if line == "":
                return ""  # holder exited without signalling
            if line.strip():
                return line.strip()

    @contextmanager
    def hold(self, key):
        """Take KEY in a second session for the life of the with-block."""
        proc = subprocess.Popen(
            [self.psql, "-XqAt", "-v", "ON_ERROR_STOP=1", self.url],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True,
            env=dict(os.environ, PGCONNECT_TIMEOUT=CONNECT_TIMEOUT,
                     PGOPTIONS=HOLDER_PGOPTIONS))
        try:
            proc.stdin.write(
                "SELECT 'ready|' || pg_backend_pid() FROM pg_advisory_lock(%d);\n" % key)
            proc.stdin.flush()
            line = self.marker_line(proc)
            self.assertTrue(
                line.startswith("ready|") and line.split("|")[1],
                "holder never signalled for key %d: %r" % (key, line))
            yield
            proc.stdin.write("SELECT pg_advisory_unlock(%d);\n" % key)
            proc.stdin.flush()
            self.assertEqual(self.marker_line(proc), "t")
        finally:
            if proc.poll() is None and proc.stdin is not None and not proc.stdin.closed:
                proc.stdin.close()
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=30)
            for stream in (proc.stdout, proc.stderr):
                stream.close()

    def test_receipt_tracks_only_the_real_key(self):
        self.assertEqual(self.receipt(), "lock_1173_holders|0")

        with self.hold(KEY):
            self.assertEqual(self.receipt(), "lock_1173_holders|1")
            self.assertEqual(self.receipt(label="raw_drain_attempt"),
                             "raw_drain_attempt|1")

        self.assertEqual(self.receipt(), "lock_1173_holders|0")

        # Same low 32 bits, different high 32 bits: the receipt must stay 0.
        with self.hold(CONTROL):
            self.assertEqual(self.receipt(), "lock_1173_holders|0")

        self.assertEqual(self.receipt(), "lock_1173_holders|0")


if __name__ == "__main__":
    unittest.main()
