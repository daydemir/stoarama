"""Real shared-server lifecycle tests: python3 -B scripts/test_test_db.py.

Creates/deletes only uniquely named test databases, never another cluster.
"""

from concurrent.futures import ThreadPoolExecutor
import importlib.machinery
import importlib.util
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import unittest
from urllib.parse import urlparse
import uuid


HELPER = Path(__file__).with_name("test-db")
loader = importlib.machinery.SourceFileLoader("test_db", str(HELPER))
spec = importlib.util.spec_from_loader(loader.name, loader)
db = importlib.util.module_from_spec(spec)
loader.exec_module(db)


def call(*args, env=None):
    return subprocess.run([str(HELPER), *args], text=True, capture_output=True, env=env)


def name_from_url(value):
    return urlparse(value.strip()).path.lstrip("/")


class SharedDatabaseTests(unittest.TestCase):
    def setUp(self):
        self.cluster = db.Cluster()
        self.names = []

    def tearDown(self):
        with self.cluster.lock():
            self.cluster.verify()
            for name in self.names:
                # Also removes this test's deliberately unmarked database.
                self.cluster.sql(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
                (self.cluster.root / (name + ".lease")).unlink(missing_ok=True)

    def create(self):
        name = db.database_name(uuid.uuid4().hex)
        self.names.append(name)
        result = call("create", name)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(name_from_url(result.stdout), name)
        return name

    def exists(self, name):
        return name in {row["name"] for row in self.cluster.databases()}

    def age(self, name):
        self.cluster.sql(f"COMMENT ON DATABASE \"{name}\" IS "
                         f"'{db.MARKER}{int(time.time()) - 13 * 3600}'")

    def test_01_concurrent_start_and_isolated_cleanup(self):
        command = ["run", "--", sys.executable, "-c",
                   "import os,time; print(os.environ['STOARAMA_TEST_DATABASE_URL']); "
                   "time.sleep(.2)"]
        with ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(lambda _: call(*command), range(6)))
        for result in results:
            self.assertEqual(result.returncode, 0, result.stderr)
        names = [name_from_url(result.stdout) for result in results]
        self.assertEqual(len(set(names)), 6)
        self.assertTrue(all(not self.exists(name) for name in names))

    def test_named_create_validation_and_ambient_pg_settings(self):
        name = self.create()
        self.assertNotEqual(call("create", name).returncode, 0)
        self.assertEqual(call("drop", "postgres").returncode, 0)
        self.assertTrue(self.exists("postgres"))
        self.assertNotEqual(call("create", "bad;name").returncode, 0)
        result = call("drop", name, env=dict(os.environ, PGHOST="invalid",
                                            PGPORT="1", PGDATABASE="production"))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(self.exists(name))

    def test_failed_command_cleanup(self):
        result = call("run", "--", sys.executable, "-c",
                      "import os,sys; print(os.environ['STOARAMA_TEST_DATABASE_URL']); "
                      "sys.exit(37)")
        self.assertEqual(result.returncode, 37, result.stderr)
        self.assertFalse(self.exists(name_from_url(result.stdout)))

    def test_gc_protects_recent_connected_reserved_and_unmarked(self):
        old, recent, connected, reserved, unmarked = [self.create() for _ in range(5)]
        for name in (old, connected, reserved):
            self.age(name)
        self.cluster.sql(f'COMMENT ON DATABASE "{unmarked}" IS NULL')
        client = subprocess.Popen([
            str(self.cluster.bin / "psql"), "-X", "-qAt", "-h", str(self.cluster.root),
            "-p", str(db.PORT), "-U", db.ROLE, "-d", connected,
            "-c", "SELECT pg_sleep(30)"], stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, env=self.cluster.env)
        try:
            deadline = time.monotonic() + 10
            while self.cluster.sql("SELECT count(*) FROM pg_stat_activity "
                                   f"WHERE datname = '{connected}'") == "0":
                self.assertLess(time.monotonic(), deadline)
                time.sleep(.02)
            with self.cluster.reservation(reserved):
                result = call("gc")
                self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse(self.exists(old))
            for name in (recent, connected, reserved, unmarked):
                self.assertTrue(self.exists(name), name)
            self.assertNotEqual(call("drop", unmarked).returncode, 0)
            self.assertNotEqual(call("stop").returncode, 0)
        finally:
            client.terminate()
            client.wait(timeout=10)
        # A crashed reservation leaves an unlocked file, which GC can reclaim.
        self.assertEqual(call("gc").returncode, 0)
        self.assertFalse(self.exists(reserved))

    def test_signal_cleanup_and_idle_run_protection(self):
        process = subprocess.Popen([
            str(HELPER), "run", "--", sys.executable, "-u", "-c",
            "import os,time; print(os.environ['STOARAMA_TEST_DATABASE_URL']); time.sleep(60)"],
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            name = name_from_url(process.stdout.readline())
            self.names.append(name)
            self.age(name)
            self.assertEqual(call("gc").returncode, 0)
            self.assertTrue(self.exists(name))
            self.assertNotEqual(call("drop", name).returncode, 0)
            self.assertNotEqual(call("stop").returncode, 0)
            process.send_signal(signal.SIGTERM)
            _, errors = process.communicate(timeout=15)
            self.assertEqual(process.returncode, 143, errors)
            self.assertFalse(self.exists(name))
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate()


if __name__ == "__main__":
    unittest.main(verbosity=2)
