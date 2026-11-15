"""Offline collector mechanics using fake transport/credentials and own signals."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import signal
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('capture_gaps_collect', HERE / 'capture_invoice_gaps.py')
gaps = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gaps)
BASELINE = PINS = None
SYNTHETIC_TOKEN = 'SYNTHETIC-NOT-A-PROVIDER-CREDENTIAL'
SOURCE = '0' * 64


class CollectorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        global BASELINE, PINS
        from synthetic_fixtures import baseline
        cls.fixture = tempfile.TemporaryDirectory(prefix='invoice-SYNTHETIC-baseline-')
        cls.addClassCleanup(cls.fixture.cleanup)
        BASELINE, PINS = baseline(Path(cls.fixture.name), gaps)
        gaps.PINS_SHA = gaps.digest(PINS.read_bytes())
        cls.baseline = gaps.load_baseline(BASELINE, PINS)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='capture-collect-SYNTHETIC-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.read_fd, self.write_fd = os.pipe()
        self.channel = os.fdopen(self.read_fd, 'rb', buffering=0)
        self.addCleanup(self.channel.close)
        self.addCleanup(self.close_writer)
        self.calls = []
        self.attempt()

    def close_writer(self):
        if self.write_fd is not None:
            os.close(self.write_fd)
            self.write_fd = None

    def attempt(self, **changes):
        value = {'schema': 'stoarama.capture-invoice-attempt.v1',
                 'shell_pid': os.getpid(), 'started_at': gaps.now(),
                 'monotonic_start': time.monotonic(),
                 'deadline': 0,
                 'source_sha256': SOURCE, 'baseline_pins_sha256': gaps.PINS_SHA}
        value['deadline'] = value['monotonic_start'] + 55
        value.update(changes)
        path = self.root / 'attempt.json'
        path.write_bytes(gaps.canonical(value))

    def body(self, index):
        if index in (0, 6):
            return copy.deepcopy(self.baseline['account.json']['body'])
        month, _, number, _, total = gaps.PAGES[index - 1]
        rows = copy.deepcopy(self.baseline['invoice-full-2026-%s.json' % month]['invoice_items'][(number - 1) * 200:])
        return {'invoice_items': rows, 'meta': {'total': total}, 'links': {}}

    def fetch(self, path, token):
        self.assertEqual(token, SYNTHETIC_TOKEN)
        index = len(self.calls)
        self.assertEqual(path, gaps.requests()[index][1])
        self.calls.append(path)
        return 200, 'SYNTHETIC-%d' % index, 'Sat, 01 Jan 2000 00:00:00 GMT', gaps.canonical(self.body(index))

    def collect(self, fetch=None, seconds=55):
        # Replace the os.environ object temporarily; no inherited real values
        # are inspected, copied, cleared or passed to the fake transport.
        with (patch.object(gaps, 'REMOTE_OUT', self.root),
              patch.object(gaps, 'REMOTE_BASE', BASELINE),
              patch.object(gaps, 'REMOTE_PINS', PINS),
              patch.object(gaps, 'RUN_SECONDS', seconds),
              patch.object(gaps.os, 'environ', {'DIGITALOCEAN_TOKEN': SYNTHETIC_TOKEN})):
            try:
                attempt = json.loads((self.root / 'attempt.json').read_bytes())
                attempt['deadline'] = attempt['monotonic_start'] + seconds
                (self.root / 'attempt.json').write_bytes(gaps.canonical(attempt))
                with gaps.Deadline(attempt['monotonic_start'], attempt['deadline']) as budget:
                    return gaps.collect(SOURCE, fetch or self.fetch, self.channel, budget)
            finally:
                self.assertFalse(any(t.name == 'capture-evidence-channel' for t in threading.enumerate()))

    def failure(self):
        return json.loads((self.root / 'failure.json').read_bytes())

    def test_exact_seven_fake_reads_and_raw_receipt_integrity(self):
        result = self.collect()
        self.assertEqual(result['outcome'], 'complete')
        self.assertEqual(self.calls, [path for _, path in gaps.requests()])
        self.assertFalse(result['apply_authority'])
        manifest = json.loads((self.root / 'manifest.json').read_bytes())
        self.assertEqual(manifest['validation']['response_count'], 7)
        self.assertFalse(manifest['validation']['owner_death_proven'])
        lines = (self.root / 'SHA256SUMS').read_text().splitlines()
        names = set()
        for line in lines:
            sha, name = line.split('  ', 1)
            names.add(name)
            raw = (self.root / name).read_bytes()
            self.assertEqual(gaps.digest(raw), sha)
            self.assertNotIn(SYNTHETIC_TOKEN.encode(), raw)
        self.assertEqual(names, {p.name for p in self.root.iterdir()} - {'SHA256SUMS'})
        for response in manifest['validation']['responses']:
            raw = (self.root / (response['name'] + '.body.json')).read_bytes()
            self.assertEqual(gaps.digest(raw), response['body_sha256'])
        with self.assertRaises(FileExistsError):
            self.collect()  # started.json is exclusive: no second request.
        self.assertEqual(len(self.calls), 7)

    def test_first_failure_preserves_stage_and_never_retries(self):
        def fetch(path, token):
            if len(self.calls) == 2:
                raise OSError('SYNTHETIC transport exception ' + token)
            return self.fetch(path, token)
        with self.assertRaises(gaps.EvidenceError):
            self.collect(fetch)
        failure = self.failure()
        self.assertEqual((failure['outcome'], failure['stage'], failure['completed_requests']), ('request_failed', 'request', 2))
        self.assertEqual(len(self.calls), 2)
        self.assertNotIn(SYNTHETIC_TOKEN, json.dumps(failure))
        self.assertFalse((self.root / 'manifest.json').exists())

    def test_wrong_account_or_status_stops_before_second_request(self):
        def fetch(path, token):
            self.calls.append(path)
            body = self.body(0)
            body['account']['team']['uuid'] = 'SYNTHETIC-wrong-team'
            return 200, 'SYNTHETIC-1', '', gaps.canonical(body)
        with self.assertRaises(gaps.EvidenceError):
            self.collect(fetch)
        self.assertEqual((len(self.calls), self.failure()['completed_requests']), (1, 0))

    def test_reflected_fake_token_is_never_written(self):
        def fetch(path, token):
            self.calls.append(path)
            return 200, 'SYNTHETIC-1', '', json.dumps({'reflected': token}).encode()
        with self.assertRaises(gaps.EvidenceError):
            self.collect(fetch)
        self.assertEqual(len(self.calls), 1)
        for path in self.root.iterdir():
            self.assertNotIn(SYNTHETIC_TOKEN.encode(), path.read_bytes())

    def test_baseline_failure_precedes_credentials_and_transport(self):
        class ForbiddenCredentials(dict):
            def pop(self, *args):
                raise AssertionError('credential accessed before baseline')
        with (patch.object(gaps, 'REMOTE_OUT', self.root),
              patch.object(gaps, 'load_baseline', side_effect=gaps.EvidenceError('SYNTHETIC bad baseline')),
              patch.object(gaps.os, 'environ', ForbiddenCredentials())):
            with self.assertRaises(gaps.EvidenceError):
                attempt = json.loads((self.root / 'attempt.json').read_bytes())
                with gaps.Deadline(attempt['monotonic_start'], attempt['deadline']) as budget:
                    gaps.collect(SOURCE, self.fetch, self.channel, budget)
        self.assertEqual(len(self.calls), 0)
        self.assertEqual(self.failure()['stage'], 'baseline')

    def test_actual_own_alarm_records_timeout_and_stops(self):
        def fetch(path, token):
            self.calls.append(path)
            time.sleep(5)
            raise AssertionError('own alarm did not interrupt')
        started = time.monotonic()
        with self.assertRaises(gaps.EvidenceError):
            self.collect(fetch, seconds=0.25)
        self.assertLess(time.monotonic() - started, 2)
        self.assertEqual((self.failure()['outcome'], len(self.calls)), ('timeout', 1))

    def test_actual_own_sigterm_records_interruption(self):
        def fetch(path, token):
            self.calls.append(path)
            os.kill(os.getpid(), signal.SIGTERM)
            raise AssertionError('own handled signal did not interrupt')
        with self.assertRaises(gaps.EvidenceError):
            self.collect(fetch)
        self.assertEqual((self.failure()['outcome'], len(self.calls)), ('interrupted', 1))

    def test_actual_pipe_disconnect_stops_and_reaps_watcher(self):
        def fetch(path, token):
            self.calls.append(path)
            self.close_writer()  # EOF is the simulated SSH stdin disconnect.
            time.sleep(5)
            raise AssertionError('own disconnect watcher did not interrupt')
        started = time.monotonic()
        with self.assertRaises(gaps.EvidenceError):
            self.collect(fetch)
        self.assertLess(time.monotonic() - started, 2)
        failure = self.failure()
        self.assertEqual((failure['outcome'], failure['channel_disconnected']), ('interrupted', True))
        self.assertEqual(len(self.calls), 1)

    def test_unproved_watcher_exit_cannot_publish_manifest(self):
        class UnprovedWatch:
            disconnected = threading.Event()
            def __init__(self, channel):
                pass
            def start(self):
                pass
            def close(self):
                raise gaps.EvidenceError('SYNTHETIC exit unproved')
        with patch.object(gaps, 'ChannelWatch', UnprovedWatch):
            with self.assertRaises(gaps.EvidenceError):
                self.collect()
        self.assertEqual(len(self.calls), 7)
        self.assertFalse((self.root / 'manifest.json').exists())


if __name__ == '__main__':
    unittest.main(verbosity=2)
