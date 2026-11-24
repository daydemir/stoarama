"""Bulk NAS-only day-directory cleanup against disposable fixtures only."""
import datetime
import importlib.util
import json
import os
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
from zoneinfo import ZoneInfo

spec = importlib.util.spec_from_file_location('pull', Path(__file__).with_name('stoarama_pull.py'))
pull = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pull)

UTC = datetime.timezone.utc
DAY = datetime.date(2026, 9, 12)  # Saturday
DIR = 'shinjuku/September/12-Saturday'


def plaza(date, hhmmss):
    return 'p_shinjuku_%04d_%s_W%d_%s_hour_%s.mp4' % (
        date.year, pull.CLEANUP_MONTHS[date.month - 1], (date.day - 1) // 7 + 1,
        pull.CLEANUP_DAY_WEEKDAYS[date.weekday()], hhmmss)


class DayCleanupTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        (self.root / 'state').mkdir()
        self.cfg = SimpleNamespace(output_dir=self.root / 'clips', state_dir=self.root / 'state',
                                   inventory_file=self.root / 'state' / 'inventory.sqlite3', dry_run=False)
        self.inventory = pull.Inventory(self.cfg)
        self.addCleanup(self.inventory.close)
        self.dir = self.cfg.output_dir / DIR
        self.dir.mkdir(parents=True)
        # Stream 3 (Tokyo) keeps local [2026-09-16, 2026-10-06); stream 78 keeps everything.
        window = (datetime.datetime(2026, 9, 15, 15, tzinfo=UTC), datetime.datetime(2026, 10, 5, 15, tzinfo=UTC), 'Asia/Tokyo')
        self.pins = dict(windows={3: window, 78: window}, folders={'shinjuku': 3, 'seoul': 78, 'recordings/366': 3},
                         keep_everything=frozenset({78}))
        patcher = mock.patch.object(pull, 'cleanup_pins', side_effect=lambda: (self.pins, None))
        patcher.start()
        self.addCleanup(patcher.stop)
        version = mock.patch.object(pull, 'CLIENT_VERSION', pull.FAST_COPY_CLIENT_VERSION)
        version.start()
        self.addCleanup(version.stop)
        self.sent = []
        api = mock.patch.object(pull, 'cleanup_api_retry', side_effect=self.api)
        api.start()
        self.addCleanup(api.stop)
        self.next_permit = None

    def api(self, cfg, path, body):
        self.sent.append((path, json.loads(json.dumps(body))))
        if path == pull.CLEANUP_DAY_PERMIT_PATH:
            return {'unit': self.next_permit}
        return {'unit_id': body['unit_id'], 'state': body['outcome']}

    def permit(self, **fields):
        permit = dict(unit_id=7, plan_id=2, plan_sha256='b' * 64, attempt=1, permit_token='c' * 64,
                      permit_expires_at=(datetime.datetime.now(UTC) + datetime.timedelta(minutes=10)).isoformat(),
                      relative_dir=DIR, local_date=DAY.isoformat(), stream_id=3, timezone='Asia/Tokyo', files=2, bytes=7)
        permit.update(fields)
        return permit

    def write(self, name, data=b'x'):
        path = self.dir / name
        path.write_bytes(data)
        return path

    def receipts(self):
        return [body for path, body in self.sent if path == pull.CLEANUP_DAY_RECEIPT_PATH]

    def ledger(self):
        return [json.loads(line) for line in (self.cfg.state_dir / pull.CLEANUP_LEDGER_NAME).read_text().splitlines()]

    def test_path_dates_match_the_server_rule(self):
        tokyo = ZoneInfo('Asia/Tokyo')
        self.assertEqual(pull.cleanup_day_file_date(DIR, plaza(DAY, '103000'), tokyo), DAY)
        self.assertEqual(pull.cleanup_day_file_date(DIR, plaza(datetime.date(2020, 9, 12), '01'), tokyo), datetime.date(2020, 9, 12))
        self.assertIsNone(pull.cleanup_day_file_date(DIR, plaza(DAY, '103000').replace('_W2_', '_W3_'), tokyo))
        self.assertIsNone(pull.cleanup_day_file_date(DIR, 'notes.mp4', tokyo))
        self.assertEqual(pull.cleanup_day_file_date('recordings/366/continuous', '1789227000.mp4', tokyo), datetime.date(2026, 9, 13))
        self.assertEqual(pull.cleanup_day_file_date('recordings/366/77', '1789227000000.mp4', tokyo), datetime.date(2026, 9, 13))
        self.assertIsNone(pull.cleanup_day_file_date('recordings/366/77', '1789227000.mp4', tokyo))

    def test_deletes_one_directory_day_with_one_intent_and_one_receipt(self):
        media = [self.write(plaza(DAY, '103000'), b'abc'), self.write(plaza(DAY, '113000'), b'defg')]
        sidecars = [self.write(m.name + '.stoarama.json', b'{}') for m in media]
        orphan = self.write(plaza(DAY, '123000') + '.stoarama.json', b'{}')
        kept = [self.write(plaza(datetime.date(2020, 9, 12), '103000')), self.write('notes.txt'),
                self.write('.' + plaza(DAY, '133000') + '.partial')]
        self.next_permit = self.permit()
        self.assertTrue(pull.cleanup_day_cycle(self.cfg, self.inventory, threading.Event()))
        self.assertFalse(any(p.exists() for p in media + sidecars + [orphan]))
        self.assertTrue(all(p.exists() for p in kept))
        [receipt] = self.receipts()
        self.assertEqual(receipt, dict(unit_id=7, attempt=1, permit_token='c' * 64, client_version=pull.FAST_COPY_CLIENT_VERSION,
                                       outcome='deleted', media_files=2, media_bytes=7, sidecar_files=3, sidecar_bytes=6,
                                       remaining_files=0, refusal=''))
        events = self.ledger()
        self.assertEqual([e['event'] for e in events], ['day_intent', 'day_receipt'])
        self.assertEqual((events[0]['file_count'], events[0]['bytes']), (5, 13))
        self.assertNotIn('permit_token', events[0])
        self.assertEqual(pull.cleanup_day_journal(self.inventory, None), [])

    def test_client_refuses_independently_before_any_unlink(self):
        media = self.write(plaza(DAY, '103000'))
        cases = {
            'keep everything': dict(relative_dir='seoul/September/12-Saturday', stream_id=78),
            'folder map disagrees': dict(stream_id=78),
            'boundary day before': dict(relative_dir='shinjuku/September/15-Tuesday', local_date='2026-09-15'),
            'in window': dict(relative_dir='shinjuku/September/20-Sunday', local_date='2026-09-20'),
            'boundary day after': dict(relative_dir='shinjuku/October/06-Tuesday', local_date='2026-10-06'),
            'date not named by directory': dict(local_date='2026-09-19'),
            'timezone differs': dict(timezone='UTC'),
            'expiring permit': dict(permit_expires_at=(datetime.datetime.now(UTC) + datetime.timedelta(seconds=5)).isoformat()),
            'undated layout': dict(relative_dir='shinjuku/misc'),
        }
        for name, fields in cases.items():
            with self.subTest(name):
                self.sent.clear()
                self.next_permit = self.permit(**fields)
                pull.cleanup_day_cycle(self.cfg, self.inventory, threading.Event())
                [receipt] = self.receipts()
                self.assertEqual((receipt['outcome'], receipt['media_files']), ('refused', 0))
                self.assertTrue(receipt['refusal'])
                self.assertTrue(media.exists())

    def test_media_beyond_the_permit_refuses_the_whole_directory(self):
        for name, fields in {'files': dict(files=1, bytes=100), 'bytes': dict(files=2, bytes=5)}.items():
            with self.subTest(name):
                self.sent.clear()
                media = [self.write(plaza(DAY, '103000'), b'abc'), self.write(plaza(DAY, '113000'), b'defg')]
                self.next_permit = self.permit(**fields)
                pull.cleanup_day_cycle(self.cfg, self.inventory, threading.Event())
                [receipt] = self.receipts()
                self.assertEqual((receipt['outcome'], receipt['media_files']), ('refused', 0))
                self.assertIn('beyond the permitted', receipt['refusal'])
                self.assertTrue(all(m.exists() for m in media))

    def test_unparseable_media_refuses_the_whole_directory(self):
        media = self.write(plaza(DAY, '103000'))
        self.write('renamed.mp4')
        self.next_permit = self.permit()
        pull.cleanup_day_cycle(self.cfg, self.inventory, threading.Event())
        self.assertEqual(self.receipts()[0]['outcome'], 'refused')
        self.assertTrue(media.exists())

    def test_interrupted_pass_is_accounted_from_disk_on_replay(self):
        first, second = self.write(plaza(DAY, '103000'), b'abc'), self.write(plaza(DAY, '113000'), b'defg')
        self.next_permit = self.permit()
        unlink = os.unlink
        with mock.patch.object(pull, 'cleanup_day_finish', side_effect=RuntimeError('crash after intent')), \
                mock.patch.object(pull.os, 'fsync'), \
                mock.patch.object(pull.os, 'unlink', side_effect=lambda name, dir_fd: (_ for _ in ()).throw(RuntimeError('crash mid-pass'))
                                  if name == second.name else unlink(name, dir_fd=dir_fd)):
            with self.assertRaises(RuntimeError):
                pull.cleanup_day_cycle(self.cfg, self.inventory, threading.Event())
        self.assertEqual([phase for _, phase, _ in pull.cleanup_day_journal(self.inventory, None)], ['intent'])
        self.next_permit = None
        self.assertFalse(pull.cleanup_day_cycle(self.cfg, self.inventory, threading.Event()))
        [receipt] = self.receipts()
        self.assertEqual((receipt['outcome'], receipt['media_files'], receipt['media_bytes'], receipt['remaining_files']),
                         ('partial', 1, 3, 1))
        self.assertTrue(second.exists())
        self.assertEqual(pull.cleanup_day_journal(self.inventory, None), [])

    def test_day_release_is_on_the_cleanup_line(self):
        self.assertIn(pull.FAST_COPY_CLIENT_VERSION, pull.LOW_MEMORY_CLEANUP_VERSIONS)
        self.assertIn(pull.FAST_COPY_CLIENT_VERSION, pull.CLEANUP_BULK_VERSIONS)


if __name__ == '__main__':
    unittest.main()
