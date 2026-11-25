"""Bulk NAS-only day-directory cleanup against disposable fixtures only."""
import datetime
import importlib.util
import json
import os
import tempfile
import threading
import time
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
FLAT = 'recordings/366/continuous'
TOKYO = ZoneInfo('Asia/Tokyo')


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
                      relative_dir=DIR, local_date=DAY.isoformat(), stream_id=3, timezone='Asia/Tokyo', files=2, bytes=7,
                      mode='dir')
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
        kept = [self.write('.' + plaza(DAY, '133000') + '.partial')]
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

    def test_day_unit_completes_while_an_app_writer_is_active(self):
        media = self.write(plaza(DAY, '103000'), b'abc')
        self.next_permit = self.permit(files=1)
        pull.NAS_MEDIA_FENCE.acquire_delivery()  # A long collated download, never drained.
        self.addCleanup(pull.NAS_MEDIA_FENCE.release_delivery)
        with mock.patch.object(pull, 'CLEANUP_EXECUTION_DRAIN_SEC', 0), mock.patch.object(pull, 'log') as logged:
            self.assertTrue(pull.cleanup_day_cycle(self.cfg, self.inventory, threading.Event()))
        timings = self.ledger()[-1]['timings']
        self.assertEqual(set(timings), {'replay_ms', 'permit_ms', 'pins_ms', 'select_ms', 'intent_ledger_ms', 'intent_journal_ms', 'unlink_ms',
                                        'unlink_files', 'unlink_mean_ms', 'dir_fsync_ms', 'account_ms', 'receipt_journal_ms'})
        self.assertEqual(timings['unlink_files'], 1)
        [line] = [c.args[1] for c in logged.call_args_list if c.args[1].startswith('NAS day unit 7')]
        self.assertIn('receipt_post_ms=', line)
        self.assertFalse(media.exists())
        self.assertEqual([r['outcome'] for r in self.receipts()], ['deleted'])
        fence = pull.NAS_MEDIA_FENCE
        self.assertEqual((fence.writers, fence.cleaners, fence.execution_owner), (1, 0, None))
        self.assertFalse(fence.execution_lock.locked())

    def test_client_refuses_independently_before_any_unlink(self):
        media = self.write(plaza(DAY, '103000'))
        cases = {
            'keep everything in dir mode': dict(relative_dir='seoul/September/12-Saturday', stream_id=78),
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

    def test_dir_mode_deletes_uncatalogued_media_of_the_day(self):
        # Stream 17241's day dirs held 516 files against 145 inventoried: the
        # whole day is outside the keep window, so the extras go too.
        media = [self.write(plaza(DAY, '%02d' % hour), b'abc') for hour in range(24)]
        self.next_permit = self.permit(files=1, bytes=3)
        pull.cleanup_day_cycle(self.cfg, self.inventory, threading.Event())
        [receipt] = self.receipts()
        self.assertEqual((receipt['outcome'], receipt['media_files'], receipt['media_bytes']), ('deleted', 24, 72))
        self.assertFalse(any(m.exists() for m in media))

    def test_dir_mode_refuses_a_foreign_entry(self):
        cases = {
            'foreign file': lambda: self.write('notes.txt'),
            'another date': lambda: self.write(plaza(datetime.date(2020, 9, 12), '103000')),
            'another layout': lambda: self.write('1789227000.mp4'),
            'directory': lambda: (self.dir / 'sub').mkdir(),
            'symlink': lambda: (self.dir / plaza(DAY, '113000')).symlink_to(self.dir / plaza(DAY, '103000')),
        }
        for name, make in cases.items():
            with self.subTest(name):
                self.sent.clear()
                media = self.write(plaza(DAY, '103000'))
                make()
                self.next_permit = self.permit()
                pull.cleanup_day_cycle(self.cfg, self.inventory, threading.Event())
                [receipt] = self.receipts()
                self.assertEqual((receipt['outcome'], receipt['media_files']), ('refused', 0))
                self.assertTrue(media.exists())
                for path in list(self.dir.iterdir()):
                    path.rmdir() if path.is_dir() and not path.is_symlink() else path.unlink()

    def flat(self, at):
        return '%d.mp4' % at.timestamp()

    def names_permit(self, entries, **fields):
        return self.permit(**dict(dict(relative_dir=FLAT, mode='names', files=len(entries), bytes=1,
                                       entries=[dict(name=n, size_bytes=z, sidecar=n.endswith('.stoarama.json')) for n, z in entries]), **fields))

    def test_names_mode_unlinks_only_listed_entries_quickly(self):
        flat = self.cfg.output_dir / FLAT
        flat.mkdir(parents=True)
        start = datetime.datetime(2026, 9, 1, tzinfo=TOKYO)
        names = [self.flat(start + datetime.timedelta(minutes=2 * n)) for n in range(14000)]  # ~19.4 days
        for name in names:
            (flat / name).write_bytes(b'abc')
            (flat / (name + '.stoarama.json')).write_bytes(b'{}')
        day = [n for n in names if datetime.datetime.fromtimestamp(int(n[:-4]), TOKYO).date() == DAY]
        self.assertEqual(len(day), 720)
        entries = [(n, 3) for n in day[1:]] + [(n + '.stoarama.json', 2) for n in day[1:]] + [(self.flat(datetime.datetime(2026, 9, 12, 23, 59, 1, tzinfo=TOKYO)), 3)]
        entries[0] = (entries[0][0], 4)  # Inventory size differs: that file stays.
        self.next_permit = self.names_permit(entries, local_date=DAY.isoformat())
        started = time.monotonic()
        with mock.patch.object(pull.os, 'stat', wraps=os.stat) as stat:
            self.assertTrue(pull.cleanup_day_cycle(self.cfg, self.inventory, threading.Event()))
        self.assertLess(time.monotonic() - started, 5)
        self.assertLessEqual(stat.call_count, 2 * len(entries))  # Listed names only (select + account), never all 28k.
        [receipt] = self.receipts()
        # One listed name is already gone (counted); one has another size (kept).
        self.assertEqual((receipt['outcome'], receipt['media_files'], receipt['sidecar_files'], receipt['remaining_files']),
                         ('deleted', 719, 719, 0))
        self.assertTrue((flat / day[0]).exists() and (flat / (day[0] + '.stoarama.json')).exists())  # Unlisted.
        self.assertTrue((flat / day[1]).exists())  # Size mismatch.
        self.assertFalse(any((flat / n).exists() for n in day[2:]))
        self.assertEqual(len(os.listdir(flat)), 2 * 14000 - 718 - 719)

    def test_names_mode_refuses_a_listed_name_of_another_date(self):
        flat = self.cfg.output_dir / FLAT
        flat.mkdir(parents=True)
        other = self.flat(datetime.datetime(2026, 9, 11, 12, tzinfo=TOKYO))
        (flat / other).write_bytes(b'abc')
        self.next_permit = self.names_permit([(other, 3)], local_date=DAY.isoformat())
        pull.cleanup_day_cycle(self.cfg, self.inventory, threading.Event())
        self.assertEqual(self.receipts()[0]['outcome'], 'refused')
        self.assertTrue((flat / other).exists())

    def test_invalid_permit_scope_is_rejected(self):
        name = self.flat(datetime.datetime(2026, 9, 12, 12, tzinfo=TOKYO))
        for bad in ('../' + name, 'sub/' + name, '.' + name, 'notes.txt', name + '.stoarama.json.bak'):
            with self.subTest(bad):
                self.next_permit = self.names_permit([(bad, 3)])
                with self.assertRaises(pull.CleanupResponseError):
                    pull.cleanup_day_cycle(self.cfg, self.inventory, threading.Event())
        for fields in (dict(mode='dir', entries=[]), dict(mode='names'), dict(mode=None)):
            with self.subTest(fields):
                self.next_permit = self.permit(**fields)
                with self.assertRaises(pull.CleanupResponseError):
                    pull.cleanup_day_cycle(self.cfg, self.inventory, threading.Event())
        self.next_permit = self.names_permit([(name, 3)], relative_dir=DIR)  # names mode on a plaza dir
        pull.cleanup_day_cycle(self.cfg, self.inventory, threading.Event())
        self.assertIn('dir mode', self.receipts()[-1]['refusal'])

    def test_keep_everything_day_leaves_only_listed_names(self):
        # Deniz: "nas shoul dnot have any of the keep everything (except the keep 20)".
        seoul = self.cfg.output_dir / 'seoul/September/12-Saturday'
        seoul.mkdir(parents=True)
        listed, unlisted = plaza(DAY, '103000'), plaza(DAY, '113000')
        for name in (listed, unlisted):
            (seoul / name).write_bytes(b'abc')
        (seoul / (listed + '.stoarama.json')).write_bytes(b'{}')
        ke = dict(relative_dir='seoul/September/12-Saturday', stream_id=78)
        self.next_permit = self.names_permit([(listed, 3), (listed + '.stoarama.json', 2)], **ke)
        self.assertTrue(pull.cleanup_day_cycle(self.cfg, self.inventory, threading.Event()))
        [receipt] = self.receipts()
        self.assertEqual((receipt['outcome'], receipt['media_files'], receipt['sidecar_files']), ('deleted', 1, 1))
        self.assertEqual(sorted(os.listdir(seoul)), [unlisted])  # No R2 counterpart listed: it stays.
        for day in (datetime.date(2026, 9, 15), datetime.date(2026, 9, 20), datetime.date(2026, 10, 6)):
            with self.subTest(day=day):
                self.sent.clear()
                directory = 'seoul/%s/%02d-%s' % (pull.CLEANUP_MONTHS[day.month - 1], day.day, pull.CLEANUP_DAY_WEEKDAYS[day.weekday()])
                self.next_permit = self.names_permit([(plaza(day, '103000'), 3)], relative_dir=directory, stream_id=78,
                                                     local_date=day.isoformat())
                pull.cleanup_day_cycle(self.cfg, self.inventory, threading.Event())
                self.assertIn('inside or adjacent', self.receipts()[0]['refusal'])

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
        self.assertEqual(pull.FAST_COPY_CLIENT_VERSION, 'r2-copy-v8-output-restore')


if __name__ == '__main__':
    unittest.main()
