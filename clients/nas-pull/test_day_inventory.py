"""Read-only stream-day NAS inventory against a disposable fake NAS tree only."""
import datetime
import hashlib
import importlib.util
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
from zoneinfo import ZoneInfo

spec = importlib.util.spec_from_file_location('pull', Path(__file__).with_name('stoarama_pull.py'))
pull = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pull)

UTC = datetime.timezone.utc
DAY = datetime.date(2026, 9, 25)  # Friday
WARSAW = ZoneInfo('Europe/Warsaw')
WINDOW = (datetime.datetime(2026, 7, 19, 22, tzinfo=UTC), datetime.datetime(2026, 8, 8, 22, tzinfo=UTC), 'Europe/Warsaw')
PINS = dict(windows={17216: WINDOW, 3: WINDOW}, keep_everything=frozenset(),
            folders={'03_Brzeg': 17216, 'recordings/335': 17216, 'other_plaza': 3})


def local_epoch(hour, minute=0):
    return int(datetime.datetime(DAY.year, DAY.month, DAY.day, hour, minute, tzinfo=WARSAW).timestamp())


def unit(**overrides):
    out = dict(unit_id=7, attempt=1, recording_id=335, stream_id=17216, local_date=DAY.isoformat(),
               timezone='Europe/Warsaw', expected_dirs=[])
    out.update(overrides)
    return out


class DayInventoryTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name) / 'clips'
        self.files = {}

    def put(self, relative, body=None):
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        data = body if body is not None else relative.encode()
        path.write_bytes(data)
        self.files[relative] = data
        return relative

    def snapshot(self):
        out = {}
        for base, dirs, names in os.walk(self.root):
            for name in dirs + names:
                info = os.lstat(os.path.join(base, name))
                out[os.path.relpath(os.path.join(base, name), self.root)] = (info.st_mode, info.st_size, info.st_mtime_ns)
        return out

    def scan(self, u):
        return {f['relative_path']: f for f in pull.day_inventory_scan(self.root, pull.valid_day_inventory_unit(u), PINS)}

    def test_lists_every_known_layout_read_only(self):
        want = {
            # flat continuous: epoch seconds, kept to the local day
            self.put('recordings/335/continuous/%d.mp4' % local_epoch(9)): 'raw',
            self.put('recordings/335/continuous/%d.mp4' % local_epoch(0, 5)): 'raw',
            # flat job dir: epoch milliseconds
            self.put('recordings/335/4411/%d.mp4' % (local_epoch(14) * 1000)): 'raw',
            # plaza <Month>/<DD-Weekday>: every media file of the directory
            self.put('03_Brzeg/September/25-Friday/03_Brzeg_2026_September_W4_Friday_hour_080000.mp4'): 'raw',
            self.put('03_Brzeg/September/25-Friday/odd-name.mp4'): 'raw',
            # joined plaza mirror and the v1 joined layout
            self.put('joined/03_Brzeg/September/25-Friday/03_Brzeg_2026_September_W4_Friday_hour_08_080000-085959.mp4'): 'joined',
            self.put('joined/recordings/335/joined/2026-09-25/335_2026-09-25_hour_09_090000-095959.mp4'): 'joined',
            # a catalog directory in no known layout, named by the lease
            self.put('legacy/335-dump/a.mp4'): 'raw',
        }
        # Never listed: other days (flat and plaza), sidecars, hidden partials,
        # manifests, another stream's folder, symlinks, nested directories.
        self.put('recordings/335/continuous/%d.mp4' % (local_epoch(0) - 60))
        self.put('recordings/335/continuous/%d.mp4' % (local_epoch(23, 59) + 120))
        self.put('03_Brzeg/September/24-Thursday/03_Brzeg_2026_September_W4_Thursday_hour_080000.mp4')
        self.put('03_Brzeg/September/25-Friday/odd-name.mp4.stoarama.json')
        self.put('03_Brzeg/September/25-Friday/.partial.mp4')
        self.put('joined/03_Brzeg/September/25-Friday/x_hour_08.manifest.json')
        self.put('other_plaza/September/25-Friday/other.mp4')
        self.put('03_Brzeg/September/25-Friday/nested/deep.mp4')
        os.symlink(self.root / 'legacy/335-dump/a.mp4', self.root / '03_Brzeg/September/25-Friday/link.mp4')
        before = self.snapshot()

        found = self.scan(unit(expected_dirs=['legacy/335-dump', 'recordings/335/continuous']))

        self.assertEqual({p: f['kind'] for p, f in found.items()}, want)
        for path, report in found.items():
            data = self.files[path]
            info = os.stat(self.root / path)
            self.assertEqual(report, dict(relative_path=path, kind=want[path], size_bytes=len(data),
                                          mtime_ns=info.st_mtime_ns, sha256=hashlib.sha256(data).hexdigest()))
        self.assertEqual(self.snapshot(), before, 'inventory changed the NAS tree')

    def test_missing_directories_are_an_empty_inventory(self):
        self.root.mkdir()
        self.assertEqual(self.scan(unit()), {})

    def test_refuses_what_the_pins_do_not_own(self):
        self.root.mkdir()
        for bad in (unit(stream_id=99), unit(timezone='Asia/Tokyo')):
            with self.assertRaises((KeyError, ValueError)):
                self.scan(bad)
        for directory in ('joined/x', '../x', '/abs', '#recycle/x', 'a//b'):
            with self.assertRaises(ValueError):
                pull.valid_day_inventory_unit(unit(expected_dirs=[directory]))

    def test_symlinked_directories_refuse_the_unit(self):
        self.put('elsewhere/September/25-Friday/a.mp4')
        (self.root / '03_Brzeg').mkdir()
        # A symlinked component of a derived plaza directory, and a symlinked
        # catalog directory named by the lease.
        os.symlink(self.root / 'elsewhere/September', self.root / '03_Brzeg/September')
        with self.assertRaisesRegex(ValueError, 'symlink'):
            self.scan(unit())
        os.unlink(self.root / '03_Brzeg/September')
        os.symlink(self.root / 'elsewhere', self.root / 'legacy')
        with self.assertRaisesRegex(ValueError, 'symlink'):
            self.scan(unit(expected_dirs=['legacy/September/25-Friday']))
        self.assertEqual(self.snapshot()['legacy'][0] & 0o170000, 0o120000)

    def test_folder_map_escape_refuses_the_unit(self):
        self.root.mkdir()
        for folder in ('../outside', 'joined/x', '/abs', 'a/../b'):
            pins = dict(PINS, folders={folder: 17216})
            with self.assertRaises(ValueError):
                pull.day_inventory_scan(self.root, pull.valid_day_inventory_unit(unit()), pins)

    def test_a_non_regular_file_after_open_fails_the_scan(self):
        self.put('03_Brzeg/September/25-Friday/a.mp4')
        directory = os.stat(self.root)
        with mock.patch.object(pull.os, 'fstat', return_value=directory):
            with self.assertRaisesRegex(ValueError, 'not a regular file'):
                self.scan(unit())

    def test_a_file_changing_while_read_fails_the_scan(self):
        path = self.put('03_Brzeg/September/25-Friday/grow.mp4')
        real, grown = os.read, []

        def growing(fd, n):
            chunk = real(fd, n)
            if chunk and not grown:
                grown.append(True)
                with open(self.root / path, 'ab') as f:
                    f.write(b'more')
            return chunk
        with mock.patch.object(pull.os, 'read', side_effect=growing):
            with self.assertRaises(pull.FileChangedDuringHash):
                self.scan(unit())

    def test_once_reports_the_scan_or_why_not(self):
        self.put('03_Brzeg/September/25-Friday/a.mp4')
        cfg = SimpleNamespace(output_dir=self.root)
        sent = []

        def api(_cfg, path, body, retry_stale=False):
            sent.append((path, body))
            if path.endswith('/lease'):
                return {'retry_after_sec': 0, 'unit': self.lease}
            return {'state': 'completed'}
        with mock.patch.object(pull, 'restore_api', side_effect=api), \
                mock.patch.object(pull, 'cleanup_pins', return_value=(PINS, None)), \
                mock.patch.object(pull, 'CLIENT_VERSION', pull.FAST_COPY_CLIENT_VERSION):
            self.lease = unit()
            self.assertEqual(pull.day_inventory_once(cfg), 0)
            self.lease = unit(unit_id=8, stream_id=99)
            pull.day_inventory_once(cfg)
        (lease_path, lease_body), (ok_path, ok), _, (bad_path, bad) = sent
        self.assertEqual((lease_path, lease_body), (pull.DAY_INVENTORY_LEASE_PATH, {'client_version': 'r2-copy-v8-inventory'}))
        self.assertEqual(ok_path, '/account/connections/day-inventory/7/result')
        self.assertEqual(([f['relative_path'] for f in ok['files']], ok['error'], ok['attempt']),
                         (['03_Brzeg/September/25-Friday/a.mp4'], '', 1))
        self.assertEqual((bad_path, bad['files']), ('/account/connections/day-inventory/8/result', []))
        self.assertIn('KeyError', bad['error'])

    def test_inventory_is_the_current_release(self):
        self.assertEqual(pull.FAST_COPY_CLIENT_VERSION, 'r2-copy-v8-inventory')


if __name__ == '__main__':
    unittest.main()
