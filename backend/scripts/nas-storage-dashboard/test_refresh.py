import datetime as dt
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('refresh', Path(__file__).with_name('refresh.py'))
m = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(m)


class CoverageTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.state = Path(self.tmp.name)
        self.db = m.cache(self.state)
        self.snapshot = {'streams': [{'recording_id': 1, 'stream_id': '12', 'name': 'Test',
            'timezone': 'UTC', 'folder': 'test'}]}

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def fact(self, name, kind, size, ranges, present=1):
        m.save_facts(self.db, [(name, 1, kind, size, json.dumps(ranges), 'managed/acct-47/'+name,
                               present, '2026-10-18T12:00:00+00:00', 'source:'+name)])

    def assemble(self):
        with patch.object(m, 'retention', return_value=m.defaultdict(set)):
            return m.assemble(self.db, self.snapshot)

    def test_renamed_inventory_retires_old_path(self):
        self.fact('old.mp4', 'clips', 100, [[0, 60]])
        row = list(self.db.execute('SELECT * FROM facts').fetchone())
        row[0] = 'new.mp4'
        row[7] = '2026-10-19T12:00:00+00:00'
        m.save_facts(self.db, [row])
        self.assertEqual(self.db.execute('SELECT path FROM facts').fetchall(), [('new.mp4',)])
        self.assertEqual(self.assemble()[1]['mapped_bytes'], 100)
        row[0] = 'stale.mp4'
        row[7] = '2026-10-18T12:00:00+00:00'
        m.save_facts(self.db, [row])
        self.assertEqual(self.db.execute('SELECT path FROM facts').fetchall(), [('new.mp4',)])

    def test_r2_headroom_pause_retains_cursor_without_network(self):
        self.fact('a', 'clips', 100, [[0, 60]])
        with patch.object(m.shutil, 'disk_usage', return_value=m.shutil._ntuple_diskusage(10, 4, 6*1024**3)), patch.object(m, 'r2_list') as request:
            with self.assertRaises(m.LocalHeadroom):
                m.refresh_r2(self.db, state=self.state)
            request.assert_not_called()
        self.assertEqual(self.db.execute('SELECT cursor,complete_at FROM prefixes').fetchone(), (None, None))

    def test_owner_cleanup_only_retires_exact_obsolete_cache(self):
        target = self.state / 'metadata.sqlite3'
        target.write_bytes(b'generated-preview-index')
        with self.assertRaises(RuntimeError):
            m.retire_obsolete_cache(self.state, '0'*64)
        self.assertTrue(target.exists())
        digest = m.hashlib.sha256(target.read_bytes()).hexdigest()
        companion = self.state / 'metadata.sqlite3-journal'
        companion.symlink_to(self.state / 'missing')
        with self.assertRaises(RuntimeError):
            m.retire_obsolete_cache(self.state, digest)
        self.assertTrue(target.exists())
        companion.unlink()
        m.retire_obsolete_cache(self.state, digest)
        self.assertFalse(target.exists())
        target.symlink_to(self.state / 'metadata-v2.sqlite3')
        with self.assertRaises(OSError):
            m.retire_obsolete_cache(self.state, digest)
        self.assertTrue(target.is_symlink())

    def test_unknown_database_is_preserved(self):
        self.db.close()
        target = self.state / 'metadata-v2.sqlite3'
        other = m.sqlite3.connect(str(target))
        other.execute('PRAGMA application_id=123')
        other.close()
        before = target.read_bytes()
        with self.assertRaises(RuntimeError):
            m.cache(self.state)
        self.assertEqual(before, target.read_bytes())
        self.db = m.sqlite3.connect(':memory:')

    def test_overlaps_count_once_but_separate_files_keep_their_bytes(self):
        self.fact('a', 'clips', 100, [[0, 60]])
        self.fact('b', 'clips', 200, [[30, 90]])
        self.fact('c', 'stitched', 250, [[0, 30], [60, 120]])
        streams, totals, fresh = self.assemble()
        day = streams['12']['days'][0]
        self.assertEqual((day['clips_seconds'], day['stitched_seconds'], day['combined_seconds']), (90, 90, 120))
        self.assertEqual(day['overlap_seconds'], 60)
        self.assertEqual((day['clips_files'], day['clips_bytes'], day['stitched_bytes']), (2, 300, 250))
        self.assertEqual(totals['mapped_bytes'], 550)
        self.assertFalse(fresh)

    def test_cross_midnight_splits_time_without_duplicating_size(self):
        self.fact('a', 'clips', 100, [[86370, 86430]])
        days = self.assemble()[0]['12']['days']
        self.assertEqual([d['clips_seconds'] for d in days], [30, 30])
        self.assertEqual([d['clips_bytes'] for d in days], [100, 0])

    def test_dst_calendar_days_have_correct_lengths(self):
        for day, expected in [('2026-03-08', 23*3600), ('2026-11-01', 25*3600)]:
            start = dt.datetime.fromisoformat(day).replace(tzinfo=m.ZoneInfo('America/New_York'))
            end = start + dt.timedelta(days=1)
            parts = list(m.split_days(start.timestamp(), end.timestamp(), 'America/New_York'))
            self.assertEqual(parts, [(day, 0, expected, expected)])

    def test_fresh_report_excludes_mismatch_and_separates_recycle(self):
        self.fact('a', 'clips', 100, [[0, 60]])
        self.fact('b', 'stitched', 200, [[0, 120]])
        self.db.executemany('INSERT INTO observed VALUES (?,?,?,?,?,?,?,?)', [
            ('a', 'unstitched_media', 100, 'test', '', '1', '2', '2026-10-19T00:00:00Z'),
            ('b', 'stitched_media', 999, 'test', '', '1', '3', '2026-10-19T00:00:00Z'),
            ('#recycle/a', 'unstitched_media', 100, '#recycle', '', '1', '4', '2026-10-19T00:00:00Z'),
            ('unknown', 'unstitched_media', 80, 'test', '', '1', '5', '2026-10-19T00:00:00Z')])
        m.set_meta(self.db, 'report_id', 'test-audit')
        streams, totals, fresh = self.assemble()
        self.assertTrue(fresh)
        self.assertEqual(totals['unmapped_observed_files'], 2)
        self.assertEqual(streams['12']['clips_bytes'], 100)
        self.assertEqual(streams['12']['stitched_bytes'], 0)
        self.assertEqual({d['area'] for d in streams['12']['days']}, {'active', 'recycle'})

    def test_unfinished_r2_listing_never_implies_missing(self):
        self.fact('a', 'clips', 100, [[0, 60]])
        self.db.execute("INSERT INTO prefixes VALUES ('managed/acct-47/',NULL,NULL,NULL)")
        day = self.assemble()[0]['12']['days'][0]
        self.assertEqual((day['r2_unchecked_files'], day['r2_not_found_files']), (1, 0))
        self.db.execute("UPDATE prefixes SET complete_at='2026-10-19T00:00:00+00:00',started_at='2026-10-19T00:00:00+00:00'")
        day = self.assemble()[0]['12']['days'][0]
        self.assertEqual((day['r2_unchecked_files'], day['r2_not_found_files']), (0, 1))

    def test_r2_crawl_start_fences_absence_for_files_observed_during_crawl(self):
        self.fact('a', 'clips', 100, [[0, 60]])
        self.db.execute("INSERT INTO prefixes VALUES ('managed/acct-47/',NULL,'2026-10-19T00:00:00Z','2026-10-18T11:00:00Z')")
        day = self.assemble()[0]['12']['days'][0]
        self.assertEqual((day['r2_unchecked_files'], day['r2_not_found_files']), (1, 0))

    def test_undated_is_unknown_instead_of_an_assumed_minute(self):
        self.fact('a', 'clips', 100, [])
        streams, totals, _ = self.assemble()
        self.assertEqual(streams['12']['days'], [])
        self.assertEqual(totals['undated_bytes'], 100)

    def test_unsafe_stream_identifier_is_rejected(self):
        self.snapshot['streams'][0]['stream_id'] = '../../other'
        with self.assertRaises(RuntimeError):
            self.assemble()

    def test_postgres_short_timezone_offsets(self):
        self.assertEqual(m.parse_time('2026-07-23 06:00:00+00').utcoffset(), dt.timedelta(0))

    def test_failed_output_validation_preserves_existing_unknown_file(self):
        target = self.state / 'data.json'
        target.write_text('{"owner":"someone else"}')
        before = target.read_bytes()
        with self.assertRaises(RuntimeError):
            m.write_json(target, {'test': 1})
        self.assertEqual(target.read_bytes(), before)

    def test_atomic_publication_and_symlink_refusal(self):
        target = self.state / 'data.json'
        m.write_json(target, {'value': 1})
        m.write_json(target, {'value': 2})
        self.assertEqual(json.loads(target.read_text())['value'], 2)
        link = self.state / 'link.json'
        link.symlink_to(target)
        with self.assertRaises(RuntimeError):
            m.write_json(link, {'value': 3})
        self.assertEqual(json.loads(target.read_text())['value'], 2)


if __name__ == '__main__':
    unittest.main()
