# /usr/bin/python3 -I ops/jelly/r2prune/test_cycle.py -q
# The lane preflight: chunked, one snapshot, fail closed. With STOARAMA_TEST_DATABASE_URL it
# also runs preflight.sql on a stub catalog and checks the chunked vetoes equal the old
# single JSON-literal statement's, and that RECEIPTS reads live clips through migration
# 0273's index with an unchanged result. Nothing reaches production.
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
import unittest.mock
import urllib.parse

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('cycle', HERE / 'cycle.py')
cycle = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cycle)

NEW_CTE = "  SELECT * FROM unnest(:'kinds'::text[], :'ids'::bigint[]) AS j(kind, source_id)\n"
OLD_CTE = "  SELECT * FROM jsonb_to_recordset('__CANDIDATES__'::jsonb) AS j(kind text, source_id bigint)\n"


def old_preflight_sql(candidates):
    """The installed lane's statement before chunking: every candidate in one JSON literal."""
    query = (HERE / 'preflight.sql').read_text()
    assert query.count(NEW_CTE) == 1
    literal = json.dumps([{'kind': k, 'source_id': s} for k, s in candidates], separators=(',', ':'))
    return query.replace(NEW_CTE, OLD_CTE).replace('__CANDIDATES__', literal)


class Lane(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)

    def use_db(self, url):
        env = self.dir / 'db.env'
        env.write_text(f"DATABASE_URL='{url}'\n")
        for name, value in (('DBENV', str(env)), ('PSQL', self.psql)):
            patch = unittest.mock.patch.object(cycle, name, value)
            patch.start()
            self.addCleanup(patch.stop)


class Script(Lane):
    def test_chunks_share_one_read_only_snapshot(self):
        candidates = [('raw' if i % 3 else 'collated', 10**12 + i) for i in range(12001)]
        lines = cycle.preflight_script(candidates).splitlines()
        self.assertEqual(lines[0], 'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;')
        self.assertEqual(lines[-1], 'ROLLBACK;')
        kinds = [l.split("'")[1].strip('{}').split(',') for l in lines if l.startswith(r'\set kinds ')]
        ids = [l.split("'")[1].strip('{}').split(',') for l in lines if l.startswith(r'\set ids ')]
        self.assertEqual([len(c) for c in ids], [5000, 5000, 2001])
        self.assertEqual([len(c) for c in kinds], [5000, 5000, 2001])
        self.assertEqual(sum(l.startswith(r'\i ') for l in lines), 3)
        self.assertEqual([(k, int(s)) for ck, cs in zip(kinds, ids) for k, s in zip(ck, cs)], candidates)
        self.assertLess(max(map(len, lines)), 100_000)

    def test_stop_file_skips_the_cycle(self):
        L = str(self.dir)
        Path(L, 'STOP').write_text('')
        with unittest.mock.patch.object(cycle, 'psql', side_effect=AssertionError('read')), \
             unittest.mock.patch.object(cycle, 'run', side_effect=AssertionError('run')), \
             unittest.mock.patch.object(cycle, 'finish_receipts', side_effect=AssertionError('replay')), \
             unittest.mock.patch('sys.stdout'):
            cycle.main(L)
        rows = [json.loads(l) for l in Path(L, 'runs.jsonl').read_text().splitlines()]
        self.assertEqual([r['skipped'] for r in rows], ['STOP'])
        self.assertRegex(rows[0]['cycle'], r'^\d{8}T\d{6}Z$')
        self.assertFalse(Path(L, 'cycle.lock').exists())
        self.assertFalse(Path(L, 'cycles').exists())

    def test_receipts_read_is_memory_capped(self):
        sets = [l for l in cycle.RECEIPTS.splitlines() if l.startswith('SET ')]
        self.assertIn("SET LOCAL work_mem='32MB';", sets)
        self.assertIn('SET LOCAL max_parallel_workers_per_gather=0;', sets)
        for forced in ('enable_', 'parallel_setup_cost', 'parallel_tuple_cost'):
            self.assertFalse([l for l in sets if forced in l], forced)

    def test_refuses_unknown_candidates(self):
        for bad in [('page', 1), ('raw', '1'), ('raw', True), ("raw}'", 1)]:
            with self.assertRaises(ValueError):
                cycle.preflight_script([('raw', 2), bad])

    def test_failed_chunk_fails_closed(self):
        # A stub psql that prints a veto, then fails: the cycle must not plan.
        self.psql = str(self.dir / 'psql')
        Path(self.psql).write_text('#!/bin/sh\ncat >/dev/null\necho "raw|1"\necho "ERROR:  canceling statement" >&2\nexit 3\n')
        os.chmod(self.psql, 0o755)
        self.use_db('postgres://u:secret@db.invalid:5432/x')
        sources = {'k': {'kind': 'raw', 'source_id': 1}}
        with self.assertRaisesRegex(RuntimeError, 'psql read failed') as caught:
            cycle.preflight(str(self.dir), ['k'], sources)
        self.assertNotIn('secret', str(caught.exception))

    def test_no_candidates_reads_nothing(self):
        self.psql = '/nonexistent/psql'
        self.use_db('postgres://u@db.invalid:5432/x')
        self.assertEqual(cycle.preflight(str(self.dir), [], {}), set())


# Stub catalog: the relations and functions preflight.sql reads, with rows hitting each veto.
SCHEMA = """
CREATE TABLE recording_clips(id bigint PRIMARY KEY, bucket text, purged_at timestamptz,
  recording_id bigint, recording_job_id bigint, object_key text);
CREATE TABLE recording_collation_outputs(id bigint PRIMARY KEY, r2_verified_at timestamptz, object_key text);
CREATE TABLE held_clips(id bigint);
CREATE FUNCTION nas_cleanup_clip_held(bigint) RETURNS boolean LANGUAGE sql STABLE
  AS 'SELECT EXISTS(SELECT FROM held_clips WHERE id=$1)';
CREATE TABLE reserved_files(kind text, source_id bigint);
CREATE FUNCTION nas_cleanup_r2_file_reserved(text, bigint) RETURNS boolean LANGUAGE sql STABLE
  AS 'SELECT EXISTS(SELECT FROM reserved_files WHERE kind=$1 AND source_id=$2)';
CREATE TABLE recording_joined_batches(id bigint, state text);
CREATE TABLE recording_joined_snapshot_scopes(batch_record_id bigint, recording_id bigint,
  recording_job_id bigint, high_water_clip_id bigint);
CREATE TABLE recording_joined_source_snapshots(batch_record_id bigint, clip_id bigint);
CREATE TABLE r2_retirement_reservations(bucket text, object_key text);
CREATE TABLE r2_object_retirements(bucket text, object_key text, kind text, source_id bigint);

INSERT INTO recording_clips SELECT i, CASE WHEN i % 17 = 0 THEN 'other' ELSE 'stoarama' END,
  CASE WHEN i % 11 = 0 THEN now() END, i % 5, i % 3, 'managed/' || i FROM generate_series(1, 400) i;
INSERT INTO recording_collation_outputs SELECT i, CASE WHEN i % 13 <> 0 THEN now() END,
  'joined/' || CASE WHEN i % 29 = 0 THEN 'dup' ELSE i::text END FROM generate_series(1, 400) i;
INSERT INTO held_clips SELECT i FROM generate_series(19, 400, 19) i;
INSERT INTO reserved_files SELECT k, i FROM generate_series(23, 400, 23) i, (VALUES ('raw'), ('collated')) v(k);
INSERT INTO recording_joined_batches VALUES (1, 'building'), (2, 'done'), (3, 'snapshotting');
INSERT INTO recording_joined_snapshot_scopes VALUES (1, 2, 1, 120), (2, 3, 2, 400);
INSERT INTO recording_joined_source_snapshots VALUES (3, 31), (3, 62), (2, 93);
INSERT INTO r2_retirement_reservations SELECT 'stoarama', 'managed/' || i FROM generate_series(37, 400, 37) i;
INSERT INTO r2_retirement_reservations VALUES ('stoarama', 'joined/41');
INSERT INTO r2_object_retirements SELECT 'stoarama', 'joined/' || i, 'collated', NULL FROM generate_series(43, 400, 43) i;
INSERT INTO r2_object_retirements SELECT 'stoarama', 'elsewhere', 'raw', i FROM generate_series(47, 400, 47) i;
"""


needs_db = unittest.skipUnless(os.environ.get('STOARAMA_TEST_DATABASE_URL'), 'needs STOARAMA_TEST_DATABASE_URL')


class PostgresLane(Lane):
    def setUp(self):
        super().setUp()
        self.psql = shutil.which('psql')
        if self.psql is None:
            self.fail('psql not found on PATH, but a test database was provided')
        admin = os.environ['STOARAMA_TEST_DATABASE_URL']
        name = f'r2prune_preflight_{os.getpid()}'
        self.admin = lambda sql: subprocess.run([self.psql, '-XqAt', '-v', 'ON_ERROR_STOP=1', admin, '-c', sql],
                                                check=True, capture_output=True)
        self.admin(f'DROP DATABASE IF EXISTS {name}')
        self.admin(f'CREATE DATABASE {name}')
        self.addCleanup(self.admin, f'DROP DATABASE {name} WITH (FORCE)')
        self.url = urllib.parse.urlunsplit(urllib.parse.urlsplit(admin)._replace(path='/' + name))
        self.sql(self.schema)
        self.use_db(self.url)

    def sql(self, script):
        return subprocess.run([self.psql, '-XqAt', '-v', 'ON_ERROR_STOP=1', self.url], input=script,
                              text=True, check=True, capture_output=True).stdout


@needs_db
class AgainstPostgres(PostgresLane):
    schema = SCHEMA

    def test_chunked_vetoes_equal_one_json_statement(self):
        candidates = [(k, i) for i in range(1, 431) for k in cycle.KINDS]  # 401..430: no catalog row
        sources = {f'{k}/{i}': {'kind': k, 'source_id': i} for k, i in candidates}
        old = subprocess.run([self.psql, '-XqAt', '-v', 'ON_ERROR_STOP=1', self.url],
                             input=old_preflight_sql(candidates) + ';\n', text=True, check=True, capture_output=True)
        expected = {(k, int(s)) for k, s in (l.split('|') for l in old.stdout.splitlines())}
        with unittest.mock.patch.object(cycle, 'ROWS_PER_STATEMENT', 37):
            got = cycle.preflight(str(self.dir), list(sources), sources)
        self.assertEqual(got, expected)
        # Every veto branch fires, and most candidates pass.
        for veto in [('raw', 17), ('raw', 11), ('raw', 19), ('raw', 23), ('raw', 7), ('raw', 31), ('raw', 37),
                     ('raw', 47), ('raw', 401), ('collated', 13), ('collated', 29), ('collated', 23),
                     ('collated', 41), ('collated', 43), ('collated', 401)]:
            self.assertIn(veto, got)
        for passes in [('raw', 1), ('raw', 93), ('raw', 200), ('collated', 1), ('collated', 5)]:
            self.assertNotIn(passes, got)
        self.assertLess(len(got), len(candidates) * 2 // 3)

    def test_chunks_read_one_snapshot(self):
        # A transaction that commits a veto between chunks must not change the result.
        sources = {f'raw/{i}': {'kind': 'raw', 'source_id': i} for i in range(1, 3)}
        # Between the chunks, another session marks clip 2 purged.
        commit = (r"\! PGOPTIONS= %s -XqAt -v ON_ERROR_STOP=1 '%s' -c 'UPDATE recording_clips SET purged_at=now() WHERE id=2'"
                  % (self.psql, self.url))
        with unittest.mock.patch.object(cycle, 'ROWS_PER_STATEMENT', 1):
            lines = cycle.preflight_script([('raw', 1), ('raw', 2)]).splitlines()
        first_include = next(i for i, l in enumerate(lines) if l.startswith(r'\i '))
        lines.insert(first_include + 1, commit)
        with unittest.mock.patch.object(cycle, 'preflight_script', lambda c: '\n'.join(lines) + '\n'):
            self.assertEqual(cycle.preflight(str(self.dir), list(sources), sources), set())
        with unittest.mock.patch.object(cycle, 'ROWS_PER_STATEMENT', 1):
            self.assertEqual(cycle.preflight(str(self.dir), list(sources), sources), {('raw', 2)})


# Stub catalog for RECEIPTS: the production clip index, and windows over recordings whose
# history is mostly purged, as on production (2026-11-27: 1,002,151 clip rows read, 350,031 kept).
RECEIPTS_SCHEMA = """
CREATE TABLE nas_cleanup_plans(id bigint PRIMARY KEY, manifest jsonb);
CREATE TABLE nas_cleanup_items(plan_id bigint, ordinal int, item jsonb, delete_ever_issued boolean);
CREATE TABLE nas_media_keep20_windows(stream_id bigint, start_at timestamptz, end_at timestamptz, keep_everything boolean);
CREATE TABLE recordings(id bigint PRIMARY KEY, stream_id bigint, account_id bigint);
CREATE INDEX ON recordings(stream_id);
CREATE TABLE recording_clips(id bigint PRIMARY KEY, recording_id bigint, bucket text, object_key text,
  display_path text, size_bytes bigint, sha256 text, etag text, purged_at timestamptz,
  clip_start_at timestamptz, clip_end_at timestamptz, pad text);
CREATE INDEX idx_recording_clips_recording_started ON recording_clips(recording_id, clip_start_at DESC);
CREATE TABLE nas_inventory_files(connection_id bigint, clip_id bigint, state text, relative_path text,
  size_bytes bigint, sha256 text, verified_at timestamptz, server_received_at timestamptz, file_mtime_ns bigint,
  PRIMARY KEY (connection_id, clip_id));
CREATE INDEX ON nas_inventory_files(clip_id);
CREATE TABLE recording_collation_hours(id bigint PRIMARY KEY, recording_id bigint);
CREATE TABLE recording_collation_outputs(id bigint PRIMARY KEY, collation_hour_id bigint, object_key text,
  nas_relative_path text, size_bytes bigint, sha256 text, start_at timestamptz, end_at timestamptz);
CREATE TABLE recording_collation_output_acks(output_id bigint, connection_id bigint, nas_relative_path text,
  size_bytes bigint, sha256 text, verified_at timestamptz);

INSERT INTO recordings SELECT i, 100 + i, CASE WHEN i = 6 THEN 48 ELSE 47 END FROM generate_series(1, 6) i;
INSERT INTO nas_media_keep20_windows SELECT 100 + i, timestamptz '2026-09-01' + i * interval '1 day',
  timestamptz '2026-09-21' + i * interval '1 day', i = 5 FROM generate_series(1, 6) i;
-- 120 days of five-minute clips per recording: purged before the window, two in three
-- purged inside it, live after it; a few in another bucket.
INSERT INTO recording_clips SELECT r * 100000 + n, r, CASE WHEN n % 97 = 0 THEN 'other' ELSE 'stoarama' END,
  'managed/' || r || '/' || n, 'raw/' || r || '/' || n || '.mp4', 1000 + n, md5(n::text) || md5(r::text), 'e' || n,
  CASE WHEN at < timestamptz '2026-09-01' + r * interval '1 day' OR (at < timestamptz '2026-09-21' + r * interval '1 day'
    AND n % 3 <> 0) THEN timestamptz '2026-11-20' END,
  at, at + interval '5 minutes', repeat('x', 300)
  FROM generate_series(1, 6) r, generate_series(0, 34559) n,
  LATERAL (SELECT timestamptz '2026-08-25' + n * interval '5 minutes' + interval '1 second' AS at) t;
-- NAS evidence: missing, moved, resized, unhashed and wrong-hash rows, and clips with none.
INSERT INTO nas_inventory_files SELECT 13, id, CASE WHEN id % 7 = 0 THEN 'missing' ELSE 'present' END,
  CASE WHEN id % 11 = 0 THEN 'moved' ELSE display_path END, size_bytes + (id % 13 = 0)::int,
  CASE WHEN id % 5 = 0 THEN '' WHEN id % 17 = 0 THEN md5('bad') ELSE upper(sha256) END,
  clip_end_at, clip_end_at + interval '1 hour', 1700000000000000000 + id
  FROM recording_clips WHERE id % 4 <> 0;
INSERT INTO recording_collation_hours SELECT r * 1000 + h, r FROM generate_series(1, 6) r, generate_series(0, 959) h;
INSERT INTO recording_collation_outputs SELECT id, id, 'joined/nightly-collation-v2/' || id, 'c/' || id, 10 + id,
  md5(id::text), timestamptz '2026-08-25' + (id % 1000) * interval '1 hour',
  timestamptz '2026-08-25' + (id % 1000 + 1) * interval '1 hour' FROM recording_collation_hours;
INSERT INTO recording_collation_output_acks SELECT id, 13, nas_relative_path, size_bytes,
  CASE WHEN id % 9 = 0 THEN '' ELSE sha256 END, timestamptz '2026-11-01' FROM recording_collation_outputs WHERE id % 6 <> 0;
-- Issued NAS deletes, item-bearing and legacy (manifest ordinal), veto both kinds.
INSERT INTO nas_cleanup_plans VALUES (1, (SELECT jsonb_agg(jsonb_build_object('kind', 'raw', 'source_id', 100000 + n * 3)
  ORDER BY n) FROM generate_series(10000, 10200) n));
INSERT INTO nas_cleanup_items SELECT 1, n - 9999, NULL, n % 2 = 0 FROM generate_series(10000, 10200) n;
INSERT INTO nas_cleanup_items SELECT 2, n, jsonb_build_object('kind', 'raw', 'source_id', 200000 + n * 3), true
  FROM generate_series(10000, 10300) n;
INSERT INTO nas_cleanup_items SELECT 2, 100000 + id, jsonb_build_object('kind', 'collated', 'source_id', id), id % 10 = 0
  FROM recording_collation_outputs WHERE id % 5 = 0;
ANALYZE;
"""
MIGRATION = HERE.parents[2] / 'infra/sql/migrations/0273_r2prune_receipts_live_clip_window.sql'


@needs_db
class Receipts(PostgresLane):
    schema = RECEIPTS_SCHEMA

    def receipts(self):
        return sorted(self.sql(cycle.RECEIPTS).splitlines())

    def clip_scan(self):
        """The recording_clips node of RECEIPTS' own plan, run under its SET LOCALs."""
        body = cycle.RECEIPTS.replace('COPY (\n', 'EXPLAIN (ANALYZE, FORMAT JSON)\n', 1)
        body = body.replace(") TO STDOUT WITH (FORMAT csv, DELIMITER E'\\t');", ';', 1)
        self.assertEqual(body.count('EXPLAIN'), 1)
        self.assertNotIn('COPY', body)
        def walk(node):
            yield node
            for child in node.get('Plans', []):
                yield from walk(child)
        plan = json.loads(self.sql(body))[0]['Plan']
        [scan] = [n for n in walk(plan) if n.get('Relation Name') == 'recording_clips']
        return scan

    def test_live_clip_index_reads_only_kept_clips(self):
        before = self.receipts()
        self.assertNotEqual(self.clip_scan().get('Index Name'), 'r2prune_receipts_live_clip_window')
        self.sql(MIGRATION.read_text() + 'ANALYZE recording_clips;\n')
        scan = self.clip_scan()
        self.assertEqual(scan['Index Name'], 'r2prune_receipts_live_clip_window')
        self.assertEqual(scan.get('Rows Removed by Filter', 0), 0)
        after = self.receipts()
        self.assertEqual(after, before)
        kinds = [line.split('\t')[3] for line in after]
        self.assertGreater(kinds.count('raw'), 1000)
        self.assertGreater(kinds.count('collated'), 100)
        # A rerun keeps the index; another definition under its name refuses.
        self.sql(MIGRATION.read_text())
        self.sql('DROP INDEX r2prune_receipts_live_clip_window;'
                 ' CREATE INDEX r2prune_receipts_live_clip_window ON recording_clips (recording_id, clip_start_at);')
        with self.assertRaises(subprocess.CalledProcessError) as caught:
            self.sql(MIGRATION.read_text())
        self.assertIn('not the valid exact index', caught.exception.stderr)


if __name__ == '__main__':
    unittest.main()
