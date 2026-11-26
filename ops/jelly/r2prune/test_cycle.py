# /usr/bin/python3 -I ops/jelly/r2prune/test_cycle.py -q
# The lane preflight: chunked, one snapshot, fail closed. With STOARAMA_TEST_DATABASE_URL it
# also runs preflight.sql on a stub catalog and checks the chunked vetoes equal the old
# single JSON-literal statement's. Nothing reaches production.
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


@unittest.skipUnless(os.environ.get('STOARAMA_TEST_DATABASE_URL'), 'needs STOARAMA_TEST_DATABASE_URL')
class AgainstPostgres(Lane):
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
        subprocess.run([self.psql, '-XqAt', '-v', 'ON_ERROR_STOP=1', self.url], input=SCHEMA,
                       text=True, check=True, capture_output=True)
        self.use_db(self.url)

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


if __name__ == '__main__':
    unittest.main()
