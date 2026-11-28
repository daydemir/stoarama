# /usr/bin/python3 -I ops/jelly/r2prune/test_cycle.py -q
# The lane preflight: chunked, one snapshot, fail closed. With STOARAMA_TEST_DATABASE_URL it
# also runs preflight.sql on a stub catalog and checks the chunked vetoes equal the old
# single JSON-literal statement's, and that RECEIPTS reads live clips through migration
# 0273's index with an unchanged result. Nothing reaches production.
import hashlib
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


# A stub `stoaramactl nas-selected-verify`: requests and reports come from a JSON state
# file the test writes, and every call is logged there.
STOARAMACTL = r"""#!/usr/bin/python3 -I
import json, os, sys
state_path = os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])), 'verify-state.json')
state = json.load(open(state_path))
state['calls'].append({'argv': sys.argv[1:], 'database_url': os.environ.get('DATABASE_URL')})
args = sys.argv[2:]
if state.get('fail'):
    json.dump(state, open(state_path, 'w'))
    sys.stderr.write('connect database: ' + os.environ['DATABASE_URL'] + '\n')
    sys.exit(1)
if args[0] == 'request':
    out = {'plan_sha256': state['plan_sha256'], 'items': state.get('plan_items', len(state['reports'][0]['items']))}
else:
    reports = state['reports']
    out = reports[0] if len(reports) == 1 else reports.pop(0)
json.dump(state, open(state_path, 'w'))
print(json.dumps(out))
"""
PLAN_SHA = 'a' * 64
# A stub r2prune: lists the snapshot rows it is given, plans every listed key for delete,
# and on delete logs which manifest keys its --fresh-listing admitted.
R2PRUNE = r"""#!/usr/bin/python3 -I
import hashlib, json, os, sys
L = os.path.dirname(os.path.abspath(sys.argv[0]))
a = sys.argv[1:]
opt = lambda name: a[a.index(name) + 1]
if a[0] == 'list':
    for k, size in (('joined/nightly-collation-v2/c', 300), ('managed/a', 100), ('managed/b', 200), ('managed/nohash', 400)):
        print(f'{k}\t{size}\t"e"\t1')
elif a[0] == 'plan':
    out = opt('--out')
    os.makedirs(out)
    rows = [l.split('\t') for l in open(opt('--snapshot'))]
    with open(out + '/delete-manifest.jsonl', 'w') as f:
        for k, size, etag, mtime in rows:
            f.write(json.dumps({'key': k, 'size': int(size), 'etag': etag, 'mtime': int(mtime), 'class': 'delete_window_20d_on_nas'}) + '\n')
    digest = hashlib.sha256(open(out + '/delete-manifest.jsonl', 'rb').read()).hexdigest()
    json.dump({'manifest_sha256': digest, 'delete': {'objects': len(rows), 'bytes': 1},
               'by_class': {'delete_window_20d_on_nas': {'objects': len(rows)}}}, open(out + '/summary.json', 'w'))
elif a[0] == 'delete':
    fresh = {l.split('\t')[0] for l in open(opt('--fresh-listing'))}
    keys = [json.loads(l)['key'] for l in open(opt('--manifest'))]
    with open(opt('--ledger'), 'w') as f:
        f.write(json.dumps({'type': 'run', 'manifest_sha256': opt('--expected-sha256'), 'limit': 0}) + '\n')
        for k in keys:
            row = json.loads(open(opt('--manifest')).read().splitlines()[keys.index(k)])
            outcome = 'deleted' if k in fresh else 'skipped_changed'
            f.write(json.dumps({'key': k, 'size': row['size'], 'outcome': outcome, 'at': '2026-11-27T09:00:00Z'}) + '\n')
    with open(L + '/deletes.jsonl', 'a') as f:
        f.write(json.dumps({'deleted': sorted(fresh & set(keys)), 'skipped': sorted(set(keys) - fresh)}) + '\n')
"""


def verify_item(kind, source_id, size, sha, state='match', completed='2099-01-01 00:00:01.5+00'):
    return {'item_id': source_id, 'kind': kind, 'source_id': source_id, 'relative_path': f'p/{source_id}',
            'size_bytes': size, 'sha256': sha, 'state': state, 'attempts': 1,
            'completed_at': completed if state not in ('pending', 'leased') else None, 'error': ''}


class Redundancy(Lane):
    """The live NAS gate in front of every lane DELETE."""

    def setUp(self):
        super().setUp()
        self.L = str(self.dir)
        Path(self.L, 'stoaramactl').write_text(STOARAMACTL)
        os.chmod(Path(self.L, 'stoaramactl'), 0o755)
        env = self.dir / 'db.env'
        env.write_text("DATABASE_URL='postgres://u:s%40cret@db.invalid:5432/x'\n")
        for name, value in (('DBENV', str(env)), ('NAS_VERIFY_POLL_SEC', 0), ('NAS_VERIFY_CHUNK', 2)):
            patch = unittest.mock.patch.object(cycle, name, value)
            patch.start()
            self.addCleanup(patch.stop)
        self.W = str(self.dir / 'cycle')
        os.makedirs(self.W)
        self.sources = {'managed/a': self.source('raw', 11, 100), 'managed/b': self.source('raw', 12, 200),
                        'joined/nightly-collation-v2/c': self.source('collated', 7, 300)}
        Path(self.W, 'snapshot.tsv').write_text(''.join(f'{k}\t{s["size"]}\t"e"\t1\n' for k, s in sorted(self.sources.items())))

    @staticmethod
    def source(kind, sid, size):
        return {'kind': kind, 'source_id': sid, 'stream_id': 1, 'size': size,
                'sha256': hashlib.sha256(str(sid).encode()).hexdigest(), 'etag': 'e'}

    def reports(self, *item_lists, created='2099-01-01 00:00:00+00'):
        return [{'label': 'x', 'plan_sha256': PLAN_SHA, 'created_at': created, 'items': items} for items in item_lists]

    def chunk_items(self, **overrides):
        # The chunk is the first NAS_VERIFY_CHUNK planned keys: joined/... and managed/a.
        items = []
        for key in ('joined/nightly-collation-v2/c', 'managed/a'):
            s = self.sources[key]
            item = verify_item(s['kind'], s['source_id'], s['size'], s['sha256'])
            item.update(overrides.get(key, {}))
            items.append(item)
        return items

    def write_state(self, reports, **extra):
        Path(self.L, 'verify-state.json').write_text(json.dumps({'calls': [], 'plan_sha256': PLAN_SHA, 'reports': reports, **extra}))

    def state(self):
        return json.loads(Path(self.L, 'verify-state.json').read_text())

    def run_gate(self):
        return cycle.verified_chunk(self.L, self.W, '20261127T090000Z', self.sources, self.sources)

    def test_fresh_exact_matches_admit_only_the_chunk(self):
        pending = [dict(i, state='leased', completed_at=None) for i in self.chunk_items()]
        self.write_state(self.reports(pending, self.chunk_items()))
        self.assertEqual(self.run_gate(), ['joined/nightly-collation-v2/c', 'managed/a'])
        listing = Path(self.W, 'delete-listing.tsv').read_text().splitlines()
        self.assertEqual([l.split('\t')[0] for l in listing], ['joined/nightly-collation-v2/c', 'managed/a'])
        calls = [c['argv'] for c in self.state()['calls']]
        base = ['nas-selected-verify', 'request', '--label', 'r2prune-lane-20261127t090000z',
                '--clip-ids', '11', '--collated-ids', '7']
        self.assertEqual(calls[0], base + ['--dry-run', '--json'])
        self.assertEqual(calls[1], base + ['--expected-plan-sha256', PLAN_SHA, '--json'])
        self.assertEqual(calls[2:], [['nas-selected-verify', 'report', '--label', 'r2prune-lane-20261127t090000z', '--json']] * 2)
        # Credentials reach stoaramactl only through the environment.
        self.assertTrue(all(c['database_url'].startswith('postgres://') for c in self.state()['calls']))
        self.assertFalse(any('cret' in a for c in calls for a in c))
        self.assertEqual(json.loads(Path(self.W, 'nas-verify.json').read_text())['plan_sha256'], PLAN_SHA)

    def assert_refused(self, reports, pattern, **extra):
        self.write_state(reports, **extra)
        with self.assertRaisesRegex(RuntimeError, pattern):
            self.run_gate()
        self.assertFalse(Path(self.W, 'delete-listing.tsv').exists())

    def test_any_single_failure_stops_the_chunk_at_once(self):
        for state in ('missing', 'conflict', 'unverified'):
            with self.subTest(state=state):
                items = self.chunk_items(**{'managed/a': {'state': state, 'error': 'local sha256 differs'}})
                items[0].update(state='pending', completed_at=None)
                self.assert_refused(self.reports(items, self.chunk_items()), f'1 NAS copies not verified.*{state}')
                self.assertEqual(len(self.state()['calls']), 3)  # no second report was waited for

    def test_identity_mismatch_refuses(self):
        other = hashlib.sha256(b'other').hexdigest()
        for override in ({'size_bytes': 101}, {'sha256': other}, {'source_id': 99},
                         {'completed_at': '2098-12-31 23:59:59+00'}):
            with self.subTest(override=override):
                self.assert_refused(self.reports(self.chunk_items(**{'managed/a': override})),
                                    'not a fresh exact match|differ from the chunk')

    def test_an_old_plan_is_not_fresh(self):
        self.assert_refused(self.reports(self.chunk_items(), created='2026-01-01 00:00:00+00'), 'predates this chunk')

    def test_unfinished_verify_times_out_without_deleting(self):
        pending = [dict(i, state='pending', completed_at=None) for i in self.chunk_items()]
        with unittest.mock.patch.object(cycle, 'NAS_VERIFY_WAIT_SEC', -1):
            self.assert_refused(self.reports(pending), 'not finished')

    def test_short_plan_refuses_before_inserting(self):
        self.assert_refused(self.reports(self.chunk_items()), 'verify plan has 1 items', plan_items=1)
        self.assertEqual(len(self.state()['calls']), 1)

    def test_stoaramactl_failure_redacts_the_password(self):
        self.write_state(self.reports(self.chunk_items()), fail=True)
        with self.assertRaisesRegex(RuntimeError, 'nas-selected-verify request failed') as caught:
            self.run_gate()
        self.assertNotIn('cret', str(caught.exception))
        self.assertIn('[redacted]', str(caught.exception))

    def test_cycle_deletes_only_the_verified_chunk(self):
        """A whole cycle on stubs: the hashless key never reaches a plan, the gate runs
        before the intent, and r2prune deletes only the chunk; a failed gate runs no DELETE."""
        L, I = self.L, Path(self.L, 'inputs')
        I.mkdir()
        (I / 'durham-holds.jsonl').write_text('{}\n')
        home = self.dir / 'home'
        (home / 'stoarama-gss-freeze').mkdir(parents=True)
        (home / 'stoarama-gss-freeze' / 'gss-protect.tsv').write_text('x\n' * 1035152)
        Path(L, 'r2.env').write_text('R2_ACCOUNT_ID=a\nR2_BUCKET=b\nR2_ACCESS_KEY_ID=c\nR2_SECRET_ACCESS_KEY=d\n')
        Path(L, 'aws').write_text('#!/bin/sh\necho "An error occurred (404)" >&2\nexit 254\n')
        Path(L, 'r2prune').write_text(R2PRUNE)
        for stub in ('aws', 'r2prune'):
            os.chmod(Path(L, stub), 0o755)
        rows = [(k, s) for k, s in sorted(self.sources.items())] + [('managed/nohash', dict(self.source('raw', 13, 400), sha256=''))]
        def fake_psql(sql, out, *, script=None):
            name = os.path.basename(out)
            text = ''.join(f'{k}\t1\t1\t{s["kind"]}\t{s["source_id"]}\t1\t{s["size"]}\t{s["sha256"]}\te\n'
                           for k, s in rows) if name == 'receipts.tsv' else ''
            Path(out).write_text(text)
        for name, value in (('psql', fake_psql), ('R2ENV', L + '/r2.env'), ('AWS', L + '/aws'),
                            ('apply_receipts', lambda W, total, fd: Path(W, 'applied').write_text(str(total)))):
            patch = unittest.mock.patch.object(cycle, name, value)
            patch.start()
            self.addCleanup(patch.stop)
        patch = unittest.mock.patch.dict(os.environ, {'HOME': str(home)})
        patch.start()
        self.addCleanup(patch.stop)

        self.write_state(self.reports(self.chunk_items(**{'managed/a': {'state': 'conflict'}})))
        W = str(self.dir / 'refused')
        os.makedirs(W)
        with unittest.mock.patch('sys.stdout'), self.assertRaisesRegex(RuntimeError, 'not verified'):
            cycle.run(L, W, '20261127T090000Z', 0)
        self.assertFalse(Path(W, 'delete-intent.json').exists())
        self.assertFalse(Path(L, 'deletes.jsonl').exists())

        self.write_state(self.reports(self.chunk_items()))
        W = str(self.dir / 'admitted')
        os.makedirs(W)
        with unittest.mock.patch('sys.stdout'):
            cycle.run(L, W, '20261127T091500Z', 0)
        [call] = [json.loads(l) for l in Path(L, 'deletes.jsonl').read_text().splitlines()]
        self.assertEqual(call['deleted'], ['joined/nightly-collation-v2/c', 'managed/a'])
        self.assertEqual(call['skipped'], ['managed/b'])
        self.assertEqual(Path(W, 'applied').read_text(), '2')
        [log] = [json.loads(l) for l in Path(L, 'runs.jsonl').read_text().splitlines()]
        self.assertEqual((log['nas_verified'], log['deferred'], log['skipped']['no_hash']), (2, 1, 1))
        self.assertEqual(log['outcomes'], {'deleted': 2, 'skipped_changed': 1})

    def test_pg_time_is_utc_only(self):
        self.assertEqual(cycle.pg_time('2026-11-27 09:02:02.359751+00').isoformat(), '2026-11-27T09:02:02.359751+00:00')
        self.assertEqual(cycle.pg_time('2026-11-27 09:02:02+00').second, 2)
        with self.assertRaises(ValueError):
            cycle.pg_time('2026-11-27 04:02:02-05')


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
-- Issued NAS deletes veto otherwise eligible in-window raw clips in both formats: a legacy
-- plan (item NULL, manifest ordinal) and item-bearing rows. Unissued rows are controls.
INSERT INTO nas_cleanup_plans VALUES (1, '[{"kind": "raw", "source_id": 102598}, {"kind": "raw", "source_id": 102601},
  {"kind": "raw", "source_id": 102607}, {"kind": "raw", "source_id": 102610}]');
INSERT INTO nas_cleanup_items VALUES (1, 1, NULL, true), (1, 2, NULL, false), (1, 3, NULL, true), (1, 4, NULL, false);
INSERT INTO nas_cleanup_items SELECT 2, n, jsonb_build_object('kind', 'raw', 'source_id', id), n <> 2
  FROM (VALUES (1, 202883), (2, 202886), (3, 202889)) v(n, id);
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

    def assert_issued_deletes_veto(self, rows):
        keys = {line.split('\t')[0] for line in rows}
        for r, n in ((1, 2598), (1, 2607), (2, 2883), (2, 2889)):
            self.assertNotIn(f'managed/{r}/{n}', keys)
        for r, n in ((1, 2601), (1, 2610), (2, 2886)):
            self.assertIn(f'managed/{r}/{n}', keys)

    def test_issued_deletes_veto_eligible_clips(self):
        self.assert_issued_deletes_veto(self.receipts())
        # The vetoed clips are otherwise eligible: unissued, they are receipts.
        self.sql('UPDATE nas_cleanup_items SET delete_ever_issued = false;')
        keys = {line.split('\t')[0] for line in self.receipts()}
        for r, n in ((1, 2598), (1, 2607), (2, 2883), (2, 2889)):
            self.assertIn(f'managed/{r}/{n}', keys)

    def test_live_clip_index_reads_only_kept_clips(self):
        before = self.receipts()
        self.assert_issued_deletes_veto(before)
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
