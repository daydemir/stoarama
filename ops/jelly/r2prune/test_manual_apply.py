# /usr/bin/python3 -I ops/jelly/r2prune/test_manual_apply.py -q
# Offline: stub psql and r2prune executables record their calls, and a local HTTP server
# answers registry GETs; nothing reaches a DB or R2.
import contextlib
import fcntl
import gzip
import hashlib
import http.server
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import unittest
import unittest.mock

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('manual_apply', HERE / 'manual_apply.py')
ma = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ma)

SHA = 'a' * 64
RAW = 'managed/acct-47/recordings/1/continuous/1.mp4'
COL = 'joined/nightly-collation-v2/objects/x.mp4'
KE = [f'managed/acct-47/ke/{i}.mp4' for i in (1, 2, 3)]
GEN = 'b' * 32
ASSET = 'datasets/global-street-scores-delivery/47/assets/a.json'
SERVED = 'joined/recovery-best14-20261125/objects/served.mp4'

# psql stub: answers the catalog lookup from sources.json, the chunk verify from db.json, the
# dry run from dry-run.out, and the receipt apply with apply.rc; every call (with the receipt
# TSV it was run on) is appended to calls.jsonl.
PSQL = r'''#!/usr/bin/env python3
import json, os, sys
S = os.environ['STUB']
script = sys.stdin.read()
args = sys.argv[1:]
f = args[args.index('-f') + 1] if '-f' in args else ''
tsv = open('receipt-backfill.tsv').read() if f and os.path.exists('receipt-backfill.tsv') else None
kind = 'pin' if 'gss_public_generation_pin' in script else 'stdin'
with open(S + '/calls.jsonl', 'a') as c:
    c.write(json.dumps({'psql': os.path.basename(f) or kind, 'args': args,
                        'cwd': os.getcwd(), 'ro': 'default_transaction_read_only=on' in os.environ['PGOPTIONS'], 'tsv': tsv, 'script_bytes': len(script)}) + '\n')
if kind == 'pin':
    print(open(S + '/pin').read().strip())
    sys.exit(0)
if f.endswith('receipt-apply.sql'):
    rc = int(open(S + '/apply.rc').read()) if os.path.exists(S + '/apply.rc') else 0
    if rc:
        sys.stderr.write('ERROR:  reservation or hold at TSV row 1\n')
    sys.exit(rc)
if f.endswith('dry-run.sql'):
    if os.path.exists(S + '/dry-run.out'):
        sys.stdout.write(open(S + '/dry-run.out').read())
        sys.exit(0)
    # Like dry-run.sql: the summary per (action, kind), then each non-insert row by seq;
    # every seq divisible by 7 reads as already applied.
    rows = [l.split('\t') for l in tsv.splitlines()]
    act = lambda r: 'already_applied' if int(r[0]) % 7 == 0 else 'insert'
    groups = {}
    for r in rows:
        g = groups.setdefault((act(r), r[1]), [0, 0, 0])
        g[0] += 1; g[1] += int(r[5]); g[2] += r[1] == 'raw' and act(r) == 'insert'
    for (a, k), (n, b, u) in sorted(groups.items()):
        print(f'{a}|{k}||{n}|{b}|{u}')
    for r in rows:
        if act(r) != 'insert':
            print(f'{r[0]}|{act(r)}||{r[1]}|{r[2]}|{r[5]}')
    sys.exit(0)
wanted = set(json.loads(script.split('$k$')[1]))
for key, s in json.load(open(S + '/sources.json')).items():
    if key in wanted:
        print('\t'.join(map(str, (key, s['kind'], s['source_id'], s['stream_id'], s['size'], s['sha256'], s['etag'] or ''))))
'''

# An old binary would accept delete. This recording stub must never be called.
R2PRUNE = r'''#!/usr/bin/env python3
import json, os, sys
with open(os.environ['STUB'] + '/calls.jsonl', 'a') as c:
    c.write(json.dumps({'r2prune': sys.argv[1:]}) + '\n')
sys.exit(1)
'''


class Head(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.server.requests.append(self.path)
        body = Path(os.environ['STUB'], 'bodies', self.path.split('/', 2)[2])
        if not body.exists():
            self.send_response(404)
            self.end_headers()
            return
        data = body.read_bytes()
        self.send_response(200)
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def unexpected(self):
        self.server.requests.append((self.command, self.path))
        self.send_response(405)
        self.end_headers()

    do_HEAD = do_POST = do_DELETE = do_PUT = unexpected

    def log_message(self, *_):
        pass


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.d = Path(self.tmp.name).resolve()
        os.environ['STUB'] = str(self.d)
        os.environ['no_proxy'] = '*'
        for name, body in (('psql', PSQL), ('r2prune', R2PRUNE)):
            p = self.d / name
            p.write_text(body.replace('#!/usr/bin/env python3', '#!' + sys.executable + ' -I'))
            p.chmod(0o755)
        self.server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Head)
        self.server.requests = []
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        (self.d / 'db.env').write_text('DATABASE_URL=postgres://u:secretpw@db.invalid:5432/stoarama?sslmode=require\n')
        (self.d / 'r2.env').write_text(f'R2_ENDPOINT=http://127.0.0.1:{self.server.server_port}\nR2_ACCESS_KEY_ID=ak\n'
                                       'R2_SECRET_ACCESS_KEY=sk\nR2_BUCKET=stoarama\n')
        self.root = self.d / 'manual'
        self.W = self.root / 'work'
        self.manifest = self.d / 'delete-manifest.jsonl'
        # The pinned generation serves SERVED; the plan protects and classifies it.
        self.serve(GEN, [SERVED])
        self.plan = self.d / 'plan'
        self.plan.mkdir()
        self.protect = self.d / 'protect-db-now.tsv'
        self.protect.write_text(f'page_link\t{SERVED}\npage_link\t{ASSET}\nstitch_input_hold\tx\n')
        (self.plan / 'classified.tsv').write_text(f'{SERVED}\t5\tprotect_live_site\tdb:page_link\n{RAW}\t100\tdelete_raw_outside_window\tr\n')

    def serve(self, gen, keys, pin=None):
        (self.d / 'pin').write_text(pin or gen)
        body = self.d / 'bodies' / f'{ma.GSS_PREFIX}public/generations/{gen}.json'
        body.parent.mkdir(parents=True, exist_ok=True)
        body.write_bytes(gzip.compress(json.dumps({'generation': gen, 'files': {str(i): {'key': k} for i, k in enumerate(keys)},
                                                   'assets': {'/a.json': {'key': ASSET}}}).encode()))

    def rule_args(self):
        return ['--summary', str(self.plan / 'summary.json'), '--db-protect', str(self.protect)]

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.tmp.cleanup()

    def write_manifest(self, rows):
        self.manifest.write_text(''.join(json.dumps({'key': k, 'size': 100, 'etag': '"e1"', 'mtime': 1, 'class': c,
                                                     'reason': 'r'}) + '\n' for k, c in rows))
        self.digest = hashlib.sha256(self.manifest.read_bytes()).hexdigest()

    def sources(self, rows):
        (self.d / 'sources.json').write_text(json.dumps({k: {'kind': kind, 'source_id': sid, 'stream_id': 7, 'size': 100,
                                                             'sha256': SHA, 'etag': etag} for k, (kind, sid, etag) in rows.items()}))

    def run_cli(self, *args):
        base = ['--psql', str(self.d / 'psql'), '--r2prune', str(self.d / 'r2prune'), '--lock', str(self.d / 'lock'),
                '--db-env', str(self.d / 'db.env'), '--r2-env', str(self.d / 'r2.env'), '--lane-cycles', str(self.d / 'lane')]
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            ma.main(base + list(args))
        return [json.loads(l) for l in out.getvalue().splitlines()]

    def calls(self, kind=None):
        p = self.d / 'calls.jsonl'
        calls = [json.loads(l) for l in p.read_text().splitlines()] if p.exists() else []
        if kind is None:  # the gate's pin read is asserted by the Gate tests
            return [c.get('psql', 'r2prune') for c in calls if c.get('psql') != 'pin']
        return [c for c in calls if c.get('psql', 'r2prune') == kind]

    def applied(self):
        """{key: deleted_at} over every receipt-apply.sql call."""
        return {r[4]: r[8] for c in self.calls('receipt-apply.sql') for r in (l.split('\t') for l in c['tsv'].splitlines())}

    def result(self):
        return json.loads((self.W / 'receipt-result.json').read_text())


class ListingMode(Base):
    def setUp(self):
        super().setUp()
        self.write_manifest([(RAW, 'delete_raw_outside_window'), (COL, 'delete_collated_outside_window')])
        self.sources({RAW: ('raw', 11, 'e1'), COL: ('collated', 22, None)})
        self.listing = self.d / 'f.tsv'
        self.listing.write_text(f'{COL}\t100\te1\t1\n{RAW}\t100\te1\t1\n')

    def delete(self, *extra):
        return self.run_cli('delete', str(self.W), '--', '--manifest', str(self.manifest), '--expected-sha256', self.digest,
                            *self.rule_args(), '--fresh-listing', str(self.listing), *extra)

    def historical(self, outcomes, *, stamp='20261125T100000Z', at='2026-11-25T10:00:00.5Z',
                   partial=False, mode='listing', listing=None, fresh_sha=None):
        """Make synthetic historical intent and ledger, without invoking any delete code."""
        self.W.mkdir(parents=True, exist_ok=True)
        source_path = self.W / 'receipt-sources.json'
        source_path.write_bytes((self.d / 'sources.json').read_bytes())
        pinned_sha = ma.sha(self.listing) if mode == 'listing' else None
        (self.W / 'delete-intent.json').write_text(json.dumps({
            'mode': mode, 'manifest': str(self.manifest), 'manifest_sha256': self.digest,
            'sources_sha256': ma.sha(source_path), 'fresh_listing_sha256': pinned_sha}))
        if listing is not None:
            path = self.W / f'listing-{stamp}.tsv'
            path.write_text(listing)
            pinned_sha = ma.sha(path)
        ledger = self.W / f'ledger-{stamp}.jsonl'
        rows = [{'type': 'run', 'manifest_sha256': self.digest, 'inputs_sha256': {'picks': 'p'},
                 'recent_from': '2026-11-18T18:00:00Z', 'fresh_listing_sha256': fresh_sha or pinned_sha,
                 'limit': 0, 'started_at': at}]
        rows += [dict({'key': k, 'size': 100, 'outcome': outcome, 'at': at}, **detail)
                 for k, outcome, detail in outcomes]
        body = ''.join(json.dumps(r) + '\n' for r in rows)
        ledger.write_text(body[:-12] if partial else body)
        return ledger

    def assert_blocked(self, *extra):
        with self.assertRaisesRegex(SystemExit, 'legacy R2 deletion is disabled'):
            self.delete(*extra)

    def test_new_and_stale_inputs_are_refused_before_any_intent_or_call(self):
        for extra in ((), ('--limit', '1'), ('--ledger', 'x'), ('--ke-coverage', 'old.tsv')):
            self.assert_blocked(*extra)
        self.digest = '0' * 64
        self.listing.unlink()
        self.assert_blocked()
        self.assertEqual(self.calls(), [])
        self.assertFalse(self.W.exists())
        self.assertFalse((self.d / 'lock').exists())
        self.assertEqual(self.server.requests, [])

    def test_receipts_for_historical_acknowledgements(self):
        self.historical([(RAW, 'deleted', {}), (COL, 'deleted', {})])
        self.run_cli('receipts', str(self.W))
        self.assertEqual(self.calls(), ['receipt-apply.sql'])
        self.assertEqual(self.applied(), {RAW: '2026-11-25T10:00:00.5Z', COL: '2026-11-25T10:00:00.5Z'})
        self.assertTrue(self.result()['complete'])

    def test_failed_receipt_write_fails_closed_and_replays(self):
        self.historical([(RAW, 'deleted', {}), (COL, 'deleted', {})])
        (self.d / 'apply.rc').write_text('3')
        with self.assertRaisesRegex(SystemExit, 'receipt apply failed'):
            self.run_cli('receipts', str(self.W))
        self.assertFalse((self.W / 'receipt-result.json').exists())
        (self.d / 'apply.rc').write_text('0')
        self.run_cli('receipts', str(self.W))
        self.assertEqual(self.result()['deleted_receipts'], 2)

    def test_failed_outcome_stays_open_without_retrying_old_delete(self):
        self.historical([(RAW, 'failed', {}), (COL, 'deleted', {})])
        self.run_cli('receipts', str(self.W))
        self.assertEqual(set(self.applied()), {COL})
        self.assertEqual((self.result()['complete'], self.result()['unresolved']), (False, 1))
        before = {p: p.read_bytes() for p in self.W.iterdir() if p.is_file()}
        self.assert_blocked()
        self.assertEqual(before, {p: p.read_bytes() for p in self.W.iterdir() if p.is_file()})
        self.assertNotIn('r2prune', self.calls())

    def test_multiple_historical_ledgers_keep_first_acknowledgement(self):
        self.historical([(RAW, 'deleted', {})])
        self.historical([(RAW, 'deleted', {}), (COL, 'deleted', {})],
                        stamp='20261125T110000Z', at='2026-11-25T11:00:00Z')
        self.run_cli('receipts', str(self.W))
        self.assertEqual(self.applied(), {RAW: '2026-11-25T10:00:00.5Z', COL: '2026-11-25T11:00:00Z'})
        self.assertTrue(self.result()['complete'])

    def test_ledger_run_against_another_listing_is_refused(self):
        self.historical([(RAW, 'deleted', {})], fresh_sha='0' * 64)
        with self.assertRaisesRegex(SystemExit, 'another fresh listing'):
            self.run_cli('receipts', str(self.W))

    def test_half_written_last_ledger_line_is_unlogged(self):
        self.historical([(RAW, 'deleted', {}), (COL, 'deleted', {})], partial=True)
        self.run_cli('receipts', str(self.W))
        self.assertEqual((self.result()['deleted_receipts'], self.result()['unresolved']), (1, 1))

    def test_duplicate_key_in_one_ledger_is_refused(self):
        self.historical([(RAW, 'deleted', {}), (RAW, 'deleted', {})])
        with self.assertRaisesRegex(SystemExit, 'duplicate or unexpected ledger row'):
            self.run_cli('receipts', str(self.W))
        self.assertEqual(self.calls('receipt-apply.sql'), [])

    def test_busy_lock_refuses_historical_receipt_replay(self):
        with open(self.d / 'lock', 'a') as held:
            fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(SystemExit, 'is held'):
                self.run_cli('receipts', str(self.W))
        self.assertEqual(self.calls(), [])

    def test_receipt_sources_are_rechecked_on_reentry(self):
        self.historical([(RAW, 'deleted', {})])
        (self.W / 'receipt-sources.json').write_text('{}\n')
        with self.assertRaisesRegex(SystemExit, 'receipt sources changed'):
            self.run_cli('receipts', str(self.W))
        self.assertEqual(self.calls(), [])

    def test_status_lists_open_intents(self):
        self.historical([(RAW, 'deleted', {})])
        out = io.StringIO()
        with contextlib.redirect_stdout(out), self.assertRaisesRegex(SystemExit, '1 open delete intents'):
            ma.main(['status', str(self.root)])
        self.assertEqual(json.loads(out.getvalue())['workdir'], str(self.W))
        self.historical([(RAW, 'deleted', {}), (COL, 'deleted', {})])
        self.run_cli('receipts', str(self.W))
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            ma.main(['status', str(self.root)])
        self.assertEqual(json.loads(out.getvalue()), {'open': 0})

    def test_historical_chunk_ledger_defers_out_of_scope_rows(self):
        self.historical([(RAW, 'deleted', {}), (COL, 'skipped_changed', {'detail': ma.ABSENT})],
                        mode='verify', listing=f'{RAW}\t100\te1\t1\n')
        self.run_cli('receipts', str(self.W))
        self.assertEqual(set(self.applied()), {RAW})
        self.assertEqual((self.result()['complete'], self.result()['unresolved']), (False, 1))


class Backfill(Base):
    def setUp(self):
        super().setUp()
        self.write_manifest([(KE[0], ma.LEGACY_KE_CLASS), (KE[1], ma.LEGACY_KE_CLASS)])
        self.sources({KE[0]: ('raw', 11, 'e1'), KE[1]: ('raw', 12, 'e1')})
        self.ledger = self.d / 'ledger.jsonl'
        self.ledger.write_text(''.join(json.dumps(r) + '\n' for r in [
            {'type': 'run', 'manifest_sha256': self.digest, 'inputs_sha256': {'ke_coverage': 'c'}, 'recent_from': '2026-11-18T18:00:00Z',
             'release': 'x', 'fresh_listing_sha256': 'f' * 64, 'limit': 0, 'started_at': '2026-11-23T23:12:00Z'},
            {'key': KE[0], 'size': 100, 'outcome': 'deleted', 'at': '2026-11-23T23:13:39.424675Z'},
            {'key': KE[1], 'size': 100, 'outcome': 'deleted', 'at': '2026-11-23T23:13:39.424677Z'}]))

    def backfill(self):
        return self.run_cli('backfill', str(self.W), '--manifest', str(self.manifest), '--ledger', str(self.ledger))

    def test_backfills_a_run_made_without_the_wrapper_once(self):
        self.backfill()
        self.assertEqual(self.calls(), ['stdin', 'dry-run.sql', 'receipt-apply.sql'])
        times = {KE[0]: '2026-11-23T23:13:39.424675Z', KE[1]: '2026-11-23T23:13:39.424677Z'}
        dry = {r[4]: r[8] for r in (l.split('\t') for l in self.calls('dry-run.sql')[0]['tsv'].splitlines())}
        self.assertEqual((dry, self.applied()), (times, times))
        self.assertTrue(self.result()['complete'])
        self.assertEqual(json.loads((self.W / 'delete-intent.json').read_text())['mode'], 'backfill')
        (self.d / 'dry-run.out').write_text('already_applied|raw||2|200|0\n1|already_applied||raw|11|100\n2|already_applied||raw|12|100\n')
        self.assertEqual(self.backfill(), [{'workdir': str(self.W), 'already_applied': 2}])
        self.assertEqual(self.calls(), ['stdin', 'dry-run.sql', 'receipt-apply.sql', 'dry-run.sql'])

    def test_a_large_backfill_runs_in_bounded_statements_with_one_shot_results(self):
        n = 12001
        keys = [f'managed/acct-47/ke/{i:06d}.mp4' for i in range(n)]
        self.write_manifest([(k, ma.LEGACY_KE_CLASS) for k in keys])
        self.sources({k: ('raw', 100 + i, 'e1') for i, k in enumerate(keys)})
        self.ledger.write_text(''.join(json.dumps(r) + '\n' for r in [
            {'type': 'run', 'manifest_sha256': self.digest, 'inputs_sha256': {}, 'recent_from': '2026-11-18T18:00:00Z', 'limit': 0}] + [
            {'key': k, 'size': 100, 'outcome': 'deleted', 'at': '2026-11-23T23:13:39.424675Z'} for k in keys]))
        self.backfill()
        self.assertEqual(self.calls(), ['stdin'] * 3 + ['dry-run.sql'] * 3 + ['receipt-apply.sql'])
        stdin = self.calls('stdin')
        self.assertLess(max(c['script_bytes'] for c in stdin), 5000 * 64)
        dry = self.calls('dry-run.sql')
        self.assertEqual([len(c['tsv'].splitlines()) for c in dry], [5000, 5000, 2001])
        self.assertLess(max(len(c['tsv']) for c in dry), 5000 * 400)
        self.assertEqual(len(json.loads((self.W / 'receipt-sources.json').read_text())), n)
        self.assertEqual(len(self.calls('receipt-apply.sql')[0]['tsv'].splitlines()), n - n // 7)  # already applied rows are skipped
        chunked = (self.W / 'preflight' / 'dry-run.out').read_text()
        one_shot = self.d / 'one-shot'
        with unittest.mock.patch.object(ma, 'ROWS_PER_STATEMENT', 10 ** 9):
            self.run_cli('backfill', str(one_shot), '--manifest', str(self.manifest), '--ledger', str(self.ledger))
        self.assertEqual(len(self.calls('dry-run.sql')), 4)
        self.assertEqual(chunked, (one_shot / 'preflight' / 'dry-run.out').read_text())
        done = n // 7  # seq 7, 14, ... read as already applied
        self.assertEqual(chunked.splitlines()[:2], [f'already_applied|raw||{done}|{done * 100}|0',
                                                    f'insert|raw||{n - done}|{(n - done) * 100}|{n - done}'])
        self.assertEqual(chunked.splitlines()[2:], [f'{s}|already_applied||raw|{99 + s}|100' for s in range(7, n + 1, 7)])

    def status(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            try:
                ma.main(['status', str(self.root)])
            except SystemExit as e:
                return str(e), [json.loads(l) for l in out.getvalue().splitlines()]
        return None, [json.loads(l) for l in out.getvalue().splitlines()]

    def test_defer_blocked_applies_the_rest_and_a_rerun_closes_once_the_hold_lifts(self):
        held = 'trigger_nas_cleanup_source_hold'
        (self.d / 'dry-run.out').write_text(f'blocked|raw|{held}|1|100|1\ninsert|raw||1|100|1\n2|blocked|{held}|raw|12|100\n')
        out = self.run_cli('backfill', str(self.W), '--manifest', str(self.manifest), '--ledger', str(self.ledger), '--defer-blocked')
        self.assertEqual(set(self.applied()), {KE[0]})
        self.assertEqual((self.W / 'deferred-blocked.tsv').read_text(), f'{KE[1]}\t100\t{held}\n')
        self.assertEqual((self.result()['state'], self.result()['complete'], self.result()['deferred']), ('deferred', False, 1))
        self.assertEqual((out[-1]['state'], out[-1]['applied_now'], out[-1]['deferred']), ('deferred', 1, 1))
        self.assertEqual(self.status(), ('1 open delete intents', [{'workdir': str(self.W), 'mode': 'backfill', 'manifest_sha256': self.digest,
                                                                    'state': 'deferred', 'unresolved': 0, 'deferred': 1}]))
        with self.assertRaisesRegex(SystemExit, 'would refuse'):  # without the flag, a held key still refuses
            self.backfill()
        self.assertEqual(len(self.calls('receipt-apply.sql')), 1)
        (self.d / 'dry-run.out').write_text('already_applied|raw||1|100|0\ninsert|raw||1|100|1\n1|already_applied||raw|11|100\n')
        self.backfill()
        self.assertEqual([c['tsv'].split('\t')[4] for c in self.calls('receipt-apply.sql')], [KE[0], KE[1]])
        self.assertEqual(self.applied()[KE[1]], '2026-11-23T23:13:39.424677Z')
        self.assertEqual((self.result()['state'], self.result()['complete'], self.result()['deferred']), ('complete', True, 0))
        self.assertFalse((self.W / 'deferred-blocked.tsv').exists())
        self.assertEqual(self.status(), (None, [{'open': 0}]))
        intent = json.loads((self.W / 'delete-intent.json').read_text())
        self.assertEqual((intent['manifest_sha256'], intent['ledger_sha256']), (self.digest, ma.sha(self.ledger)))
        (self.d / 'dry-run.out').write_text('already_applied|raw||2|200|0\n1|already_applied||raw|11|100\n2|already_applied||raw|12|100\n')
        self.assertEqual(self.backfill(), [{'workdir': str(self.W), 'already_applied': 2}])

    def test_ledger_for_another_manifest_is_refused(self):
        self.write_manifest([(KE[0], ma.LEGACY_KE_CLASS)])
        with self.assertRaisesRegex(SystemExit, 'ledger header does not describe'):
            self.backfill()
        self.assertEqual(self.calls(), [])

    def test_receipt_refusal_stops_before_the_intent(self):
        (self.d / 'dry-run.out').write_text('blocked|raw|purged_at_differs|1|100|0\n')
        with self.assertRaisesRegex(SystemExit, 'would refuse'):
            self.backfill()
        self.assertEqual(self.calls(), ['stdin', 'dry-run.sql'])
        self.assertFalse((self.W / 'delete-intent.json').exists())


class BlockedManifests(Base):
    def test_old_mixed_and_best14_manifests_refuse_without_catalog_or_r2_calls(self):
        for rows in ([(KE[0], ma.LEGACY_KE_CLASS)], [(RAW, ma.BEST14_CLASS), (KE[0], ma.LEGACY_KE_CLASS)],
                     [(KE[0], ma.BEST14_CLASS)], [(RAW, 'delete_raw_outside_window')]):
            self.write_manifest(rows)
            for extra in ([], ['--ke-coverage', 'legacy.tsv'], ['-ke-coverage=legacy.tsv']):
                with self.assertRaisesRegex(SystemExit, 'legacy R2 deletion is disabled'):
                    self.run_cli('delete', '--chunk', '1', '--chunks', '1', str(self.W), '--',
                                 '--manifest', str(self.manifest), '--expected-sha256', self.digest, *self.rule_args(), *extra)
            self.assertFalse(self.W.exists())
        self.assertEqual(self.calls(), [])
        self.assertEqual(self.calls('pin'), [])
        self.assertEqual(self.server.requests, [])


class Gate(Base):
    """The served-generation read-only gate remains available."""

    def setUp(self):
        super().setUp()
        self.write_manifest([(k, ma.BEST14_CLASS) for k in KE])
        (self.plan / 'delete-manifest.jsonl').write_text(self.manifest.read_text())

    def test_fails_closed(self):
        def other_registry():
            gens = self.d / 'bodies' / f'{ma.GSS_PREFIX}public/generations'
            (gens / f'{"e" * 32}.json').write_bytes((gens / f'{GEN}.json').read_bytes())
            (self.d / 'pin').write_text('e' * 32)
        cases = {
            'served key in the delete': (lambda: self.serve(GEN, [SERVED, KE[0]]), 'are in this delete'),
            'served key without page_link': (lambda: self.protect.write_text(f'page_link\t{ASSET}\n'), 'no page_link row'),
            'served key classified otherwise': (lambda: (self.plan / 'classified.tsv').write_text(
                f'{SERVED}\t5\tprotect_unclassified\tno_catalog_identity\n'), 'not classified protect_live_site'),
            'no pin': (lambda: (self.d / 'pin').write_text(''), 'want one generation'),
            'pin without registry': (lambda: (self.d / 'pin').write_text('d' * 32), 'is not in R2'),
            'registry of another generation': (other_registry, 'names generation'),
        }
        for name, (break_it, message) in cases.items():
            with self.subTest(name):
                self.tearDown()
                self.setUp()
                break_it()
                with self.assertRaisesRegex(SystemExit, 'gate: .*' + message):
                    self.run_cli('gate', str(self.plan), '--db-protect', str(self.protect))
                self.assertNotIn('r2prune', self.calls())
                self.assertFalse((self.W / 'delete-intent.json').exists())

    def test_protect_served_unions_and_never_removes(self):
        gss, cycle = self.d / 'gss-protect.tsv', self.d / 'cycle.py'
        gss.write_text(f'page_link\told1\npage_link\t{SERVED}\n')
        cycle.write_text('x = 1\n        if rows.count("\\n") < 2:\n            raise ValueError\n')
        args = ['--gss-protect', str(gss), '--cycle', str(cycle), 'protect-served']
        out = self.run_cli(*args)
        self.assertEqual((out[0]['added'], out[0]['rows']), (1, 3))
        self.assertEqual(gss.read_text(), f'page_link\told1\npage_link\t{SERVED}\npage_link\t{ASSET}\n')
        self.assertIn('rows.count("\\n") < 3:', cycle.read_text())
        self.assertEqual(len(list(self.d.glob('gss-protect.tsv.pre-served-*'))), 1)
        self.assertEqual(self.run_cli(*args)[0]['added'], 0)
        gss.write_text('stitch_input_hold\tx\n')
        with self.assertRaisesRegex(SystemExit, 'unexpected row'):
            self.run_cli(*args)
        gss.write_text('page_link\ta\n' * 1 + 'page_link\tb\npage_link\tc\npage_link\td\n')
        with self.assertRaisesRegex(SystemExit, 'threshold at most'):
            cycle.write_text('if rows.count("\\n") < 99:\n')
            self.run_cli(*args)

    def test_served_keys_writes_the_pinned_generation(self):
        out = self.d / 'served.txt'
        got = self.run_cli('served-keys', str(out))
        self.assertEqual((got[0]['generation'], got[0]['served_keys']), (GEN, 2))
        self.assertEqual(out.read_text(), f'{ASSET}\n{SERVED}\n')

    def test_gate_command_on_a_plan(self):
        (self.plan / 'delete-manifest.jsonl').write_text(self.manifest.read_text())
        out = self.run_cli('gate', str(self.plan), '--db-protect', str(self.protect))
        self.assertEqual(out, [{'gate': 'pass', 'generation': GEN, 'served_keys': 2}])
        self.serve(GEN, [SERVED, KE[1]])
        with self.assertRaisesRegex(SystemExit, 'are in this delete'):
            self.run_cli('gate', str(self.plan), '--db-protect', str(self.protect))


if __name__ == '__main__':
    unittest.main()
