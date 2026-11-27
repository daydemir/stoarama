# /usr/bin/python3 -I ops/jelly/r2prune/test_manual_apply.py -q
# Offline: stub psql and r2prune executables record their calls, and a local HTTP server
# answers R2 HEADs; nothing reaches a DB or R2.
import contextlib
import email.utils
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
OUT1, OUT2 = 'joined/goodplus-20260821-generation-1/objects/o1.mp4', 'joined/goodplus-20260821-generation-1/objects/o2.mp4'

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
kind = 'pin' if 'gss_public_generation_pin' in script else 'verify' if 'jsonb_to_recordset' in script else 'stdin'
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
if 'jsonb_to_recordset' in script:
    raws, pairs = [json.loads(p) for p in script.split('$k$')[1::2]]
    db = json.load(open(S + '/db.json'))
    for r in raws:
        if r['key'] in db['clips']:
            print('\t'.join(['raw', r['key']] + db['clips'][r['key']] + [str(r['key'] in db.get('kept', [])).lower(), '', '']))
    for o in sorted({p['output'] for p in pairs}):
        print('\t'.join(['output', o] + db['outputs'].get(o, ['0', '', 'false', 'false', 'false', 'false', '', 'false', 'none'])))
    for p in pairs:
        print('\t'.join(['pair', p['raw'], p['output'], 'true' if p['raw'] in db['contains'].get(p['output'], []) else 'false', '', '', '']))
    sys.exit(0)
wanted = set(json.loads(script.split('$k$')[1]))
for key, s in json.load(open(S + '/sources.json')).items():
    if key in wanted:
        print('\t'.join(map(str, (key, s['kind'], s['source_id'], s['stream_id'], s['size'], s['sha256'], s['etag'] or ''))))
'''

# r2prune stub: like delete.go, logs every manifest key (or the first --limit): absent from
# --fresh-listing is skipped_changed, a key in r2prune.fail is failed, the rest deleted (and
# removed from r2.json). r2prune.drop leaves the last row unlogged, r2prune.partial half-writes
# it, r2prune.dup logs the first row twice. Exits with r2prune.rc.
R2PRUNE = r'''#!/usr/bin/env python3
import hashlib, json, os, sys
S = os.environ['STUB']
a = sys.argv[1:]
opt = lambda n, d=None: a[a.index(n) + 1] if n in a else d
flag = lambda n: os.path.exists(S + '/r2prune.' + n)
with open(S + '/calls.jsonl', 'a') as c:
    c.write(json.dumps({'r2prune': a}) + '\n')
keys = [json.loads(l) for l in open(opt('--manifest'))]
limit = int(opt('--limit', 0))
listing = {l.split('\t')[0] for l in open(opt('--fresh-listing'))}
fail = json.load(open(S + '/r2prune.fail')) if flag('fail') else []
at = open(S + '/r2prune.at').read().strip()
r2 = json.load(open(S + '/r2.json'))
rows = []
for k in keys[:limit or None]:
    out = ('skipped_changed', 'absent_from_fresh_listing') if k['key'] not in listing else ('failed', 'x') if k['key'] in fail else ('deleted', None)
    rows.append(json.dumps(dict({'key': k['key'], 'size': k['size'], 'outcome': out[0], 'at': at}, **({'detail': out[1]} if out[1] else {}))) + '\n')
    if out[0] == 'deleted':
        r2.pop(k['key'], None)
json.dump(r2, open(S + '/r2.json', 'w'))
if flag('drop'):
    rows = rows[:-1]
if flag('partial'):
    rows[-1] = rows[-1][:-12]
if flag('dup'):
    rows.append(rows[0])
with open(opt('--ledger'), 'w') as f:
    f.write(json.dumps({'type': 'run', 'manifest_sha256': opt('--expected-sha256'), 'inputs_sha256': {'picks': 'p'},
                        'recent_from': '2026-11-18T18:00:00Z', 'fresh_listing_sha256': hashlib.sha256(open(opt('--fresh-listing'), 'rb').read()).hexdigest(),
                        'limit': limit, 'started_at': at}) + '\n')
    f.write(''.join(rows))
sys.exit(int(open(S + '/r2prune.rc').read()) if flag('rc') else 0)
'''


class Head(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
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

    def do_HEAD(self):
        objects = json.loads(Path(os.environ['STUB'], 'r2.json').read_text())
        key = self.path.split('/', 2)[2]
        self.server.auth.append(self.headers['Authorization'].startswith('AWS4-HMAC-SHA256 Credential=ak/'))
        if key not in objects:
            self.send_response(404)
            self.end_headers()
            return
        size, etag, mtime = objects[key]
        self.send_response(200)
        self.send_header('Content-Length', str(size))
        self.send_header('ETag', f'"{etag}"')
        self.send_header('Last-Modified', email.utils.formatdate(mtime, usegmt=True))
        self.end_headers()

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
        self.server.auth = []
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        (self.d / 'db.env').write_text('DATABASE_URL=postgres://u:secretpw@db.invalid:5432/stoarama?sslmode=require\n')
        (self.d / 'r2.env').write_text(f'R2_ENDPOINT=http://127.0.0.1:{self.server.server_port}\nR2_ACCESS_KEY_ID=ak\n'
                                       'R2_SECRET_ACCESS_KEY=sk\nR2_BUCKET=stoarama\n')
        (self.d / 'r2prune.at').write_text('2026-11-25T10:00:00.5Z')
        (self.d / 'r2.json').write_text('{}')
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

    def flag(self, name, value=''):
        (self.d / ('r2prune.' + name)).write_text(value)

    def unflag(self, name):
        (self.d / ('r2prune.' + name)).unlink()

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

    def test_apply_writes_receipts_through_receipt_apply(self):
        self.delete()
        rows = [l.split('\t') for l in (self.W / 'receipt-backfill.tsv').read_text().splitlines()]
        self.assertEqual([(r[0], r[1], r[2], r[4], r[8], r[9], r[10], r[11], r[13]) for r in rows], [
            ('1', 'collated', '22', COL, '2026-11-25T10:00:00.5Z', 'r2prune_ledger_delete_collated_outside_window', 'none', self.digest, r'\N'),
            ('2', 'raw', '11', RAW, '2026-11-25T10:00:00.5Z', 'r2prune_ledger_delete_raw_outside_window', 'none', self.digest, r'\N')])
        policy = hashlib.sha256(b'{"inputs_sha256":{"picks":"p"},"recent_from":"2026-11-18T18:00:00Z"}').hexdigest()
        self.assertEqual({r[12] for r in rows}, {policy})
        self.assertEqual(self.calls(), ['stdin', 'dry-run.sql', 'r2prune', 'receipt-apply.sql'])
        calls = [json.loads(l) for l in (self.d / 'calls.jsonl').read_text().splitlines()]
        self.assertEqual([(c['psql'], c['ro']) for c in calls if 'psql' in c],
                         [('stdin', True), ('pin', True), ('dry-run.sql', True), ('receipt-apply.sql', False)])
        apply = calls[-1]
        self.assertEqual((apply['cwd'], apply['args'][apply['args'].index('total=2') - 1]), (str(self.W), '-v'))
        self.assertNotIn('secretpw', json.dumps(calls))
        self.assertTrue(self.result()['complete'])
        intent = json.loads((self.W / 'delete-intent.json').read_text())
        self.assertEqual((intent['mode'], intent['fresh_listing_sha256']), ('listing', ma.sha(self.listing)))
        self.assertEqual(self.delete(), [{'workdir': str(self.W), 'complete': True}])

    def test_key_without_catalog_identity_stops_before_delete(self):
        self.write_manifest([(RAW, 'delete_raw_outside_window'), ('experimental/stitching-confident-repairs/1/a.mp4', 'delete_owner_break_link')])
        with self.assertRaisesRegex(SystemExit, '1 manifest keys have no receipt identity'):
            self.delete()
        self.assertNotIn('r2prune', self.calls())
        self.assertFalse((self.W / 'delete-intent.json').exists())

    def test_catalog_identity_drift_stops_before_delete(self):
        self.sources({RAW: ('raw', 11, 'other-etag'), COL: ('collated', 22, None)})
        with self.assertRaisesRegex(SystemExit, 'no receipt identity'):
            self.delete()
        self.assertNotIn('r2prune', self.calls())

    def test_receipt_refusal_in_preflight_stops_before_delete(self):
        (self.d / 'dry-run.out').write_text('blocked|raw|trigger_nas_cleanup_source_hold|1|100|1\n')
        with self.assertRaisesRegex(SystemExit, 'would refuse'):
            self.delete()
        self.assertNotIn('r2prune', self.calls())
        self.assertFalse((self.W / 'delete-intent.json').exists())

    def test_failed_receipt_write_fails_closed_and_replays(self):
        (self.d / 'apply.rc').write_text('3')
        with self.assertRaisesRegex(SystemExit, 'receipt apply failed'):
            self.delete()
        self.assertFalse((self.W / 'receipt-result.json').exists())
        (self.d / 'apply.rc').write_text('0')
        self.run_cli('receipts', str(self.W))
        self.assertEqual(self.result()['deleted_receipts'], 2)

    def test_failed_outcome_keeps_the_intent_open_until_a_rerun_settles_it(self):
        self.flag('fail', json.dumps([RAW]))
        self.flag('rc', '1')
        with self.assertRaisesRegex(SystemExit, 'r2prune delete exited 1'):
            self.delete()
        self.assertEqual(set(self.applied()), {COL})
        self.assertEqual((self.result()['complete'], self.result()['unresolved']), (False, 1))
        self.unflag('fail')
        self.unflag('rc')
        (self.d / 'r2prune.at').write_text('2026-11-25T11:00:00Z')
        self.delete()
        self.assertEqual(self.applied(), {COL: '2026-11-25T10:00:00.5Z', RAW: '2026-11-25T11:00:00Z'})
        self.assertTrue(self.result()['complete'])

    def test_canary_then_full_keeps_the_first_acknowledgement(self):
        self.delete('--limit', '1')
        self.assertEqual((self.result()['complete'], self.result()['unresolved']), (False, 1))
        (self.d / 'r2prune.at').write_text('2026-11-25T11:00:00Z')
        self.delete()
        self.assertEqual(self.applied(), {RAW: '2026-11-25T10:00:00.5Z', COL: '2026-11-25T11:00:00Z'})
        self.assertEqual(len(self.calls('dry-run.sql')), 2)
        self.assertEqual(len(list(self.W.glob('ledger-*.jsonl'))), 2)

    def test_rerun_with_another_fresh_listing_is_refused(self):
        self.delete('--limit', '1')
        self.listing.write_text(f'{COL}\t100\te1\t1\n')  # RAW would read as absent_from_fresh_listing: final, no receipt
        with self.assertRaisesRegex(SystemExit, 'the open intent pins fresh listing'):
            self.delete()
        self.assertEqual(self.calls().count('r2prune'), 1)

    def test_ledger_run_against_another_listing_is_refused(self):
        self.delete('--limit', '1')
        ledger = next(self.W.glob('ledger-*.jsonl'))
        header, rest = ledger.read_text().split('\n', 1)
        ledger.write_text(json.dumps(dict(json.loads(header), fresh_listing_sha256='0' * 64)) + '\n' + rest)
        with self.assertRaisesRegex(SystemExit, 'another fresh listing'):
            self.run_cli('receipts', str(self.W))

    def test_half_written_last_ledger_line_is_unlogged(self):
        self.flag('partial')
        with self.assertRaisesRegex(SystemExit, '1 planned keys have no final ledger outcome'):
            self.delete()
        self.assertEqual((self.result()['deleted_receipts'], self.result()['unresolved']), (1, 1))
        self.unflag('partial')
        self.delete()
        self.assertTrue(self.result()['complete'])

    def test_unlogged_key_keeps_a_full_run_open(self):
        self.flag('drop')
        with self.assertRaisesRegex(SystemExit, '1 planned keys have no final ledger outcome'):
            self.delete()
        self.unflag('drop')
        self.delete()
        self.assertTrue(self.result()['complete'])

    def test_duplicate_key_in_one_ledger_is_refused(self):
        self.flag('dup')
        with self.assertRaisesRegex(SystemExit, 'duplicate or unexpected ledger row'):
            self.delete()
        self.assertEqual(self.calls('receipt-apply.sql'), [])

    def test_busy_lock_refuses_before_anything(self):
        with open(self.d / 'lock', 'a') as held:
            fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(SystemExit, 'is held'):
                self.delete()
        self.assertEqual(self.calls(), [])

    def test_wrapper_owns_the_ledger(self):
        for flag in (['--ledger', 'x.jsonl'], ['-ledger', 'x.jsonl'], ['-ledger=x.jsonl']):
            with self.assertRaisesRegex(SystemExit, 'owns --ledger'):
                self.delete(*flag)
        with self.assertRaisesRegex(SystemExit, 'exactly once'):
            self.delete('-manifest', str(self.manifest))
        self.assertEqual(self.calls(), [])

    def test_manifest_digest_must_match(self):
        self.digest = '0' * 64
        with self.assertRaisesRegex(SystemExit, 'differs from --expected-sha256'):
            self.delete()

    def test_receipt_sources_are_rechecked_on_reentry(self):
        self.delete('--limit', '1')
        (self.W / 'receipt-sources.json').write_text('{}\n')
        with self.assertRaisesRegex(SystemExit, 'receipt sources changed'):
            self.delete()
        self.assertEqual(self.calls().count('r2prune'), 1)

    def test_status_lists_open_intents(self):
        self.delete('--limit', '1')
        out = io.StringIO()
        with contextlib.redirect_stdout(out), self.assertRaisesRegex(SystemExit, '1 open delete intents'):
            ma.main(['status', str(self.root)])
        self.assertEqual(json.loads(out.getvalue())['workdir'], str(self.W))
        self.delete()
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            ma.main(['status', str(self.root)])
        self.assertEqual(json.loads(out.getvalue()), {'open': 0})


class Backfill(Base):
    def setUp(self):
        super().setUp()
        self.write_manifest([(KE[0], ma.KE_CLASS), (KE[1], ma.KE_CLASS)])
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
        self.write_manifest([(k, ma.KE_CLASS) for k in keys])
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
        self.write_manifest([(KE[0], ma.KE_CLASS)])
        with self.assertRaisesRegex(SystemExit, 'ledger header does not describe'):
            self.backfill()
        self.assertEqual(self.calls(), [])

    def test_receipt_refusal_stops_before_the_intent(self):
        (self.d / 'dry-run.out').write_text('blocked|raw|purged_at_differs|1|100|0\n')
        with self.assertRaisesRegex(SystemExit, 'would refuse'):
            self.backfill()
        self.assertEqual(self.calls(), ['stdin', 'dry-run.sql'])
        self.assertFalse((self.W / 'delete-intent.json').exists())


class ChunkVerify(Base):
    CLS, FLAG = ma.KE_CLASS, '--ke-coverage'

    def setUp(self):
        super().setUp()
        self.write_manifest([(k, self.CLS) for k in KE])
        self.sources({k: ('raw', 11 + i, 'e1') for i, k in enumerate(KE)})
        self.coverage = self.d / 'ke-coverage.tsv'
        self.coverage.write_text(f'{KE[0]}\t100\t{OUT1}\t500\n{KE[1]}\t100\t{OUT1}\t500\n{KE[2]}\t100\t{OUT2}\t600\n'
                                 f'{KE[2]}\t999\t{OUT1}\t500\n')
        self.r2({**{k: [100, 'e1', 1] for k in KE}, OUT1: [500, 'o1', 5], OUT2: [600, 'o2', 6]})
        # [rows, size, receipt, reservation, nonraw intent, collated, delivery hold size:etag, owner keep, NAS restore]
        self.db({'clips': {k: ['100', 'true'] for k in KE},
                 'outputs': {OUT1: ['1', '500', 'false', 'false', 'false', 'true', '500:o1', 'false', 'none'],
                             OUT2: ['1', '600', 'false', 'false', 'false', 'true', '', 'true', 'verified']},
                 'contains': {OUT1: KE[:2], OUT2: KE[2:]}})

    def r2(self, objects):
        (self.d / 'r2.json').write_text(json.dumps(objects))

    def db(self, db):
        (self.d / 'db.json').write_text(json.dumps(db))

    def delete(self, *wrapper, extra=()):
        return self.run_cli('delete', *wrapper, str(self.W), '--', '--manifest', str(self.manifest), '--expected-sha256', self.digest,
                            *self.rule_args(), self.FLAG, str(self.coverage), *extra)

    def listings(self):
        return [sorted(l.split('\t')[0] for l in p.read_text().splitlines()) for p in sorted(self.W.glob('listing-*.tsv'))]

    def test_each_chunk_is_verified_then_deleted_with_its_own_listing(self):
        self.delete('--chunk', '2')
        self.assertEqual(self.calls(), ['stdin', 'verify', 'dry-run.sql', 'r2prune', 'receipt-apply.sql',
                                        'verify', 'dry-run.sql', 'r2prune', 'receipt-apply.sql'])
        self.assertEqual(self.listings(), [sorted(KE[:2] + [OUT1]), sorted([KE[2], OUT2])])
        self.assertEqual(sorted(self.W.glob('listing-*.tsv'))[1].read_text(), f'{OUT2}\t600\to2\t6\n{KE[2]}\t100\te1\t1\n')
        for call in self.calls('r2prune'):
            self.assertEqual(call['r2prune'].count('--fresh-listing'), 1)
        self.assertEqual(set(self.applied()), set(KE))
        self.assertEqual([len(p.read_text().splitlines()) for p in sorted(self.W.glob('ledger-*.jsonl'))], [3, 2])
        self.assertTrue(self.result()['complete'])
        self.assertTrue(self.server.auth and all(self.server.auth))

    def test_a_cover_under_owner_keep_still_covers(self):
        # collation_v2_recovery_held keeps every cover of the 2026-11-25 canary: the protection a sole copy needs
        db = json.loads((self.d / 'db.json').read_text())
        owner = ['1', '500', 'false', 'false', 'false', 'true', '', 'true', 'none']
        self.db({**db, 'outputs': {OUT1: owner, OUT2: owner[:1] + ['600'] + owner[2:]}})
        self.delete('--chunk', '2')
        self.assertEqual(set(self.applied()), set(KE))
        self.assertTrue(self.result()['complete'])

    def test_keys_outside_the_chunk_are_deferred_not_final(self):
        self.delete('--chunk', '1', '--chunks', '1')
        self.assertEqual((self.result()['complete'], self.result()['deleted_receipts'], self.result()['unresolved']), (False, 1, 2))
        self.assertEqual(set(self.applied()), {KE[0]})
        self.delete('--chunk', '1')
        self.assertEqual(self.listings(), [sorted([KE[0], OUT1]), sorted([KE[1], OUT1]), sorted([KE[2], OUT2])])
        self.assertEqual(set(self.applied()), set(KE))
        self.assertTrue(self.result()['complete'])

    def test_a_failed_key_is_settled_first_by_the_next_chunk_even_once_gone(self):
        self.flag('fail', json.dumps([KE[0]]))
        self.flag('rc', '1')
        with self.assertRaisesRegex(SystemExit, 'r2prune delete exited 1'):
            self.delete('--chunk', '2')
        self.assertEqual(set(self.applied()), {KE[1]})
        self.unflag('fail')
        self.unflag('rc')
        objects = json.loads((self.d / 'r2.json').read_text())
        del objects[KE[0]]  # the failed DELETE did remove it
        self.r2(objects)
        (self.d / 'r2prune.at').write_text('2026-11-25T11:00:00Z')
        self.delete('--chunk', '2')
        self.assertEqual(self.listings()[1], sorted([KE[0], KE[2], OUT1, OUT2]))
        self.assertEqual(self.applied(), {KE[1]: '2026-11-25T10:00:00.5Z', KE[0]: '2026-11-25T11:00:00Z', KE[2]: '2026-11-25T11:00:00Z'})
        self.assertTrue(self.result()['complete'])

    def test_first_mismatch_stops_before_that_chunks_r2prune(self):
        def other_intent():
            other = self.root / 'other'
            other.mkdir(parents=True)
            (other / 'm.jsonl').write_text(json.dumps({'key': OUT2}) + '\n')
            (other / 'delete-intent.json').write_text(json.dumps({'manifest': str(other / 'm.jsonl'), 'manifest_sha256': SHA}))
        objects = json.loads((self.d / 'r2.json').read_text())
        db = json.loads((self.d / 'db.json').read_text())
        cases = {
            'raw etag': (lambda: self.r2({**objects, KE[2]: [100, 'e2', 1]}), 'R2 HEAD of ' + KE[2]),
            'raw absent, never sent': (lambda: self.r2({k: v for k, v in objects.items() if k != KE[2]}), 'R2 HEAD of ' + KE[2]),
            'output size': (lambda: self.r2({**objects, OUT2: [601, 'o2', 6]}), 'covering output ' + OUT2),
            'raw owner keep': (lambda: self.db({**db, 'kept': [KE[2]]}), 'recording_clips row for ' + KE[2]),
            'clip purged': (lambda: self.db({**db, 'clips': {**db['clips'], KE[2]: ['100', 'false']}}), 'recording_clips row for ' + KE[2]),
            'clip size': (lambda: self.db({**db, 'clips': {**db['clips'], KE[2]: ['101', 'true']}}), 'recording_clips row for ' + KE[2]),
            'output receipted': (lambda: out2(2, 'true'), OUT2),
            'output reserved': (lambda: out2(3, 'true'), OUT2),
            'output nonraw intent': (lambda: out2(4, 'true'), OUT2),
            'output catalog size': (lambda: out2(1, '599'), OUT2),
            'output two catalog rows': (lambda: out2(0, '2'), OUT2),
            'output hour not collated': (lambda: out2(5, 'false'), OUT2),
            'output unheld': (lambda: out2(7, 'false'), OUT2),
            'output held at another size': (lambda: self.db({**db, 'outputs': {**db['outputs'], OUT2: db['outputs'][OUT2][:6] + ['601:o2', 'false', 'none']}}), OUT2),
            'output held at another etag': (lambda: self.db({**db, 'outputs': {**db['outputs'], OUT2: db['outputs'][OUT2][:6] + ['600:x', 'false', 'none']}}), OUT2),
            'output restore unverified': (lambda: out2(8, 'unverified'), OUT2),
            'clip left the output or its span': (lambda: self.db({**db, 'contains': {OUT1: KE[:2], OUT2: []}}), 'source_clip_ids of ' + OUT2),
            'output in another open intent': (other_intent, "another open intent's manifest"),
        }
        def out2(i, value):
            row = list(db['outputs'][OUT2])
            row[i] = value
            self.db({**db, 'outputs': {**db['outputs'], OUT2: row}})
        for name, (break_it, message) in cases.items():
            with self.subTest(name):
                self.tearDown()
                self.setUp()
                objects = json.loads((self.d / 'r2.json').read_text())
                db = json.loads((self.d / 'db.json').read_text())
                self.delete('--chunk', '2', '--chunks', '1')
                break_it()
                with self.assertRaisesRegex(SystemExit, 'verify: .*' + message.replace('.', r'\.')):
                    self.delete('--chunk', '2')
                self.assertEqual(self.calls().count('r2prune'), 1)
                self.assertEqual(len(self.listings()), 1)
                self.assertFalse(self.result()['complete'])

    def test_wrapper_owns_the_fresh_listing_for_covered_classes(self):
        for extra in (['--fresh-listing', 'f.tsv'], ['--limit', '1']):
            with self.assertRaisesRegex(SystemExit, 'wrapper owns --fresh-listing'):
                self.delete(extra=extra)
        for other in ('delete_raw_outside_window', ma.KE_CLASS if self.CLS == ma.BEST14_CLASS else ma.BEST14_CLASS):
            self.write_manifest([(KE[0], self.CLS), (RAW, other)])
            with self.assertRaisesRegex(SystemExit, 'only that class'):
                self.delete()
        self.assertEqual(self.calls(), [])

    def test_the_coverage_flag_must_be_the_classs_own(self):
        other = '--best14-coverage' if self.FLAG == '--ke-coverage' else '--ke-coverage'
        with self.assertRaisesRegex(SystemExit, 'pass ' + self.FLAG + ' exactly once'):
            self.run_cli('delete', str(self.W), '--', '--manifest', str(self.manifest), '--expected-sha256', self.digest,
                         *self.rule_args(), other, str(self.coverage))
        self.assertNotIn('r2prune', self.calls())


class Best14ChunkVerify(ChunkVerify):
    """Class (b), best-14 raws inside a served stitched output: the same verify, its own coverage flag."""
    CLS, FLAG = ma.BEST14_CLASS, '--best14-coverage'


class Gate(Best14ChunkVerify):
    """The served-generation gate: before every chunk, from the generation pinned now."""

    def test_the_pin_is_read_before_every_chunk(self):
        self.delete('--chunk', '2')
        kinds = [c['psql'] for c in self.calls('pin')] + [c.get('psql', 'r2prune') for c in self.calls('r2prune')]
        self.assertEqual(kinds.count('pin'), 2)
        self.assertTrue(self.result()['complete'])

    def test_a_generation_swap_mid_run_is_caught_before_the_next_chunk(self):
        self.delete('--chunk', '2', '--chunks', '1')
        self.serve('c' * 32, [SERVED, 'joined/recovery-best14nas-20261126/objects/new.mp4'])
        with self.assertRaisesRegex(SystemExit, 'gate: 1 keys served by c{32} have no page_link row'):
            self.delete('--chunk', '2')
        self.assertEqual(self.calls().count('r2prune'), 1)

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
                    self.delete('--chunk', '2')
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
