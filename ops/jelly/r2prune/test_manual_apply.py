# /usr/bin/python3 -I ops/jelly/r2prune/test_manual_apply.py -q
# Offline: stub psql and r2prune executables record their calls; nothing reaches a DB or R2.
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('manual_apply', HERE / 'manual_apply.py')
ma = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ma)

SHA = 'a' * 64
RAW = 'managed/acct-47/recordings/1/continuous/1.mp4'
COL = 'joined/nightly-collation-v2/objects/x.mp4'

# psql stub: answers the catalog lookup from sources.json, the dry run from dry-run.out,
# and the receipt apply with apply.rc; every call is appended to calls.jsonl.
PSQL = r'''#!/usr/bin/env python3
import json, os, sys
S = os.environ['STUB']
script = sys.stdin.read()
args = sys.argv[1:]
f = args[args.index('-f') + 1] if '-f' in args else ''
with open(S + '/calls.jsonl', 'a') as c:
    c.write(json.dumps({'psql': os.path.basename(f) or 'stdin', 'args': args, 'cwd': os.getcwd(),
                        'ro': os.environ['PGOPTIONS'].endswith('=on')}) + '\n')
if f.endswith('receipt-apply.sql'):
    rc = int(open(S + '/apply.rc').read()) if os.path.exists(S + '/apply.rc') else 0
    if rc:
        sys.stderr.write('ERROR:  reservation or hold at TSV row 1\n')
    sys.exit(rc)
if f.endswith('dry-run.sql'):
    sys.stdout.write(open(S + '/dry-run.out').read() if os.path.exists(S + '/dry-run.out') else 'insert|raw||1|1|1\n')
    sys.exit(0)
for key, s in json.load(open(S + '/sources.json')).items():
    if key in script:
        print('\t'.join(map(str, (key, s['kind'], s['source_id'], s['stream_id'], s['size'], s['sha256'], s['etag'] or ''))))
'''

# r2prune stub: writes a ledger (header + one 'deleted' row per manifest key, or the first
# --limit keys) at a fixed time from r2prune.at, then exits with r2prune.rc.
R2PRUNE = r'''#!/usr/bin/env python3
import json, os, sys
S = os.environ['STUB']
a = sys.argv[1:]
opt = lambda n, d=None: a[a.index(n) + 1] if n in a else d
with open(S + '/calls.jsonl', 'a') as c:
    c.write(json.dumps({'r2prune': a}) + '\n')
keys = [json.loads(l) for l in open(opt('--manifest'))]
limit = int(opt('--limit', 0))
at = open(S + '/r2prune.at').read().strip()
with open(opt('--ledger'), 'w') as f:
    f.write(json.dumps({'type': 'run', 'manifest_sha256': opt('--expected-sha256'), 'inputs_sha256': {'picks': 'p'},
                        'recent_from': '2026-11-18T18:00:00Z', 'limit': limit, 'started_at': at}) + '\n')
    for k in keys[:limit or None][:-1 if os.path.exists(S + '/r2prune.drop') else None]:
        f.write(json.dumps({'key': k['key'], 'size': k['size'], 'outcome': 'deleted', 'at': at}) + '\n')
sys.exit(int(open(S + '/r2prune.rc').read()) if os.path.exists(S + '/r2prune.rc') else 0)
'''


class ManualApply(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.d = Path(self.tmp.name).resolve()
        os.environ['STUB'] = str(self.d)
        for name, body in (('psql', PSQL), ('r2prune', R2PRUNE)):
            p = self.d / name
            p.write_text(body.replace('#!/usr/bin/env python3', '#!' + sys.executable + ' -I'))
            p.chmod(0o755)
        (self.d / 'db.env').write_text('DATABASE_URL=postgres://u:secretpw@db.invalid:5432/stoarama?sslmode=require\n')
        (self.d / 'r2.env').write_text('R2_BUCKET=stoarama\n')
        (self.d / 'r2prune.at').write_text('2026-11-25T10:00:00.5Z')
        self.manifest = self.d / 'delete-manifest.jsonl'
        self.write_manifest([(RAW, 'delete_raw_outside_window'), (COL, 'delete_collated_outside_window')])
        self.sources({RAW: ('raw', 11, 'e1'), COL: ('collated', 22, None)})
        self.W = self.d / 'work'

    def tearDown(self):
        self.tmp.cleanup()

    def write_manifest(self, rows):
        self.manifest.write_text(''.join(json.dumps({'key': k, 'size': 100, 'etag': '"e1"', 'mtime': 1, 'class': c,
                                                     'reason': 'outside_20d_window'}) + '\n' for k, c in rows))
        self.digest = hashlib.sha256(self.manifest.read_bytes()).hexdigest()

    def sources(self, rows):
        (self.d / 'sources.json').write_text(json.dumps({k: {'kind': kind, 'source_id': sid, 'stream_id': 7, 'size': 100,
                                                             'sha256': SHA, 'etag': etag} for k, (kind, sid, etag) in rows.items()}))

    def run_cli(self, *args):
        base = ['--psql', str(self.d / 'psql'), '--r2prune', str(self.d / 'r2prune'), '--lock', str(self.d / 'lock'),
                '--db-env', str(self.d / 'db.env'), '--r2-env', str(self.d / 'r2.env')]
        ma.main(base + list(args))

    def delete(self, *extra):
        self.run_cli('delete', str(self.W), '--', '--manifest', str(self.manifest), '--expected-sha256', self.digest,
                     '--summary', 's.json', '--fresh-listing', 'f.tsv', *extra)

    def calls(self):
        p = self.d / 'calls.jsonl'
        return [json.loads(l) for l in p.read_text().splitlines()] if p.exists() else []

    def receipts(self):
        return [l.split('\t') for l in (self.W / 'receipt-backfill.tsv').read_text().splitlines()]

    def test_apply_writes_receipts_through_receipt_apply(self):
        self.delete()
        rows = self.receipts()
        self.assertEqual([(r[0], r[1], r[2], r[4], r[8], r[9], r[10], r[11], r[13]) for r in rows], [
            ('1', 'collated', '22', COL, '2026-11-25T10:00:00.5Z', 'r2prune_ledger_delete_collated_outside_window', 'none', self.digest, r'\N'),
            ('2', 'raw', '11', RAW, '2026-11-25T10:00:00.5Z', 'r2prune_ledger_delete_raw_outside_window', 'none', self.digest, r'\N')])
        policy = hashlib.sha256(b'{"inputs_sha256":{"picks":"p"},"recent_from":"2026-11-18T18:00:00Z"}').hexdigest()
        self.assertEqual({r[12] for r in rows}, {policy})
        calls = self.calls()
        self.assertEqual([c.get('psql', 'r2prune') for c in calls], ['stdin', 'dry-run.sql', 'r2prune', 'receipt-apply.sql'])
        self.assertEqual([c['ro'] for c in calls if 'psql' in c], [True, True, False])
        apply = calls[-1]
        self.assertEqual((apply['cwd'], apply['args'][apply['args'].index('total=2') - 1]), (str(self.W), '-v'))
        self.assertNotIn('secretpw', json.dumps(calls))
        self.assertTrue(json.loads((self.W / 'receipt-result.json').read_text())['complete'])

    def test_key_without_catalog_identity_stops_before_delete(self):
        repair = 'experimental/stitching-confident-repairs/1/a.mp4'
        self.write_manifest([(RAW, 'delete_raw_outside_window'), (repair, 'delete_owner_break_link')])
        with self.assertRaisesRegex(SystemExit, '1 manifest keys have no receipt identity'):
            self.delete()
        self.assertFalse(any('r2prune' in c for c in self.calls()))
        self.assertFalse((self.W / 'delete-intent.json').exists())

    def test_catalog_identity_drift_stops_before_delete(self):
        self.sources({RAW: ('raw', 11, 'other-etag'), COL: ('collated', 22, None)})
        with self.assertRaisesRegex(SystemExit, 'no receipt identity'):
            self.delete()
        self.assertFalse(any('r2prune' in c for c in self.calls()))

    def test_receipt_refusal_in_preflight_stops_before_delete(self):
        (self.d / 'dry-run.out').write_text('blocked|raw|trigger_nas_cleanup_source_hold|1|100|1\n')
        with self.assertRaisesRegex(SystemExit, 'would refuse'):
            self.delete()
        self.assertFalse(any('r2prune' in c for c in self.calls()))

    def test_failed_receipt_write_fails_closed_and_replays(self):
        (self.d / 'apply.rc').write_text('3')
        with self.assertRaisesRegex(SystemExit, 'receipt apply failed'):
            self.delete()
        self.assertFalse((self.W / 'receipt-result.json').exists())
        (self.d / 'apply.rc').write_text('0')
        self.run_cli('receipts', str(self.W))
        self.assertEqual(json.loads((self.W / 'receipt-result.json').read_text())['deleted_receipts'], 2)

    def test_failed_delete_still_receipts_what_it_deleted(self):
        (self.d / 'r2prune.rc').write_text('1')
        with self.assertRaisesRegex(SystemExit, 'r2prune delete exited 1'):
            self.delete('--limit', '1')
        self.assertEqual([r[4] for r in self.receipts()], [RAW])

    def test_canary_then_full_keeps_the_first_acknowledgement(self):
        self.delete('--limit', '1')
        (self.d / 'r2prune.at').write_text('2026-11-25T11:00:00Z')
        self.delete()
        self.assertEqual({r[4]: r[8] for r in self.receipts()}, {RAW: '2026-11-25T10:00:00.5Z', COL: '2026-11-25T11:00:00Z'})
        preflights = [c for c in self.calls() if c.get('psql') == 'dry-run.sql']
        self.assertEqual(len(preflights), 2)
        self.assertEqual(len(list(self.W.glob('ledger-*.jsonl'))), 2)

    def test_wrapper_owns_the_ledger(self):
        for flag in (['--ledger', 'x.jsonl'], ['-ledger', 'x.jsonl'], ['-ledger=x.jsonl']):
            with self.assertRaisesRegex(SystemExit, 'owns --ledger'):
                self.delete(*flag)
        with self.assertRaisesRegex(SystemExit, 'exactly once'):
            self.delete('-manifest', str(self.manifest))
        self.assertFalse(any('r2prune' in c for c in self.calls()))

    def test_full_run_with_unlogged_keys_stays_open(self):
        (self.d / 'r2prune.drop').write_text('')
        with self.assertRaisesRegex(SystemExit, '1 planned keys have no final ledger outcome'):
            self.delete()
        result = json.loads((self.W / 'receipt-result.json').read_text())
        self.assertEqual((result['complete'], result['deleted_receipts'], result['unresolved']), (False, 1, 1))
        (self.d / 'r2prune.drop').unlink()
        self.delete()
        self.assertTrue(json.loads((self.W / 'receipt-result.json').read_text())['complete'])

    def test_canary_leaves_the_intent_open_without_failing(self):
        self.delete('--limit', '1')
        result = json.loads((self.W / 'receipt-result.json').read_text())
        self.assertEqual((result['complete'], result['unresolved']), (False, 1))

    def test_manifest_digest_must_match(self):
        self.digest = '0' * 64
        with self.assertRaisesRegex(SystemExit, 'differs from --expected-sha256'):
            self.delete()


if __name__ == '__main__':
    unittest.main()
