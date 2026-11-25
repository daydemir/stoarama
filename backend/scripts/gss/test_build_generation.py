import gzip
import hashlib
import json
import math
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import build_generation as b  # noqa: E402

BACKEND = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
PARENT_GEN = 'c4049f6dc0f940be8b7eb8b9bef6f42f'
GEN = 'aa' * 16
NOW = '2026-11-25T08:28:20.642065Z'
DAYS = ('2026-07-23', '2026-07-24')
RIDS = list(range(301, 350))  # 49 recordings, the validator's minimum


def assembly_id(rid):
    return 'sha256:' + hashlib.sha256(str(rid).encode()).hexdigest()


def hold_kind(path):
    return path.split('/')[2].split('?')[0] if path.count('/') > 1 else path.strip('/')


class Fixture:
    """A small, internally consistent parent generation: 49 recordings, 2 days x 2 hours,
    two originals per hour, and one assembly replacing the first hour's originals."""

    def __init__(self, root):
        self.root = root
        self.assets_dir = os.path.join(root, 'assets')
        assets, files, objects = {}, {}, {}
        md_streams, readiness, counts, cat_streams, repairs = [], {}, {}, [], []
        for rid in RIDS:
            outputs, days, ranges, paths = [], [], {}, {}
            for d, date in enumerate(DAYS):
                hours = []
                for h in range(2):
                    t0 = 1784786400.0 + (d * 2 + h) * 3600
                    parts = []
                    for k in range(2):
                        oid = rid * 100 + d * 4 + h * 2 + k
                        dur = 1800.0 if k == 0 else 1700.0
                        parts.append([oid, k + 1, dur, k * 1800.0, k * 1800.0 + dur, dur, dur, 0, f'{oid}.mp4', 1, None])
                        ranges[str(oid)] = {'ranges': [[t0 + k * 1800.0, t0 + k * 1800.0 + dur]]}
                    hours.append(['collated', 3600.0, 3500.0, 0.0, 60, 3500.0, 0, t0, t0 + 3600, parts, [], 1])
                    for p in parts:
                        if not (d == 0 and h == 0):
                            outputs.append(self.output(rid, p[0], date))
                days.append({'date': date, 'grade': 'A', 'hours': hours})
            inputs = [rid * 100, rid * 100 + 1]
            asm = dict(self.output(rid, assembly_id(rid), DAYS[0]), replacement_inputs=inputs)
            outputs.insert(0, asm)
            repairs.append({'sha256': asm['sha256'], 'recording_id': rid, 'local_date': DAYS[0], 'seconds': 3500.0,
                            'input_ids': inputs, 'production_version': 85})
            for o in outputs:
                files[str(o['id'])] = {'etag': '"e"', 'key': f'joined/g/objects/{o["sha256"]}.mp4', 'path': o['path'],
                                       'recording_id': rid, 'sha256': o['sha256'], 'size_bytes': o['size_bytes'], 'verified': True}
                paths[str(o['id'])] = o['path']
            man = {'schema': 2, 'recording_id': rid, 'original_count': 8, 'replaced_original_count': 2,
                   'replacement_count': 1, 'outputs': outputs}
            stream = {'id': rid, 'days': days, 'current_download_generation': PARENT_GEN, 'current_download_paths': paths}
            rng = {'id': rid, 'by_output_id': ranges}
            objects[f'/public/manifests/{rid}.json'] = man
            objects[f'/api/coverage-delivery/manifest?stream={rid}&scope=available'] = man
            objects[f'/public/streams/{rid}.json'] = stream
            objects[f'/public/streams/{rid}.json.gz'] = stream
            objects[f'/public/ranges/{rid}.json'] = rng
            objects[f'/public/ranges/{rid}.json.gz'] = rng
            md_streams.append({'id': rid, 'timezone': 'Europe/Warsaw', 'source_hours': 4.0, 'excluded_hours': 0.0,
                               'captured_scheduled_seconds': 14400.0, 'registered_hours': 4, 'missing_registration_hours': 0,
                               'state_counts': {'nas_parts': 0, 'registered': 4, 'parts': 8, 'multi': 4, 'collated': 4},
                               'largest_gap_seconds': 100.0, 'current_download_generation': PARENT_GEN, 'sha256': ''})
            readiness[str(rid)] = {'recording_id': rid, 'scheduled_slots': 4, 'manifest_present_slots': 4,
                                   'captured_scheduled_seconds': 14400.0, 'length_thresholds': {},
                                   'captured_without_registered_output_union_seconds': 0.0,
                                   'delivery_state': 'all_scheduled_manifests_present'}
            counts[str(rid)] = {'files': 7}
            cat_streams.append({'id': rid, 'available_start': DAYS[0],
                                'weekly_exports': {'available': {'weeks': [{'number': 1, 'start': DAYS[0], 'end': DAYS[1]}]}}})
        summary = {'scheduled_hours': 4 * len(RIDS), 'nas_receipted_parts': 0}
        objects['/public/metadata.json'] = {'streams': md_streams, 'summary': dict(summary)}
        objects['/public/readiness.json'] = {'by_recording_id': readiness, 'summary': dict(summary)}
        objects['/public/growth.json'] = {'repairs': repairs, 'current_stream_counts': counts, 'generation': PARENT_GEN,
                                          'production_model_counts': {'85': len(RIDS), 'prior_verified_assemblies': 0},
                                          'published_at': 'p', 'updated_at': 'p', 'current_file_count': len(files)}
        objects['/public/catalog.json'] = {'streams': cat_streams, 'verification': {'available': len(files)}}
        objects['/public/images.json'] = {'images': [f'/public/images/{RIDS[0]}.jpg']}
        bodies = {p: self.encode(p, o) for p, o in objects.items()}
        bodies[f'/public/images/{RIDS[0]}.jpg'] = b'\xff\xd8jpeg'
        bodies['/download-script.py'] = b'print("download")\n'
        self.holds = []
        for p, body in bodies.items():
            sha = hashlib.sha256(body).hexdigest()
            name = 'manifest' if p.startswith('/api/') else p.rsplit('/', 1)[1]
            key = b.ASSET_PREFIX + sha + '/' + name
            local = os.path.join(self.assets_dir, key)
            os.makedirs(os.path.dirname(local), exist_ok=True)
            with open(local, 'wb') as f:
                f.write(body)
            content_type = 'application/gzip' if p.endswith('.gz') else 'image/jpeg' if p.endswith('.jpg') else 'text/x-python' if p.endswith('.py') else 'application/json'
            assets[p] = {'content_type': content_type, 'etag': '"' + hashlib.md5(body).hexdigest() + '"', 'key': key,
                         'sha256': sha, 'size_bytes': len(body), 'verified': True}
            rid = next((r for r in RIDS if f'/{r}.' in p or f'stream={r}&' in p), None)
            self.holds.append({'object_key': key, 'kind': hold_kind(p), 'recording_id': rid, 'relative_path': f'rel/{name}'})
        self.parent = {'account_id': 47, 'assets': assets, 'created_at': 'c', 'files': files, 'generation': 'cb' * 16,
                       'parent_generation': PARENT_GEN, 'public_delivery': True, 'publication_note': 'parent',
                       'recording_ids': RIDS, 'schema_version': 1, 'selection_scope': 'available'}
        self.registry_path = os.path.join(root, 'parent.json.gz')
        with open(self.registry_path, 'wb') as f:
            f.write(gzip.compress(b.dumps(self.parent)))
        self.holds_path = os.path.join(root, 'holds.json')
        with open(self.holds_path, 'w') as f:
            json.dump(self.holds, f)

    @staticmethod
    def output(rid, oid, date):
        sha = oid[7:] if str(oid).startswith('sha256:') else hashlib.sha256(str(oid).encode()).hexdigest()
        return {'id': oid, 'sha256': sha, 'size_bytes': 1000, 'recording_id': rid, 'local_date': date,
                'available': True, 'path': f'{rid}/{date}/{str(oid).replace(":", "_")}.mp4'}

    @staticmethod
    def encode(path, o):
        body = b.dumps(o)
        return gzip.compress(body, mtime=0) if path.endswith('.gz') else body

    def build(self, present, out):
        store = b.AssetStore(self.parent['assets'], self.assets_dir)
        return b.build(self.parent, store, present, self.holds, GEN, NOW, 'note', out)


class SyntheticBuildTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.fx = Fixture(self.tmp)
        self.store = b.AssetStore(self.fx.parent['assets'], self.fx.assets_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def out(self, name):
        return os.path.join(self.tmp, name)

    def object(self, out, reg, path):
        o = reg['assets'][path]
        local = os.path.join(out, 'objects', o['key'])
        body = Path(local).read_bytes() if os.path.exists(local) else Path(os.path.join(self.fx.assets_dir, o['key'])).read_bytes()
        return json.loads(gzip.decompress(body) if path.endswith('.gz') else body)

    def test_present_file_selection(self):
        gone, empty = RIDS[0], RIDS[1]
        drop = {assembly_id(gone)} | {k for k, v in self.fx.parent['files'].items() if v['recording_id'] == empty}
        present = b.select_files(self.fx.parent, [k for k in self.fx.parent['files'] if k not in drop])
        out = self.out('gen')
        summary = self.fx.build(present, out)
        reg = b.read_registry(os.path.join(out, 'registry.json.gz'))
        self.assertEqual(set(reg['files']), present)
        self.assertEqual((reg['generation'], reg['parent_generation'], reg['created_at']), (GEN, 'cb' * 16, NOW))
        self.assertEqual(summary['changed_recordings'], [gone, empty])

        # Dropping an assembly drops its replaced originals; the hour empties to missing_output.
        stream = self.object(out, reg, f'/public/streams/{gone}.json')
        self.assertEqual(stream, self.object(out, reg, f'/public/streams/{gone}.json.gz'))
        first = stream['days'][0]['hours'][0]
        self.assertEqual((first[0], first[9], first[2]), ('missing_output', [], 0))
        self.assertEqual(stream['current_download_generation'], GEN)
        self.assertEqual(set(stream['current_download_paths']), {k for k in present if reg['files'][k]['recording_id'] == gone})
        man = self.object(out, reg, f'/public/manifests/{gone}.json')
        self.assertEqual(man, self.object(out, reg, f'/api/coverage-delivery/manifest?stream={gone}&scope=available'))
        self.assertEqual((man['original_count'], man['replacement_count'], len(man['outputs'])), (6, 0, 6))

        # A kept assembly keeps its replaced originals on the timeline.
        kept = self.object(out, reg, f'/public/streams/{RIDS[2]}.json')
        self.assertEqual(reg['assets'][f'/public/streams/{RIDS[2]}.json'], self.fx.parent['assets'][f'/public/streams/{RIDS[2]}.json'])
        self.assertEqual([p[0] for p in kept['days'][0]['hours'][0][9]], [RIDS[2] * 100, RIDS[2] * 100 + 1])

        # A zero-file recording uses the page's disabled state.
        md = {s['id']: s for s in self.object(out, reg, '/public/metadata.json')['streams']}
        rd = self.object(out, reg, '/public/readiness.json')['by_recording_id']
        self.assertEqual((md[empty]['parts'], md[empty]['registered_hours'], md[empty]['missing_registration_hours']), (0, 0, 4))
        self.assertEqual(rd[str(empty)]['delivery_state'], 'awaiting_verified_outputs')
        self.assertEqual(rd[str(gone)]['delivery_state'], 'partial_registered_delivery')
        self.assertEqual(md[empty]['current_download_generation'], GEN)
        self.assertEqual(md[empty]['largest_gap_seconds'], 100.0)  # known stale, carried over
        self.assertIn('metadata.json streams[].largest_gap_seconds', summary['known_stale_fields'])

        gr = self.object(out, reg, '/public/growth.json')
        self.assertEqual((gr['generation'], gr['published_at'], gr['current_file_count']), (GEN, NOW, len(present)))
        self.assertEqual(gr['production_model_counts'], {'85': len(RIDS) - 2, 'prior_verified_assemblies': 0})
        cat = {s['id']: s for s in self.object(out, reg, '/public/catalog.json')['streams']}
        self.assertEqual((cat[empty]['available_parts'], cat[empty]['weekly_exports']['available']['weeks'][0]['sample_path']), (0, None))

        # Untouched assets keep their objects.
        for p in ('/download-script.py', '/public/images.json', f'/public/images/{RIDS[0]}.jpg'):
            self.assertEqual(reg['assets'][p], self.fx.parent['assets'][p])
        self.check_objects(out, reg)

    def check_objects(self, out, reg):
        uploads = json.loads(Path(os.path.join(out, 'uploads.json')).read_text())
        holds = json.loads(Path(os.path.join(out, 'holds.json')).read_text())
        parent_holds = {h['object_key']: h for h in self.fx.holds}
        by_key = {o['key']: (p, o) for p, o in reg['assets'].items()}
        self.assertEqual([u['key'] for u in uploads], [h['object_key'] for h in holds])
        self.assertEqual({u['key'] for u in uploads}, {o['key'] for p, o in reg['assets'].items() if o != self.fx.parent['assets'][p]})
        for u, h in zip(uploads, holds):
            body = Path(os.path.join(out, u['local_path'])).read_bytes()
            path, o = by_key[u['key']]
            self.assertEqual(u['key'], b.ASSET_PREFIX + hashlib.sha256(body).hexdigest() + '/' + u['key'].rsplit('/', 1)[1])
            self.assertEqual((u['sha256'], u['size_bytes'], u['etag'], u['content_type']),
                             (o['sha256'], len(body), hashlib.md5(body).hexdigest(), self.fx.parent['assets'][path]['content_type']))
            self.assertEqual(o['etag'], '"' + u['etag'] + '"')
            old = parent_holds[self.fx.parent['assets'][path]['key']]
            self.assertEqual(h, {'bucket': 'stoarama', 'object_key': u['key'], 'etag': u['etag'], 'size_bytes': len(body),
                                 'recording_id': old['recording_id'], 'kind': old['kind'], 'sha256': u['sha256'],
                                 'relative_path': old['relative_path']})

    def test_day_selection(self):
        rid = RIDS[3]
        days = {(r, d) for r in RIDS for d in DAYS if r != rid} | {(rid, DAYS[1])}
        present = b.select_days(self.fx.parent, self.store, days)
        expected = {k for k, v in self.fx.parent['files'].items() if v['recording_id'] != rid}
        expected |= {o['id'] for o in self.store.load(f'/public/manifests/{rid}.json')['outputs'] if o['local_date'] == DAYS[1]}
        self.assertEqual(present, {str(x) for x in expected})
        out = self.out('days')
        self.assertEqual(self.fx.build(present, out)['changed_recordings'], [rid])
        reg = b.read_registry(os.path.join(out, 'registry.json.gz'))
        stream = self.object(out, reg, f'/public/streams/{rid}.json')
        self.assertEqual([h[0] for h in stream['days'][0]['hours']], ['missing_output', 'missing_output'])
        self.assertEqual([h[0] for h in stream['days'][1]['hours']], ['collated', 'collated'])
        man = self.object(out, reg, f'/public/manifests/{rid}.json')
        self.assertEqual({o['local_date'] for o in man['outputs']}, {DAYS[1]})
        self.check_objects(out, reg)

        with self.assertRaisesRegex(SystemExit, 'not on its timeline'):
            b.select_days(self.fx.parent, self.store, {(rid, '2026-08-01')})
        with self.assertRaisesRegex(SystemExit, 'not in the parent registry'):
            b.select_days(self.fx.parent, self.store, {(999, DAYS[0])})

    def test_fails_fast(self):
        with self.assertRaisesRegex(SystemExit, 'not in the parent registry'):
            b.select_files(self.fx.parent, ['nope'])
        present = set(self.fx.parent['files']) - {assembly_id(RIDS[0])}
        self.fx.holds = [h for h in self.fx.holds if '/growth.json' not in h['object_key']]
        with self.assertRaisesRegex(SystemExit, 'no delivery_keep_holds row'):
            self.fx.build(present, self.out('noholds'))
        os.makedirs(self.out('exists'))
        with self.assertRaises(FileExistsError):
            self.fx.build(present, self.out('exists'))

    def test_cli_and_server_validator(self):
        days = os.path.join(self.tmp, 'days.tsv')
        with open(days, 'w') as f:
            f.write('recording_id\tstream_id\tlocal_date\n')
            f.writelines(f'{r}\t1\t{DAYS[0]}\n' for r in RIDS[:-1])
        out = self.out('cli')
        subprocess.run([sys.executable, '-I', os.path.join(BACKEND, 'scripts', 'gss', 'build_generation.py'),
                        '--parent', self.fx.registry_path, '--assets', self.fx.assets_dir, '--parent-holds', self.fx.holds_path,
                        '--days', days, '--note', 'cli', '--out', out], check=True, stdout=subprocess.DEVNULL)
        reg = b.read_registry(os.path.join(out, 'registry.json.gz'))
        self.assertEqual(len(bytes.fromhex(reg['generation'])), 16)
        self.assertEqual({v['recording_id'] for v in reg['files'].values()}, set(RIDS[:-1]))
        self.check_objects(out, reg)
        go_validate(self, os.path.join(out, 'registry.json.gz'))


def go_validate(test, registry):
    env = dict(os.environ, STOARAMA_GSS_BUILT_REGISTRY=registry)
    r = subprocess.run(['go', 'test', './internal/api', '-run', '^TestGlobalDeliveryBuiltRegistry$', '-count=1', '-v'],
                       cwd=BACKEND, env=env, capture_output=True, text=True)
    test.assertEqual(r.returncode, 0, r.stdout + r.stderr)
    test.assertIn('--- PASS: TestGlobalDeliveryBuiltRegistry', r.stdout)


def close(a, c, path=''):
    """Structural equality with float tolerance; returns the differing paths."""
    if isinstance(a, float) or isinstance(c, float):
        return [] if math.isclose(a, c, rel_tol=1e-9, abs_tol=1e-6) else [f'{path}: {a} != {c}']
    if isinstance(a, dict) and isinstance(c, dict):
        if set(a) != set(c):
            return [f'{path}: keys {set(a) ^ set(c)}']
        return [d for k in a for d in close(a[k], c[k], f'{path}.{k}')]
    if isinstance(a, list) and isinstance(c, list):
        if len(a) != len(c):
            return [f'{path}: len {len(a)} != {len(c)}']
        return [d for i, (x, y) in enumerate(zip(a, c)) for d in close(x, y, f'{path}[{i}]')]
    return [] if a == c else [f'{path}: {str(a)[:80]} != {str(c)[:80]}']


REGRESSION = os.environ.get('STOARAMA_GSS_REGRESSION_DIR')


@unittest.skipUnless(REGRESSION, 'set STOARAMA_GSS_REGRESSION_DIR to the Nov 25 GSS build inputs and outputs')
class RegressionTests(unittest.TestCase):
    """Generation 782959ab345c5e3dce666d42ac93ecf6 (live Nov 25) was built by hand from
    cb33e28c (gen.json.gz) with its present files. The directory holds gen.json.gz,
    gen2.json.gz, current.json (c4049f6d), assets/ and gen2/objects/."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp)

    @staticmethod
    def holds(parent):
        return [{'object_key': o['key'], 'kind': 'asset', 'recording_id': None, 'relative_path': ''} for o in parent['assets'].values()]

    def test_reproduces_live_generation_bytes(self):
        parent = b.read_registry(os.path.join(REGRESSION, 'gen.json.gz'))
        live = b.read_registry(os.path.join(REGRESSION, 'gen2.json.gz'))
        store = b.AssetStore(parent['assets'], os.path.join(REGRESSION, 'assets'))
        out = os.path.join(self.tmp, 'gen')
        b.build(parent, store, b.select_files(parent, list(parent['files'])), self.holds(parent),
                live['generation'], live['created_at'], live['publication_note'], out)
        reg = b.read_registry(os.path.join(out, 'registry.json.gz'))
        # The hand build left etag PENDING for the controller; the builder writes the body MD5.
        strip = lambda o: {k: v for k, v in o.items() if k != 'etag'}  # noqa: E731
        self.assertEqual({k: v for k, v in reg.items() if k != 'assets'}, {k: v for k, v in live.items() if k != 'assets'})
        self.assertEqual({p: strip(o) for p, o in reg['assets'].items()}, {p: strip(o) for p, o in live['assets'].items()})
        replaced = [p for p, o in live['assets'].items() if o['etag'] == 'PENDING']
        self.assertEqual(len(replaced), 184)
        for p in replaced:
            ours = Path(os.path.join(out, 'objects', reg['assets'][p]['key'])).read_bytes()
            theirs = Path(os.path.join(REGRESSION, 'gen2', 'objects', live['assets'][p]['key'])).read_bytes()
            self.assertEqual(ours, theirs, p)
            self.assertEqual(reg['assets'][p]['etag'], '"' + hashlib.md5(ours).hexdigest() + '"')
        go_validate(self, os.path.join(out, 'registry.json.gz'))

    def test_full_file_set_rebuilds_parent(self):
        """With every file of c4049f6d and its own id, each recomputed field equals the published one."""
        parent = b.read_registry(os.path.join(REGRESSION, 'current.json'))
        store = b.AssetStore(parent['assets'], os.path.join(REGRESSION, 'assets'))
        gr = store.load('/public/growth.json')
        out, changed = b.rebuild(store, parent['recording_ids'], set(parent['files']), gr['generation'], gr['published_at'])
        self.assertEqual(changed, set())
        diffs = [d for p, o in out.items() for d in close(o, store.load(p), p)]
        self.assertEqual(diffs, [])


if __name__ == '__main__':
    unittest.main()
