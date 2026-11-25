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
DAYS = tuple(f'2026-07-{d:02d}' for d in range(10, 24))  # a 14-day window
OUTPUTS = 4 * len(DAYS) - 1  # two originals x two hours a day, two replaced by one assembly
RIDS = list(range(301, 350))  # 49 recordings, the validator's minimum


def assembly_id(rid):
    return 'sha256:' + hashlib.sha256(str(rid).encode()).hexdigest()


def hold_kind(path):
    return path.split('/')[2].split('?')[0] if path.count('/') > 1 else path.strip('/')


class Fixture:
    """A small, internally consistent parent generation: 49 recordings, 14 days x 2 hours,
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
                        oid = rid * 1000 + d * 4 + h * 2 + k
                        dur = 1800.0 if k == 0 else 1700.0
                        parts.append([oid, k + 1, dur, k * 1800.0, k * 1800.0 + dur, dur, dur, 0, f'{oid}.mp4', 1, None])
                        ranges[str(oid)] = {'ranges': [[t0 + k * 1800.0, t0 + k * 1800.0 + dur]]}
                    hours.append(['collated', 3600.0, 3500.0, 0.0, 60, 3500.0, 0, t0, t0 + 3600, parts, [], 1])
                    for p in parts:
                        if not (d == 0 and h == 0):
                            outputs.append(self.output(rid, p[0], date))
                days.append({'date': date, 'grade': 'A', 'hours': hours})
            inputs = [rid * 1000, rid * 1000 + 1]
            asm = dict(self.output(rid, assembly_id(rid), DAYS[0]), replacement_inputs=inputs)
            outputs.insert(0, asm)
            repairs.append({'sha256': asm['sha256'], 'recording_id': rid, 'local_date': DAYS[0], 'seconds': 3500.0,
                            'input_ids': inputs, 'production_version': 85})
            for o in outputs:
                files[str(o['id'])] = {'etag': '"e"', 'key': f'joined/g/objects/{o["sha256"]}.mp4', 'path': o['path'],
                                       'recording_id': rid, 'sha256': o['sha256'], 'size_bytes': o['size_bytes'], 'verified': True}
                paths[str(o['id'])] = o['path']
            man = {'schema': 2, 'recording_id': rid, 'original_count': OUTPUTS + 1, 'replaced_original_count': 2,
                   'replacement_count': 1, 'outputs': outputs}
            stream = {'id': rid, 'days': days, 'current_download_generation': PARENT_GEN, 'current_download_paths': paths}
            rng = {'id': rid, 'by_output_id': ranges}
            objects[f'/public/manifests/{rid}.json'] = man
            objects[f'/api/coverage-delivery/manifest?stream={rid}&scope=available'] = man
            objects[f'/public/streams/{rid}.json'] = stream
            objects[f'/public/streams/{rid}.json.gz'] = stream
            objects[f'/public/ranges/{rid}.json'] = rng
            objects[f'/public/ranges/{rid}.json.gz'] = rng
            hours = 2 * len(DAYS)
            md_streams.append({'id': rid, 'timezone': 'Europe/Warsaw', 'source_hours': float(hours), 'excluded_hours': 0.0,
                               'captured_scheduled_seconds': hours * 3600.0, 'registered_hours': hours, 'missing_registration_hours': 0,
                               'state_counts': {'nas_parts': 0, 'registered': hours, 'parts': 2 * hours, 'multi': hours, 'collated': hours},
                               'largest_gap_seconds': 100.0, 'current_download_generation': PARENT_GEN, 'sha256': ''})
            readiness[str(rid)] = {'recording_id': rid, 'scheduled_slots': hours, 'manifest_present_slots': hours,
                                   'captured_scheduled_seconds': hours * 3600.0, 'length_thresholds': {},
                                   'captured_without_registered_output_union_seconds': 0.0,
                                   'delivery_state': 'all_scheduled_manifests_present'}
            counts[str(rid)] = {'files': OUTPUTS}
            cat_streams.append({'id': rid, 'available_start': DAYS[0],
                                'weekly_exports': {'available': {'weeks': [{'number': 1, 'start': DAYS[0], 'end': DAYS[-1]}]}}})
        summary = {'scheduled_hours': 2 * len(DAYS) * len(RIDS), 'nas_receipted_parts': 0}
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

    def build(self, present, out, best14=None, added=()):
        store = b.AssetStore(self.parent, self.assets_dir)
        return b.build(self.parent, store, present, self.holds, GEN, NOW, 'note', out, best14, added)


class SyntheticBuildTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.fx = Fixture(self.tmp)
        self.store = b.AssetStore(self.fx.parent, self.fx.assets_dir)

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
        self.assertEqual((man['original_count'], man['replacement_count'], len(man['outputs'])), (OUTPUTS - 1, 0, OUTPUTS - 1))

        # A kept assembly keeps its replaced originals on the timeline.
        kept = self.object(out, reg, f'/public/streams/{RIDS[2]}.json')
        self.assertEqual(reg['assets'][f'/public/streams/{RIDS[2]}.json'], self.fx.parent['assets'][f'/public/streams/{RIDS[2]}.json'])
        self.assertEqual([p[0] for p in kept['days'][0]['hours'][0][9]], [RIDS[2] * 1000, RIDS[2] * 1000 + 1])

        # A zero-file recording uses the page's disabled state.
        md = {s['id']: s for s in self.object(out, reg, '/public/metadata.json')['streams']}
        rd = self.object(out, reg, '/public/readiness.json')['by_recording_id']
        self.assertEqual((md[empty]['parts'], md[empty]['registered_hours'], md[empty]['missing_registration_hours']), (0, 0, 2 * len(DAYS)))
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

        # Rebuilt from the same base as a child of that generation: every key is already published.
        again = self.out('again')
        b.build(reg, self.store, present, self.fx.holds, GEN, NOW, 'note', again)
        self.assertEqual(b.read_registry(os.path.join(again, 'registry.json.gz'))['assets'], reg['assets'])
        self.assertEqual(Path(again, 'uploads.tsv').read_text(), '\t'.join(b.UPLOAD_COLUMNS) + '\n')
        self.assertEqual(Path(again, 'holds.tsv').read_text(), '\t'.join(b.HOLD_COLUMNS) + '\n')

    def check_objects(self, out, reg):
        uploads = b.read_tsv(os.path.join(out, 'uploads.tsv'), b.UPLOAD_COLUMNS)
        holds = b.read_tsv(os.path.join(out, 'holds.tsv'), b.HOLD_COLUMNS)
        parent_holds = {h['object_key']: h for h in self.fx.holds}
        by_key = {o['key']: (p, o) for p, o in reg['assets'].items()}
        self.assertEqual([u['key'] for u in uploads], [h['object_key'] for h in holds])
        self.assertEqual({u['key'] for u in uploads}, {o['key'] for p, o in reg['assets'].items() if o != self.fx.parent['assets'][p]})
        for u, h in zip(uploads, holds):
            body = Path(os.path.join(out, u['local_path'])).read_bytes()
            path, o = by_key[u['key']]
            self.assertEqual(u['key'], b.ASSET_PREFIX + hashlib.sha256(body).hexdigest() + '/' + u['key'].rsplit('/', 1)[1])
            self.assertEqual((u['sha256'], int(u['size_bytes']), u['etag'], u['content_type']),
                             (o['sha256'], len(body), hashlib.md5(body).hexdigest(), self.fx.parent['assets'][path]['content_type']))
            self.assertEqual(o['etag'], '"' + u['etag'] + '"')
            old = parent_holds[self.fx.parent['assets'][path]['key']]
            self.assertEqual(h, {'bucket': 'stoarama', 'object_key': u['key'], 'etag': u['etag'], 'size_bytes': str(len(body)),
                                 'recording_id': '\\N' if old['recording_id'] is None else str(old['recording_id']),
                                 'kind': old['kind'], 'sha256': u['sha256'],
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
        self.assertEqual([[h[0] for h in d['hours']] for d in stream['days']],
                         [['collated'] * 2 if d == DAYS[1] else ['missing_output'] * 2 for d in DAYS])
        man = self.object(out, reg, f'/public/manifests/{rid}.json')
        self.assertEqual({o['local_date'] for o in man['outputs']}, {DAYS[1]})
        self.check_objects(out, reg)

        with self.assertRaisesRegex(SystemExit, 'not on its timeline'):
            b.select_days(self.fx.parent, self.store, {(rid, '2026-08-30')})
        with self.assertRaisesRegex(SystemExit, 'not in the parent registry'):
            b.select_days(self.fx.parent, self.store, {(999, DAYS[0])})

    def test_best14_availability(self):
        full, missing, unstitched, shifted, quiet, blank = RIDS[:6]
        best14 = {r: {(d, 'R') for d in DAYS} for r in RIDS}
        best14[unstitched] = {(d, 'S' if i < 3 else 'R') for i, d in enumerate(DAYS)}
        best14[shifted] = {(f'2026-07-{d:02d}', 'R') for d in range(11, 25)}
        best14[quiet] = {(d, '.' if i == 5 else 'R') for i, d in enumerate(DAYS)}
        best14[blank] = {(d, '.') for d in DAYS}
        drop = str(missing * 1000 + 9)
        present = set(self.fx.parent['files']) - {drop}
        summary = self.fx.build(present, self.out('best14'), best14)
        self.assertEqual(summary['availability'], {'available': len(RIDS) - 4, 'partial': 4})
        reg = b.read_registry(os.path.join(self.out('best14'), 'registry.json.gz'))
        md = {s['id']: s for s in self.object(self.out('best14'), reg, '/public/metadata.json')['streams']}
        pick = lambda s: (s['availability'], s['best14_start'], s['best14_end'], s['best14_days_ready'])  # noqa: E731
        self.assertEqual(pick(md[full]), ('available', DAYS[0], DAYS[-1], 14))
        self.assertEqual(pick(md[quiet]), ('available', DAYS[0], DAYS[-1], 14))
        self.assertEqual(pick(md[missing]), ('partial', DAYS[0], DAYS[-1], 14))  # a part is gone from R2
        self.assertEqual(pick(md[unstitched]), ('partial', DAYS[0], DAYS[-1], 11))
        self.assertEqual(pick(md[blank]), ('partial', DAYS[0], DAYS[-1], 14))  # no captured day
        self.assertEqual(pick(md[shifted]), ('partial', '2026-07-11', '2026-07-24', 14))  # window is not the timeline
        # Partial streams still serve every selected file.
        self.assertEqual(set(reg['files']), present)
        go_validate(self, os.path.join(self.out('best14'), 'registry.json.gz'))

        best14[full] = set(list(best14[full])[:13])
        with self.assertRaisesRegex(SystemExit, 'needs 14 days'):
            self.fx.build(present, self.out('short'), best14)

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
        self.fx.parent['files'][str(RIDS[1] * 1000 + 2)]['path'] = self.fx.parent['files'][str(RIDS[1] * 1000 + 3)]['path']
        with self.assertRaisesRegex(SystemExit, 'listed by more than one file'):
            self.fx.build(set(self.fx.parent['files']), self.out('duppath'))
        self.assertFalse(os.path.exists(self.out('duppath')))

    def clock_dir(self, name, rid, inputs, path, sha='c' * 64):
        """A clock-join output directory holding one assembly over inputs."""
        d = os.path.join(self.tmp, name)
        video, proof = b'mp4' + sha.encode(), b'{"proof":1}'
        key, proof_key = f'joined/clock-prestitch-20261126/objects/{sha}.mp4', f'joined/clock-prestitch-20261126/proofs/{sha}.json'
        for k, body in ((key, video), (proof_key, proof)):
            os.makedirs(os.path.dirname(os.path.join(d, k)), exist_ok=True)
            Path(d, k).write_bytes(body)
        ann = {'scope': 'boundary', 'method': 'CLOCK_20', 'status': 'verified', 'artifact_sha256': sha,
               'proof_sha256': hashlib.sha256(proof).hexdigest(), 'source_input_ids': inputs[:2], 'maximum_relative_fps_error': 0.05}
        a = {'sha256': sha, 'size_bytes': len(video), 'etag': hashlib.md5(video).hexdigest(), 'key': key, 'proof_key': proof_key,
             'proof_sha256': hashlib.sha256(proof).hexdigest(), 'proof_size_bytes': len(proof), 'proof_etag': hashlib.md5(proof).hexdigest(),
             'recording_id': rid, 'local_date': DAYS[1], 'hour': 1, 'seconds': 3500.0,
             'media_seconds_measurement': 'verified_native_video_presentation_span', 'input_ids': inputs, 'input_parts': len(inputs),
             'parts_reduced': len(inputs) - 1, 'path': path, 'kind': 'clock', 'production_version': 'clock-prestitched',
             'clock_boundary_annotations': [ann], 'audio_packets': 10, 'original_source_audio_present': True,
             'temporary_no_audio': False, 'audio_delivery_status': 'original_audio_stream_copied'}
        os.makedirs(os.path.join(d, 'results'))
        Path(d, 'results', 'h.json').write_text(json.dumps({'assemblies': [a]}))
        return d

    def test_clock_assemblies(self):
        rid = RIDS[3]
        inputs = [rid * 1000 + 4, rid * 1000 + 5]  # day 1, hour 0: both originals
        path = self.fx.parent['files'][str(inputs[0])]['path']
        added = b.read_clock_assemblies([self.clock_dir('clock-join', rid, inputs, path)])
        out = self.out('clock')
        summary = self.fx.build(set(self.fx.parent['files']), out, added=added)
        self.assertEqual(summary['clock_assemblies'], 1)
        reg = b.read_registry(os.path.join(out, 'registry.json.gz'))
        aid = 'sha256:' + 'c' * 64
        self.assertEqual(reg['files'][aid], {'etag': '"' + added[0]['etag'] + '"', 'key': added[0]['key'], 'path': path,
                                             'recording_id': rid, 'sha256': 'c' * 64, 'size_bytes': added[0]['size_bytes'], 'verified': True})
        self.assertFalse({str(i) for i in inputs} & set(reg['files']))
        man = self.object(out, reg, f'/public/manifests/{rid}.json')
        self.assertEqual([o['replacement_inputs'] for o in man['outputs'] if o['id'] == aid], [inputs])
        self.assertEqual((man['replacement_count'], man['replaced_original_count']), (2, 4))
        stream = self.object(out, reg, f'/public/streams/{rid}.json')
        self.assertEqual([p[0] for p in stream['days'][1]['hours'][0][9]], inputs)  # inputs stay on the timeline
        self.assertEqual(stream['current_download_paths'][aid], path)
        gr = self.object(out, reg, '/public/growth.json')
        rep = [r for r in gr['repairs'] if r['sha256'] == 'c' * 64]
        self.assertEqual(len(rep), 1)
        self.assertEqual((rep[0]['production_version'], rep[0]['input_ids'], rep[0]['file_url'], rep[0]['collections']),
                         ('clock-prestitched', inputs, '/api/v1/global-street-scores-delivery/file/' + aid, ['available']))
        self.assertEqual(gr['production_model_counts']['clock-prestitched'], 1)
        self.assertEqual(gr['current_stream_counts'][str(rid)]['current_assemblies'], 2)
        uploads = {u['key']: u for u in b.read_tsv(os.path.join(out, 'uploads.tsv'), b.UPLOAD_COLUMNS)}
        holds = {h['object_key']: h for h in b.read_tsv(os.path.join(out, 'holds.tsv'), b.HOLD_COLUMNS)}
        for key, kind, ctype in ((added[0]['key'], 'video_assembly', 'video/mp4'), (added[0]['proof_key'], 'clock_proof', 'application/json')):
            body = Path(out, uploads[key]['local_path']).read_bytes()
            self.assertEqual((uploads[key]['etag'], uploads[key]['content_type']), (hashlib.md5(body).hexdigest(), ctype))
            self.assertEqual((holds[key]['kind'], holds[key]['etag'], holds[key]['recording_id']), (kind, uploads[key]['etag'], str(rid)))
        self.assertEqual(holds[added[0]['key']]['relative_path'], path)
        self.assertEqual(b.read_tsv(os.path.join(out, 'dependencies.tsv'), b.DEPENDENCY_COLUMNS),
                         [{'output_id': str(i), 'recording_id': str(rid)} for i in inputs])
        go_validate(self, os.path.join(out, 'registry.json.gz'))

    def test_clock_assemblies_fail_fast(self):
        rid = RIDS[3]
        everything = set(self.fx.parent['files'])
        replaced = [rid * 1000, rid * 1000 + 1]  # already replaced by the fixture's V85 assembly
        added = b.read_clock_assemblies([self.clock_dir('taken', rid, replaced, 'x')])
        with self.assertRaisesRegex(SystemExit, 'already replaced'):
            self.fx.build(everything, self.out('taken-out'), added=added)
        inputs = [rid * 1000 + 4, rid * 1000 + 5]
        other = self.fx.parent['files'][str(rid * 1000 + 6)]['path']  # a file still delivered
        added = b.read_clock_assemblies([self.clock_dir('dup', rid, inputs, other)])
        with self.assertRaisesRegex(SystemExit, 'listed by more than one file'):
            self.fx.build(everything, self.out('dup-out'), added=added)
        added = b.read_clock_assemblies([self.clock_dir('unselected', rid, inputs, 'p')])
        with self.assertRaisesRegex(SystemExit, 'not a selected file'):
            self.fx.build(everything - {str(inputs[1])}, self.out('unselected-out'), added=added)
        added = b.read_clock_assemblies([self.clock_dir('a', rid, inputs, 'p'), self.clock_dir('b', rid, inputs, 'q', sha='d' * 64)])
        with self.assertRaisesRegex(SystemExit, 'inputs of two clock assemblies'):
            self.fx.build(everything, self.out('twice-out'), added=added)

    def test_cli_and_server_validator(self):
        days = os.path.join(self.tmp, 'days.tsv')
        with open(days, 'w') as f:
            f.write('recording_id\tstream_id\tlocal_date\tday_stitched\n')
            f.writelines(f'{r}\t1\t{d}\tR\n' for r in RIDS for d in DAYS if r != RIDS[-1] or d == DAYS[0])
        best14 = os.path.join(self.tmp, 'best14.tsv')
        with open(best14, 'w') as f:
            f.write('recording_id\tstream_id\tlocal_date\tday_stitched\n')
            f.writelines(f'{r}\t1\t{d}\tR\n' for r in RIDS for d in DAYS)
        out = self.out('cli')
        subprocess.run([sys.executable, '-I', os.path.join(BACKEND, 'scripts', 'gss', 'build_generation.py'),
                        '--parent', self.fx.registry_path, '--base', self.fx.registry_path, '--assets', self.fx.assets_dir,
                        '--base-holds', self.fx.holds_path, '--best14', best14,
                        '--days', days, '--note', 'cli', '--out', out], check=True, stdout=subprocess.DEVNULL)
        reg = b.read_registry(os.path.join(out, 'registry.json.gz'))
        self.assertEqual(len(bytes.fromhex(reg['generation'])), 16)
        self.assertEqual({v['recording_id'] for v in reg['files'].values()}, set(RIDS))
        md = {s['id']: s['availability'] for s in self.object(out, reg, '/public/metadata.json')['streams']}
        self.assertEqual((md[RIDS[0]], md[RIDS[-1]]), ('available', 'partial'))
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
        store = b.AssetStore(parent, os.path.join(REGRESSION, 'assets'))
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
        store = b.AssetStore(parent, os.path.join(REGRESSION, 'assets'))
        gr = store.load('/public/growth.json')
        out, changed = b.rebuild(store, parent['recording_ids'], set(parent['files']), gr['generation'], gr['published_at'])
        self.assertEqual(changed, set())
        diffs = [d for p, o in out.items() for d in close(o, store.load(p), p)]
        self.assertEqual(diffs, [])


if __name__ == '__main__':
    unittest.main()
