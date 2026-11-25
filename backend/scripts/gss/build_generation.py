"""Build a Global Street Scores (GSS) public delivery generation.

Rebuilds every page asset of a base generation for a file selection, marks each
stream available or partial for its best 14 days, and writes into a new, empty
output directory:

  registry.json.gz   the new public registry (gzip, deterministic)
  objects/<key>      every new content-addressed asset body, laid out by R2 key
  uploads.tsv        key, local_path, sha256, size_bytes, content_type, etag
  holds.tsv          delivery_keep_holds rows to insert (psql \\copy ... WITH (FORMAT text, HEADER))
  dependencies.tsv   delivery_keep_output_dependencies rows for added assemblies' inputs
  summary.json       counts plus the known-stale fields this build does not recompute

It never uploads and never writes the database. The controller uploads each
uploads.tsv row create-only (If-None-Match: *) and checks the returned ETag
equals the listed one (R2's single-part ETag is the body MD5), inserts
holds.tsv, then publishes registry.json.gz as public/generations/<id>.json and
public/current.json. A key already in the parent registry is neither uploaded
nor held again.

--parent is the generation being replaced: the new registry keeps its top-level
fields and lists the selected subset of its files. --base is the generation
whose asset bodies are rebuilt; its timelines and manifests must still list
every file (for a plain filter of the live generation, pass it as both).

Selection is either --files (one parent file id per line) or --days (a TSV
whose header has recording_id and local_date columns). A day selects every
parent file whose manifest output has that recording_id and local_date;
local_date is the stream-timezone date the manifests and timelines carry.

--best14 is best14-days.tsv (recording_id, local_date, day_stitched; 14 rows per
recording). day_stitched R means every captured hour is stitched in R2 and .
means no capture. A stream is "available" when all 14 days are R or . with at
least one R, its
base timeline covers exactly those days, and every captured hour keeps all its
base parts; otherwise it is "partial" and serves what is selected.

--clock-assemblies adds the assemblies of one or more `stoaramactl collation-v2
clock-join` output directories (their results/*.json). Each replaces its input
files in the file list and manifest (its inputs stay on the timeline, as for
every assembly), joins growth.json repairs as production_version
clock-prestitched, and adds its video and proof to uploads.tsv (hard-linked
into objects/), holds.tsv and dependencies.tsv. Every input must be a selected
collated file that no other assembly replaces.

--base-holds is the base asset holds, exported read-only:
  psql -At -c "SELECT json_agg(json_build_object('object_key',object_key,
    'kind',kind,'recording_id',recording_id,'relative_path',relative_path))
    FROM delivery_keep_holds WHERE bucket='stoarama'
    AND object_key LIKE 'datasets/global-street-scores-delivery/47/assets/%'"
"""
import argparse
import collections
import copy
import csv
import datetime
import enum
import gzip
import hashlib
import json
import os
import secrets
import statistics
from pathlib import Path

ASSET_PREFIX = 'datasets/global-street-scores-delivery/47/assets/'
BUCKET = 'stoarama'
MAX_ASSET_BYTES = 64 << 20

READY_DAY_CODES = frozenset('R.')
DAY_CODES = frozenset('RNSs.')
NO_CAPTURE_STATES = frozenset({'gap_only', 'quarantine_only'})


class Availability(str, enum.Enum):
    AVAILABLE = 'available'
    PARTIAL = 'partial'


# Assets this builder carries over byte for byte: they do not list files.
UNTOUCHED_ASSETS = ('/download-script.py', '/public/images.json', '/public/blue-observations.json')

# Fields that depend on the file set but are not recomputed (no source data in
# the assets to recompute them from). They keep the parent's values.
KNOWN_STALE_FIELDS = (
    'metadata.json streams[].excluded_hours',
    'metadata.json streams[].source_hours',
    'metadata.json streams[].source_clips',
    'metadata.json streams[].hour_lanes',
    'metadata.json streams[].largest_gap_seconds',
    'metadata.json streams[].gap_over_5m_count',
    'metadata.json streams[].gap_over_30s_count',
    'metadata.json streams[].improvement_seconds',
    'metadata.json streams[].state_counts.nas_parts',
    'metadata.json summary.registered_source_hours',
    'metadata.json summary.nas_receipted_parts',
    'metadata.json summary.new11_measured_playback_length_parts',
    'metadata.json summary.original49_known_accounting_subtotals',
    'readiness.json summary (same fields as metadata.json summary)',
    'catalog.json streams[].available_start',
    'catalog.json streams[].available_end',
    'catalog.json verification',
    'growth.json production_demand_original_parts',
    'growth.json known_source_gap_original_ids',
)


class AssetStore:
    """Parses parent asset bodies from a local mirror laid out by R2 key."""

    def __init__(self, registry, assets_dir):
        self.assets = registry['assets']
        self.recording_ids = registry['recording_ids']
        self.dir = assets_dir

    def raw(self, path):
        o = self.assets[path]
        body = Path(self.dir, o['key']).read_bytes()
        if len(body) != o['size_bytes'] or hashlib.sha256(body).hexdigest() != o['sha256']:
            raise SystemExit(f'asset body differs from the parent registry: {path}')
        return body

    def load(self, path):
        body = self.raw(path)
        return json.loads(gzip.decompress(body) if path.endswith('.gz') else body)


def dumps(o):
    return json.dumps(o, ensure_ascii=False, separators=(',', ':')).encode()


def merge(iv):
    out = []
    for a, b in sorted(iv):
        if out and a <= out[-1][1]:
            out[-1][1] = max(out[-1][1], b)
        else:
            out.append([a, b])
    return out


def length(iv):
    return sum(b - a for a, b in merge(iv))


def rebuild(store, recording_ids, present, gen_id, now, added=()):
    """Return ({asset path without .gz: new object}, changed recording ids).

    Assembly rule: a present assembly (id sha256:...) keeps its replaced
    originals on the timeline, so an original part stays if it is present or
    replaced by a present assembly.
    """
    A = store.assets
    load = store.load
    md = load('/public/metadata.json'); rd = load('/public/readiness.json')
    gr = load('/public/growth.json'); cat = load('/public/catalog.json')
    MS = {s['id']: s for s in md['streams']}; CS = {s['id']: s for s in cat['streams']}
    out = {}
    changed_streams = set()
    kept_repairs = [r for r in gr['repairs'] if 'sha256:' + r['sha256'] in present] + [clock_repair(a) for a in added]
    for rid in recording_ids:
        man = load(f'/public/manifests/{rid}.json')
        assert man == load(f'/api/coverage-delivery/manifest?stream={rid}&scope=available'), rid
        outputs = [o for o in man['outputs'] if str(o['id']) in present] + [clock_output(a) for a in added if a['recording_id'] == rid]
        assemblies = [o for o in outputs if str(o['id']).startswith('sha256:')]
        originals_current = {str(o['id']) for o in outputs if not str(o['id']).startswith('sha256:')}
        replaced = {str(i) for o in assemblies for i in o['replacement_inputs']}
        kept = originals_current | replaced
        man2 = dict(man, outputs=outputs, original_count=len(kept), replaced_original_count=len(replaced), replacement_count=len(assemblies))
        out[f'/public/manifests/{rid}.json'] = man2
        out[f'/api/coverage-delivery/manifest?stream={rid}&scope=available'] = man2

        sp = load(f'/public/streams/{rid}.json'); sp2 = copy.deepcopy(sp)
        emptied = 0
        hours = [h for day in sp2['days'] for h in day['hours']]
        for h in hours:
            parts = [p for p in h[9] if str(p[0]) in kept]
            if len(parts) != len(h[9]):
                assert all(p[5] is not None and p[6] is not None for p in parts)
                if not parts:
                    # Zero-file hours (and recordings) use the page's existing missing_output state.
                    assert h[0] == 'collated' and h[1] > 0, (rid, h[0])
                    h[0] = 'missing_output'; emptied += 1
                h[9] = parts; h[2] = sum(p[5] for p in parts); h[5] = sum(p[6] for p in parts)
        all_ids = {str(p[0]) for h in hours for p in h[9]}
        assert all_ids == kept, rid  # every kept original appears once on the timeline
        stream_changed = sp2 != sp or any(k not in present for k in sp.get('current_download_paths', {}))
        if stream_changed:
            changed_streams.add(rid)
            assert f'/public/ranges/{rid}.json' in A, rid
            sp2['current_download_paths'] = {str(o['id']): o['path'] for o in outputs if o['available']}
            sp2['current_download_generation'] = gen_id
        out[f'/public/streams/{rid}.json'] = sp2

        rp = f'/public/ranges/{rid}.json'
        U = None
        parts = [p for h in hours for p in h[9]]
        if rp in A:
            rg = load(rp)
            rg2 = dict(rg, by_output_id={k: v for k, v in rg['by_output_id'].items() if k in kept})
            out[rp] = rg2
            sched = [(h[7], h[8]) for h in hours]

            def represented(ids):
                iv = []
                for i in ids:
                    for a, b in rg2['by_output_id'][str(i)]['ranges']:
                        for lo, hi in sched:
                            if min(b, hi) > max(a, lo):
                                iv.append((max(a, lo), min(b, hi)))
                return length(iv)
            U = represented(kept)

        # metadata stream
        s = MS[rid]; cap = s['captured_scheduled_seconds']
        dur = [p[2] for p in parts if p[2] is not None]
        with_parts = sum(1 for h in hours if h[9])
        s['parts'] = len(parts)
        s['included_hours'] = sum(h[2] for h in hours) / 3600
        s['coverage'] = 100 * s['included_hours'] / s['source_hours'] if s['source_hours'] else 0.0
        s['fragmentation'] = len(parts) / with_parts if with_parts else 0.0
        s['registered_hours'] -= emptied; s['missing_registration_hours'] += emptied
        s['multi_part_hours'] = sum(1 for h in hours if len(h[9]) > 1)
        s['parts_under_2min'] = sum(1 for x in dur if x < 120)
        s['max_parts_per_hour'] = max(len(h[9]) for h in hours)
        sc = s['state_counts']; sc['parts'] = len(parts); sc['multi'] = s['multi_part_hours']; sc['registered'] = s['registered_hours']
        if 'collated' in sc:
            sc['collated'] = sum(1 for h in hours if h[0] == 'collated')
        if emptied:
            sc['missing_output'] = sum(1 for h in hours if h[0] == 'missing_output')
        sched_total = length([(h[7], h[8]) for h in hours])
        if U is not None:
            s['represented_scheduled_union_seconds'] = U
            s['represented_pct_of_schedule'] = 100 * U / sched_total
            s['captured_without_output_seconds'] = max(0, cap - U)
        if stream_changed:
            s['current_download_generation'] = gen_id
            s['sha256'] = hashlib.sha256(dumps(sp2)).hexdigest()

        # readiness stream
        R = rd['by_recording_id'][str(rid)]
        if R.get('manifest_present_slots') is not None:
            R['manifest_present_slots'] = s['registered_hours']
            R['manifest_missing_slots'] = R['scheduled_slots'] - s['registered_hours']
            R['manifest_coverage_pct'] = 100 * s['registered_hours'] / R['scheduled_slots']
        if 'registered_output_slots' in R:
            R['registered_output_slots'] = with_parts
        R['registered_part_count'] = len(parts)
        R['longest_part_seconds'] = max(dur) if dur else None
        R['median_part_seconds'] = statistics.median(dur) if dur else None
        R['parts_under_2min'] = s['parts_under_2min']; R['multi_part_slots'] = s['multi_part_hours']; R['max_parts_per_slot'] = s['max_parts_per_hour']
        if U is not None:
            R['represented_scheduled_union_seconds'] = U
            R['represented_pct_of_schedule'] = 100 * U / (R['scheduled_slots'] * 3600)
            R['represented_pct_of_captured_timeline'] = 100 * U / R['captured_scheduled_seconds'] if R['captured_scheduled_seconds'] else 0.0
            if 'captured_without_registered_output_union_seconds' in R:
                R['captured_without_registered_output_union_seconds'] = max(0, R['captured_scheduled_seconds'] - U)
            if parts:
                th = {}
                for n, old in R['length_thresholds'].items() if R['length_thresholds'] else ((str(n), None) for n in (15, 30, 45, 60)):
                    sel = [p[0] for p in parts if p[2] is not None and p[2] >= int(n) * 60]
                    u = represented(sel)
                    th[n] = {'minimum_playback_minutes': int(n), 'represented_scheduled_union_seconds': u,
                             'pct_of_schedule': 100 * u / (R['scheduled_slots'] * 3600),
                             'pct_of_captured_timeline': 100 * u / R['captured_scheduled_seconds'],
                             'pct_of_registered_represented_timeline': 100 * u / U if U else 0.0, 'parts': len(sel)}
                R['length_thresholds'] = th
            else:
                R['length_thresholds'] = {}
        if stream_changed:
            state = 'awaiting_verified_outputs' if not parts else 'partial_registered_delivery' if s['registered_hours'] < R['scheduled_slots'] else R['delivery_state']
            R['delivery_state'] = state
            if 'delivery_state' in s:
                s['delivery_state'] = state

        # growth per-stream counts
        secs = []
        rep = {'sha256:' + r['sha256']: r for r in kept_repairs if r['recording_id'] == rid}
        pd = {str(p[0]): p[2] for p in parts}
        for o in outputs:
            secs.append(rep[str(o['id'])]['seconds'] if str(o['id']) in rep else pd[str(o['id'])])
        for o in assemblies:
            assert sorted(map(str, o['replacement_inputs'])) == sorted(map(str, rep[str(o['id'])]['input_ids']))
        known = [x for x in secs if x is not None]
        x = gr['current_stream_counts'][str(rid)]
        tot = sum(known)
        x['files'] = x['current_files'] = len(outputs)
        x['replacements'] = x['current_assemblies'] = len(assemblies)
        x['registered_media_seconds_measured'] = x['known_media_seconds'] = tot
        x['registered_media_hours_measured'] = tot / 3600
        x['media_unknown_file_count'] = len(secs) - len(known)
        for n, lim in (('30', 1800), ('55', 3300)):
            sel = [v for v in known if v >= lim]
            x[f'files_at_least_{n}m'] = len(sel)
            x[f'media_hours_at_least_{n}m'] = sum(sel) / 3600
            x[f'pct_registered_media_at_least_{n}m'] = 100 * sum(sel) / tot if tot else None

        # catalog
        c = CS[rid]; av = [o for o in outputs if o['available']]
        c['available_parts'] = len(outputs); c['available_available'] = len(av); c['available_size_bytes'] = sum(o['size_bytes'] for o in av)
        for w in c['weekly_exports']['available']['weeks']:
            wo = [o for o in outputs if w['start'] <= o['local_date'] <= w['end']]
            wa = [o for o in wo if o['available']]
            w['parts'] = len(wo); w['available'] = len(wa); w['size_bytes'] = sum(o['size_bytes'] for o in wo)
            w['sample_path'] = wa[0]['path'] if wa else None
            w['sample_local_date'] = wa[0]['local_date'] if wa else None

    for summ in (md['summary'], rd['summary']):
        S = md['streams']
        summ['r2_parts'] = sum(s['parts'] for s in S)
        summ['registered_hours'] = sum(s['registered_hours'] for s in S)
        summ['missing_registration_hours'] = sum(s['missing_registration_hours'] for s in S)
        summ['represented_scheduled_union_seconds'] = sum(s['represented_scheduled_union_seconds'] for s in S)
        summ['represented_pct_of_schedule'] = 100 * summ['represented_scheduled_union_seconds'] / (summ['scheduled_hours'] * 3600)
        summ['registered_included_hours'] = sum(s['included_hours'] for s in S)
        summ['captured_without_output_hours'] = sum(s['captured_without_output_seconds'] for s in S) / 3600
    gr['repairs'] = kept_repairs
    gr['verified_assembly_objects'] = len(kept_repairs)
    gr['current_file_count'] = len(present)
    pmc = gr['production_model_counts']
    if added:
        pmc.setdefault(CLOCK_VERSION, 0)
    for k in pmc:
        if k != 'prior_verified_assemblies':
            pmc[k] = sum(1 for r in kept_repairs if str(r.get('production_version')) == k)
    pmc['prior_verified_assemblies'] = len(kept_repairs) - sum(v for k, v in pmc.items() if k != 'prior_verified_assemblies')
    # The growth asset names the generation it describes; the page checks it.
    gr['generation'] = gen_id
    if now:
        gr['published_at'] = gr['updated_at'] = now
    out['/public/metadata.json'] = md; out['/public/readiness.json'] = rd
    out['/public/growth.json'] = gr; out['/public/catalog.json'] = cat
    return out, changed_streams


CLOCK_VERSION = 'clock-prestitched'
FILE_URL = '/api/v1/global-street-scores-delivery/file/'
# growth.json repair fields a clock-join assembly carries (Assembly in backend/internal/clockjoin).
CLOCK_REPAIR_FIELDS = ('sha256', 'recording_id', 'local_date', 'hour', 'seconds', 'media_seconds_measurement', 'size_bytes',
                       'input_ids', 'input_parts', 'parts_reduced', 'kind', 'production_version', 'path',
                       'clock_boundary_annotations', 'temporary_no_audio', 'original_source_audio_present',
                       'audio_delivery_status', 'proof_key', 'proof_sha256')


def clock_id(a):
    return 'sha256:' + a['sha256']


def clock_repair(a):
    return dict({k: a[k] for k in CLOCK_REPAIR_FIELDS}, collections=['available'], file_url=FILE_URL + clock_id(a))


def clock_output(a):
    return {'available': True, 'file_url': FILE_URL + clock_id(a), 'id': clock_id(a), 'local_date': a['local_date'],
            'path': a['path'], 'recording_id': a['recording_id'], 'replacement_inputs': list(a['input_ids']),
            'sha256': a['sha256'], 'size_bytes': a['size_bytes'], 'ticket_presentation_path': True}


def read_clock_assemblies(dirs):
    """Every assembly in the results/*.json of each clock-join output directory, with its local files."""
    out = []
    for d in dirs:
        results = sorted(Path(d, 'results').glob('*.json'))
        if not results:
            raise SystemExit(f'{d}: no clock-join results')
        for r in results:
            for a in json.loads(r.read_text(encoding='utf-8'))['assemblies']:
                if a['production_version'] != CLOCK_VERSION:
                    raise SystemExit(f'{r}: assembly {a["sha256"]} is not {CLOCK_VERSION}')
                for key, sha, size in ((a['key'], a['sha256'], a['size_bytes']), (a['proof_key'], a['proof_sha256'], a['proof_size_bytes'])):
                    local = os.path.join(d, key)
                    if os.path.getsize(local) != size:
                        raise SystemExit(f'{local}: size differs from its clock-join result')
                out.append(dict(a, local_dir=os.path.abspath(d)))
    return out


def add_clock_assemblies(parent, present, added):
    """The file selection after added assemblies replace their inputs."""
    ids = collections.Counter(i for a in added for i in a['input_ids'])
    twice = sorted(i for i, n in ids.items() if n > 1)
    if twice:
        raise SystemExit(f'{len(twice)} files are inputs of two clock assemblies, e.g. {twice[:3]}')
    for a in added:
        if clock_id(a) in parent['files']:
            raise SystemExit(f'{clock_id(a)} is already in the parent registry')
        if a['recording_id'] not in parent['recording_ids']:
            raise SystemExit(f'{clock_id(a)}: recording {a["recording_id"]} is not in the parent registry')
        for i in a['input_ids']:
            f = parent['files'].get(str(i))
            if str(i) not in present or f is None or f['recording_id'] != a['recording_id']:
                raise SystemExit(f'{clock_id(a)}: input {i} is not a selected file of recording {a["recording_id"]}')
    return (present - {str(i) for i in ids}) | {clock_id(a) for a in added}


def check_clock_inputs_unreplaced(store, recording_ids, added):
    """No added assembly may take an input another assembly of the base already replaces."""
    inputs = {str(i) for a in added for i in a['input_ids']}
    for rid in recording_ids:
        for o in store.load(f'/public/manifests/{rid}.json')['outputs']:
            taken = inputs & {str(i) for i in o.get('replacement_inputs', ())}
            if taken:
                raise SystemExit(f'clock assembly inputs {sorted(taken)[:3]} are already replaced by {o["id"]}')


def link_object(out_dir, src, key):
    dst = os.path.join(out_dir, 'objects', key)
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    os.link(src, dst)  # same filesystem as the clock-join output: no copy of the video
    return os.path.join('objects', key)


def rebuilt_asset(path):
    return not (path in UNTOUCHED_ASSETS or path.startswith('/public/images/'))


def select_files(parent, ids):
    unknown = sorted(set(ids) - set(parent['files']))
    if unknown:
        raise SystemExit(f'{len(unknown)} selected file ids are not in the parent registry, e.g. {unknown[:3]}')
    return set(ids)


def select_days(parent, store, days):
    """Every parent file whose manifest output falls on a selected (recording_id, local_date)."""
    wanted = {}
    for rid, date in days:
        wanted.setdefault(rid, set()).add(date)
    unknown = sorted(set(wanted) - set(parent['recording_ids']))
    if unknown:
        raise SystemExit(f'selected recordings are not in the parent registry: {unknown}')
    present = set()
    for rid, dates in wanted.items():
        timeline = {d['date'] for d in store.load(f'/public/streams/{rid}.json')['days']}
        if not dates <= timeline:
            raise SystemExit(f'recording {rid}: selected days are not on its timeline: {sorted(dates - timeline)}')
        man = store.load(f'/public/manifests/{rid}.json')
        present |= {str(o['id']) for o in man['outputs'] if o['local_date'] in dates and str(o['id']) in parent['files']}
    return present


def annotate_availability(out, store, best14):
    """Set availability, best14_start, best14_end and best14_days_ready on each metadata stream."""
    counts = {a: 0 for a in Availability}
    for s in out['/public/metadata.json']['streams']:
        rid = s['id']
        days = sorted(best14[rid])
        if len(days) != 14:
            raise SystemExit(f'recording {rid}: best14 needs 14 days, got {len(days)}')
        dates = [d for d, _ in days]
        ready = sum(code in READY_DAY_CODES for _, code in days)
        captured = any(code != '.' for _, code in days)
        base = store.load(f'/public/streams/{rid}.json')['days']
        new = out[f'/public/streams/{rid}.json']['days']
        served = [d['date'] for d in base] == dates and all(
            h[1] == 0 or h[0] in NO_CAPTURE_STATES or (h[0] == 'collated' and n[9] == h[9])
            for bd, nd in zip(base, new) for h, n in zip(bd['hours'], nd['hours']))
        a = Availability.AVAILABLE if ready == 14 and captured and served else Availability.PARTIAL
        s.update(availability=a.value, best14_start=dates[0], best14_end=dates[-1], best14_days_ready=ready)
        counts[a] += 1
    return {a.value: n for a, n in counts.items()}


def build(parent, store, present, holds, gen_id, now, note, out_dir, best14=None, added=()):
    """Write the generation into out_dir (must not exist). Returns the summary.

    store holds the base assets. best14=None skips availability (only to
    reproduce generations built before it existed)."""
    if store.recording_ids != parent['recording_ids']:
        raise SystemExit('base and parent recordings differ')
    if added:
        check_clock_inputs_unreplaced(store, parent['recording_ids'], added)
        present = add_clock_assemblies(parent, present, added)
    files = {k: v for k, v in parent['files'].items() if k in present}
    for a in added:
        files[clock_id(a)] = {'etag': f'"{a["etag"]}"', 'key': a['key'], 'path': a['path'], 'recording_id': a['recording_id'],
                              'sha256': a['sha256'], 'size_bytes': a['size_bytes'], 'verified': True}
    dup = sorted(p for p, n in collections.Counter(v['path'] for v in files.values()).items() if n > 1)
    if dup:
        raise SystemExit(f'{len(dup)} delivery paths are listed by more than one file, e.g. {dup[:3]}')
    assets = store.assets
    out, changed = rebuild(store, parent['recording_ids'], present, gen_id, now, added)
    availability = annotate_availability(out, store, best14) if best14 is not None else None
    published = {o['key'] for o in parent['assets'].values()}
    hold_by_key = {h['object_key']: h for h in holds}
    new_assets = copy.deepcopy(assets)
    uploads = {}
    new_holds = {}
    os.makedirs(out_dir)
    for p in assets:
        if not rebuilt_asset(p):
            continue
        gz = p.endswith('.gz')
        obj = out[p[:-3] if gz else p]
        old = store.raw(p)
        if json.loads(gzip.decompress(old) if gz else old) == obj:
            continue
        body = dumps(obj)
        if gz:
            body = gzip.compress(body, compresslevel=9, mtime=0)
        if len(body) > MAX_ASSET_BYTES:
            raise SystemExit(f'asset over 64 MiB: {p}')
        sha = hashlib.sha256(body).hexdigest()
        etag = hashlib.md5(body).hexdigest()
        name = 'manifest' if p.startswith('/api/') else p.rsplit('/', 1)[1]
        key = ASSET_PREFIX + sha + '/' + name
        new_assets[p] = dict(assets[p], etag=f'"{etag}"', key=key, sha256=sha, size_bytes=len(body), verified=True)
        if key in published:
            continue
        replaced = hold_by_key.get(assets[p]['key'])
        if replaced is None:
            raise SystemExit(f'no delivery_keep_holds row for the base asset {p} ({assets[p]["key"]})')
        hold = {'bucket': BUCKET, 'object_key': key, 'etag': etag, 'size_bytes': len(body),
                'recording_id': replaced['recording_id'], 'kind': replaced['kind'], 'sha256': sha,
                'relative_path': replaced['relative_path']}
        if new_holds.setdefault(key, hold) != hold:
            raise SystemExit(f'two assets map to {key} with different holds')
        local = os.path.join(out_dir, 'objects', key)
        os.makedirs(os.path.dirname(local), exist_ok=True)
        with open(local, 'wb') as f:
            f.write(body)
        uploads[key] = {'key': key, 'local_path': os.path.join('objects', key), 'sha256': sha,
                        'size_bytes': len(body), 'content_type': assets[p]['content_type'], 'etag': etag}
    dependencies = []
    for a in added:
        for key, sha, size, etag, ctype, kind, rel in (
                (a['key'], a['sha256'], a['size_bytes'], a['etag'], 'video/mp4', 'video_assembly', a['path']),
                (a['proof_key'], a['proof_sha256'], a['proof_size_bytes'], a['proof_etag'], 'application/json', 'clock_proof', '')):
            uploads[key] = {'key': key, 'local_path': link_object(out_dir, os.path.join(a['local_dir'], key), key), 'sha256': sha,
                            'size_bytes': size, 'content_type': ctype, 'etag': etag}
            new_holds[key] = {'bucket': BUCKET, 'object_key': key, 'etag': etag, 'size_bytes': size,
                              'recording_id': a['recording_id'], 'kind': kind, 'sha256': sha, 'relative_path': rel}
        dependencies += [{'output_id': i, 'recording_id': a['recording_id']} for i in a['input_ids']]
    registry = dict(parent, generation=gen_id, parent_generation=parent['generation'], created_at=now,
                    publication_note=note, assets=new_assets, files=files)
    with open(os.path.join(out_dir, 'registry.json.gz'), 'wb') as f:
        f.write(gzip.compress(dumps(registry), compresslevel=9, mtime=0))
    write_tsv(os.path.join(out_dir, 'uploads.tsv'), UPLOAD_COLUMNS, [uploads[k] for k in sorted(uploads)])
    write_tsv(os.path.join(out_dir, 'holds.tsv'), HOLD_COLUMNS, [new_holds[k] for k in sorted(new_holds)])
    write_tsv(os.path.join(out_dir, 'dependencies.tsv'), DEPENDENCY_COLUMNS, sorted(dependencies, key=lambda d: d['output_id']))
    summary = {
        'generation': gen_id, 'parent_generation': parent['generation'], 'created_at': now,
        'files': len(files), 'recordings_with_files': len({v['recording_id'] for v in files.values()}),
        'changed_recordings': sorted(changed), 'assets_replaced': sum(new_assets[p] != assets[p] for p in assets),
        'objects': len(uploads), 'availability': availability, 'clock_assemblies': len(added),
        'bytes_served': sum(v['size_bytes'] for v in files.values()), 'known_stale_fields': list(KNOWN_STALE_FIELDS),
    }
    write_json(os.path.join(out_dir, 'summary.json'), summary)
    return summary


UPLOAD_COLUMNS = ('key', 'local_path', 'sha256', 'size_bytes', 'content_type', 'etag')
HOLD_COLUMNS = ('bucket', 'object_key', 'etag', 'size_bytes', 'recording_id', 'kind', 'sha256', 'relative_path')
DEPENDENCY_COLUMNS = ('output_id', 'recording_id')


def write_tsv(path, columns, rows):
    """PostgreSQL text-format COPY rows with a header; NULL is \\N."""
    def cell(v):
        v = '\\N' if v is None else str(v)
        if any(c in v for c in '\t\n\r') or (v.count('\\') and v != '\\N'):
            raise SystemExit(f'{path}: value needs COPY escaping: {v!r}')
        return v
    with open(path, 'w', encoding='utf-8') as f:
        f.write('\t'.join(columns) + '\n')
        f.writelines('\t'.join(cell(r[c]) for c in columns) + '\n' for r in rows)


def read_tsv(path, columns):
    with open(path, newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f, delimiter='\t'))
    if not rows or not set(columns) <= set(rows[0]):
        raise SystemExit(f'{path}: need a TSV header with {", ".join(columns)}')
    return rows


def write_json(path, o):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(o, f, ensure_ascii=False, indent=1)
        f.write('\n')


def read_registry(path):
    with open(path, 'rb') as f:
        raw = f.read()
    return json.loads(gzip.decompress(raw) if raw[:2] == b'\x1f\x8b' else raw)


def read_days(path):
    days = {(int(r['recording_id']), r['local_date']) for r in read_tsv(path, ('recording_id', 'local_date'))}
    for _, d in days:
        datetime.date.fromisoformat(d)
    return days


def read_best14(path):
    best14 = {}
    for r in read_tsv(path, ('recording_id', 'local_date', 'day_stitched')):
        code = r['day_stitched']
        if code not in DAY_CODES:
            raise SystemExit(f'{path}: unknown day_stitched {code!r}')
        datetime.date.fromisoformat(r['local_date'])
        best14.setdefault(int(r['recording_id']), set()).add((r['local_date'], code))
    return best14


def read_lines(path):
    with open(path, encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--parent', required=True, help='public registry being replaced (.json or .json.gz)')
    ap.add_argument('--base', required=True, help='registry whose assets are rebuilt (.json or .json.gz)')
    ap.add_argument('--assets', required=True, help='directory holding the base asset bodies at their R2 keys')
    ap.add_argument('--base-holds', required=True, help='JSON array of the base asset delivery_keep_holds rows')
    ap.add_argument('--best14', required=True, help='best14-days.tsv')
    sel = ap.add_mutually_exclusive_group(required=True)
    sel.add_argument('--files', help='file with one selected parent file id per line')
    sel.add_argument('--days', help='TSV with recording_id and local_date columns')
    ap.add_argument('--clock-assemblies', nargs='+', default=[], help='clock-join output directories whose assemblies to add')
    ap.add_argument('--note', required=True, help='publication_note of the new generation')
    ap.add_argument('--out', required=True, help='output directory (must not exist)')
    a = ap.parse_args(argv)
    parent = read_registry(a.parent)
    store = AssetStore(read_registry(a.base), a.assets)
    present = select_files(parent, read_lines(a.files)) if a.files else select_days(parent, store, read_days(a.days))
    with open(a.base_holds, encoding='utf-8') as f:
        holds = json.load(f)
    now = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    summary = build(parent, store, present, holds, secrets.token_hex(16), now, a.note, a.out, read_best14(a.best14),
                    read_clock_assemblies(a.clock_assemblies))
    print(json.dumps({k: v for k, v in summary.items() if k != 'known_stale_fields'}, indent=1))


if __name__ == '__main__':
    main()
