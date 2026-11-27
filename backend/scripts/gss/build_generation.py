"""Build a Global Street Scores (GSS) public delivery generation.

Rebuilds every page asset of a base generation for a file selection, marks each
stream available or partial for its best 14 days, serves this directory's
download_script.py as /download-script.py, and writes into a new, empty
output directory:

  registry.json.gz   the new public registry (gzip, deterministic)
  objects/<key>      every new content-addressed asset body, laid out by R2 key
  uploads.tsv        key, local_path, sha256, size_bytes, content_type, etag
  holds.tsv          delivery_keep_holds rows to insert (psql \\copy ... WITH (FORMAT text, HEADER))
  summary.json       counts plus the known-stale fields this build does not recompute

It never uploads and never writes the database. The controller uploads each
uploads.tsv row create-only (If-None-Match: *) and checks the returned ETag
equals the listed one (R2's single-part ETag is the body MD5), inserts
holds.tsv, then stages registry.json.gz create-only as
public/generations/<id>.json. Moving public/current.json (preview, If-Match
swap) is a separate, reviewed step. A key already in the parent registry is
neither uploaded nor held again.

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
least one R, its base timeline covers exactly those days, and every captured hour keeps all its
base parts; otherwise it is "partial" and serves what is selected.

--base-holds is the base asset holds, exported read-only:
  psql -At -c "SELECT json_agg(json_build_object('object_key',object_key,
    'kind',kind,'recording_id',recording_id,'relative_path',relative_path))
    FROM delivery_keep_holds WHERE bucket='stoarama'
    AND object_key LIKE 'datasets/global-street-scores-delivery/47/assets/%'"

--ledger publishes from the best-14 ledger instead of a file selection. It takes
best14_ledger.py's inputs (--registry, the live generation, is both parent and
base) plus --grades (best14-grades.tsv), recomputes the ledger, and publishes
every ledger file in R2: complete hours whole, other hours with what R2 holds.
- A file the live generation lists keeps its entry and path; any other part is
  added under its catalogued key, sha256, size and live ETag, at its NAS
  relative path, with a video_collated delivery_keep_holds row unless an
  identical hold exists (a differing hold fails).
- Each stream's timeline, manifest, catalog weeks and metadata window become its
  best-14 days; an hour whose parts are unchanged keeps its live hour as is.
- Fails closed, before writing, when any stream-day of the live generation
  would serve fewer complete hours or fewer assemblies (days outside the new
  window serve none). A live assembly on best14_ledger's veto list
  (assembly-veto.tsv) does not count: its parts serve in its place, and the
  build fails if a vetoed assembly would be published.
- Every served day's grade is best14-grades.tsv's; a day it lacks fails. --dry-run builds everything in memory, writes nothing and
  prints the counts, including those regressions.
"""
import argparse
import collections
import copy
import csv
import dataclasses
import datetime
import enum
import gzip
import hashlib
import json
import os
import secrets
import statistics
import sys
from pathlib import Path

ASSET_PREFIX = 'datasets/global-street-scores-delivery/47/assets/'
BUCKET = 'stoarama'
MAX_ASSET_BYTES = 64 << 20

READY_DAY_CODES = frozenset('R.')
DAY_CODES = frozenset('RNSs.')
NO_CAPTURE_STATES = frozenset({'gap_only', 'quarantine_only'})
FULL_DELIVERY_STATES = frozenset({'all_scheduled_manifests_present', 'verified_original_outputs_quality_unassessed'})


class Availability(str, enum.Enum):
    AVAILABLE = 'available'
    PARTIAL = 'partial'


# Assets this builder carries over byte for byte: they do not list files.
UNTOUCHED_ASSETS = ('/public/images.json', '/public/blue-observations.json')

# Every generation serves this repository's download script (one-line.js pins
# its sha256 from the manifest's download_script_sha256).
DOWNLOAD_SCRIPT_ASSET = '/download-script.py'
DOWNLOAD_SCRIPT = Path(__file__).resolve().with_name('download_script.py')

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


def rebuild(store, recording_ids, present, gen_id, now, refresh_paths=False):
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
    kept_repairs = [r for r in gr['repairs'] if 'sha256:' + r['sha256'] in present]
    for rid in recording_ids:
        man = load(f'/public/manifests/{rid}.json')
        assert man == load(f'/api/coverage-delivery/manifest?stream={rid}&scope=available'), rid
        outputs = [o for o in man['outputs'] if str(o['id']) in present]
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
        stream_changed = refresh_paths or sp2 != sp or any(k not in present for k in sp.get('current_download_paths', {}))
        if stream_changed:
            changed_streams.add(rid)
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
                             'pct_of_captured_timeline': 100 * u / R['captured_scheduled_seconds'] if R['captured_scheduled_seconds'] else 0.0,
                             'pct_of_registered_represented_timeline': 100 * u / U if U else 0.0, 'parts': len(sel)}
                R['length_thresholds'] = th
            else:
                R['length_thresholds'] = {}
        if stream_changed:
            state = 'awaiting_verified_outputs' if not parts else 'partial_registered_delivery' if s['registered_hours'] < R['scheduled_slots'] \
                else R['delivery_state'] if R['delivery_state'] in FULL_DELIVERY_STATES else 'all_scheduled_manifests_present'
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


def new_body(p, out, store):
    """The new generation's bytes for asset p, or None when it keeps the base object."""
    if p in UNTOUCHED_ASSETS or p.startswith('/public/images/'):
        return None
    old = store.raw(p)
    if p == DOWNLOAD_SCRIPT_ASSET:
        body = DOWNLOAD_SCRIPT.read_bytes()
        return None if body == old else body
    gz = p.endswith('.gz')
    obj = out[p[:-3] if gz else p]
    if json.loads(gzip.decompress(old) if gz else old) == obj:
        return None
    body = dumps(obj)
    return gzip.compress(body, compresslevel=9, mtime=0) if gz else body


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


def build(parent, store, present, holds, gen_id, now, note, out_dir, best14=None, added=None, file_holds=(), refresh_paths=False):
    """Write the generation into out_dir (must not exist; None is a dry run that
    writes nothing). Returns the summary.

    store holds the base assets. best14=None skips availability (only to
    reproduce generations built before it existed). added maps file ids the
    parent does not list to their registry entries; file_holds are the
    delivery_keep_holds rows those files still need."""
    if store.recording_ids != parent['recording_ids']:
        raise SystemExit('base and parent recordings differ')
    files = {k: v for k, v in parent['files'].items() if k in present}
    files.update(added or {})
    if set(files) != set(present):
        raise SystemExit(f'selected files missing from the parent and the additions: {sorted(set(present) - set(files))[:3]}')
    dup = sorted(p for p, n in collections.Counter(v['path'] for v in files.values()).items() if n > 1)
    if dup:
        raise SystemExit(f'{len(dup)} delivery paths are listed by more than one file, e.g. {dup[:3]}')
    assets = store.assets
    out, changed = rebuild(store, parent['recording_ids'], present, gen_id, now, refresh_paths)
    availability = annotate_availability(out, store, best14) if best14 is not None else None
    published = {o['key'] for o in parent['assets'].values()}
    hold_by_key = {h['object_key']: h for h in holds}
    new_assets = copy.deepcopy(assets)
    uploads = {}
    new_holds = {h['object_key']: h for h in file_holds}
    if out_dir is not None:
        os.makedirs(out_dir)
    for p in assets:
        body = new_body(p, out, store)
        if body is None:
            continue
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
        if out_dir is not None:
            local = os.path.join(out_dir, 'objects', key)
            os.makedirs(os.path.dirname(local), exist_ok=True)
            with open(local, 'wb') as f:
                f.write(body)
        uploads[key] = {'key': key, 'local_path': os.path.join('objects', key), 'sha256': sha,
                        'size_bytes': len(body), 'content_type': assets[p]['content_type'], 'etag': etag}
    registry = dict(parent, generation=gen_id, parent_generation=parent['generation'], created_at=now,
                    publication_note=note, assets=new_assets, files=files)
    summary = {
        'generation': gen_id, 'parent_generation': parent['generation'], 'created_at': now,
        'files': len(files), 'recordings_with_files': len({v['recording_id'] for v in files.values()}),
        'changed_recordings': sorted(changed), 'assets_replaced': sum(new_assets[p] != assets[p] for p in assets),
        'objects': len(uploads), 'holds': len(new_holds), 'added_files': len(added or {}), 'availability': availability,
        'stream_availability': {str(s['id']): s.get('availability') for s in out['/public/metadata.json']['streams']},
        'bytes_served': sum(v['size_bytes'] for v in files.values()), 'known_stale_fields': list(KNOWN_STALE_FIELDS),
    }
    if out_dir is not None:
        with open(os.path.join(out_dir, 'registry.json.gz'), 'wb') as f:
            f.write(gzip.compress(dumps(registry), compresslevel=9, mtime=0))
        write_tsv(os.path.join(out_dir, 'uploads.tsv'), UPLOAD_COLUMNS, [uploads[k] for k in sorted(uploads)])
        write_tsv(os.path.join(out_dir, 'holds.tsv'), HOLD_COLUMNS, [new_holds[k] for k in sorted(new_holds)])
        write_json(os.path.join(out_dir, 'summary.json'), summary)
    return summary


# ---- publishing from the best-14 ledger ---------------------------------------

FILE_URL = '/api/v1/global-street-scores-delivery/file/'
DAY_SCHEDULED_SECONDS = 12 * 3600
FIRST_LOCAL_HOUR = 8
COLLATED_HOLD_KIND = 'video_collated'

# What publishing from the ledger rebuilds only approximately for a day the
# parent timeline does not hold (no source accounting is extracted for it).
LEDGER_STALE_FIELDS = (
    'streams/<id>.json new-day hours: [1] captured and [2] represented are the sum of the hour\'s part spans, [3] excluded 0',
    'streams/<id>.json new-day parts: [6] source clips 0',
    'streams/<id>.json new days: captured_scheduled_seconds is the sum of its hours\' [1], redone when a parent day built this way changes',
)


class OverlayStore(AssetStore):
    """The base assets, with some parsed bodies replaced (raw() still returns the base bytes)."""

    def __init__(self, base, overlay):
        self.__dict__.update(base.__dict__)
        self.overlay = overlay

    def load(self, path):
        return copy.deepcopy(self.overlay[path]) if path in self.overlay else super().load(path)


def epoch(ts):
    return datetime.datetime.fromisoformat(ts).timestamp()


def hour_bounds(tz, date, delivery_hour):
    from zoneinfo import ZoneInfo
    d = datetime.date.fromisoformat(date)
    t0 = datetime.datetime(d.year, d.month, d.day, FIRST_LOCAL_HOUR + delivery_hour - 1, tzinfo=ZoneInfo(tz)).timestamp()
    return t0, t0 + 3600


def read_grades(path):
    return {(int(r['recording_id']), r['local_date']): r['grade'] for r in read_tsv(path, ('recording_id', 'local_date', 'grade'))}


def ledger_generation(inputs, hours, store, grades, is_assembly, windows):
    """(overlay of rebuilt base assets, present file ids, added or renamed registry files,
    new file holds).

    Every servable ledger file (in R2, and held or holdable) is published at its
    delivery name (delivery_naming): a file the parent lists keeps its entry, renamed
    when its parent path is not that name; any other part is added under its
    catalogued key. Every published path must pass delivery_naming.check_paths. Each
    stream's timeline becomes its chosen window; a parent hour whose parts are
    unchanged keeps its accounting."""
    from delivery_naming import check_paths, delivery_path
    reg = inputs.registry
    files = reg['files']
    md, rd, cat = (store.load(p) for p in ('/public/metadata.json', '/public/readiness.json', '/public/catalog.json'))
    MS = {s['id']: s for s in md['streams']}
    RS = rd['by_recording_id']
    CS = {s['id']: s for s in cat['streams']}
    by_rid = collections.defaultdict(list)
    for h in hours:
        by_rid[h.recording_id].append(h)
    overlay, present, added, file_holds, renamed, published, periods = {}, set(), {}, [], {}, [], {}
    for rid in reg['recording_ids']:
        hs = sorted(by_rid[rid], key=lambda h: h.slot)
        tz = MS[rid]['timezone']
        man = store.load(f'/public/manifests/{rid}.json')
        base_out = {str(o['id']): o for o in man['outputs']}
        sp = store.load(f'/public/streams/{rid}.json')
        base_days = {d['date']: d for d in sp['days']}
        base_parts = {str(p[0]): p for d in sp['days'] for h in d['hours'] for p in h[9]}
        part_of = {str(p.output_id): (h, p) for h in hs if h.record for p in h.record.parts}
        pub = [(h, x) for h in hs for x in h.served if x.live]
        served_path = {x.file_id: x.path for h in hs for x in h.served}
        on_timeline = {i for _, x in pub for i in x.registry_ids}
        outputs = []
        for h, x in pub:
            present.add(x.file_id)
            published.append((rid, h.local_date, x.path))
            if x.file_id in files:
                f = files[x.file_id]
                if (f['key'], f['size_bytes'], f['sha256']) != (x.key, x.size_bytes, x.sha256):
                    raise SystemExit(f'ledger file {x.file_id} differs from its parent entry')
                if x.file_id not in base_out:
                    raise SystemExit(f'parent file {x.file_id} is not in the manifest of recording {rid}')
                if f['path'] != x.path:
                    added[x.file_id] = dict(f, path=x.path)
                    renamed[x.file_id] = x.path
                outputs.append(dict(base_out[x.file_id], path=x.path))
                continue
            if is_assembly(x):
                raise SystemExit(f'assembly {x.file_id} is not in the parent: publishing new assemblies needs the clock-join builder')
            _, p = part_of[x.file_id]
            added[x.file_id] = {'etag': f'"{x.etag}"', 'key': x.key, 'path': x.path, 'recording_id': rid,
                                'sha256': x.sha256, 'size_bytes': x.size_bytes, 'verified': True}
            held = inputs.holds.get(x.key)
            if held is None:
                file_holds.append({'bucket': BUCKET, 'object_key': x.key, 'etag': x.etag, 'size_bytes': x.size_bytes,
                                   'recording_id': rid, 'kind': COLLATED_HOLD_KIND, 'sha256': x.sha256, 'relative_path': x.path})
            elif held != (x.etag, x.size_bytes):
                raise SystemExit(f'{x.key}: its keep hold {held} differs from R2 {(x.etag, x.size_bytes)}')
            outputs.append({'id': p.output_id, 'size_bytes': x.size_bytes, 'sha256': x.sha256, 'part': p.part, 'parts': p.parts,
                            'start_at': p.start_at, 'end_at': p.end_at, 'recording_id': rid, 'local_date': h.local_date,
                            'delivery_hour': h.delivery_hour, 'available': True, 'scope': 'audited',
                            'collections': ['audited', 'available'], 'path': x.path, 'file_url': FILE_URL + x.file_id})
        days = []
        window = sorted({h.local_date for h in hs})
        for date in window:
            bd = base_days.get(date)
            day_hours = []
            for h in (h for h in hs if h.local_date == date):
                t0, t1 = hour_bounds(tz, date, h.delivery_hour)
                ids = [str(p.output_id) for p in h.record.parts if str(p.output_id) in on_timeline] if h.record else []
                state = 'collated' if ids else 'gap_only' if not h.captured else 'missing_output' if h.record else 'unregistered'
                bh = bd['hours'][h.delivery_hour - 1] if bd else None
                def name(i):
                    _, p = part_of[i]
                    return (served_path.get(i) or delivery_path(inputs.names, rid, h.local_date, h.delivery_hour, p.part, p.parts)).rsplit('/', 1)[-1]
                if bh is not None and [str(q[0]) for q in bh[9]] == ids and (bh[0] == state or (not h.captured and bh[0] in NO_CAPTURE_STATES)):
                    day_hours.append(bh[:9] + [[q[:8] + [name(str(q[0]))] + q[9:] for q in bh[9]]] + bh[10:])
                    continue
                parts = []
                for i in ids:
                    if i in base_parts:
                        q = base_parts[i]
                        parts.append(q[:8] + [name(i)] + q[9:])
                        continue
                    _, p = part_of[i]
                    a, b_ = epoch(p.start_at), epoch(p.end_at)
                    parts.append([p.output_id, p.part, b_ - a, a - t0, b_ - t0, b_ - a, 0, 0, name(i), 1, None])
                span = sum(q[2] for q in parts)
                rec = h.record
                day_hours.append([state, span, span, 0.0, rec.source_clips if rec else 0, rec.included_clips if rec else 0, 0,
                                  t0, t1, parts, [], h.delivery_hour])
            # A parent day keeps its source accounting. A day whose accounting is its hours' [1]
            # (built here, LEDGER_STALE_FIELDS) is redone from the new hours, so a stream
            # published before its hours were registered gains their time.
            # best14-grades.tsv owns every day's grade, so a corrected grade replaces the parent's.
            day = dict(bd or {'date': date}, grade=grades[(rid, date)])
            captured = sum(x[1] for x in day_hours)
            if not bd or bd['captured_scheduled_seconds'] == sum(x[1] for x in bd['hours']) != captured:
                day.update(captured_scheduled_seconds=captured, captured_scheduled_coverage_pct=100 * captured / DAY_SCHEDULED_SECONDS)
            days.append(dict(day, hours=day_hours))
        start, end = window[0], window[-1]
        man2 = dict(man, period=[start, end], outputs=outputs)
        overlay[f'/public/manifests/{rid}.json'] = overlay[f'/api/coverage-delivery/manifest?stream={rid}&scope=available'] = man2
        dirs = list(dict.fromkeys(o['path'].rsplit('/', 1)[0] for o in outputs))
        overlay[f'/public/streams/{rid}.json'] = dict(sp, days=days, directories=dirs)
        rp = f'/public/ranges/{rid}.json'
        if rp in store.assets:
            rg = store.load(rp)
            for i in on_timeline - set(rg['by_output_id']):
                _, p = part_of[i]
                rg['by_output_id'][i] = {'sha256': p.sha256, 'ranges': [[epoch(p.start_at), epoch(p.end_at)]]}
            overlay[rp] = rg
        hs_cap = [h for h in hs if h.captured]
        registered = sum(1 for h in hs_cap if h.record is not None and h.record.status == 'collated')
        s = MS[rid]
        b14 = inputs.best14[rid][1]
        s.update(window=windows[rid].value, best14_dates=[b14[0][0], b14[-1][0]])
        s.update(start=start, end=end, period_anchor=start, weekly_period_anchor=start, grades=''.join(d['grade'] for d in days),
                 captured_scheduled_seconds=sum(d['captured_scheduled_seconds'] for d in days),
                 registered_hours=registered, missing_registration_hours=len(hs_cap) - registered)
        RS[str(rid)].update(start=start, end=end, captured_scheduled_seconds=s['captured_scheduled_seconds'])
        w2 = (datetime.date.fromisoformat(start) + datetime.timedelta(days=7)).isoformat()
        w1end = (datetime.date.fromisoformat(start) + datetime.timedelta(days=6)).isoformat()
        c = CS[rid]
        c.update(start=start, end=end, available_start=start, available_end=end)
        if outputs:
            c['folder'] = inputs.names[rid][0]  # the delivery folder of every path it serves
        periods[rid] = [start, end]
        c['weekly_exports']['available'] = {'period': {'start': start, 'end': end},
                                            'weeks': [{'number': 1, 'start': start, 'end': w1end}, {'number': 2, 'start': w2, 'end': end}]}
    problems = check_paths(inputs.names, published)
    if problems:
        raise SystemExit(f'{len(problems)} published paths break the delivery naming, e.g. {problems[:3]}')
    gr = store.load('/public/growth.json')
    for rid, period in periods.items():
        gr['current_stream_counts'][str(rid)]['period'] = period  # coverage-growth.js registered() compares it
    for r in gr['repairs']:
        if 'sha256:' + r['sha256'] in renamed:
            r['path'] = renamed['sha256:' + r['sha256']]
    overlay['/public/metadata.json'], overlay['/public/readiness.json'], overlay['/public/catalog.json'] = md, rd, cat
    overlay['/public/growth.json'] = gr
    return overlay, present, added, file_holds


class Window(str, enum.Enum):
    """The days a stream's timeline shows: the live generation's, or its best-14."""
    ORIGINAL = 'original'
    BEST14 = 'best14'


LIVE_NO_CAPTURE = frozenset({'gap_only', 'gap_unregistered', 'quarantine_only'})


def live_windows(registry, store, best14):
    """Each stream's live timeline days as a ledger window (an hour the live timeline shows
    without capture is '.')."""
    out = {}
    for rid in registry['recording_ids']:
        days = store.load(f'/public/streams/{rid}.json')['days']
        out[rid] = (best14[rid][0], [(d['date'], ''.join('.' if h[0] in LIVE_NO_CAPTURE else 'R' for h in d['hours'])) for d in days])
    return out


def live_counts(inputs, store):
    """{(recording_id, local_date): [complete hours, assemblies]} the live generation serves.
    An hour is complete when one of its records has every part listed, directly or as an
    assembly input. A vetoed assembly is not counted: the new generation serves its parts."""
    from best14_ledger import VETOED
    files = inputs.registry['files']
    inputs_of = {'sha256:' + r['sha256']: [str(i) for i in r['input_ids']] for r in store.load('/public/growth.json')['repairs']}
    slot_of = {str(p.output_id): slot for slot, rs in inputs.records.items() for r in rs for p in r.parts}
    covered = {i for f in files for i in (inputs_of[f] if f.startswith('sha256:') else [f])}
    unknown = sorted(i for i in covered if i not in slot_of)
    if unknown:
        raise SystemExit(f'{len(unknown)} live parts are not in the extract, e.g. {unknown[:3]}')
    live = collections.defaultdict(lambda: [0, 0])
    for slot, rs in inputs.records.items():
        if any(r.complete() and all(str(p.output_id) in covered for p in r.parts) for r in rs):
            live[slot[:2]][0] += 1
    for f in files:
        if f.startswith('sha256:') and f not in VETOED:
            live[slot_of[inputs_of[f][0]][:2]][1] += 1
    return live


def new_counts(hours, is_assembly):
    new = collections.defaultdict(lambda: [0, 0])
    for h in hours:
        new[h.slot[:2]][0] += h.ok
        new[h.slot[:2]][1] += sum(1 for x in h.served if is_assembly(x))
    return new


def regressions(live, new, days):
    """The days where the new generation serves fewer complete hours or fewer assemblies than live."""
    return {d: (live[d], new[d]) for d in sorted(days) if new[d][0] < live[d][0] or new[d][1] < live[d][1]}


def choose_windows(best14_hours, live_hours, live, is_assembly):
    """{recording_id: Window}: best-14 when every best-14 day is complete in R2, or when it
    serves at least the live window's complete hours and assemblies; else the live window."""
    def totals(counts, hours):
        out = collections.defaultdict(lambda: [0, 0])
        for day in {h.slot[:2] for h in hours}:
            out[day[0]][0] += counts[day][0]
            out[day[0]][1] += counts[day][1]
        return out
    b14, old = totals(new_counts(best14_hours, is_assembly), best14_hours), totals(live, live_hours)
    captured = collections.defaultdict(list)
    for h in best14_hours:
        if h.captured:
            captured[h.recording_id].append(h.ok)
    complete = {rid for rid, oks in captured.items() if all(oks)}
    return {rid: Window.BEST14 if rid in complete or (b14[rid][0] >= old[rid][0] and b14[rid][1] >= old[rid][1]) else Window.ORIGINAL
            for rid in sorted({h.recording_id for h in best14_hours})}


def main_ledger(a):
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import best14_ledger
    is_assembly = lambda x: x.source in best14_ledger.ASSEMBLY_SOURCES
    try:
        inputs, store = best14_ledger.load(a)
        best14_hours = best14_ledger.select(inputs)
        live_hours = best14_ledger.select(dataclasses.replace(inputs, best14=live_windows(inputs.registry, store, inputs.best14)))
        live = live_counts(inputs, store)
        windows = choose_windows(best14_hours, live_hours, live, is_assembly)
        chosen = [h for h in best14_hours if windows[h.recording_id] is Window.BEST14] + \
                 [h for h in live_hours if windows[h.recording_id] is Window.ORIGINAL]
        best14_ledger.check_rules(chosen, inputs.names)
    except best14_ledger.LedgerError as e:
        raise SystemExit(f'best14 ledger: {e}')
    lost = regressions(live, new_counts(chosen, is_assembly), {h.slot[:2] for h in chosen})
    report = {'windows': {w.value: sum(v is w for v in windows.values()) for w in Window},
              'regressed_stream_days': len(lost),
              'regressions_live_vs_new_[complete_hours,assemblies]': {f'{r} {d}': v for (r, d), v in lost.items()}}
    if lost and not a.dry_run:
        print(json.dumps(report, indent=1))
        raise SystemExit(f'{len(lost)} stream-days would serve fewer complete hours or assemblies than the live generation')
    overlay, present, added, file_holds = ledger_generation(inputs, chosen, store, read_grades(a.grades), is_assembly, windows)
    vetoed = sorted(present & best14_ledger.VETOED)
    if vetoed:
        raise SystemExit(f'vetoed assemblies would be published: {vetoed}')
    best14 = collections.defaultdict(set)
    for d in best14_ledger.day_rows(best14_hours):
        best14[d['recording_id']].add((d['local_date'], d['day_stitched']))
    with open(a.base_holds, encoding='utf-8') as f:
        holds = json.load(f)
    now = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    summary = build(inputs.registry, OverlayStore(store, overlay), present, holds, secrets.token_hex(16), now, a.note,
                    None if a.dry_run else a.out, best14, added, file_holds, refresh_paths=True)
    summary.update(report, window={str(rid): w.value for rid, w in windows.items()},
                   best14_days_ready={str(rid): sum(c in READY_DAY_CODES for _, c in v) for rid, v in sorted(best14.items())},
                   known_stale_fields=list(KNOWN_STALE_FIELDS) + list(LEDGER_STALE_FIELDS))
    if a.out and not a.dry_run:
        write_json(os.path.join(a.out, 'summary.json'), summary)
    if a.ledger_out and not a.dry_run:
        pub = best14_ledger.Publication(summary['generation'], False, {rid: w.value for rid, w in windows.items()},
                                        {int(k): v for k, v in summary['stream_availability'].items()})
        summary['ledger'] = best14_ledger.write(a.ledger_out, chosen, best14_hours, inputs.unheld, pub, present)
    return summary


UPLOAD_COLUMNS = ('key', 'local_path', 'sha256', 'size_bytes', 'content_type', 'etag')
HOLD_COLUMNS = ('bucket', 'object_key', 'etag', 'size_bytes', 'recording_id', 'kind', 'sha256', 'relative_path')


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
    ap.add_argument('--parent', help='public registry being replaced (.json or .json.gz)')
    ap.add_argument('--base', help='registry whose assets are rebuilt (.json or .json.gz)')
    ap.add_argument('--assets', required=True, help='directory holding the base asset bodies at their R2 keys')
    ap.add_argument('--base-holds', required=True, help='JSON array of the base asset delivery_keep_holds rows')
    ap.add_argument('--best14', required=True, help='best14-days.tsv')
    sel = ap.add_mutually_exclusive_group(required=True)
    sel.add_argument('--files', help='file with one selected parent file id per line')
    sel.add_argument('--days', help='TSV with recording_id and local_date columns')
    sel.add_argument('--ledger', action='store_true', help='publish every best-14 ledger file in R2 (best14_ledger.py inputs)')
    led = ap.add_argument_group('--ledger inputs (see best14_ledger.py; --registry is the live generation, parent and base)')
    led.add_argument('--registry')
    led.add_argument('--extract')
    led.add_argument('--holds')
    led.add_argument('--listing', nargs='+')
    led.add_argument('--clock-proofs')
    led.add_argument('--grades', help='best14-grades.tsv (recording_id, local_date, grade)')
    led.add_argument('--ledger-out', help='write the ledger of the new generation here (best14-ledger*.tsv, summary)')
    ap.add_argument('--note', required=True, help='publication_note of the new generation')
    out = ap.add_mutually_exclusive_group(required=True)
    out.add_argument('--out', help='output directory (must not exist)')
    out.add_argument('--dry-run', action='store_true', help='build in memory, write nothing, print the counts')
    a = ap.parse_args(argv)
    if a.ledger:
        missing = [n for n in ('registry', 'extract', 'holds', 'listing', 'grades') if getattr(a, n) is None]
        if missing or a.parent or a.base:
            raise SystemExit(f'--ledger takes --registry (not --parent/--base) and needs --{", --".join(missing) or "registry"}')
        summary = main_ledger(a)
    else:
        if not (a.parent and a.base) or a.dry_run:
            raise SystemExit('--files/--days need --parent, --base and --out')
        parent = read_registry(a.parent)
        store = AssetStore(read_registry(a.base), a.assets)
        present = select_files(parent, read_lines(a.files)) if a.files else select_days(parent, store, read_days(a.days))
        with open(a.base_holds, encoding='utf-8') as f:
            holds = json.load(f)
        now = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        summary = build(parent, store, present, holds, secrets.token_hex(16), now, a.note, a.out, read_best14(a.best14))
    print(json.dumps({k: v for k, v in summary.items() if k != 'known_stale_fields'}, indent=1))

if __name__ == '__main__':
    main()
