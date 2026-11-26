"""Ledger of what serves each Global Street Scores (GSS) best-14 stream-day hour.

Deniz, Nov 25: "just make sure we're keeping a sane set and knowing we've
stitched, and knowing we have full best 14 day periods". The ledger is the one
source of truth per best-14 stream-day and delivery hour (1-12, local 08-19).
It is read-only: it reads a DB extract, the keep holds, a live R2 listing and
the live public generation, and writes only local files. build_generation.py
--ledger publishes from the same selection.

Inputs:
  --best14     best14-days.tsv (recording_id, stream_id, local_date, hours_08_19;
               14 rows per recording). hours_08_19 decides capture only for an
               hour with no hour record: '.' is no capture.
  --registry, --assets  the live generation (current.json) and a mirror of its
               asset bodies laid out by R2 key, as build_generation.py reads them.
  --extract    `extract-sql` run read-only (default_transaction_read_only=on) with
               psql -A -F TAB: every hour record and part on the best-14 days and
               on the live generation's timeline days.
  --holds      `holds-sql` run the same way: delivery_keep_holds key, etag, size.
  --listing    live listings of joined/ and experimental/, one file each:
               aws s3api list-objects-v2 --bucket B --prefix P
                 --query 'Contents[].[Key,Size,ETag]' --output text
  --clock-proofs  optional directory of clock-join proof JSONs, copied read-only
               from joined/clock-prestitch-*/proofs/.

An assembly serves in place of the consecutive parts it was built from (across
an hour boundary, from the end of one hour into the start of the next): a live
generation assembly when its key is in the listing at the registry's size and
ETag and holds a matching keep hold, a clock-join assembly when its key is in
the listing. It is listed on the hour of its first part. Per hour the serving
set is one hour record with its assemblies. Among an hour's records the one
complete in R2 serves, preferring a non-recovery record, then the earliest
registered; `alternates` counts the other complete-in-R2 ones.

Source of each served file:
  v85_assembly, v79_assembly, v49_assembly, strict_assembly, reviewed_assembly
                  a live generation assembly, by its growth.json repair kind
  clock_assembly  a clock-prestitched assembly (live generation or clock-join)
  new_stitch      a part of an hour registered in a recovery- batch
  nas_upload      a part restored from the NAS (nas_restore_requests target
                  'output', state verified)
  restore         a part re-uploaded by a collation-v2 recovery run (a completed
                  r2_retirement_writer_operations recovery intent on its key)
  original        any other registered part

complete = every part 1..parts of the record is registered; in_r2 = every file
serving the record's parts is servable: in the listing at its catalogued size
and, for a part, keep-held already or clear of the delivery hold guard's
collated-source closure (no collated retirement reservation, nonraw intent or
retirement on the output, no raw reservation or inverse intent on a source
clip). A blocked part leaves its hour incomplete (closed_parts counts them). A day is
complete when every captured hour is complete and in R2; a stream is available
when all 14 days are complete and at least one hour is captured.

Rules, each a hard failure: no hour is served by two sets (one record per hour,
every file from a serving record); an assembly replaces its parts and never
duplicates them (no part is served twice, directly or by two assemblies, and an
assembly's parts are all in serving records); no two served files share a
delivery path. A delivery path is the live generation's path for a file it
lists, else the catalogued NAS collated path; a clock-join assembly takes its
parts' own name format.

Outputs, in --out-dir:
  best14-ledger.tsv          one row per (recording_id, local_date, delivery_hour)
  best14-ledger-streams.tsv  per stream: days complete of 14, available y/n
"""
import argparse
import collections
import csv
import dataclasses
import datetime
import enum
import json
import os
import re
import sys
import unicodedata
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import build_generation  # noqa: E402
import delivery_naming  # noqa: E402

BUCKET = build_generation.BUCKET

HOURS = range(1, 13)  # delivery hours 1-12 = local 08-19
FIRST_LOCAL_HOUR = 8
CLOCK_PREFIX = 'joined/clock-prestitch-'
RECOVERY_BATCH_PREFIX = 'recovery-'
NO_CAPTURE_STATES = frozenset({'gap_only', 'quarantine_only'})
COLLATED = 'collated'
SEP = ','


class Source(str, enum.Enum):
    ORIGINAL = 'original'
    RESTORE = 'restore'
    NAS_UPLOAD = 'nas_upload'
    NEW_STITCH = 'new_stitch'
    V85_ASSEMBLY = 'v85_assembly'
    V79_ASSEMBLY = 'v79_assembly'
    V49_ASSEMBLY = 'v49_assembly'
    STRICT_ASSEMBLY = 'strict_assembly'
    REVIEWED_ASSEMBLY = 'reviewed_assembly'
    CLOCK_ASSEMBLY = 'clock_assembly'


# growth.json repair kind -> source of a live generation assembly.
REPAIR_KINDS = {'model85': Source.V85_ASSEMBLY, 'model79': Source.V79_ASSEMBLY, 'model49': Source.V49_ASSEMBLY,
                'strict': Source.STRICT_ASSEMBLY, 'reviewed': Source.REVIEWED_ASSEMBLY,
                'modelclock-prestitched': Source.CLOCK_ASSEMBLY}
ASSEMBLY_SOURCES = frozenset(REPAIR_KINDS.values())


class LedgerError(Exception):
    pass


@dataclasses.dataclass(frozen=True)
class Part:
    output_id: int
    part: int
    parts: int
    key: str
    size_bytes: int
    sha256: str
    start_at: str
    end_at: str
    nas_path: str
    source: Source
    closed: bool = False  # a retirement closure clause holds: no new keep hold can land on it


@dataclasses.dataclass(frozen=True)
class Record:
    row_id: int
    hour_id: str
    batch_id: str
    status: str
    registered_at: str
    parts: tuple
    source_clips: int = 0
    included_clips: int = 0

    def complete(self):
        return bool(self.parts) and len({p.parts for p in self.parts}) == 1 and \
            sorted(p.part for p in self.parts) == list(range(1, self.parts[0].parts + 1))


@dataclasses.dataclass(frozen=True)
class Assembly:
    file_id: str
    key: str
    size_bytes: int
    etag: str
    sha256: str
    input_ids: tuple
    source: Source
    path: str  # None for a clock-join assembly: named from its parts


@dataclasses.dataclass(frozen=True)
class Served:
    file_id: str
    key: str
    size_bytes: int
    etag: str
    sha256: str
    path: str
    source: Source
    registry_ids: tuple  # the parts this file serves: itself, or an assembly's inputs
    live: bool = True    # servable: in R2, and held or holdable


@dataclasses.dataclass(frozen=True)
class Hour:
    recording_id: int
    stream_id: str
    local_date: str
    delivery_hour: int
    captured: bool
    record: Record = None
    served: tuple = ()
    complete: bool = False
    in_r2: bool = False
    alternates: int = 0
    covered_by: tuple = ()  # assemblies listed on an earlier hour that serve this hour's first parts

    @property
    def ok(self):
        return self.complete and self.in_r2

    @property
    def slot(self):
        return self.recording_id, self.local_date, self.delivery_hour


@dataclasses.dataclass(frozen=True)
class Inputs:
    best14: dict
    records: dict      # (recording_id, local_date, delivery_hour) -> [Record]
    assemblies: list   # live Assembly
    listing: dict      # key -> (size, etag)
    registry: dict     # the live generation registry
    holds: dict        # delivery_keep_holds key -> (etag, size)
    unheld: int = 0    # live generation assemblies in R2 without a matching keep hold
    names: dict = None  # recording_id -> (delivery folder, file stem), delivery_naming.catalog_names()


def yn(b):
    return 'y' if b else 'n'


# ---- inputs ---------------------------------------------------------------

def read_tsv(path, columns, empty_ok=False):
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='\t')
        rows = list(reader)
    if (not rows and not empty_ok) or not set(columns) <= set(reader.fieldnames or ()):
        raise LedgerError(f'{path}: need a TSV header with {", ".join(columns)}')
    return rows


BEST14_COLUMNS = ('recording_id', 'stream_id', 'local_date', 'hours_08_19')


def read_best14(path):
    """{recording_id: (stream_id, [(local_date, hours_08_19)] in date order)}."""
    out = {}
    for r in read_tsv(path, BEST14_COLUMNS):
        datetime.date.fromisoformat(r['local_date'])
        if len(r['hours_08_19']) != len(HOURS):
            raise LedgerError(f'{path}: hours_08_19 needs {len(HOURS)} codes: {r}')
        rid = int(r['recording_id'])
        stream, days = out.setdefault(rid, (r['stream_id'], []))
        if stream != r['stream_id']:
            raise LedgerError(f'{path}: recording {rid} has two stream ids')
        days.append((r['local_date'], r['hours_08_19']))
    for rid, (_, days) in out.items():
        days.sort()
        dates = [datetime.date.fromisoformat(d) for d, _ in days]
        if len(dates) != 14 or (dates[-1] - dates[0]).days != 13 or len(set(dates)) != 14:
            raise LedgerError(f'{path}: recording {rid} needs 14 consecutive days')
    return out


def live_days(registry, store):
    """Every (recording_id, local_date) on the live generation's timelines."""
    return {(rid, d['date']) for rid in registry['recording_ids'] for d in store.load(f'/public/streams/{rid}.json')['days']}


def extract_sql(days):
    values = ','.join(f"({rid},date '{d}')" for rid, d in sorted(days))
    return f"""WITH d(recording_id, local_date) AS (VALUES {values})
SELECT h.recording_id, h.local_date, h.delivery_hour, h.id AS hour_row_id, h.hour_id, h.batch_id, h.status, h.registered_at,
 h.source_clip_count, h.included_clip_count,
 o.id AS output_id, o.part, o.parts, o.object_key, o.nas_relative_path, o.size_bytes, o.sha256,
 to_char(o.start_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"+00:00"') AS start_at,
 to_char(o.end_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"+00:00"') AS end_at,
 EXISTS (SELECT 1 FROM nas_restore_requests n WHERE n.target='output' AND n.output_id=o.id AND n.state='verified') AS nas_restored,
 EXISTS (SELECT 1 FROM r2_retirement_writer_operations w WHERE w.collation_recovery_hour_id IS NOT NULL
   AND w.completed_at IS NOT NULL AND w.object_key=o.object_key) AS recovery_put,
 EXISTS (SELECT 1 FROM r2_retirement_reservations x WHERE x.kind='collated' AND x.source_id=o.id) AS reserved_collated,
 EXISTS (SELECT 1 FROM r2_nonraw_intents i WHERE i.kind='collated' AND i.source_id=o.id) AS nonraw_intent_collated,
 EXISTS (SELECT 1 FROM r2_object_retirements x WHERE x.kind='collated' AND x.source_id=o.id
   AND NOT EXISTS (SELECT 1 FROM nas_restore_requests q WHERE q.output_id=o.id AND q.target='output'
     AND q.state='verified' AND q.completed_at>x.retired_at AND q.verified_etag<>''
     AND q.object_key=o.object_key AND q.sha256=o.sha256 AND q.size_bytes=o.size_bytes
     AND x.bucket='{BUCKET}' AND x.object_key=q.object_key AND x.sha256=q.sha256 AND x.size_bytes=q.size_bytes)) AS retired_collated,
 EXISTS (SELECT 1 FROM r2_retirement_reservations x WHERE x.kind='raw' AND x.source_id=ANY(o.source_clip_ids)) AS reserved_raw_source,
 EXISTS (SELECT 1 FROM r2_inverse_intents i WHERE i.source_id=ANY(o.source_clip_ids)) AS inverse_intent_source
FROM recording_collation_hours h JOIN d USING (recording_id, local_date)
LEFT JOIN recording_collation_outputs o ON o.collation_hour_id=h.id
ORDER BY 1, 2, 3, 4, o.part;
"""


# The delivery hold guard's collated-source closure (guard_r2_delivery_keys_admitted,
# migrations 0267 and 0269): any of these refuses a new video_collated keep hold on the
# output, so an output not already held is not servable. A collated retirement stops
# blocking once a verified NAS output restore completed after it re-created the same
# bytes; the builder's hold names the output's own key, sha256 and size, so the
# guard's hold-side equalities reduce to the output's.
CLOSURE_COLUMNS = ('reserved_collated', 'nonraw_intent_collated', 'retired_collated', 'reserved_raw_source', 'inverse_intent_source')
HOLDS_SQL = "SELECT object_key, etag, size_bytes FROM delivery_keep_holds WHERE bucket='stoarama';\n"
EXTRACT_COLUMNS = ('recording_id', 'local_date', 'delivery_hour', 'hour_row_id', 'hour_id', 'batch_id', 'status',
                   'registered_at', 'source_clip_count', 'included_clip_count', 'output_id', 'part', 'parts',
                   'object_key', 'nas_relative_path', 'size_bytes', 'sha256', 'start_at', 'end_at',
                   'nas_restored', 'recovery_put') + CLOSURE_COLUMNS


def flag(v):
    if v not in ('t', 'f'):
        raise LedgerError(f'extract: boolean must be t or f, got {v!r}')
    return v == 't'


def part_source(batch_id, key, nas_restored, recovery_put):
    if key.startswith(CLOCK_PREFIX):
        raise LedgerError(f'registered output {key} is under {CLOCK_PREFIX}*')
    if batch_id.startswith(RECOVERY_BATCH_PREFIX):
        return Source.NEW_STITCH
    if nas_restored:
        return Source.NAS_UPLOAD
    if recovery_put:
        return Source.RESTORE
    return Source.ORIGINAL


def read_extract(path):
    """{(recording_id, local_date, delivery_hour): [Record]}."""
    rows = collections.defaultdict(list)
    meta = {}
    for r in read_tsv(path, EXTRACT_COLUMNS):
        slot = (int(r['recording_id']), r['local_date'], int(r['delivery_hour']))
        row_id = int(r['hour_row_id'])
        m = (slot, r['hour_id'], r['batch_id'], r['status'], r['registered_at'],
             int(r['source_clip_count'] or 0), int(r['included_clip_count'] or 0))
        if meta.setdefault(row_id, m) != m:
            raise LedgerError(f'{path}: hour row {row_id} disagrees with itself')
        if r['output_id']:
            rows[row_id].append(Part(int(r['output_id']), int(r['part']), int(r['parts']), r['object_key'],
                                     int(r['size_bytes']), r['sha256'], r['start_at'], r['end_at'], r['nas_relative_path'],
                                     part_source(r['batch_id'], r['object_key'], flag(r['nas_restored']), flag(r['recovery_put'])),
                                     any(flag(r[c]) for c in CLOSURE_COLUMNS)))
    out = collections.defaultdict(list)
    for row_id, (slot, hour_id, batch, status, registered_at, clips, included) in meta.items():
        parts = tuple(sorted(rows[row_id], key=lambda p: p.part))
        if (status == COLLATED) != bool(parts) or status not in NO_CAPTURE_STATES | {COLLATED}:
            raise LedgerError(f'hour {hour_id}: status {status} with {len(parts)} outputs')
        out[slot].append(Record(row_id, hour_id, batch, status, registered_at, parts, clips, included))
    return dict(out)


def read_listing(paths):
    """{key: (size, etag)} from `aws s3api list-objects-v2 ... --query 'Contents[].[Key,Size,ETag]' --output text`."""
    out = {}
    for path in paths:
        with open(path, encoding='utf-8') as f:
            for line in f:
                fields = line.rstrip('\n').split('\t')
                if len(fields) != 3 or not fields[1].isdigit() or not fields[2].startswith('"'):
                    raise LedgerError(f'{path}: not a key/size/ETag listing line: {line!r}')
                if fields[0] in out:
                    raise LedgerError(f'{path}: {fields[0]} listed twice')
                out[fields[0]] = (int(fields[1]), fields[2].strip('"'))
    if not out:
        raise LedgerError('empty listing')
    return out


def read_holds(path):
    out = {}
    for r in read_tsv(path, ('object_key', 'etag', 'size_bytes'), empty_ok=True):
        out[r['object_key']] = (r['etag'].strip('"'), int(r['size_bytes']))
    return out


def registry_assemblies(registry, store, listing, holds):
    """(live assemblies of the live generation, count in R2 without a matching keep hold)."""
    repairs = {'sha256:' + x['sha256']: x for x in store.load('/public/growth.json')['repairs']}
    live, unheld = [], 0
    for fid, f in sorted(registry['files'].items()):
        if not fid.startswith('sha256:'):
            continue
        rep = repairs.get(fid)
        if rep is None or rep['kind'] not in REPAIR_KINDS:
            raise LedgerError(f'assembly {fid}: no growth.json repair of a known kind')
        etag = f['etag'].strip('"')
        if not in_r2(listing, f['key'], f['size_bytes'], etag):
            continue
        if holds.get(f['key']) != (etag, f['size_bytes']):
            unheld += 1
            continue
        live.append(Assembly(fid, f['key'], f['size_bytes'], etag, f['sha256'], tuple(int(i) for i in rep['input_ids']),
                             REPAIR_KINDS[rep['kind']], f['path']))
    return live, unheld


def read_proofs(directory, listing):
    """Every clock-join proof (joined/<batch>/proofs/<sha256>.json) whose video is in the listing."""
    out = []
    for p in sorted(Path(directory).rglob('*.json')):
        proof = json.loads(p.read_text(encoding='utf-8'))
        o = proof['output']
        if not o['key'].startswith(CLOCK_PREFIX):
            raise LedgerError(f'{p}: assembly key {o["key"]} is not under {CLOCK_PREFIX}*')
        if in_r2(listing, o['key'], int(o['size_bytes'])):
            out.append(Assembly('sha256:' + o['sha256'], o['key'], int(o['size_bytes']), listing[o['key']][1], o['sha256'],
                                tuple(int(i['output_id']) for i in proof['inputs']), Source.CLOCK_ASSEMBLY, None))
    return out


def load(a):
    """Inputs from parsed arguments."""
    listing = read_listing(a.listing)
    registry = build_generation.read_registry(a.registry)
    store = build_generation.AssetStore(registry, a.assets)
    holds = read_holds(a.holds)
    live, unheld = registry_assemblies(registry, store, listing, holds)
    if a.clock_proofs:
        live += read_proofs(a.clock_proofs, listing)
    return Inputs(read_best14(a.best14), read_extract(a.extract), live, listing, registry, holds, unheld,
                  delivery_naming.catalog_names(store.load('/public/catalog.json'))), store


# ---- selection -------------------------------------------------------------

def in_r2(listing, key, size, etag=None):
    got = listing.get(key)
    if got is not None and (got[0] != size or (etag is not None and got[1] != etag)):
        raise LedgerError(f'{key}: R2 holds {got}, catalogued {(size, etag)}')
    return got is not None


def check_assembly_run(a, records_of):
    """An assembly's inputs are one run of parts: consecutive within each hour record,
    from the end of one hour record into the start of the next."""
    runs = []
    for i in a.input_ids:
        rec, part = records_of[i]
        if runs and runs[-1][0] is rec:
            runs[-1][1].append(part.part)
        else:
            runs.append((rec, [part.part]))
    if len(a.input_ids) < 2 or len({id(r) for r, _ in runs}) != len(runs):
        raise LedgerError(f'assembly {a.file_id}: inputs {list(a.input_ids)} are not one run of parts')
    for k, (rec, ords) in enumerate(runs):
        n = rec.parts[0].parts
        if ords != list(range(ords[0], ords[0] + len(ords))) or (k > 0 and ords[0] != 1) or (k < len(runs) - 1 and ords[-1] != n):
            raise LedgerError(f'assembly {a.file_id}: parts {ords} of {rec.hour_id} are not consecutive')


def serving_set(record, slot, by_input, listing, holds, names):
    """(files listed on record's hour, assemblies listed earlier that serve its first parts).
    Every file takes the delivery name (delivery_naming) of its place among the hour's
    files in source order, an assembly at its first part's place."""
    ids = {p.output_id for p in record.parts}
    served, carried, seen = [], [], set()
    for p in record.parts:
        a = by_input.get(p.output_id)
        if a is None:
            etag = listing.get(p.key, (0, None))[1]
            live = in_r2(listing, p.key, p.size_bytes) and (not p.closed or holds.get(p.key) == (etag, p.size_bytes))
            served.append(Served(str(p.output_id), p.key, p.size_bytes, etag, p.sha256, None, p.source, (str(p.output_id),), live))
        elif a.file_id not in seen:
            seen.add(a.file_id)
            if a.input_ids[0] not in ids:
                carried.append(a.file_id)
                continue
            if a.source is Source.CLOCK_ASSEMBLY and a.path is None and not set(a.input_ids) <= ids:
                raise LedgerError(f'clock-join assembly {a.file_id} crosses an hour')
            served.append(Served(a.file_id, a.key, a.size_bytes, a.etag, a.sha256, None, a.source,
                                 tuple(str(i) for i in a.input_ids)))
    n = len(served)
    return tuple(dataclasses.replace(x, path=delivery_naming.delivery_path(names, *slot, k, n))
                 for k, x in enumerate(served, 1)), tuple(carried)


def assembly_index(inputs):
    """{part output_id: live assembly} for assemblies touching a best-14 hour; checks each run."""
    records_of = {p.output_id: (r, p) for rs in inputs.records.values() for r in rs for p in r.parts}
    window = {(rid, d, h) for rid, (_, days) in inputs.best14.items() for d, _ in days for h in HOURS}
    slot_of = {p.output_id: slot for slot, rs in inputs.records.items() for r in rs for p in r.parts}
    by_input = {}
    for a in inputs.assemblies:
        inside = [i in slot_of and slot_of[i] in window for i in a.input_ids]
        if not any(inside):
            continue
        if not all(inside):
            raise LedgerError(f'assembly {a.file_id}: inputs {list(a.input_ids)} reach outside the best-14 hours')
        check_assembly_run(a, records_of)
        for i in a.input_ids:
            if by_input.setdefault(i, a) is not a:
                raise LedgerError(f'part {i} is an input of two assemblies: {by_input[i].file_id} and {a.file_id}')
    return by_input


def select(inputs):
    """One Hour per best-14 (recording_id, local_date, delivery_hour)."""
    by_input = assembly_index(inputs)
    listing = inputs.listing
    hours = []
    for rid, (stream, days) in sorted(inputs.best14.items()):
        for date, codes in days:
            for dh in HOURS:
                rs = inputs.records.get((rid, date, dh), [])
                collated = [r for r in rs if r.status == COLLATED]
                if rs and len(collated) not in (0, len(rs)):
                    raise LedgerError(f'recording {rid} {date} hour {dh}: collated and no-capture records together')
                if not collated:
                    hours.append(Hour(rid, stream, date, dh, captured=not rs and codes[dh - 1] != '.',
                                      record=rs[0] if rs else None))
                    continue
                cands = []
                for r in collated:
                    served, carried = serving_set(r, (rid, date, dh), by_input, listing, inputs.holds, inputs.names)
                    live = all(s.live for s in served)
                    ok = r.complete() and live
                    cands.append((not ok, r.batch_id.startswith(RECOVERY_BATCH_PREFIX), r.registered_at, r.row_id,
                                  r, served, carried, live, ok))
                cands.sort(key=lambda c: c[:4])
                *_, r, served, carried, live, _ = cands[0]
                hours.append(Hour(rid, stream, date, dh, True, r, served, r.complete(), live,
                                  sum(c[-1] for c in cands[1:]), carried))
    check_rules(hours, inputs.names)
    return hours


# ---- rules -----------------------------------------------------------------

def check_rules(hours, names):
    """Fail on an hour served by two sets, an assembly duplicating its parts, or a duplicate delivery path."""
    record_slot = {}
    for h in hours:
        if h.record is not None and record_slot.setdefault(h.record.row_id, h.slot) != h.slot:
            raise LedgerError(f'hour record {h.record.hour_id} serves two hours')
        if (h.served or h.covered_by) and h.record is None:
            raise LedgerError(f'{h.slot}: served without an hour record')
    part_slot = {str(p.output_id): h.slot for h in hours if h.record is not None for p in h.record.parts}
    served_by = {}
    seen = {}
    for h in hours:
        for s in h.served:
            for k in (s.file_id, s.key):
                if seen.setdefault(k, h.slot) != h.slot:
                    raise LedgerError(f'{k} serves two hours: {seen[k]} and {h.slot}')
            if s.source not in ASSEMBLY_SOURCES and part_slot.get(s.file_id) != h.slot:
                raise LedgerError(f'{h.slot}: {s.file_id} is not from the serving record {h.record.hour_id}')
            for i in s.registry_ids:
                if i not in part_slot:
                    raise LedgerError(f'{h.slot}: {s.file_id} serves part {i} of a record that serves no hour')
                if served_by.setdefault(i, s.file_id) != s.file_id:
                    raise LedgerError(f'part {i} is served twice: by {served_by[i]} and {s.file_id} (an assembly duplicates its parts)')
    for h in hours:
        if h.served or h.covered_by:
            missing = [p.output_id for p in h.record.parts if str(p.output_id) not in served_by]
            if missing:
                raise LedgerError(f'{h.slot}: parts {missing} are served by no file of a serving set')
    problems = delivery_naming.check_paths(names, [(h.recording_id, h.local_date, s.path) for h in hours for s in h.served])
    if problems:
        raise LedgerError(f'{len(problems)} served paths break the delivery naming, e.g. {problems[:3]}')


# ---- outputs ---------------------------------------------------------------

LEDGER_COLUMNS = ('recording_id', 'stream_id', 'local_date', 'delivery_hour', 'local_hour', 'captured', 'status',
                  'hour_id', 'batch_id', 'parts', 'served', 'complete', 'in_r2', 'source', 'alternates', 'covered_by',
                  'sources', 'file_ids', 'object_keys', 'delivery_paths', 'generation', 'published')
# available: the best-14 window is complete. window and page_availability: what the
# generation's page shows for the stream (metadata.json window and availability).
STREAM_COLUMNS = ('recording_id', 'stream_id', 'window_start', 'window_end', 'days_complete', 'days', 'captured_hours',
                  'hours_complete', 'hours_with_record', 'available', 'window', 'page_availability', 'generation', 'published')
SUMMARY_FILE = 'best14-ledger-summary.txt'


@dataclasses.dataclass(frozen=True)
class Publication:
    """The generation a ledger describes, whether it is the one the live page serves, and
    what that generation's page shows per stream."""
    generation: str
    published: bool
    window: dict        # recording_id -> 'original' | 'best14'
    availability: dict  # recording_id -> 'available' | 'partial'


def metadata_window(s):
    """A metadata stream's window; generations before the window field show best-14 when
    their period starts on the best-14 start."""
    return s.get('window') or ('best14' if s['start'] == s.get('best14_start') else 'original')


def publication_of(registry, store, published):
    streams = store.load('/public/metadata.json')['streams']
    return Publication(registry['generation'], published, {s['id']: metadata_window(s) for s in streams},
                       {s['id']: s.get('availability') for s in streams})


def joined(values):
    values = list(values)
    bad = [v for v in values if SEP in v or '\t' in v or '\n' in v]
    if bad:
        raise LedgerError(f'value holds a separator: {bad[0]!r}')
    return SEP.join(values)


def ledger_row(h):
    r = h.record
    return {'recording_id': h.recording_id, 'stream_id': h.stream_id, 'local_date': h.local_date,
            'delivery_hour': h.delivery_hour, 'local_hour': FIRST_LOCAL_HOUR + h.delivery_hour - 1,
            'captured': yn(h.captured), 'status': r.status if r else 'no_record',
            'hour_id': r.hour_id if r else '', 'batch_id': r.batch_id if r else '',
            'parts': r.parts[0].parts if r and r.parts else 0, 'served': len(h.served),
            'complete': yn(h.complete), 'in_r2': yn(h.in_r2),
            'source': '+'.join(s.value for s in Source if any(x.source is s for x in h.served)),
            'alternates': h.alternates, 'covered_by': joined(h.covered_by),
            'sources': joined(s.source.value for s in h.served), 'file_ids': joined(s.file_id for s in h.served),
            'object_keys': joined(s.key for s in h.served), 'delivery_paths': joined(s.path for s in h.served)}


def day_rows(hours):
    """Per (recording_id, stream_id, local_date): captured hours, complete hours, and the day code
    R (complete), . (no capture) or S."""
    days = collections.defaultdict(list)
    for h in hours:
        days[(h.recording_id, h.stream_id, h.local_date)].append(h)
    out = []
    for (rid, stream, date), hs in sorted(days.items()):
        captured = [h for h in hs if h.captured]
        done = sum(h.ok for h in captured)
        code = '.' if not captured else 'R' if done == len(captured) else 'S'
        out.append({'recording_id': rid, 'stream_id': stream, 'local_date': date, 'day_stitched': code,
                    'captured_hours': len(captured), 'hours_complete': done})
    return out


def stream_rows(hours, days):
    out = []
    for rid in sorted({d['recording_id'] for d in days}):
        ds = [d for d in days if d['recording_id'] == rid]
        hs = [h for h in hours if h.recording_id == rid]
        complete = sum(d['day_stitched'] != 'S' for d in ds)
        captured = sum(d['captured_hours'] for d in ds)
        out.append({'recording_id': rid, 'stream_id': ds[0]['stream_id'], 'window_start': ds[0]['local_date'],
                    'window_end': ds[-1]['local_date'], 'days_complete': complete, 'days': len(ds),
                    'captured_hours': captured, 'hours_complete': sum(d['hours_complete'] for d in ds),
                    'hours_with_record': sum(h.record is not None and h.captured for h in hs),
                    'available': yn(complete == len(ds) and captured > 0)})
    return out


def closed_keys(h):
    return {p.key for p in h.record.parts if p.closed} if h.record else set()


def summary_line(hours, streams, unheld, pub):
    captured = [h for h in hours if h.captured]
    ok = [h for h in captured if h.ok]
    counts = collections.Counter(s.source for h in ok for s in h.served)
    return (f'generation={pub.generation} published={yn(pub.published)} '
            f"page_available={sum(v == 'available' for v in pub.availability.values())} "
            f"windows_best14={sum(v == 'best14' for v in pub.window.values())} "
            f"streams_available={sum(s['available'] == 'y' for s in streams)}/{len(streams)} "
            f"days_complete={sum(s['days_complete'] for s in streams)}/{sum(s['days'] for s in streams)} "
            f"hours_complete={len(ok)}/{len(captured)} "
            + ' '.join(f'{s.value}={counts[s]}' for s in Source)
            + f' alternates={sum(h.alternates > 0 for h in ok)} unheld_assemblies={unheld}'
            + f' closed_parts={sum(1 for h in hours for s in h.served if s.key in closed_keys(h) and not s.live)}')


def write_tsv(path, columns, rows):
    with open(path, 'w', encoding='utf-8', newline='') as f:
        w = csv.DictWriter(f, columns, delimiter='\t', lineterminator='\n', quoting=csv.QUOTE_NONE, escapechar=None)
        w.writeheader()
        w.writerows(rows)


def write(out_dir, hours, unheld, pub):
    """Write the ledger of one generation (pub) and return its summary line."""
    os.makedirs(out_dir, exist_ok=True)
    stamp = {'generation': pub.generation, 'published': yn(pub.published)}
    streams = [dict(s, window=pub.window[s['recording_id']], page_availability=pub.availability[s['recording_id']], **stamp)
               for s in stream_rows(hours, day_rows(hours))]
    write_tsv(os.path.join(out_dir, 'best14-ledger.tsv'), LEDGER_COLUMNS, [dict(ledger_row(h), **stamp) for h in hours])
    write_tsv(os.path.join(out_dir, 'best14-ledger-streams.tsv'), STREAM_COLUMNS, streams)
    line = summary_line(hours, streams, unheld, pub)
    Path(out_dir, SUMMARY_FILE).write_text(line + '\n', encoding='utf-8')
    return line


def live_check(out_dir, src):
    """Problems unless the ledger in out_dir describes the generation the live page serves,
    with the same windows and availability. With none, marks the ledger published."""
    rows = read_tsv(os.path.join(out_dir, 'best14-ledger-streams.tsv'), STREAM_COLUMNS)
    gens = {r['generation'] for r in rows}
    live = src.get('/public/growth.json')['generation']
    problems = [] if gens == {live} else [f'ledger generation {sorted(gens)} is not the live generation {live}']
    streams = {s['id']: s for s in src.get('/public/metadata.json')['streams']}
    if sorted(streams) != sorted(int(r['recording_id']) for r in rows):
        problems.append('ledger streams differ from the live streams')
    for r in rows:
        s = streams.get(int(r['recording_id']))
        if s and (r['window'], r['page_availability']) != (metadata_window(s), s.get('availability')):
            problems.append(f"recording {r['recording_id']}: ledger {r['window']}/{r['page_availability']}, "
                            f"live {metadata_window(s)}/{s.get('availability')}")
    if not problems:
        for name in ('best14-ledger.tsv', 'best14-ledger-streams.tsv', SUMMARY_FILE):
            p = Path(out_dir, name)
            p.write_text(p.read_text(encoding='utf-8').replace(f'\t{live}\tn\n', f'\t{live}\ty\n')
                         .replace(f'generation={live} published=n ', f'generation={live} published=y '), encoding='utf-8')
    return problems


def add_input_args(ap):
    ap.add_argument('--best14', required=True, help='best14-days.tsv')
    ap.add_argument('--registry', required=True, help='the live generation registry (current.json)')
    ap.add_argument('--assets', required=True, help='its asset bodies laid out by R2 key')
    ap.add_argument('--extract', required=True, help='extract-sql output')
    ap.add_argument('--holds', required=True, help='holds-sql output')
    ap.add_argument('--listing', required=True, nargs='+', help='key/size/ETag listings of joined/ and experimental/')
    ap.add_argument('--clock-proofs', help='directory of clock-join proof JSONs')


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest='cmd', required=True)
    q = sub.add_parser('extract-sql', help='print the read-only DB extract query')
    q.add_argument('--best14', required=True)
    q.add_argument('--registry', required=True)
    q.add_argument('--assets', required=True)
    sub.add_parser('holds-sql', help='print the read-only keep-hold query')
    b = sub.add_parser('build', help='write the ledger of the live generation (--registry)')
    add_input_args(b)
    b.add_argument('--out-dir', required=True)
    c = sub.add_parser('live-check', help='fail unless a ledger describes what the live page serves; then mark it published')
    c.add_argument('--ledger-dir', required=True)
    c.add_argument('--current', action='store_true', required=True, help='check against the live public API')
    c.add_argument('--origin', default='https://stoarama.com')
    a = ap.parse_args(argv)
    try:
        if a.cmd == 'holds-sql':
            sys.stdout.write(HOLDS_SQL)
        elif a.cmd == 'extract-sql':
            registry = build_generation.read_registry(a.registry)
            best14 = read_best14(a.best14)
            days = {(rid, d) for rid, (_, ds) in best14.items() for d, _ in ds}
            sys.stdout.write(extract_sql(days | live_days(registry, build_generation.AssetStore(registry, a.assets))))
        elif a.cmd == 'live-check':
            from viewer_smoke import Live
            problems = live_check(a.ledger_dir, Live(a.origin, None))
            for p in problems:
                print('MISMATCH ' + p)
            print(f'ledger live-check: {"ok, marked published" if not problems else f"{len(problems)} mismatches"}')
            if problems:
                raise SystemExit(1)
        else:
            inputs, store = load(a)
            print(write(a.out_dir, select(inputs), inputs.unheld, publication_of(inputs.registry, store, True)))
    except LedgerError as e:
        raise SystemExit(f'best14 ledger: {e}')


if __name__ == '__main__':
    main()
