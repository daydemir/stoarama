"""Ledger of what serves each Global Street Scores (GSS) best-14 stream-day hour.

Deniz, Nov 25: "just make sure we're keeping a sane set and knowing we've
stitched, and knowing we have full best 14 day periods". The ledger is the one
source of truth per best-14 stream-day and delivery hour (1-12, local 08-19).
It is read-only: it reads a DB extract, a live R2 listing and the public
registry, and writes only local files.

Inputs:
  --best14     best14-days.tsv (recording_id, stream_id, local_date, hours_08_19;
               14 rows per recording). hours_08_19 decides capture only for an
               hour with no hour record: '.' is no capture.
  --extract    the DB extract: run `best14_ledger.py extract-sql --best14 ...`
               read-only (default_transaction_read_only=on) with psql -A -F TAB.
  --listing    a live recursive listing of s3://<bucket>/joined/ (aws s3 ls --recursive).
  --registry   the generation the builder will use as --parent (.json or .json.gz).
  --clock-proofs  optional directory of clock-join proof JSONs, copied read-only
               from joined/clock-prestitch-*/proofs/. An assembly serves only while
               its video key is in the listing.

Per hour the serving set is one hour record: its parts 1..parts, where each
in-R2 clock assembly replaces the consecutive parts it was built from. Among an
hour's records the one complete in R2 serves, preferring a non-recovery record,
then the earliest registered; `alternates` counts the other complete-in-R2 ones.

Source of each served file:
  clock_assembly  a key under joined/clock-prestitch-*/
  new_stitch      a part of an hour registered in a recovery- batch
  nas_upload      a part restored from the NAS (nas_restore_requests target
                  'output', state verified)
  restore         a part re-uploaded by a collation-v2 recovery run (a completed
                  r2_retirement_writer_operations recovery intent on its key)
  original        any other registered part

complete = every part 1..parts of the record is registered; in_r2 = every served
key is in the listing at its catalogued size. A day is complete when every
captured hour is complete and in R2; a stream is available when all 14 days are
complete and at least one hour is captured.

Rules, each a hard failure: no hour is served by two sets (one record per hour,
no file serving two hours); an assembly replaces its parts and never duplicates
them (its inputs are consecutive parts of its own record, taken by no other
assembly and not served themselves); no two served files share a delivery path.
A delivery path is the registry's path for a file the registry lists, else the
catalogued NAS collated path; an assembly takes its parts' own name format.

Outputs, in --out-dir:
  best14-ledger.tsv          one row per (recording_id, local_date, delivery_hour)
  best14-ledger-streams.tsv  per stream: days complete of 14, available y/n
  best14-ledger-days.tsv     build_generation.py --best14 input (day_stitched R,
                             '.' for no capture, S otherwise)
  best14-ledger-files.txt    build_generation.py --files input: the registry file
                             ids serving every complete-in-R2 hour (a clock
                             assembly contributes its inputs, which the builder's
                             --clock-assemblies replaces)

Publish only from the ledger: build_generation.py --parent <the --registry> \\
  --best14 best14-ledger-days.tsv --files best14-ledger-files.txt. Served files
the registry does not list (restores, NAS uploads, new stitches) are counted as
`unpublishable` on the summary line: the builder can only select files its
parent lists.
"""
import argparse
import collections
import csv
import dataclasses
import datetime
import enum
import gzip
import json
import os
import re
import sys
from pathlib import Path

HOURS = range(1, 13)  # delivery hours 1-12 = local 08-19
FIRST_LOCAL_HOUR = 8
CLOCK_PREFIX = 'joined/clock-prestitch-'
RECOVERY_BATCH_PREFIX = 'recovery-'
NO_CAPTURE_STATES = frozenset({'gap_only', 'quarantine_only'})
COLLATED = 'collated'
SEP = ','
# A collated part's delivery name (clockjoin.AssemblyPath's partName).
PART_NAME = re.compile(r'^(.*_hour_[0-9]{2}(?:_dst2)?)(_part_[0-9]{2,})?\.mp4$')


class Source(str, enum.Enum):
    ORIGINAL = 'original'
    RESTORE = 'restore'
    NAS_UPLOAD = 'nas_upload'
    NEW_STITCH = 'new_stitch'
    CLOCK_ASSEMBLY = 'clock_assembly'


class LedgerError(Exception):
    pass


@dataclasses.dataclass(frozen=True)
class Part:
    output_id: int
    part: int
    parts: int
    key: str
    size_bytes: int
    nas_path: str
    source: Source


@dataclasses.dataclass(frozen=True)
class Record:
    row_id: int
    hour_id: str
    batch_id: str
    status: str
    registered_at: str
    parts: tuple

    def complete(self):
        return bool(self.parts) and len({p.parts for p in self.parts}) == 1 and \
            sorted(p.part for p in self.parts) == list(range(1, self.parts[0].parts + 1))


@dataclasses.dataclass(frozen=True)
class Assembly:
    sha256: str
    key: str
    size_bytes: int
    hour_id: str
    input_ids: tuple


@dataclasses.dataclass(frozen=True)
class Served:
    file_id: str
    key: str
    size_bytes: int
    path: str
    source: Source
    registry_ids: tuple  # the registry files this serves: itself, or an assembly's inputs


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
    in_registry: bool = False

    @property
    def ok(self):
        return self.complete and self.in_r2


def yn(b):
    return 'y' if b else 'n'


# ---- inputs ---------------------------------------------------------------

def read_tsv(path, columns):
    with open(path, newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f, delimiter='\t'))
    if not rows or not set(columns) <= set(rows[0]):
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


def extract_sql(best14):
    values = ','.join(f"({rid},date '{d}')" for rid, (_, days) in sorted(best14.items()) for d, _ in days)
    return f"""WITH d(recording_id, local_date) AS (VALUES {values})
SELECT h.recording_id, h.local_date, h.delivery_hour, h.id AS hour_row_id, h.hour_id, h.batch_id, h.status, h.registered_at,
 o.id AS output_id, o.part, o.parts, o.object_key, o.nas_relative_path, o.size_bytes,
 EXISTS (SELECT 1 FROM nas_restore_requests n WHERE n.target='output' AND n.output_id=o.id AND n.state='verified') AS nas_restored,
 EXISTS (SELECT 1 FROM r2_retirement_writer_operations w WHERE w.collation_recovery_hour_id IS NOT NULL
   AND w.completed_at IS NOT NULL AND w.object_key=o.object_key) AS recovery_put
FROM recording_collation_hours h JOIN d USING (recording_id, local_date)
LEFT JOIN recording_collation_outputs o ON o.collation_hour_id=h.id
ORDER BY 1, 2, 3, 4, o.part;
"""


EXTRACT_COLUMNS = ('recording_id', 'local_date', 'delivery_hour', 'hour_row_id', 'hour_id', 'batch_id', 'status',
                   'registered_at', 'output_id', 'part', 'parts', 'object_key', 'nas_relative_path', 'size_bytes',
                   'nas_restored', 'recovery_put')


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
        if slot[2] not in HOURS:
            raise LedgerError(f'{path}: delivery_hour {slot[2]} outside 1-12')
        row_id = int(r['hour_row_id'])
        m = (slot, r['hour_id'], r['batch_id'], r['status'], r['registered_at'])
        if meta.setdefault(row_id, m) != m:
            raise LedgerError(f'{path}: hour row {row_id} disagrees with itself')
        if r['output_id']:
            rows[row_id].append(Part(int(r['output_id']), int(r['part']), int(r['parts']), r['object_key'],
                                     int(r['size_bytes']), r['nas_relative_path'],
                                     part_source(r['batch_id'], r['object_key'], flag(r['nas_restored']), flag(r['recovery_put']))))
    out = collections.defaultdict(list)
    for row_id, (slot, hour_id, batch, status, registered_at) in meta.items():
        parts = tuple(sorted(rows[row_id], key=lambda p: p.part))
        if (status == COLLATED) != bool(parts) or status not in NO_CAPTURE_STATES | {COLLATED}:
            raise LedgerError(f'hour {hour_id}: status {status} with {len(parts)} outputs')
        out[slot].append(Record(row_id, hour_id, batch, status, registered_at, parts))
    return out


def read_listing(path):
    """{key: size} from `aws s3 ls --recursive s3://<bucket>/joined/`."""
    out = {}
    with open(path, encoding='utf-8') as f:
        for line in f:
            if not line.strip():
                continue
            fields = line.split(None, 3)
            if len(fields) != 4 or not fields[3].startswith('joined/'):
                raise LedgerError(f'{path}: not a recursive listing of joined/: {line!r}')
            key = fields[3].rstrip('\n')
            if key in out:
                raise LedgerError(f'{path}: {key} listed twice')
            out[key] = int(fields[2])
    if not out:
        raise LedgerError(f'{path}: empty listing')
    return out


def read_registry(path):
    raw = Path(path).read_bytes()
    reg = json.loads(gzip.decompress(raw) if raw[:2] == b'\x1f\x8b' else raw)
    return {k: v['path'] for k, v in reg['files'].items()}


def read_proofs(directory):
    """Every clock-join proof (joined/<batch>/proofs/<sha256>.json) in a directory tree."""
    out = []
    for p in sorted(Path(directory).rglob('*.json')):
        proof = json.loads(p.read_text(encoding='utf-8'))
        o = proof['output']
        if not o['key'].startswith(CLOCK_PREFIX):
            raise LedgerError(f'{p}: assembly key {o["key"]} is not under {CLOCK_PREFIX}*')
        out.append(Assembly(o['sha256'], o['key'], int(o['size_bytes']), proof['hour_id'],
                            tuple(int(i['output_id']) for i in proof['inputs'])))
    return out


# ---- selection -------------------------------------------------------------

def assembly_path(inputs, whole_hour):
    """clockjoin.AssemblyPath: a whole-hour run takes the single-part name, else its first part's."""
    m = PART_NAME.match(inputs[0])
    if not m or not m.group(2):
        raise LedgerError(f'assembly input {inputs[0]} is not a part name')
    for p in inputs[1:]:
        n = PART_NAME.match(p)
        if not n or n.group(1) != m.group(1) or not n.group(2):
            raise LedgerError(f'assembly inputs {inputs[0]} and {p} are not parts of one hour')
    return m.group(1) + '.mp4' if whole_hour else inputs[0]


def in_r2(listing, key, size):
    got = listing.get(key)
    if got is not None and got != size:
        raise LedgerError(f'{key}: R2 holds {got} bytes, catalogued {size}')
    return got is not None


def serving_set(record, assemblies, listing, registry):
    """The files serving record: each in-R2 assembly in place of its parts, then the rest."""
    by_id = {p.output_id: p for p in record.parts}
    path = lambda p: registry.get(str(p.output_id), p.nas_path)
    live = [a for a in assemblies if in_r2(listing, a.key, a.size_bytes)]
    check_assemblies(record, live)
    replaced = {i for a in live for i in a.input_ids}
    served = [Served('sha256:' + a.sha256, a.key, a.size_bytes,
                     assembly_path([path(by_id[i]) for i in a.input_ids], len(a.input_ids) == len(record.parts)),
                     Source.CLOCK_ASSEMBLY, tuple(str(i) for i in a.input_ids))
              for a in live]
    served += [Served(str(p.output_id), p.key, p.size_bytes, path(p), p.source, (str(p.output_id),))
               for p in record.parts if p.output_id not in replaced]
    return tuple(served)


def select(best14, records, assemblies, listing, registry):
    """One Hour per best-14 (recording_id, local_date, delivery_hour)."""
    by_hour_id = collections.defaultdict(list)
    for a in assemblies:
        by_hour_id[a.hour_id].append(a)
    known = {r.hour_id for rs in records.values() for r in rs}
    orphan = sorted(set(by_hour_id) - known)
    if orphan:
        raise LedgerError(f'{len(orphan)} clock assemblies name hours outside the best-14 extract, e.g. {orphan[:3]}')
    hours = []
    for rid, (stream, days) in sorted(best14.items()):
        for date, codes in days:
            for dh in HOURS:
                rs = records.get((rid, date, dh), [])
                collated = [r for r in rs if r.status == COLLATED]
                if rs and len(collated) not in (0, len(rs)):
                    raise LedgerError(f'recording {rid} {date} hour {dh}: collated and no-capture records together')
                if not collated:
                    hours.append(Hour(rid, stream, date, dh, captured=not rs and codes[dh - 1] != '.',
                                      record=rs[0] if rs else None))
                    continue
                cands = []
                for r in collated:
                    served = serving_set(r, by_hour_id[r.hour_id], listing, registry)
                    live = all(in_r2(listing, s.key, s.size_bytes) for s in served)
                    ok = r.complete() and live
                    cands.append((not ok, r.batch_id.startswith(RECOVERY_BATCH_PREFIX), r.registered_at, r.row_id, r, served, live, ok))
                cands.sort(key=lambda c: c[:4])
                *_, r, served, live, _ = cands[0]
                hours.append(Hour(rid, stream, date, dh, True, r, served, r.complete(), live,
                                  sum(c[-1] for c in cands[1:]),
                                  all(i in registry for s in served for i in s.registry_ids)))
    check_rules(hours)
    return hours


# ---- rules -----------------------------------------------------------------

def check_assemblies(record, assemblies):
    """An assembly replaces consecutive parts of its own record, and no part twice."""
    by_id = {p.output_id: p for p in record.parts}
    taken = collections.Counter(i for a in assemblies for i in a.input_ids)
    twice = sorted(i for i, n in taken.items() if n > 1)
    if twice:
        raise LedgerError(f'hour {record.hour_id}: parts {twice} are inputs of two assemblies')
    for a in assemblies:
        if len(a.input_ids) < 2 or any(i not in by_id for i in a.input_ids):
            raise LedgerError(f'assembly {a.sha256}: inputs {list(a.input_ids)} are not parts of hour {record.hour_id}')
        ordinals = [by_id[i].part for i in a.input_ids]
        if ordinals != list(range(ordinals[0], ordinals[0] + len(ordinals))):
            raise LedgerError(f'assembly {a.sha256}: parts {ordinals} are not consecutive')


def check_rules(hours):
    """Fail on an hour served by two sets, an assembly duplicating its parts, or a duplicate delivery path."""
    serving = {}
    records = {}
    for h in hours:
        slot = (h.recording_id, h.local_date, h.delivery_hour)
        if h.record is not None and records.setdefault(h.record.row_id, slot) != slot:
            raise LedgerError(f'hour record {h.record.hour_id} serves two hours')
        if h.served and h.record is None:
            raise LedgerError(f'{slot}: served without an hour record')
        ids = {p.output_id for p in h.record.parts} if h.served else set()
        for s in h.served:
            if not {int(i) for i in s.registry_ids} <= ids:
                raise LedgerError(f'{slot}: {s.file_id} is not from the serving record {h.record.hour_id}')
            for k in (s.file_id, s.key):
                if serving.setdefault(k, slot) != slot:
                    raise LedgerError(f'{k} serves two hours: {serving[k]} and {slot}')
        files = [i for s in h.served for i in s.registry_ids]
        replaced = {i for s in h.served if s.source is Source.CLOCK_ASSEMBLY for i in s.registry_ids}
        dup = sorted(i for i, n in collections.Counter(files).items() if n > 1)
        if dup or replaced & {s.file_id for s in h.served}:
            raise LedgerError(f'{slot}: an assembly duplicates parts {dup or sorted(replaced & {s.file_id for s in h.served})}')
    paths = collections.Counter(s.path for h in hours for s in h.served)
    dup = sorted(p for p, n in paths.items() if n > 1)
    if dup:
        raise LedgerError(f'{len(dup)} delivery paths are served twice, e.g. {dup[:3]}')


# ---- outputs ---------------------------------------------------------------

LEDGER_COLUMNS = ('recording_id', 'stream_id', 'local_date', 'delivery_hour', 'local_hour', 'captured', 'status',
                  'hour_id', 'batch_id', 'parts', 'served', 'complete', 'in_r2', 'source', 'alternates', 'in_registry',
                  'sources', 'file_ids', 'object_keys', 'delivery_paths')
STREAM_COLUMNS = ('recording_id', 'stream_id', 'window_start', 'window_end', 'days_complete', 'days', 'captured_hours',
                  'hours_complete', 'hours_with_record', 'available')
DAY_COLUMNS = ('recording_id', 'stream_id', 'local_date', 'day_stitched', 'captured_hours', 'hours_complete')


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
            'alternates': h.alternates, 'in_registry': yn(h.in_registry),
            'sources': joined(s.source.value for s in h.served), 'file_ids': joined(s.file_id for s in h.served),
            'object_keys': joined(s.key for s in h.served), 'delivery_paths': joined(s.path for s in h.served)}


def day_rows(hours):
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


def builder_files(hours):
    return sorted({i for h in hours if h.ok and h.in_registry for s in h.served for i in s.registry_ids})


def summary_line(hours, streams):
    captured = [h for h in hours if h.captured]
    ok = [h for h in captured if h.ok]
    counts = collections.Counter(s.source for h in ok for s in h.served)
    unpublishable = sum(1 for h in ok if not h.in_registry)
    return (f"streams_available={sum(s['available'] == 'y' for s in streams)}/{len(streams)} "
            f"days_complete={sum(s['days_complete'] for s in streams)}/{sum(s['days'] for s in streams)} "
            f"hours_complete={len(ok)}/{len(captured)} "
            + ' '.join(f'{s.value}={counts[s]}' for s in Source)
            + f' alternates={sum(h.alternates > 0 for h in ok)} unpublishable_hours={unpublishable}')


def write_tsv(path, columns, rows):
    with open(path, 'w', encoding='utf-8', newline='') as f:
        w = csv.DictWriter(f, columns, delimiter='\t', lineterminator='\n', quoting=csv.QUOTE_NONE, escapechar=None)
        w.writeheader()
        w.writerows(rows)


def write(out_dir, hours):
    os.makedirs(out_dir, exist_ok=True)
    days = day_rows(hours)
    streams = stream_rows(hours, days)
    write_tsv(os.path.join(out_dir, 'best14-ledger.tsv'), LEDGER_COLUMNS, [ledger_row(h) for h in hours])
    write_tsv(os.path.join(out_dir, 'best14-ledger-streams.tsv'), STREAM_COLUMNS, streams)
    write_tsv(os.path.join(out_dir, 'best14-ledger-days.tsv'), DAY_COLUMNS, days)
    with open(os.path.join(out_dir, 'best14-ledger-files.txt'), 'w', encoding='utf-8') as f:
        f.writelines(i + '\n' for i in builder_files(hours))
    return summary_line(hours, streams)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest='cmd', required=True)
    q = sub.add_parser('extract-sql', help='print the read-only DB extract query')
    q.add_argument('--best14', required=True)
    b = sub.add_parser('build', help='write the ledger')
    b.add_argument('--best14', required=True)
    b.add_argument('--extract', required=True)
    b.add_argument('--listing', required=True)
    b.add_argument('--registry', required=True)
    b.add_argument('--clock-proofs')
    b.add_argument('--out-dir', required=True)
    a = ap.parse_args(argv)
    try:
        best14 = read_best14(a.best14)
        if a.cmd == 'extract-sql':
            sys.stdout.write(extract_sql(best14))
            return
        hours = select(best14, read_extract(a.extract), read_proofs(a.clock_proofs) if a.clock_proofs else [],
                       read_listing(a.listing), read_registry(a.registry))
        print(write(a.out_dir, hours))
    except LedgerError as e:
        raise SystemExit(f'best14 ledger: {e}')


if __name__ == '__main__':
    main()
