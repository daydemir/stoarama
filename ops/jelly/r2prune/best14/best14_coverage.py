# usage: /usr/bin/python3 -I best14_coverage.py OUT_DIR --data DIR --ledger best14-ledger.tsv --generation G
#          --ke-sole-copy ke-sole-copy-outputs.tsv --seam-flags zero-gap-flags-visual.tsv
#          --seam-exceptions zero-gap-exceptions.tsv [--exclude-hour REC:YYYY-MM-DD:H ...] [--within KEYS]
# Offline: decides class (b) coverage from extract.py's read-only files and the live listing (DIR/snapshot.tsv).
#
# Owner, Nov 26: "ok lets just make sure we're deleting the raw clips that we know are already stitched.
# we dont need both". A best-14 raw clip is covered only when a stitched output that is registered,
# collated, live in R2 at its catalog size and keep-held contains that clip's full time span, and that
# output's hour record serves the public GSS page in generation G. Writes:
#   coverage.tsv  raw_key, raw_size, output_key, output_size (r2prune --best14-coverage), sorted
#   excluded.tsv  every listed raw of a served hour that is not covered, with the first reason
#                 (--within KEYS limits coverage to the raw keys the owner approved by count)
#   raws.txt, outputs.txt, summary.json
import argparse
import collections
import csv
import json
import sys
from decimal import Decimal
from pathlib import Path

csv.field_size_limit(1 << 30)
CLOCK = 'clock_assembly'


def tsv(path, header=False):
    with open(path, newline='') as f:
        return list(csv.DictReader(f, delimiter='\t') if header else csv.reader(f, delimiter='\t'))


def listing(path):
    out = {}
    with open(path) as f:
        for line in f:
            key, size, etag, _ = line.rstrip('\n').split('\t')
            out[key] = (int(size), etag.strip('"'))
    return out


def excluded_hours(ledger, flags, exceptions, clip_hour, extra):
    """(recording_id, local_date, delivery_hour) whose raws stay: an hour served by or after a clock
    assembly (one may run on into the next hour), both hours at every seam the audit flagged (any
    verdict), the hours of the clips at each reviewer exception, and --exclude-hour."""
    out = {}

    def add(rec, date, hour, why):
        out.setdefault((int(rec), date, int(hour)), why)
    for r in ledger:
        if CLOCK in r['sources'].split(','):
            add(r['recording_id'], r['local_date'], r['delivery_hour'], 'clock_assembly_hour')
            add(r['recording_id'], r['local_date'], int(r['delivery_hour']) + 1, 'after_clock_assembly_hour')
    for r in flags:
        add(r['recording_id'], r['local_date'], r['prev_hour'], 'seam_flagged')
        add(r['recording_id'], r['local_date'], int(r['prev_hour']) + 1, 'seam_flagged')
    for r in exceptions:
        for clip in (r['prev_clip'], r['next_clip']):
            for hour in clip_hour.get(int(clip), ()):
                add(*hour, 'seam_exception')
    for h in extra:
        rec, date, hour = h.split(':')
        add(rec, date, hour, 'excluded_by_operator')
    return out


def build(a):
    d = Path(a.data)
    ledger = tsv(a.ledger, header=True)
    served = {r['hour_id']: r for r in ledger
              if (r['complete'], r['in_r2'], r['published'], r['generation']) == ('y', 'y', 'y', a.generation)}
    if not served:
        raise SystemExit(f'no served hour in generation {a.generation}')
    outputs = collections.defaultdict(list)  # (hour_id, part) -> output rows
    hour_of = {}
    for (hour_id, status, rec, date, dh, oid, key, size, part, start, end, sources,
         chain, timing, edge) in tsv(d / 'outputs.tsv'):
        hour_of[hour_id] = (int(rec), date, int(dh))
        outputs[(hour_id, part)].append(dict(id=int(oid), key=key, size=int(size), status=status, start=Decimal(start),
                                             end=Decimal(end), sources={int(s) for s in sources.split(',') if s},
                                             verified=(chain, timing, edge) == ('true', 'true', 'true')))
    manifest = tsv(d / 'manifest-clips.tsv')
    clip_hour = collections.defaultdict(set)
    for row in manifest:
        if row[0] in hour_of:
            clip_hour[int(row[1])].add(hour_of[row[0]])
    skip_hours = excluded_hours(ledger, tsv(a.seam_flags, header=True), tsv(a.seam_exceptions, header=True), clip_hour,
                                a.exclude_hour)
    catalog = {}
    for cid, stream, key, size, etag, digest, live, start, end in tsv(d / 'catalog-clips.tsv'):
        catalog[int(cid)] = dict(key=key, size=int(size), etag=etag, sha=digest, live=live == 'true',
                                 start=Decimal(start) if start else None, end=Decimal(end) if end else None)
    holds = {}
    for key, hold, owner, receipt, reservation, nonraw, restore in tsv(d / 'output-holds.tsv'):
        holds[key] = dict(hold=hold, owner=owner == 'true', other=(receipt, reservation, nonraw) != ('false',) * 3, restore=restore)
    with open(a.ke_sole_copy, newline='') as f:
        sole = {r['object_key'] for r in csv.DictReader((l for l in f if not l.startswith('#')), delimiter='\t')}
    listed = listing(d / 'snapshot.tsv')
    within = set(Path(a.within).read_text().split()) if a.within else None
    output_keys = {o['key'] for rows in outputs.values() for o in rows}

    def cover_problem(row, o):
        """Why output o does not cover manifest clip row, or None."""
        cid, key, size, digest, etag = int(row[1]), row[4], int(row[5]), row[6], row[7]
        c = catalog.get(cid)
        if c is None or (c['key'], c['size'], c['sha'], c['etag']) != (key, size, digest, etag):
            return 'catalog_differs_from_stitched_bytes'
        if not c['live']:
            return 'catalog_purged'
        if listed.get(key) != (size, etag):
            return 'raw_listing_differs'
        if o['status'] != 'collated':
            return 'hour_not_collated'
        if cid not in o['sources']:
            return 'clip_not_in_output_sources'
        if not o['verified']:
            return 'output_not_verified'
        if not row[10] or Decimal(row[10]) <= 0:
            return 'no_content_seconds'
        if c['start'] is None or c['end'] is None or not (o['start'] <= c['start'] < c['end'] <= o['end']):
            return 'clip_span_outside_output'
        if listed.get(o['key'], (None,))[0] != o['size']:
            return 'output_not_live_at_catalog_size'
        h = holds[o['key']]
        if h['other']:
            return 'output_retirement_named'
        if h['restore'] == 'unverified':
            return 'output_restore_unverified'
        if not (h['owner'] or h['hold'] == f"{o['size']}:{listed[o['key']][1]}"):
            return 'output_not_keep_held'
        if o['key'] in sole:
            return 'output_ke_sole_copy'
        return None

    coverage, excluded, seen = {}, {}, set()
    for row in manifest:
        hour_id, cid, part, disposition, key = row[0], int(row[1]), row[2], row[3], row[4]
        if not key.startswith('managed/') or key not in listed:
            continue  # only live raw keys are in scope
        seen.add(key)
        why = None
        if hour_id not in served:
            why = 'hour_not_serving'
        elif disposition != 'included':
            why = 'disposition_' + disposition
        elif hour_of[hour_id] in skip_hours:
            why = skip_hours[hour_of[hour_id]]
        elif key in output_keys:
            why = 'raw_is_an_output_key'
        elif within is not None and key not in within:
            why = 'outside_approved_list'
        else:
            covers = outputs.get((hour_id, part), [])
            if len(covers) != 1:
                why = f'{len(covers)}_outputs_for_part'
            else:
                why = cover_problem(row, covers[0])
        if why:
            excluded.setdefault(key, (cid, int(row[5]), why))
        else:
            coverage[(key, covers[0]['key'])] = (int(row[5]), covers[0]['size'])
    for key in {k for k, _ in coverage} & excluded.keys():
        del excluded[key]  # covered by its serving hour; an excluded row elsewhere (a non-serving record) is moot
    raws = {k for k, _ in coverage}
    return coverage, excluded, raws, seen


def main(argv):
    p = argparse.ArgumentParser()
    p.add_argument('out')
    p.add_argument('--data', required=True)
    p.add_argument('--ledger', required=True)
    p.add_argument('--generation', required=True)
    p.add_argument('--ke-sole-copy', required=True)
    p.add_argument('--seam-flags', required=True)
    p.add_argument('--seam-exceptions', required=True)
    p.add_argument('--exclude-hour', action='append', default=[])
    p.add_argument('--within')
    a = p.parse_args(argv)
    coverage, excluded, raws, seen = build(a)
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    rows = sorted((k, s, o, os_) for (k, o), (s, os_) in coverage.items())
    (out / 'coverage.tsv').write_text(''.join(f'{k}\t{s}\t{o}\t{os_}\n' for k, s, o, os_ in rows))
    (out / 'raws.txt').write_text(''.join(k + '\n' for k in sorted(raws)))
    (out / 'outputs.txt').write_text(''.join(o + '\n' for o in sorted({o for _, _, o, _ in rows})))
    (out / 'excluded.tsv').write_text('raw_key\tclip_id\tsize_bytes\treason\n' + ''.join(
        f'{k}\t{c}\t{s}\t{w}\n' for k, (c, s, w) in sorted(excluded.items())))
    by = collections.defaultdict(lambda: [0, 0])
    for _, s, w in excluded.values():
        by[w][0] += 1
        by[w][1] += s
    sizes = {k: s for k, s, _, _ in rows}
    summary = {'generation': a.generation, 'listed_raws_in_extracted_hours': len(seen), 'covered_raws': len(raws),
               'covered_bytes': sum(sizes.values()), 'covering_outputs': len({o for _, _, o, _ in rows}),
               'excluded': {w: {'raws': n, 'bytes': b} for w, (n, b) in sorted(by.items())}}
    (out / 'summary.json').write_text(json.dumps(summary, indent=1) + '\n')
    print(json.dumps(summary, indent=1))


if __name__ == '__main__':
    main(sys.argv[1:])
