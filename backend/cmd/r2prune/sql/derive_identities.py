"""Derive catalog identities for keys with no direct catalog row (pass 2).
Inputs: snapshot, base identities, derived-db.tsv, best14-sources.tsv.
Writes derived-identities.tsv (identities format, column 10 = provenance tag)
and appends Gen1 acknowledgements to nas-evidence-derived.tsv."""
import sys, re, collections
snap, ids_path, ddb, b14 = sys.argv[1:5]
listed = {}
for l in open(snap):
    k, s, _, _ = l.rstrip('\n').split('\t'); listed[k] = int(s)
clip = {}   # clip id -> (stream, start, end)
raw = {}    # raw key -> (stream, start, end)
for l in open(ids_path):
    c = l.rstrip('\n').split('\t')
    if c[0] == 'raw':
        clip[c[9]] = (int(c[2]), int(c[3]), int(c[4])); raw[c[1]] = clip[c[9]]
rec2stream = {}; gen1 = []; parts = collections.defaultdict(list); page = {}
for l in open(ddb):
    c = l.rstrip('\n').split('\t')
    if c[0] == 'rec': rec2stream[c[1]] = int(c[3])
    elif c[0] == 'gen1': gen1.append(c)
    elif c[0] == 'colpart': parts[(c[3], c[10])].append((int(c[5]), int(c[6])))
    elif c[0] == 'page': page[c[1]] = c
for c in gen1:
    parts[(c[3], c[10])].append((int(c[5]), int(c[6])))
out = open('derived-identities.tsv', 'w'); ev = open('nas-evidence-derived.tsv', 'w')
tags = collections.Counter()
def row(kind, key, stream, a, b, nas, tag):
    if key not in listed: return
    out.write(f"{kind}\t{key}\t{stream}\t{a}\t{b}\t{listed[key]}\t{'t' if nas else 'f'}\t0\t\t{tag}\n"); tags[tag] += 1
# Gen1 artifacts: stream from the hour's recording; interval from schedule and sources.
for c in gen1:
    key, kind, rec, size, a, b, nullsrc, ack, ackv = c[1], c[2], c[3], int(c[4]), int(c[5]), int(c[6]), c[7] == 't', c[8] == 't', int(c[9])
    if kind == 'allocation_ledger' or rec == '0' or rec not in rec2stream: continue   # no hour: stays unclassified
    if nullsrc or size != listed.get(key, -1): a = 0                                   # unknown interval or drift: protected
    row('col', key, rec2stream[rec], a, b, ack, 'gen1_' + kind)
    if ack and key in listed: ev.write(f"col\t{key}\tt\t{ackv}\t0\t{ackv}\n")
# best14: every source clip's stream and interval, one row per stream.
for l in open(b14):
    key, _, srcs, _ = l.rstrip('\n').split('\t')
    by = collections.defaultdict(list); missing = False
    for sid in filter(None, srcs.split(',')):
        if sid in clip: by[clip[sid][0]].append(clip[sid][1:])
        else: missing = True
    if missing or not by: row('col', key, next(iter(by), 0), 0, 0, False, 'best14_missing_source'); continue
    for s, iv in by.items(): row('col', key, s, min(x[0] for x in iv), max(x[1] for x in iv), False, 'best14_receipt')
# repaired/<orig>: the original clip's identity; never on the NAS (the NAS holds the truncated original).
for k in listed:
    if k.startswith('repaired/') and k[len('repaired/'):] in raw:
        s, a, b = raw[k[len('repaired/'):]]; row('raw', k, s, a, b, False, 'repaired_copy')
# confident repairs: recording from the path or the page row; interval = the hour span of the parts it joins.
hourre = re.compile(r'^(.*_hour_\d\d)(?:_dst2)?_parts?_\d+(?:_\d+)?(?:_v\d+)?\.mp4$|^(.*_hour_\d\d)\.mp4$')
def hourkey(name):
    m = hourre.match(name); return (m.group(1) or m.group(2)) if m else None
hourspan = collections.defaultdict(list)
for (rec, base), iv in parts.items():
    h = hourkey(base)
    if h: hourspan[(rec, h)].extend(iv)
pagesha = {}
for k, c in page.items():
    sha, base = c[10].split('|', 1)
    if k.startswith('experimental/stitching-confident-repairs/') and sha: pagesha[sha] = (c[3], base, int(c[4]), k)
for k in listed:
    if not k.startswith('experimental/stitching-confident-repairs/') or k in page: continue
    p = k.split('/')
    if len(p) == 5 and p[4][:64] in pagesha:
        rec, base, size, pkey = pagesha[p[4][:64]]
        if size == listed[k] and pkey in listed and rec in rec2stream:   # same bytes as a live page key
            out.write(f"dup\t{k}\t{rec2stream[rec]}\t0\t0\t{size}\tf\t0\t\tpage_duplicate:{pkey}\n"); tags['repair_page_duplicate'] += 1
        continue
    # Other repairs: names use the delivery page's July/August week-and-weekday hour
    # numbering, which matches no catalog part, and filename dates do not establish
    # intervals. Left without identity: they stay protected.
print(dict(tags))
