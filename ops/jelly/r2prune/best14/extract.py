# usage: /usr/bin/python3 -I extract.py hours OUT_DIR --ledger best14-ledger.tsv --snapshot-sql CMD --prod-sql CMD
#        /usr/bin/python3 -I extract.py catalog OUT_DIR --listing snapshot.tsv --prod-sql CMD
# Read-only inputs for best14_coverage.py. Each CMD reads one SQL script on stdin and prints its COPY
# rows (on Jelly: `python3 ~/stoarama-analysis-pg/sqlsnap.py` and `python3 ~/stoarama-gss-freeze/sqlro.py`,
# the capped read-only prod path). Hour records and manifests come from the analysis snapshot; a serving
# hour the snapshot does not hold yet is read from prod by hour_id. Raw catalog rows and output holds
# are not in the snapshot and are read from prod by primary key, ROWS rows per statement.
import argparse
import csv
import json
import shlex
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROWS = 5000
HOURS = 500
csv.field_size_limit(1 << 30)


def run(cmd, sql, **lists):
    for name, values in lists.items():
        text = json.dumps(values, sort_keys=True)
        if '$k$' in text:
            raise SystemExit('a value contains $k$')
        sql = sql.replace(f":'{name}'", '$k$' + text + '$k$')
    res = subprocess.run(shlex.split(cmd), input=sql, capture_output=True, text=True)
    if res.returncode:
        raise SystemExit(f'read-only SQL failed: {res.stderr[-2000:]}')
    return [line + '\n' for line in res.stdout.splitlines()]


def chunks(values, n):
    return [values[i:i + n] for i in range(0, len(values), n)]


def serving_hours(ledger):
    with open(ledger, newline='') as f:
        return sorted({r['hour_id'] for r in csv.DictReader(f, delimiter='\t') if r['complete'] == 'y' and r['in_r2'] == 'y'})


def sql(name):
    return (HERE / f'{name}.sql').read_text()


def hours_step(a, out):
    """outputs.tsv and manifest-clips.tsv of every serving hour record, and the R2 prefixes to list."""
    hours = serving_hours(a.ledger)
    outputs, clips = [], []
    for part in chunks(hours, HOURS):
        outputs += run(a.snapshot_sql, sql('outputs'), hours=part)
        clips += run(a.snapshot_sql, sql('manifest-clips'), hours=part)
    missing = sorted(set(hours) - {line.split('\t', 1)[0] for line in outputs})
    for part in chunks(missing, HOURS):
        outputs += run(a.prod_sql, sql('outputs'), hours=part)
        clips += run(a.prod_sql, sql('manifest-clips'), hours=part)
    (out / 'outputs.tsv').write_text(''.join(outputs))
    (out / 'manifest-clips.tsv').write_text(''.join(clips))
    # r2prune list --prefix roots: each raw's stream folder (managed/acct-N/<folder>/, or
    # managed/acct-N/recordings/<id>/) and each output's directory, nested ones dropped.
    raws = [k for k in (line.split('\t')[4] for line in clips) if k.startswith('managed/')]
    dirs = sorted({'/'.join(k.split('/')[:4 if k.split('/')[2] == 'recordings' else 3]) + '/' for k in raws}
                  | {line.split('\t')[6].rsplit('/', 1)[0] + '/' for line in outputs})
    roots = [d for i, d in enumerate(dirs) if not any(d.startswith(r) for r in dirs[:i])]
    (out / 'prefixes.txt').write_text(''.join(r + '\n' for r in roots))
    print(json.dumps({'serving_hours': len(hours), 'hours_read_from_prod': len(missing), 'outputs': len(outputs),
                      'manifest_clips': len(clips), 'prefixes': len(roots)}))


def catalog_step(a, out):
    """catalog-clips.tsv for every included clip whose raw key is listed, and output-holds.tsv for every listed output."""
    with open(a.listing) as f:
        listed = {line.split('\t', 1)[0] for line in f}
    with open(out / 'manifest-clips.tsv') as f:
        ids = sorted({int(c[1]) for c in (line.split('\t') for line in f) if c[3] == 'included' and c[4] in listed})
    with open(out / 'outputs.tsv') as f:
        keys = sorted({c[6] for c in (line.split('\t') for line in f) if c[6] in listed})
    (out / 'catalog-clips.tsv').write_text(''.join(r for part in chunks(ids, ROWS) for r in run(a.prod_sql, sql('catalog-clips'), ids=part)))
    (out / 'output-holds.tsv').write_text(''.join(r for part in chunks(keys, ROWS) for r in run(a.prod_sql, sql('output-holds'), keys=part)))
    print(json.dumps({'catalog_clips': len(ids), 'output_keys': len(keys)}))


def main(argv):
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest='cmd', required=True)
    h = sub.add_parser('hours')
    h.add_argument('out')
    h.add_argument('--ledger', required=True)
    h.add_argument('--snapshot-sql', required=True)
    h.add_argument('--prod-sql', required=True)
    c = sub.add_parser('catalog')
    c.add_argument('out')
    c.add_argument('--listing', required=True)
    c.add_argument('--prod-sql', required=True)
    a = p.parse_args(argv)
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    (hours_step if a.cmd == 'hours' else catalog_step)(a, out)


if __name__ == '__main__':
    main(sys.argv[1:])
