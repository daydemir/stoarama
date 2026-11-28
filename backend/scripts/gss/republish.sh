#!/bin/sh
# One read-only GSS republish build: fetch the live generation, its assets, the
# DB extract, the keep holds and live R2 listings, then write the best-14 ledger
# and a new generation ready for create-only staging.
#
#   republish.sh BEST14_DAYS_TSV BEST14_GRADES_TSV OUT_DIR [--dry-run] [--clock-proofs DIR --clock-uploads LOG]
#
# OUT_DIR must not exist. It gets in/ (every input), ledger/ (the new generation's
# best14-ledger*.tsv and summary, published=n until best14_ledger.py live-check) and generation/ (registry.json.gz, objects/, uploads.tsv, holds.tsv,
# summary.json), the directory a stager uploads create-only. Nothing here writes
# R2 or the database, and current.json is only read.
#
# Environment: R2_BUCKET, R2_ENDPOINT and AWS credentials that can read it
# (read-only keys are enough). GSS_SQL is a command that runs SQL from stdin
# and prints psql -A -F TAB output with a header; by default psql with the
# caller's PG* environment and default_transaction_read_only=on.
set -eu
[ $# -ge 3 ] || { sed -n '2,16p' "$0"; exit 2; }
BEST14=$1 GRADES=$2 OUT=$3
shift 3
MODE= CLOCK_PROOFS= CLOCK_UPLOADS=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --dry-run) MODE=--dry-run; shift ;;
    --clock-proofs) [ "$#" -ge 2 ] || exit 2; CLOCK_PROOFS=$2; shift 2 ;;
    --clock-uploads) [ "$#" -ge 2 ] || exit 2; CLOCK_UPLOADS=$2; shift 2 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done
if [ -n "$CLOCK_PROOFS" ] || [ -n "$CLOCK_UPLOADS" ]; then
  [ -d "$CLOCK_PROOFS" ] && [ -f "$CLOCK_UPLOADS" ] || {
    echo 'clock proofs and verified upload log must be supplied together' >&2; exit 2;
  }
fi
HERE=$(cd "$(dirname "$0")" && pwd)
PY="python3 -I"
sql() {
  if [ -n "${GSS_SQL:-}" ]; then sh -c "$GSS_SQL"
  else PGOPTIONS='-c default_transaction_read_only=on' psql -XqA -F "$(printf '\t')" -P footer=off -v ON_ERROR_STOP=1 -f -; fi
}
PUBLIC=datasets/global-street-scores-delivery/47/public
s3() { aws --endpoint-url "$R2_ENDPOINT" "$@"; }

[ ! -e "$OUT" ] || { echo "$OUT exists" >&2; exit 1; }
mkdir -p "$OUT/in/assets"
IN=$OUT/in
s3 s3 cp --quiet "s3://$R2_BUCKET/$PUBLIC/current.json" "$IN/current.json"
$PY - "$IN/current.json" > "$IN/asset-keys.txt" <<'EOF'
import gzip, json, sys
raw = open(sys.argv[1], 'rb').read()
for a in json.loads(gzip.decompress(raw) if raw[:2] == b'\x1f\x8b' else raw)['assets'].values():
    print(a['key'])
EOF
export R2_ENDPOINT R2_BUCKET IN
sort -u "$IN/asset-keys.txt" | xargs -P 16 -n 1 sh -c \
  'mkdir -p "$IN/assets/$(dirname "$0")" && aws --endpoint-url "$R2_ENDPOINT" s3 cp --quiet "s3://$R2_BUCKET/$0" "$IN/assets/$0"'

$PY "$HERE/best14_ledger.py" extract-sql --best14 "$BEST14" --registry "$IN/current.json" --assets "$IN/assets" | sql > "$IN/extract.tsv"
$PY "$HERE/best14_ledger.py" holds-sql | sql > "$IN/holds.tsv"
echo "SELECT json_agg(json_build_object('object_key',object_key,'kind',kind,'recording_id',recording_id,'relative_path',relative_path)) FROM delivery_keep_holds WHERE bucket='stoarama' AND object_key LIKE 'datasets/global-street-scores-delivery/47/assets/%';" |
  sql | tail -n +2 > "$IN/base-holds.json"
for p in joined experimental; do
  s3 s3api list-objects-v2 --bucket "$R2_BUCKET" --prefix "$p/" --query 'Contents[].[Key,Size,ETag]' --output text > "$IN/listing-$p.txt"
done

set -- --best14 "$BEST14" --registry "$IN/current.json" --assets "$IN/assets" --extract "$IN/extract.tsv" --holds "$IN/holds.tsv" --listing "$IN/listing-joined.txt" "$IN/listing-experimental.txt"
if [ -n "$CLOCK_PROOFS" ]; then
  set -- "$@" --clock-proofs "$CLOCK_PROOFS" --clock-uploads "$CLOCK_UPLOADS"
fi
if [ "$MODE" = --dry-run ]; then
  $PY "$HERE/build_generation.py" --ledger "$@" --base-holds "$IN/base-holds.json" --grades "$GRADES" \
    --note "Best 14 days from the delivery ledger" --dry-run
else
  $PY "$HERE/build_generation.py" --ledger "$@" --base-holds "$IN/base-holds.json" --grades "$GRADES" \
    --note "Best 14 days from the delivery ledger" --out "$OUT/generation" --ledger-out "$OUT/ledger"
  cat "$OUT/ledger/best14-ledger-summary.txt"
  $PY "$HERE/viewer_smoke.py" --registry "$OUT/generation/registry.json.gz" --assets "$OUT/generation/objects" "$IN/assets"
  $PY "$HERE/download_smoke.py" --registry "$OUT/generation/registry.json.gz" --assets "$OUT/generation/objects" "$IN/assets"
fi
