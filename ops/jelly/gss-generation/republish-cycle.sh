#!/bin/sh
# One GSS republish cycle from Mooney.
#   republish-cycle.sh REPO_CHECKOUT [EXISTING_BUILD_DIR]
# Build/read-only, offline smokes, create-only stage, conditional swap, live
# preview/viewer/download/ledger gates. Any failure after arming the swap
# attempts a conditional rollback, and an unverified rollback exits 8.
set -eu
[ "$#" -ge 1 ] && [ "$#" -le 2 ] || { echo "usage: $0 REPO_CHECKOUT [EXISTING_BUILD_DIR]" >&2; exit 2; }
REPO=$1; G=$HOME/Build/stoarama/records/finish-plan-20261117/gss-delivery
S=$REPO/backend/scripts/gss
API='https://stoarama-api.onrender.com/api/v1/global-street-scores-delivery/asset?path=/public/metadata.json'
LOCK=$HOME/.stoarama-gss-cycle.lock
mkdir "$LOCK" || { echo "REFUSED: another republish cycle holds $LOCK" >&2; exit 6; }
armed=0
ledger_tmp=
ledger_started=0
remote() {
  ssh -o BatchMode=yes jelly@jellys-mac-mini "cd ~/stoarama-gss-freeze && /usr/bin/python3 -I gss_gen.py $*"
}
finish() {
  status=$1
  trap - 0 HUP INT TERM
  set +e
  if [ "$armed" = 1 ]; then
    for f in preview-live viewer-live download-live livecheck; do
      [ ! -f "$OUT.$f.log" ] || tail -3 "$OUT.$f.log" >&2
    done
    echo "PUBLISH FAILED; attempting conditional rollback $GEN -> $PREV" >&2
    if remote rollback "$ATTEMPT" "$GEN" "$PREV"; then
      echo "ROLLBACK VERIFIED $PREV" >&2
      status=4
    else
      echo "UNRESOLVED DIVERGENCE: unable to verify rollback $GEN -> $PREV" >&2
      status=8
    fi
  fi
  if [ "$ledger_started" = 1 ]; then
    for f in "$ledger_tmp"/new/*; do
      name=${f##*/}
      if [ -e "$ledger_tmp/old/$name" ]; then
        cp -p "$ledger_tmp/old/$name" "$G/$name" || status=8
      else
        rm -f "$G/$name" || status=8
      fi
    done
    if [ "$status" = 8 ]; then
      echo "Ledger recovery may be needed; backup retained at $ledger_tmp" >&2
      ledger_tmp=
    fi
  fi
  [ -z "$ledger_tmp" ] || rm -rf "$ledger_tmp"
  rmdir "$LOCK"
  exit "$status"
}
trap 'finish "$?"' 0
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
set -a; . "$HOME/.credentials/stoarama-r2-readonly.env"; set +a
export AWS_ACCESS_KEY_ID=$R2_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$R2_SECRET_ACCESS_KEY AWS_DEFAULT_REGION=auto
export R2_ENDPOINT=https://$R2_ACCOUNT_ID.r2.cloudflarestorage.com
export GSS_SQL='ssh -o BatchMode=yes jelly@jellys-mac-mini STOARAMA_SQL_TIMEOUT_MS=1800000 /usr/bin/python3 -I stoarama-gss-freeze/sqlro.py'
if [ $# -ge 2 ]; then OUT=$2; else
  SP=${TMPDIR:-/tmp}/gss-cycle; mkdir -p "$SP"; OUT=$SP/$(date -u +%Y%m%dT%H%M%SZ)
  sh "$S/republish.sh" "$G/best14-days.tsv" "$G/best14-grades.tsv" "$OUT" > "$OUT.log" 2>&1 \
    || { tail -8 "$OUT.log"; echo "REFUSED: build or offline smoke failed"; exit 2; }
fi
LINE=$(cat "$OUT/ledger/best14-ledger-summary.txt" 2>/dev/null || grep -h 'streams_available=' "$OUT.log" 2>/dev/null | tail -1 || true)
SUMMARY=$(python3 -I -c '
import json, re, sys
s = json.load(open(sys.argv[1]))
gen, parent = s["generation"], s["parent_generation"]
if gen == parent or not all(re.fullmatch("[0-9a-f]{32}", x) for x in (gen, parent)):
    raise ValueError("invalid candidate/parent pair")
print(gen, parent, int(s["availability"]["available"]), int(s["regressed_stream_days"]))
' "$OUT/generation/summary.json")
read -r GEN PREV AV REG <<EOF
$SUMMARY
EOF
ATTEMPT=$(python3 -I -c '
import json, os, pathlib, sys, uuid
directory = pathlib.Path(sys.argv[1])
directory.mkdir(mode=0o700, exist_ok=True)
attempt = uuid.uuid4().hex
with (directory / (attempt + ".json")).open("x") as f:
    json.dump({"parent": sys.argv[2], "candidate": sys.argv[3]}, f)
    f.write("\n"); f.flush(); os.fsync(f.fileno())
for path in (directory, directory.parent):
    fd = os.open(path, os.O_RDONLY)
    try: os.fsync(fd)
    finally: os.close(fd)
print(attempt)
' "$G/cadence/attempts" "$PREV" "$GEN")
echo "Publication attempt $ATTEMPT: $PREV -> $GEN"
live() {
  response=$(curl -sf "$API") || return 1
  printf '%s\n' "$response" | python3 -I -c "import json,sys; m=json.load(sys.stdin); print(sum(1 for x in (m.get('streams') or m) if x.get('availability')=='available'))"
}
if remote verify "$PREV"; then
  :
else
  status=$?
  if [ "$status" = 8 ]; then
    echo "UNRESOLVED DIVERGENCE: prior publication requires reconciliation" >&2
    exit 8
  fi
  echo "REFUSED: manifest parent is not verified live" >&2
  exit 6
fi
CUR=$(live)
[ "$REG" = 0 ] && [ "$AV" -ge "$CUR" ] || { echo "REFUSED $GEN: regressed=$REG available=$AV live=$CUR"; exit 3; }
for smoke in viewer_smoke.py download_smoke.py; do
  python3 -I "$S/$smoke" --registry "$OUT/generation/registry.json.gz" --assets "$OUT/generation/objects" "$OUT/in/assets" > "$OUT.$smoke.log" 2>&1 \
    || { tail -5 "$OUT.$smoke.log"; echo "REFUSED $GEN: offline $smoke failed"; exit 7; }
done
D8=gen-ledger-$(echo "$GEN" | cut -c1-8)
rsync -a "$OUT/generation/" "jelly@jellys-mac-mini:stoarama-gss-freeze/$D8/"
ssh -o BatchMode=yes jelly@jellys-mac-mini "cd ~/stoarama-gss-freeze && /usr/bin/python3 -I gss_gen.py stage $D8 > stage-$D8.log 2>&1 || { tail -3 stage-$D8.log; exit 1; }; tail -1 stage-$D8.log" || { echo "STAGE FAILED $GEN; nothing swapped"; exit 5; }
# Arm BEFORE SSH: a disconnect may happen after a successful remote write.
armed=1
if remote swap "$ATTEMPT" "$PREV" "$GEN"; then
  :
else
  status=$?
  case $status in
    3) armed=0; echo "REFUSED: swap precondition failed; nothing swapped" >&2 ;;
    4) armed=0; echo "SWAP FAILED; remote rollback verified" >&2 ;;
  esac
  exit "$status"
fi
sleep 5
post() {
  python3 -I "$G/cadence/gss_preview_check.py" "$GEN" "$OUT/generation/registry.json.gz" current > "$OUT.preview-live.log" 2>&1 \
    && grep -q "failures 0" "$OUT.preview-live.log" \
    && python3 -I "$S/viewer_smoke.py" --current > "$OUT.viewer-live.log" 2>&1 \
    && python3 -I "$S/download_smoke.py" --current > "$OUT.download-live.log" 2>&1 \
    && python3 -I "$S/best14_ledger.py" live-check --ledger-dir "$OUT/ledger" --current > "$OUT.livecheck.log" 2>&1 \
    && available=$(live) && [ "$available" -ge "$CUR" ] \
    && remote verify "$GEN"
}
post || { echo "POST-SWAP GATE FAILED" >&2; exit 4; }
# Prepare and back up all ledger files before changing any of the record copies.
ledger_tmp=$(mktemp -d "$G/.gss-ledger.XXXXXX")
mkdir "$ledger_tmp/new" "$ledger_tmp/old"
cp "$OUT"/ledger/best14-ledger*.tsv "$ledger_tmp/new/"
if [ -e "$OUT/ledger/best14-ledger-summary.txt" ]; then
  cp "$OUT/ledger/best14-ledger-summary.txt" "$ledger_tmp/new/"
else
  printf '%s\n' "$LINE" > "$ledger_tmp/new/best14-ledger-summary.txt"
fi
printf '%s\n' "Ledger status ($(date -u +%FT%TZ)): ledger files describe live gen $GEN, $AV/60 available (all post-swap gates passed)." > "$ledger_tmp/new/best14-ledger-STATUS.txt"
for f in "$ledger_tmp"/new/*; do
  name=${f##*/}
  [ ! -e "$G/$name" ] || cp -p "$G/$name" "$ledger_tmp/old/$name"
done
ledger_started=1
for f in "$ledger_tmp"/new/*; do cp "$f" "$G/${f##*/}"; done
remote verify "$GEN"
# No fallible publication step after this point. Unarm together so a signal
# cannot restore the ledger without also rolling back the pointers.
armed=0 ledger_started=0
tail -1 "$OUT.preview-live.log" || true
echo "PUBLISHED $GEN available=$AV (was $CUR) | $LINE"
