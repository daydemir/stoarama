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
mkdir "$LOCK" || { echo "UNRESOLVED: publication lock unavailable at $LOCK; reconcile any prior cycle before retrying" >&2; exit 8; }
armed=0
ledger_tmp=
ledger_started=0
remote() {
  ssh -o BatchMode=yes jelly@jellys-mac-mini "cd ~/stoarama-gss-freeze && /usr/bin/python3 -I gss_gen.py $*"
}
ledger_records() {
  python3 -I - "$1" "$ledger_tmp" "$G" <<'PY'
import hashlib, json, os, pathlib, shutil, stat, sys, tempfile

action, temp, dest = sys.argv[1], pathlib.Path(sys.argv[2]), pathlib.Path(sys.argv[3])
manifest = temp / 'manifest.json'

def sync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try: os.fsync(fd)
    finally: os.close(fd)

def digest(path):
    try: mode = path.lstat().st_mode
    except FileNotFoundError: return None
    if not stat.S_ISREG(mode): raise OSError('ledger is not a regular file: ' + str(path))
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''): h.update(chunk)
    return h.hexdigest()

def copy_verified(source, target, expected):
    # Copy into a sibling scratch file; a read/copy failure must not truncate
    # the destination. Verify exact backup/candidate bytes before replace.
    if expected is None: raise ValueError('missing expected ledger digest')
    fd, name = tempfile.mkstemp(dir=target.parent, prefix='.ledger-copy-')
    os.close(fd)
    scratch = pathlib.Path(name)
    try:
        shutil.copy2(source, scratch)
        if digest(scratch) != expected: raise OSError('ledger copy differs: ' + str(source))
        with scratch.open('rb') as f: os.fsync(f.fileno())
        os.replace(scratch, target)
        sync_dir(target.parent)
    finally:
        try: scratch.unlink()
        except FileNotFoundError: pass

if action == 'prepare':
    records = {}
    for source in sorted((temp / 'new').iterdir()):
        name = source.name
        candidate, original = digest(source), digest(dest / name)
        if candidate is None: raise OSError('candidate ledger disappeared: ' + name)
        if original is not None: copy_verified(dest / name, temp / 'old' / name, original)
        records[name] = {'original': original, 'candidate': candidate}
    if not records: raise ValueError('empty ledger manifest')
    with manifest.open('x') as f:
        json.dump(records, f, sort_keys=True); f.write('\n'); f.flush(); os.fsync(f.fileno())
    sync_dir(temp)
else:
    records = json.loads(manifest.read_text())
    if not records or set(records) != {x.name for x in (temp / 'new').iterdir()}:
        raise ValueError('ledger manifest differs from prepared files')
    failed = False
    for name, record in records.items():
        target = dest / name
        try:
            original, candidate = record['original'], record['candidate']
            current = digest(target)
            if action == 'publish':
                if current != original: raise OSError('original ledger changed: ' + name)
                copy_verified(temp / 'new' / name, target, candidate)
            elif action == 'restore':
                if current not in (None, original, candidate):
                    raise OSError('unrelated ledger replacement: ' + name)
                if original is not None:
                    # Existence came from the durable manifest, not a
                    # possibly unreadable backup directory at recovery time.
                    copy_verified(temp / 'old' / name, target, original)
                elif current is not None:
                    target.unlink()
                    sync_dir(dest)
            else: raise ValueError('unknown ledger action')
        except (OSError, ValueError, KeyError) as error:
            print('ledger %s failed: %s' % (name, error), file=sys.stderr)
            failed = True
            if action == 'publish': break
    if failed: sys.exit(1)
PY
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
      echo "POINTER ROLLBACK VERIFIED $PREV" >&2
      status=4
    else
      echo "UNRESOLVED DIVERGENCE: unable to verify rollback $GEN -> $PREV" >&2
      status=8
    fi
  fi
  if [ "$ledger_started" = 1 ]; then
    ledger_records restore || status=8
    if [ "$status" = 8 ]; then
      echo "Ledger recovery may be needed; backup retained at $ledger_tmp" >&2
      ledger_tmp=
    fi
  fi
  [ -z "$ledger_tmp" ] || rm -rf "$ledger_tmp"
  # Every unresolved cycle must hold subsequent publication, including a
  # rollback failure before ledger preparation has begun.
  if [ "$status" = 8 ]; then
    echo "Publication lock retained at $LOCK until reconciliation" >&2
    exit 8
  fi
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
  if [ "$status" = 8 ] || [ "$status" = 255 ]; then
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
if ssh -o BatchMode=yes jelly@jellys-mac-mini "cd ~/stoarama-gss-freeze || exit 1; /usr/bin/python3 -I gss_gen.py stage $D8 > stage-$D8.log 2>&1; stage_rc=\$?; tail -3 stage-$D8.log || :; exit \"\$stage_rc\""; then
  :
else
  status=$?
  if [ "$status" = 8 ] || [ "$status" = 255 ]; then
    echo "UNRESOLVED DIVERGENCE: staging could not establish a safe publication state" >&2
    exit 8
  fi
  echo "STAGE FAILED $GEN; nothing swapped" >&2
  exit 5
fi
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
ledger_records prepare
ledger_started=1
ledger_records publish
remote verify "$GEN"
# No fallible publication step after this point. Unarm together so a signal
# cannot restore the ledger without also rolling back the pointers.
armed=0 ledger_started=0
tail -1 "$OUT.preview-live.log" || true
echo "PUBLISHED $GEN available=$AV (was $CUR) | $LINE"
