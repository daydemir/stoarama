#!/bin/sh
# Collation v2 nightly, ops-host side (holds DATABASE_URL + R2; no media work).
#   nightly-ops.sh plan      -> plan the last 2 closed local days of every active
#                               continuous 08-20 recording and publish the worklist
#   nightly-ops.sh register  -> register every published nightly hour (idempotent)
# Schedule plan once a day after 11:00 UTC (every timezone's day has closed) and
# register hourly. The droplet timer (stoarama-collate-nightly.timer) does the media.
set -eu
CTL=${STOARAMACTL:-stoaramactl}
TODAY=$(date -u +%F)
case "${1:-}" in
plan)
  OUT=$(mktemp)
  "$CTL" collation-v2 plan --scope nightly --days 2 --broken-ids "${BROKEN_IDS:-/dev/null}" --out "$OUT"
  "$CTL" collation-v2 put-worklist --worklist "$OUT" --key "collation-v2/worklists/nightly/$TODAY-$(date -u +%H%M).jsonl"
  rm -f "$OUT" ;;
register)
  YESTERDAY=$(date -u -v-1d +%F 2>/dev/null || date -u -d yesterday +%F)
  for d in "$YESTERDAY" "$TODAY"; do
    "$CTL" collation-v2 register --worklist "r2prefix:collation-v2/worklists/nightly/$d-"
  done ;;
*) echo "usage: nightly-ops.sh plan|register" >&2; exit 2 ;;
esac
