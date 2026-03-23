#!/bin/zsh
set -euo pipefail

ROOT="/Users/deniz/Build/thesis/stoarama"
SCRIPT="${ROOT}/local/scripts/backfill_missing_stream_images.py"
STOARAMA_BIN="${ROOT}/local/bin/stoarama"

if [[ ! -x "${STOARAMA_BIN}" ]]; then
  echo "error: missing stoarama binary at ${STOARAMA_BIN}" >&2
  exit 1
fi

if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "error: set DATABASE_URL before starting missing-stream-image backfill" >&2
  exit 1
fi

set -a
source "${ROOT}/local/render.env"
set +a

exec python3 "${SCRIPT}" \
  --backend-base-url "https://stoarama.com" \
  --stoarama-bin "${STOARAMA_BIN}" \
  "$@"
