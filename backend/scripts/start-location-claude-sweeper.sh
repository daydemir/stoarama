#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${SI_ENV_FILE:-${ROOT_DIR}/local/operator.env}"

if [[ -f "${ENV_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
fi

require_cmd() {
  local bin="$1"
  if ! command -v "${bin}" >/dev/null 2>&1; then
    echo "error: required binary not found: ${bin}" >&2
    exit 1
  fi
}

require_env() {
  local key="$1"
  if [[ -z "${!key:-}" ]]; then
    echo "error: missing required env var: ${key}" >&2
    exit 1
  fi
}

require_cmd python3
require_cmd "${CLAUDE_BIN:-claude}"

export BACKEND_API_URL="${BACKEND_API_URL:-${INFERCTL_API_URL:-}}"
require_env BACKEND_API_URL
require_env API_TOKEN

export LOCATION_CLAUDE_SWEEP_CLAUDE_BIN="${LOCATION_CLAUDE_SWEEP_CLAUDE_BIN:-${CLAUDE_BIN:-claude}}"
export LOCATION_CLAUDE_SWEEP_MODEL="${LOCATION_CLAUDE_SWEEP_MODEL:-haiku}"
export LOCATION_CLAUDE_SWEEP_TIMEOUT_SEC="${LOCATION_CLAUDE_SWEEP_TIMEOUT_SEC:-180}"
export LOCATION_CLAUDE_SWEEP_CLAUDE_MAX_RETRIES="${LOCATION_CLAUDE_SWEEP_CLAUDE_MAX_RETRIES:-4}"
export LOCATION_CLAUDE_SWEEP_ENABLE_TOOLS="${LOCATION_CLAUDE_SWEEP_ENABLE_TOOLS:-0}"
export LOCATION_CLAUDE_SWEEP_SHARD_COUNT="${LOCATION_CLAUDE_SWEEP_SHARD_COUNT:-1}"
export LOCATION_CLAUDE_SWEEP_SHARD_INDEX="${LOCATION_CLAUDE_SWEEP_SHARD_INDEX:-0}"
export LOCATION_CLAUDE_SWEEP_PAGE_SIZE="${LOCATION_CLAUDE_SWEEP_PAGE_SIZE:-300}"
export LOCATION_CLAUDE_SWEEP_FRAME_COUNT="${LOCATION_CLAUDE_SWEEP_FRAME_COUNT:-3}"
export LOCATION_CLAUDE_SWEEP_MAX_STREAMS_PER_PASS="${LOCATION_CLAUDE_SWEEP_MAX_STREAMS_PER_PASS:-20}"
export LOCATION_CLAUDE_SWEEP_BATCH_SIZE="${LOCATION_CLAUDE_SWEEP_BATCH_SIZE:-20}"
export LOCATION_CLAUDE_SWEEP_METADATA_MAX_CHARS="${LOCATION_CLAUDE_SWEEP_METADATA_MAX_CHARS:-5000}"
export LOCATION_CLAUDE_SWEEP_BATCH_METADATA_MAX_CHARS="${LOCATION_CLAUDE_SWEEP_BATCH_METADATA_MAX_CHARS:-1400}"
export LOCATION_CLAUDE_SWEEP_BATCH_FALLBACK_SINGLE="${LOCATION_CLAUDE_SWEEP_BATCH_FALLBACK_SINGLE:-1}"
export LOCATION_CLAUDE_SWEEP_RECHECK_HOURS="${LOCATION_CLAUDE_SWEEP_RECHECK_HOURS:-168}"
export LOCATION_CLAUDE_SWEEP_MIN_CONFIDENCE="${LOCATION_CLAUDE_SWEEP_MIN_CONFIDENCE:-0.72}"
export LOCATION_CLAUDE_SWEEP_SLEEP_SEC="${LOCATION_CLAUDE_SWEEP_SLEEP_SEC:-2.0}"
export LOCATION_CLAUDE_SWEEP_LOOP_SEC="${LOCATION_CLAUDE_SWEEP_LOOP_SEC:-180}"
export LOCATION_CLAUDE_SWEEP_REQUIRE_CITY="${LOCATION_CLAUDE_SWEEP_REQUIRE_CITY:-0}"
export LOCATION_CLAUDE_SWEEP_FORCE="${LOCATION_CLAUDE_SWEEP_FORCE:-0}"
export LOCATION_CLAUDE_SWEEP_SKIP_OPENAI_PROCESSED="${LOCATION_CLAUDE_SWEEP_SKIP_OPENAI_PROCESSED:-1}"
export LOCATION_CLAUDE_SWEEP_TAGS="${LOCATION_CLAUDE_SWEEP_TAGS:-}"
export LOCATION_CLAUDE_SWEEP_REPORT_DIR="${LOCATION_CLAUDE_SWEEP_REPORT_DIR:-${ROOT_DIR}/local/reports}"
mkdir -p "${LOCATION_CLAUDE_SWEEP_REPORT_DIR}"

trap 'echo "location-claude sweeper: stopping"; exit 0' INT TERM

echo "starting location-claude sweeper model=${LOCATION_CLAUDE_SWEEP_MODEL} page_size=${LOCATION_CLAUDE_SWEEP_PAGE_SIZE} max_per_pass=${LOCATION_CLAUDE_SWEEP_MAX_STREAMS_PER_PASS} batch_size=${LOCATION_CLAUDE_SWEEP_BATCH_SIZE} shard=${LOCATION_CLAUDE_SWEEP_SHARD_INDEX}/${LOCATION_CLAUDE_SWEEP_SHARD_COUNT}"

while true; do
  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  report_path="${LOCATION_CLAUDE_SWEEP_REPORT_DIR}/location-claude-pass-${ts}.json"

  args=(
    "${ROOT_DIR}/tools/enrich_stream_locations_claude_code.py"
    --claude-bin "${LOCATION_CLAUDE_SWEEP_CLAUDE_BIN}"
    --model "${LOCATION_CLAUDE_SWEEP_MODEL}"
    --shard-count "${LOCATION_CLAUDE_SWEEP_SHARD_COUNT}"
    --shard-index "${LOCATION_CLAUDE_SWEEP_SHARD_INDEX}"
    --timeout-sec "${LOCATION_CLAUDE_SWEEP_TIMEOUT_SEC}"
    --claude-max-retries "${LOCATION_CLAUDE_SWEEP_CLAUDE_MAX_RETRIES}"
    --page-size "${LOCATION_CLAUDE_SWEEP_PAGE_SIZE}"
    --frame-count "${LOCATION_CLAUDE_SWEEP_FRAME_COUNT}"
    --max-streams "${LOCATION_CLAUDE_SWEEP_MAX_STREAMS_PER_PASS}"
    --batch-size "${LOCATION_CLAUDE_SWEEP_BATCH_SIZE}"
    --metadata-max-chars "${LOCATION_CLAUDE_SWEEP_METADATA_MAX_CHARS}"
    --batch-metadata-max-chars "${LOCATION_CLAUDE_SWEEP_BATCH_METADATA_MAX_CHARS}"
    --recheck-hours "${LOCATION_CLAUDE_SWEEP_RECHECK_HOURS}"
    --min-confidence-apply "${LOCATION_CLAUDE_SWEEP_MIN_CONFIDENCE}"
    --sleep-sec "${LOCATION_CLAUDE_SWEEP_SLEEP_SEC}"
    --report-path "${report_path}"
    --apply
  )

  if [[ "${LOCATION_CLAUDE_SWEEP_ENABLE_TOOLS}" == "1" ]]; then
    args+=(--enable-tools)
  fi
  if [[ -n "${LOCATION_CLAUDE_SWEEP_TAGS}" ]]; then
    args+=(--tags "${LOCATION_CLAUDE_SWEEP_TAGS}")
  fi
  if [[ "${LOCATION_CLAUDE_SWEEP_REQUIRE_CITY}" == "1" ]]; then
    args+=(--require-city)
  fi
  if [[ "${LOCATION_CLAUDE_SWEEP_FORCE}" == "1" ]]; then
    args+=(--force)
  fi
  if [[ "${LOCATION_CLAUDE_SWEEP_SKIP_OPENAI_PROCESSED}" == "1" ]]; then
    args+=(--skip-openai-processed)
  else
    args+=(--include-openai-processed)
  fi
  if [[ "${LOCATION_CLAUDE_SWEEP_BATCH_FALLBACK_SINGLE}" == "1" ]]; then
    args+=(--batch-fallback-single)
  else
    args+=(--no-batch-fallback-single)
  fi

  if ! PYTHONUNBUFFERED=1 python3 "${args[@]}"; then
    echo "location-claude sweeper: pass failed" >&2
  fi
  sleep "${LOCATION_CLAUDE_SWEEP_LOOP_SEC}"
done
