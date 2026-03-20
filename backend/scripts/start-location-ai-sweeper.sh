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

export BACKEND_API_URL="${BACKEND_API_URL:-${INFERCTL_API_URL:-}}"
require_env BACKEND_API_URL
require_env API_TOKEN
require_env OPENAI_API_KEY

export LOCATION_AI_SWEEP_MODEL="${LOCATION_AI_SWEEP_MODEL:-gpt-4o-mini}"
export LOCATION_AI_SWEEP_TIMEOUT_SEC="${LOCATION_AI_SWEEP_TIMEOUT_SEC:-90}"
export LOCATION_AI_SWEEP_PAGE_SIZE="${LOCATION_AI_SWEEP_PAGE_SIZE:-300}"
export LOCATION_AI_SWEEP_FRAME_COUNT="${LOCATION_AI_SWEEP_FRAME_COUNT:-3}"
export LOCATION_AI_SWEEP_MAX_STREAMS_PER_PASS="${LOCATION_AI_SWEEP_MAX_STREAMS_PER_PASS:-120}"
export LOCATION_AI_SWEEP_RECHECK_HOURS="${LOCATION_AI_SWEEP_RECHECK_HOURS:-168}"
export LOCATION_AI_SWEEP_MIN_CONFIDENCE="${LOCATION_AI_SWEEP_MIN_CONFIDENCE:-0.72}"
export LOCATION_AI_SWEEP_SLEEP_SEC="${LOCATION_AI_SWEEP_SLEEP_SEC:-1.2}"
export LOCATION_AI_SWEEP_LOOP_SEC="${LOCATION_AI_SWEEP_LOOP_SEC:-120}"
export LOCATION_AI_SWEEP_REQUIRE_CITY="${LOCATION_AI_SWEEP_REQUIRE_CITY:-0}"
export LOCATION_AI_SWEEP_FORCE="${LOCATION_AI_SWEEP_FORCE:-0}"
export LOCATION_AI_SWEEP_TAGS="${LOCATION_AI_SWEEP_TAGS:-}"
export LOCATION_AI_SWEEP_REPORT_DIR="${LOCATION_AI_SWEEP_REPORT_DIR:-${ROOT_DIR}/local/reports}"
mkdir -p "${LOCATION_AI_SWEEP_REPORT_DIR}"

trap 'echo "location-ai sweeper: stopping"; exit 0' INT TERM

echo "starting location-ai sweeper model=${LOCATION_AI_SWEEP_MODEL} page_size=${LOCATION_AI_SWEEP_PAGE_SIZE} max_per_pass=${LOCATION_AI_SWEEP_MAX_STREAMS_PER_PASS}"

while true; do
  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  report_path="${LOCATION_AI_SWEEP_REPORT_DIR}/location-ai-pass-${ts}.json"

  args=(
    "${ROOT_DIR}/tools/enrich_stream_locations_openai.py"
    --model "${LOCATION_AI_SWEEP_MODEL}"
    --timeout-sec "${LOCATION_AI_SWEEP_TIMEOUT_SEC}"
    --page-size "${LOCATION_AI_SWEEP_PAGE_SIZE}"
    --frame-count "${LOCATION_AI_SWEEP_FRAME_COUNT}"
    --max-streams "${LOCATION_AI_SWEEP_MAX_STREAMS_PER_PASS}"
    --recheck-hours "${LOCATION_AI_SWEEP_RECHECK_HOURS}"
    --min-confidence-apply "${LOCATION_AI_SWEEP_MIN_CONFIDENCE}"
    --sleep-sec "${LOCATION_AI_SWEEP_SLEEP_SEC}"
    --report-path "${report_path}"
    --apply
  )

  if [[ -n "${LOCATION_AI_SWEEP_TAGS}" ]]; then
    args+=(--tags "${LOCATION_AI_SWEEP_TAGS}")
  fi
  if [[ "${LOCATION_AI_SWEEP_REQUIRE_CITY}" == "1" ]]; then
    args+=(--require-city)
  fi
  if [[ "${LOCATION_AI_SWEEP_FORCE}" == "1" ]]; then
    args+=(--force)
  fi

  if ! PYTHONUNBUFFERED=1 python3 "${args[@]}"; then
    echo "location-ai sweeper: pass failed" >&2
  fi
  sleep "${LOCATION_AI_SWEEP_LOOP_SEC}"
done
