#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
THESIS_DIR="$(cd "${ROOT_DIR}/.." && pwd)"
SOCIAL_DIR="${THESIS_DIR}/social-isolation"
ENV_FILE="${SI_ENV_FILE:-${SOCIAL_DIR}/local/wolff-source-refresh.env}"

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

require_cmd poetry
require_cmd ffprobe

export STOARAMA_API_URL="${STOARAMA_API_URL:-${BACKEND_API_URL:-${INFERCTL_API_URL:-}}}"
export STOARAMA_SERVICE_TOKEN="${STOARAMA_SERVICE_TOKEN:-${SERVICE_TOKEN:-${API_TOKEN:-}}}"
require_env STOARAMA_API_URL
require_env STOARAMA_SERVICE_TOKEN

export WOLFF_SOURCE_REFRESH_INTERVAL_SEC="${WOLFF_SOURCE_REFRESH_INTERVAL_SEC:-21600}"
export WOLFF_SOURCE_REFRESH_LIMIT="${WOLFF_SOURCE_REFRESH_LIMIT:-27}"
export WOLFF_SOURCE_REFRESH_REPORT_DIR="${WOLFF_SOURCE_REFRESH_REPORT_DIR:-${SOCIAL_DIR}/local/reports}"
export WOLFF_SOURCE_REFRESH_APPLY="${WOLFF_SOURCE_REFRESH_APPLY:-0}"

if [[ "${WOLFF_SOURCE_REFRESH_INTERVAL_SEC}" -le 0 ]]; then
  echo "error: WOLFF_SOURCE_REFRESH_INTERVAL_SEC must be > 0" >&2
  exit 1
fi
if [[ "${WOLFF_SOURCE_REFRESH_LIMIT}" -le 0 ]]; then
  echo "error: WOLFF_SOURCE_REFRESH_LIMIT must be > 0" >&2
  exit 1
fi

trap 'echo "wolff source refresh sweeper: stopping"; exit 0' INT TERM

while true; do
  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  report_path="${WOLFF_SOURCE_REFRESH_REPORT_DIR}/wolff-source-refresh-${ts}.json"
  args=(
    "tools/resolve_wolff_stream_sources.py"
    "--backend-api-url" "${STOARAMA_API_URL}"
    "--api-token" "${STOARAMA_SERVICE_TOKEN}"
    "--limit" "${WOLFF_SOURCE_REFRESH_LIMIT}"
    "--report-path" "${report_path}"
  )
  if [[ "${WOLFF_SOURCE_REFRESH_APPLY}" == "1" ]]; then
    args+=("--apply")
  fi

  (
    cd "${SOCIAL_DIR}"
    poetry run python "${args[@]}"
  )

  sleep "${WOLFF_SOURCE_REFRESH_INTERVAL_SEC}"
done
