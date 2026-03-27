#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BACKEND_DIR="${ROOT_DIR}/backend"
ENV_FILE="${SI_ENV_FILE:-${ROOT_DIR}/local/recording-supervisor.env}"

if [[ -f "${ENV_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
fi

require_env() {
  local key="$1"
  if [[ -z "${!key:-}" ]]; then
    echo "error: missing required env var: ${key}" >&2
    exit 1
  fi
}

BIN_PATH="${ROOT_DIR}/local/bin/stoaramactl"
if [[ ! -x "${BIN_PATH}" ]]; then
  echo "error: supervisor binary not found: ${BIN_PATH}" >&2
  exit 1
fi

backend_api_url="${BACKEND_API_URL:-${INFERCTL_API_URL:-}}"
if [[ -z "${backend_api_url}" ]]; then
  echo "error: missing BACKEND_API_URL (or INFERCTL_API_URL)" >&2
  exit 1
fi

require_env API_TOKEN

interval_sec="${RECORDING_SUPERVISOR_INTERVAL_SEC:-60}"
limit="${RECORDING_SUPERVISOR_LIMIT:-500}"

cmd=(
  "${BIN_PATH}"
  recording supervisor run
  --backend-api-url "${backend_api_url}"
  --api-token "${API_TOKEN}"
  --interval-sec "${interval_sec}"
  --limit "${limit}"
)

echo "starting recording supervisor: backend_api_url=${backend_api_url} interval_sec=${interval_sec} limit=${limit}"

cd "${BACKEND_DIR}"
exec "${cmd[@]}" "$@"
