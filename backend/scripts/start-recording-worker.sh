#!/usr/bin/env bash
set -euo pipefail

# Bootstraps the standalone stream-recorder worker on a recorder droplet (or
# locally). It runs a COMPILED stoaramactl binary, never `go run`, so a prebuilt
# droplet image with the binary baked in boots fast. It authenticates with a
# per-droplet local_recorder node token (RECORDER_NODE_TOKEN); the shared
# SERVICE_TOKEN is never used here. RECORDER_SERVER_ID is passed via env (cloud
# -init) so the worker does not need to fetch the cloud metadata service.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BACKEND_DIR="${ROOT_DIR}/backend"
ENV_FILE="${RECORDER_ENV_FILE:-/etc/stoarama/recorder.env}"

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

require_cmd ffmpeg

backend_api_url="${BACKEND_API_URL:-}"
if [[ -z "${backend_api_url}" ]]; then
  echo "error: missing BACKEND_API_URL" >&2
  exit 1
fi
require_env RECORDER_NODE_TOKEN

default_host_id="$(hostname -s | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9-' '-' | sed -E 's/^-+//; s/-+$//; s/-+/-/g')"
export RECORDER_SERVER_ID="${RECORDER_SERVER_ID:-${default_host_id}}"
export RECORDING_WORKER_CONCURRENCY="${RECORDING_WORKER_CONCURRENCY:-1}"
export RECORDING_WORKER_HEARTBEAT_SEC="${RECORDING_WORKER_HEARTBEAT_SEC:-15}"
export RECORDING_WORKER_POLL_SEC="${RECORDING_WORKER_POLL_SEC:-5}"

if [[ -z "${RECORDER_SERVER_ID}" ]]; then
  echo "error: missing RECORDER_SERVER_ID and failed to resolve hostname" >&2
  exit 1
fi

# Prefer a prebuilt binary baked into the droplet image; fall back to a one-time
# local build for hand-launched/dev runs. Never `go run` per invocation.
BIN="${STOARAMACTL_BIN:-/opt/stoarama/bin/stoaramactl}"
if [[ ! -x "${BIN}" ]]; then
  require_cmd go
  BIN="${BACKEND_DIR}/bin/stoaramactl"
  if [[ ! -x "${BIN}" ]]; then
    echo "building stoaramactl at ${BIN} (no prebuilt binary found)"
    (cd "${BACKEND_DIR}" && go build -o bin/stoaramactl ./cmd/stoaramactl)
  fi
fi

cmd=(
  "${BIN}"
  recording-worker run
  --backend-api-url "${backend_api_url}"
  --node-token "${RECORDER_NODE_TOKEN}"
  --worker-id "${RECORDER_SERVER_ID}"
  --concurrency "${RECORDING_WORKER_CONCURRENCY}"
  --heartbeat-sec "${RECORDING_WORKER_HEARTBEAT_SEC}"
  --poll-sec "${RECORDING_WORKER_POLL_SEC}"
)

echo "starting recording worker: worker_id=${RECORDER_SERVER_ID} concurrency=${RECORDING_WORKER_CONCURRENCY} heartbeat_sec=${RECORDING_WORKER_HEARTBEAT_SEC} poll_sec=${RECORDING_WORKER_POLL_SEC}"

exec "${cmd[@]}" "$@"
