#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BACKEND_DIR="${ROOT_DIR}/backend"
ENV_FILE="${SI_ENV_FILE:-${ROOT_DIR}/local/capture-server.env}"

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

require_cmd go
require_cmd ffmpeg

backend_api_url="${BACKEND_API_URL:-${INFERCTL_API_URL:-}}"
if [[ -z "${backend_api_url}" ]]; then
  echo "error: missing BACKEND_API_URL (or INFERCTL_API_URL)" >&2
  exit 1
fi
require_env API_TOKEN
require_env CAPTURE_SERVER_CAPTURE_SHARED_CAPACITY

default_host_id="$(hostname -s | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9-' '-' | sed -E 's/^-+//; s/-+$//; s/-+/-/g')"
export WORKER_ID="${WORKER_ID:-capture-server-${default_host_id}}"
export CAPTURE_SERVER_ID="${CAPTURE_SERVER_ID:-}"
export CAPTURE_TICK_SEC="${CAPTURE_TICK_SEC:-5}"
export CAPTURE_SERVER_HEARTBEAT_SEC="${CAPTURE_SERVER_HEARTBEAT_SEC:-15}"
export CAPTURE_SERVER_LEASE_SEC="${CAPTURE_SERVER_LEASE_SEC:-45}"
export CAPTURE_UNSUPPORTED_THRESHOLD="${CAPTURE_UNSUPPORTED_THRESHOLD:-8}"
export CAPTURE_FRAME_QUEUE_SIZE="${CAPTURE_FRAME_QUEUE_SIZE:-64}"
export CAPTURE_FRAME_ENQUEUE_TIMEOUT_SEC="${CAPTURE_FRAME_ENQUEUE_TIMEOUT_SEC:-3}"
export CAPTURE_FRAME_WRITERS="${CAPTURE_FRAME_WRITERS:-2}"
export CAPTURE_SERVER_METADATA_JSON="${CAPTURE_SERVER_METADATA_JSON:-{}}"

if [[ -z "${CAPTURE_SERVER_ID}" && -n "${default_host_id}" ]]; then
  CAPTURE_SERVER_ID="${default_host_id}"
fi

if [[ -z "${CAPTURE_SERVER_ID}" ]] && command -v curl >/dev/null 2>&1; then
  for _ in 1 2 3 4 5; do
    droplet_id="$(curl -fsS --max-time 2 'http://169.254.169.254/metadata/v1/id' 2>/dev/null || true)"
    droplet_id="$(echo "${droplet_id}" | tr -d '[:space:]')"
    if [[ -n "${droplet_id}" ]]; then
      CAPTURE_SERVER_ID="do-${droplet_id}"
      break
    fi
    sleep 1
  done
fi

if [[ -z "${CAPTURE_SERVER_ID}" ]]; then
  echo "error: missing CAPTURE_SERVER_ID and failed to resolve hostname or DO droplet metadata id" >&2
  exit 1
fi

if [[ -n "${CAPTURE_SERVER_STREAM_IDS:-}" && "${ALLOW_UNMANAGED_STREAM_FILTER:-0}" != "1" ]]; then
  echo "error: CAPTURE_SERVER_STREAM_IDS is disabled for production launchers; use assignments or set ALLOW_UNMANAGED_STREAM_FILTER=1 for explicit debug-only runs" >&2
  exit 1
fi

cmd=(
  go run ./cmd/stoaramactl
  capture-server run
  --backend-api-url "${backend_api_url}"
  --worker-id "${WORKER_ID}"
  --capture-shared-capacity "${CAPTURE_SERVER_CAPTURE_SHARED_CAPACITY}"
  --heartbeat-sec "${CAPTURE_SERVER_HEARTBEAT_SEC}"
  --lease-sec "${CAPTURE_SERVER_LEASE_SEC}"
  --refresh-sec "${CAPTURE_TICK_SEC}"
  --unsupported-threshold "${CAPTURE_UNSUPPORTED_THRESHOLD}"
  --frame-queue-size "${CAPTURE_FRAME_QUEUE_SIZE}"
  --frame-enqueue-timeout-sec "${CAPTURE_FRAME_ENQUEUE_TIMEOUT_SEC}"
  --frame-writer-workers "${CAPTURE_FRAME_WRITERS}"
  --metadata-json "${CAPTURE_SERVER_METADATA_JSON}"
)
cmd+=(--server-id "${CAPTURE_SERVER_ID}")
if [[ -n "${CAPTURE_SERVER_EXECUTION_CLASSES:-}" ]]; then
  cmd+=(--execution-classes "${CAPTURE_SERVER_EXECUTION_CLASSES}")
fi
if [[ -n "${CAPTURE_SERVER_STREAM_IDS:-}" ]]; then
  cmd+=(--stream-ids "${CAPTURE_SERVER_STREAM_IDS}")
fi
if [[ -n "${CAPTURE_SERVER_DRAINING_EXECUTION_CLASSES:-}" ]]; then
  cmd+=(--draining-execution-classes "${CAPTURE_SERVER_DRAINING_EXECUTION_CLASSES}")
fi

echo "starting capture-server: worker_id=${WORKER_ID} server_id=${CAPTURE_SERVER_ID} video_live_capacity=${CAPTURE_SERVER_CAPTURE_SHARED_CAPACITY} heartbeat_sec=${CAPTURE_SERVER_HEARTBEAT_SEC} lease_sec=${CAPTURE_SERVER_LEASE_SEC} refresh_sec=${CAPTURE_TICK_SEC}"
if [[ -n "${CAPTURE_SERVER_EXECUTION_CLASSES:-}" ]]; then
  echo "execution class filter: ${CAPTURE_SERVER_EXECUTION_CLASSES}"
fi
if [[ -n "${CAPTURE_SERVER_STREAM_IDS:-}" ]]; then
  echo "stream filter: ${CAPTURE_SERVER_STREAM_IDS}"
fi

cd "${BACKEND_DIR}"
exec "${cmd[@]}" "$@"
