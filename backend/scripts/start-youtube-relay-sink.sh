#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BACKEND_DIR="${ROOT_DIR}/backend"
ENV_FILE="${SI_ENV_FILE:-${ROOT_DIR}/local/youtube-relay-sink.env}"

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

export YOUTUBE_RELAY_SINK_SERVER_ID="${YOUTUBE_RELAY_SINK_SERVER_ID:-}"
if [[ -z "${YOUTUBE_RELAY_SINK_SERVER_ID}" ]] && command -v curl >/dev/null 2>&1; then
  droplet_id="$(curl -fsS --max-time 2 'http://169.254.169.254/metadata/v1/id' 2>/dev/null || true)"
  droplet_id="$(echo "${droplet_id}" | tr -d '[:space:]')"
  if [[ -n "${droplet_id}" ]]; then
    YOUTUBE_RELAY_SINK_SERVER_ID="do-${droplet_id}-yt-relay"
  fi
fi
if [[ -z "${YOUTUBE_RELAY_SINK_SERVER_ID}" ]]; then
  echo "error: missing YOUTUBE_RELAY_SINK_SERVER_ID and failed DO metadata lookup" >&2
  exit 1
fi

export YOUTUBE_RELAY_SINK_WORKER_ID="${YOUTUBE_RELAY_SINK_WORKER_ID:-yt-relay-sink-${YOUTUBE_RELAY_SINK_SERVER_ID}}"
export YOUTUBE_RELAY_SINK_CAPACITY="${YOUTUBE_RELAY_SINK_CAPACITY:-8}"
export YOUTUBE_RELAY_SINK_HEARTBEAT_SEC="${YOUTUBE_RELAY_SINK_HEARTBEAT_SEC:-15}"
export YOUTUBE_RELAY_SINK_LEASE_SEC="${YOUTUBE_RELAY_SINK_LEASE_SEC:-45}"
export YOUTUBE_RELAY_SINK_REFRESH_SEC="${YOUTUBE_RELAY_SINK_REFRESH_SEC:-5}"
export YOUTUBE_RELAY_SINK_UNSUPPORTED_THRESHOLD="${YOUTUBE_RELAY_SINK_UNSUPPORTED_THRESHOLD:-8}"
export YOUTUBE_RELAY_SINK_FRAME_QUEUE_SIZE="${YOUTUBE_RELAY_SINK_FRAME_QUEUE_SIZE:-64}"
export YOUTUBE_RELAY_SINK_FRAME_ENQUEUE_TIMEOUT_SEC="${YOUTUBE_RELAY_SINK_FRAME_ENQUEUE_TIMEOUT_SEC:-3}"
export YOUTUBE_RELAY_SINK_FRAME_WRITERS="${YOUTUBE_RELAY_SINK_FRAME_WRITERS:-2}"
export YOUTUBE_RELAY_NETWORK_TRANSPORT="${YOUTUBE_RELAY_NETWORK_TRANSPORT:-wireguard}"
export YOUTUBE_RELAY_TOPOLOGY_ID="${YOUTUBE_RELAY_TOPOLOGY_ID:-do-youtube-relay-hub}"
export YOUTUBE_RELAY_TOPOLOGY_ROLE="${YOUTUBE_RELAY_TOPOLOGY_ROLE:-sink}"
export YOUTUBE_RELAY_HUB_SERVER_ID="${YOUTUBE_RELAY_HUB_SERVER_ID:-${YOUTUBE_RELAY_TOPOLOGY_ID}}"
export YOUTUBE_RELAY_WG_INTERFACE="${YOUTUBE_RELAY_WG_INTERFACE:-wg0}"
export YOUTUBE_RELAY_WG_IP="${YOUTUBE_RELAY_WG_IP:-}"
export YOUTUBE_RELAY_SOURCE_SERVER_ID="${YOUTUBE_RELAY_SOURCE_SERVER_ID:-}"
export YOUTUBE_RELAY_SINK_METADATA_JSON="${YOUTUBE_RELAY_SINK_METADATA_JSON:-{}}"

cmd=(
  go run ./cmd/stoaramactl
  youtube-relay sink run
  --backend-api-url "${backend_api_url}"
  --api-token "${API_TOKEN}"
  --server-id "${YOUTUBE_RELAY_SINK_SERVER_ID}"
  --worker-id "${YOUTUBE_RELAY_SINK_WORKER_ID}"
  --capacity "${YOUTUBE_RELAY_SINK_CAPACITY}"
  --heartbeat-sec "${YOUTUBE_RELAY_SINK_HEARTBEAT_SEC}"
  --lease-sec "${YOUTUBE_RELAY_SINK_LEASE_SEC}"
  --refresh-sec "${YOUTUBE_RELAY_SINK_REFRESH_SEC}"
  --unsupported-threshold "${YOUTUBE_RELAY_SINK_UNSUPPORTED_THRESHOLD}"
  --frame-queue-size "${YOUTUBE_RELAY_SINK_FRAME_QUEUE_SIZE}"
  --frame-enqueue-timeout-sec "${YOUTUBE_RELAY_SINK_FRAME_ENQUEUE_TIMEOUT_SEC}"
  --frame-writer-workers "${YOUTUBE_RELAY_SINK_FRAME_WRITERS}"
  --network-transport "${YOUTUBE_RELAY_NETWORK_TRANSPORT}"
  --topology-id "${YOUTUBE_RELAY_TOPOLOGY_ID}"
  --topology-role "${YOUTUBE_RELAY_TOPOLOGY_ROLE}"
  --hub-server-id "${YOUTUBE_RELAY_HUB_SERVER_ID}"
  --wg-interface "${YOUTUBE_RELAY_WG_INTERFACE}"
  --metadata-json "${YOUTUBE_RELAY_SINK_METADATA_JSON}"
)
if [[ -n "${YOUTUBE_RELAY_WG_IP}" ]]; then
  cmd+=(--wg-ip "${YOUTUBE_RELAY_WG_IP}")
fi
if [[ -n "${YOUTUBE_RELAY_SOURCE_SERVER_ID}" ]]; then
  cmd+=(--relay-source-server-id "${YOUTUBE_RELAY_SOURCE_SERVER_ID}")
fi
if [[ -n "${YOUTUBE_RELAY_SOURCE_PUBLIC_BASE_URL:-}" ]]; then
  cmd+=(--relay-source-public-base-url "${YOUTUBE_RELAY_SOURCE_PUBLIC_BASE_URL}")
fi

echo "starting youtube-relay sink: server_id=${YOUTUBE_RELAY_SINK_SERVER_ID} worker_id=${YOUTUBE_RELAY_SINK_WORKER_ID} capacity=${YOUTUBE_RELAY_SINK_CAPACITY} transport=${YOUTUBE_RELAY_NETWORK_TRANSPORT} topology=${YOUTUBE_RELAY_TOPOLOGY_ID} role=${YOUTUBE_RELAY_TOPOLOGY_ROLE} hub=${YOUTUBE_RELAY_HUB_SERVER_ID} wg=${YOUTUBE_RELAY_WG_INTERFACE}@${YOUTUBE_RELAY_WG_IP:-na} source=${YOUTUBE_RELAY_SOURCE_SERVER_ID:-na}"

cd "${BACKEND_DIR}"
exec "${cmd[@]}" "$@"
