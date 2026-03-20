#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BACKEND_DIR="${ROOT_DIR}/backend"
ENV_FILE="${SI_ENV_FILE:-${ROOT_DIR}/local/youtube-relay-source.env}"

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

BIN_DIR="${ROOT_DIR}/local/bin"
BIN_PATH="${BIN_DIR}/stoaramactl"
STATE_DIR="${ROOT_DIR}/local/state"
CACHE_FILE="${YOUTUBE_RELAY_SOURCE_CACHE_FILE:-${STATE_DIR}/youtube-relay-source-cache.json}"
if [[ ! -x "${BIN_PATH}" ]]; then
  echo "error: relay source binary not found: ${BIN_PATH}" >&2
  echo "hint: run backend/scripts/install-local-youtube-relay-source-launchd.sh to build and install it" >&2
  exit 1
fi
mkdir -p "${STATE_DIR}"

if [[ -n "${YT_DLP_BIN:-}" ]]; then
  if [[ ! -x "${YT_DLP_BIN}" ]]; then
    echo "error: YT_DLP_BIN is not executable: ${YT_DLP_BIN}" >&2
    exit 1
  fi
else
  require_cmd yt-dlp
fi

if [[ -z "${YT_DLP_COOKIES_FILE:-}" && -z "${YT_DLP_COOKIES_FROM_BROWSER:-}" ]]; then
  echo "error: set YT_DLP_COOKIES_FILE or YT_DLP_COOKIES_FROM_BROWSER for YouTube auth" >&2
  exit 1
fi
if [[ -n "${YT_DLP_COOKIES_FILE:-}" && ! -f "${YT_DLP_COOKIES_FILE}" ]]; then
  echo "error: YT_DLP_COOKIES_FILE does not exist: ${YT_DLP_COOKIES_FILE}" >&2
  exit 1
fi

backend_api_url="${BACKEND_API_URL:-${INFERCTL_API_URL:-}}"
if [[ -z "${backend_api_url}" ]]; then
  echo "error: missing BACKEND_API_URL (or INFERCTL_API_URL)" >&2
  exit 1
fi
require_env API_TOKEN
require_env YOUTUBE_RELAY_SOURCE_PUBLIC_BASE_URL
require_env YOUTUBE_RELAY_SHARED_TOKEN

relay_public_host="$(python3 - <<'PY'
import os, sys
from urllib.parse import urlparse
raw = os.environ.get("YOUTUBE_RELAY_SOURCE_PUBLIC_BASE_URL", "").strip()
try:
    parsed = urlparse(raw)
except Exception:
    sys.exit(1)
print((parsed.hostname or "").strip())
PY
)"
if [[ -z "${relay_public_host}" ]]; then
  echo "error: failed to parse YOUTUBE_RELAY_SOURCE_PUBLIC_BASE_URL host" >&2
  exit 1
fi
if [[ "${YOUTUBE_RELAY_ALLOW_LOOPBACK_PUBLIC_BASE_URL:-0}" != "1" ]]; then
  case "${relay_public_host}" in
    localhost|127.0.0.1|::1)
      echo "error: YOUTUBE_RELAY_SOURCE_PUBLIC_BASE_URL must be reachable from relay sinks; loopback host ${relay_public_host} is not allowed" >&2
      echo "hint: set YOUTUBE_RELAY_ALLOW_LOOPBACK_PUBLIC_BASE_URL=1 only for local dev" >&2
      exit 1
      ;;
  esac
fi

default_host_id="$(hostname -s | tr '[:upper:]' '[:lower:]')"
export YOUTUBE_RELAY_SOURCE_SERVER_ID="${YOUTUBE_RELAY_SOURCE_SERVER_ID:-yt-relay-source-${default_host_id}}"
export YOUTUBE_RELAY_SOURCE_SHARD_ID="${YOUTUBE_RELAY_SOURCE_SHARD_ID:-yt-account-1}"
export YOUTUBE_RELAY_SOURCE_CAPACITY="${YOUTUBE_RELAY_SOURCE_CAPACITY:-4}"
export YOUTUBE_RELAY_SOURCE_HEARTBEAT_SEC="${YOUTUBE_RELAY_SOURCE_HEARTBEAT_SEC:-15}"
export YOUTUBE_RELAY_SOURCE_LEASE_SEC="${YOUTUBE_RELAY_SOURCE_LEASE_SEC:-45}"
export YOUTUBE_RELAY_SOURCE_REFRESH_SEC="${YOUTUBE_RELAY_SOURCE_REFRESH_SEC:-20}"
export YOUTUBE_RELAY_SOURCE_RESOLVE_TIMEOUT_SEC="${YOUTUBE_RELAY_SOURCE_RESOLVE_TIMEOUT_SEC:-60}"
export YOUTUBE_RELAY_SOURCE_RESOLVE_FAILURE_THRESHOLD="${YOUTUBE_RELAY_SOURCE_RESOLVE_FAILURE_THRESHOLD:-3}"
export YOUTUBE_RELAY_SOURCE_BIND_ADDR="${YOUTUBE_RELAY_SOURCE_BIND_ADDR:-:18080}"
export YOUTUBE_RELAY_NETWORK_TRANSPORT="${YOUTUBE_RELAY_NETWORK_TRANSPORT:-wireguard}"
export YOUTUBE_RELAY_TOPOLOGY_ID="${YOUTUBE_RELAY_TOPOLOGY_ID:-do-youtube-relay-hub}"
export YOUTUBE_RELAY_TOPOLOGY_ROLE="${YOUTUBE_RELAY_TOPOLOGY_ROLE:-source}"
export YOUTUBE_RELAY_HUB_SERVER_ID="${YOUTUBE_RELAY_HUB_SERVER_ID:-${YOUTUBE_RELAY_TOPOLOGY_ID}}"
export YOUTUBE_RELAY_WG_INTERFACE="${YOUTUBE_RELAY_WG_INTERFACE:-wg0}"
export YOUTUBE_RELAY_WG_IP="${YOUTUBE_RELAY_WG_IP:-}"
export YOUTUBE_RELAY_SOURCE_ENDPOINT="${YOUTUBE_RELAY_SOURCE_ENDPOINT:-}"
export YOUTUBE_RELAY_SOURCE_METADATA_JSON="${YOUTUBE_RELAY_SOURCE_METADATA_JSON:-{}}"

cmd=(
  "${BIN_PATH}"
  youtube-relay source run
  --backend-api-url "${backend_api_url}"
  --api-token "${API_TOKEN}"
  --server-id "${YOUTUBE_RELAY_SOURCE_SERVER_ID}"
  --shard-id "${YOUTUBE_RELAY_SOURCE_SHARD_ID}"
  --capacity "${YOUTUBE_RELAY_SOURCE_CAPACITY}"
  --heartbeat-sec "${YOUTUBE_RELAY_SOURCE_HEARTBEAT_SEC}"
  --lease-sec "${YOUTUBE_RELAY_SOURCE_LEASE_SEC}"
  --refresh-sec "${YOUTUBE_RELAY_SOURCE_REFRESH_SEC}"
  --resolve-timeout-sec "${YOUTUBE_RELAY_SOURCE_RESOLVE_TIMEOUT_SEC}"
  --resolve-failure-threshold "${YOUTUBE_RELAY_SOURCE_RESOLVE_FAILURE_THRESHOLD}"
  --bind-addr "${YOUTUBE_RELAY_SOURCE_BIND_ADDR}"
  --public-base-url "${YOUTUBE_RELAY_SOURCE_PUBLIC_BASE_URL}"
  --shared-token "${YOUTUBE_RELAY_SHARED_TOKEN}"
  --cache-file "${CACHE_FILE}"
  --network-transport "${YOUTUBE_RELAY_NETWORK_TRANSPORT}"
  --topology-id "${YOUTUBE_RELAY_TOPOLOGY_ID}"
  --topology-role "${YOUTUBE_RELAY_TOPOLOGY_ROLE}"
  --hub-server-id "${YOUTUBE_RELAY_HUB_SERVER_ID}"
  --wg-interface "${YOUTUBE_RELAY_WG_INTERFACE}"
  --metadata-json "${YOUTUBE_RELAY_SOURCE_METADATA_JSON}"
)
if [[ -n "${YOUTUBE_RELAY_WG_IP}" ]]; then
  cmd+=(--wg-ip "${YOUTUBE_RELAY_WG_IP}")
fi
if [[ -n "${YOUTUBE_RELAY_SOURCE_ENDPOINT}" ]]; then
  cmd+=(--source-endpoint "${YOUTUBE_RELAY_SOURCE_ENDPOINT}")
fi

if [[ -n "${YT_DLP_COOKIES_FILE:-}" ]]; then
  cmd+=(--yt-dlp-cookies-file "${YT_DLP_COOKIES_FILE}")
fi
if [[ -n "${YT_DLP_COOKIES_FROM_BROWSER:-}" ]]; then
  cmd+=(--yt-dlp-cookies-from-browser "${YT_DLP_COOKIES_FROM_BROWSER}")
fi
if [[ -n "${YT_DLP_BIN:-}" ]]; then
  cmd+=(--yt-dlp-bin "${YT_DLP_BIN}")
fi
if [[ -n "${YT_DLP_FORMAT:-}" ]]; then
  cmd+=(--yt-dlp-format "${YT_DLP_FORMAT}")
fi
if [[ -n "${YT_DLP_FORMAT_SORT:-}" ]]; then
  cmd+=(--yt-dlp-format-sort "${YT_DLP_FORMAT_SORT}")
fi

echo "starting youtube-relay source: server_id=${YOUTUBE_RELAY_SOURCE_SERVER_ID} shard_id=${YOUTUBE_RELAY_SOURCE_SHARD_ID} capacity=${YOUTUBE_RELAY_SOURCE_CAPACITY} bind_addr=${YOUTUBE_RELAY_SOURCE_BIND_ADDR} public_base_url=${YOUTUBE_RELAY_SOURCE_PUBLIC_BASE_URL} transport=${YOUTUBE_RELAY_NETWORK_TRANSPORT} topology=${YOUTUBE_RELAY_TOPOLOGY_ID} role=${YOUTUBE_RELAY_TOPOLOGY_ROLE} hub=${YOUTUBE_RELAY_HUB_SERVER_ID} wg=${YOUTUBE_RELAY_WG_INTERFACE}@${YOUTUBE_RELAY_WG_IP:-na}"

cd "${BACKEND_DIR}"
exec "${cmd[@]}" "$@"
