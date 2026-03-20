#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT_FILE="${1:-${ROOT_DIR}/local/youtube-relay-source.env}"

if [[ -f "${ROOT_DIR}/local/operator.env" ]]; then
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/local/operator.env"
fi
if [[ -f "${ROOT_DIR}/local/youtube-worker.env" ]]; then
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/local/youtube-worker.env"
fi
if [[ -f "${ROOT_DIR}/local/do-youtube-relay.env" ]]; then
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/local/do-youtube-relay.env"
fi

require_env() {
  local key="$1"
  if [[ -z "${!key:-}" ]]; then
    echo "error: missing required env var: ${key}" >&2
    exit 1
  fi
}

require_env BACKEND_API_URL
require_env API_TOKEN
require_env TF_VAR_youtube_relay_source_public_base_url
require_env TF_VAR_youtube_relay_shared_token

cookies_file="${YT_DLP_COOKIES_FILE:-}"
cookies_browser="${YT_DLP_COOKIES_FROM_BROWSER:-}"
if [[ -z "${cookies_file}" && -z "${cookies_browser}" ]]; then
  echo "error: missing YT_DLP_COOKIES_FILE/YT_DLP_COOKIES_FROM_BROWSER; set one in local/youtube-worker.env first" >&2
  exit 1
fi
if [[ -n "${cookies_file}" && ! -f "${cookies_file}" ]]; then
  echo "error: YT_DLP_COOKIES_FILE does not exist: ${cookies_file}" >&2
  exit 1
fi

server_id_default="yt-relay-source-$(hostname -s | tr '[:upper:]' '[:lower:]')"
server_id="${YOUTUBE_RELAY_SOURCE_SERVER_ID:-${server_id_default}}"
shard_id="${YOUTUBE_RELAY_SOURCE_SHARD_ID:-yt-account-1}"
capacity="${YOUTUBE_RELAY_SOURCE_CAPACITY:-8}"
topology_id="${YOUTUBE_RELAY_TOPOLOGY_ID:-${TF_VAR_youtube_relay_topology_id:-do-youtube-relay-hub}}"
hub_server_id="${YOUTUBE_RELAY_HUB_SERVER_ID:-${TF_VAR_youtube_relay_hub_server_id:-${topology_id}}}"
network_transport="${YOUTUBE_RELAY_NETWORK_TRANSPORT:-${TF_VAR_youtube_relay_network_transport:-wireguard}}"
wg_interface="${YOUTUBE_RELAY_WG_INTERFACE:-${TF_VAR_youtube_relay_wg_interface:-wg0}}"
wg_ip="${YOUTUBE_RELAY_WG_IP:-${TF_VAR_youtube_relay_wg_source_ip:-10.77.0.2}}"
source_endpoint_default="$(python3 - <<'PY'
import os
from urllib.parse import urlparse
raw = os.environ["TF_VAR_youtube_relay_source_public_base_url"].strip()
parsed = urlparse(raw)
host = (parsed.hostname or "").strip()
port = parsed.port
scheme = (parsed.scheme or "").strip().lower()
if not host:
    raise SystemExit(1)
if port is None:
    port = 443 if scheme == "https" else 80
print(f"{host}:{port}")
PY
)"
source_endpoint="${YOUTUBE_RELAY_SOURCE_ENDPOINT:-${source_endpoint_default}}"

mkdir -p "$(dirname "${OUT_FILE}")"
cat > "${OUT_FILE}" <<ENV
export BACKEND_API_URL='${BACKEND_API_URL}'
export API_TOKEN='${API_TOKEN}'

export YOUTUBE_RELAY_SOURCE_SERVER_ID='${server_id}'
export YOUTUBE_RELAY_SOURCE_SHARD_ID='${shard_id}'
export YOUTUBE_RELAY_SOURCE_CAPACITY='${capacity}'
export YOUTUBE_RELAY_SOURCE_HEARTBEAT_SEC='15'
export YOUTUBE_RELAY_SOURCE_LEASE_SEC='45'
export YOUTUBE_RELAY_SOURCE_REFRESH_SEC='20'
export YOUTUBE_RELAY_SOURCE_RESOLVE_TIMEOUT_SEC='60'
export YOUTUBE_RELAY_SOURCE_RESOLVE_FAILURE_THRESHOLD='3'

export YOUTUBE_RELAY_SOURCE_BIND_ADDR='0.0.0.0:18080'
export YOUTUBE_RELAY_SOURCE_PUBLIC_BASE_URL='${TF_VAR_youtube_relay_source_public_base_url}'
export YOUTUBE_RELAY_SHARED_TOKEN='${TF_VAR_youtube_relay_shared_token}'
export YOUTUBE_RELAY_NETWORK_TRANSPORT='${network_transport}'
export YOUTUBE_RELAY_TOPOLOGY_ID='${topology_id}'
export YOUTUBE_RELAY_TOPOLOGY_ROLE='source'
export YOUTUBE_RELAY_HUB_SERVER_ID='${hub_server_id}'
export YOUTUBE_RELAY_WG_INTERFACE='${wg_interface}'
export YOUTUBE_RELAY_WG_IP='${wg_ip}'
export YOUTUBE_RELAY_SOURCE_ENDPOINT='${source_endpoint}'
ENV

if [[ -n "${cookies_file}" ]]; then
  printf "export YT_DLP_COOKIES_FILE='%s'\n" "${cookies_file}" >> "${OUT_FILE}"
fi
if [[ -n "${cookies_browser}" ]]; then
  printf "export YT_DLP_COOKIES_FROM_BROWSER='%s'\n" "${cookies_browser}" >> "${OUT_FILE}"
fi

chmod 600 "${OUT_FILE}"
echo "wrote ${OUT_FILE}"
echo "source id: ${server_id} shard: ${shard_id} capacity: ${capacity}"
