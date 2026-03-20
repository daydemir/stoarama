#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT_FILE="${1:-${ROOT_DIR}/local/do-youtube-relay.env}"

if [[ -f "${ROOT_DIR}/local/operator.env" ]]; then
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/local/operator.env"
fi
if [[ -f "${ROOT_DIR}/local/do-capture.env" ]]; then
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/local/do-capture.env"
fi

require_env() {
  local key="$1"
  if [[ -z "${!key:-}" ]]; then
    echo "error: missing required env var: ${key}" >&2
    exit 1
  fi
}

require_cmd() {
  local bin="$1"
  if ! command -v "${bin}" >/dev/null 2>&1; then
    echo "error: missing required command: ${bin}" >&2
    exit 1
  fi
}

require_cmd openssl

require_env DIGITALOCEAN_TOKEN
require_env AWS_ACCESS_KEY_ID
require_env AWS_SECRET_ACCESS_KEY
require_env BACKEND_API_URL
require_env API_TOKEN
require_env TF_VAR_ssh_public_key

source_public_base_url="${YOUTUBE_RELAY_SOURCE_PUBLIC_BASE_URL:-${TF_VAR_youtube_relay_source_public_base_url:-}}"
if [[ -z "${source_public_base_url}" ]]; then
  echo "error: set YOUTUBE_RELAY_SOURCE_PUBLIC_BASE_URL (or TF_VAR_youtube_relay_source_public_base_url) before running" >&2
  exit 1
fi

shared_token="${YOUTUBE_RELAY_SHARED_TOKEN:-${TF_VAR_youtube_relay_shared_token:-}}"
if [[ -z "${shared_token}" ]]; then
  shared_token="$(openssl rand -base64 48 | tr '+/' '-_' | tr -d '=\n')"
fi

project_name="${TF_VAR_project_name:-Stoarama}"
region="${TF_VAR_region:-nyc3}"
droplet_count="${TF_VAR_droplet_count:-2}"
droplet_size="${TF_VAR_droplet_size:-s-2vcpu-4gb}"
repo_url="${TF_VAR_repo_url:-https://github.com/daydemir/stoarama.git}"
repo_ref="${TF_VAR_repo_ref:-main}"
repo_clone_token="${TF_VAR_repo_clone_token:-}"
network_transport="${TF_VAR_youtube_relay_network_transport:-wireguard}"
topology_id="${TF_VAR_youtube_relay_topology_id:-do-youtube-relay-hub}"
hub_server_id="${TF_VAR_youtube_relay_hub_server_id:-${topology_id}}"
source_server_id="${TF_VAR_youtube_relay_source_server_id:-yt-relay-source-$(hostname -s | tr '[:upper:]' '[:lower:]')}"
wg_interface="${TF_VAR_youtube_relay_wg_interface:-wg0}"
wg_source_ip="${TF_VAR_youtube_relay_wg_source_ip:-10.77.0.2}"
wg_sink_cidr="${TF_VAR_youtube_relay_wg_sink_cidr:-10.77.0.0/24}"
wg_sink_offset="${TF_VAR_youtube_relay_wg_sink_offset:-11}"

mkdir -p "$(dirname "${OUT_FILE}")"
cat > "${OUT_FILE}" <<ENV
export DIGITALOCEAN_TOKEN='${DIGITALOCEAN_TOKEN}'
export AWS_ACCESS_KEY_ID='${AWS_ACCESS_KEY_ID}'
export AWS_SECRET_ACCESS_KEY='${AWS_SECRET_ACCESS_KEY}'

export TF_VAR_project_name='${project_name}'
export TF_VAR_region='${region}'
export TF_VAR_droplet_count='${droplet_count}'
export TF_VAR_droplet_size='${droplet_size}'

export TF_VAR_backend_api_url='${BACKEND_API_URL}'
export TF_VAR_backend_api_token='${API_TOKEN}'
export TF_VAR_ssh_public_key='${TF_VAR_ssh_public_key}'
export TF_VAR_repo_url='${repo_url}'
export TF_VAR_repo_ref='${repo_ref}'
export TF_VAR_repo_clone_token='${repo_clone_token}'

export TF_VAR_youtube_relay_source_public_base_url='${source_public_base_url}'
export TF_VAR_youtube_relay_shared_token='${shared_token}'
export TF_VAR_youtube_relay_network_transport='${network_transport}'
export TF_VAR_youtube_relay_topology_id='${topology_id}'
export TF_VAR_youtube_relay_hub_server_id='${hub_server_id}'
export TF_VAR_youtube_relay_source_server_id='${source_server_id}'
export TF_VAR_youtube_relay_wg_interface='${wg_interface}'
export TF_VAR_youtube_relay_wg_source_ip='${wg_source_ip}'
export TF_VAR_youtube_relay_wg_sink_cidr='${wg_sink_cidr}'
export TF_VAR_youtube_relay_wg_sink_offset='${wg_sink_offset}'

export TF_VAR_youtube_relay_sink_capacity='8'
export TF_VAR_youtube_relay_heartbeat_sec='15'
export TF_VAR_youtube_relay_lease_sec='45'
export TF_VAR_youtube_relay_refresh_sec='5'
export TF_VAR_youtube_relay_unsupported_threshold='8'
export TF_VAR_youtube_relay_frame_queue_size='64'
export TF_VAR_youtube_relay_frame_enqueue_timeout_sec='3'
export TF_VAR_youtube_relay_frame_writers='2'
ENV
chmod 600 "${OUT_FILE}"

echo "wrote ${OUT_FILE}"
echo "shared token prefix: ${shared_token:0:6}... len=${#shared_token}"
