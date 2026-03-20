#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${SI_ENV_FILE:-/etc/stoarama/capture.env}"
HEALTH_MAX_STALE_SEC="${CAPTURE_HEALTH_MAX_STALE_SEC:-180}"
HEALTH_ASSIGNMENT_LIMIT="${CAPTURE_HEALTH_ASSIGNMENT_LIMIT:-200}"
SERVICE_NAME="${CAPTURE_HEALTH_SERVICE_NAME:-stoarama-capture.service}"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "healthcheck error: missing env file: ${ENV_FILE}" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "${ENV_FILE}"

require_cmd() {
  local bin="$1"
  if ! command -v "${bin}" >/dev/null 2>&1; then
    echo "healthcheck error: required binary not found: ${bin}" >&2
    exit 1
  fi
}

require_cmd curl
require_cmd jq
require_cmd systemctl

backend_api_url="${BACKEND_API_URL:-${INFERCTL_API_URL:-}}"
api_token="${API_TOKEN:-}"
server_id="${CAPTURE_SERVER_ID:-}"

if [[ -z "${backend_api_url}" || -z "${api_token}" || -z "${server_id}" ]]; then
  echo "healthcheck error: missing BACKEND_API_URL/API_TOKEN/CAPTURE_SERVER_ID" >&2
  exit 1
fi

auth_header="Authorization: Bearer ${api_token}"
assignments_url="${backend_api_url}/api/v1/recording/assignments?server_id=${server_id}&limit=${HEALTH_ASSIGNMENT_LIMIT}&offset=0"
if ! payload="$(curl -fsS -H "${auth_header}" "${assignments_url}" 2>/dev/null)"; then
  echo "healthcheck warn: failed to load assignments for server_id=${server_id}; skip restart" >&2
  exit 0
fi

mapfile -t stream_ids < <(jq -r '.items[]?.stream_id' <<<"${payload}")
if [[ "${#stream_ids[@]}" -eq 0 ]]; then
  echo "healthcheck ok: no assignments for server_id=${server_id}"
  exit 0
fi

now_epoch="$(date -u +%s)"
fresh_count=0
checked_count=0

for stream_id in "${stream_ids[@]}"; do
  if [[ -z "${stream_id}" ]]; then
    continue
  fi
  frames_url="${backend_api_url}/api/v1/frames?stream_id=${stream_id}&limit=20&offset=0"
  if ! frames_payload="$(curl -fsS -H "${auth_header}" "${frames_url}" 2>/dev/null)"; then
    echo "healthcheck warn: failed to load frames for stream_id=${stream_id}; continue" >&2
    continue
  fi
  captured_at="$(jq -r '.items[]? | select(.capture_status=="success") | .captured_at' <<<"${frames_payload}" | head -n 1)"
  checked_count=$((checked_count + 1))
  if [[ -z "${captured_at}" ]]; then
    continue
  fi
  if ! captured_epoch="$(date -u -d "${captured_at}" +%s 2>/dev/null)"; then
    continue
  fi
  stale_sec=$((now_epoch - captured_epoch))
  if [[ "${stale_sec}" -le "${HEALTH_MAX_STALE_SEC}" ]]; then
    fresh_count=$((fresh_count + 1))
  fi
done

if [[ "${fresh_count}" -gt 0 ]]; then
  echo "healthcheck ok: fresh assigned streams=${fresh_count}/${#stream_ids[@]} server_id=${server_id}"
  exit 0
fi

if [[ "${checked_count}" -eq 0 ]]; then
  echo "healthcheck warn: no frame payloads checked for server_id=${server_id}; skip restart" >&2
  exit 0
fi

echo "healthcheck restart: assigned streams stale or missing fresh frames for server_id=${server_id}; restarting ${SERVICE_NAME}" >&2
systemctl restart "${SERVICE_NAME}"
