#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${SI_ENV_FILE:-${ROOT_DIR}/local/youtube-worker.env}"
HEALTH_MAX_STALE_SEC="${HEALTH_MAX_STALE_SEC:-180}"
HEALTH_LABEL="${HEALTH_LABEL:-io.stoarama.youtube-capture}"
BACKEND_URL_OVERRIDE="${BACKEND_API_URL_OVERRIDE:-}"
API_TOKEN_OVERRIDE="${API_TOKEN_OVERRIDE:-}"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "healthcheck error: missing env file: ${ENV_FILE}" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "${ENV_FILE}"

backend_api_url="${BACKEND_URL_OVERRIDE:-${BACKEND_API_URL:-${INFERCTL_API_URL:-}}}"
api_token="${API_TOKEN_OVERRIDE:-${API_TOKEN:-}}"
if [[ -z "${backend_api_url}" ]]; then
  echo "healthcheck error: missing BACKEND_API_URL (or INFERCTL_API_URL)" >&2
  exit 1
fi
if [[ -z "${api_token}" ]]; then
  echo "healthcheck error: missing API_TOKEN" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "healthcheck error: jq not found" >&2
  exit 1
fi
if ! command -v launchctl >/dev/null 2>&1; then
  echo "healthcheck error: launchctl not found" >&2
  exit 1
fi

payload="$(curl -fsS \
  -H "Authorization: Bearer ${api_token}" \
  "${backend_api_url}/api/v1/dashboard/streams?recording_state=on&limit=200&offset=0&include_image_urls=false")"

yt_recording_count="$(jq -r '[.items[] | select(.stream.capture_type=="youtube_watch")] | length' <<<"${payload}")"
if [[ "${yt_recording_count}" -eq 0 ]]; then
  echo "healthcheck ok: no recording youtube streams"
  exit 0
fi

fresh_count="$(jq -r --argjson max_stale "${HEALTH_MAX_STALE_SEC}" '
  [.items[]
    | select(.stream.capture_type=="youtube_watch")
    | select((.freshness_sec // 1000000000) <= $max_stale)
  ] | length
' <<<"${payload}")"

if [[ "${fresh_count}" -gt 0 ]]; then
  echo "healthcheck ok: youtube fresh streams=${fresh_count}/${yt_recording_count}"
  exit 0
fi

capacity_payload="$(curl -fsS \
  -H "Authorization: Bearer ${api_token}" \
  "${backend_api_url}/api/v1/dashboard/recording/capacity")"
active_workers="$(jq -r '
  [.items[]
    | select(.execution_class=="youtube_relay")
    | .active_workers // 0
  ] | if length == 0 then 0 else .[0] end
' <<<"${capacity_payload}")"
if [[ "${active_workers}" -gt 0 ]]; then
  echo "healthcheck warn: youtube streams stale but active_workers=${active_workers}; skip restart"
  exit 0
fi

domain="gui/$(id -u)"
target="${domain}/${HEALTH_LABEL}"
echo "healthcheck restart: all recorded youtube streams stale (>${HEALTH_MAX_STALE_SEC}s), kickstart ${target}" >&2
launchctl kickstart -k "${target}"
