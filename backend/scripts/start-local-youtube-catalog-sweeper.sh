#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${SI_ENV_FILE:-${ROOT_DIR}/local/youtube-worker.env}"
CALLER_YOUTUBE_CATALOG_SWEEP_SERVER_ID="${YOUTUBE_CATALOG_SWEEP_SERVER_ID:-}"
CALLER_YOUTUBE_CATALOG_SWEEP_SHARD_COUNT="${YOUTUBE_CATALOG_SWEEP_SHARD_COUNT:-}"
CALLER_YOUTUBE_CATALOG_SWEEP_SHARD_INDEX="${YOUTUBE_CATALOG_SWEEP_SHARD_INDEX:-}"
CALLER_YOUTUBE_CATALOG_SWEEP_MAX_STREAMS="${YOUTUBE_CATALOG_SWEEP_MAX_STREAMS:-}"

if [[ -f "${ENV_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
fi
if [[ -n "${CALLER_YOUTUBE_CATALOG_SWEEP_SERVER_ID}" ]]; then
  export YOUTUBE_CATALOG_SWEEP_SERVER_ID="${CALLER_YOUTUBE_CATALOG_SWEEP_SERVER_ID}"
fi
if [[ -n "${CALLER_YOUTUBE_CATALOG_SWEEP_SHARD_COUNT}" ]]; then
  export YOUTUBE_CATALOG_SWEEP_SHARD_COUNT="${CALLER_YOUTUBE_CATALOG_SWEEP_SHARD_COUNT}"
fi
if [[ -n "${CALLER_YOUTUBE_CATALOG_SWEEP_SHARD_INDEX}" ]]; then
  export YOUTUBE_CATALOG_SWEEP_SHARD_INDEX="${CALLER_YOUTUBE_CATALOG_SWEEP_SHARD_INDEX}"
fi
if [[ -n "${CALLER_YOUTUBE_CATALOG_SWEEP_MAX_STREAMS}" ]]; then
  export YOUTUBE_CATALOG_SWEEP_MAX_STREAMS="${CALLER_YOUTUBE_CATALOG_SWEEP_MAX_STREAMS}"
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

require_cmd curl
require_cmd jq
require_cmd go
require_cmd ffmpeg
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

export BACKEND_API_URL="${BACKEND_API_URL:-${INFERCTL_API_URL:-}}"
require_env BACKEND_API_URL
require_env API_TOKEN

trap 'echo "youtube catalog sweeper: stopping"; exit 0' INT TERM

export YOUTUBE_CATALOG_SWEEP_MAX_STREAMS="${YOUTUBE_CATALOG_SWEEP_MAX_STREAMS:-${CAPTURE_MAX_SESSIONS:-${CAPTURE_CONCURRENCY:-1}}}"
export YOUTUBE_CATALOG_SWEEP_SCAN_PAGE_SIZE="${YOUTUBE_CATALOG_SWEEP_SCAN_PAGE_SIZE:-200}"
export YOUTUBE_CATALOG_SWEEP_MAX_SCAN_PAGES="${YOUTUBE_CATALOG_SWEEP_MAX_SCAN_PAGES:-2}"
export YOUTUBE_CATALOG_SWEEP_DURATION="${YOUTUBE_CATALOG_SWEEP_DURATION:-30s}"
export YOUTUBE_CATALOG_SWEEP_IDLE_SEC="${YOUTUBE_CATALOG_SWEEP_IDLE_SEC:-20}"
export YOUTUBE_CATALOG_SWEEP_POLL_TIMEOUT_SEC="${YOUTUBE_CATALOG_SWEEP_POLL_TIMEOUT_SEC:-20}"
export YOUTUBE_CATALOG_SWEEP_SERVER_ID="${YOUTUBE_CATALOG_SWEEP_SERVER_ID:-${YOUTUBE_SERVER_ID:-$(hostname -s)}-catalog}"
export YOUTUBE_CATALOG_SWEEP_SHARD_COUNT="${YOUTUBE_CATALOG_SWEEP_SHARD_COUNT:-1}"
export YOUTUBE_CATALOG_SWEEP_SHARD_INDEX="${YOUTUBE_CATALOG_SWEEP_SHARD_INDEX:-0}"
export YOUTUBE_CATALOG_SWEEP_SORT_BY="${YOUTUBE_CATALOG_SWEEP_SORT_BY:-latest_captured_at}"
export YOUTUBE_CATALOG_SWEEP_SORT_DIR="${YOUTUBE_CATALOG_SWEEP_SORT_DIR:-asc}"
export YOUTUBE_CATALOG_SWEEP_SKIP_ERROR_THRESHOLD="${YOUTUBE_CATALOG_SWEEP_SKIP_ERROR_THRESHOLD:-12}"
export YOUTUBE_CATALOG_SWEEP_ERROR_RECHECK_SEC="${YOUTUBE_CATALOG_SWEEP_ERROR_RECHECK_SEC:-60}"
export YOUTUBE_CATALOG_SWEEP_ERROR_QUEUE_FILE="${YOUTUBE_CATALOG_SWEEP_ERROR_QUEUE_FILE:-${ROOT_DIR}/local/reports/youtube-catalog-error-queue-${YOUTUBE_CATALOG_SWEEP_SERVER_ID}.txt}"

if [[ "${YOUTUBE_CATALOG_SWEEP_MAX_STREAMS}" -le 0 ]]; then
  echo "error: YOUTUBE_CATALOG_SWEEP_MAX_STREAMS must be > 0" >&2
  exit 1
fi
if [[ "${YOUTUBE_CATALOG_SWEEP_SCAN_PAGE_SIZE}" -le 0 ]]; then
  echo "error: YOUTUBE_CATALOG_SWEEP_SCAN_PAGE_SIZE must be > 0" >&2
  exit 1
fi
if [[ "${YOUTUBE_CATALOG_SWEEP_MAX_SCAN_PAGES}" -le 0 ]]; then
  echo "error: YOUTUBE_CATALOG_SWEEP_MAX_SCAN_PAGES must be > 0" >&2
  exit 1
fi
if [[ "${YOUTUBE_CATALOG_SWEEP_ERROR_RECHECK_SEC}" -le 0 ]]; then
  echo "error: YOUTUBE_CATALOG_SWEEP_ERROR_RECHECK_SEC must be > 0" >&2
  exit 1
fi
if [[ "${YOUTUBE_CATALOG_SWEEP_SHARD_COUNT}" -le 0 ]] || [[ "${YOUTUBE_CATALOG_SWEEP_SHARD_INDEX}" -lt 0 ]] || [[ "${YOUTUBE_CATALOG_SWEEP_SHARD_INDEX}" -ge "${YOUTUBE_CATALOG_SWEEP_SHARD_COUNT}" ]]; then
  echo "error: invalid shard config" >&2
  exit 1
fi

queue_dir="$(dirname "${YOUTUBE_CATALOG_SWEEP_ERROR_QUEUE_FILE}")"
mkdir -p "${queue_dir}"
touch "${YOUTUBE_CATALOG_SWEEP_ERROR_QUEUE_FILE}"

queue_normalize() {
  local tmp
  tmp="$(mktemp "${queue_dir}/ytq.XXXXXX")"
  awk '/^[0-9]+$/ { if (!seen[$0]++) print $0 }' "${YOUTUBE_CATALOG_SWEEP_ERROR_QUEUE_FILE}" > "${tmp}" || true
  mv "${tmp}" "${YOUTUBE_CATALOG_SWEEP_ERROR_QUEUE_FILE}"
}

queue_has_id() {
  local id="$1"
  grep -qx "${id}" "${YOUTUBE_CATALOG_SWEEP_ERROR_QUEUE_FILE}" 2>/dev/null
}

queue_enqueue_id() {
  local id="$1"
  [[ "${id}" =~ ^[0-9]+$ ]] || return 0
  if queue_has_id "${id}"; then
    return 0
  fi
  echo "${id}" >> "${YOUTUBE_CATALOG_SWEEP_ERROR_QUEUE_FILE}"
}

queue_pop_id() {
  local id
  id="$(head -n 1 "${YOUTUBE_CATALOG_SWEEP_ERROR_QUEUE_FILE}" | tr -d '[:space:]')"
  if [[ ! "${id}" =~ ^[0-9]+$ ]]; then
    return 1
  fi
  awk 'NR>1 { print }' "${YOUTUBE_CATALOG_SWEEP_ERROR_QUEUE_FILE}" > "${YOUTUBE_CATALOG_SWEEP_ERROR_QUEUE_FILE}.tmp" || true
  mv "${YOUTUBE_CATALOG_SWEEP_ERROR_QUEUE_FILE}.tmp" "${YOUTUBE_CATALOG_SWEEP_ERROR_QUEUE_FILE}"
  printf '%s\n' "${id}"
}

load_stream_detail() {
  local id="$1"
  curl -fsS --max-time "${YOUTUBE_CATALOG_SWEEP_POLL_TIMEOUT_SEC}" \
    -H "Authorization: Bearer ${API_TOKEN}" \
    "${BACKEND_API_URL%/}/api/v1/dashboard/streams/${id}"
}

latest_frame_status_for_stream() {
  local id="$1"
  local line status
  line="$((cd "${ROOT_DIR}/backend" && go run ./cmd/stoaramactl streams frames --stream-id "${id}" --limit 1 --backend-api-url "${BACKEND_API_URL}" --api-token "${API_TOKEN}" 2>/dev/null) | awk '/^frame_id=/{ print; exit }')"
  if [[ -z "${line}" ]]; then
    printf 'unknown\n'
    return 0
  fi
  status="$(awk -F'status=' '{print $2}' <<<"${line}" | awk '{print $1}')"
  case "${status}" in
    success|error) printf '%s\n' "${status}" ;;
    *) printf 'unknown\n' ;;
  esac
}

should_skip_youtube_candidate() {
  local captures_success="$1"
  local captures_error="$2"
  local runtime_status="$3"
  local runtime_error="$4"
  local lowered_error
  lowered_error="$(echo "${runtime_error}" | tr '[:upper:]' '[:lower:]')"
  runtime_status="$(echo "${runtime_status}" | tr '[:upper:]' '[:lower:]')"

  if [[ "${runtime_status}" == "unsupported" ]]; then
    return 0
  fi
  if [[ "${captures_success}" =~ ^[0-9]+$ ]] && [[ "${captures_success}" -eq 0 ]] && [[ "${captures_error}" =~ ^[0-9]+$ ]] && [[ "${captures_error}" -ge "${YOUTUBE_CATALOG_SWEEP_SKIP_ERROR_THRESHOLD}" ]]; then
    return 0
  fi
  case "${lowered_error}" in
    *"capture disabled after "*|*"consecutive errors"*|*"not available"*|*"video unavailable"*|*"private video"*|*"this live stream recording is not available."*)
      return 0
      ;;
  esac
  return 1
}

select_candidates() {
  local needed="${YOUTUBE_CATALOG_SWEEP_MAX_STREAMS}"
  local limit="${YOUTUBE_CATALOG_SWEEP_SCAN_PAGE_SIZE}"
  local max_pages="${YOUTUBE_CATALOG_SWEEP_MAX_SCAN_PAGES}"
  local page=0
  local offset=0
  local total=-1
  local page_len=0
  local payload item_json
  local items_json_lines
  local -a uncaptured_ids=()
  local -a fallback_ids=()

  while (( page < max_pages )); do
    payload="$(curl -fsS --max-time "${YOUTUBE_CATALOG_SWEEP_POLL_TIMEOUT_SEC}" -H "Authorization: Bearer ${API_TOKEN}" "${BACKEND_API_URL%/}/api/v1/dashboard/streams?recording_state=off&capture_type=youtube_watch&limit=${limit}&offset=${offset}&include_image_urls=false&sort_by=${YOUTUBE_CATALOG_SWEEP_SORT_BY}&sort_dir=${YOUTUBE_CATALOG_SWEEP_SORT_DIR}")" || return 1
    page_len="$(jq -r '.items | length' <<<"${payload}")"
    total="$(jq -r '(.total // -1) | tonumber? // -1' <<<"${payload}")"
    if [[ ! "${page_len}" =~ ^[0-9]+$ ]] || (( page_len == 0 )); then
      break
    fi

    items_json_lines="$(jq -c '.items[]?' <<<"${payload}")"
    if [[ -z "${items_json_lines}" ]]; then
      offset=$((offset + page_len))
      page=$((page + 1))
      if (( total >= 0 && offset >= total )); then
        break
      fi
      continue
    fi

    while IFS= read -r item_json; do
      local id captures_success captures_error runtime_status runtime_error stream_tags
      id="$(jq -r '.stream.id // empty' <<<"${item_json}")"
      [[ "${id}" =~ ^[0-9]+$ ]] || continue

      if (( YOUTUBE_CATALOG_SWEEP_SHARD_COUNT > 1 )) && (( id % YOUTUBE_CATALOG_SWEEP_SHARD_COUNT != YOUTUBE_CATALOG_SWEEP_SHARD_INDEX )); then
        continue
      fi

      stream_tags="$(jq -r '(.stream.tags // []) | join(",")' <<<"${item_json}")"
      if [[ ",${stream_tags}," == *",capture_error,"* ]]; then
        continue
      fi
      if queue_has_id "${id}"; then
        continue
      fi

      captures_success="$(jq -r '.captures_success // 0' <<<"${item_json}")"
      captures_error="$(jq -r '.captures_error // 0' <<<"${item_json}")"
      runtime_status="$(jq -r '.stream.capture_runtime_status // ""' <<<"${item_json}")"
      runtime_error="$(jq -r '.stream.capture_runtime_last_error // ""' <<<"${item_json}")"
      if should_skip_youtube_candidate "${captures_success}" "${captures_error}" "${runtime_status}" "${runtime_error}"; then
        continue
      fi

      if [[ "${captures_success}" =~ ^[0-9]+$ ]] && (( captures_success == 0 )); then
        uncaptured_ids+=("${id}")
        if (( ${#uncaptured_ids[@]} >= needed )); then
          printf '%s\n' "${uncaptured_ids[@]:0:${needed}}"
          return 0
        fi
      elif (( ${#fallback_ids[@]} < needed )); then
        fallback_ids+=("${id}")
      fi
    done <<< "${items_json_lines}"

    offset=$((offset + page_len))
    page=$((page + 1))
    if (( total >= 0 && offset >= total )); then
      break
    fi
  done

  if (( ${#uncaptured_ids[@]} > 0 )); then
    printf '%s\n' "${uncaptured_ids[@]:0:${needed}}"
    return 0
  fi
  if (( ${#fallback_ids[@]} > 0 )); then
    printf '%s\n' "${fallback_ids[@]:0:${needed}}"
  fi
}

enqueue_if_capture_error() {
  local id="$1"
  local payload rt_status rt_error frame_status
  payload="$(load_stream_detail "${id}" 2>/dev/null || true)"
  if [[ -z "${payload}" ]]; then
    return 0
  fi
  rt_status="$(jq -r '.stream.capture_runtime_status // ""' <<<"${payload}")"
  rt_error="$(jq -r '.stream.capture_runtime_last_error // ""' <<<"${payload}")"
  frame_status="$(latest_frame_status_for_stream "${id}")"
  if [[ "${rt_status}" == "error" || -n "${rt_error}" || "${frame_status}" == "error" ]]; then
    queue_enqueue_id "${id}"
  fi
}

recheck_one_from_queue() {
  local id payload capture_type recording_state rt_status rt_error frame_status
  id="$(queue_pop_id || true)"
  [[ "${id}" =~ ^[0-9]+$ ]] || return 0

  payload="$(load_stream_detail "${id}" 2>/dev/null || true)"
  if [[ -z "${payload}" ]]; then
    return 0
  fi
  capture_type="$(jq -r '.stream.capture_type // ""' <<<"${payload}")"
  recording_state="$(jq -r '.stream.recording_state // ""' <<<"${payload}")"
  if [[ "${capture_type}" != "youtube_watch" || "${recording_state}" != "off" ]]; then
    return 0
  fi

  rt_status="$(jq -r '.stream.capture_runtime_status // ""' <<<"${payload}")"
  rt_error="$(jq -r '.stream.capture_runtime_last_error // ""' <<<"${payload}")"
  frame_status="$(latest_frame_status_for_stream "${id}")"
  if [[ "${rt_status}" == "error" || -n "${rt_error}" || "${frame_status}" == "error" ]]; then
    if (cd "${ROOT_DIR}/backend" && go run ./cmd/stoaramactl streams tags-add --id "${id}" --tags capture_error >/dev/null 2>&1); then
      echo "youtube catalog sweeper: stream=${id} tagged capture_error after queue recheck"
    fi
  fi
}

echo "starting local youtube catalog sweeper: server_id=${YOUTUBE_CATALOG_SWEEP_SERVER_ID} max_streams=${YOUTUBE_CATALOG_SWEEP_MAX_STREAMS} scan_page_size=${YOUTUBE_CATALOG_SWEEP_SCAN_PAGE_SIZE} duration=${YOUTUBE_CATALOG_SWEEP_DURATION} shard=${YOUTUBE_CATALOG_SWEEP_SHARD_INDEX}/${YOUTUBE_CATALOG_SWEEP_SHARD_COUNT}"

cd "${ROOT_DIR}"
queue_normalize
last_error_recheck_ts=0
while true; do
  now_ts="$(date +%s)"
  if (( now_ts - last_error_recheck_ts >= YOUTUBE_CATALOG_SWEEP_ERROR_RECHECK_SEC )); then
    recheck_one_from_queue
    last_error_recheck_ts="${now_ts}"
  fi

  ids=()
  ids_text="$(select_candidates || true)"
  while IFS= read -r sid; do
    sid="$(echo "${sid}" | tr -d '[:space:]')"
    [[ "${sid}" =~ ^[0-9]+$ ]] || continue
    ids+=("${sid}")
  done <<< "${ids_text}"

  if (( ${#ids[@]} == 0 )); then
    echo "youtube catalog sweeper: no non-recording youtube streams found; sleeping ${YOUTUBE_CATALOG_SWEEP_IDLE_SEC}s"
    sleep "${YOUTUBE_CATALOG_SWEEP_IDLE_SEC}"
    continue
  fi

  ids_csv="$(IFS=,; echo "${ids[*]}")"
  echo "youtube catalog sweeper: running batch streams=${#ids[@]} ids=${ids_csv}"

  if ! SI_ENV_FILE="${ENV_FILE}" YOUTUBE_SERVER_ID="${YOUTUBE_CATALOG_SWEEP_SERVER_ID}" CAPTURE_RECORDING_HEARTBEAT="${CAPTURE_RECORDING_HEARTBEAT:-0}" CAPTURE_MAX_SESSIONS="${YOUTUBE_CATALOG_SWEEP_MAX_STREAMS}" \
    backend/scripts/start-local-youtube-worker.sh --stream-ids "${ids_csv}" --duration "${YOUTUBE_CATALOG_SWEEP_DURATION}"; then
    echo "youtube catalog sweeper: batch failed; sleeping ${YOUTUBE_CATALOG_SWEEP_IDLE_SEC}s" >&2
    sleep "${YOUTUBE_CATALOG_SWEEP_IDLE_SEC}"
  fi

  for sid in "${ids[@]}"; do
    enqueue_if_capture_error "${sid}"
  done
  queue_normalize
done
