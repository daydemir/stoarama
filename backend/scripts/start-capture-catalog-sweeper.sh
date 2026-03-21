#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BACKEND_DIR="${ROOT_DIR}/backend"
ENV_FILE="${SI_ENV_FILE:-${ROOT_DIR}/local/capture-catalog-sweeper.env}"

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
require_cmd curl
require_cmd jq
require_cmd ffmpeg

export BACKEND_API_URL="${BACKEND_API_URL:-${INFERCTL_API_URL:-}}"
require_env BACKEND_API_URL
AUTH_TOKEN="${SERVICE_TOKEN:-${API_TOKEN:-}}"
if [[ -z "${AUTH_TOKEN}" ]]; then
  echo "error: missing required env var: SERVICE_TOKEN (or legacy API_TOKEN)" >&2
  exit 1
fi
require_env CAPTURE_SERVER_CAPTURE_SHARED_CAPACITY

trap 'echo "capture catalog sweeper: stopping"; exit 0' INT TERM

default_host_id="$(hostname -s | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9-' '-')"
default_host_id="$(echo "${default_host_id}" | sed -E 's/^-+//; s/-+$//; s/-+/-/g')"
export CAPTURE_CATALOG_SWEEP_SERVER_ID="${CAPTURE_CATALOG_SWEEP_SERVER_ID:-capture-catalog-${default_host_id}}"
export CAPTURE_CATALOG_SWEEP_WORKER_ID="${CAPTURE_CATALOG_SWEEP_WORKER_ID:-capture-catalog-sweeper-${default_host_id}}"
export CAPTURE_CATALOG_SWEEP_EXECUTION_CLASSES="${CAPTURE_CATALOG_SWEEP_EXECUTION_CLASSES:-video_live,image_poll}"
export CAPTURE_CATALOG_SWEEP_BATCH_PER_CLASS="${CAPTURE_CATALOG_SWEEP_BATCH_PER_CLASS:-10}"
export CAPTURE_CATALOG_SWEEP_MAX_STREAMS="${CAPTURE_CATALOG_SWEEP_MAX_STREAMS:-30}"
export CAPTURE_CATALOG_SWEEP_DURATION="${CAPTURE_CATALOG_SWEEP_DURATION:-4m}"
export CAPTURE_CATALOG_SWEEP_IDLE_SEC="${CAPTURE_CATALOG_SWEEP_IDLE_SEC:-20}"
export CAPTURE_CATALOG_SWEEP_POLL_TIMEOUT_SEC="${CAPTURE_CATALOG_SWEEP_POLL_TIMEOUT_SEC:-20}"
export CAPTURE_CATALOG_SWEEP_REFRESH_SEC="${CAPTURE_CATALOG_SWEEP_REFRESH_SEC:-5}"
export CAPTURE_CATALOG_SWEEP_SCAN_PAGE_SIZE="${CAPTURE_CATALOG_SWEEP_SCAN_PAGE_SIZE:-200}"
export CAPTURE_CATALOG_SWEEP_HLS_SKIP_ERROR_THRESHOLD="${CAPTURE_CATALOG_SWEEP_HLS_SKIP_ERROR_THRESHOLD:-12}"
export CAPTURE_CATALOG_SWEEP_IMAGE_SKIP_ERROR_THRESHOLD="${CAPTURE_CATALOG_SWEEP_IMAGE_SKIP_ERROR_THRESHOLD:-6}"
if [[ -z "${CAPTURE_CATALOG_SWEEP_METADATA_JSON:-}" ]]; then
  export CAPTURE_CATALOG_SWEEP_METADATA_JSON='{"role":"capture_catalog_sweeper"}'
fi

if [[ "${CAPTURE_CATALOG_SWEEP_BATCH_PER_CLASS}" -le 0 ]]; then
  echo "error: CAPTURE_CATALOG_SWEEP_BATCH_PER_CLASS must be > 0" >&2
  exit 1
fi
if [[ "${CAPTURE_CATALOG_SWEEP_MAX_STREAMS}" -le 0 ]]; then
  echo "error: CAPTURE_CATALOG_SWEEP_MAX_STREAMS must be > 0" >&2
  exit 1
fi
if [[ "${CAPTURE_CATALOG_SWEEP_IDLE_SEC}" -le 0 ]]; then
  echo "error: CAPTURE_CATALOG_SWEEP_IDLE_SEC must be > 0" >&2
  exit 1
fi
if [[ "${CAPTURE_CATALOG_SWEEP_SCAN_PAGE_SIZE}" -le 0 ]]; then
  echo "error: CAPTURE_CATALOG_SWEEP_SCAN_PAGE_SIZE must be > 0" >&2
  exit 1
fi

declare -a execution_class_list=()
IFS=',' read -r -a raw_execution_classes <<<"${CAPTURE_CATALOG_SWEEP_EXECUTION_CLASSES}"
for raw_execution_class in "${raw_execution_classes[@]}"; do
  execution_class="$(echo "${raw_execution_class}" | tr '[:upper:]' '[:lower:]' | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//')"
  [[ -z "${execution_class}" ]] && continue
  case "${execution_class}" in
    video_live|image_poll) ;;
    *)
      echo "error: unsupported execution class in CAPTURE_CATALOG_SWEEP_EXECUTION_CLASSES: ${execution_class}" >&2
      exit 1
      ;;
  esac
  execution_class_exists=0
  if [[ "${#execution_class_list[@]}" -gt 0 ]]; then
    for existing_execution_class in "${execution_class_list[@]}"; do
      if [[ "${existing_execution_class}" == "${execution_class}" ]]; then
        execution_class_exists=1
        break
      fi
    done
  fi
  if [[ "${execution_class_exists}" -eq 1 ]]; then
    continue
  fi
  execution_class_list+=("${execution_class}")
done
if [[ "${#execution_class_list[@]}" -eq 0 ]]; then
  echo "error: CAPTURE_CATALOG_SWEEP_EXECUTION_CLASSES resolved to empty set" >&2
  exit 1
fi
execution_class_csv="$(IFS=,; echo "${execution_class_list[*]}")"

should_skip_hls_candidate() {
  local captures_success="$1"
  local captures_error="$2"
  local runtime_status="$3"
  local runtime_error="$4"
  local lowered_error=""
  lowered_error="$(echo "${runtime_error}" | tr '[:upper:]' '[:lower:]')"
  runtime_status="$(echo "${runtime_status}" | tr '[:upper:]' '[:lower:]')"

  if [[ "${runtime_status}" == "unsupported" ]]; then
    return 0
  fi
  if [[ "${captures_success}" =~ ^[0-9]+$ ]] && [[ "${captures_success}" -eq 0 ]] && [[ "${captures_error}" =~ ^[0-9]+$ ]] && [[ "${captures_error}" -ge "${CAPTURE_CATALOG_SWEEP_HLS_SKIP_ERROR_THRESHOLD}" ]]; then
    return 0
  fi
  case "${lowered_error}" in
    *"stream map '' matches no streams."*|*"failed to set value '0:v:0' for option 'map'"*|*"error opening output files: invalid argument"*|*"404 not found"*|*"server returned 404"*|*"this live stream recording is not available."*)
      return 0
      ;;
  esac
  return 1
}

should_skip_image_candidate() {
  local captures_success="$1"
  local captures_error="$2"
  local runtime_status="$3"
  local runtime_error="$4"
  local lowered_error=""
  lowered_error="$(echo "${runtime_error}" | tr '[:upper:]' '[:lower:]')"
  runtime_status="$(echo "${runtime_status}" | tr '[:upper:]' '[:lower:]')"

  if [[ "${runtime_status}" == "unsupported" ]]; then
    return 0
  fi
  if [[ "${captures_success}" =~ ^[0-9]+$ ]] && [[ "${captures_success}" -eq 0 ]] && [[ "${captures_error}" =~ ^[0-9]+$ ]] && [[ "${captures_error}" -ge "${CAPTURE_CATALOG_SWEEP_IMAGE_SKIP_ERROR_THRESHOLD}" ]]; then
    return 0
  fi
  case "${lowered_error}" in
    *"image request failed status=403"*|*"image request failed status=404"*|*"content-type is not image"*|*"forbidden"*|*"access denied"*|*"capture disabled after "*)
      return 0
      ;;
  esac
  return 1
}

fetch_ids_for_execution_class_candidates() {
  local execution_class="$1"
  local needed="$2"
  local skip_problematic="$3"
  local page_size="${CAPTURE_CATALOG_SWEEP_SCAN_PAGE_SIZE}"
  local offset=0
  local total=-1
  local page_count=0
  local payload=""
  local -a uncaptured_ids=()
  local -a oldest_ids=()

  while true; do
    local url="${BACKEND_API_URL%/}/api/v1/service/capture/catalog/candidates?execution_class=${execution_class}&limit=${page_size}&offset=${offset}"
    if ! payload="$(curl -fsS --max-time "${CAPTURE_CATALOG_SWEEP_POLL_TIMEOUT_SEC}" \
      -H "Authorization: Bearer ${AUTH_TOKEN}" \
      "${url}")"; then
      return 1
    fi

    if ! page_count="$(jq -r '.items | length' <<<"${payload}")"; then
      return 1
    fi
    if [[ ! "${page_count}" =~ ^[0-9]+$ ]]; then
      return 1
    fi
    if [[ "${page_count}" -eq 0 ]]; then
      break
    fi

    total="$(jq -r '(.total // -1) | tonumber? // -1' <<<"${payload}")"
    if [[ ! "${total}" =~ ^-?[0-9]+$ ]]; then
      total=-1
    fi

    while IFS=$'\t' read -r id capture_type captures_success captures_error runtime_status runtime_error; do
      [[ "${id}" =~ ^[0-9]+$ ]] || continue
      if [[ "${skip_problematic}" -eq 1 ]]; then
        if [[ "${capture_type}" == "hls" || "${capture_type}" == "http_video" ]]; then
          if should_skip_hls_candidate "${captures_success}" "${captures_error}" "${runtime_status}" "${runtime_error}"; then
            continue
          fi
        elif [[ "${capture_type}" == "still_image" ]]; then
          if should_skip_image_candidate "${captures_success}" "${captures_error}" "${runtime_status}" "${runtime_error}"; then
            continue
          fi
        fi
      fi
      if [[ "${captures_success}" =~ ^[0-9]+$ ]] && [[ "${captures_success}" -eq 0 ]]; then
        uncaptured_ids+=("${id}")
        if [[ "${#uncaptured_ids[@]}" -ge "${needed}" ]]; then
          break
        fi
      elif [[ "${#oldest_ids[@]}" -lt "${needed}" ]]; then
        oldest_ids+=("${id}")
      fi
    done < <(jq -r '.items[]? | [.stream_id, .capture_type, (.captures_success // 0), (.captures_error // 0), (.runtime_status // ""), (.runtime_error // "")] | @tsv' <<<"${payload}")

    if [[ "${#uncaptured_ids[@]}" -ge "${needed}" ]]; then
      break
    fi
    offset=$((offset + page_count))
    if [[ "${total}" -ge 0 && "${offset}" -ge "${total}" ]]; then
      break
    fi
  done

  if [[ "${#uncaptured_ids[@]}" -gt 0 ]]; then
    printf '%s\n' "${uncaptured_ids[@]:0:${needed}}"
    return 0
  fi
  if [[ "${skip_problematic}" -eq 1 ]]; then
    # For video_live catalog sweep, do not backfill already-captured streams when every uncaptured
    # candidate is known-dead/unsupported; this avoids wasting capacity on stale endpoints.
    return 0
  fi
  if [[ "${#oldest_ids[@]}" -gt 0 ]]; then
    printf '%s\n' "${oldest_ids[@]:0:${needed}}"
  fi
}

fetch_ids_for_execution_class() {
  local execution_class="$1"
  local needed="${CAPTURE_CATALOG_SWEEP_BATCH_PER_CLASS}"
  local -a selected_ids=()

  add_unique_ids() {
    local id=""
    while IFS= read -r id; do
      id="$(echo "${id}" | tr -d '[:space:]')"
      [[ "${id}" =~ ^[0-9]+$ ]] || continue
      id_exists=0
      if [[ "${#selected_ids[@]}" -gt 0 ]]; then
        for existing_id in "${selected_ids[@]}"; do
          if [[ "${existing_id}" == "${id}" ]]; then
            id_exists=1
            break
          fi
        done
      fi
      if [[ "${id_exists}" -eq 1 ]]; then
        continue
      fi
      selected_ids+=("${id}")
      if [[ "${#selected_ids[@]}" -ge "${needed}" ]]; then
        break
      fi
    done
  }

  case "${execution_class}" in
    image_poll)
      add_unique_ids < <(fetch_ids_for_execution_class_candidates "image_poll" "${needed}" 0 || true)
      ;;
    video_live)
      add_unique_ids < <(fetch_ids_for_execution_class_candidates "video_live" "${needed}" 1 || true)
      ;;
    *)
      echo "error: unsupported execution class in fetch_ids_for_execution_class: ${execution_class}" >&2
      return 1
      ;;
  esac

  if [[ "${#selected_ids[@]}" -gt 0 ]]; then
    printf '%s\n' "${selected_ids[@]:0:${needed}}"
  fi
}

echo "starting capture catalog sweeper: server_id=${CAPTURE_CATALOG_SWEEP_SERVER_ID} worker_id=${CAPTURE_CATALOG_SWEEP_WORKER_ID} execution_classes=${execution_class_csv} max_streams=${CAPTURE_CATALOG_SWEEP_MAX_STREAMS} duration=${CAPTURE_CATALOG_SWEEP_DURATION}"

cd "${BACKEND_DIR}"
while true; do
  declare -a stream_ids=()

  for execution_class in "${execution_class_list[@]}"; do
    while IFS= read -r id; do
      id="$(echo "${id}" | tr -d '[:space:]')"
      [[ "${id}" =~ ^[0-9]+$ ]] || continue
      id_exists=0
      if [[ "${#stream_ids[@]}" -gt 0 ]]; then
        for existing_id in "${stream_ids[@]}"; do
          if [[ "${existing_id}" == "${id}" ]]; then
            id_exists=1
            break
          fi
        done
      fi
      if [[ "${id_exists}" -eq 1 ]]; then
        continue
      fi
      stream_ids+=("${id}")
      if [[ "${#stream_ids[@]}" -ge "${CAPTURE_CATALOG_SWEEP_MAX_STREAMS}" ]]; then
        break 2
      fi
    done < <(fetch_ids_for_execution_class "${execution_class}" || true)
  done

  if [[ "${#stream_ids[@]}" -eq 0 ]]; then
    echo "capture catalog sweeper: no non-recording streams found for execution_classes=${execution_class_csv}; sleeping ${CAPTURE_CATALOG_SWEEP_IDLE_SEC}s"
    sleep "${CAPTURE_CATALOG_SWEEP_IDLE_SEC}"
    continue
  fi

  ids_csv="$(IFS=,; echo "${stream_ids[*]}")"
  echo "capture catalog sweeper: running batch streams=${#stream_ids[@]} ids=${ids_csv}"

  cmd=(
    go run ./cmd/stoaramactl
    capture-server run
    --backend-api-url "${BACKEND_API_URL}"
    --api-token "${AUTH_TOKEN}"
    --server-id "${CAPTURE_CATALOG_SWEEP_SERVER_ID}"
    --worker-id "${CAPTURE_CATALOG_SWEEP_WORKER_ID}"
    --capture-shared-capacity "${CAPTURE_SERVER_CAPTURE_SHARED_CAPACITY}"
    --refresh-sec "${CAPTURE_CATALOG_SWEEP_REFRESH_SEC}"
    --unsupported-threshold "${CAPTURE_UNSUPPORTED_THRESHOLD:-8}"
    --frame-queue-size "${CAPTURE_FRAME_QUEUE_SIZE:-64}"
    --frame-enqueue-timeout-sec "${CAPTURE_FRAME_ENQUEUE_TIMEOUT_SEC:-3}"
    --frame-writer-workers "${CAPTURE_FRAME_WRITERS:-2}"
    --metadata-json "${CAPTURE_CATALOG_SWEEP_METADATA_JSON}"
    --execution-classes "${execution_class_csv}"
    --stream-ids "${ids_csv}"
    --duration "${CAPTURE_CATALOG_SWEEP_DURATION}"
  )
  if ! "${cmd[@]}"; then
    echo "capture catalog sweeper: batch failed; sleeping ${CAPTURE_CATALOG_SWEEP_IDLE_SEC}s" >&2
    sleep "${CAPTURE_CATALOG_SWEEP_IDLE_SEC}"
  fi
done
