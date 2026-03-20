#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BACKEND_DIR="${ROOT_DIR}/backend"
ENV_FILE="${SI_ENV_FILE:-${ROOT_DIR}/local/youtube-worker.env}"
CALLER_YOUTUBE_SERVER_ID="${YOUTUBE_SERVER_ID:-}"
CALLER_WORKER_ID="${WORKER_ID:-}"

if [[ -f "${ENV_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
fi

if [[ -n "${CALLER_YOUTUBE_SERVER_ID}" ]]; then
  export YOUTUBE_SERVER_ID="${CALLER_YOUTUBE_SERVER_ID}"
fi
if [[ -n "${CALLER_WORKER_ID}" ]]; then
  export WORKER_ID="${CALLER_WORKER_ID}"
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

default_host_id="$(hostname -s | tr '[:upper:]' '[:lower:]')"
export YOUTUBE_SERVER_ID="${YOUTUBE_SERVER_ID:-${default_host_id}}"
export YOUTUBE_SERVER_ID="$(echo "${YOUTUBE_SERVER_ID}" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9-' '-' | sed -E 's/^-+//; s/-+$//; s/-+/-/g')"
if [[ -z "${YOUTUBE_SERVER_ID}" ]]; then
  echo "error: YOUTUBE_SERVER_ID resolved empty; set YOUTUBE_SERVER_ID explicitly" >&2
  exit 1
fi
if [[ ! "${YOUTUBE_SERVER_ID}" =~ ^[a-z0-9][a-z0-9-]{1,62}$ ]]; then
  echo "error: YOUTUBE_SERVER_ID must match ^[a-z0-9][a-z0-9-]{1,62}$ (got: ${YOUTUBE_SERVER_ID})" >&2
  exit 1
fi
expected_worker_id="local-youtube-worker-${YOUTUBE_SERVER_ID}"
if [[ -n "${WORKER_ID:-}" && "${WORKER_ID}" != "${expected_worker_id}" ]]; then
  echo "error: WORKER_ID must equal ${expected_worker_id} for stable telemetry (got: ${WORKER_ID})" >&2
  exit 1
fi
export WORKER_ID="${expected_worker_id}"
export CAPTURE_TICK_SEC="${CAPTURE_TICK_SEC:-5}"
export CAPTURE_CONCURRENCY="${CAPTURE_CONCURRENCY:-4}"
export CAPTURE_UNSUPPORTED_THRESHOLD="${CAPTURE_UNSUPPORTED_THRESHOLD:-8}"
export CAPTURE_FRAME_QUEUE_SIZE="${CAPTURE_FRAME_QUEUE_SIZE:-256}"
export CAPTURE_FRAME_ENQUEUE_TIMEOUT_SEC="${CAPTURE_FRAME_ENQUEUE_TIMEOUT_SEC:-8}"
export CAPTURE_FRAME_WRITERS="${CAPTURE_FRAME_WRITERS:-6}"
export CAPTURE_RECORDING_HEARTBEAT="${CAPTURE_RECORDING_HEARTBEAT:-0}"
export YOUTUBE_RUNNER_CAPACITY="${YOUTUBE_RUNNER_CAPACITY:-10}"
export YOUTUBE_WORKER_HEARTBEAT_SEC="${YOUTUBE_WORKER_HEARTBEAT_SEC:-15}"
export YT_DLP_FORMAT="${YT_DLP_FORMAT:-bestvideo[vcodec^=avc1]/bestvideo/best}"
export YT_DLP_FORMAT_SORT="${YT_DLP_FORMAT_SORT:-}"
export CAPTURE_FFMPEG_JPEG_Q="${CAPTURE_FFMPEG_JPEG_Q:-4}"
export CAPTURE_FFMPEG_THREADS="${CAPTURE_FFMPEG_THREADS:-1}"
export CAPTURE_FFMPEG_HWACCEL="${CAPTURE_FFMPEG_HWACCEL:-}"
export CAPTURE_FFMPEG_RECONNECT="${CAPTURE_FFMPEG_RECONNECT:-true}"
export CAPTURE_FFMPEG_RECONNECT_DELAY_MAX_SEC="${CAPTURE_FFMPEG_RECONNECT_DELAY_MAX_SEC:-2}"

if [[ -n "${CAPTURE_MAX_SESSIONS:-}" ]]; then
  max_sessions="${CAPTURE_MAX_SESSIONS}"
elif [[ -n "${YOUTUBE_STREAM_IDS:-}" ]]; then
  max_sessions="${CAPTURE_CONCURRENCY}"
else
  # Auto-discovery mode caps active sessions at runner capacity.
  max_sessions="${YOUTUBE_RUNNER_CAPACITY}"
fi
backend_api_url="${BACKEND_API_URL:-${INFERCTL_API_URL:-}}"
if [[ -z "${backend_api_url}" ]]; then
  echo "error: missing BACKEND_API_URL (or INFERCTL_API_URL)" >&2
  exit 1
fi
require_env API_TOKEN

if [[ -n "${CAPTURE_RUNNER_MODE:-}" && "${CAPTURE_RUNNER_MODE}" != "api" ]]; then
  echo "error: CAPTURE_RUNNER_MODE=${CAPTURE_RUNNER_MODE} is not supported; this launcher is API-only" >&2
  exit 1
fi

if [[ -n "${YOUTUBE_STREAM_IDS:-}" && "${ALLOW_UNMANAGED_STREAM_FILTER:-0}" != "1" ]]; then
  echo "error: YOUTUBE_STREAM_IDS is disabled for the default launcher; use assignments or set ALLOW_UNMANAGED_STREAM_FILTER=1 for explicit debug-only runs" >&2
  exit 1
fi

cmd=(
  go run ./cmd/stoaramactl
  youtube-server run
  --backend-api-url "${backend_api_url}"
  --api-token "${API_TOKEN}"
  --server-id "${YOUTUBE_SERVER_ID}"
  --worker-id "${WORKER_ID}"
  --capacity "${YOUTUBE_RUNNER_CAPACITY}"
  --heartbeat-sec "${YOUTUBE_WORKER_HEARTBEAT_SEC}"
  --refresh-sec "${CAPTURE_TICK_SEC}"
  --max-sessions "${max_sessions}"
  --unsupported-threshold "${CAPTURE_UNSUPPORTED_THRESHOLD}"
  --frame-queue-size "${CAPTURE_FRAME_QUEUE_SIZE}"
  --frame-enqueue-timeout-sec "${CAPTURE_FRAME_ENQUEUE_TIMEOUT_SEC}"
  --frame-writer-workers "${CAPTURE_FRAME_WRITERS}"
)
cmd+=(--recording-heartbeat="${CAPTURE_RECORDING_HEARTBEAT}")
cmd+=(--yt-dlp-format "${YT_DLP_FORMAT}")
if [[ -n "${YT_DLP_FORMAT_SORT:-}" ]]; then
  cmd+=(--yt-dlp-format-sort "${YT_DLP_FORMAT_SORT}")
fi
cmd+=(--ffmpeg-jpeg-quality "${CAPTURE_FFMPEG_JPEG_Q}")
cmd+=(--ffmpeg-threads "${CAPTURE_FFMPEG_THREADS}")
reconnect_opt="$(echo "${CAPTURE_FFMPEG_RECONNECT}" | tr '[:upper:]' '[:lower:]')"
case "${reconnect_opt}" in
  1|true|yes|on) reconnect_opt="true" ;;
  0|false|no|off) reconnect_opt="false" ;;
  *)
    echo "error: CAPTURE_FFMPEG_RECONNECT must be true/false (or 1/0)" >&2
    exit 1
    ;;
esac
cmd+=(--ffmpeg-reconnect="${reconnect_opt}")
cmd+=(--ffmpeg-reconnect-delay-max-sec "${CAPTURE_FFMPEG_RECONNECT_DELAY_MAX_SEC}")
if [[ -n "${CAPTURE_FFMPEG_HWACCEL:-}" ]]; then
  cmd+=(--ffmpeg-hwaccel "${CAPTURE_FFMPEG_HWACCEL}")
fi
if [[ -n "${YOUTUBE_STREAM_IDS:-}" ]]; then
  cmd+=(--stream-ids "${YOUTUBE_STREAM_IDS}")
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
echo "starting local youtube capture (server mode): server_id=${YOUTUBE_SERVER_ID} worker_id=${WORKER_ID} capacity=${YOUTUBE_RUNNER_CAPACITY} heartbeat_sec=${YOUTUBE_WORKER_HEARTBEAT_SEC} max_sessions=${max_sessions} recording_heartbeat=${CAPTURE_RECORDING_HEARTBEAT} frame_queue=${CAPTURE_FRAME_QUEUE_SIZE} frame_enqueue_timeout_sec=${CAPTURE_FRAME_ENQUEUE_TIMEOUT_SEC} frame_writers=${CAPTURE_FRAME_WRITERS} yt_format=${YT_DLP_FORMAT} ffmpeg_q=${CAPTURE_FFMPEG_JPEG_Q} ffmpeg_threads=${CAPTURE_FFMPEG_THREADS} ffmpeg_hwaccel=${CAPTURE_FFMPEG_HWACCEL:-none} backend_api=${backend_api_url}"

if [[ -n "${YOUTUBE_STREAM_IDS:-}" ]]; then
  echo "stream filter: ${YOUTUBE_STREAM_IDS}"
fi

cd "${BACKEND_DIR}"
exec "${cmd[@]}" "$@"
