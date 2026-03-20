#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${SI_ENV_FILE:-${ROOT_DIR}/local/youtube-relay-source.env}"
LABEL_SOURCE="${LABEL_SOURCE:-io.stoarama.youtube-relay-source}"
DRY_RUN="${DRY_RUN:-0}"
BIN_DIR="${ROOT_DIR}/local/bin"
BIN_PATH="${BIN_DIR}/stoaramactl"

AGENTS_DIR="${HOME}/Library/LaunchAgents"
LOG_DIR="${ROOT_DIR}/local/logs/launchd"
SOURCE_PLIST="${AGENTS_DIR}/${LABEL_SOURCE}.plist"

require_cmd() {
  local bin="$1"
  if ! command -v "${bin}" >/dev/null 2>&1; then
    echo "install error: required command not found: ${bin}" >&2
    exit 1
  fi
}

require_cmd launchctl
require_cmd bash
require_cmd go

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "install error: env file not found: ${ENV_FILE}" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "${ENV_FILE}"
if [[ -z "${API_TOKEN:-}" ]]; then
  echo "install error: API_TOKEN missing in ${ENV_FILE}" >&2
  exit 1
fi
if [[ -z "${BACKEND_API_URL:-${INFERCTL_API_URL:-}}" ]]; then
  echo "install error: BACKEND_API_URL missing in ${ENV_FILE}" >&2
  exit 1
fi
if [[ -z "${YOUTUBE_RELAY_SOURCE_PUBLIC_BASE_URL:-}" ]]; then
  echo "install error: YOUTUBE_RELAY_SOURCE_PUBLIC_BASE_URL missing in ${ENV_FILE}" >&2
  exit 1
fi
if [[ -z "${YOUTUBE_RELAY_SHARED_TOKEN:-}" ]]; then
  echo "install error: YOUTUBE_RELAY_SHARED_TOKEN missing in ${ENV_FILE}" >&2
  exit 1
fi
if [[ -z "${YT_DLP_COOKIES_FILE:-}" && -z "${YT_DLP_COOKIES_FROM_BROWSER:-}" ]]; then
  echo "install error: set YT_DLP_COOKIES_FILE or YT_DLP_COOKIES_FROM_BROWSER in ${ENV_FILE}" >&2
  exit 1
fi
if [[ -n "${YT_DLP_COOKIES_FILE:-}" && ! -f "${YT_DLP_COOKIES_FILE}" ]]; then
  echo "install error: YT_DLP_COOKIES_FILE does not exist: ${YT_DLP_COOKIES_FILE}" >&2
  exit 1
fi

mkdir -p "${AGENTS_DIR}" "${LOG_DIR}"
mkdir -p "${BIN_DIR}"

echo "building relay source binary: ${BIN_PATH}"
(
  cd "${ROOT_DIR}/backend"
  go build -o "${BIN_PATH}" ./cmd/stoaramactl
)

cat >"${SOURCE_PLIST}" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key>
    <string>${LABEL_SOURCE}</string>
    <key>ProgramArguments</key>
    <array>
      <string>/bin/bash</string>
      <string>-lc</string>
      <string>cd "${ROOT_DIR}" &amp;&amp; SI_ENV_FILE="${ENV_FILE}" backend/scripts/start-youtube-relay-source.sh</string>
    </array>
    <key>WorkingDirectory</key>
    <string>${ROOT_DIR}</string>
    <key>EnvironmentVariables</key>
    <dict>
      <key>PATH</key>
      <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
    </dict>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <dict>
      <key>SuccessfulExit</key>
      <false/>
    </dict>
    <key>ThrottleInterval</key>
    <integer>15</integer>
    <key>ProcessType</key>
    <string>Background</string>
    <key>StandardOutPath</key>
    <string>${LOG_DIR}/youtube-relay-source.out.log</string>
    <key>StandardErrorPath</key>
    <string>${LOG_DIR}/youtube-relay-source.err.log</string>
  </dict>
</plist>
PLIST

domain="gui/$(id -u)"
if [[ "${DRY_RUN}" == "1" ]]; then
  echo "dry run: wrote plist only; skipped launchctl bootstrap"
else
  launchctl bootout "${domain}" "${SOURCE_PLIST}" >/dev/null 2>&1 || true
  launchctl bootstrap "${domain}" "${SOURCE_PLIST}"
  launchctl enable "${domain}/${LABEL_SOURCE}"
  launchctl kickstart -k "${domain}/${LABEL_SOURCE}"
fi

echo "installed: ${SOURCE_PLIST}"
echo "status: launchctl print ${domain}/${LABEL_SOURCE} | head -n 60"
echo "logs:"
echo "  ${LOG_DIR}/youtube-relay-source.out.log"
echo "  ${LOG_DIR}/youtube-relay-source.err.log"
