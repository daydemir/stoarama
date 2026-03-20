#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${SI_ENV_FILE:-${ROOT_DIR}/local/youtube-worker.env}"
LABEL_WORKER="${LABEL_WORKER:-io.stoarama.youtube-capture}"
LABEL_WATCHDOG="${LABEL_WATCHDOG:-io.stoarama.youtube-capture-watchdog}"
HEALTH_MAX_STALE_SEC="${HEALTH_MAX_STALE_SEC:-180}"
HEALTH_INTERVAL_SEC="${HEALTH_INTERVAL_SEC:-60}"
DRY_RUN="${DRY_RUN:-0}"

AGENTS_DIR="${HOME}/Library/LaunchAgents"
LOG_DIR="${ROOT_DIR}/local/logs/launchd"
WORKER_PLIST="${AGENTS_DIR}/${LABEL_WORKER}.plist"
WATCHDOG_PLIST="${AGENTS_DIR}/${LABEL_WATCHDOG}.plist"

require_cmd() {
  local bin="$1"
  if ! command -v "${bin}" >/dev/null 2>&1; then
    echo "install error: required command not found: ${bin}" >&2
    exit 1
  fi
}

require_cmd launchctl
require_cmd bash
require_cmd jq

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
if [[ -z "${YT_DLP_COOKIES_FILE:-}" && -z "${YT_DLP_COOKIES_FROM_BROWSER:-}" ]]; then
  echo "install error: set YT_DLP_COOKIES_FILE or YT_DLP_COOKIES_FROM_BROWSER in ${ENV_FILE}" >&2
  exit 1
fi
if [[ -n "${YT_DLP_COOKIES_FILE:-}" && ! -f "${YT_DLP_COOKIES_FILE}" ]]; then
  echo "install error: YT_DLP_COOKIES_FILE does not exist: ${YT_DLP_COOKIES_FILE}" >&2
  exit 1
fi
if [[ "${HEALTH_MAX_STALE_SEC}" -le 0 ]]; then
  echo "install error: HEALTH_MAX_STALE_SEC must be > 0" >&2
  exit 1
fi
if [[ "${HEALTH_INTERVAL_SEC}" -le 0 ]]; then
  echo "install error: HEALTH_INTERVAL_SEC must be > 0" >&2
  exit 1
fi

mkdir -p "${AGENTS_DIR}" "${LOG_DIR}"

cat >"${WORKER_PLIST}" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key>
    <string>${LABEL_WORKER}</string>
    <key>ProgramArguments</key>
    <array>
      <string>/bin/bash</string>
      <string>-lc</string>
      <string>cd "${ROOT_DIR}" &amp;&amp; SI_ENV_FILE="${ENV_FILE}" CAPTURE_RUNNER_MODE=api backend/scripts/start-local-youtube-worker.sh</string>
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
    <string>${LOG_DIR}/youtube-worker.out.log</string>
    <key>StandardErrorPath</key>
    <string>${LOG_DIR}/youtube-worker.err.log</string>
  </dict>
</plist>
EOF

cat >"${WATCHDOG_PLIST}" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key>
    <string>${LABEL_WATCHDOG}</string>
    <key>ProgramArguments</key>
    <array>
      <string>/bin/bash</string>
      <string>-lc</string>
      <string>cd "${ROOT_DIR}" &amp;&amp; SI_ENV_FILE="${ENV_FILE}" HEALTH_LABEL="${LABEL_WORKER}" HEALTH_MAX_STALE_SEC="${HEALTH_MAX_STALE_SEC}" backend/scripts/local-youtube-launchd-healthcheck.sh</string>
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
    <key>StartInterval</key>
    <integer>${HEALTH_INTERVAL_SEC}</integer>
    <key>ThrottleInterval</key>
    <integer>15</integer>
    <key>ProcessType</key>
    <string>Background</string>
    <key>StandardOutPath</key>
    <string>${LOG_DIR}/youtube-healthcheck.out.log</string>
    <key>StandardErrorPath</key>
    <string>${LOG_DIR}/youtube-healthcheck.err.log</string>
  </dict>
</plist>
EOF

domain="gui/$(id -u)"
if [[ "${DRY_RUN}" == "1" ]]; then
  echo "dry run: wrote plist files only; skipped launchctl bootstrap"
else
  launchctl bootout "${domain}" "${WATCHDOG_PLIST}" >/dev/null 2>&1 || true
  launchctl bootout "${domain}" "${WORKER_PLIST}" >/dev/null 2>&1 || true

  launchctl bootstrap "${domain}" "${WORKER_PLIST}"
  launchctl bootstrap "${domain}" "${WATCHDOG_PLIST}"
  launchctl enable "${domain}/${LABEL_WORKER}"
  launchctl enable "${domain}/${LABEL_WATCHDOG}"
  launchctl kickstart -k "${domain}/${LABEL_WORKER}"
  launchctl kickstart -k "${domain}/${LABEL_WATCHDOG}"
fi

echo "installed: ${WORKER_PLIST}"
echo "installed: ${WATCHDOG_PLIST}"
echo "status commands:"
echo "  launchctl print ${domain}/${LABEL_WORKER} | head -n 60"
echo "  launchctl print ${domain}/${LABEL_WATCHDOG} | head -n 60"
echo "log files:"
echo "  ${LOG_DIR}/youtube-worker.out.log"
echo "  ${LOG_DIR}/youtube-worker.err.log"
echo "  ${LOG_DIR}/youtube-healthcheck.out.log"
echo "  ${LOG_DIR}/youtube-healthcheck.err.log"
