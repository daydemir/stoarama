#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${SI_ENV_FILE:-${ROOT_DIR}/local/operator.env}"
LABEL_SWEEPER="${LABEL_SWEEPER:-io.stoarama.location-ai-sweeper}"
DRY_RUN="${DRY_RUN:-0}"

AGENTS_DIR="${HOME}/Library/LaunchAgents"
LOG_DIR="${ROOT_DIR}/local/logs/launchd"
SWEEPER_PLIST="${AGENTS_DIR}/${LABEL_SWEEPER}.plist"

require_cmd() {
  local bin="$1"
  if ! command -v "${bin}" >/dev/null 2>&1; then
    echo "install error: required command not found: ${bin}" >&2
    exit 1
  fi
}

require_cmd launchctl
require_cmd bash

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "install error: env file not found: ${ENV_FILE}" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "${ENV_FILE}"
if [[ -z "${OPENAI_API_KEY:-}" ]]; then
  echo "install error: OPENAI_API_KEY missing in ${ENV_FILE}" >&2
  exit 1
fi
if [[ -z "${API_TOKEN:-}" ]]; then
  echo "install error: API_TOKEN missing in ${ENV_FILE}" >&2
  exit 1
fi
if [[ -z "${BACKEND_API_URL:-${INFERCTL_API_URL:-}}" ]]; then
  echo "install error: BACKEND_API_URL missing in ${ENV_FILE}" >&2
  exit 1
fi

mkdir -p "${AGENTS_DIR}" "${LOG_DIR}"

cat >"${SWEEPER_PLIST}" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key>
    <string>${LABEL_SWEEPER}</string>
    <key>ProgramArguments</key>
    <array>
      <string>/bin/bash</string>
      <string>-lc</string>
      <string>cd "${ROOT_DIR}" &amp;&amp; SI_ENV_FILE="${ENV_FILE}" backend/scripts/start-location-ai-sweeper.sh</string>
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
    <string>${LOG_DIR}/location-ai-sweeper.out.log</string>
    <key>StandardErrorPath</key>
    <string>${LOG_DIR}/location-ai-sweeper.err.log</string>
  </dict>
</plist>
EOF

domain="gui/$(id -u)"
if [[ "${DRY_RUN}" == "1" ]]; then
  echo "dry run: wrote plist only; skipped launchctl bootstrap"
else
  launchctl bootout "${domain}" "${SWEEPER_PLIST}" >/dev/null 2>&1 || true
  launchctl bootstrap "${domain}" "${SWEEPER_PLIST}"
  launchctl enable "${domain}/${LABEL_SWEEPER}"
  launchctl kickstart -k "${domain}/${LABEL_SWEEPER}"
fi

echo "installed: ${SWEEPER_PLIST}"
echo "status command:"
echo "  launchctl print ${domain}/${LABEL_SWEEPER} | head -n 80"
echo "log files:"
echo "  ${LOG_DIR}/location-ai-sweeper.out.log"
echo "  ${LOG_DIR}/location-ai-sweeper.err.log"
