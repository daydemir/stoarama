#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LABEL_WORKER="${LABEL_WORKER:-io.stoarama.youtube-capture}"
LABEL_WATCHDOG="${LABEL_WATCHDOG:-io.stoarama.youtube-capture-watchdog}"
AGENTS_DIR="${HOME}/Library/LaunchAgents"
WORKER_PLIST="${AGENTS_DIR}/${LABEL_WORKER}.plist"
WATCHDOG_PLIST="${AGENTS_DIR}/${LABEL_WATCHDOG}.plist"

if ! command -v launchctl >/dev/null 2>&1; then
  echo "uninstall error: launchctl not found" >&2
  exit 1
fi

domain="gui/$(id -u)"

launchctl bootout "${domain}" "${WATCHDOG_PLIST}" >/dev/null 2>&1 || true
launchctl bootout "${domain}" "${WORKER_PLIST}" >/dev/null 2>&1 || true

rm -f "${WATCHDOG_PLIST}" "${WORKER_PLIST}"

echo "removed launch agents:"
echo "  ${WORKER_PLIST}"
echo "  ${WATCHDOG_PLIST}"
echo "logs preserved under:"
echo "  ${ROOT_DIR}/local/logs/launchd"

