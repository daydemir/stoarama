#!/usr/bin/env bash
set -euo pipefail

LABEL_SOURCE="${LABEL_SOURCE:-io.stoarama.youtube-relay-source}"
AGENTS_DIR="${HOME}/Library/LaunchAgents"
SOURCE_PLIST="${AGENTS_DIR}/${LABEL_SOURCE}.plist"

domain="gui/$(id -u)"
launchctl bootout "${domain}" "${SOURCE_PLIST}" >/dev/null 2>&1 || true
launchctl disable "${domain}/${LABEL_SOURCE}" >/dev/null 2>&1 || true
rm -f "${SOURCE_PLIST}"

echo "removed: ${SOURCE_PLIST}"
echo "status: launchctl print ${domain}/${LABEL_SOURCE}"
