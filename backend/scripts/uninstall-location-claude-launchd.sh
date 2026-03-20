#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LABEL_SWEEPER="${LABEL_SWEEPER:-io.stoarama.location-claude-sweeper}"
AGENTS_DIR="${HOME}/Library/LaunchAgents"
SWEEPER_PLIST="${AGENTS_DIR}/${LABEL_SWEEPER}.plist"

domain="gui/$(id -u)"
launchctl bootout "${domain}" "${SWEEPER_PLIST}" >/dev/null 2>&1 || true
rm -f "${SWEEPER_PLIST}"

echo "removed: ${SWEEPER_PLIST}"
echo "status command:"
echo "  launchctl print ${domain}/${LABEL_SWEEPER} || true"
