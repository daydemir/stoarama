#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LABEL_SWEEPER="${LABEL_SWEEPER:-io.stoarama.youtube-catalog-sweeper}"
AGENTS_DIR="${HOME}/Library/LaunchAgents"
SWEEPER_PLIST="${AGENTS_DIR}/${LABEL_SWEEPER}.plist"

if ! command -v launchctl >/dev/null 2>&1; then
  echo "uninstall error: launchctl not found" >&2
  exit 1
fi

domain="gui/$(id -u)"
launchctl bootout "${domain}" "${SWEEPER_PLIST}" >/dev/null 2>&1 || true
rm -f "${SWEEPER_PLIST}"

echo "removed launch agent:"
echo "  ${SWEEPER_PLIST}"
echo "logs preserved under:"
echo "  ${ROOT_DIR}/local/logs/launchd"
