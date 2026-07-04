#!/usr/bin/env bash
# If this script was started by a POSIX sh (e.g. `sh uninstall.sh`) instead of bash,
# re-exec it under bash so the bash-only constructs below ([[ ]], set -o pipefail)
# work. This block is POSIX-safe on purpose: $BASH_VERSION is empty in non-bash
# shells and nothing here is a bashism, so it must stay ABOVE `set -o pipefail`
# (which dash rejects). A piped `curl | sh` has no file at "$0" to re-exec, which is
# why the documented command below uses `| bash`; this guard covers the
# `sh uninstall.sh` case.
if [ -z "${BASH_VERSION:-}" ]; then
  exec bash "$0" "$@"
fi

set -euo pipefail

# Stoarama relay uninstaller. Served from the API at <api>/relay/uninstall.sh
# (streamed from R2 relay-releases/uninstall.sh). It stops and removes the relay
# from this computer: the launchd user agent (macOS) or systemd user unit (Linux),
# and the ~/.stoarama directory (binaries, logs, config). It is fully self-contained
# and cleans up even if the relay binary is missing.
#
#   curl -fsSL https://stoarama.com/relay/uninstall.sh | bash

LAUNCHD_LABEL="com.stoarama.relay"
SYSTEMD_UNIT="stoarama-relay.service"

INSTALL_DIR="${HOME}/.stoarama"
BIN_DIR="${INSTALL_DIR}/bin"
RELAY_BIN="${BIN_DIR}/stoarama-relay"

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"   # darwin | linux
case "${OS}" in
  darwin|linux) ;;
  *) echo "error: unsupported OS: ${OS}" >&2; exit 1 ;;
esac

echo "Removing the Stoarama relay from this computer..."

# If the binary is present, let it perform its own service teardown first. It is a
# best-effort step; the direct teardown below still runs so a missing or broken
# binary never leaves the service loaded.
if [[ -x "${RELAY_BIN}" ]]; then
  echo "Running stoarama-relay uninstall..."
  "${RELAY_BIN}" uninstall || echo "warning: stoarama-relay uninstall reported an error; continuing cleanup" >&2
fi

if [[ "${OS}" == "darwin" ]]; then
  DOMAIN="gui/$(id -u)"
  PLIST_PATH="${HOME}/Library/LaunchAgents/${LAUNCHD_LABEL}.plist"
  echo "Stopping launchd user agent ${LAUNCHD_LABEL}..."
  launchctl bootout "${DOMAIN}/${LAUNCHD_LABEL}" 2>/dev/null || true
  if [[ -f "${PLIST_PATH}" ]]; then
    rm -f "${PLIST_PATH}"
    echo "Removed ${PLIST_PATH}"
  fi
else
  UNIT_PATH="${HOME}/.config/systemd/user/${SYSTEMD_UNIT}"
  echo "Stopping systemd user unit ${SYSTEMD_UNIT}..."
  systemctl --user disable --now "${SYSTEMD_UNIT}" 2>/dev/null || true
  if [[ -f "${UNIT_PATH}" ]]; then
    rm -f "${UNIT_PATH}"
    echo "Removed ${UNIT_PATH}"
  fi
  systemctl --user daemon-reload 2>/dev/null || true
fi

if [[ -d "${INSTALL_DIR}" ]]; then
  echo "Removing ${INSTALL_DIR}..."
  rm -rf "${INSTALL_DIR}"
fi

echo ""
echo "Stoarama relay removed"
echo "Also click Remove on this computer's row in the Stoarama computers panel to revoke its access."
