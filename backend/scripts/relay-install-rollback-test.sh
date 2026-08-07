#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_ROOT="$(mktemp -d)"
trap 'rm -rf "${TEST_ROOT}"' EXIT
HOME="${TEST_ROOT}/home"
INSTALL_DIR="${HOME}/.stoarama"
BIN_DIR="${INSTALL_DIR}/bin"
ROLLBACK_DIR="${INSTALL_DIR}/install-rollback.test"
mkdir -p "${BIN_DIR}" "${ROLLBACK_DIR}" "${TEST_ROOT}/stub"
printf 'old-binary\000bytes' > "${ROLLBACK_DIR}/stoarama-relay"
printf 'old-config\nexact' > "${ROLLBACK_DIR}/config.json"
printf 'new-binary' > "${BIN_DIR}/stoarama-relay"
printf 'new-config' > "${INSTALL_DIR}/config.json"
printf '#!/bin/sh\necho "$*" >> "$LAUNCHCTL_TEST_LOG"\nexit 1\n' > "${TEST_ROOT}/stub/launchctl"
chmod +x "${TEST_ROOT}/stub/launchctl"
printf '#!/bin/sh\nexit 1\n' > "${TEST_ROOT}/stub/systemctl"
chmod +x "${TEST_ROOT}/stub/systemctl"
PATH="${TEST_ROOT}/stub:${PATH}"
export LAUNCHCTL_TEST_LOG="${TEST_ROOT}/launchctl-calls"

# Exercise the installer function itself, not a copied approximation.
eval "$(sed -n '/^cleanup_install() {$/,/^}$/p' "${SCRIPT_DIR}/relay-install.sh")"
LATEST_JSON="${TEST_ROOT}/latest"; RELAY_ARCHIVE="${TEST_ROOT}/relay"; FFMPEG_ARCHIVE="${TEST_ROOT}/ffmpeg"
touch "${LATEST_JSON}" "${RELAY_ARCHIVE}" "${FFMPEG_ARCHIVE}"
HAD_RELAY=1; HAD_CONFIG=1; HAD_SYSTEMD_UNIT=0; SYSTEMD_UNIT_PATH="${TEST_ROOT}/unit"
SERVICE_TARGET=""; INSTALL_COMMITTED=0; OS=darwin

( false; cleanup_install ) && { echo "rollback unexpectedly succeeded" >&2; exit 1; }
cmp "${BIN_DIR}/stoarama-relay" <(printf 'old-binary\000bytes')
cmp "${INSTALL_DIR}/config.json" <(printf 'old-config\nexact')
[[ ! -e "${ROLLBACK_DIR}" ]] || { echo "successful rollback retained backup directory" >&2; exit 1; }

# A failed binary restore must never restart the prior job with mixed artifacts.
ROLLBACK_DIR="${INSTALL_DIR}/install-rollback.failure"
mkdir -p "${ROLLBACK_DIR}"
printf 'old-config' > "${ROLLBACK_DIR}/config.json"
printf 'new-binary' > "${BIN_DIR}/stoarama-relay"
printf 'new-config' > "${INSTALL_DIR}/config.json"
LATEST_JSON="${TEST_ROOT}/latest2"; RELAY_ARCHIVE="${TEST_ROOT}/relay2"; FFMPEG_ARCHIVE="${TEST_ROOT}/ffmpeg2"
touch "${LATEST_JSON}" "${RELAY_ARCHIVE}" "${FFMPEG_ARCHIVE}"
SERVICE_TARGET="gui/501/com.stoarama.relay"; INSTALL_COMMITTED=0
: > "${LAUNCHCTL_TEST_LOG}"
FAILURE_LOG="${TEST_ROOT}/rollback-failure.log"
( false; cleanup_install ) >"${FAILURE_LOG}" 2>&1 && { echo "failed restore unexpectedly succeeded" >&2; exit 1; }
if grep -q '^kickstart' "${LAUNCHCTL_TEST_LOG}"; then echo "service restarted after failed restore" >&2; exit 1; fi
[[ -d "${ROLLBACK_DIR}" ]] || { echo "incomplete rollback directory was deleted" >&2; exit 1; }
grep -Fq "incomplete rollback artifacts retained at ${ROLLBACK_DIR}" "${FAILURE_LOG}" || { echo "retained rollback path was not reported" >&2; exit 1; }

# Systemd unit removal failure must be accounted and retain recovery material.
ROLLBACK_DIR="${INSTALL_DIR}/install-rollback.systemd-rm"
mkdir -p "${ROLLBACK_DIR}" "${TEST_ROOT}/unit-dir/not-empty"
cp -p "${BIN_DIR}/stoarama-relay" "${ROLLBACK_DIR}/stoarama-relay"
cp -p "${INSTALL_DIR}/config.json" "${ROLLBACK_DIR}/config.json"
SYSTEMD_UNIT_PATH="${TEST_ROOT}/unit-dir"; HAD_SYSTEMD_UNIT=0; SERVICE_TARGET=""; INSTALL_COMMITTED=0; OS=linux
( false; cleanup_install ) >"${TEST_ROOT}/systemd-rm.log" 2>&1 && exit 1
[[ -d "${ROLLBACK_DIR}" ]] || { echo "systemd removal failure deleted rollback directory" >&2; exit 1; }
grep -Fq 'failed to remove uncommitted systemd unit' "${TEST_ROOT}/systemd-rm.log"

# Systemd unit directory creation failure is also rollback-accounted.
ROLLBACK_DIR="${INSTALL_DIR}/install-rollback.systemd-mkdir"
mkdir -p "${ROLLBACK_DIR}"
cp -p "${BIN_DIR}/stoarama-relay" "${ROLLBACK_DIR}/stoarama-relay"
cp -p "${INSTALL_DIR}/config.json" "${ROLLBACK_DIR}/config.json"
printf 'old-unit' > "${ROLLBACK_DIR}/stoarama-relay.service"
printf 'blocking-file' > "${TEST_ROOT}/blocked-parent"
SYSTEMD_UNIT_PATH="${TEST_ROOT}/blocked-parent/unit"; HAD_SYSTEMD_UNIT=1; SERVICE_TARGET=""; INSTALL_COMMITTED=0
( false; cleanup_install ) >"${TEST_ROOT}/systemd-mkdir.log" 2>&1 && exit 1
[[ -d "${ROLLBACK_DIR}" ]] || { echo "systemd mkdir failure deleted rollback directory" >&2; exit 1; }
grep -Fq 'failed to create systemd unit directory' "${TEST_ROOT}/systemd-mkdir.log"
