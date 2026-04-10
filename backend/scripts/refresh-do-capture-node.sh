#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="${STOARAMA_REPO_DIR:-/opt/stoarama}"
SERVICE_NAME="${CAPTURE_SERVICE_NAME:-stoarama-capture.service}"
REPO_REF="${1:-${REPO_REF:-main}}"

require_cmd() {
  local bin="$1"
  if ! command -v "${bin}" >/dev/null 2>&1; then
    echo "error: required binary not found: ${bin}" >&2
    exit 1
  fi
}

require_cmd git
require_cmd systemctl

if [[ ! -d "${REPO_DIR}/.git" ]]; then
  echo "error: git repo not found at ${REPO_DIR}" >&2
  exit 1
fi

echo "refreshing capture node repo=${REPO_DIR} ref=${REPO_REF} service=${SERVICE_NAME}"
git -C "${REPO_DIR}" fetch --depth 1 origin "${REPO_REF}"
git -C "${REPO_DIR}" checkout "${REPO_REF}"
git -C "${REPO_DIR}" reset --hard "origin/${REPO_REF}"
systemctl restart "${SERVICE_NAME}"

echo "active sha: $(git -C "${REPO_DIR}" rev-parse HEAD)"
echo "service status:"
systemctl --no-pager --full status "${SERVICE_NAME}" | sed -n '1,12p'
