#!/usr/bin/env bash
# If this script was started by a POSIX sh (e.g. `sh install.sh ...`) instead of bash,
# re-exec it under bash so the bash-only constructs below (arrays, [[ ]], set -o
# pipefail) work. This block is POSIX-safe on purpose: $BASH_VERSION is empty in
# non-bash shells and nothing here is a bashism, so it must stay ABOVE `set -o
# pipefail` (which dash rejects). A piped `curl | sh` has no file at "$0" to re-exec,
# which is why the documented command below uses `| bash`; this guard covers the
# `sh install.sh` case.
if [ -z "${BASH_VERSION:-}" ]; then
  exec bash "$0" "$@"
fi

set -euo pipefail

# Stoarama relay installer. Served from the API at <api>/relay/install.sh (streamed
# from R2 relay-releases/install.sh). It detects the OS/arch, downloads the relay
# binary plus the pinned yt-dlp and ffmpeg builds (all from <api>/relay/download/),
# clears the macOS quarantine bit, enrolls with the supplied token, then installs
# and starts the launchd user agent (macOS) or systemd user unit (Linux).
#
#   curl -fsSL https://stoarama.com/relay/install.sh | bash -s -- --token sie_xxxx
#   curl -fsSL https://stoarama.com/relay/download/install-VERSION.sh \
#     | bash -s -- --token sie_xxxx --manifest latest-VERSION.json

API_URL="https://stoarama.com"
TOKEN=""
NAME=""
MANIFEST_NAME="latest.json"

PATH="/opt/homebrew/bin:/usr/local/bin:${PATH}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --token)       TOKEN="${2:-}"; shift 2 ;;
    --api-url)     API_URL="${2:-}"; shift 2 ;;
    --name)        NAME="${2:-}"; shift 2 ;;
    --manifest)    MANIFEST_NAME="${2:-}"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "${TOKEN}" ]]; then
  echo "error: --token is required" >&2
  exit 1
fi
if ! [[ "${MANIFEST_NAME}" =~ ^latest(-[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?)?\.json$ ]] || [[ "${MANIFEST_NAME}" == *..* ]]; then
  echo "error: --manifest must be latest.json or latest-VERSION.json" >&2
  exit 1
fi
API_URL="${API_URL%/}"

INSTALL_DIR="${HOME}/.stoarama"
BIN_DIR="${INSTALL_DIR}/bin"
LOG_DIR="${INSTALL_DIR}/logs"
mkdir -p "${BIN_DIR}" "${LOG_DIR}"

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"   # darwin | linux
ARCH="$(uname -m)"
case "${ARCH}" in
  arm64|aarch64) ARCH="arm64" ;;
  x86_64|amd64)  ARCH="amd64" ;;
  *) echo "error: unsupported architecture: ${ARCH}" >&2; exit 1 ;;
esac
case "${OS}" in
  darwin|linux) ;;
  *) echo "error: unsupported OS: ${OS}" >&2; exit 1 ;;
esac

KEY="${OS}-${ARCH}"

download() {
  # download <artifact-name> <dest-path>
  curl -fsSL "${API_URL}/relay/download/$1" -o "$2"
}

unquarantine() {
  # Clear the macOS quarantine bit so a curl-downloaded binary runs without a
  # Gatekeeper prompt. No-op on Linux.
  if [[ "${OS}" == "darwin" ]]; then
    xattr -d com.apple.quarantine "$1" 2>/dev/null || true
  fi
}

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

# sha_for_artifact <artifact-name>: prints the sha256 recorded for that artifact.
sha_for_artifact() {
  printf '%s\n' "${MANIFEST_ENTRIES}" \
    | grep -F "\"artifact\": \"$1\"" \
    | grep -oE '[0-9a-fA-F]{64}' \
    | head -n1 \
    || true
}

verify_sha() {
  # verify_sha <local-file> <artifact-name>: fail-fast if latest.json has no digest
  # for the artifact or the downloaded bytes do not match it.
  local want got
  want="$(sha_for_artifact "$2")"
  if [[ -z "${want}" ]]; then
    echo "error: no sha256 for $2 in latest.json; refusing to install" >&2
    return 1
  fi
  got="$(sha256_of "$1")"
  if [[ "${got}" != "${want}" ]]; then
    echo "error: sha256 mismatch for $2 (got ${got}, want ${want}); aborting install" >&2
    return 1
  fi
}

install_verified_executable() {
  local artifact="$1" target="$2" label="$3" want tmp
  want="$(sha_for_artifact "${artifact}")"
  if [[ -z "${want}" ]]; then
    echo "error: no ${label} artifact for ${KEY} in ${MANIFEST_NAME}" >&2
    return 1
  fi
  if [[ -f "${target}" && "$(sha256_of "${target}")" == "${want}" ]]; then
    chmod +x "${target}"
    unquarantine "${target}"
    return 0
  fi
  echo "Downloading ${label}..."
  tmp="$(mktemp "${target}.new.XXXXXX")"
  if ! download "${artifact}" "${tmp}" || ! verify_sha "${tmp}" "${artifact}"; then
    rm -f "${tmp}"
    return 1
  fi
  chmod +x "${tmp}"
  unquarantine "${tmp}"
  mv -f "${tmp}" "${target}"
}

# Fetch the release manifest up front so every downloaded artifact it lists (the
# relay tarball and yt-dlp) is checksum-verified before anything is executed.
LATEST_JSON="$(mktemp)"
RELAY_ARCHIVE="$(mktemp)"
FFMPEG_ARCHIVE="$(mktemp)"
ROLLBACK_DIR="$(mktemp -d "${INSTALL_DIR}/install-rollback.XXXXXX")"
HAD_RELAY=0
HAD_CONFIG=0
SERVICE_TARGET=""
SYSTEMD_UNIT_PATH="${HOME}/.config/systemd/user/stoarama-relay.service"
HAD_SYSTEMD_UNIT=0
INSTALL_COMMITTED=0
if [[ -f "${BIN_DIR}/stoarama-relay" ]]; then cp -p "${BIN_DIR}/stoarama-relay" "${ROLLBACK_DIR}/stoarama-relay"; HAD_RELAY=1; fi
if [[ -f "${INSTALL_DIR}/config.json" ]]; then cp -p "${INSTALL_DIR}/config.json" "${ROLLBACK_DIR}/config.json"; HAD_CONFIG=1; fi
if [[ "${OS}" == "darwin" ]]; then
  for domain in "gui/$(id -u)" "user/$(id -u)"; do
    if launchctl print "${domain}/com.stoarama.relay" >/dev/null 2>&1; then SERVICE_TARGET="${domain}/com.stoarama.relay"; break; fi
  done
elif [[ -f "${SYSTEMD_UNIT_PATH}" ]]; then
  cp -p "${SYSTEMD_UNIT_PATH}" "${ROLLBACK_DIR}/stoarama-relay.service"
  HAD_SYSTEMD_UNIT=1
  if systemctl --user is-active --quiet stoarama-relay.service; then SERVICE_TARGET="systemd"; fi
fi
cleanup_install() {
  status=$?
  # A fresh macOS install may have completed launchd activation just before an
  # interrupt. Never remove its binary/config while that candidate remains live.
  if [[ "${INSTALL_COMMITTED}" -ne 1 && "${OS}" == "darwin" && -z "${SERVICE_TARGET}" ]]; then
    for domain in "gui/$(id -u)" "user/$(id -u)"; do
      if launchctl print "${domain}/com.stoarama.relay" >/dev/null 2>&1; then
        INSTALL_COMMITTED=1
        echo "warning: retaining verified relay artifacts because launchd activation is live" >&2
        break
      fi
    done
  fi
  if [[ "${INSTALL_COMMITTED}" -ne 1 && "${OS}" == "linux" && "${HAD_SYSTEMD_UNIT}" -eq 0 ]]; then
    load_state="$(systemctl --user show stoarama-relay.service -p LoadState --value 2>/dev/null || true)"
    if systemctl --user is-active --quiet stoarama-relay.service || [[ "${load_state}" == "loaded" ]]; then
      INSTALL_COMMITTED=1
      echo "warning: retaining relay artifacts because systemd activation is active or loaded" >&2
    fi
  fi
  if [[ "${INSTALL_COMMITTED}" -ne 1 ]]; then
	RESTORE_OK=1
    if [[ "${HAD_RELAY}" -eq 1 ]]; then
	  if ! cp -p "${ROLLBACK_DIR}/stoarama-relay" "${BIN_DIR}/stoarama-relay.restore" || ! mv -f "${BIN_DIR}/stoarama-relay.restore" "${BIN_DIR}/stoarama-relay"; then echo "error: failed to restore prior relay binary" >&2; RESTORE_OK=0; fi
	else rm -f "${BIN_DIR}/stoarama-relay" || { echo "error: failed to remove uncommitted relay binary" >&2; RESTORE_OK=0; }; fi
    if [[ "${HAD_CONFIG}" -eq 1 ]]; then
	  if ! cp -p "${ROLLBACK_DIR}/config.json" "${INSTALL_DIR}/config.json.restore" || ! mv -f "${INSTALL_DIR}/config.json.restore" "${INSTALL_DIR}/config.json"; then echo "error: failed to restore prior relay config" >&2; RESTORE_OK=0; fi
	else rm -f "${INSTALL_DIR}/config.json" || { echo "error: failed to remove uncommitted relay config" >&2; RESTORE_OK=0; }; fi
    if [[ "${OS}" == "linux" ]]; then
      if [[ "${HAD_SYSTEMD_UNIT}" -eq 1 ]]; then
		if ! mkdir -p "$(dirname "${SYSTEMD_UNIT_PATH}")"; then
		  echo "error: failed to create systemd unit directory" >&2
		  RESTORE_OK=0
		elif ! cp -p "${ROLLBACK_DIR}/stoarama-relay.service" "${SYSTEMD_UNIT_PATH}.restore" || ! mv -f "${SYSTEMD_UNIT_PATH}.restore" "${SYSTEMD_UNIT_PATH}"; then
		  echo "error: failed to restore prior systemd unit" >&2
		  RESTORE_OK=0
		fi
      else
		rm -f "${SYSTEMD_UNIT_PATH}" || { echo "error: failed to remove uncommitted systemd unit" >&2; RESTORE_OK=0; }
      fi
	  if [[ "${RESTORE_OK}" -eq 1 ]]; then
		systemctl --user daemon-reload || { echo "error: restored systemd unit could not be reloaded" >&2; RESTORE_OK=0; }
		if [[ "${RESTORE_OK}" -eq 1 && "${SERVICE_TARGET}" == "systemd" ]]; then systemctl --user restart stoarama-relay.service || { echo "error: restored systemd relay could not be restarted" >&2; RESTORE_OK=0; }; fi
	  fi
	elif [[ -n "${SERVICE_TARGET}" && "${RESTORE_OK}" -eq 1 ]]; then
      before="$(grep -oE '"heartbeat_success_count":[0-9]+' "${INSTALL_DIR}/relay-recovery.json" 2>/dev/null | tail -n1 | cut -d: -f2 || true)"
      before="${before:-0}"
      if launchctl kickstart -k "${SERVICE_TARGET}" >/dev/null 2>&1; then
        restored=0
        for _ in $(seq 1 70); do
          now="$(grep -oE '"heartbeat_success_count":[0-9]+' "${INSTALL_DIR}/relay-recovery.json" 2>/dev/null | tail -n1 | cut -d: -f2 || true)"
          now="${now:-0}"
          if (( now >= before + 2 )) && launchctl print "${SERVICE_TARGET}" 2>/dev/null | grep -q 'state = running'; then restored=1; break; fi
          sleep 1
        done
        [[ "${restored}" -eq 1 ]] || { echo "error: restored relay did not verify two heartbeats" >&2; RESTORE_OK=0; }
      else
        echo "error: restored relay could not be restarted" >&2
		RESTORE_OK=0
	  fi
	fi
	if [[ "${RESTORE_OK}" -ne 1 ]]; then
	  status=1
	  if [[ "${OS}" == "darwin" && -n "${SERVICE_TARGET}" ]]; then
		launchctl bootout "${SERVICE_TARGET}" >/dev/null 2>&1 || echo "error: failed to stop relay after incomplete rollback" >&2
		launchctl print "${SERVICE_TARGET}" >/dev/null 2>&1 && echo "error: relay remains loaded after incomplete rollback" >&2
	  elif [[ "${OS}" == "linux" && "${SERVICE_TARGET}" == "systemd" ]]; then
		systemctl --user stop stoarama-relay.service >/dev/null 2>&1 || echo "error: failed to stop relay after incomplete rollback" >&2
		systemctl --user is-active --quiet stoarama-relay.service && echo "error: relay remains active after incomplete rollback" >&2
	  fi
	fi
  fi
  rm -f "${LATEST_JSON}" "${RELAY_ARCHIVE}" "${FFMPEG_ARCHIVE}"
  if [[ "${INSTALL_COMMITTED}" -eq 1 || "${RESTORE_OK:-1}" -eq 1 ]]; then
	rm -rf "${ROLLBACK_DIR}"
  else
	echo "error: incomplete rollback artifacts retained at ${ROLLBACK_DIR}" >&2
  fi
  exit "${status}"
}
trap cleanup_install EXIT
echo "Fetching release manifest ${MANIFEST_NAME}..."
download "${MANIFEST_NAME}" "${LATEST_JSON}"
MANIFEST="$(tr '\n' ' ' < "${LATEST_JSON}" | sed -E 's/[[:space:]]+/ /g')"
RELEASE_VERSION="$(
  printf '%s\n' "${MANIFEST}" \
    | grep -oE '"version"[[:space:]]*:[[:space:]]*"[A-Za-z0-9._-]+"' \
    | head -n1 \
    | sed -E 's/.*"([A-Za-z0-9._-]+)"$/\1/'
)"
if [[ -z "${RELEASE_VERSION}" ]]; then
  echo "error: invalid release version in ${MANIFEST_NAME}" >&2
  exit 1
fi
if [[ "${MANIFEST_NAME}" != "latest.json" && \
      "${MANIFEST_NAME}" != "latest-${RELEASE_VERSION}.json" ]]; then
  echo "error: ${MANIFEST_NAME} contains release ${RELEASE_VERSION}" >&2
  exit 1
fi
MANIFEST_ENTRIES="$(
  printf '%s\n' "${MANIFEST}" \
    | grep -oE '\{[[:space:]]*"artifact"[[:space:]]*:[[:space:]]*"[^"]+"[[:space:]]*,[[:space:]]*"sha256"[[:space:]]*:[[:space:]]*"[0-9a-fA-F]{64}"[[:space:]]*\}' \
    | sed -E 's/[[:space:]]+/ /g; s/^\{ /\{/'
)"

echo "Downloading stoarama-relay (${OS}/${ARCH})..."
RELAY_TARBALL="stoarama-relay-${RELEASE_VERSION}-${KEY}.tar.gz"
if [[ -z "$(sha_for_artifact "${RELAY_TARBALL}")" ]]; then
  echo "error: no relay artifact for ${KEY} in ${MANIFEST_NAME}" >&2
  exit 1
fi
download "${RELAY_TARBALL}" "${RELAY_ARCHIVE}"
verify_sha "${RELAY_ARCHIVE}" "${RELAY_TARBALL}"

YTDLP_ARTIFACT="yt-dlp-${RELEASE_VERSION}-${KEY}"
if [[ -z "$(sha_for_artifact "${YTDLP_ARTIFACT}")" ]]; then
  YTDLP_ARTIFACT="yt-dlp-${KEY}"
fi
install_verified_executable "${YTDLP_ARTIFACT}" "${BIN_DIR}/yt-dlp" "yt-dlp"

DENO_ARTIFACT="deno-${RELEASE_VERSION}-${KEY}"
if [[ -n "$(sha_for_artifact "${DENO_ARTIFACT}")" ]]; then
  install_verified_executable "${DENO_ARTIFACT}" "${BIN_DIR}/deno" "Deno JavaScript runtime"
else
  echo "No Deno artifact in this legacy manifest; YouTube will use the no-JS fallback."
fi

# Replace the relay only after every required dependency has been verified. This
# leaves an existing installation untouched if dependency preparation fails.
tar -xzf "${RELAY_ARCHIVE}" -C "${BIN_DIR}"
chmod +x "${BIN_DIR}/stoarama-relay"
unquarantine "${BIN_DIR}/stoarama-relay"

# ffmpeg is optional to bundle: some targets (notably darwin/arm64) have no
# statically linkable build we can safely ship. Prefer a bundled+verified ffmpeg
# when latest.json advertises one for this os/arch (has a sha256); otherwise fall
# back to a system ffmpeg/ffprobe already on PATH. Never proceed without a working
# ffmpeg.
if [[ ! -x "${BIN_DIR}/ffmpeg" ]]; then
  FFMPEG_TARBALL="ffmpeg-${RELEASE_VERSION}-${KEY}.tar.gz"
  if [[ -z "$(sha_for_artifact "${FFMPEG_TARBALL}")" ]]; then
    FFMPEG_TARBALL="ffmpeg-${KEY}.tar.gz"
  fi
  if [[ -n "$(sha_for_artifact "${FFMPEG_TARBALL}")" ]]; then
    echo "Downloading ffmpeg..."
    download "${FFMPEG_TARBALL}" "${FFMPEG_ARCHIVE}"
    verify_sha "${FFMPEG_ARCHIVE}" "${FFMPEG_TARBALL}"
    tar -xzf "${FFMPEG_ARCHIVE}" -C "${BIN_DIR}"
    chmod +x "${BIN_DIR}/ffmpeg" "${BIN_DIR}/ffprobe" 2>/dev/null || true
    unquarantine "${BIN_DIR}/ffmpeg"
    unquarantine "${BIN_DIR}/ffprobe"
  elif command -v ffmpeg >/dev/null 2>&1 && command -v ffprobe >/dev/null 2>&1; then
    echo "No bundled ffmpeg for ${OS}/${ARCH}; using system ffmpeg at $(command -v ffmpeg)."
    ln -sf "$(command -v ffmpeg)" "${BIN_DIR}/ffmpeg"
    ln -sf "$(command -v ffprobe)" "${BIN_DIR}/ffprobe"
  else
    FFMPEG_INSTALLED=0
    if [[ "${OS}" == "darwin" ]] && command -v brew >/dev/null 2>&1; then
      echo "No bundled ffmpeg for ${OS}/${ARCH} and none found on PATH. Installing via Homebrew..."
      brew install ffmpeg || true
      if command -v ffmpeg >/dev/null 2>&1 && command -v ffprobe >/dev/null 2>&1; then
        echo "Using Homebrew ffmpeg at $(command -v ffmpeg)."
        ln -sf "$(command -v ffmpeg)" "${BIN_DIR}/ffmpeg"
        ln -sf "$(command -v ffprobe)" "${BIN_DIR}/ffprobe"
        FFMPEG_INSTALLED=1
      fi
    fi
    if [[ "${FFMPEG_INSTALLED}" -eq 0 ]]; then
      echo "error: ffmpeg not found. Install Homebrew (https://brew.sh) and run 'brew install ffmpeg', then re-run this installer." >&2
      exit 1
    fi
  fi
fi

echo "Enrolling this computer with Stoarama..."
ENROLL_ARGS=(enroll --token "${TOKEN}" --api-url "${API_URL}")
[[ -n "${NAME}" ]] && ENROLL_ARGS+=(--name "${NAME}")
[[ "${MANIFEST_NAME}" != "latest.json" ]] && ENROLL_ARGS+=(--update-manifest "${MANIFEST_NAME}")
"${BIN_DIR}/stoarama-relay" "${ENROLL_ARGS[@]}"

echo ""
# COOKIELESS install (decision 2026-07-04): the relay records generally PUBLIC streams
# and resolves YouTube cookieless. A pinned Deno runtime is bundled because some
# public live streams now require yt-dlp's JavaScript challenge solver.
# There is NO cookie-export step and NO macOS Keychain prompt during install. The
# with-cookies path for private/members YouTube (stoarama-relay link-youtube) is
# dormant and gated behind STOARAMA_RELAY_YT_COOKIES=1.
#
# Load/start the background service. install-launchd/install-systemd replace any prior
# instance and kickstart it, so a re-run also restarts an already-loaded service.
if [[ "${OS}" == "darwin" ]]; then
  "${BIN_DIR}/stoarama-relay" install-launchd && INSTALL_COMMITTED=1
else
  "${BIN_DIR}/stoarama-relay" install-systemd && INSTALL_COMMITTED=1
fi

echo ""
echo "Done. This computer will appear in the Stoarama relay computers panel shortly."
