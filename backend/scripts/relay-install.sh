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

API_URL="https://stoarama.com"
TOKEN=""
NAME=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --token)   TOKEN="${2:-}"; shift 2 ;;
    --api-url) API_URL="${2:-}"; shift 2 ;;
    --name)    NAME="${2:-}"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "${TOKEN}" ]]; then
  echo "error: --token is required" >&2
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

# sha_for_artifact <artifact-name>: prints the sha256 recorded for that artifact in
# latest.json, or nothing if absent. release-relay.sh emits each artifact on its own
# JSON line, so the only 64-hex token on the matching line is that artifact's digest.
sha_for_artifact() {
  grep -F "\"artifact\": \"$1\"" "${LATEST_JSON}" | grep -oE '[0-9a-fA-F]{64}' | head -n1
}

verify_sha() {
  # verify_sha <local-file> <artifact-name>: fail-fast if latest.json has no digest
  # for the artifact or the downloaded bytes do not match it.
  local want got
  want="$(sha_for_artifact "$2")"
  if [[ -z "${want}" ]]; then
    echo "error: no sha256 for $2 in latest.json; refusing to install" >&2
    exit 1
  fi
  got="$(sha256_of "$1")"
  if [[ "${got}" != "${want}" ]]; then
    echo "error: sha256 mismatch for $2 (got ${got}, want ${want}); aborting install" >&2
    exit 1
  fi
}

# Fetch the release manifest up front so every downloaded artifact it lists (the
# relay tarball and yt-dlp) is checksum-verified before anything is executed.
LATEST_JSON="$(mktemp)"
trap 'rm -f "${LATEST_JSON}"' EXIT
echo "Fetching release manifest..."
download "latest.json" "${LATEST_JSON}"

echo "Downloading stoarama-relay (${OS}/${ARCH})..."
RELAY_TARBALL="stoarama-relay-${KEY}.tar.gz"
download "${RELAY_TARBALL}" "/tmp/stoarama-relay.tar.gz"
verify_sha "/tmp/stoarama-relay.tar.gz" "${RELAY_TARBALL}"
tar -xzf "/tmp/stoarama-relay.tar.gz" -C "${BIN_DIR}"
chmod +x "${BIN_DIR}/stoarama-relay"
unquarantine "${BIN_DIR}/stoarama-relay"

if [[ ! -x "${BIN_DIR}/yt-dlp" ]]; then
  echo "Downloading yt-dlp..."
  YTDLP_ARTIFACT="yt-dlp-${KEY}"
  download "${YTDLP_ARTIFACT}" "${BIN_DIR}/yt-dlp"
  verify_sha "${BIN_DIR}/yt-dlp" "${YTDLP_ARTIFACT}"
  chmod +x "${BIN_DIR}/yt-dlp"
  unquarantine "${BIN_DIR}/yt-dlp"
fi

# ffmpeg is optional to bundle: some targets (notably darwin/arm64) have no
# statically linkable build we can safely ship. Prefer a bundled+verified ffmpeg
# when latest.json advertises one for this os/arch (has a sha256); otherwise fall
# back to a system ffmpeg/ffprobe already on PATH. Never proceed without a working
# ffmpeg.
if [[ ! -x "${BIN_DIR}/ffmpeg" ]]; then
  FFMPEG_TARBALL="ffmpeg-${KEY}.tar.gz"
  if [[ -n "$(sha_for_artifact "${FFMPEG_TARBALL}")" ]]; then
    echo "Downloading ffmpeg..."
    download "${FFMPEG_TARBALL}" "/tmp/stoarama-ffmpeg.tar.gz"
    verify_sha "/tmp/stoarama-ffmpeg.tar.gz" "${FFMPEG_TARBALL}"
    tar -xzf "/tmp/stoarama-ffmpeg.tar.gz" -C "${BIN_DIR}"
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
"${BIN_DIR}/stoarama-relay" "${ENROLL_ARGS[@]}"

if [[ "${OS}" == "darwin" ]]; then
  "${BIN_DIR}/stoarama-relay" install-launchd
else
  "${BIN_DIR}/stoarama-relay" install-systemd
fi

echo ""
# Interactive YouTube cookie export. This runs in the user's GUI Terminal session,
# which is the ONLY place the macOS "Always Allow" Keychain prompt can appear and be
# clicked; the background launchd/systemd agent can never decrypt Chrome cookies on
# its own. Best-effort and non-fatal: link-youtube prints an honest note and exits
# non-zero if Chrome is absent, the user is not logged into YouTube, or the prompt is
# declined. Public streams record without any of this, so we never abort the install.
# link-youtube's own 120s timeout bounds the wait on the Keychain prompt.
echo "Setting up YouTube access (optional, for private/members streams)..."
"${BIN_DIR}/stoarama-relay" link-youtube || true

echo ""
echo "Done. This computer will appear in the Stoarama relay computers panel shortly."
