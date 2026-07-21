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
CONCURRENCY="6"
MANIFEST_NAME="latest.json"

PATH="/opt/homebrew/bin:/usr/local/bin:${PATH}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --token)       TOKEN="${2:-}"; shift 2 ;;
    --api-url)     API_URL="${2:-}"; shift 2 ;;
    --name)        NAME="${2:-}"; shift 2 ;;
    --concurrency) CONCURRENCY="${2:-}"; shift 2 ;;
    --manifest)    MANIFEST_NAME="${2:-}"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "${TOKEN}" ]]; then
  echo "error: --token is required" >&2
  exit 1
fi
if ! [[ "${CONCURRENCY}" =~ ^[1-9][0-9]*$ ]] || (( CONCURRENCY > 20 )); then
  echo "error: --concurrency must be between 1 and 20" >&2
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
download "${RELAY_TARBALL}" "/tmp/stoarama-relay.tar.gz"
verify_sha "/tmp/stoarama-relay.tar.gz" "${RELAY_TARBALL}"
tar -xzf "/tmp/stoarama-relay.tar.gz" -C "${BIN_DIR}"
chmod +x "${BIN_DIR}/stoarama-relay"
unquarantine "${BIN_DIR}/stoarama-relay"

if [[ ! -x "${BIN_DIR}/yt-dlp" ]]; then
  echo "Downloading yt-dlp..."
  YTDLP_ARTIFACT="yt-dlp-${RELEASE_VERSION}-${KEY}"
  if [[ -z "$(sha_for_artifact "${YTDLP_ARTIFACT}")" ]]; then
    YTDLP_ARTIFACT="yt-dlp-${KEY}"
  fi
  if [[ -z "$(sha_for_artifact "${YTDLP_ARTIFACT}")" ]]; then
    echo "error: no yt-dlp artifact for ${KEY} in ${MANIFEST_NAME}" >&2
    exit 1
  fi
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
  FFMPEG_TARBALL="ffmpeg-${RELEASE_VERSION}-${KEY}.tar.gz"
  if [[ -z "$(sha_for_artifact "${FFMPEG_TARBALL}")" ]]; then
    FFMPEG_TARBALL="ffmpeg-${KEY}.tar.gz"
  fi
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
ENROLL_ARGS=(enroll --token "${TOKEN}" --api-url "${API_URL}" --concurrency "${CONCURRENCY}")
[[ -n "${NAME}" ]] && ENROLL_ARGS+=(--name "${NAME}")
[[ "${MANIFEST_NAME}" != "latest.json" ]] && ENROLL_ARGS+=(--update-manifest "${MANIFEST_NAME}")
"${BIN_DIR}/stoarama-relay" "${ENROLL_ARGS[@]}"

echo ""
# COOKIELESS install (decision 2026-07-04): the relay records generally PUBLIC streams
# and resolves YouTube cookieless (yt-dlp's android client, no cookies, no JS runtime).
# There is NO cookie-export step and NO macOS Keychain prompt during install. The
# with-cookies path for private/members YouTube (stoarama-relay link-youtube) is
# dormant and gated behind STOARAMA_RELAY_YT_COOKIES=1 plus a bundled JS runtime (Deno)
# that we do not ship; enable it only if the cookieless bypass stops working.
#
# Load/start the background service. install-launchd/install-systemd replace any prior
# instance and kickstart it, so a re-run also restarts an already-loaded service.
if [[ "${OS}" == "darwin" ]]; then
  "${BIN_DIR}/stoarama-relay" install-launchd
else
  "${BIN_DIR}/stoarama-relay" install-systemd
fi

echo ""
echo "Done. This computer will appear in the Stoarama relay computers panel shortly."
