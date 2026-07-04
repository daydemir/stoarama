#!/usr/bin/env bash
set -euo pipefail

# Cross-compiles the stoarama-relay binary for all supported targets, packages each
# as a tar.gz, pulls the pinned yt-dlp static builds, computes sha256 for every
# artifact, writes latest.json, and uploads everything to the R2 bucket under
# relay-releases/. The API serves these at <api>/relay/install.sh and
# <api>/relay/download/{artifact}.
#
# Required env (R2, S3-compatible):
#   R2_ACCOUNT_ID        Cloudflare account id (endpoint https://<id>.r2.cloudflarestorage.com)
#   R2_BUCKET            target bucket
#   AWS_ACCESS_KEY_ID    R2 access key id
#   AWS_SECRET_ACCESS_KEY R2 secret access key
# Optional env:
#   RELAY_VERSION        version stamped into the binary + latest.json (default: git describe)
#   YTDLP_VERSION        pinned yt-dlp release tag (default: latest)
#   FFMPEG_DEPS_DIR      dir holding pinned ffmpeg-{os}-{arch}.tar.gz to upload (each
#                        tarball must contain ffmpeg + ffprobe at its root)

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"   # backend/
BUILD_DIR="$(mktemp -d)"
trap 'rm -rf "${BUILD_DIR}"' EXIT

RELAY_VERSION="${RELAY_VERSION:-$(git -C "${ROOT_DIR}" describe --tags --always --dirty 2>/dev/null || echo dev)}"
YTDLP_VERSION="${YTDLP_VERSION:-latest}"

: "${R2_ACCOUNT_ID:?R2_ACCOUNT_ID is required}"
: "${R2_BUCKET:?R2_BUCKET is required}"
: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID is required}"
: "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY is required}"
R2_ENDPOINT="https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

TARGETS=("darwin/arm64" "darwin/amd64" "linux/amd64" "linux/arm64")

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

r2_put() {
  # r2_put <local-file> <key-name> <content-type>
  aws s3 cp "$1" "s3://${R2_BUCKET}/relay-releases/$2" \
    --endpoint-url "${R2_ENDPOINT}" \
    --content-type "$3" \
    --only-show-errors
}

# yt-dlp pinned asset names by target (stable yt-dlp release asset names).
ytdlp_asset() {
  case "$1" in
    darwin/arm64|darwin/amd64) echo "yt-dlp_macos" ;;
    linux/amd64)               echo "yt-dlp" ;;
    linux/arm64)               echo "yt-dlp_linux_aarch64" ;;
  esac
}
ytdlp_base_url() {
  if [[ "${YTDLP_VERSION}" == "latest" ]]; then
    echo "https://github.com/yt-dlp/yt-dlp/releases/latest/download"
  else
    echo "https://github.com/yt-dlp/yt-dlp/releases/download/${YTDLP_VERSION}"
  fi
}

echo "Building stoarama-relay ${RELAY_VERSION}"
RELAY_JSON=""
YTDLP_JSON=""
FFMPEG_JSON=""
for t in "${TARGETS[@]}"; do
  GOOS="${t%/*}"; GOARCH="${t#*/}"
  key="${GOOS}-${GOARCH}"

  # relay binary + tarball
  bin="${BUILD_DIR}/stoarama-relay"
  GOOS="${GOOS}" GOARCH="${GOARCH}" CGO_ENABLED=0 \
    go build -C "${ROOT_DIR}" -ldflags "-X main.version=${RELAY_VERSION}" \
    -o "${bin}" ./cmd/stoarama-relay
  tarball="stoarama-relay-${key}.tar.gz"
  tar -C "${BUILD_DIR}" -czf "${BUILD_DIR}/${tarball}" stoarama-relay
  rm -f "${bin}"
  relay_sha="$(sha256_of "${BUILD_DIR}/${tarball}")"
  r2_put "${BUILD_DIR}/${tarball}" "${tarball}" "application/gzip"
  RELAY_JSON="${RELAY_JSON}    \"${key}\": {\"artifact\": \"${tarball}\", \"sha256\": \"${relay_sha}\"},\n"

  # pinned yt-dlp for this target
  yt_name="yt-dlp-${key}"
  curl -fsSL "$(ytdlp_base_url)/$(ytdlp_asset "${t}")" -o "${BUILD_DIR}/${yt_name}"
  yt_sha="$(sha256_of "${BUILD_DIR}/${yt_name}")"
  r2_put "${BUILD_DIR}/${yt_name}" "${yt_name}" "application/octet-stream"
  YTDLP_JSON="${YTDLP_JSON}    \"${key}\": {\"artifact\": \"${yt_name}\", \"sha256\": \"${yt_sha}\"},\n"

  # pinned ffmpeg (operator-provided; uploaded + checksummed if present so the
  # installer can verify it exactly like the relay tarball and yt-dlp).
  ff_tarball="ffmpeg-${key}.tar.gz"
  if [[ -n "${FFMPEG_DEPS_DIR:-}" && -f "${FFMPEG_DEPS_DIR}/${ff_tarball}" ]]; then
    ff_sha="$(sha256_of "${FFMPEG_DEPS_DIR}/${ff_tarball}")"
    r2_put "${FFMPEG_DEPS_DIR}/${ff_tarball}" "${ff_tarball}" "application/gzip"
    FFMPEG_JSON="${FFMPEG_JSON}    \"${key}\": {\"artifact\": \"${ff_tarball}\", \"sha256\": \"${ff_sha}\"},\n"
  else
    echo "WARN: ${ff_tarball} not found in FFMPEG_DEPS_DIR; skipping (install will 404 on ffmpeg)" >&2
  fi
done

# latest.json (trailing commas trimmed)
latest="${BUILD_DIR}/latest.json"
{
  echo "{"
  echo "  \"version\": \"${RELAY_VERSION}\","
  echo "  \"relay\": {"
  printf "%b" "${RELAY_JSON}" | sed '$ s/,$//'
  echo "  },"
  echo "  \"ytdlp\": {"
  printf "%b" "${YTDLP_JSON}" | sed '$ s/,$//'
  echo "  },"
  echo "  \"ffmpeg\": {"
  printf "%b" "${FFMPEG_JSON}" | sed '$ s/,$//'
  echo "  }"
  echo "}"
} > "${latest}"
r2_put "${latest}" "latest.json" "application/json"

# installer
r2_put "${ROOT_DIR}/scripts/relay-install.sh" "install.sh" "text/x-shellscript"

echo "Published relay ${RELAY_VERSION} to s3://${R2_BUCKET}/relay-releases/"
