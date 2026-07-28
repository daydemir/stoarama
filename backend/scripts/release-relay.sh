#!/usr/bin/env bash
set -euo pipefail

# Cross-compiles the stoarama-relay binary for all supported targets, packages each
# as a tar.gz, pulls the pinned yt-dlp static builds, computes sha256 for every
# artifact, writes latest.json, and uploads everything to the R2 bucket under
# relay-releases/. The API serves these at <api>/relay/install.sh and
# <api>/relay/download/{artifact}.
#
# Required env:
#   R2_ACCOUNT_ID        Cloudflare account id (endpoint https://<id>.r2.cloudflarestorage.com)
#   R2_BUCKET            target bucket
#   AWS_ACCESS_KEY_ID    R2 access key id
#   AWS_SECRET_ACCESS_KEY R2 secret access key
#   YTDLP_VERSION        pinned yt-dlp release tag
#   RELAY_SIGNING_PRIVATE_KEY_FILE file containing a base64 Ed25519 private key
# Optional env:
#   RELAY_VERSION        immutable version stamped into artifacts + latest.json
#                        (default: the current eight-character Git revision)
#   RELAY_SIGNING_ADDITIONAL_PUBLIC_KEY base64 Ed25519 public key trusted beside
#                        the signing key for one overlapping rotation release
#   RELAY_SIGNING_BOOTSTRAP=1 permits the one-time migration of the current
#                        byte-identical live/immutable unsigned manifest.
#
# ffmpeg: statically linkable builds are fetched and republished automatically for
# linux-amd64, linux-arm64 (johnvansickle static) and darwin-amd64 (evermeet.cx).
# darwin-arm64 has no static build we can safely ship, so it is intentionally
# omitted from latest.json; the installer falls back to a system ffmpeg (brew) on
# Apple Silicon. Any required upstream download failure aborts the release.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"   # backend/
BUILD_DIR="$(mktemp -d)"
trap 'rm -rf "${BUILD_DIR}"' EXIT

RELAY_VERSION="${RELAY_VERSION:-$(git -C "${ROOT_DIR}" rev-parse --short=8 HEAD)}"
: "${YTDLP_VERSION:?YTDLP_VERSION must be an explicit release tag}"
: "${RELAY_SIGNING_PRIVATE_KEY_FILE:?RELAY_SIGNING_PRIVATE_KEY_FILE is required}"
[[ -f "${RELAY_SIGNING_PRIVATE_KEY_FILE}" ]] || {
  echo "error: RELAY_SIGNING_PRIVATE_KEY_FILE does not exist" >&2
  exit 1
}
RELAY_SIGNING_PUBLIC_KEY="$(
  go run -C "${ROOT_DIR}" ./cmd/relay-manifest-sign public \
    --private-key-file "${RELAY_SIGNING_PRIVATE_KEY_FILE}"
)"
RELAY_TRUSTED_PUBLIC_KEYS="${RELAY_SIGNING_PUBLIC_KEY}"
if [[ -n "${RELAY_SIGNING_ADDITIONAL_PUBLIC_KEY:-}" ]]; then
  go run -C "${ROOT_DIR}" ./cmd/relay-manifest-sign validate-public \
    --public-key "${RELAY_SIGNING_ADDITIONAL_PUBLIC_KEY}"
  RELAY_TRUSTED_PUBLIC_KEYS+=",${RELAY_SIGNING_ADDITIONAL_PUBLIC_KEY}"
fi

if [[ ! "${RELAY_VERSION}" =~ ^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?$ || "${RELAY_VERSION}" == *..* ]]; then
  echo "error: RELAY_VERSION must start and end with a letter or number and contain no consecutive dots" >&2
  exit 1
fi
if [[ -n "$(git -C "${ROOT_DIR}" status --porcelain)" ]]; then
  echo "error: refusing to publish a dirty checkout" >&2
  exit 1
fi

: "${R2_ACCOUNT_ID:?R2_ACCOUNT_ID is required}"
: "${R2_BUCKET:?R2_BUCKET is required}"
: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID is required}"
: "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY is required}"
R2_ENDPOINT="https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
command -v aws >/dev/null || { echo "error: aws CLI is required" >&2; exit 1; }
command -v jq >/dev/null || { echo "error: jq is required" >&2; exit 1; }

head_error="${BUILD_DIR}/head-object.error"
if aws s3api head-object \
  --bucket "${R2_BUCKET}" \
  --key "relay-releases/latest-${RELAY_VERSION}.json" \
  --endpoint-url "${R2_ENDPOINT}" > /dev/null 2> "${head_error}"; then
  echo "error: relay release ${RELAY_VERSION} already exists; refusing to overwrite immutable artifacts" >&2
  exit 1
elif ! grep -Eq '404|Not Found|NoSuchKey' "${head_error}"; then
  echo "error: could not verify release immutability" >&2
  cat "${head_error}" >&2
  exit 1
fi

TARGETS=("darwin/arm64" "darwin/amd64" "linux/amd64" "linux/arm64")

previous_latest="${BUILD_DIR}/previous-latest.json"
previous_latest_signature="${BUILD_DIR}/previous-latest.json.sig"
aws s3 cp "s3://${R2_BUCKET}/relay-releases/latest.json" "${previous_latest}" \
  --endpoint-url "${R2_ENDPOINT}" --only-show-errors
if ! aws s3 cp "s3://${R2_BUCKET}/relay-releases/latest.json.sig" "${previous_latest_signature}" \
  --endpoint-url "${R2_ENDPOINT}" --only-show-errors; then
  if [[ "${RELAY_SIGNING_BOOTSTRAP:-}" != "1" ]]; then
    echo "error: live relay manifest is unsigned; set RELAY_SIGNING_BOOTSTRAP=1 once after verifying the current live release" >&2
    exit 1
  fi
  bootstrap_version="$(jq -er '.version' "${previous_latest}")"
  if [[ ! "${bootstrap_version}" =~ ^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?$ || "${bootstrap_version}" == *..* ]]; then
    echo "error: unsigned bootstrap manifest has an invalid version" >&2
    exit 1
  fi
  bootstrap_immutable="${BUILD_DIR}/bootstrap-immutable.json"
  aws s3 cp "s3://${R2_BUCKET}/relay-releases/latest-${bootstrap_version}.json" "${bootstrap_immutable}" \
    --endpoint-url "${R2_ENDPOINT}" --only-show-errors
  cmp -s "${previous_latest}" "${bootstrap_immutable}" || {
    echo "error: live and immutable bootstrap manifests differ" >&2
    exit 1
  }
  go run -C "${ROOT_DIR}" ./cmd/relay-manifest-sign sign \
    --private-key-file "${RELAY_SIGNING_PRIVATE_KEY_FILE}" \
    --input "${previous_latest}" \
    --output "${previous_latest_signature}"
  aws s3 cp "${previous_latest_signature}" \
    "s3://${R2_BUCKET}/relay-releases/latest-${bootstrap_version}.json.sig" \
    --endpoint-url "${R2_ENDPOINT}" --content-type application/octet-stream --only-show-errors
  aws s3 cp "${previous_latest_signature}" \
    "s3://${R2_BUCKET}/relay-releases/latest.json.sig" \
    --endpoint-url "${R2_ENDPOINT}" --content-type application/octet-stream --only-show-errors
fi
go run -C "${ROOT_DIR}" ./cmd/relay-manifest-sign verify \
  --public-key "${RELAY_TRUSTED_PUBLIC_KEYS}" \
  --input "${previous_latest}" \
  --signature "${previous_latest_signature}"
PREVIOUS_VERSION="$(jq -er '.version' "${previous_latest}")"
if [[ ! "${PREVIOUS_VERSION}" =~ ^[A-Za-z0-9._-]+$ || "${PREVIOUS_VERSION}" == "${RELAY_VERSION}" ]]; then
  echo "error: live relay version is invalid or already ${RELAY_VERSION}" >&2
  exit 1
fi
jq -e '.relay | type == "object" and length == 4' "${previous_latest}" >/dev/null
PREVIOUS_RELAY_JSON="$(jq -c '.relay' "${previous_latest}")"

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
# build_ffmpeg <target> <stage-dir> <tarball-name>: fetches an upstream static
# ffmpeg build for the target, assembles a tarball containing ffmpeg AND ffprobe at
# its root, uploads it to R2, and prints the latest.json fragment for it on stdout.
# Any unreachable source or missing binary fails the release.
build_ffmpeg() {
  local target="$1" stage="$2" tarball="$3"
  local key="${target/\//-}"
  rm -rf "${stage}"; mkdir -p "${stage}"
  case "${target}" in
    linux/amd64|linux/arm64)
      local arch="${target#*/}"
      local url="https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-${arch}-static.tar.xz"
      local xz="${stage}/src.tar.xz"
      if ! curl -fsSL "${url}" -o "${xz}"; then
        echo "error: ffmpeg download failed for ${target} (${url})" >&2
        return 1
      fi
      local ex="${stage}/ex"; mkdir -p "${ex}"
      tar -C "${ex}" -xJf "${xz}"
      local ffdir="" d
      for d in "${ex}"/*/; do
        if [[ -f "${d}ffmpeg" && -f "${d}ffprobe" ]]; then ffdir="${d}"; break; fi
      done
      if [[ -z "${ffdir}" ]]; then
        echo "error: ffmpeg/ffprobe not found in ${url} for ${target}" >&2
        return 1
      fi
      cp "${ffdir}ffmpeg" "${ffdir}ffprobe" "${stage}/"
      ;;
    darwin/amd64)
      local ff_zip="${stage}/ffmpeg.zip" fp_zip="${stage}/ffprobe.zip"
      if ! curl -fsSL "https://evermeet.cx/ffmpeg/getrelease/ffmpeg/zip" -o "${ff_zip}" \
         || ! curl -fsSL "https://evermeet.cx/ffmpeg/getrelease/ffprobe/zip" -o "${fp_zip}"; then
        echo "error: ffmpeg download failed for ${target} (evermeet.cx)" >&2
        return 1
      fi
      unzip -o -q "${ff_zip}" -d "${stage}"
      unzip -o -q "${fp_zip}" -d "${stage}"
      if [[ ! -f "${stage}/ffmpeg" || ! -f "${stage}/ffprobe" ]]; then
        echo "error: ffmpeg/ffprobe missing after unzip for ${target}" >&2
        return 1
      fi
      ;;
    *)
      return 1
      ;;
  esac
  chmod +x "${stage}/ffmpeg" "${stage}/ffprobe"
  tar -C "${stage}" -czf "${stage}/${tarball}" ffmpeg ffprobe
  local sha; sha="$(sha256_of "${stage}/${tarball}")"
  r2_put "${stage}/${tarball}" "${tarball}" "application/gzip"
  printf '    "%s": {"artifact": "%s", "sha256": "%s"},\n' "${key}" "${tarball}" "${sha}"
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
    go build -C "${ROOT_DIR}" \
    -ldflags "-X main.version=${RELAY_VERSION} -X main.releasePublicKeyBase64=${RELAY_TRUSTED_PUBLIC_KEYS}" \
    -o "${bin}" ./cmd/stoarama-relay
  tarball="stoarama-relay-${RELAY_VERSION}-${key}.tar.gz"
  tar -C "${BUILD_DIR}" -czf "${BUILD_DIR}/${tarball}" stoarama-relay
  rm -f "${bin}"
  relay_sha="$(sha256_of "${BUILD_DIR}/${tarball}")"
  r2_put "${BUILD_DIR}/${tarball}" "${tarball}" "application/gzip"
  RELAY_JSON="${RELAY_JSON}    \"${key}\": {\"artifact\": \"${tarball}\", \"sha256\": \"${relay_sha}\"},\n"

  # pinned yt-dlp for this target
  yt_name="yt-dlp-${RELAY_VERSION}-${key}"
  curl -fsSL "https://github.com/yt-dlp/yt-dlp/releases/download/${YTDLP_VERSION}/$(ytdlp_asset "${t}")" -o "${BUILD_DIR}/${yt_name}"
  yt_sha="$(sha256_of "${BUILD_DIR}/${yt_name}")"
  r2_put "${BUILD_DIR}/${yt_name}" "${yt_name}" "application/octet-stream"
  YTDLP_JSON="${YTDLP_JSON}    \"${key}\": {\"artifact\": \"${yt_name}\", \"sha256\": \"${yt_sha}\"},\n"

  # pinned ffmpeg: fetched from upstream static builds and republished with sha256
  # so the installer can verify it exactly like the relay tarball and yt-dlp.
  # darwin/arm64 is intentionally not bundled (no static build); the installer's
  # system-ffmpeg fallback handles Apple Silicon.
  ff_tarball="ffmpeg-${RELAY_VERSION}-${key}.tar.gz"
  if [[ "${t}" == "darwin/arm64" ]]; then
    echo "NOTE: not bundling ffmpeg for darwin/arm64; installer uses system ffmpeg (brew install ffmpeg) on Apple Silicon" >&2
  else
    ff_line="$(build_ffmpeg "${t}" "${BUILD_DIR}/ffstage-${key}" "${ff_tarball}")"
    # command substitution strips the fragment's trailing newline, so re-append a
    # literal \n (rendered by printf "%b" below) to keep each ffmpeg entry on its
    # own JSON line, exactly like RELAY_JSON/YTDLP_JSON.
    FFMPEG_JSON="${FFMPEG_JSON}${ff_line}\n"
  fi
done

# latest.json (trailing commas trimmed)
latest="${BUILD_DIR}/latest.json"
{
  echo "{"
  echo "  \"version\": \"${RELAY_VERSION}\","
  echo "  \"previous_version\": \"${PREVIOUS_VERSION}\","
  echo "  \"previous_relay\": ${PREVIOUS_RELAY_JSON},"
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
latest_signature="${BUILD_DIR}/latest-${RELAY_VERSION}.json.sig"
go run -C "${ROOT_DIR}" ./cmd/relay-manifest-sign sign \
  --private-key-file "${RELAY_SIGNING_PRIVATE_KEY_FILE}" \
  --input "${latest}" \
  --output "${latest_signature}"
r2_put "${latest_signature}" "latest-${RELAY_VERSION}.json.sig" "application/octet-stream"
r2_put "${latest}" "latest-${RELAY_VERSION}.json" "application/json"

# Stage versioned scripts beside the immutable artifacts. Activation is a separate,
# explicit promotion after candidate-specific Mac and Linux canaries pass.
r2_put "${ROOT_DIR}/scripts/relay-install.sh" "install-${RELAY_VERSION}.sh" "text/x-shellscript"
r2_put "${ROOT_DIR}/scripts/relay-uninstall.sh" "uninstall-${RELAY_VERSION}.sh" "text/x-shellscript"

echo "Staged relay ${RELAY_VERSION}; run promote-relay.sh --mode promote --version ${RELAY_VERSION} when ready"
