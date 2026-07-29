#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" != "--mode" || ! "${2:-}" =~ ^(promote|rollback)$ || \
      "${3:-}" != "--version" || ! "${4:-}" =~ ^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?$ || \
      "${4:-}" == *..* || $# -ne 4 ]]; then
  echo "usage: promote-relay.sh --mode promote|rollback --version VERSION" >&2
  exit 2
fi
MODE="$2"
VERSION="$4"

: "${R2_ACCOUNT_ID:?R2_ACCOUNT_ID is required}"
: "${R2_BUCKET:?R2_BUCKET is required}"
: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID is required}"
: "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY is required}"
: "${RELAY_SIGNING_PUBLIC_KEY:?RELAY_SIGNING_PUBLIC_KEY is required}"
command -v aws >/dev/null || { echo "error: aws CLI is required" >&2; exit 1; }
command -v jq >/dev/null || { echo "error: jq is required" >&2; exit 1; }

endpoint="https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
stage="$(mktemp -d)"
trap 'rm -rf "${stage}"' EXIT
candidate="${stage}/candidate.json"
candidate_signature="${stage}/candidate.json.sig"
live="${stage}/live.json"
live_signature="${stage}/live.json.sig"

download() {
  aws s3 cp "s3://${R2_BUCKET}/relay-releases/$1" "$2" \
    --endpoint-url "${endpoint}" --only-show-errors
}
require_object() {
  aws s3api head-object --bucket "${R2_BUCKET}" --key "relay-releases/$1" \
    --endpoint-url "${endpoint}" >/dev/null
}

download "latest-${VERSION}.json" "${candidate}"
download "latest-${VERSION}.json.sig" "${candidate_signature}"
go run -C "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)" \
  ./cmd/relay-manifest-sign verify \
  --public-key "${RELAY_SIGNING_PUBLIC_KEY}" \
  --input "${candidate}" \
  --signature "${candidate_signature}"
jq -e --arg version "${VERSION}" '
  .version == $version and
  ([.relay, .ytdlp] | all(type == "object" and length == 4)) and
  (.ffmpeg | type == "object" and length == 3)
' "${candidate}" >/dev/null

while IFS= read -r artifact; do
  require_object "${artifact}"
done < <(jq -er '.relay[], .ytdlp[], .ffmpeg[] | .artifact' "${candidate}")
require_object "install-${VERSION}.sh"
require_object "uninstall-${VERSION}.sh"

download "latest.json" "${live}"
live_version="$(jq -er '.version' "${live}")"
if download "latest.json.sig" "${live_signature}"; then
  go run -C "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)" \
    ./cmd/relay-manifest-sign verify \
    --public-key "${RELAY_SIGNING_PUBLIC_KEY}" \
    --input "${live}" \
    --signature "${live_signature}"
else
  if [[ "${MODE}" != "promote" || "${RELAY_SIGNING_BOOTSTRAP:-}" != "1" ]]; then
    echo "error: live relay manifest is unsigned; set RELAY_SIGNING_BOOTSTRAP=1 only for the initial signed promotion" >&2
    exit 1
  fi
  if [[ ! "${live_version}" =~ ^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?$ || "${live_version}" == *..* ]]; then
    echo "error: unsigned live relay manifest has an invalid version" >&2
    exit 1
  fi
  unsigned_immutable="${stage}/unsigned-immutable.json"
  download "latest-${live_version}.json" "${unsigned_immutable}"
  cmp -s "${live}" "${unsigned_immutable}" || {
    echo "error: unsigned live and immutable relay manifests differ" >&2
    exit 1
  }
fi
require_object "latest-${live_version}.json"
if [[ "${MODE}" == "promote" ]]; then
  jq -e --slurpfile live "${live}" '
    (.previous_version | type == "string" and length > 0) and
    (.previous_relay | type == "object" and length == 4) and
    .previous_version == $live[0].version and .previous_relay == $live[0].relay
  ' "${candidate}" >/dev/null
  while IFS= read -r artifact; do
    require_object "${artifact}"
  done < <(jq -er '.previous_relay[] | .artifact' "${candidate}")
else
  jq -e --slurpfile candidate "${candidate}" '
    .previous_version == $candidate[0].version and .previous_relay == $candidate[0].relay
  ' "${live}" >/dev/null
fi

aws s3 cp "s3://${R2_BUCKET}/relay-releases/install-${VERSION}.sh" \
  "s3://${R2_BUCKET}/relay-releases/install.sh" --endpoint-url "${endpoint}" \
  --content-type text/x-shellscript --only-show-errors
aws s3 cp "s3://${R2_BUCKET}/relay-releases/uninstall-${VERSION}.sh" \
  "s3://${R2_BUCKET}/relay-releases/uninstall.sh" --endpoint-url "${endpoint}" \
  --content-type text/x-shellscript --only-show-errors
aws s3 cp "${candidate_signature}" "s3://${R2_BUCKET}/relay-releases/latest.json.sig" \
  --endpoint-url "${endpoint}" --content-type application/octet-stream --only-show-errors
aws s3 cp "${candidate}" "s3://${R2_BUCKET}/relay-releases/latest.json" \
  --endpoint-url "${endpoint}" --content-type application/json --only-show-errors

echo "Relay ${MODE} complete: ${VERSION}"
