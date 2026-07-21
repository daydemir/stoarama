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
command -v aws >/dev/null || { echo "error: aws CLI is required" >&2; exit 1; }
command -v jq >/dev/null || { echo "error: jq is required" >&2; exit 1; }

endpoint="https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
stage="$(mktemp -d)"
trap 'rm -rf "${stage}"' EXIT
candidate="${stage}/candidate.json"
live="${stage}/live.json"

download() {
  aws s3 cp "s3://${R2_BUCKET}/relay-releases/$1" "$2" \
    --endpoint-url "${endpoint}" --only-show-errors
}
require_object() {
  aws s3api head-object --bucket "${R2_BUCKET}" --key "relay-releases/$1" \
    --endpoint-url "${endpoint}" >/dev/null
}

download "latest-${VERSION}.json" "${candidate}"
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
aws s3 cp "${candidate}" "s3://${R2_BUCKET}/relay-releases/latest.json" \
  --endpoint-url "${endpoint}" --content-type application/json --only-show-errors

echo "Relay ${MODE} complete: ${VERSION}"
