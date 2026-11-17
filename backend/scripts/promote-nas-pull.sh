#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" != "--version" || ! "${2:-}" =~ ^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?$ || \
      "${2:-}" == *..* || "${3:-}" != "--expected-live-version" || \
      ! "${4:-}" =~ ^(none|[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?)$ || $# -ne 4 ]]; then
  echo "usage: promote-nas-pull.sh --version VERSION --expected-live-version VERSION|none" >&2
  exit 2
fi
version="$2"
expected_live="$4"

: "${R2_ACCOUNT_ID:?R2_ACCOUNT_ID is required}"
: "${R2_BUCKET:?R2_BUCKET is required}"
: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID is required}"
: "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY is required}"
command -v aws >/dev/null || { echo "error: aws CLI is required" >&2; exit 1; }
command -v jq >/dev/null || { echo "error: jq is required" >&2; exit 1; }

endpoint="https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
prefix="nas-releases"
stage="$(mktemp -d)"
trap 'rm -rf "${stage}"' EXIT
candidate="${stage}/candidate.json"
live="${stage}/live.json"

aws s3 cp "s3://${R2_BUCKET}/${prefix}/latest-${version}.json" "${candidate}" \
  --endpoint-url "${endpoint}" --only-show-errors
jq -e --arg version "${version}" '
  .version == $version and
  (.artifact | type == "string" and test("^[A-Za-z0-9._-]+$")) and
  (.sha256 | type == "string" and test("^[a-f0-9]{64}$"))
' "${candidate}" >/dev/null
artifact="$(jq -er .artifact "${candidate}")"
aws s3api head-object --bucket "${R2_BUCKET}" --key "${prefix}/${artifact}" \
  --endpoint-url "${endpoint}" >/dev/null

if aws s3 cp "s3://${R2_BUCKET}/${prefix}/latest.json" "${live}" \
  --endpoint-url "${endpoint}" --only-show-errors 2>/dev/null; then
  live_version="$(jq -er .version "${live}")"
else
  live_version="none"
fi
if [[ "${live_version}" != "${expected_live}" ]]; then
  echo "error: live NAS version is ${live_version}, expected ${expected_live}; refusing promotion" >&2
  exit 1
fi

aws s3 cp "${candidate}" "s3://${R2_BUCKET}/${prefix}/latest.json" \
  --endpoint-url "${endpoint}" --content-type application/json --only-show-errors
echo "NAS pull promotion complete: ${live_version} -> ${version}"
