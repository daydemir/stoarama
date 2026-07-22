#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
STAGE="$(mktemp -d)"
trap 'rm -rf "${STAGE}"' EXIT

NAS_VERSION="${NAS_VERSION:-$(git -C "${ROOT_DIR}" rev-parse --short=8 HEAD)}"
if [[ ! "${NAS_VERSION}" =~ ^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?$ || "${NAS_VERSION}" == *..* ]]; then
  echo "error: invalid NAS_VERSION" >&2
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
command -v aws >/dev/null || { echo "error: aws CLI is required" >&2; exit 1; }
command -v jq >/dev/null || { echo "error: jq is required" >&2; exit 1; }

endpoint="https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
prefix="nas-releases"
artifact="stoarama-pull-${NAS_VERSION}.py"
manifest="latest-${NAS_VERSION}.json"

if aws s3api head-object --bucket "${R2_BUCKET}" --key "${prefix}/${manifest}" \
  --endpoint-url "${endpoint}" >/dev/null 2>&1; then
  echo "error: NAS release ${NAS_VERSION} already exists" >&2
  exit 1
fi

source="${ROOT_DIR}/clients/nas-pull/stoarama_pull.py"
if [[ "$(grep -Fc 'CLIENT_VERSION = "development"' "${source}")" -ne 1 ]]; then
  echo "error: expected exactly one development version placeholder" >&2
  exit 1
fi
sed "s/CLIENT_VERSION = \"development\"/CLIENT_VERSION = \"${NAS_VERSION}\"/" "${source}" > "${STAGE}/${artifact}"
python3 -m py_compile "${STAGE}/${artifact}"
sha256="$(python3 -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1], "rb").read()).hexdigest())' "${STAGE}/${artifact}")"
jq -n --arg version "${NAS_VERSION}" --arg artifact "${artifact}" --arg sha256 "${sha256}" \
  '{version:$version,artifact:$artifact,sha256:$sha256}' > "${STAGE}/${manifest}"

aws s3 cp "${STAGE}/${artifact}" "s3://${R2_BUCKET}/${prefix}/${artifact}" \
  --endpoint-url "${endpoint}" --content-type text/x-python --only-show-errors
aws s3 cp "${STAGE}/${manifest}" "s3://${R2_BUCKET}/${prefix}/${manifest}" \
  --endpoint-url "${endpoint}" --content-type application/json --only-show-errors
echo "Staged NAS pull ${NAS_VERSION}; promote it only after the backend download endpoint is live"
