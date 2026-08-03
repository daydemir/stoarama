#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEST_DIR="$(mktemp -d)"
trap 'rm -rf "${TEST_DIR}"' EXIT
mkdir -p "${TEST_DIR}/bin" "${TEST_DIR}/state"

cat > "${TEST_DIR}/bin/aws" <<'FAKE_AWS'
#!/usr/bin/env bash
set -euo pipefail
state="${FAKE_AWS_STATE:?}"
mode="${FAKE_AWS_MODE:?}"
if [[ "$1 $2" == "s3api put-object" ]]; then
  shift 2
  body=""; key="" condition=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --body) body="$2"; shift 2 ;;
      --key) key="$2"; shift 2 ;;
      --if-none-match) condition="$2"; shift 2 ;;
      *) shift ;;
    esac
  done
  calls=0
  [[ ! -f "${state}/calls" ]] || calls="$(cat "${state}/calls")"
  calls=$((calls + 1)); printf '%s' "${calls}" > "${state}/calls"
  if [[ "${mode}" == "retry" && "${calls}" -eq 1 ]]; then exit 1; fi
  if [[ "${condition}" == "*" && -f "${state}/objects/${key}" ]]; then exit 1; fi
  mkdir -p "${state}/objects/$(dirname "${key}")"
  cp "${body}" "${state}/objects/${key}"
  exit 0
fi
if [[ "$1 $2" == "s3 cp" ]]; then
  source="$3"; destination="$4"
  key="${source#s3://*/}"
  [[ -f "${state}/objects/${key}" ]] || exit 1
  cp "${state}/objects/${key}" "${destination}"
  exit 0
fi
exit 2
FAKE_AWS
chmod +x "${TEST_DIR}/bin/aws"

export PATH="${TEST_DIR}/bin:${PATH}"
export BUILD_DIR="${TEST_DIR}/build" R2_BUCKET="test" R2_ENDPOINT="https://example.invalid"
export FAKE_AWS_STATE="${TEST_DIR}/state"
mkdir -p "${BUILD_DIR}" "${FAKE_AWS_STATE}/objects/relay-releases"
. "${ROOT_DIR}/scripts/relay-release-immutable.sh"

printf 'first bytes\n' > "${TEST_DIR}/source"
export FAKE_AWS_MODE=create
r2_put "${TEST_DIR}/source" "create.bin" application/octet-stream
cmp -s "${TEST_DIR}/source" "${FAKE_AWS_STATE}/objects/relay-releases/create.bin"

# Keep the fake faithful to S3: only the conditional header makes an existing
# key conflict. An ordinary PutObject remains mutable.
printf 'replacement bytes\n' > "${TEST_DIR}/replacement"
aws s3api put-object --bucket test --key relay-releases/create.bin \
  --body "${TEST_DIR}/replacement" --content-type application/octet-stream >/dev/null
cmp -s "${TEST_DIR}/replacement" "${FAKE_AWS_STATE}/objects/relay-releases/create.bin"

cp "${TEST_DIR}/source" "${FAKE_AWS_STATE}/objects/relay-releases/identical.bin"
export FAKE_AWS_MODE=existing
r2_put "${TEST_DIR}/source" "identical.bin" application/octet-stream

printf 'different bytes\n' > "${FAKE_AWS_STATE}/objects/relay-releases/mismatch.bin"
if r2_put "${TEST_DIR}/source" "mismatch.bin" application/octet-stream; then
  echo "non-identical immutable object was accepted" >&2
  exit 1
fi

rm -f "${FAKE_AWS_STATE}/calls"
export FAKE_AWS_MODE=retry
r2_put "${TEST_DIR}/source" "retry.bin" application/octet-stream
[[ "$(cat "${FAKE_AWS_STATE}/calls")" == "2" ]]
cmp -s "${TEST_DIR}/source" "${FAKE_AWS_STATE}/objects/relay-releases/retry.bin"
