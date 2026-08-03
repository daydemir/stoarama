#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: relay-release-provenance.sh version SOURCE_REVISION VERSION | binary PATH SOURCE_REVISION VERSION" >&2
  exit 2
}

[[ $# -ge 1 ]] || usage
mode="$1"
shift

validate_version() {
  local revision="$1" version="$2" short
  [[ "${revision}" =~ ^[0-9a-f]{40}$ ]] || {
    echo "error: relay source revision must be a full lowercase Git SHA" >&2
    return 1
  }
  short="${revision:0:8}"
  [[ "${version}" == "${short}" || "${version}" =~ ^${short}[-._][A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?$ ]] || {
    echo "error: relay version ${version} does not identify source revision ${revision}" >&2
    return 1
  }
}

case "${mode}" in
  version)
    [[ $# -eq 2 ]] || usage
    validate_version "$1" "$2"
    ;;
  binary)
    [[ $# -eq 3 ]] || usage
    binary="$1" revision="$2" version="$3"
    [[ -f "${binary}" ]] || { echo "error: relay binary does not exist: ${binary}" >&2; exit 1; }
    validate_version "${revision}" "${version}"
    metadata="$(go version -m "${binary}")"
    grep -Fq -- "-X main.version=${version}" <<<"${metadata}" || {
      echo "error: relay binary version provenance does not match ${version}" >&2
      exit 1
    }
    grep -Fq -- "-X main.sourceRevision=${revision}" <<<"${metadata}" || {
      echo "error: relay binary source provenance does not match ${revision}" >&2
      exit 1
    }
    ;;
  *) usage ;;
esac
