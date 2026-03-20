#!/usr/bin/env bash
set -euo pipefail

bytes="${1:-48}"
if ! [[ "${bytes}" =~ ^[0-9]+$ ]]; then
  echo "error: bytes must be an integer" >&2
  exit 1
fi
if [[ "${bytes}" -lt 16 ]]; then
  echo "error: bytes must be >= 16" >&2
  exit 1
fi

if ! command -v openssl >/dev/null 2>&1; then
  echo "error: openssl is required" >&2
  exit 1
fi

# URL-safe token without padding.
openssl rand -base64 "${bytes}" | tr '+/' '-_' | tr -d '=\n'
printf '\n'
