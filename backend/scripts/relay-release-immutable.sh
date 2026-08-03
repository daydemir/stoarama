#!/usr/bin/env bash

# r2_put <local-file> <key-name> <content-type>
#
# Immutable release names are a compare-and-create contract, not a convention.
# The first byte sequence wins across hosts. Ambiguous conditional conflicts
# retry briefly because a competing PutObject may not yet be readable; a
# readable non-identical object always fails closed.
r2_put() {
  local source="$1" name="$2" content_type="$3"
  local key="relay-releases/${name}"
  local put_error="${BUILD_DIR}/put-${name}.error"
  local existing="${BUILD_DIR}/existing-${name}"
  local attempt
  for attempt in 1 2 3; do
    if aws s3api put-object \
      --bucket "${R2_BUCKET}" \
      --key "${key}" \
      --body "${source}" \
      --content-type "${content_type}" \
      --if-none-match '*' \
      --endpoint-url "${R2_ENDPOINT}" > /dev/null 2> "${put_error}"; then
      return 0
    fi

    if aws s3 cp "s3://${R2_BUCKET}/${key}" "${existing}" \
        --endpoint-url "${R2_ENDPOINT}" --only-show-errors; then
      if cmp -s "${source}" "${existing}"; then
        echo "Immutable relay artifact already staged byte-identically: ${name}" >&2
        return 0
      fi
      cat "${put_error}" >&2
      echo "error: refusing to overwrite non-identical immutable relay artifact ${name}" >&2
      return 1
    fi
    if [[ "${attempt}" -lt 3 ]]; then
      sleep "${attempt}"
    fi
  done
  cat "${put_error}" >&2
  echo "error: could not create or verify immutable relay artifact ${name}" >&2
  return 1
}
