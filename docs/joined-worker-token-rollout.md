# Joined worker token rollout

This migration lets each dedicated join host create and retain its own worker
bootstrap token. Only the token's SHA-256 hash leaves that host. The existing
shared bootstrap token remains valid while old workers move, then can be
retired in a separate change after this rollout.

It changes bootstrap authentication only. It does not change recording, raw
objects, joined objects, claims, leases, or the signing key.

## Safety contract

- Keep every new worker service disabled and stopped until its read-only auth
  check passes.
- Never print, copy, or send a host token. Do not place one in Git, Render, a
  command argument, or an operator transcript.
- Add hashes through the single Render env key
  `JOINED_WORKER_BOOTSTRAP_TOKEN_SHA256_ALLOWLIST`. Do not replace the service's
  env set.
- Keep `JOINED_WORKER_BOOTSTRAP_TOKEN` during the migration so current workers
  continue to authenticate.
- Deploy the API before adding any hashes. An empty hash allowlist keeps the old
  behavior.
- Stop a host before revoking its hash. A claim token minted before revocation
  can remain valid for up to 10 minutes, and an active operation token remains
  fenced by its database lease.
- A per-host token supports individual revocation. It is not host attestation
  or trusted attribution. An allowed token can be replayed from another host,
  and the later `worker_id` is supplied by the client.

## Host-local token creation

Run this on each disabled worker host with shell tracing off. The command emits
one hash and never emits the token.

```bash
set -euo pipefail
set +x
umask 077
token_file=/etc/stoarama-joined-worker-token.env
if test -e "$token_file"; then
  echo "refusing to overwrite existing worker token file" >&2
  exit 1
fi
worker_token="$(openssl rand -hex 32)"
set -o noclobber
printf 'STOARAMA_JOINED_WORKER_TOKEN=%s\n' "$worker_token" >"$token_file"
set +o noclobber
chmod 600 "$token_file"
printf '%s' "$worker_token" | sha256sum | awk '{print $1}'
unset worker_token
```

Load that file after the worker's ordinary environment file. This overrides
only `STOARAMA_JOINED_WORKER_TOKEN` and leaves the rest of the host config
unchanged.

## Disabled-first rollout

1. Merge and deploy the API with an empty hash allowlist. Verify the live
   commit, API health, recording health, and an existing worker's read-only
   joined status request.
2. Generate one token on each disabled host. Record the host-to-hash mapping in
   the private rollout log. Copy only the hashes.
3. Set the allowlist with a one-key Render env update. Redeploy the exact
   verified commit. Confirm the required storage and database env keys still
   exist before continuing.
4. On each stopped host, make a read-only `GET
   /api/v1/recording/joined/status` request using the token from its local env
   file. A `200` or intentional `204` proves auth without claiming work. A
   `401` stops that host's rollout.
5. Start one worker. Verify one fresh heartbeat, one terminal task result, its
   output manifest and object hashes, and unchanged recording health.
6. Add workers in bounded rungs. Stop the rung on API errors, lease expiry,
   scratch pressure, recording drift, or failed output verification.

To revoke one host, stop its service first, remove only its hash from the
allowlist, redeploy the same verified API commit, and wait out any existing
claim or lease before reassigning its work.
