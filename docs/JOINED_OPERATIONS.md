# Joined recordings: unattended operation

## Progress and review

- [Recordings and playable joined clips](https://stoarama.com/shared/mit-scl/recordings): public access; sort by Joined and open a recording's Joined clips folder.
- [Processing and seam audit](https://jellys-mac-mini.taildfdcc0.ts.net/): Tailscale access; check the snapshot timestamp, processed hours, joined coverage, and per-stream review cases. Jelly's existing audit refresh runs every 300 seconds.

Joined coverage uses recorded source duration inside each selected best consecutive 14-day window. A processed hour can contain separate parts, gaps, or review-needed sources. Only fully accounted ready source duration earns the media-ready label. NAS receipt is a separate delivery state.

## Operating limits

Recording and raw NAS transfers retain priority. Joining runs on dedicated hosts under the API's task cap. Existing raw and joined objects on R2 and NAS are never modified or deleted; publication is create-only, additive, and verified. Failed seams retain source identities and diagnostic evidence. Deferred preseal failures remain unresolved review work; they do not count as completed footage or confirmed gaps. Temporary worker caches may be released only by the existing API-proven inactive-lease cleanup; historical protected evidence roots stay read-only.

The worker handles explicitly classified recoverable failures. Authentication, integrity, unknown failures, and ambiguous failure acknowledgements remain visible stops. Never solve a stop with an unconditional restart loop, a manual database reset, or relaxed media validation.

## Deploy a worker change

1. Merge reviewed code into the default branch and verify its tests, including real PostgreSQL claim/lease tests.
2. Dispatch `build-stoaramactl-worker.yml` for the exact merged commit. Verify the independently reproducible Linux-amd64 artifact and its SHA before staging it.
3. Identify the actual active systemd unit on each host. Inspect effective configuration, including all drop-ins: executable, scratch path, read-only evidence paths, and `Restart=no`. A filename's apparent ordering is insufficient proof.
4. Stage one stopped dedicated host with a fresh private scratch root. Check the effective executable and permissions before starting; check the running executable's SHA afterward. Preserve host-local credentials.
5. Verify a distinct fenced claim, renewed heartbeat, terminal result, immutable output reconciliation, and recording health. Roll remaining hosts through graceful task boundaries. `SIGTERM` stops new claims; `TimeoutStopSec=14700` preserves the admitted task's four-hour budget and bounded shutdown.

Do not claim rollout completion from a staged binary or a running PID alone. Record the effective deployed commit, canary result, and remaining old workers in the campaign checkpoint. Enable boot startup only with the explicit authorization marker retained as an operator kill switch; preserve fail-closed process restart behavior.

## When attention is required

Use the existing status commands and timestamped audit snapshots first. A flat interval during long verification is expected; compare successful output growth, renewing leases, task deadlines, and failure reasons. Check recording health before raising concurrency. Investigate only the affected host or row unless shared infrastructure is unhealthy.

For NAS problems, use the supported client release and delivery-status APIs. Preserve raw priority and generation/acknowledgement checks. Do not log into the NAS, reset cursors, or delete clips to accelerate delivery.

Archive ZIP availability is optional and separately configured. Individual joined playback and progress must remain available when the archive service is disabled.
