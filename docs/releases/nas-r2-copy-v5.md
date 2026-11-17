# R2 COPY v5 heartbeat contract

`r2-copy-v5` retains v4 COPY admission and fresh per-file cleanup permits (one
execution file). It grants no stitch-proof admission. The source keeps its
`development` placeholder; the release script substitutes the exact literal.
Existing v4 and v2.2 eligibility is unchanged.

Deploy the reviewed backend before activating the v5 client: strict heartbeat
decoding rejects unknown host fields on older servers. Drain COPY and updater
children before replacing the client. The sole production controller binds the
backend commit, client literal and immutable artifact hash at its idle release
boundary. This change makes no production operation and has no migration.

## Storage challenge and bounded work

Each successful heartbeat response provides `storage_challenge_nonce`, a fresh
64-character lowercase-hex nonce issued/stored by the server. The next storage
attempt samples **after receiving** that challenge and echoes it as
`storage.challenge_nonce`. The process-local cache is consumed before sampling
or transport, including compatibility retries. A failed request, lost response
or failed sample cannot reuse the spent nonce. Heartbeat sends are single-flight;
the server rotates on every response. Duplicate immediate responses are ignored.
The nonce is removed from generic response diagnostics and never persisted.

First boot or response loss sends fresh display-only storage without a nonce.
That is normal: the server updates display facts, clears budget eligibility and
returns a new challenge. Missing/stale/mismatched proof earns no budget. D owns
migration0203, connection-row atomic verification/rotation and the separate
`nas_storage_budget_at`, copied from the matched challenge's server issuance.
Budget freshness uses that server clock and `T = 180 seconds`. A delayed/buffered
request cannot acquire a fresh budget timestamp merely by arriving late.

After inventory/host payload assembly, **each** actual attempt reads
`storage_status` through its existing stable mounted-directory descriptor.
Compatibility retries also resample. Cached runtime storage is overwritten only
in that attempt's payload. If the fresh read fails or is unavailable, the entire
heartbeat is skipped; no cached storage is sent.

`STORAGE_TELEMETRY_MAX_AGE_SEC = 90` and `HEARTBEAT_TIMEOUT_SEC = 20` remain local
work bounds. The sample age is checked after JSON assembly, during transport and
immediately before sends after connection setup. The I/O child's first actions
set default-action SIGALRM and arm20s, then only shorten it to the original
absolute shared-monotonic deadline. Parent exec/crash/SIGKILL cannot remove the
child deadline. The parent checks resources and reaps without a watchdog thread.
These bounds limit wasted work; they do not prove publication time at buffered
ingress. The earlier M=90+20 receipt-age claim is superseded by the challenge.
No heartbeat response or ambiguous failed POST is automatically replayed.

## Optional host pressure diagnostics

The wire field is `host.memory_diagnostics`; the server stores it in
`connections.nas_host->'memory_diagnostics'` with the existing
`nas_host_reported_at` receipt timestamp. The values describe host `/proc`, not
the client's RSS or a container memory limit:

- `meminfo_bytes`: MemFree, Buffers, Cached, SwapCached, Active(anon),
  Inactive(anon), Active(file), Inactive(file), Dirty, Writeback, AnonPages,
  Shmem, Slab, SReclaimable, SUnreclaim, SwapTotal, SwapFree. Linux kB values are
  multiplied by 1024.
- `proc_stat`: procs_running, procs_blocked (counts).
- `vmstat`: pswpin, pswpout, pgmajfault, pgscan_kswapd* and pgsteal_kswapd*
  (kernel cumulative counters, with their kernel-defined units).

Missing, malformed, negative and overflowing values are omitted. Each proc read
is capped at 64 KiB; vmstat names at 32 characters and retained counters at 20.
The server caps this optional subtree at 4096 bytes and drops invalid diagnostics
without rejecting valid liveness or existing host facts. Global decoding remains
strict. COPY and stitch memory HOLD lines append the same bounded JSON snapshot.
Existing resource parsers, floors, hold reasons, counters and retry waits remain
unchanged.

## Supported timestamp runtimes

The central `parse_certification_timestamp` accepts RFC3339 fractions of 1–9
digits, Z and explicit ±hh:mm offsets. It pads to six fractional digits for
Python 3.9 and truncates sub-microsecond digits without rounding or extending
expiry. There are no caller-specific shims or server timestamp changes.
Malformed dates, fractions and offsets still fail. Controller-recorded NAS
heartbeat host facts at `2026-11-17T10:04:29.99246Z` report Python 3.14.6.
CI's primary backend interpreter reproduces that production fact. Python 3.9
remains an explicitly tested compatibility runtime in the existing CI matrix.
The pipeline harness requires an absolute `STOARAMA_TEST_PYTHON` executable path;
CI binds it to the exact `setup-python` output, rather than resolving a name on
PATH. The Ubuntu 24.04 Python 3.9 job also runs the parser, complete cleanup suite
and explicitly bound real v5 pipeline.
Jelly validation uses `/usr/bin/python3` (3.9.6) and an isolated exact 3.14.6
interpreter matching the recorded NAS runtime, with no production execution.
The exact old v2.2 artifact accepts all tested fractional widths on 3.14.6.
The supplied bounded observation/control/command-failure records do not
establish timestamp parsing as their live failure cause; the 3.9 compatibility
reproduction is not evidence of a production failure rate.
