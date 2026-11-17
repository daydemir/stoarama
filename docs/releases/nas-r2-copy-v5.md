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

## Storage age at delivery

After inventory/host payload assembly, **each** actual heartbeat attempt reads
`storage_status` again through its existing stable mounted-directory descriptor.
Compatibility retries also resample. Cached runtime storage is overwritten only
in that attempt's payload. If the fresh read fails or is unavailable, the entire
heartbeat is skipped; no cached storage is sent.

`STORAGE_TELEMETRY_MAX_AGE_SEC = 90` and `HEARTBEAT_TIMEOUT_SEC = 20` are unchanged.
The sample timestamp precedes the filesystem read. Its age is checked after JSON
assembly, during transport, and in the isolated child immediately before header
and body sends, after DNS/TCP/TLS preparation. The shared POSIX monotonic clock
works across processes, including macOS Python 3.9. The heartbeat uses the
existing bounded child transport with a **20-second total** watchdog (startup,
DNS, TLS, send and response), rather than urllib's per-operation timeout. Timeout
kills/reaps the child; it never replays that ambiguous request.

Thus the receipt-age bound is `M = 90 + 20 = 110 seconds`. D's independent
controller contract retains `T = 180 seconds`, leaving a 70-second receipt age
window. D owns migration 0203 and the storage-update version binding: only an
actual v5 storage update can establish eligibility. This PR does not change the
server's storage UPDATE or authorize cleanup.

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
