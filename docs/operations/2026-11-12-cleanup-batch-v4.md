# Cleanup batch v4: local throughput and durability

Baseline: merged `38cf409849b974b3356076d4e472a01d86f3675c` (`cleanup-batch-v3`).
No production actions, client staging/promotion, migrations, or deployment were
performed. `cleanup-batch-v4` is the new source selection and server allowlist
label; existing published clients and the v1/v2/v3 server protocol paths remain.
Future staging uses the existing explicit `NAS_VERSION=cleanup-batch-v4` release
script, after review; that script was not run.

## Changes and crash contract

- Reserve the entire bounded group in one `BEGIN IMMEDIATE`/FULL-WAL transaction.
  One cap scan replaces duplicate headroom scans and eight reservations. Table
  initialization is part of Inventory startup. Reservation insert/commit failure
  rolls back the whole group. Live exits release exact inserted row IDs for all
  unattempted members, including members never reached. Historical rows for the
  same item and attempted charges are preserved. A killed process conservatively
  retains all reserved charges until the existing 24-hour expiry.
- Persist all final diagnostic outcomes in one transaction. Diagnostics remain
  separate from authoritative per-file checkpoints and uploaded log entries.
- Remove the redundant first per-file SQLite intent commit. The durable
  observation already covers crashes after permit issuance but before the
  attempt marker. The ledger intent is still fsynced before deletion, the sole
  SQLite intent records `unlink_attempted=true` durably before either unlink,
  and live pathname/identity/deadline checks run after that commit. The marker
  remains valid for the sidecar syscall too. Directory fsync, result ledger
  fsync, and exact per-file result checkpoint all remain before reporting.
  A live refusal may replace an intent only while the executing owner knows no
  syscall was attempted. A crash after either unlink and before the result
  checkpoint replays uncertainty, never a certified refusal. Replay cannot unlink
  or request a new permit. Lost acknowledgments replay exact durable results.
- Migration 0188 creates `(plan_id,lease_expires_at) WHERE state='leased'`,
  supporting both lease expiry and connection lock-scope queries. The index
  excludes the pending backlog. Above 128 MiB the migration fails with an exact
  concurrent-precreation instruction; it validates existing index definition,
  validity, readiness, access method, columns, uniqueness, and predicate. No
  schema/lock/lease semantics change. The guard test lowers only the byte threshold
  to exercise the large-table path without allocating another large local table.

## Harness measurements

`bench_cleanup_group.py` runs real eight-file groups against disposable media,
FULL-WAL SQLite, real ledger/directory syncs, and a local fake permit/receipt API.
It prewarms lazy schema/ledger creation; it includes initial ownership, group
observations, caps, serial deletion/results, clear, and final diagnostics. It
excludes startup and media-file creation. All eight deletions and an empty replay
journal are asserted. The C interposer counts actual libc fsync/fdatasync calls,
including SQLite C calls; a startup probe rejects incomplete SQLite interception.
No durability is disabled. Detailed samples are in the adjacent benchmark JSON.

| Measurement, eight files | v3 | v4 |
|---|---:|---:|
| FULL-WAL SQLite commits | 43 | 21 |
| Actual fsync calls | 67 | 45 |
| fsyncs/file (including group overhead) | 8.375 | 5.625 |
| Native local median, five runs | 0.008789 s | 0.007622 s |
| Latency-injected median, three runs | 36.525395 s | 27.936836 s |
| Latency-injected group-service projection | 13.14 files/min | 17.18 files/min |

The injected runs add **385 ms to each real sync**, 779 ms per serial permit,
255 ms per proof, and 1,952 ms once per group for leasing. Both revisions receive
identical delays. The sync delay is a synthetic calibration near the supplied
36.019 s successful-group baseline, not a measurement of NAS fsync latency.
It does not model real cap-scan contention, network jitter, NAS hashing throughput,
maintenance or writer-drain failures. Native storage times are not NAS forecasts.

A separate PostgreSQL 17 fixture has 42,406 pending rows, eight future leases,
1,404 plans, and an expired lease on another connection. The real expiry SELECT's
EXPLAIN ANALYZE changes from **3.181 ms** and a backlog scan to **0.016 ms** using
the partial index. The lock-scope SELECT also uses the new index. Tests check
connection isolation, detection of an actual expired own lease, repeated migration,
large-table guard, concurrent exact precreation, and wrong precreation rejection.
These local query times do not substitute for the supplied 181.455 ms live probe.

The synthetic client saving is 8.588559 s/group. Applying it to the supplied
36.019 s clean-service budget gives 17.50 files/min. If the live query probes
represent normal service, removing 8 × 181.455 ms + 188.181 ms adds 1.639821 s/group
of savings: approximately **18.61 clean-service files/min**. Applying those same
absolute savings to the supplied **8.27 sustained files/min** while preserving
all other maintenance/failure/gap overhead gives approximately **10.04 sustained
files/min**. These are conditional projections, not a production speed claim.

## Reproduce locally

Use session scratch for the shared library and saved baseline. On macOS:

```sh
git show 38cf409849b974b3356076d4e472a01d86f3675c:clients/nas-pull/stoarama_pull.py > "$scratch/before.py"
cc -dynamiclib -o "$scratch/count_fsync.dylib" clients/nas-pull/test-support/count_fsync.c
DYLD_INSERT_LIBRARIES="$scratch/count_fsync.dylib" python3 clients/nas-pull/bench_cleanup_group.py --client "$scratch/before.py" --runs 3 --sync-delay-ms 385 --permit-delay-ms 779 --proof-delay-ms 255 --lease-delay-ms 1952
DYLD_INSERT_LIBRARIES="$scratch/count_fsync.dylib" python3 clients/nas-pull/bench_cleanup_group.py --runs 3 --sync-delay-ms 385 --permit-delay-ms 779 --proof-delay-ms 255 --lease-delay-ms 1952
python3 -m unittest discover -s clients/nas-pull -p 'test_cleanup*.py' -q
scripts/test-db run -- go -C backend test ./internal/api ./internal/db ./cmd/stoaramactl -run '^Test(NASCleanup|NASClient|Cleanup)' -race -count=1
```

Linux uses `cc -shared -fPIC ... -ldl` and `LD_PRELOAD` instead of the macOS
compiler/preload flags. The sync-count regression compiles and runs this harness
in CI and requires 45 actual syncs and 21 commits for each eight-file group.
