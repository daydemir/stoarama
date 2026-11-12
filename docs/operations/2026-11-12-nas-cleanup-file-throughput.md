# Bounded NAS execution groups

## Read-only production diagnosis, before implementation

Jelly PostgreSQL was queried with `PGOPTIONS='-c default_transaction_read_only=on
-c statement_timeout=30000'`, without printing or transferring credentials.
At 06:59 UTC, plan 1402 had 3,547 would-delete observations and 48 leases.
At 07:02 it had 3,563 observations and 32 pending items; at 07:10 all 3,595
were checked. **Zero ever-issued items had #468's detailed receipt timings.**
The latest execution evidence still dated to 04:04:44 UTC, before #468.
The runner subsequently latched `live API master differs` at 07:08:25 after an
ON attempt, then recovery verification. No production state was changed here.
This failure explains why the requested post-#468 execution samples do not
exist. It is not evidence of zero latency for the missing stages.

Historical plan 1401's successful receipts, from the #470 investigation:

| Stage | Mean | Interpretation |
| --- | ---: | --- |
| Full media + sidecar hashing | 170 ms | Nested inside proof; cleanup hashing is **unthrottled**, unlike inventory's 20 MB/s default |
| Physical proof | 178 ms | Hash, sidecar parsing/binding, identity and namespace checks |
| Observation request | 309 ms | Transport + server policy + R2 freshness + transaction |
| Server policy | 4.1 ms | Nested inside observation, not additive |
| Client execution before result send | 1,602 ms | Includes proof/observation plus unattributed local work |
| Receipt spacing | 2.474 s median | Includes leasing, receipt transport, scheduling/maintenance |
| Open/stat, sidecar alone, journal/ledger fsync, unlink/fsync, receipt RTT, lease RTT, lane | unavailable | Old receipts cannot separate them; no post-#468 sample exists |

Execution already uses an exact conditional R2 HEAD against dry-run evidence;
MD5/full-SHA auditing happens in dry run. This change keeps that path and its
failure behavior. It does not pretend that hashing or policy explains the
multi-second residual. See the [earlier report](2026-11-12-nas-cleanup-throughput.md)
for sample sizes, outliers, dry-run duration and redeploy evidence.

## Implementation and crash contract

An explicit reviewed release, `cleanup-batch-v1`, can lease at most eight
execution items in **one group**. Existing releases retain their single slot.
The database derives group membership from one lease request, plan and live
boot/version. No second group can lease while any first-group lease or issued
permit remains. Partial receipts cannot release the slot.

The NAS opens and fully hashes every file under the existing writer fence,
retaining all directory/file FDs and unlink-log references for the group.
Independent keep-window guards and complete unchanged dry-run proofs remain.
Observations commit together in SQLite before one execution request. R2 HEADs
run concurrently; all existing per-file policy/freeze/session/budget gates run
inside one database transaction. The client must validate **every** exact
permit before it can unlink anything. Execution requests never split, retry or
queue behind batch linger. A lost response uses reconciliation only.

Intent checkpoints commit together, then local rolling cap reservations commit,
then every ledger intent is appended with one fsync. Only after all three
barriers succeed are exact unlinks performed **sequentially**, with existing
per-file guards, path/namespace/identity checks, stop checks and deadlines.
Each parent directory is still fsynced. An unlink or directory fsync failure
stops the group immediately. Prior successful removals and untouched remainder
retain their durable intents as uncertainty.

After every unlink succeeds, ledger results share one append/fsync; exact
result checkpoints share one SQLite commit; receipts share one HTTP request and
server transaction. Checkpoints clear only after all individual acknowledgments
match. A partial ledger write/fsync or failed SQLite commit never produces a
speculative successful receipt. Group stages retain #468 timing names; shared
barrier latencies are repeated in per-file diagnostics, so must be counted
**once per group**, not summed across files. Post-send journal/receipt timings
remain local and do not alter immutable receipt bytes during replay.

The 5 TB rolling ever-issued cap, permanent recovery holds, source freeze,
independent outside-window verification, one enabled plan, 15-second observation
budget, 30-second permit maximum, OFF between plans and between-batch-only
rollout remain. No schema migration, deployment, master toggle, client staging,
runner restart or production mutation is performed. Future staging must choose
`NAS_VERSION=cleanup-batch-v1` explicitly after review and server deployment.

## Throughput model and validation

The real Python/PostgreSQL/fake-R2 fixture, with 200 ms per API request and
200 ms per R2 HEAD, completed 16 execution files plus 16 successor dry runs in
14.621 s serially versus 2.810 s with groups (5.2x). Every current file was
confirmed deleted, every successor file remained, and successor issuance was
zero. These are control-plane measurements on Mooney, not NAS disk measurements.
With an additional 180 ms per physical proof and 100 ms per real fsync,
the same fixture measured **23.201→7.347 s** (3.16x): 2.18 executing files/s,
or **2.82–3.76 TB/day** at 15–20 MB/file before fixed plan overhead. This
is a simulated latency budget, not an observation of Synology fsync. It includes
unchanged per-directory fsync and background successor preflight.

A conditional NAS model is `seconds/file = H + D + S/8`: unchanged physical
proof H, unshared unlink/directory cost D, and amortized transport/durability S.
Using H=0.18 s and the historical 2.5 s median, D=0.1–0.2 s gives about
0.56–0.65 s/file, or **2.0–3.1 TB/day** for 15–20 MB files, before fixed plan
and recovery overhead. At 15 MB, ≥2 TB/day requires ≤0.648 s/file, and unshared
cost ≲0.20 s before those overheads. Higher historical mean spacing and NAS
contention can reduce throughput below target. The 5 TB cap is unchanged.

This is a target-sized design and a projection, **not proof of ≥2 TB/day on the
Synology**. Production's stopped runner prevents a measured per-stage forecast;
the first later authorized run must establish actual unshared disk latency and
sustained end-to-end bytes/day. No unsafe stat-only proof or concurrent unlink
was introduced to inflate a benchmark.

Validation: 13 new group crash/guard tests and 127 existing cleanup tests pass
(one existing skip); the 85 runner tests and eight preparer tests pass. Scoped
API/CLI/database cleanup suites pass under the Go race detector; API/CLI vet
passes. Tests include complete group membership and exact-session binding,
lease/receipt response loss, partial receipt blocking, uncertain receipt pause,
SQLite rollback, partial intent/result ledger writes, crashes after each of six
unlinks, directory fsync failure, deadline expiry, local cap refusal, external
namespace changes, exact acknowledgment validation, and the real client/DB
throughput fixture. Fresh plan review found no authorization or safety blocker.
