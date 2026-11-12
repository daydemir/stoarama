# NAS cleanup throughput, read-only investigation

Evidence cut: 2026-11-12 06:49 UTC. Production access was read-only: Jelly
runner JSONL/status, PostgreSQL with `default_transaction_read_only=on` and a
30-second statement timeout, and Render deploy-list GETs. No deployment,
configuration, plan, client release, receipt, or production data was changed.

## Measured budget

Times below distinguish wall time from nested per-file timings. Execution
wall time spans first through last terminal receipt; a receipt's execution
duration starts **after leasing**. Hash is contained in proof, and policy is
contained in observation. Do not add nested columns together.

| Plan | Dry/admission/check wall time | Execution wall time | Confirmed deleted | ON deployment (Render create→finish) | Outcome |
|---|---:|---:|---:|---:|---|
| 1397 | 39.5 min admission→first deletion, including toggle/checks | 226.5 min | 99.982 GB / 3,596 files | 98 s latest pre-execution deploy | Completed |
| 1398 | 58.1 min admission→dry-run pass | 197.4 min | 45.141 GB / 2,873 files | 145 s | Partial, retired |
| 1400 | 52.5 min admission→last dry observation | 19.5 min through failure | 1.920 GB / 125 files | 93 s | Pre-#468 deadline failure |
| 1401 | 37.6 min admission→last dry observation | 10.7 min through failure | 2.993 GB / 183 files | 83 s | Pre-#468 deadline failure |
| 1402 | ≥45 min admission→snapshot; 3,547/3,595 checked | None yet | None | Not enabled | 48 outstanding dry leases |

1398 had another **73.6 minutes** between dry-run pass and the eventual ON
request; this interval includes failed attempts and operator/restart delay,
not NAS execution. After 1401 the runner logged failure at 04:12, then its
next startup OFF confirmation at 06:02: an additional **110.6 minutes** of
failure/operator/restart wall time. These intervals cannot be classified more
precisely from the available logs.

Successful Render redeploys generally took **80–145 seconds each**; the 1400
OFF deployment took 131 seconds. OFF runtime verification/retries and recovery
are additional: 1400's final receipt→recovery-verified interval was 3.8 minutes;
1401's was 4.9 minutes, with multiple OFF attempts. The existing logs do not
separate drain, runtime verification, and recovery HEAD time within those
intervals. They must not be labelled pure recovery or pure redeploy time.

| Plan, successful deletions only | Client execution mean | Observe mean | Proof mean | Hash mean | Backend policy mean | Receipt-to-receipt median / p95 |
|---|---:|---:|---:|---:|---:|---:|
| 1396 | 1,576 ms | 290 ms | 248 ms | 238 ms | 2.3 ms | 2.443 / 3.682 s |
| 1397 | 1,764 ms | 291 ms | 318 ms | 295 ms | 2.3 ms | 2.639 / 4.728 s |
| 1398 | 1,865 ms | 423 ms | 170 ms | 158 ms | 5.4 ms | 2.880 / 4.955 s |
| 1400 | 4,119 ms | 341 ms | 196 ms | 164 ms | 6.1 ms | 2.691 / 50.004 s |
| 1401 | 1,602 ms | 309 ms | 178 ms | 170 ms | 4.1 ms | 2.474 / 3.344 s |

Failed receipts were excluded from this table: 1400/1401's final client
execution durations were 71.6/50.6 seconds. Historical rows lack the new #468
stage details; journal, ledger, final unlink/fsync, receipt acknowledgment,
lease transport and between-item scheduling cannot be individually recovered
from these aggregates. The ~1–2 seconds between client execution duration and
receipt spacing is an **unattributed residual**, including leasing, local
headroom reads, maintenance windows and transport; it is not a measured hash
or policy bottleneck.

After the #468 rollout (latest OFF deployment finished 06:24:07), the available
execution sample was **zero**. For 1,153 subsequent 1402 dry observations,
mean proof/hash/policy were **599/489/27 ms**, with ranking work averaging
22 ms (mostly cached, occasional expensive refresh). No post-#468 execution
improvement is claimed from those dry-run samples.

At 06:49, the last 48 dry leases dated from **06:29:13–06:30:35**, expiring
**06:59:13–07:00:37**. The client had reported interruption around 06:30.
Those unreturned leases alone can delay completion for nearly 30 minutes
after a restart. No lease was modified during this investigation.

Handled dry-run interruptions now return unused leases through the existing
durable reconciliation-only protocol, just as execution writer-drain refusals
already do. Matching live session, exact attempt/plan, never-issued status and
absence of any observation/intent/result checkpoint remain required. Lost
responses replay the exact durable return, never a permit request. This avoids
the expiry wait for graceful interruption, while abrupt crashes and old-session
orphans correctly retain the existing expiry/reconciliation requirement.

## Changes and limits

The runner admits **one atomic successor** while master is still OFF. A durable
pending-admission latch makes an interrupted/ambiguous admission stop for owner
reconciliation instead of creating another lookahead on restart. Existing
recursive admission splitting is disabled for the runner. Admission remains
dry-run-only; current execution ownership is unchanged.

During serial execution, two bounded client workers preflight the successor
through existing dry-only leases (`execution_files=0`, `execution_bytes=0`).
Executable work has scheduling priority over fresh dry work. The background
lane drains before primary journal replay and API shutdown. The write fence
is released before waiting for that drain. Every file is still freshly opened,
fully hashed and independently guarded; execution still awaits its own permit
and acknowledged receipt. The runner monitors successor failures too.

Current OFF, drain, R2 recovery verification, close, successor independent
outside-window verification, budget wait, ON redeploy and final gate rechecks
remain in their original order. The 100 GB/10,000-item limits, one executing
plan/item, source freeze, 15-second observation budget, permanent reservations,
rolling ever-issued 5 TB cap, durable journals/ledger and owner alerts remain.
Master stays OFF between batches. No permissions or credentials change.

Connection queue locking now excludes terminal retired plans with no leases,
while retaining paused retirement candidates, leases in every mode, batch
scope expansion and canonical lock order. Production had 1,395 such retired
plans among 1,399 total. A local 1,399-plan fixture measured **1.74→0.56 ms** per
lock call: useful contention isolation, but too small to explain throughput.

New local execution outcomes time leasing, result-journal persistence, receipt
acknowledgment and journal clearing. Lease time is also included in the bounded
receipt timing map; post-send timings remain local so durable receipt bytes
never change during replay.

## Throughput projection and proof

The real Python client + PostgreSQL + fake R2 fixture used two 16-file plans,
1 MiB files, 40 ms simulated API delay and 80 ms R2 HEAD delay. The serial path
took **4.337 s**, pipeline **3.955 s** (8.8% less wall time, 9.7% more throughput).
A race-detector repeat took **4.592→4.332 s**. Both paths deleted exactly the
current 16 files; pipeline alone overlapped successor preflight with execution.
Peak execution observation concurrency was one; successor issuance was zero,
and every successor file remained. This is transport/control-plane evidence,
not a Synology disk benchmark.

For a healthy small-raw-file workload like 1398, extrapolating its 6,398-file
100 GB plan at 4.124 seconds/file gives ~440 minutes execution. Removing a
58-minute serial dry run, while keeping ~4 minutes of toggles, changes roughly
**0.287→0.325 TB/day** (13%). The larger-file 1397 workload projects roughly
**0.53→0.63 TB/day** if its admission/check interval can be hidden. These are
conditional steady-state projections: successor preflight must finish during
execution, and extra NAS contention/failures can reduce the gain. They exclude
the measured operator/failure idle intervals and do **not** establish 5 TB/day.

Larger plans only amortize a few minutes of fixed overhead and do not remove
per-file serial cost. Batch observation/receipt endpoints are already used;
execution observation deliberately bypasses batching. Skipping rehash cannot
save the multi-second residual shown above and weakens the current physical
proof. Parallel execution or broad master-ON reuse would change owner/safety
conditions, so neither is included. Reaching 5 TB/day on ~16 MB files requires
about **3.6 files/second**, far above the measured ~0.24 files/second; this PR
does not claim that target.

Future staging must use explicit `NAS_VERSION=cleanup-pipeline-v1` after the
server change is reviewed and deployed between batches. This PR performs no
staging, deployment, runner restart, master toggle or cleanup mutation.
