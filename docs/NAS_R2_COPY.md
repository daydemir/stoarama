# R2 keep28 copy to NAS

Owner decision: keep28 footage only in R2 → **“Copy to NAS, then delete”**.
The raw copy/verification lane remains default OFF. The production controller
alone deploys, stages/promotes clients, queues reviewed manifests and enables it
between cleanup batches. No R2 deletion authority is added. Collated copies
remain blocked by the owner's follow-up below.

Owner throughput decision, 2026-11-14: **“no copy limit, so delete and transfer
as fast as possible”**. `r2-copy-v2` removes the daily grant cap, fixed 2 MB/s
pacing and nice19/idle-I/O priority. It carries v7 bulk cleanup durability (and
must carry the execution-poll fix when released).

## Reconciled size and evidence

The latest saved v9 classification is **502,048 objects / 9.993297901426 TB**:
455,363 raw / 6.465899440735 TB and 46,685 collated / 3.527398460691 TB.
All 502,048 object intervals intersect the pinned keep28 selection. No object
exceeds 4 GB. These are **copy-or-verify** bytes, not proven physical NAS absence.

The earlier 4.03 TB figure is reproduced by Jelly
`~/r2-retire-plans/20261111-final.log`, created 2026-11-11 23:49:38Z:
272,935 raw objects / **4.031313694496 TB** classified `copy_to_nas`.
Another 182,428 raw objects / **2.434585746239 TB** were separately classified
`nas_identity_unproven`. Their exact sum is v9's raw total, both objects and
bytes. V9's `r2RetireDeliveryDecide` explicitly reclassifies
`nas_identity_unproven` and `kept_collated_no_exact_inventory` as `copy_to_nas`.
The catalog scan counts are identical in both summaries: 1,875,732 raw and
158,503 collated rows. This is a classification change, not 5.96 TB of new footage.
The older collated-no-exact-inventory category was 44,700 / 3.927694960178 TB;
new public-delivery protections and changed eligibility also alter that cohort.

The old v3 directory no longer exists on Jelly and its archive search has no
match. The surviving log reproduces the exact 4.031314 TB claim. V7's owner
outside-window-only preview has an empty copy manifest (zero objects/bytes), so
it cannot be used as a kept-window copy census. V9's guarded and full previews
agree on the 9.993298 TB classification. Its compressed manifest SHA256 is
`f35e1299bd741bc5827927168fe47ed9a228c18183d795bd71c3bc88efff5a9c`.
The pinned selection file was read, never overwritten.

[Machine-readable evidence](records/nas-r2-copy-20261114.json) records exact
bytes/counts/hashes and every nonzero stream. No production catalog/inventory
scan was run. **Fresh physical R2-only size remains unknown until the NAS
verifies existing paths**; 4.031314 TB is the earlier no-presence raw cohort,
6.465899 TB is the conservative raw copy-or-verify ceiling. Existing files are
hashed without download; mismatched files and sidecars are preserved and HOLD.

## Execution design

`stoaramactl nas-r2-copy plan` verifies the exact saved classification, pinned
policy and all row/header totals before producing a new directory of immutable
raw JSONL batches and `collated-blocked.jsonl`. Nothing is silently filtered.
`queue` defaults to read-only validation with four workers and at most four CLI
connections, each checking one primary-key source at a time. `--apply`
idempotently queues the reviewed batch. Every item has its own transaction with
10s statement timeout, 2s lock timeout and 16 MB work memory per query operation.
The entire manifest passes its SHA, totals and duplicate checks before work
starts. Each item commits independently: a failure can leave any valid subset
queued. The command cancels and joins its workers before reporting failure;
repeat the exact manifest to finish an interrupted import. Existing queue
identity and progress are preserved, and a conflicting identity refuses.
All work remains bound to account 47 / connection 13 and the keep28 policy.

`NAS_R2_COPY_ENABLED=false` is the server default. Four independent workers
lease at most four files per connection. Lease admission retains the serialized
count. Active operations take a shared admission fence before their own item
lock, so renewals remain parallel but cannot race that count. Lease checks and
renewals use the current wall clock after lock acquisition. Each range is <=16 MiB,
ETag/version/Range-bound, signed for 10s, and consumed by one non-retrying,
non-redirecting GET with a 10s total deadline. Nonces and the per-item dispatch
gate prevent simultaneous grants and stale completions. A retry obtains a new
grant. R2 URLs remain reusable until expiry; trusted-client single use is not a
defense against deliberate hostile replay. Historical daily ledger rows are
preserved but are neither read nor updated by grant admission.

Mounted-root, host/cgroup memory and load checks, the **2 TB runtime free floor**
and shared storage reservations remain. Four bounded HTTP buffers require extra
memory headroom. 256 KiB disk chunks remain cancellable. A reservation includes
its range plus an aggregate concurrency guard; other downloads share the same
reservation counter. There is no fixed byte rate or idle/nice throttle.

### Concurrent cleanup proof and local fences

`Keep28Policy` exports cleanup's exact embedded policy and SHA. Copy validation
requires `start < keep_end && end > keep_start`, with valid nonempty bounds.
`nasCleanupPolicy.outside` requires its negation; ordinary cleanup rechecks the
file AND every source interval at admission, observation and permission. Unknown
bounds/streams remain KEEP. Boundary tests execute both predicates. Raw copy
rejects `joined/`, so collated deletion has a disjoint namespace. Stitched-source
retirement is a separate inside-window exception: its source holds/reservations
still veto copy and its permissions require stitching switches ON. Stitching
remains OFF. Copy does not alter those gates.

Source-ID holds and retirement reservations are rechecked on every operation.
An indexed `recording_clips(display_path)` probe rejects any second raw catalog
identity at the destination, including equal-content aliases. Equal bytes at a
different path do not authorize unlinking the copy's path. Independently, cleanup
requires exact frozen media identity, physical sidecar source/time binding and
its own pinned-window guard immediately before unlink; a new catalog alias cannot
make kept sidecar bytes into an outside-window file. No cleanup check is weakened.
Global enabled-plan/batch/live-lease/delete-permit vetoes are therefore removed;
the global maintenance veto remains.

Copy and ordinary raw delivery share per-path ownership across media, sidecar
and inventory publication. Cleanup never waits for that ownership. Every copy
namespace mutation (including ancestor mkdir, private creation and publication)
checks cleanup's directory registry under its short lock. Cleanup registers before
its first namespace snapshot and retains registration through unlink/durability.
Copy refuses namespace mutations in a registered directory; existing partial
writes and hashes can run alongside it. No fsync, network request or hashing runs
under the registry lock. A crash/interruption after publication is recovered by
removing only an exact same-inode private link; conflicts are preserved.

The complete cleanup bulk permit/durability interval has cooperative priority
over copy disk work. Pre-permit hashes share ordinary I/O; background dry runs
do not impose an unmeasured transfer throttle. Copy may finish a started chunk or fsync, then waits
at its next disk checkpoint while continuing cancellation, resource and remote
checks. Bounded download transports can continue; no cleanup worker waits for a
copy network request or whole-file hash. The pool supervisor drains every worker
before update or inventory close. Verify live cleanup permit timings and rate
with the combined client before claiming production throughput.

The NAS writes a no-follow resumable private partial, checks each range's
Content-Range/ETag, verifies whole-file SHA/size, fsyncs and hardlinks the final
without overwriting it. It hashes the committed file again, creates its normal
provenance sidecar without overwriting, and publishes the existing raw inventory
proof. The server marks the queue item verified only after that exact fresh
inventory proof exists. An integration test runs retirement's unchanged exact
NAS SQL against this shape. This grants **no R2 deletion authority**. Evidence
freshness for any later retirement must be re-established within that tool's
24-hour window; a months-old copy result is not fresh proof.

Control HTTP has a 2s total request deadline. Each of the four workers owns one
isolated, reusable stdlib control process, initialized before its first request.
Imports and verified TLS context initialization are paid once per healthy
process, outside the request deadline. Startup accounting has a separate 2s
expiry; OS process creation itself cannot be preempted before Popen returns.
On an overrun or cancellation, ownership is assigned before killing/reaping,
and no API request is sent. Do not claim a fixed maximum first-call wall time.

Each control request retains a killable 2s deadline covering IPC, DNS, TLS,
backend work and complete JSON response/parsing. Requests are at most 32 KiB,
responses at most 1 MiB, and frames at most 4 KiB. A failed transport is killed
and reaped before a later ordinary poll can create another; operations are
never automatically replayed. If the OS cannot reap after 2s, that worker
retains ownership and update/drain gating until the same process exits.

Each range GET keeps its separate, one-shot 10s deadline, including interpreter
startup. Its pipe output is bounded to 16 MiB plus a 4 KiB metadata header.
Both transports have no media paths or inherited credential environment.
The parent
checks cancellation while polling I/O, hashing and writing; a range GET
may also have one bounded control request in flight for its checkpoint.
Transient lease tokens and URLs are scoped to active operations, never accumulated
in the shared diagnostic secret registry. Policy, OFF and maintenance state are checkpointed at most
4s apart during work. The worker drains before self-update or inventory close.
Only its private temporary links are removed after successful no-overwrite
commit; existing source media, final files and conflicts are never deleted.

### Control timing log contract (r2-copy-v3)

The fixed INFO topic is R2 copy lease-control, admitted by the existing
diagnostic ring and uploaded through its existing client-log endpoint. Values
are integers only. No URLs, paths, headers, request/response bodies or
credentials are logged. Outer log records supply boot ID, sequence, timestamp
and client label; consumers must retain those and report coverage/relay lag.

Phase1 is startup: worker (0–3), generation (positive), startup_ready_ms,
total_ms, status=-1. Startup-ready time is measured once per process, from
before Popen to validated READY after imports/TLS initialization. It is -1
if READY was not observed. Startup total_ms includes failure cleanup.

Phase2 is one control request:

| Field | Meaning |
| --- | --- |
| worker, generation, request | Worker index, owned process generation, monotonically increasing per-worker request number |
| operation | 1 lease, 2 checkpoint, 3 range, 4 inventory publication, 5 result |
| startup_ready_ms | Cached measurement for this generation; repeated so READY eviction does not erase it |
| request_sent_ms | Child elapsed until HTTP request headers/body were handed to its socket, including DNS/connect/TLS |
| sent_to_first_byte_ms | From that send completion to the first HTTP response application byte, before complete headers |
| total_ms | Parent elapsed from before IPC write through JSON completion or failure, excluding subsequent reaping |
| status | Parsed HTTP status, retained even if a later body phase fails |

Unobserved timing/status values are -1, never fabricated zeros. Genuine
sub-millisecond values can round to zero. The child reports sent, first-byte
and status progress before the complete body so a later timeout retains those
observations. Sent-to-first-byte combines network return and backend time; it
does not identify HEAD, auth, fence or lock time individually. Parent and child
timings start at different IPC stages; do not subtract them into exact server
phase durations.

Sanitized examples:

    R2 copy lease-control phase=1 worker=0 generation=1 startup_ready_ms=740 total_ms=740 status=-1
    R2 copy lease-control phase=2 worker=0 generation=1 request=1 operation=1 startup_ready_ms=740 request_sent_ms=12 sent_to_first_byte_ms=84 total_ms=99 status=200
    R2 copy lease-control phase=2 worker=0 generation=1 request=2 operation=2 startup_ready_ms=740 request_sent_ms=10 sent_to_first_byte_ms=-1 total_ms=2001 status=-1

The existing generic hold lines remain. A separate controller-owned, pinned v3
consumer must parse this schema; the active earlier measurement helper is not
modified. A missing success metric alone does not measure in-flight downloads.

## Rate and capacity (2026-11-14 inputs)

Before activation, measured copied bytes/day is **zero** (switch OFF). The old
single-worker ideal ceiling was 86.4 GB/day for download plus rehash, with a
100 GB/day grant cap. The uncapped rate must be measured on the NAS; do not use
link speed as an achieved rate. Each successful file logs numeric source ID,
size, newly downloaded bytes, committed new-file bytes and elapsed milliseconds, separately from
verification-only work. Queue verified totals and timestamped cleanup receipts
supply the simultaneous custody/deletion measurement. Partial download/retries
are not verified custody.

Use decimal TB. With F=1.77 TB currently free, D=6.99 TB cleanup remaining,
C=6.465899440735 TB conservative raw copy-or-verify ceiling, and R=2 TB floor:

- After all cleanup and worst-case raw copy: F+D-C = **2.294100559265 TB** free,
  about **294.1 GB above the runtime floor** before filesystem overhead, sidecars,
  other writers/growth and measurement uncertainty. This is an arithmetic fit,
  not proof of allocated physical bytes. The 2.434586 TB identity-unproven cohort
  may verify existing files without consuming new media space; known-absent raw
  is 4.031314 TB, which would leave 4.728686 TB free if that were the true deficit.
- The controller's **2.23 TB one-time enable threshold** is an operator buffer,
  distinct from the runtime floor. From 1.77 TB it needs **460 GB** cleanup first.
  The worst-case final free is only 64.1 GB above that threshold, but no continuing
  2.23 TB floor is added. The runtime floor currently blocks start; it is retained.
- To guarantee no space pauses for the whole raw ceiling, cleanup first needs
  C+R-F = **6.695899440735 TB**, plus allocation/sidecar/concurrency overhead.
  Of 6.99 TB this leaves only about 294.1 GB margin. To start earlier, keep
  cumulative deletion D(t) ahead of physical new-copy bytes C(t) by at least
  **230 GB plus reservations/overhead**, since F+D(t)-C(t) must stay above 2 TB.
  If copy outruns cleanup, it MUST pause at the floor. No scheduling order can
  guarantee continuous full-speed copy from the early threshold without knowing
  both rates. Prefer verification-only work and cleanup first, then absent raw.
- Blocked collated adds 3.527398460691 TB. If all raw+collated needed new space,
  even all stated cleanup would leave **-1.233297901426 TB** free: **3.2333 TB**
  short of the 2 TB floor. It does not fit under that assumption, and remains
  blocked regardless of capacity.
- The newly discussed ~20 TB blocked R2 classes are NOT additional queue
  authority. Await fresh per-class NAS custody/needs-copy totals, deduplicated
  against raw copy sources. After worst-case raw, only ~294 GB new media fits
  above the floor before overhead. Extra physical deficit X needs at least
  max(0, X-0.294100559265 TB) further space. Footage already on NAS can verify
  without another media copy. Keep-all exclusions and the owner's stitching
  condition remain with the retirement delegate; no copy scope expands here.

## Initial copy-v2 controller release procedure

1. Merge the reviewed exact head on current main with `[skip render]`; retain
   v7 group durability and the controller's execution-poll fix. All exact bulk
   predicates and polling predicates must accept `r2-copy-v2`, without prefix
   matching. No new copy migration is required.
2. Controller only: deploy backend at a natural batch boundary (master OFF,
   no enabled plan/batch/live lease/permit/replay/quarantine). A deploy during
   enabled cleanup trips the runner's live-commit safety stop. Keep copy OFF.
3. From clean merged source, stage with
   `NAS_VERSION=r2-copy-v2 backend/scripts/release-nas-pull.sh`, then promote with
   `backend/scripts/promote-nas-pull.sh --version r2-copy-v2 --expected-live-version <observed>`.
   Use operator-local credentials in place; never move them to the NAS or another
   computer. Verify the complete published artifact against reviewed source.
4. Confirm heartbeat/client `r2-copy-v2`, healthy inventory and v7 bulk + execution
   polling behavior. Revalidate fresh current classification/policy/catalog before
   queueing; stale v9 totals are evidence, never sufficient apply authority.
   Validate a raw batch read-only, then queue with its exact SHA and `--apply`.
   Collated and unknown objects remain unqueued.
5. Once observed actual free exceeds the operator's 2.23 TB threshold, controller
   enables only `NAS_R2_COPY_ENABLED=true` between batches. Run alongside cleanup;
   preserve stitching OFF. Capture timestamped copy logs (downloaded bytes vs
   verified files), queue states, free space and cleanup confirmed-byte/file totals.
   Compare cleanup permit timings/deletion rate with baseline; resolve contention
   before calling throughput proven. Do not lower the 2 TB floor.
6. Calculate copied GB/day from new bytes over elapsed wall time, verified files
   separately, then project remaining raw completion from measured physical
   download and verification rates plus space/cleanup availability. A separate
   reviewed retirement flow must reprove fresh custody before any R2 deletion.
   This copy change has no R2 deletion capability.

### Connection 13 20-day work window

Migration 0236 installs the 109 pinned windows with the Keep Everything
streams marked. The existing delivery fence and historical plan hashes stay
available for recovery. Installing the migration leaves the work epoch unset.

After verifying that the entire deployment reads the new work view, attest the
current NAS reader and writer drain, keep the cleanup runner stopped and its
master off, and activate the epoch with
`stoaramactl nas-r2-copy activate-keep20 --sha256 BOUND_POLICY_SHA --apply`.
The policy SHA is the SHA-256 of `nas_media_keep20_manifest()` in the reviewed
migration. A single insert switches the ordinary feed, new copy admission and
stitch scopes. It refuses live leases and pending work outside the new window.
Copy and stitch receipts retain the existing transport and evidence digests.

## Copy-v3 upgrade (controller only)

The exact r2-copy-v3 label supports bulk/grouped cleanup, execution-only polling,
dry_run_limit2, v8 per-item truth and the retained v2 proof capability. The shared
proof-capability predicate admits exactly v85r-nas-proof-v2, r2-copy-v3 and
cleanup-batch-v8, only with selector1/connection13; default selector0 is unchanged.
This is eligibility, never artifact/profile acceptance or proof/deletion
authority. V3 is not a legacy-v6 client. Unknown/prefix labels are refused.

The native proof implementation and same-producer drift checks stay unchanged.
Legacy v1 verification remains strictly v2-only: its single client SHA cannot
represent a different current reader. Cross-version preserved-output renewal
requires the proof owner's new explicit readback protocol/handler/verifier and
versioned schemas, plus gates-owner finite exact original-producer/current-reader
profiles. Do not widen the old SQL, relabel new bytes5009, refresh native run
times or weaken24h/5min/DB expiry. V3 publication is blocked until that renewal
integration and consumer acceptance land on the final combined source/artifact.

1. Require the reviewed exact head, green CI and full artifact/source binding.
   Deploy the backend containing the exact v3 allowlist at a natural cleanup
   boundary and verify it LIVE before v3 can run.
2. Immutable5009 proof-v2 must have been published/promoted and verified stable.
   Its separately authorized proof window must finish and be runtime OFF
   before switching to v3. Never overwrite or relabel that artifact.
3. Build/stage/promote only the distinct immutable r2-copy-v3 artifact using the
   supported release mechanism and freshly verified pointers. Preserve v7/v8
   group durability/polling and proof+copy+cleanup union. Proof stays OFF without
   separate approval; no new producer/reader profile is implied by the label.
4. Keep the 305 already committed rows untouched: no reimport/retry, expiry
   extension or new cohort is part of this upgrade. Preserve resource/floor
   checks and the original operator-approved expiry/window.
5. Bind a separate pinned v3 numeric consumer/profile to the committed timing
   schema/tests above. Preserve outer boot/seq/label/time and coverage limits;
   do not edit an active earlier sampler or turn unknown fields into zeros.
6. Measure the next real run. No startup or server phase is historically
   established by the refused request-log probe. HEAD under1192 stays unchanged
   in this release; sent/first-byte/total timing guides any subsequent reviewed
   root fix. No shared timeout, grant or integrity check is relaxed.

## Collated follow-up after #483

Do not represent outputs using fabricated raw clip IDs. Add a proper output
identity/custody ledger or polymorphic inventory schema, then a bounded per-output
reporting endpoint and matching client publication. Existing raw inventory sync
rejects `joined/` paths, and ACK alone cannot satisfy exact retirement custody.
If adding nullable `clip_id` rows to `nas_inventory_files`, update these exact
readers to retain path/content observations for every kind while adding only
non-null IDs to raw-ID maps (no whole-table production verification scans):

- `r2_retire_evidence.go`: `load...` inventory loop at current line 76, and its
  fresh inventory-vs-listing mismatch loop at line 188, scan nullable raw IDs;
  retain path/content mismatch vetoes for collated observations.
- `r2_retire_owner.go:loadOwnerProtections`: inventory loop at line 71, scan
  nullable raw IDs, preserve PresentPaths/PresentContent for both kinds.
- `r2_retire_catalog.go:r2RetireLoadSets`: fresh exact-inventory snapshot at
  line 470, scan nullable IDs; add ExactRaw only for raw sidecar proofs while
  preserving ExactPaths for collated.
- `r2_retire_fence_projection.go`: inventory loop at line 45 must preserve
  collated unsafe paths without inserting null IDs into rawAliases/rawOutside.
- `r2_retire_phases.go:r2RetireLockExactNAS`: current line 314 selects clip_id
  into int64 even for collated. Lock/scan a genuine non-null inventory row key
  or nullable clip ID; preserve all path/SHA/size/freshness constraints.

Rebase those edits onto merged #483 first and audit its changed readers rather
than applying these line numbers mechanically. Prove mixed raw/collated rows
through planning, owner protections, mismatch vetoes, fence projections and the
live exact-custody lock. Keep reservation/retirement authority unchanged. The
current copy protocol and typed kind field can carry a collated implementation
later; this PR deliberately refuses to queue or execute that kind.

## Per-stream raw copy-or-verify and earlier missing cohort

TB below is decimal. Streams absent from both tables have zero entries in this
saved classification. The earlier missing cohort is historical, not a fresh
physical absence observation.

| Stream | Raw objects | Raw TB | Earlier no-presence objects | Earlier no-presence TB |
|---:|---:|---:|---:|---:|
| 3 | 11,276 | 0.297385550 | 5,754 | 0.200959204 |
| 78 | 10,055 | 0.075046051 | 0 | 0.000000000 |
| 158 | 19,542 | 0.142989570 | 19,542 | 0.142989570 |
| 175 | 807 | 0.005791533 | 0 | 0.000000000 |
| 178 | 228 | 0.003053726 | 0 | 0.000000000 |
| 179 | 259 | 0.000932459 | 0 | 0.000000000 |
| 182 | 2 | 0.000010400 | 2 | 0.000010400 |
| 186 | 3 | 0.000040893 | 3 | 0.000040893 |
| 195 | 11,319 | 0.170059061 | 11,319 | 0.170059061 |
| 293 | 253 | 0.003262919 | 0 | 0.000000000 |
| 295 | 245 | 0.003295424 | 0 | 0.000000000 |
| 415 | 10,051 | 0.075340289 | 0 | 0.000000000 |
| 469 | 2,957 | 0.078113233 | 2,931 | 0.077421299 |
| 487 | 3,895 | 0.026470568 | 3,835 | 0.026042074 |
| 2666 | 3,251 | 0.008505754 | 3,203 | 0.008383838 |
| 2667 | 218 | 0.000747383 | 0 | 0.000000000 |
| 2674 | 2,487 | 0.016598016 | 0 | 0.000000000 |
| 2675 | 3,863 | 0.026396573 | 3,800 | 0.025969443 |
| 2676 | 3,332 | 0.087110411 | 3,310 | 0.086552164 |
| 2677 | 260 | 0.003735040 | 0 | 0.000000000 |
| 2678 | 3,352 | 0.044423952 | 3,298 | 0.043693772 |
| 2680 | 3,523 | 0.047372433 | 0 | 0.000000000 |
| 2681 | 3,504 | 0.032892382 | 3,448 | 0.032548826 |
| 2693 | 2,902 | 0.077361712 | 2,878 | 0.076781018 |
| 2708 | 3,270 | 0.085924726 | 3,243 | 0.085278669 |
| 2713 | 233 | 0.000767307 | 0 | 0.000000000 |
| 2717 | 3,379 | 0.025921165 | 0 | 0.000000000 |
| 2718 | 3,196 | 0.044853606 | 3,145 | 0.044182272 |
| 2720 | 2,835 | 0.070195876 | 1,250 | 0.031793598 |
| 2726 | 2,957 | 0.077317808 | 0 | 0.000000000 |
| 2740 | 134 | 0.001965103 | 0 | 0.000000000 |
| 2757 | 3,305 | 0.025486041 | 3,252 | 0.025057559 |
| 2963 | 2,816 | 0.037974329 | 2,785 | 0.037587432 |
| 2965 | 3,208 | 0.033548750 | 0 | 0.000000000 |
| 2973 | 2,717 | 0.069867847 | 741 | 0.018516611 |
| 2975 | 278 | 0.001006908 | 0 | 0.000000000 |
| 3006 | 229 | 0.000836775 | 0 | 0.000000000 |
| 3007 | 3,135 | 0.021377025 | 3,076 | 0.020965013 |
| 3018 | 273 | 0.002021405 | 0 | 0.000000000 |
| 3021 | 256 | 0.000909652 | 0 | 0.000000000 |
| 3022 | 260 | 0.000966800 | 0 | 0.000000000 |
| 3025 | 229 | 0.001759522 | 0 | 0.000000000 |
| 3039 | 2,804 | 0.073397267 | 2,766 | 0.072382229 |
| 3049 | 2,729 | 0.071522075 | 2,691 | 0.070634764 |
| 12725 | 6,480 | 0.015048579 | 0 | 0.000000000 |
| 14782 | 11,200 | 0.153326788 | 11,200 | 0.153326788 |
| 17186 | 254 | 0.003828101 | 0 | 0.000000000 |
| 17208 | 10,762 | 0.121225914 | 0 | 0.000000000 |
| 17216 | 1,777 | 0.009939055 | 0 | 0.000000000 |
| 17219 | 1,772 | 0.027187817 | 0 | 0.000000000 |
| 17233 | 10,715 | 0.105351436 | 0 | 0.000000000 |
| 17234 | 7,653 | 0.226127099 | 0 | 0.000000000 |
| 17237 | 1,678 | 0.024360560 | 0 | 0.000000000 |
| 17238 | 3,143 | 0.066571954 | 0 | 0.000000000 |
| 17240 | 2,367 | 0.053089050 | 0 | 0.000000000 |
| 17241 | 7,695 | 0.086051128 | 0 | 0.000000000 |
| 17242 | 268 | 0.003945804 | 0 | 0.000000000 |
| 17243 | 1,895 | 0.033823899 | 0 | 0.000000000 |
| 17247 | 2,297 | 0.032665307 | 0 | 0.000000000 |
| 17248 | 248 | 0.003334484 | 0 | 0.000000000 |
| 17249 | 250 | 0.003506974 | 0 | 0.000000000 |
| 17251 | 18,895 | 0.273500952 | 18,895 | 0.273500952 |
| 17254 | 18,959 | 0.274509509 | 18,959 | 0.274509509 |
| 17256 | 18,986 | 0.274546145 | 18,986 | 0.274546145 |
| 17261 | 18,976 | 0.274527719 | 18,976 | 0.274527719 |
| 17268 | 18,939 | 0.274069820 | 18,939 | 0.274069820 |
| 17312 | 1,440 | 0.012829524 | 0 | 0.000000000 |
| 17315 | 1,440 | 0.049478988 | 0 | 0.000000000 |
| 17316 | 12,916 | 0.061529085 | 0 | 0.000000000 |
| 17319 | 10,031 | 0.022881752 | 0 | 0.000000000 |
| 17320 | 11,300 | 0.086772231 | 11,300 | 0.086772231 |
| 17321 | 1,439 | 0.008059438 | 0 | 0.000000000 |
| 17322 | 11,315 | 0.229892496 | 11,315 | 0.229892496 |
| 17323 | 10,590 | 0.249091970 | 0 | 0.000000000 |
| 17324 | 11,356 | 0.044296016 | 11,356 | 0.044296016 |
| 17325 | 21 | 0.000100926 | 0 | 0.000000000 |
| 17326 | 14,251 | 0.120708001 | 0 | 0.000000000 |
| 17328 | 11,317 | 0.079749949 | 11,317 | 0.079749949 |
| 17329 | 11,418 | 0.257833414 | 11,418 | 0.257833414 |
| 17337 | 12,671 | 0.163140655 | 12,671 | 0.163140655 |
| 17338 | 11,331 | 0.347298291 | 11,331 | 0.347298291 |
| 17339 | 722 | 0.010823554 | 0 | 0.000000000 |
| 17340 | 11,318 | 0.318033233 | 0 | 0.000000000 |
| 17342 | 9,451 | 0.188286505 | 0 | 0.000000000 |
| **Total** | **455,363** | **6.465899441** | **272,935** | **4.031313694** |

## Per-stream blocked collated copy-or-verify

Physical absence is unproven for this category; these objects need fresh NAS
custody proof or copying after the follow-up.

| Stream | Objects | TB |
|---:|---:|---:|
| 3 | 157 | 0.206851950 |
| 158 | 65 | 0.011096334 |
| 175 | 359 | 0.036618887 |
| 195 | 32 | 0.008892748 |
| 12725 | 7,128 | 0.029303241 |
| 14782 | 2,004 | 0.133686443 |
| 17208 | 8,496 | 0.112784045 |
| 17216 | 2,487 | 0.043331389 |
| 17219 | 335 | 0.098846242 |
| 17233 | 653 | 0.091206658 |
| 17234 | 389 | 0.166428706 |
| 17237 | 176 | 0.065122281 |
| 17238 | 344 | 0.115110320 |
| 17240 | 491 | 0.092725665 |
| 17241 | 627 | 0.091857539 |
| 17243 | 2,072 | 0.205233742 |
| 17247 | 5,003 | 0.151094702 |
| 17251 | 567 | 0.097575616 |
| 17254 | 225 | 0.043658393 |
| 17256 | 207 | 0.044222690 |
| 17261 | 248 | 0.054399537 |
| 17268 | 294 | 0.045903329 |
| 17312 | 333 | 0.024394670 |
| 17315 | 751 | 0.141241880 |
| 17316 | 930 | 0.036668234 |
| 17319 | 257 | 0.002372074 |
| 17320 | 958 | 0.070155280 |
| 17321 | 751 | 0.039482032 |
| 17322 | 835 | 0.178901349 |
| 17323 | 698 | 0.205154519 |
| 17324 | 2,151 | 0.035862858 |
| 17326 | 1,306 | 0.101493800 |
| 17328 | 1,263 | 0.064849274 |
| 17329 | 921 | 0.206744694 |
| 17336 | 544 | 0.099248341 |
| 17338 | 1,906 | 0.280955395 |
| 17339 | 188 | 0.048472652 |
| 17340 | 42 | 0.002923403 |
| 17342 | 492 | 0.042527551 |
| **Total** | **46,685** | **3.527398461** |
