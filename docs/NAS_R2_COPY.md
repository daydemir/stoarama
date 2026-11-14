# R2 keep28 copy to NAS (build only)

Owner decision: keep28 footage only in R2 → **“Copy to NAS, then delete”**.
This change implements raw copy/verification only. No NAS client release or
promotion, switch activation, queue import, production migration, or deletion
is part of this implementation. Collated copies are explicitly blocked by the
owner's follow-up; the existing retirement and cleanup selection code is unchanged.

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
`queue` defaults to a read-only validation against one primary-key source at a
time; `--apply` idempotently queues the reviewed batch. Every transaction has
10s statement timeout, 2s lock timeout, 16 MB work memory and one CLI connection.
All work remains bound to account 47 / connection 13 and the keep28 policy.

`NAS_R2_COPY_ENABLED=false` is the server default. When enabled, the existing
always-running NAS client leases one file, nice19 / idle I/O, one thread and
256 KiB working chunks. It refuses low host/cgroup memory or critical load,
uses the existing storage reservations, requires the mounted output and a
**2 TB free-space floor**, and yields to cleanup's local execution fence.
The server refuses enabled cleanup plans/batches, maintenance and live cleanup
leases/delete permits. The worker runs only in natural gaps between batches;
no cleanup suspension is required or authorized.

Each exact GET range is <=8 MiB, ETag/version/Range-bound and signed for 10s.
Before every GET the server durably charges its full range to the connection's
**100 GB UTC-day** ledger. Abandoned grants, failed GETs and retries retain their
charge. Grants in the last 30s of a UTC day charge both dates, covering their
10s URL lifetime plus the client's bounded transfer deadline. A GET is consumed
once, with no retry or redirect; capabilities are never persisted. A crash or
retry obtains and charges a new grant. This bound depends on the trusted client
obeying single-use dispatch: R2 presigned GETs themselves are reusable until
expiry. It is not a defense against deliberate capability replay by a hostile NAS.

The NAS writes a no-follow resumable private partial, checks each range's
Content-Range/ETag, verifies whole-file SHA/size, fsyncs and hardlinks the final
without overwriting it. It hashes the committed file again, creates its normal
provenance sidecar without overwriting, and publishes the existing raw inventory
proof. The server marks the queue item verified only after that exact fresh
inventory proof exists. An integration test runs retirement's unchanged exact
NAS SQL against this shape. This grants **no R2 deletion authority**. Evidence
freshness for any later retirement must be re-established within that tool's
24-hour window; a months-old copy result is not fresh proof.

Control HTTP has a 2s total deadline; each range GET has a 10s total deadline.
A small isolated Python stdlib transport process makes DNS, TLS, trickling
headers/body and reads killable. Its pipe output is bounded to 8 MiB plus a
4 KiB metadata header, it has no media paths or inherited credential environment,
and it is always killed/reaped before releasing the writer fence. The parent
checks cancellation while polling I/O, hashing and pacing writes; a range GET
may also have one bounded control request in flight for its checkpoint.
Transient lease tokens and URLs are scoped to active operations, never accumulated
in the shared diagnostic secret registry. Policy, OFF and cleanup state are checkpointed at most
4s apart during work. The worker drains before self-update or inventory close.
Only its private temporary links are removed after successful no-overwrite
commit; existing source media, final files and conflicts are never deleted.

## Rate and space

Defaults: 2 MB/s network writes and 2 MB/s verification reads; 100 GB/day of
charged grants. The ideal download+rehash ceiling is 86.4 GB/day, before API,
per-file and retry overhead. Plan for approximately **70–85 GB/day of new
bytes during continuously available copy time**, then measure on NAS. Existing
files need only hashing. Cleanup gaps and resource holds reduce calendar rate.
No rate has been measured on the NAS by this build-only task.

Given 1.77 TB free of 92.14 TB and a 2 TB floor, let C be cleanup GB/day,
assuming no other disk growth and no copy consumption while accumulating space:

| Threshold | Additional free GB | At 109 GB/day | At 200 GB/day |
|---|---:|---:|---:|
| First bounded copy range (includes two 8 MiB guards) | 230.017 | 2.11 days | 1.15 days |
| Earlier 4.031314 TB no-presence raw cohort fits in full | 4,261.314 | 39.09 days | 21.31 days |
| Conservative 6.465899 TB raw ceiling fits in full | 6,695.899 | 61.43 days | 33.48 days |
| All 9.993298 TB incl. future collated support fits in full | 10,223.298 | 93.79 days | 51.12 days |

Time is additional-free-GB / C. Copying is resumable and can start at the first
threshold; it need not wait for the whole cohort to fit. With cleanup and copy
alternating, free-space gain is cleanup bytes minus actual copied bytes.
The supplied 109 GB/day is plan 1409's measured active rate before v6/byte
ordering; 200 GB/day is an unmeasured target. Actual last-24h cleanup was only
20–45 GB/day because it was stopped part of the time. 12.47 TB remains eligible
for cleanup: 114.40 active days at 109 or 62.35 at 200 GB/day, not a verified
“coming weeks” completion forecast. These are estimates, not a reason to lower
the floor or interrupt cleanup.

## Parent release procedure (do not execute during implementation)

1. Merge this reviewed PR on top of merged #486. Deploy backend migrations and
   API through the normal server flow with `NAS_R2_COPY_ENABLED` absent/false.
   Confirm all API instances serve the new endpoints with the switch OFF.
2. Wait for a natural cleanup batch drain: no enabled plan/batch, live cleanup
   lease/delete permit, replay backlog, or unresolved quarantine. Do not pause
   an issued enabled plan to manufacture a release window.
3. From the clean reviewed merged checkout, use the existing operator-local R2
   credential environment (never DSM env, no auth movement):
   `NAS_VERSION=r2-copy-v1 backend/scripts/release-nas-pull.sh`.
   Then `backend/scripts/promote-nas-pull.sh --version r2-copy-v1 --expected-live-version <observed-live-version>`.
   These commands stage/promote through the existing server-controlled updater.
   Confirm heartbeat/logs show `r2-copy-v1`, healthy inventory, and the v6 cleanup
   capability path. This implementation does not run either command.
4. Recreate/validate a current copy-or-verify classification if catalog/policy
   changed. Do not generate a production R2-retire plan with unbounded legacy
   scans. The saved v9 input is evidence, not current apply authority. Plan:
   `stoaramactl nas-r2-copy plan --input <copy.jsonl.gz> --sha256 <exact-file-sha> --out <new-dir>`.
   Review `summary.json`, the raw batches and the blocked collated manifest.
5. Validate one raw batch without `--apply`, then queue that reviewed batch:
   `stoaramactl nas-r2-copy queue --input <raw-NNNN.jsonl> --sha256 <batch-sha> --apply`.
   Queueing does not enable work. Start with one bounded batch, inspect
   `stoaramactl nas-r2-copy status`, and verify NAS has >2 TB plus range guards.
6. In the chosen gap, edit only server key `NAS_R2_COPY_ENABLED=true` through
   the existing single-key server settings/deploy flow. Observe first copies,
   stable inventory/sidecars, quota charges, free space and cleanup yielding.
   Return the key to false before the next cleanup batch if desired; cleanup
   activation independently stops copy admission and checkpoints active work.
   No NAS environment variable or shell command is needed.
7. A separate reviewed retirement step must reverify fresh exact NAS custody
   and all existing holds/policy/identity gates before deleting anything in R2.
   Collated work remains blocked until the follow-up below lands.

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
