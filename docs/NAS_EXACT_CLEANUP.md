# Exact NAS cleanup safety contract

## Current policy: keep28 (one 28-day period per stream)

Approved by Deniz (2026-11). Each of the 109 streams keeps exactly ONE period:
its best 14 local days ±7 days, i.e. `[best_start-7d, best_start+21d)` in the
stream's canonical local timezone (28 local days). Every NAS file outside that
period is cleanup-eligible, including raw clips and stitched outputs behind
published deliveries, collation manifests or joined-source snapshots. Nothing
inside the period can be admitted, observed as `would_delete`, or deleted.

- **Policy source.** `backend/internal/api/nas_cleanup_keep28_policy.json` is
  `~/stoarama-r2-select/selection.json` byte-for-byte (SHA-256
  `060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811`), generated
  by `select.py`: per local day, the union of seconds covered by clips not in
  the collation-quarantine set (R2 availability not required); best 14-day sum;
  ties go to the earliest start. It is embedded with `go:embed`; the server
  refuses every cleanup decision unless the bytes hash to the pinned SHA, there
  are exactly 109 distinct streams, and each window is exactly 28 local days
  centred on `best_start` with UTC bounds equal to local midnights.
- **Per-file KEEP decision** (every dry-run and enabled observation): the file's
  directory is a reviewed folder owned only by the file's stream; the file and
  every source clip are valid, nonempty intervals that do not intersect the
  window (half-open); the canonical timezone and pinned source manifest are
  unchanged; and a live replay of the best-14 selection
  (`cleanupCurrentRankingsSQL`, same scoring as `select.py`) still returns the
  pinned `best_start`. Any difference keeps the file. Unknown streams keep.
- **Removed protections (by decision):** the public-delivery registry check,
  collation-manifest/quarantine references and `recording_joined_source_snapshots`
  no longer block files outside the window. The DB fences that stop *new*
  references to held clips while a plan is armed are unchanged.
- **Unchanged gates:** exact-file immutable manifests and plan digest; the
  frozen NAS filesystem identity (now index schema v2, covering every
  single-owner folder, root `82b067b2…`; see `build_frozen_index.py`); fresh NAS
  rehash and exact proof; R2 byte-verified recovery copy, or `--allow-nas-only`
  for files proven missing from R2; path confinement; per-plan enable with the
  expected plan SHA, a complete dry run fresher than 24 h and a 1-hour approval;
  and the backend `NAS_CLEANUP_DELETION_ENABLED` master switch (default off).
- **Plan binding.** New plans store the keep28 policy SHA in
  `nas_cleanup_plans.report_sha256`. Plans bound to any other policy are never
  leased, are observed only as `keep`, and can only be paused or retired.

### Retiring plans built for the old policy

Migration `0170_nas_cleanup_retired_plans.sql` adds the terminal `retired` mode.
Retired plans keep every row (plans, items, holds); their holds stop blocking R2
purges exactly like paused holds. Plans that are enabled or ever issued a
deletion are never retired.

```sh
stoaramactl nas-cleanup retire-superseded --reason "keep28 policy supersedes 2026-11-02 plans"            # preview, no change
stoaramactl nas-cleanup retire-superseded --reason "keep28 policy supersedes 2026-11-02 plans" --expected-plans N
```

### Preparing plans and a pilot

Before the first admission, publish the v2 frozen identity index (metadata
only, immutable keys, deterministic root): `python3
backend/scripts/nas-cleanup/build_frozen_index.py --publish` with the R2
environment; it must print `index_sha256` `82b067b2…`.

`backend/scripts/nas-cleanup/prepare_keep28_plans.py` lists every outside-window
raw clip present on the NAS (`nas_inventory_files`) and every collated output
acknowledged on the NAS (`recording_collation_output_acks`) whose sources are
all outside, in a read-only DB transaction; it batches them per stream, kind and
recovery class (`-nasonly` for R2-purged sources) into plans of at most 500
items. Without `--submit` it only writes the plan inputs and a summary.

```sh
# Pilot: one stream, one plan of at most ~5 GB, read-only dry run only.
prepare_keep28_plans.py --stream 3 --max-bytes 5e9 --max-plans 1 --out pilot-s3            # review
prepare_keep28_plans.py --stream 3 --max-bytes 5e9 --max-plans 1 --out pilot-s3 --submit   # admit dry run
stoaramactl nas-cleanup resume-dry-run --plan-id ID --expected-plan-sha256 SHA --reason pilot --priority 100
stoaramactl nas-cleanup report --plan-id ID
```

Arming a plan remains a separate, explicit decision (`enable` with the exact
plan SHA after a complete fresh dry run, with the master switch on).

## Historical design notes

NAS cleanup is an evidence-bound, two-key workflow. It never accepts a name,
folder, substring, prefix, suffix, regular expression, or glob as a target.
Production cleanup is disabled unless every target is an immutable manifest
item derived from explicit recording IDs.

## State machine

1. `building`: the server snapshots exact clip rows and one complete NAS
   inventory generation. Every item includes connection, recording, window,
   job and clip IDs; canonical relative path; actual NAS byte count and SHA-256;
   filesystem identity; sidecar identity; and immutable R2 recovery identity.
2. `proposed`: items are frozen, sorted, canonically encoded, and hashed. The
   proposal reports exact bytes and makes no claim that those bytes are safe to
   reclaim until every evidence gate passes.
3. `approved`: the exact digest and totals are displayed in this Codex thread.
   Deniz must explicitly approve that exact digest here. No caller-asserted
   approval API exists. A local privileged operator invocation may consume the
   displayed manifest only after that in-thread approval.
4. `claimed`: one NAS client claims the plan with a fencing token. The normal
   pull process's exclusive local lock prevents concurrent inventory, delivery,
   update, or cleanup filesystem work.
5. `quarantining`: the client revalidates the mount and every exact file. A file
   is moved atomically on the same filesystem to a plan-scoped quarantine only
   after path confinement, identity, size, NAS SHA, sidecar and R2 recovery
   proofs still match. A receipt is durable before the next item starts.
6. `reconciling`: a new complete, zero-skip inventory generation must account
   for every receipt. Drift stops the plan. Quarantine does not reclaim bytes.
7. `quarantined`: files remain byte-preserved and exactly restorable. Permanent
   purge is deliberately outside the first implementation. It requires a new
   approval over a new digest, a fresh R2 byte-restore proof and an exact
   one-file operation. There is no folder deletion or recursive delete path.

## Fail-closed gates

- The inventory generation is complete, current, zero-skip, and has no active
  scan. Mutable live inventory rows are copied into immutable plan items.
- Every database clip has exactly one present NAS entry matching canonical path,
  size and SHA-256; duplicate paths, unmatched files, missing rows and mismatches
  block the plan.
- The client freshly rehashes actual NAS bytes after claim. Inventory SHA alone
  never authorizes a move.
- R2 recoverability identifies the exact bucket, object key, immutable object
  version/ETag when available, byte count and content SHA. HEAD-only existence
  is insufficient when it cannot prove the content SHA; such an item is
  `UNKNOWN` and blocks approval.
- Paths are relative canonical POSIX paths. Empty components, `.`, `..`,
  backslashes, absolute paths, symlinks, non-regular files, device crossings,
  remounts and root identity changes are rejected. Traversal uses directory
  descriptors with no-follow semantics rather than resolve-then-open.
- Manifest digest, approval digest, claim fencing token and stored immutable
  items must agree at every transition. Any drift or API failure stops further
  operations.
- No executor uses recursive deletion. Quarantine and restoration operate on
  one exact file plus its exact sidecar at a time and fsync both directories.

## Approval message

The operator displays, but does not automatically execute, this exact proposal:

```
STOARAMA NAS CLEANUP PROPOSAL
Plan: <plan-id>
Recording IDs: <sorted explicit IDs>
Files: <exact count>
Bytes: <exact bytes>
Manifest SHA-256: <digest>
Recovery proof: <count>/<count> exact R2 byte-hash verified
Action now: move exact verified files to same-NAS quarantine only (0 bytes reclaimed)
No clips are permanently deleted. Approve in this Codex thread exactly:
APPROVE NAS QUARANTINE <plan-id> <digest>
```

Permanent purge, if later implemented, uses a separate message that states the
exact reclaim bytes and loss/recovery consequences. A quarantine approval can
never authorize purge.

## Candidate policy

Rank only paused recordings, prioritizing low completed-window coverage and
meaningful bytes while retaining unique historical scenes unless Deniz chooses
otherwise. Membership in SDOT or removal from the live roster is never cleanup
evidence: preserve all clips/evidence needed to audit qualification streaks and
the dynamic 60-candidate reserve. Cleanup considers only separately obsolete,
low-quality recordings after exact evidence and digest approval. One recording
per initial plan keeps review comprehensible. Current
mutable evidence shows the complete small validation cohort is about 33.28 GB;
that is useful to validate the workflow but does not solve current NAS runway.
No candidate is called safe until the complete-scan, fresh-rehash and R2
byte-restore gates pass.

## Direct original presentation witnesses

Use the existing selected-verification lane with `--original-witness` to measure
raw originals on the NAS without copying, remuxing, stitching or deleting media:

```sh
stoaramactl nas-selected-verify request --clip-ids 123 --original-witness --label retention-witness-example --dry-run --json
# Repeat the identical request with --expected-plan-sha256 <the dry-run SHA>.
stoaramactl nas-selected-verify report --label retention-witness-example --json
```

The digest binds byte identity, witness policy, immutable registered UTC bounds,
canonical timezone and one local date. Only clients advertising witness support
lease these requests. A byte `MATCH` remains separate from the structured witness
status. Successful witnesses retain one descriptor through SHA, complete strict
video/optional-audio decode and bounded rational frame observations. Supported
presentation intervals are normalized on the registered date, merged before
inward microsecond rounding, and capped by the registered span. They are stored
media lower bounds, not camera UTC or legacy ingest timestamp contracts.

Start with decisive exclusive boundary dates, including one last-positive-date
clip for each eligible sparse stream. A finished global NAS walk or native-stitch
receipt is not a prerequisite. Unsupported timing, cross-date attribution,
decode errors, cancellation or identity changes create no usable-coverage
upgrade; they never establish a zero upper bound or permission to delete.
Current public delivery, unknowns, alternative periods and original sources
remain protected. This read-only mode cannot approve cleanup or backfill.
