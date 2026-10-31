# Exact NAS cleanup safety contract

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
