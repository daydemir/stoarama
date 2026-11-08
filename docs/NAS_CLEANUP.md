# NAS cleanup: preparation and explicit enablement

Operator commands use the existing service credential, matching the other
service control APIs. Account and NAS pull keys cannot create or enable plans.

The NAS app polls the backend over its existing authenticated outbound channel.
No SSH or new service is required. Preparing a plan never enables deletion.

`stoaramactl nas-cleanup prepare --input plan.json` accepts a label and up to 500
explicit `{kind,source_id}` pairs. Supported media kinds are raw and collated;
unknown/legacy joined artifacts remain retained. The backend derives identities
from account 47/connection 13 catalog rows, requires their exact observed paths and
fingerprints in the complete audit, and checks the independently reviewed folder
policy. Import labels and manifests are immutable and idempotent.

The app opens the mounted root and each ancestor with directory FDs and
`O_NOFOLLOW`, rejects nested devices/hardlinks, verifies original fingerprints and
full SHA, and parses/binds raw sidecars including registered dates. It reports the
measured root/ancestors/file/sidecar identities and current parent membership.
Every outcome is durably journaled in the existing app-owned inventory database.
App replacement waits for the cleanup worker to checkpoint and stop.

The backend revalidates source identities, all 221 previously protected UTC windows (223 deduplicated
windows after the November2 refresh),31 whole-stream protections, all three current
registered/availability/quarantine rankings, current source/error references and
public delivery. Unknowns, changed metadata, shifted sensitivity windows and
unsupported content stay retained. Historical inventory MATCH or download ACKs
are never promoted into cleanup proof.

Report a plan with `stoaramactl nas-cleanup report --plan-id ID`. Results separately
show full R2 SHA verification, proven 404 absence, unchecked/failed recovery,
physical conflicts and policy holds. HEAD equality alone is not recovery proof.

Two independent gates remain OFF during preparation:

1. Backend `NAS_CLEANUP_DELETION_ENABLED` defaults false.
2. A plan defaults `dry_run`. Explicit `nas-cleanup enable` requires its exact
   `--expected-plan-sha256`, `--reason`, a complete matching dry run less than 24 hours
   old, and full R2 recovery for every item. Approval expires after 1 hour.

### Belt-and-braces safeguards (safety council, 2026-11)

All are code constants (raising one is a reviewed code change) and fail closed:

- **No NAS-only deletion.** `allow_nas_only` is rejected (API 400, DB check
  `nas_cleanup_plans_no_nas_only`); a permit is issued only for an item whose R2
  copy was streamed and SHA-verified. Re-introducing NAS-only deletion needs its
  own reviewed change.
- **One enabled plan** at a time; a plan above **100 GB** cannot be enabled; no
  permit is issued beyond a rolling **3 TB / 24 h** of issued deletions.
- **Auto-pause.** During execution any observation that is not a permit
  (unverified, conflict, missing, client error, policy/recovery mismatch,
  budget), an expired execution lease, or any receipt other than `deleted`
  pauses the plan.
- **NAS client guard** (independent of the server): pinned copy of the keep28
  selection and of `clients/nas-pull/nas_cleanup_folder_streams.json`, both
  SHA-checked at startup (a failed check refuses every deletion; other work
  continues). Before each unlink the client maps the folder to a stream, reads
  the interval from the physical sidecar (raw) or the output bounds confirmed
  by the dated path (collated), and refuses on keep-window overlap, unknown
  folder/bounds or a disagreeing server `stream_id`. Hard local caps: 50,000
  files / 3 TB per rolling 24 h. Every unlink is bracketed by fsynced intent and
  result lines in `/state/cleanup-deletions.jsonl` (append-only). A media unlink
  whose sidecar step failed is receipted `media_deleted_sidecar_failed`.
- **R2 purge fence.** `nas-verified-purge` refuses to run (dry run included)
  while any cleanup plan is enabled and skips clips held by any non-retired plan.
- **Recovery check.** `stoaramactl nas-cleanup verify-recovery --plan-id ID`
  HEADs (read-only) the R2 copy of every item whose deletion was ever issued
  and reports any that are missing or differ in size (exit status 2).

Runbook notes:

- **An auto-paused plan that already deleted items cannot be re-enabled.**
  Enable requires every item to be a fresh `would_delete`, and resume-dry-run
  refuses plans with any issued deletion. Investigate the pause reason
  (`nas-cleanup report`, `approval_reason`), run `verify-recovery`, then admit
  the remaining files as a new plan.
- **`approved_until` expiring does not change the mode.** The plan stays
  `enabled` (no new permits are issued) and still blocks enabling any other
  plan and every `nas-verified-purge` run until it is explicitly paused.
- **Deploy the API (and promote a NAS client) only while no cleanup item is
  `leased`**: pause any enabled plan first and let outstanding leases finish or
  expire. The client also refuses in dry runs (reported as `conflict`), so a
  plan the client disagrees with fails to enable instead of pausing mid-run.
- Migrations 0171 (metadata-only, NOT VALID) and 0172 (VALIDATE, which proves
  no legacy `allow_nas_only=true` row exists) run as separate transactions.

Enablement is a separate user decision. Do not change either gate while doing
preparation or dry runs. A later exact-file permit expires after 30 seconds. The app
fences its delivery writers and re-hashes/rechecks each approved file before its
final exact-name unlink. Directories and unlisted siblings are never removed.
Changing sibling membership after one file finishes does not change another
file's identity approval; that next file still measures a fresh stable parent.

A lost permit response is reconciled without executing deletion. An interrupted
or partially failed destructive attempt is reported `unverified`; it is never
retried blindly. A monotonic ever-issued flag preserves recovery holds even after
uncertain outcomes and pauses. DB triggers fence old purge binaries, source/date/
namespace changes and new joined/collation references. Successful NAS removal
retains its R2 recovery copy; this workflow does not authorize R2 deletion.
Source-reference fences end with approval and permit grace, so retained R2
sources can be used for future stitching. Dry runs do not prohibit new source
references; those references are rechecked before any deletion grant.

While approval/permits can be outstanding, public delivery serves the immutable
reviewed generation. A shared DB fence serializes public reads with enablement;
new generations are deferred until approval and the short permit grace end.
Existing reviewed public downloads remain available. A changed registry prevents
further cleanup permits until reviewed.

`nas-cleanup pause --plan-id ID --expected-plan-sha256 SHA --reason TEXT` stops
future grants. An already issued 30-second permit may finish. Recovery holds for any
ever-issued operation remain; pausing never asserts that NAS bytes survived.

The original 430 folder roots / 6.43 TB logical are policy candidates, not an executable
recursive command or guaranteed reclaimed storage. This workflow materializes a
narrower exact-file set. NAS-only treatment and the final selected batch are left
for explicit approval. Full kept 28-day transfer and decoded optimality are separate.

Bulk verification performance: parent fingerprints describe byte-exact entry
names, not unrelated sibling metadata. Full target/sidecar hashes, inode/stat/
hardlink checks, directory-FD confinement and final permit deadlines still apply.
Client namespace/hash/proof durations and backend policy/recovery durations are
reported separately. Dry-run ranking snapshots may be reused for at most five
minutes; enabled observations always query live rankings. Cached preflight evidence
never authorizes a deletion. Both deletion gates remain OFF by default.

The November2 policy refresh covers all109 NAS streams in one read-only repeatable-read snapshot. All221 previous UTC intervals and all31 whole-stream holds remain; two newer intervals are added. Unassigned catalog records remain retained. Source snapshot and policy gzip fixtures are pinned by their full SHA256 values and monotonic-protection tests.

Full-manifest preparation uses a content-addressed folder index of the original
immutable audit. Its pinned root binds all2,943,864 observations, the original
audit SHA and reviewed folder ownership to3,918 fully verified shards. Admission
fetches only the required folders, checks every referenced shard hash, and
retains all original frozen-stat, sidecar, hardlink, owner and duplicate checks.
A missing, corrupt or mismatched index fails closed. This index grants no
deletion authority; both gates and fresh live checks remain unchanged.
The metadata-only producer is backend/scripts/nas-cleanup/build_frozen_index.py.

Preflight is bounded to eight concurrent read-only tasks, with rolling refills
and at most64 leases per drain/replay cycle. Batch requests lease only free slots,
up to eight; legacy requests default to one task. One coordinator owns
receipt replay and drains every worker, including after a lease/worker failure,
before replay, replacement or serial execution. Approved destructive tasks are
never dispatched concurrently. Independent observations share a plan read lock;
operator control still needs the exclusive parent lock. Public-registry evidence
may be cached for one minute only in dry runs; enabled grants always check live.

A verifier release can become stable when a fresh full physical proof receives
an exact matching nonexecuting backend acknowledgement with R2 SHA verification,
and this boot has passed authenticated heartbeat and fresh mount checks. This
does not clear legacy raw-feed errors or advance its progress. Replayed receipts,
empty leases and mere thread liveness cannot qualify a release. Promotion checks
the immutable version-specific manifest for the compiled CLIENT_VERSION and
installs only the captured verified source bytes,
not a mutable candidate pathname. In-process staging is serialized, and the
candidate path is retained so a concurrent external stager cannot lose its newer
release. A candidate mismatch or unavailable release metadata defers promotion without
blocking raw delivery. This also covers an external stager replacing the source
pathname during interpreter startup; the expected hash is not read from that path.
Optional heartbeat host.release
telemetry reports candidate/stable status through the existing connection API.

When a backend rollback rejects optional release telemetry or batch-limit fields
with HTTP400, the client retries once without only that optional field. All
authentication, source identities, bounds and other telemetry remain unchanged.
Other status codes and a second rejection use the ordinary retry/outage path.
