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
   old, and full R2 recovery. `--allow-nas-only` additionally permits only proven
   R2absence; it never permits unchecked recovery. Approval expires after 1 hour.

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
the source SHA captured at boot and installs only the captured verified bytes,
not a mutable candidate pathname. In-process staging is serialized, and the
candidate path is retained so a concurrent external stager cannot lose its newer
release. A candidate mismatch defers promotion without blocking raw delivery.
Optional heartbeat host.release
telemetry reports candidate/stable status through the existing connection API.
