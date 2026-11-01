# NAS cleanup: preparation and explicit enablement

Operator commands use the existing service credential, matching the other
service control APIs. Account and NAS pull keys cannot create or enable plans.

The NAS app polls the backend over its existing authenticated outbound channel.
No SSH or new service is required. Preparing a plan never enables deletion.

`stoaramactl nas-cleanup prepare --input plan.json` accepts a label and up to500
explicit `{kind,source_id}` pairs. Supported media kinds are raw and collated;
unknown/legacy joined artifacts remain retained. The backend derives identities
from account47/connection13 catalog rows, requires their exact observed paths and
fingerprints in the complete audit, and checks the independently reviewed folder
policy. Import labels and manifests are immutable and idempotent.

The app opens the mounted root and each ancestor with directory FDs and
`O_NOFOLLOW`, rejects nested devices/hardlinks, verifies original fingerprints and
full SHA, and parses/binds raw sidecars including registered dates. It reports the
measured root/ancestors/file/sidecar identities and current parent membership.
Every outcome is durably journaled in the existing app-owned inventory database.
App replacement waits for the cleanup worker to checkpoint and stop.

The backend revalidates source identities, all185 older UTCwindows (221 dedup
windows with newer buffers),31 whole-stream protections, allthree current
registered/availability/quarantine rankings, current source/error references and
public delivery. Unknowns, changed metadata, shifted sensitivity windows and
unsupported content stay retained. Historical inventory MATCH or download ACKs
are never promoted into cleanup proof.

Report a plan with `stoaramactl nas-cleanup report --plan-id ID`. Results separately
show full R2 SHA verification, proven404 absence, unchecked/failed recovery,
physical conflicts and policy holds. HEAD equality alone is not recovery proof.

Two independent gates remain OFF during preparation:

1. Backend `NAS_CLEANUP_DELETION_ENABLED` defaults false.
2. A plan defaults `dry_run`. Explicit `nas-cleanup enable` requires its exact
   `--expected-plan-sha256`, `--reason`, a complete matching dry run less than24hours
   old, and full R2 recovery. `--allow-nas-only` additionally permits only proven
   R2absence; it never permits unchecked recovery. Approval expires after1hour.

Enablement is a separate user decision. Do not change either gate while doing
preparation or dry runs. A later exact-file permit expires after30seconds. The app
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

While approval/permits can be outstanding, public delivery serves the immutable
reviewed generation. A shared DB fence serializes public reads with enablement;
new generations are deferred until approval and the short permit grace end.
Existing reviewed public downloads remain available. A changed registry prevents
further cleanup permits until reviewed.

`nas-cleanup pause --plan-id ID --expected-plan-sha256 SHA --reason TEXT` stops
future grants. An already issued30second permit may finish. Recovery holds for any
ever-issued operation remain; pausing never asserts that NAS bytes survived.

The original430 folder roots /6.43TBlogical are policy candidates, not an executable
recursive command or guaranteed reclaimed storage. This workflow materializes a
narrower exact-file set. NAS-only treatment and the final selected batch are left
for explicit approval. Full kept28 transfer and decoded optimality are separate.
