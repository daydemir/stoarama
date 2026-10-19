The NAS space audit scans all entries visible under the app's clips mount,
including hidden files and joined/. It never opens media payloads, deletes media,
moves media, follows symlinks, or crosses filesystem devices. Other shares,
host-only folders, snapshots and recycle bins outside this mount are unobservable.

The one-shot `nas-space-stat-v1-20261019` audit starts after the client update,
resumes from a separate SQLite checkpoint under the app state directory, and
reports progress about once per minute. Mounted root/state identities are checked
before scanning and before sealing/reporting; a changed root gets a new audit
generation while prior checkpoints remain intact. Its rate is capped near 2,000 stats/s.
Checkpoints are also scoped to a digest of the API origin and authentication key;
re-enrollment or key rotation starts a fresh generation without storing credentials.
The app state directory also holds its compressed per-entry metadata report.
Export uses an exclusive temporary file and atomic replacement, so preexisting
report symlinks/hardlinks cannot redirect writes into media. Deterministic gzip
bytes are fsynced before the report identity is committed; missing/corrupted
reports are regenerated and verified before upload. Upload reads the same pinned
descriptor that was size/SHA checked. A 256 MiB state-volume reserve protects
control metadata; audit errors are reported without claiming completeness.
Interrupted export recovery reclaims only a temporary file whose private inode
was recorded in the checkpoint before writing. Unregistered lookalike files and
replaced paths are preserved.
This is a live, non-atomic observation; per-entry times, scan errors/exclusions,
and the total observation window must accompany any cleanup decision.

Categories distinguish unstitched media, stitched media, metadata, temporary,
unknown and special entries using path/extension. Duration is not inferred.
Folder totals use logical lengths and allocated blocks separately. Inode totals
avoid hardlink double counting; allocated blocks do not prove unique extents or
reclaimable space on a filesystem with copy-on-write or snapshots.

The authenticated API accepts only this connection's audit metadata. It grants a
bounded create-only PUT for an immutable report key, then verifies current R2 HEAD
length and uploader-declared SHA metadata. That evidence is not a fresh payload
hash. The report command downloads the report with an independent size/SHA check:

    stoaramactl nas-space-audit report --connection-id 13 --json
    stoaramactl nas-space-audit report --connection-id 13 --download /new/path/report.jsonl.gz

The CLI creates a private output file exclusively and verifies its size/SHA and
path identity before success. A failed download leaves its partial output in place;
retry with a new destination. Failure cleanup never removes output paths that may
have been replaced concurrently.

An uploaded report is not a statement that every NAS file has an R2 media copy.
Join its per-file paths and lengths against current R2 listings/HEAD responses;
keep unknown mappings, mismatches and scope gaps explicit. Existing raw inventory
and stitched acknowledgements provide expected keys, not current existence proof.
No cleanup is enabled by this update.

SQLite path keys use reversible ASCII JSON strings. Report paths with invalid
UTF-8 filesystem bytes are displayed with escaped surrogate bytes and carry
`path_bytes_b64` (and `folder_bytes_b64` when needed) as their lossless identity.
Normal paths remain UTF-8 strings. Never use an escaped display path alone as a
delete target. No deletion is authorized or implemented by this audit.
