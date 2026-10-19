The NAS space audit scans all entries visible under the app's clips mount,
including hidden files and joined/. It never opens media payloads, deletes media,
moves media, follows symlinks, or crosses nested filesystems. Other shares,
host-only folders, snapshots and recycle bins outside this mount are unobservable.

The one-shot `nas-space-stat-v1-20261019` audit starts after the client update,
resumes from a separate SQLite checkpoint under the app state directory, and
reports progress about once per minute. Its rate is capped near 2,000 stats/s.
The app state directory also holds its compressed per-entry metadata report.
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

An uploaded report is not a statement that every NAS file has an R2 media copy.
Join its per-file paths and lengths against current R2 listings/HEAD responses;
keep unknown mappings, mismatches and scope gaps explicit. Existing raw inventory
and stitched acknowledgements provide expected keys, not current existence proof.
No cleanup is enabled by this update.
