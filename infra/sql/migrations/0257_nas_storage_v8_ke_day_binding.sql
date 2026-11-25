-- r2-copy-v8-ke-day: the tunable-modes line that also accepts Keep Everything
-- day permits (names mode only). r2-copy-v8-tunable-modes stays admitted until
-- the NAS updates.
ALTER TABLE connections DROP CONSTRAINT connections_nas_storage_client_version_check,
 ADD CONSTRAINT connections_nas_storage_client_version_check
 CHECK(nas_storage_client_version IN ('','r2-copy-v5','r2-copy-v6','r2-copy-v7-certified-stitch','r2-copy-v8-low-memory','r2-copy-v8-low-memory-cleanup','r2-copy-v8-keep20-cleanup','r2-copy-v8-fast-copy','r2-copy-v8-tunable-copy','r2-copy-v8-tunable-day','r2-copy-v8-tunable-modes','r2-copy-v8-ke-day'));
