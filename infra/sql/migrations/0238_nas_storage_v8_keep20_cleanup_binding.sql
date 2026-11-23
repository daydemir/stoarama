-- The keep20 cleanup release is the low-memory cleanup artifact built with the
-- 20-day cleanup pin. This only admits one more exact literal and preserves
-- every earlier value and its evidence.
ALTER TABLE connections DROP CONSTRAINT connections_nas_storage_client_version_check,
 ADD CONSTRAINT connections_nas_storage_client_version_check
 CHECK(nas_storage_client_version IN ('','r2-copy-v5','r2-copy-v6','r2-copy-v7-certified-stitch','r2-copy-v8-low-memory','r2-copy-v8-low-memory-cleanup','r2-copy-v8-keep20-cleanup'));
