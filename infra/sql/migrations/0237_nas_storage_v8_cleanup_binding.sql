-- The cleanup-capable low-memory artifact keeps the identical nonce-bound
-- storage budget contract. 0235 stays deployed as written; this only admits
-- one more exact literal and preserves every earlier value and its evidence.
ALTER TABLE connections DROP CONSTRAINT connections_nas_storage_client_version_check,
 ADD CONSTRAINT connections_nas_storage_client_version_check
 CHECK(nas_storage_client_version IN ('','r2-copy-v5','r2-copy-v6','r2-copy-v7-certified-stitch','r2-copy-v8-low-memory','r2-copy-v8-low-memory-cleanup'));
