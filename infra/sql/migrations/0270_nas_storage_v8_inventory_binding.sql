-- r2-copy-v8-inventory: the output-restore line that also leases read-only
-- stream-day NAS inventories (0272); its other capabilities are exactly
-- output-restore's. r2-copy-v8-output-restore stays admitted until the NAS updates.
ALTER TABLE connections DROP CONSTRAINT connections_nas_storage_client_version_check,
 ADD CONSTRAINT connections_nas_storage_client_version_check
 CHECK(nas_storage_client_version IN ('','r2-copy-v5','r2-copy-v6','r2-copy-v7-certified-stitch','r2-copy-v8-low-memory','r2-copy-v8-low-memory-cleanup','r2-copy-v8-keep20-cleanup','r2-copy-v8-fast-copy','r2-copy-v8-tunable-copy','r2-copy-v8-tunable-day','r2-copy-v8-tunable-modes','r2-copy-v8-ke-day','r2-copy-v8-output-restore','r2-copy-v8-inventory'));
