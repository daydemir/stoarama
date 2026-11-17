-- Existing storage receipts have no send-time freshness proof. Only the
-- heartbeat handler may bind a newly received sample to the reviewed v5 literal.
ALTER TABLE connections ADD COLUMN nas_storage_client_version text NOT NULL DEFAULT ''
 CHECK(nas_storage_client_version IN ('','r2-copy-v5'));
