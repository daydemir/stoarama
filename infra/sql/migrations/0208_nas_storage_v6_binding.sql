-- A new immutable artifact identity with the unchanged nonce-bound storage
-- contract. Preserve existing v5/unproven evidence; never promote any row.
ALTER TABLE connections
 DROP CONSTRAINT connections_nas_storage_client_version_check,
 DROP CONSTRAINT nas_storage_budget_pair,
 ADD CONSTRAINT connections_nas_storage_client_version_check CHECK(nas_storage_client_version IN ('','r2-copy-v5','r2-copy-v6')),
 ADD CONSTRAINT nas_storage_budget_pair CHECK((nas_storage_client_version<>'')=(nas_storage_budget_at IS NOT NULL));
