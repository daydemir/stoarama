-- Only a storage update bound to the current server-issued challenge earns
-- budget time. Display receipts and the next challenge never refresh that time.
ALTER TABLE connections
 ADD COLUMN nas_storage_client_version text NOT NULL DEFAULT ''
  CHECK(nas_storage_client_version IN ('','r2-copy-v5')),
 ADD COLUMN nas_storage_challenge_nonce text NOT NULL DEFAULT ''
  CHECK(nas_storage_challenge_nonce='' OR nas_storage_challenge_nonce ~ '^[0-9a-f]{64}$'),
 ADD COLUMN nas_storage_challenge_issued_at timestamptz,
 ADD COLUMN nas_storage_budget_at timestamptz,
 ADD CONSTRAINT nas_storage_challenge_pair CHECK
  ((nas_storage_challenge_nonce='')=(nas_storage_challenge_issued_at IS NULL)),
 ADD CONSTRAINT nas_storage_budget_pair CHECK
  ((nas_storage_client_version='r2-copy-v5')=(nas_storage_budget_at IS NOT NULL));
