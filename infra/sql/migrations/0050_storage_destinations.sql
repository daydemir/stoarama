BEGIN;

-- BYO storage: an account registers one or more S3-compatible destinations the
-- shared recording server writes recordings to. The operator no longer stores
-- recordings centrally; each destination carries its own endpoint/bucket/creds.
-- The secret access key is stored reversibly encrypted (AES-256-GCM, see
-- internal/secretbox) because the API must sign presign requests with it.
CREATE TABLE IF NOT EXISTS storage_destinations (
  id BIGSERIAL PRIMARY KEY,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  provider TEXT NOT NULL DEFAULT 's3_compatible',
  endpoint TEXT NOT NULL,
  region TEXT NOT NULL,
  bucket TEXT NOT NULL,
  key_prefix TEXT NOT NULL DEFAULT '',
  access_key_id TEXT NOT NULL,
  secret_access_key_enc BYTEA NOT NULL,
  status TEXT NOT NULL DEFAULT 'unverified' CHECK (status IN ('unverified', 'verified', 'failed')),
  last_verify_error TEXT NOT NULL DEFAULT '',
  verified_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_storage_destinations_updated_at ON storage_destinations;
CREATE TRIGGER trg_storage_destinations_updated_at
BEFORE UPDATE ON storage_destinations
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_storage_destinations_account_created
ON storage_destinations (account_id, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_storage_destinations_account_name
ON storage_destinations (account_id, lower(name));

COMMIT;
