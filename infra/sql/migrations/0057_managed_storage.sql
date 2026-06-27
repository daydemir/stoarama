BEGIN;

-- Managed storage as a real destination row holding OPERATOR creds + a unique
-- per-account key_prefix. Distinguished from BYO only by `managed`.
ALTER TABLE storage_destinations ADD COLUMN managed BOOLEAN NOT NULL DEFAULT false;

-- One managed row per account; makes auto-provision idempotent via ON CONFLICT.
CREATE UNIQUE INDEX idx_storage_destinations_managed_account
  ON storage_destinations (account_id) WHERE managed;

-- Mark a clip whose R2 object has been deleted by the purge job; excluded from
-- the daily byte snapshot so purged objects stop counting toward gb_month.
ALTER TABLE recording_clips ADD COLUMN purged_at TIMESTAMPTZ;

-- Daily SUM(size_bytes) of non-purged managed clips, per managed account.
CREATE TABLE account_storage_snapshots (
  account_id    BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  snapshot_date DATE   NOT NULL,
  bytes_stored  BIGINT NOT NULL CHECK (bytes_stored >= 0),
  PRIMARY KEY (account_id, snapshot_date)
);

COMMIT;
