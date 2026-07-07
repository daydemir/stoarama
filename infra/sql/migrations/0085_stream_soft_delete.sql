BEGIN;

ALTER TABLE streams
  ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS deleted_by_account_id BIGINT REFERENCES accounts(id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS deleted_reason TEXT NOT NULL DEFAULT '';

ALTER TABLE nodes
  ADD COLUMN IF NOT EXISTS survey_enabled BOOLEAN NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS idx_streams_not_deleted_id
ON streams(id)
WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_streams_deleted_at
ON streams(deleted_at)
WHERE deleted_at IS NOT NULL;

COMMIT;
