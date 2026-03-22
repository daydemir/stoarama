BEGIN;

ALTER TABLE pipelines
  ADD COLUMN IF NOT EXISTS owner_account_id BIGINT REFERENCES accounts(id) ON DELETE RESTRICT;

ALTER TABLE pipeline_versions
  ADD COLUMN IF NOT EXISTS owner_account_id BIGINT REFERENCES accounts(id) ON DELETE RESTRICT;

ALTER TABLE pipeline_runs
  ADD COLUMN IF NOT EXISTS owner_account_id BIGINT REFERENCES accounts(id) ON DELETE RESTRICT;

UPDATE pipelines
SET owner_account_id = COALESCE(
  owner_account_id,
  (
    SELECT id
    FROM accounts
    WHERE role='admin' AND status='active'
    ORDER BY id ASC
    LIMIT 1
  )
)
WHERE owner_account_id IS NULL;

UPDATE pipeline_versions pv
SET owner_account_id = COALESCE(
  pv.owner_account_id,
  p.owner_account_id,
  (
    SELECT id
    FROM accounts
    WHERE role='admin' AND status='active'
    ORDER BY id ASC
    LIMIT 1
  )
)
FROM pipelines p
WHERE pv.pipeline_id = p.id
  AND pv.owner_account_id IS NULL;

UPDATE pipeline_runs pr
SET owner_account_id = COALESCE(
  pr.owner_account_id,
  p.owner_account_id,
  (
    SELECT id
    FROM accounts
    WHERE role='admin' AND status='active'
    ORDER BY id ASC
    LIMIT 1
  )
)
FROM pipelines p
WHERE pr.pipeline_id = p.id
  AND pr.owner_account_id IS NULL;

CREATE INDEX IF NOT EXISTS idx_pipelines_owner_account
ON pipelines (owner_account_id, id);

CREATE INDEX IF NOT EXISTS idx_pipeline_versions_owner_account
ON pipeline_versions (owner_account_id, pipeline_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_owner_account
ON pipeline_runs (owner_account_id, created_at DESC, id DESC);

COMMIT;
