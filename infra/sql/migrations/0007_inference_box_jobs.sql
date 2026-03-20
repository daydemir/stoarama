BEGIN;

ALTER TABLE inference_results
  DROP CONSTRAINT IF EXISTS inference_results_status_check;

ALTER TABLE inference_results
  ADD CONSTRAINT inference_results_status_check
  CHECK (status IN ('queued_boxed', 'success', 'error'));

CREATE TABLE IF NOT EXISTS inference_box_jobs (
  id BIGSERIAL PRIMARY KEY,
  inference_result_id BIGINT NOT NULL UNIQUE REFERENCES inference_results(id) ON DELETE CASCADE,
  status TEXT NOT NULL CHECK (status IN ('pending', 'leased', 'done', 'error')),
  lease_owner TEXT,
  lease_expires_at TIMESTAMPTZ,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 8 CHECK (max_attempts > 0),
  next_retry_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  error_text TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_inference_box_jobs_updated_at ON inference_box_jobs;
CREATE TRIGGER trg_inference_box_jobs_updated_at
BEFORE UPDATE ON inference_box_jobs
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_inference_box_jobs_sched
ON inference_box_jobs(status, next_retry_at, id);

CREATE INDEX IF NOT EXISTS idx_inference_box_jobs_lease_exp
ON inference_box_jobs(lease_expires_at)
WHERE status = 'leased';

WITH candidates AS (
  SELECT ir.id
  FROM inference_results ir
  WHERE ir.status = 'success'
    AND ir.boxed_media_object_id IS NULL
    AND EXISTS (
      SELECT 1
      FROM detections d
      WHERE d.inference_result_id = ir.id
    )
), updated AS (
  UPDATE inference_results ir
  SET status = 'queued_boxed'
  FROM candidates c
  WHERE ir.id = c.id
  RETURNING ir.id
)
INSERT INTO inference_box_jobs (
  inference_result_id, status, max_attempts
)
SELECT u.id, 'pending', 8
FROM updated u
ON CONFLICT (inference_result_id) DO NOTHING;

COMMIT;
