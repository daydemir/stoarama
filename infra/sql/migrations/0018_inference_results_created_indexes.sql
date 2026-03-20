BEGIN;

CREATE INDEX IF NOT EXISTS idx_inference_results_created_desc
ON inference_results (created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_inference_results_status_created_desc
ON inference_results (status, created_at DESC, id DESC);

COMMIT;
