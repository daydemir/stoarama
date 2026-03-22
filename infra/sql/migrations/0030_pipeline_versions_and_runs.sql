BEGIN;

CREATE TABLE IF NOT EXISTS pipeline_versions (
  id BIGSERIAL PRIMARY KEY,
  pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
  version_id TEXT NOT NULL,
  runner_kind TEXT NOT NULL DEFAULT 'external',
  spec_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_by TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (pipeline_id, version_id)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_versions_pipeline_created
ON pipeline_versions (pipeline_id, created_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS pipeline_runs (
  id BIGSERIAL PRIMARY KEY,
  pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
  pipeline_version_id BIGINT NOT NULL REFERENCES pipeline_versions(id) ON DELETE RESTRICT,
  label TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'pending',
  worker_kind TEXT NOT NULL DEFAULT 'external',
  selector_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_by TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  CONSTRAINT pipeline_runs_status_check
    CHECK (status IN ('pending', 'running', 'completed', 'completed_with_errors', 'canceled'))
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_pipeline_created
ON pipeline_runs (pipeline_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status_created
ON pipeline_runs (status, created_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS pipeline_run_targets (
  id BIGSERIAL PRIMARY KEY,
  run_id BIGINT NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
  frame_id BIGINT NOT NULL REFERENCES frames(id) ON DELETE CASCADE,
  stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
  status TEXT NOT NULL DEFAULT 'pending',
  claim_id BIGINT REFERENCES inference_claims(id) ON DELETE SET NULL,
  claimed_by TEXT NOT NULL DEFAULT '',
  lease_expires_at TIMESTAMPTZ,
  result_id BIGINT REFERENCES inference_results(id) ON DELETE SET NULL,
  error_text TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pipeline_run_targets_status_check
    CHECK (status IN ('pending', 'leased', 'completed', 'error', 'abandoned', 'skipped')),
  UNIQUE (run_id, frame_id)
);

DROP TRIGGER IF EXISTS trg_pipeline_run_targets_updated_at ON pipeline_run_targets;
CREATE TRIGGER trg_pipeline_run_targets_updated_at
BEFORE UPDATE ON pipeline_run_targets
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_pipeline_run_targets_run_status
ON pipeline_run_targets (run_id, status, lease_expires_at, frame_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_targets_stream
ON pipeline_run_targets (stream_id, run_id, frame_id);

ALTER TABLE inference_claims
  ADD COLUMN IF NOT EXISTS pipeline_version_id BIGINT REFERENCES pipeline_versions(id) ON DELETE SET NULL;

ALTER TABLE inference_claims
  ADD COLUMN IF NOT EXISTS pipeline_run_id BIGINT REFERENCES pipeline_runs(id) ON DELETE SET NULL;

ALTER TABLE inference_results
  ADD COLUMN IF NOT EXISTS pipeline_version_id BIGINT REFERENCES pipeline_versions(id) ON DELETE SET NULL;

ALTER TABLE inference_results
  ADD COLUMN IF NOT EXISTS pipeline_run_id BIGINT REFERENCES pipeline_runs(id) ON DELETE SET NULL;

DROP INDEX IF EXISTS uniq_inference_claims_active;

CREATE UNIQUE INDEX IF NOT EXISTS uniq_inference_claims_active_pipeline
ON inference_claims (pipeline_id, frame_id)
WHERE status = 'leased' AND pipeline_run_id IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uniq_inference_claims_active_run
ON inference_claims (pipeline_run_id, frame_id)
WHERE status = 'leased' AND pipeline_run_id IS NOT NULL;

ALTER TABLE inference_results
  DROP CONSTRAINT IF EXISTS inference_results_pipeline_id_frame_id_revision_key;

CREATE UNIQUE INDEX IF NOT EXISTS uniq_inference_results_revision_pipeline
ON inference_results (pipeline_id, frame_id, revision)
WHERE pipeline_run_id IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uniq_inference_results_revision_run
ON inference_results (pipeline_run_id, frame_id, revision)
WHERE pipeline_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_inference_results_run_frame
ON inference_results (pipeline_run_id, frame_id, created_at DESC)
WHERE pipeline_run_id IS NOT NULL;

COMMIT;
