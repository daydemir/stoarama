BEGIN;

CREATE TABLE IF NOT EXISTS eval_suites (
  id BIGSERIAL PRIMARY KEY,
  owner_account_id BIGINT REFERENCES accounts(id) ON DELETE RESTRICT,
  slug TEXT NOT NULL,
  title TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  source_kind TEXT NOT NULL DEFAULT 'stoarama',
  primary_metric TEXT NOT NULL DEFAULT 'detection_f1',
  source_url TEXT NOT NULL DEFAULT '',
  metadata_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_by TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT eval_suites_source_kind_check
    CHECK (source_kind IN ('public', 'stoarama', 'hybrid')),
  UNIQUE (owner_account_id, slug)
);

DROP TRIGGER IF EXISTS trg_eval_suites_updated_at ON eval_suites;
CREATE TRIGGER trg_eval_suites_updated_at
BEFORE UPDATE ON eval_suites
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_eval_suites_owner_slug
ON eval_suites (owner_account_id, slug, created_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS eval_suite_items (
  id BIGSERIAL PRIMARY KEY,
  suite_id BIGINT NOT NULL REFERENCES eval_suites(id) ON DELETE CASCADE,
  frame_id BIGINT REFERENCES frames(id) ON DELETE SET NULL,
  item_key TEXT NOT NULL,
  split TEXT NOT NULL DEFAULT 'benchmark',
  source_label TEXT NOT NULL DEFAULT '',
  source_url TEXT NOT NULL DEFAULT '',
  metadata_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (suite_id, item_key)
);

DROP TRIGGER IF EXISTS trg_eval_suite_items_updated_at ON eval_suite_items;
CREATE TRIGGER trg_eval_suite_items_updated_at
BEFORE UPDATE ON eval_suite_items
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_eval_suite_items_suite_split
ON eval_suite_items (suite_id, split, id DESC);

CREATE INDEX IF NOT EXISTS idx_eval_suite_items_frame
ON eval_suite_items (frame_id, suite_id)
WHERE frame_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS eval_annotations (
  id BIGSERIAL PRIMARY KEY,
  suite_item_id BIGINT NOT NULL REFERENCES eval_suite_items(id) ON DELETE CASCADE,
  annotation_kind TEXT NOT NULL DEFAULT 'bbox',
  label_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_by TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT eval_annotations_kind_check
    CHECK (annotation_kind IN ('bbox', 'track', 'attribute'))
);

CREATE INDEX IF NOT EXISTS idx_eval_annotations_item_kind
ON eval_annotations (suite_item_id, annotation_kind, id DESC);

CREATE TABLE IF NOT EXISTS pipeline_experiments (
  id BIGSERIAL PRIMARY KEY,
  owner_account_id BIGINT REFERENCES accounts(id) ON DELETE RESTRICT,
  pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
  slug TEXT NOT NULL,
  title TEXT NOT NULL,
  goal_text TEXT NOT NULL DEFAULT '',
  primary_metric TEXT NOT NULL DEFAULT 'detection_f1',
  active BOOLEAN NOT NULL DEFAULT true,
  metadata_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_by TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (owner_account_id, pipeline_id, slug)
);

DROP TRIGGER IF EXISTS trg_pipeline_experiments_updated_at ON pipeline_experiments;
CREATE TRIGGER trg_pipeline_experiments_updated_at
BEFORE UPDATE ON pipeline_experiments
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_pipeline_experiments_owner_pipeline
ON pipeline_experiments (owner_account_id, pipeline_id, created_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS pipeline_experiment_iterations (
  id BIGSERIAL PRIMARY KEY,
  experiment_id BIGINT NOT NULL REFERENCES pipeline_experiments(id) ON DELETE CASCADE,
  candidate_pipeline_version_id BIGINT REFERENCES pipeline_versions(id) ON DELETE SET NULL,
  baseline_pipeline_version_id BIGINT REFERENCES pipeline_versions(id) ON DELETE SET NULL,
  iteration_index INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  hypothesis_text TEXT NOT NULL DEFAULT '',
  change_summary TEXT NOT NULL DEFAULT '',
  change_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  result_classification TEXT NOT NULL DEFAULT 'pending',
  primary_metric_before DOUBLE PRECISION,
  primary_metric_after DOUBLE PRECISION,
  primary_metric_delta DOUBLE PRECISION,
  log_url TEXT NOT NULL DEFAULT '',
  artifact_url TEXT NOT NULL DEFAULT '',
  metadata_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_by TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pipeline_experiment_iterations_status_check
    CHECK (status IN ('pending', 'running', 'completed', 'completed_with_errors', 'error', 'canceled')),
  CONSTRAINT pipeline_experiment_iterations_result_classification_check
    CHECK (result_classification IN ('pending', 'better', 'neutral', 'worse', 'error')),
  UNIQUE (experiment_id, iteration_index)
);

DROP TRIGGER IF EXISTS trg_pipeline_experiment_iterations_updated_at ON pipeline_experiment_iterations;
CREATE TRIGGER trg_pipeline_experiment_iterations_updated_at
BEFORE UPDATE ON pipeline_experiment_iterations
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_pipeline_experiment_iterations_experiment
ON pipeline_experiment_iterations (experiment_id, iteration_index DESC, id DESC);

CREATE TABLE IF NOT EXISTS pipeline_experiment_iteration_runs (
  iteration_id BIGINT NOT NULL REFERENCES pipeline_experiment_iterations(id) ON DELETE CASCADE,
  pipeline_run_id BIGINT NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
  run_role TEXT NOT NULL DEFAULT 'candidate',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pipeline_experiment_iteration_runs_role_check
    CHECK (run_role IN ('baseline', 'candidate', 'support')),
  PRIMARY KEY (iteration_id, pipeline_run_id)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_experiment_iteration_runs_role
ON pipeline_experiment_iteration_runs (pipeline_run_id, run_role, iteration_id);

CREATE TABLE IF NOT EXISTS pipeline_experiment_iteration_suites (
  iteration_id BIGINT NOT NULL REFERENCES pipeline_experiment_iterations(id) ON DELETE CASCADE,
  suite_id BIGINT NOT NULL REFERENCES eval_suites(id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (iteration_id, suite_id)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_experiment_iteration_suites_suite
ON pipeline_experiment_iteration_suites (suite_id, iteration_id);

CREATE TABLE IF NOT EXISTS pipeline_eval_metrics (
  id BIGSERIAL PRIMARY KEY,
  experiment_iteration_id BIGINT REFERENCES pipeline_experiment_iterations(id) ON DELETE SET NULL,
  suite_id BIGINT NOT NULL REFERENCES eval_suites(id) ON DELETE CASCADE,
  pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
  pipeline_version_id BIGINT REFERENCES pipeline_versions(id) ON DELETE SET NULL,
  pipeline_run_id BIGINT REFERENCES pipeline_runs(id) ON DELETE SET NULL,
  metric_name TEXT NOT NULL,
  split TEXT NOT NULL DEFAULT '',
  metric_value DOUBLE PRECISION NOT NULL,
  metadata_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_eval_metrics_suite_metric
ON pipeline_eval_metrics (suite_id, metric_name, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_eval_metrics_iteration_metric
ON pipeline_eval_metrics (experiment_iteration_id, metric_name, created_at DESC, id DESC)
WHERE experiment_iteration_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_pipeline_eval_metrics_pipeline_metric
ON pipeline_eval_metrics (pipeline_id, metric_name, created_at DESC, id DESC);

COMMIT;
