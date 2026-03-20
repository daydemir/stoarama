BEGIN;

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS streams (
  id BIGSERIAL PRIMARY KEY,
  provider TEXT NOT NULL,
  external_id TEXT NOT NULL,
  name TEXT NOT NULL,
  slug TEXT NOT NULL,
  stream_url TEXT NOT NULL,
  source_page_url TEXT NOT NULL DEFAULT '',
  lat DOUBLE PRECISION,
  lon DOUBLE PRECISION,
  location_text TEXT NOT NULL DEFAULT '',
  metadata_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  enabled BOOLEAN NOT NULL DEFAULT true,
  capture_interval_sec INTEGER NOT NULL DEFAULT 600 CHECK (capture_interval_sec > 0),
  priority INTEGER NOT NULL DEFAULT 0,
  tags TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  shortlist_flag BOOLEAN NOT NULL DEFAULT false,
  excluded_flag BOOLEAN NOT NULL DEFAULT false,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (provider, external_id),
  UNIQUE (slug)
);

DROP TRIGGER IF EXISTS trg_streams_updated_at ON streams;
CREATE TRIGGER trg_streams_updated_at
BEFORE UPDATE ON streams
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS capture_jobs (
  id BIGSERIAL PRIMARY KEY,
  stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
  scheduled_for TIMESTAMPTZ NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('pending', 'leased', 'done', 'error')),
  lease_owner TEXT,
  lease_expires_at TIMESTAMPTZ,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts > 0),
  error_text TEXT,
  idempotency_key TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (idempotency_key)
);

DROP TRIGGER IF EXISTS trg_capture_jobs_updated_at ON capture_jobs;
CREATE TRIGGER trg_capture_jobs_updated_at
BEFORE UPDATE ON capture_jobs
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_capture_jobs_status_scheduled_for
ON capture_jobs(status, scheduled_for);

CREATE TABLE IF NOT EXISTS media_objects (
  id BIGSERIAL PRIMARY KEY,
  storage_provider TEXT NOT NULL CHECK (storage_provider IN ('r2')),
  bucket TEXT NOT NULL,
  object_key TEXT NOT NULL,
  mime_type TEXT NOT NULL,
  size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
  etag TEXT NOT NULL DEFAULT '',
  sha256 TEXT,
  width INTEGER,
  height INTEGER,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (bucket, object_key)
);

CREATE TABLE IF NOT EXISTS frames (
  id BIGSERIAL PRIMARY KEY,
  stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
  capture_job_id BIGINT REFERENCES capture_jobs(id) ON DELETE SET NULL,
  captured_at TIMESTAMPTZ NOT NULL,
  raw_media_object_id BIGINT REFERENCES media_objects(id) ON DELETE SET NULL,
  capture_status TEXT NOT NULL CHECK (capture_status IN ('success', 'error')),
  capture_error TEXT,
  source_kind TEXT NOT NULL CHECK (source_kind IN ('live', 'snapshot_url')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (stream_id, captured_at, raw_media_object_id)
);

CREATE INDEX IF NOT EXISTS idx_frames_stream_id_captured_at
ON frames(stream_id, captured_at DESC);

CREATE INDEX IF NOT EXISTS idx_frames_capture_status
ON frames(capture_status, captured_at DESC);

CREATE TABLE IF NOT EXISTS pipelines (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL CHECK (kind IN ('detector')),
  spec_jsonb JSONB NOT NULL,
  active BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_pipelines_updated_at ON pipelines;
CREATE TRIGGER trg_pipelines_updated_at
BEFORE UPDATE ON pipelines
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS inference_claims (
  id BIGSERIAL PRIMARY KEY,
  pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
  frame_id BIGINT NOT NULL REFERENCES frames(id) ON DELETE CASCADE,
  claimed_by TEXT NOT NULL,
  lease_expires_at TIMESTAMPTZ NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('leased', 'completed', 'abandoned')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_inference_claims_updated_at ON inference_claims;
CREATE TRIGGER trg_inference_claims_updated_at
BEFORE UPDATE ON inference_claims
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE UNIQUE INDEX IF NOT EXISTS uniq_inference_claims_active
ON inference_claims(pipeline_id, frame_id)
WHERE status = 'leased';

CREATE INDEX IF NOT EXISTS idx_inference_claims_status_expiry
ON inference_claims(status, lease_expires_at);

CREATE TABLE IF NOT EXISTS inference_results (
  id BIGSERIAL PRIMARY KEY,
  pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
  frame_id BIGINT NOT NULL REFERENCES frames(id) ON DELETE CASCADE,
  revision INTEGER NOT NULL CHECK (revision > 0),
  status TEXT NOT NULL CHECK (status IN ('success', 'error')),
  summary_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  boxed_media_object_id BIGINT REFERENCES media_objects(id) ON DELETE SET NULL,
  raw_output_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  error_text TEXT,
  runner_info_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (pipeline_id, frame_id, revision)
);

CREATE INDEX IF NOT EXISTS idx_inference_results_pipeline_frame
ON inference_results(pipeline_id, frame_id, created_at DESC);

CREATE TABLE IF NOT EXISTS detections (
  id BIGSERIAL PRIMARY KEY,
  inference_result_id BIGINT NOT NULL REFERENCES inference_results(id) ON DELETE CASCADE,
  class_id TEXT,
  class_name TEXT NOT NULL,
  confidence DOUBLE PRECISION NOT NULL,
  x1 DOUBLE PRECISION NOT NULL,
  y1 DOUBLE PRECISION NOT NULL,
  x2 DOUBLE PRECISION NOT NULL,
  y2 DOUBLE PRECISION NOT NULL,
  area_px DOUBLE PRECISION NOT NULL,
  extra_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_detections_result_id
ON detections(inference_result_id);

CREATE TABLE IF NOT EXISTS stream_health (
  stream_id BIGINT PRIMARY KEY REFERENCES streams(id) ON DELETE CASCADE,
  captures_total BIGINT NOT NULL DEFAULT 0,
  captures_success BIGINT NOT NULL DEFAULT 0,
  captures_error BIGINT NOT NULL DEFAULT 0,
  last_error_at TIMESTAMPTZ,
  last_error_text TEXT,
  last_capture_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_stream_health_updated_at ON stream_health;
CREATE TRIGGER trg_stream_health_updated_at
BEFORE UPDATE ON stream_health
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS upload_intents (
  id UUID PRIMARY KEY,
  kind TEXT NOT NULL CHECK (kind IN ('boxed')),
  bucket TEXT NOT NULL,
  object_key TEXT NOT NULL,
  mime_type TEXT NOT NULL,
  expected_size_bytes BIGINT,
  expected_etag TEXT,
  status TEXT NOT NULL CHECK (status IN ('pending', 'consumed', 'expired')),
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (bucket, object_key)
);

CREATE INDEX IF NOT EXISTS idx_upload_intents_status_expiry
ON upload_intents(status, expires_at);

CREATE TABLE IF NOT EXISTS api_idempotency (
  endpoint TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(endpoint, idempotency_key)
);

CREATE OR REPLACE VIEW v_stream_latest_frame AS
SELECT DISTINCT ON (f.stream_id)
  f.stream_id,
  f.id AS frame_id,
  f.captured_at,
  f.capture_status,
  f.capture_error,
  f.source_kind,
  f.raw_media_object_id
FROM frames f
ORDER BY f.stream_id, f.captured_at DESC, f.id DESC;

CREATE OR REPLACE VIEW v_stream_latest_inference_per_pipeline AS
SELECT x.*
FROM (
  SELECT
    f.stream_id,
    ir.pipeline_id,
    ir.id AS inference_result_id,
    ir.frame_id,
    ir.revision,
    ir.status,
    ir.summary_jsonb,
    ir.boxed_media_object_id,
    ir.finished_at,
    ir.created_at,
    ROW_NUMBER() OVER (
      PARTITION BY f.stream_id, ir.pipeline_id
      ORDER BY ir.created_at DESC, ir.id DESC
    ) AS rn
  FROM inference_results ir
  JOIN frames f ON f.id = ir.frame_id
) x
WHERE x.rn = 1;

CREATE OR REPLACE VIEW v_stream_overview AS
SELECT
  s.id,
  s.provider,
  s.external_id,
  s.name,
  s.slug,
  s.enabled,
  s.shortlist_flag,
  s.excluded_flag,
  s.capture_interval_sec,
  s.priority,
  s.tags,
  lf.frame_id AS latest_frame_id,
  lf.captured_at AS latest_captured_at,
  lf.capture_status AS latest_capture_status,
  lf.capture_error AS latest_capture_error,
  sh.captures_total,
  sh.captures_success,
  sh.captures_error,
  CASE
    WHEN COALESCE(sh.captures_total, 0) = 0 THEN NULL
    ELSE (sh.captures_success::DOUBLE PRECISION / sh.captures_total::DOUBLE PRECISION)
  END AS capture_success_rate
FROM streams s
LEFT JOIN v_stream_latest_frame lf ON lf.stream_id = s.id
LEFT JOIN stream_health sh ON sh.stream_id = s.id;

COMMIT;
