BEGIN;

CREATE TABLE IF NOT EXISTS stream_pipeline_settings (
  stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
  pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
  enabled BOOLEAN NOT NULL DEFAULT true,
  updated_by TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (stream_id, pipeline_id)
);

DROP TRIGGER IF EXISTS trg_stream_pipeline_settings_updated_at ON stream_pipeline_settings;
CREATE TRIGGER trg_stream_pipeline_settings_updated_at
BEFORE UPDATE ON stream_pipeline_settings
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_stream_pipeline_settings_pipeline_enabled
ON stream_pipeline_settings (pipeline_id, enabled, stream_id);

CREATE TABLE IF NOT EXISTS processing_worker_heartbeats (
  worker_id TEXT NOT NULL,
  worker_kind TEXT NOT NULL CHECK (worker_kind IN ('capture', 'inference', 'inference_box', 'other')),
  mode TEXT NOT NULL DEFAULT '',
  pipeline_id TEXT NOT NULL DEFAULT '',
  metadata_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  lease_expires_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (worker_id, worker_kind, mode, pipeline_id)
);

DROP TRIGGER IF EXISTS trg_processing_worker_heartbeats_updated_at ON processing_worker_heartbeats;
CREATE TRIGGER trg_processing_worker_heartbeats_updated_at
BEFORE UPDATE ON processing_worker_heartbeats
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_processing_worker_heartbeats_expiry
ON processing_worker_heartbeats (lease_expires_at DESC);

CREATE INDEX IF NOT EXISTS idx_processing_worker_heartbeats_kind
ON processing_worker_heartbeats (worker_kind, pipeline_id, lease_expires_at DESC);

COMMIT;
