BEGIN;

ALTER TABLE recording_assignments DROP CONSTRAINT IF EXISTS recording_assignments_mode_check;
ALTER TABLE recording_assignments DROP CONSTRAINT IF EXISTS chk_recording_assignments_mode;
ALTER TABLE recording_assignments
  ADD CONSTRAINT chk_recording_assignments_mode
  CHECK (mode IN ('youtube_live', 'youtube_relay', 'hls_live', 'image_poll', 'ffmpeg_direct'));

ALTER TABLE server_mode_capacity DROP CONSTRAINT IF EXISTS server_mode_capacity_mode_check;
ALTER TABLE server_mode_capacity DROP CONSTRAINT IF EXISTS chk_server_mode_capacity_mode;
ALTER TABLE server_mode_capacity
  ADD CONSTRAINT chk_server_mode_capacity_mode
  CHECK (mode IN ('youtube_live', 'youtube_relay', 'hls_live', 'image_poll', 'ffmpeg_direct'));

ALTER TABLE recording_process_runs DROP CONSTRAINT IF EXISTS recording_process_runs_mode_check;
ALTER TABLE recording_process_runs DROP CONSTRAINT IF EXISTS chk_recording_process_runs_mode;
ALTER TABLE recording_process_runs
  ADD CONSTRAINT chk_recording_process_runs_mode
  CHECK (mode IN ('youtube_live', 'youtube_relay', 'hls_live', 'image_poll', 'ffmpeg_direct'));

ALTER TABLE capture_worker_heartbeats DROP CONSTRAINT IF EXISTS capture_worker_heartbeats_mode_check;
ALTER TABLE capture_worker_heartbeats DROP CONSTRAINT IF EXISTS chk_capture_worker_heartbeats_mode;
ALTER TABLE capture_worker_heartbeats
  ADD CONSTRAINT chk_capture_worker_heartbeats_mode
  CHECK (mode IN ('youtube_live', 'youtube_relay', 'hls_live', 'image_poll', 'ffmpeg_direct'));

ALTER TABLE recording_mode_capacity DROP CONSTRAINT IF EXISTS recording_mode_capacity_mode_check;
ALTER TABLE recording_mode_capacity DROP CONSTRAINT IF EXISTS chk_recording_mode_capacity_mode;
ALTER TABLE recording_mode_capacity
  ADD CONSTRAINT chk_recording_mode_capacity_mode
  CHECK (mode IN ('youtube_live', 'youtube_relay', 'hls_live', 'image_poll', 'ffmpeg_direct'));

INSERT INTO recording_mode_capacity (mode, max_active)
VALUES ('youtube_relay', 0)
ON CONFLICT (mode) DO NOTHING;

CREATE TABLE IF NOT EXISTS youtube_relay_sources (
  server_id TEXT PRIMARY KEY,
  shard_id TEXT NOT NULL,
  max_active INTEGER NOT NULL CHECK (max_active > 0),
  draining BOOLEAN NOT NULL DEFAULT false,
  heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  lease_expires_at TIMESTAMPTZ NOT NULL,
  metadata_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_youtube_relay_sources_updated_at ON youtube_relay_sources;
CREATE TRIGGER trg_youtube_relay_sources_updated_at
BEFORE UPDATE ON youtube_relay_sources
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_youtube_relay_sources_lease
ON youtube_relay_sources (lease_expires_at DESC);

CREATE INDEX IF NOT EXISTS idx_youtube_relay_sources_shard_lease
ON youtube_relay_sources (shard_id, lease_expires_at DESC);

CREATE TABLE IF NOT EXISTS youtube_relay_routes (
  stream_id BIGINT PRIMARY KEY REFERENCES streams(id) ON DELETE CASCADE,
  source_server_id TEXT NOT NULL,
  sink_server_id TEXT NOT NULL,
  assignment_revision BIGINT NOT NULL CHECK (assignment_revision > 0),
  status TEXT NOT NULL CHECK (status IN ('assigned', 'source_ready', 'running', 'stopped', 'failed')),
  relay_pull_url TEXT NOT NULL DEFAULT '',
  error_text TEXT NOT NULL DEFAULT '',
  started_at TIMESTAMPTZ,
  stopped_at TIMESTAMPTZ,
  metadata_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_youtube_relay_routes_updated_at ON youtube_relay_routes;
CREATE TRIGGER trg_youtube_relay_routes_updated_at
BEFORE UPDATE ON youtube_relay_routes
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_youtube_relay_routes_source_status
ON youtube_relay_routes (source_server_id, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_youtube_relay_routes_sink_status
ON youtube_relay_routes (sink_server_id, status, updated_at DESC);

CREATE TABLE IF NOT EXISTS youtube_relay_events (
  id BIGSERIAL PRIMARY KEY,
  stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
  source_server_id TEXT,
  sink_server_id TEXT,
  status TEXT NOT NULL CHECK (status IN ('assigned', 'source_ready', 'running', 'stopped', 'failed')),
  actor TEXT NOT NULL DEFAULT '',
  reason TEXT NOT NULL DEFAULT '',
  error_text TEXT NOT NULL DEFAULT '',
  metadata_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_youtube_relay_events_stream_created
ON youtube_relay_events (stream_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_youtube_relay_events_source_created
ON youtube_relay_events (source_server_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_youtube_relay_events_sink_created
ON youtube_relay_events (sink_server_id, created_at DESC);

COMMIT;
