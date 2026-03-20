BEGIN;

CREATE TABLE IF NOT EXISTS recording_assignments (
  stream_id BIGINT PRIMARY KEY REFERENCES streams(id) ON DELETE CASCADE,
  server_id TEXT NOT NULL,
  mode TEXT NOT NULL CHECK (mode IN ('youtube_live', 'hls_live', 'image_poll', 'ffmpeg_direct')),
  assignment_revision BIGINT NOT NULL DEFAULT 1 CHECK (assignment_revision > 0),
  assigned_by TEXT NOT NULL DEFAULT '',
  assigned_reason TEXT NOT NULL DEFAULT '',
  assigned_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_recording_assignments_updated_at ON recording_assignments;
CREATE TRIGGER trg_recording_assignments_updated_at
BEFORE UPDATE ON recording_assignments
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_recording_assignments_server_mode
ON recording_assignments (server_id, mode, stream_id);

CREATE TABLE IF NOT EXISTS server_mode_capacity (
  server_id TEXT NOT NULL,
  mode TEXT NOT NULL CHECK (mode IN ('youtube_live', 'hls_live', 'image_poll', 'ffmpeg_direct')),
  max_active INTEGER NOT NULL CHECK (max_active >= 0),
  draining BOOLEAN NOT NULL DEFAULT false,
  heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  lease_expires_at TIMESTAMPTZ NOT NULL,
  metadata_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (server_id, mode)
);

DROP TRIGGER IF EXISTS trg_server_mode_capacity_updated_at ON server_mode_capacity;
CREATE TRIGGER trg_server_mode_capacity_updated_at
BEFORE UPDATE ON server_mode_capacity
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_server_mode_capacity_expiry
ON server_mode_capacity (lease_expires_at DESC);

CREATE INDEX IF NOT EXISTS idx_server_mode_capacity_mode_expiry
ON server_mode_capacity (mode, lease_expires_at DESC);

CREATE TABLE IF NOT EXISTS recording_assignment_events (
  id BIGSERIAL PRIMARY KEY,
  stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
  server_id TEXT,
  mode TEXT,
  assignment_revision BIGINT,
  event_type TEXT NOT NULL CHECK (event_type IN ('assign', 'unassign', 'reassign')),
  actor TEXT NOT NULL DEFAULT '',
  reason TEXT NOT NULL DEFAULT '',
  metadata_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_recording_assignment_events_stream_created
ON recording_assignment_events (stream_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_recording_assignment_events_server_created
ON recording_assignment_events (server_id, created_at DESC);

ALTER TABLE recording_process_runs
  ADD COLUMN IF NOT EXISTS assignment_revision BIGINT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_recording_process_runs_stream_revision
ON recording_process_runs (stream_id, assignment_revision, started_at DESC);

COMMIT;
