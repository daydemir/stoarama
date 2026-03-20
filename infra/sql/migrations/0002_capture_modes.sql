ALTER TABLE streams
  ADD COLUMN IF NOT EXISTS recording_enabled BOOLEAN NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS capture_mode TEXT NOT NULL DEFAULT 'auto',
  ADD COLUMN IF NOT EXISTS capture_config_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS capture_disabled_reason TEXT;

UPDATE streams
SET recording_enabled = shortlist_flag
WHERE recording_enabled = false
  AND shortlist_flag = true;

CREATE TABLE IF NOT EXISTS stream_capture_runtime (
  stream_id BIGINT PRIMARY KEY REFERENCES streams(id) ON DELETE CASCADE,
  effective_mode TEXT,
  resolved_url TEXT,
  status TEXT NOT NULL CHECK (status IN ('idle','resolving','running','unsupported','error','stopped')),
  last_resolved_at TIMESTAMPTZ,
  last_frame_at TIMESTAMPTZ,
  consecutive_errors INTEGER NOT NULL DEFAULT 0,
  last_error_text TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_stream_capture_runtime_updated_at ON stream_capture_runtime;
CREATE TRIGGER trg_stream_capture_runtime_updated_at
BEFORE UPDATE ON stream_capture_runtime
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_stream_capture_runtime_status
ON stream_capture_runtime(status, updated_at DESC);

CREATE TABLE IF NOT EXISTS capture_session_leases (
  stream_id BIGINT PRIMARY KEY REFERENCES streams(id) ON DELETE CASCADE,
  lease_owner TEXT NOT NULL,
  lease_expires_at TIMESTAMPTZ NOT NULL,
  heartbeat_at TIMESTAMPTZ NOT NULL,
  acquired_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_capture_session_leases_updated_at ON capture_session_leases;
CREATE TRIGGER trg_capture_session_leases_updated_at
BEFORE UPDATE ON capture_session_leases
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_capture_session_leases_expiry
ON capture_session_leases(lease_expires_at);
