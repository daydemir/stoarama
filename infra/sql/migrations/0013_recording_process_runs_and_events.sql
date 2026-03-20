BEGIN;

CREATE TABLE IF NOT EXISTS recording_process_runs (
  id BIGSERIAL PRIMARY KEY,
  stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
  mode TEXT NOT NULL CHECK (mode IN ('youtube_live', 'hls_live', 'image_poll', 'ffmpeg_direct')),
  server_id TEXT NOT NULL,
  process_id TEXT NOT NULL,
  worker_id TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('starting', 'running', 'stopped', 'crashed', 'failed')),
  start_reason TEXT NOT NULL DEFAULT '',
  stop_reason TEXT NOT NULL DEFAULT '',
  started_at TIMESTAMPTZ NOT NULL,
  stopped_at TIMESTAMPTZ,
  last_heartbeat_at TIMESTAMPTZ,
  last_frame_at TIMESTAMPTZ,
  restart_count INTEGER NOT NULL DEFAULT 0,
  last_error_text TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (stream_id, process_id)
);

DROP TRIGGER IF EXISTS trg_recording_process_runs_updated_at ON recording_process_runs;
CREATE TRIGGER trg_recording_process_runs_updated_at
BEFORE UPDATE ON recording_process_runs
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_recording_process_runs_stream_started
ON recording_process_runs (stream_id, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_recording_process_runs_server_started
ON recording_process_runs (server_id, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_recording_process_runs_status_heartbeat
ON recording_process_runs (status, last_heartbeat_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_recording_process_runs_active_stream
ON recording_process_runs (stream_id)
WHERE status IN ('starting', 'running') AND stopped_at IS NULL;

CREATE TABLE IF NOT EXISTS recording_state_events (
  id BIGSERIAL PRIMARY KEY,
  stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
  prev_state recording_state_enum,
  next_state recording_state_enum NOT NULL,
  actor TEXT NOT NULL DEFAULT 'db_trigger',
  reason TEXT NOT NULL DEFAULT '',
  metadata_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_recording_state_events_stream_created
ON recording_state_events (stream_id, created_at DESC);

CREATE OR REPLACE FUNCTION log_recording_state_change()
RETURNS TRIGGER AS $$
DECLARE
  event_actor TEXT;
  event_reason TEXT;
BEGIN
  IF NEW.recording_state IS DISTINCT FROM OLD.recording_state THEN
    event_actor := COALESCE(NULLIF(current_setting('app.recording_actor', true), ''), 'db_trigger');
    event_reason := COALESCE(NULLIF(current_setting('app.recording_reason', true), ''), '');
    INSERT INTO recording_state_events (
      stream_id,
      prev_state,
      next_state,
      actor,
      reason,
      metadata_jsonb,
      created_at
    )
    VALUES (
      NEW.id,
      OLD.recording_state,
      NEW.recording_state,
      event_actor,
      event_reason,
      jsonb_build_object(
        'recording_failed_reason_prev', OLD.recording_failed_reason,
        'recording_failed_reason_next', NEW.recording_failed_reason,
        'recording_failed_at_prev', OLD.recording_failed_at,
        'recording_failed_at_next', NEW.recording_failed_at
      ),
      now()
    );
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_streams_recording_state_events ON streams;
CREATE TRIGGER trg_streams_recording_state_events
AFTER UPDATE OF recording_state, recording_failed_reason, recording_failed_at ON streams
FOR EACH ROW
EXECUTE FUNCTION log_recording_state_change();

COMMIT;
