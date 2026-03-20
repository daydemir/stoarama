BEGIN;

CREATE INDEX IF NOT EXISTS idx_frames_success_latest_per_stream
ON frames (stream_id, captured_at DESC, id DESC)
WHERE capture_status = 'success';

COMMIT;
