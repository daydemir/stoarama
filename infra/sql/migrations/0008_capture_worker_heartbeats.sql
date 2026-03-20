BEGIN;

CREATE TABLE IF NOT EXISTS capture_worker_heartbeats (
  worker_id TEXT NOT NULL,
  mode TEXT NOT NULL CHECK (mode IN ('youtube_live', 'hls_live', 'image_poll', 'ffmpeg_direct')),
  capacity INTEGER NOT NULL CHECK (capacity > 0),
  heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  lease_expires_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (worker_id, mode)
);

DROP TRIGGER IF EXISTS trg_capture_worker_heartbeats_updated_at ON capture_worker_heartbeats;
CREATE TRIGGER trg_capture_worker_heartbeats_updated_at
BEFORE UPDATE ON capture_worker_heartbeats
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_capture_worker_heartbeats_mode_expiry
ON capture_worker_heartbeats(mode, lease_expires_at DESC);

COMMIT;

