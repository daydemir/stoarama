BEGIN;

ALTER TABLE upload_intents
  DROP CONSTRAINT IF EXISTS upload_intents_kind_check;

ALTER TABLE upload_intents
  ADD CONSTRAINT upload_intents_kind_check
  CHECK (kind IN ('boxed', 'capture_segment'));

CREATE TABLE IF NOT EXISTS capture_segments (
  id BIGSERIAL PRIMARY KEY,
  stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
  capture_job_id BIGINT REFERENCES capture_jobs(id) ON DELETE SET NULL,
  media_object_id BIGINT REFERENCES media_objects(id) ON DELETE SET NULL,
  execution_class TEXT NOT NULL CHECK (execution_class IN ('youtube_direct', 'youtube_relay', 'video_live')),
  resolved_capture_type TEXT CHECK (resolved_capture_type IN ('youtube_watch', 'hls', 'dash', 'rtsp', 'rtmp', 'http_video', 'still_image', 'webrtc', 'unknown')),
  resolved_url TEXT,
  segment_start_at TIMESTAMPTZ NOT NULL,
  segment_end_at TIMESTAMPTZ NOT NULL,
  duration_ms BIGINT NOT NULL CHECK (duration_ms >= 0),
  target_fps INTEGER NOT NULL CHECK (target_fps > 0),
  actual_fps DOUBLE PRECISION,
  video_codec TEXT,
  audio_codec TEXT,
  container TEXT NOT NULL DEFAULT 'mp4',
  audio_present BOOLEAN NOT NULL DEFAULT false,
  capture_status TEXT NOT NULL CHECK (capture_status IN ('success', 'error')),
  capture_error TEXT,
  source_kind TEXT NOT NULL CHECK (source_kind IN ('live', 'snapshot_url')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (stream_id, segment_start_at, media_object_id)
);

CREATE INDEX IF NOT EXISTS idx_capture_segments_stream_start
ON capture_segments(stream_id, segment_start_at DESC);

CREATE INDEX IF NOT EXISTS idx_capture_segments_status_start
ON capture_segments(capture_status, segment_start_at DESC);

COMMIT;
