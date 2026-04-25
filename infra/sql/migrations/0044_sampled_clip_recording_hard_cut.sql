ALTER TABLE recording_settings
  ADD COLUMN IF NOT EXISTS clip_duration_sec INTEGER NOT NULL DEFAULT 30 CHECK (clip_duration_sec = 30),
  ADD COLUMN IF NOT EXISTS sample_interval_min_sec INTEGER NOT NULL DEFAULT 240 CHECK (sample_interval_min_sec > 0),
  ADD COLUMN IF NOT EXISTS sample_interval_max_sec INTEGER NOT NULL DEFAULT 480 CHECK (sample_interval_max_sec >= sample_interval_min_sec),
  ADD COLUMN IF NOT EXISTS stale_grace_sec INTEGER NOT NULL DEFAULT 300 CHECK (stale_grace_sec >= 0);

INSERT INTO recording_settings (
  id, capture_interval_sec, clip_duration_sec, sample_interval_min_sec, sample_interval_max_sec, stale_grace_sec, updated_at
)
VALUES (true, 1, 30, 240, 480, 300, now())
ON CONFLICT (id)
DO UPDATE SET
  capture_interval_sec=1,
  clip_duration_sec=30,
  sample_interval_min_sec=240,
  sample_interval_max_sec=480,
  stale_grace_sec=300,
  updated_at=now();

ALTER TABLE capture_jobs
  ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;

UPDATE capture_jobs
SET status='error',
    error_text='sampled clip hard cut reset',
    lease_owner=NULL,
    lease_expires_at=NULL,
    completed_at=COALESCE(completed_at, now()),
    updated_at=now()
WHERE status IN ('pending', 'leased');

CREATE UNIQUE INDEX IF NOT EXISTS idx_capture_jobs_one_active_per_stream
ON capture_jobs (stream_id)
WHERE status IN ('pending', 'leased');

CREATE INDEX IF NOT EXISTS idx_capture_jobs_pending_due
ON capture_jobs (scheduled_for, id)
WHERE status='pending';

DELETE FROM server_execution_capacity
WHERE execution_class IN ('video_live', 'youtube_direct', 'youtube_relay');
