BEGIN;

-- Continuous capture mode. A 'sampled' recording (the existing default) records
-- one clip per cron fire. A 'continuous' recording records back-to-back gapless
-- segments for a daily time window, so it reuses the recordings/recording_jobs/
-- worker/clips/billing path unchanged: clip_duration_sec becomes the segment
-- length, each finalized segment is an ordinary recording_clips row, and a
-- continuous job is ONE window-long recording_jobs lease (kind='continuous_window')
-- rather than per-clip jobs.

-- recordings: mode + daily window. cron_expr becomes nullable so a continuous
-- recording (which has no per-fire cadence) can leave it NULL. A shape constraint
-- enforces the two valid forms: sampled rows carry a cron_expr; continuous rows
-- carry a daily window. Window ordering (start < end, no midnight crossing) is
-- validated in Go at create time.
ALTER TABLE recordings ADD COLUMN IF NOT EXISTS mode TEXT NOT NULL DEFAULT 'sampled'
  CHECK (mode IN ('sampled','continuous'));
ALTER TABLE recordings ADD COLUMN IF NOT EXISTS daily_window_start TIME NULL;
ALTER TABLE recordings ADD COLUMN IF NOT EXISTS daily_window_end   TIME NULL;
ALTER TABLE recordings ALTER COLUMN cron_expr DROP NOT NULL;
ALTER TABLE recordings DROP CONSTRAINT IF EXISTS recordings_mode_shape_chk;
ALTER TABLE recordings ADD CONSTRAINT recordings_mode_shape_chk CHECK (
  (mode='sampled'    AND cron_expr IS NOT NULL) OR
  (mode='continuous' AND daily_window_start IS NOT NULL AND daily_window_end IS NOT NULL)
);

-- recording_bundles: mirror so a continuous bundle fans out continuous members.
ALTER TABLE recording_bundles ADD COLUMN IF NOT EXISTS mode TEXT NOT NULL DEFAULT 'sampled'
  CHECK (mode IN ('sampled','continuous'));
ALTER TABLE recording_bundles ADD COLUMN IF NOT EXISTS daily_window_start TIME NULL;
ALTER TABLE recording_bundles ADD COLUMN IF NOT EXISTS daily_window_end   TIME NULL;
ALTER TABLE recording_bundles ALTER COLUMN cron_expr DROP NOT NULL;
ALTER TABLE recording_bundles DROP CONSTRAINT IF EXISTS recording_bundles_mode_shape_chk;
ALTER TABLE recording_bundles ADD CONSTRAINT recording_bundles_mode_shape_chk CHECK (
  (mode='sampled'    AND cron_expr IS NOT NULL) OR
  (mode='continuous' AND daily_window_start IS NOT NULL AND daily_window_end IS NOT NULL)
);

-- recording_jobs: reuse the existing jobs/lease/reclaim machinery for continuous.
-- A 'continuous_window' job is one window-long lease; window_end_at tells the
-- worker when to stop ffmpeg. fire_at = the window-open instant; clip_duration_sec
-- is reused as the segment length.
ALTER TABLE recording_jobs ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'clip'
  CHECK (kind IN ('clip','continuous_window'));
ALTER TABLE recording_jobs ADD COLUMN IF NOT EXISTS window_end_at TIMESTAMPTZ NULL;
ALTER TABLE recording_jobs DROP CONSTRAINT IF EXISTS recording_jobs_continuous_window_chk;
ALTER TABLE recording_jobs ADD CONSTRAINT recording_jobs_continuous_window_chk CHECK (
  kind='clip' OR window_end_at IS NOT NULL
);

COMMIT;
