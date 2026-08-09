ALTER TABLE recording_jobs
  ADD COLUMN IF NOT EXISTS relay_fairness_started_at TIMESTAMPTZ;
