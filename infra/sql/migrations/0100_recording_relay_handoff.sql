ALTER TABLE recording_jobs
  ADD COLUMN IF NOT EXISTS handoff_owner TEXT,
  ADD COLUMN IF NOT EXISTS handoff_until TIMESTAMPTZ;

ALTER TABLE recording_jobs
  DROP CONSTRAINT IF EXISTS recording_jobs_handoff_pair_chk,
  ADD CONSTRAINT recording_jobs_handoff_pair_chk CHECK (
    (handoff_owner IS NULL) = (handoff_until IS NULL)
  );
