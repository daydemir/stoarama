-- A lease owner identifies a machine, not a particular worker process. During a
-- restart, an expired lease can be reassigned to a new process on the same
-- machine while the old process is still alive. A generation token makes every
-- mutation prove that it belongs to the exact lease issuance, preventing both
-- processes from recording the same window concurrently.
ALTER TABLE recording_jobs
  ADD COLUMN IF NOT EXISTS lease_token UUID;

-- Preserve the generation on every ingested clip. Besides making overlap audits
-- attributable, this lets us distinguish a duplicated capture chain from source
-- timestamp behavior without guessing from the job's final lease owner.
ALTER TABLE recording_clips
  ADD COLUMN IF NOT EXISTS capture_lease_token UUID;
ALTER TABLE recording_clips
  ADD COLUMN IF NOT EXISTS capture_sequence BIGINT;

ALTER TABLE recording_clips
  DROP CONSTRAINT IF EXISTS recording_clips_capture_sequence_positive_chk;
ALTER TABLE recording_clips
  ADD CONSTRAINT recording_clips_capture_sequence_positive_chk
  CHECK (capture_sequence IS NULL OR capture_sequence > 0);

CREATE UNIQUE INDEX IF NOT EXISTS uq_recording_clips_capture_sequence
  ON recording_clips (recording_job_id, capture_lease_token, capture_sequence)
  WHERE capture_lease_token IS NOT NULL AND capture_sequence IS NOT NULL;

-- Historical NULL-generation rows contain known reconnect duplicates, so scope
-- enforcement to generation-aware clips. New workers preflight the hash before
-- upload; this index is the final concurrency backstop.
CREATE UNIQUE INDEX IF NOT EXISTS uq_recording_clips_capture_sha256
  ON recording_clips (recording_job_id, capture_lease_token, sha256)
  WHERE capture_lease_token IS NOT NULL AND sha256 <> '';

CREATE INDEX IF NOT EXISTS idx_recording_clips_capture_lease_token
  ON recording_clips (capture_lease_token)
  WHERE capture_lease_token IS NOT NULL;
