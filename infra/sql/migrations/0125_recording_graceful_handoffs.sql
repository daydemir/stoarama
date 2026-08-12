ALTER TABLE recording_jobs
  ADD COLUMN IF NOT EXISTS graceful_handoff_request_id UUID,
  ADD COLUMN IF NOT EXISTS graceful_handoff_requested_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS graceful_handoff_reason TEXT,
  ADD COLUMN IF NOT EXISTS graceful_handoff_owner TEXT,
  ADD COLUMN IF NOT EXISTS graceful_handoff_lease_token UUID,
  ADD COLUMN IF NOT EXISTS graceful_handoff_excluded_relay_group_id BIGINT REFERENCES relay_groups(id);

ALTER TABLE recording_jobs
  DROP CONSTRAINT IF EXISTS recording_jobs_graceful_handoff_state_chk,
  ADD CONSTRAINT recording_jobs_graceful_handoff_state_chk CHECK (
    (graceful_handoff_request_id IS NULL
      AND graceful_handoff_requested_at IS NULL
      AND graceful_handoff_reason IS NULL
      AND graceful_handoff_owner IS NULL
      AND graceful_handoff_lease_token IS NULL)
    OR
    (graceful_handoff_request_id IS NOT NULL
      AND graceful_handoff_requested_at IS NOT NULL
      AND graceful_handoff_reason IS NOT NULL
      AND graceful_handoff_owner IS NOT NULL
      AND graceful_handoff_lease_token IS NOT NULL)
  );

CREATE UNIQUE INDEX IF NOT EXISTS recording_jobs_graceful_handoff_request_uidx
  ON recording_jobs(graceful_handoff_request_id)
  WHERE graceful_handoff_request_id IS NOT NULL;
