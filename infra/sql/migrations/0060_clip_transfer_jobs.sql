-- 0060_clip_transfer_jobs.sql
--
-- Async "send my recorded clip to my own S3 bucket" queue. Each row is one
-- COPY (streamed GET source -> PUT target) of a recording_clip into a different
-- storage destination the same account owns. It is never a move: the source
-- clip's purged_at is left untouched. The clip transfer worker in
-- recorder-control leases rows with FOR UPDATE SKIP LOCKED, so multiple ticks /
-- instances never double-lease.

BEGIN;

CREATE TABLE clip_transfer_jobs (
  id                            BIGSERIAL PRIMARY KEY,
  account_id                    BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  recording_clip_id             BIGINT NOT NULL REFERENCES recording_clips(id) ON DELETE CASCADE,
  target_storage_destination_id BIGINT NOT NULL REFERENCES storage_destinations(id) ON DELETE CASCADE,
  target_object_key             TEXT NOT NULL,
  status                        TEXT NOT NULL DEFAULT 'pending'
                                  CHECK (status IN ('pending','leased','done','error','canceled')),
  lease_owner                   TEXT,
  lease_expires_at              TIMESTAMPTZ,
  attempt_count                 INT NOT NULL DEFAULT 0,
  max_attempts                  INT NOT NULL DEFAULT 3,
  error_text                    TEXT NOT NULL DEFAULT '',
  bytes_copied                  BIGINT NOT NULL DEFAULT 0,
  idempotency_key               TEXT NOT NULL UNIQUE,
  completed_at                  TIMESTAMPTZ,
  created_at                    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at                    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Worker poll: only pending/leased rows are ever scanned for a lease.
CREATE INDEX idx_clip_transfer_jobs_poll
  ON clip_transfer_jobs (status)
  WHERE status IN ('pending','leased');

COMMIT;
