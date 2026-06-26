BEGIN;

-- Usage-billing pivot: every recording gains a capture window [start_at, end_at).
-- The scheduler captures only within this window and auto-stops at end_at, flipping
-- status to 'completed' (window finished, distinct from 'canceled' = user deleted).
-- Defaults: start_at = now() (start immediately), end_at = NULL (open-ended).
ALTER TABLE recordings ADD COLUMN start_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE recordings ADD COLUMN end_at   TIMESTAMPTZ;           -- NULL = open-ended
ALTER TABLE recordings ADD CONSTRAINT recordings_window_chk
  CHECK (end_at IS NULL OR end_at > start_at);

-- Extend the status enum with 'completed' (auto-stop terminal state). The original
-- inline unnamed CHECK from 0051 is auto-named recordings_status_check by Postgres.
ALTER TABLE recordings DROP CONSTRAINT recordings_status_check;
ALTER TABLE recordings ADD  CONSTRAINT recordings_status_check
  CHECK (status IN ('active','paused','canceled','completed'));

-- Cheap auto-stop sweep: find active recordings whose window has closed.
CREATE INDEX IF NOT EXISTS idx_recordings_active_endat
  ON recordings (status, end_at) WHERE status = 'active';

COMMIT;
