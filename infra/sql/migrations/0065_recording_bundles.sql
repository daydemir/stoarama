BEGIN;

-- Recording bundles: a thin grouping that fans out into N ordinary recordings,
-- one per selected catalog stream, all sharing ONE schedule. The bundle row
-- carries only the shared schedule (cron + user-chosen timezone applied
-- uniformly to every member + clip length + fps + window + storage); it owns no
-- capture-path state. Member recordings are normal rows that reuse the existing
-- recordings/recording_jobs/worker/autoscaler/billing/clips path unchanged. A
-- bundle is invisible to billing: recording_billing_days (0056) bills per
-- recording_id, so a bundle of N members bills as N record-days automatically.
CREATE TABLE IF NOT EXISTS recording_bundles (
  id                              BIGSERIAL PRIMARY KEY,
  account_id                      BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  name                            TEXT NOT NULL,
  cron_expr                       TEXT NOT NULL,                       -- standard 5-field (same contract as recordings.cron_expr)
  cron_timezone                   TEXT NOT NULL DEFAULT 'UTC',         -- IANA; the USER-CHOSEN uniform tz applied to every member
  clip_duration_sec               INTEGER NOT NULL DEFAULT 60 CHECK (clip_duration_sec BETWEEN 5 AND 900),
  target_fps                      INTEGER CHECK (target_fps IS NULL OR target_fps BETWEEN 1 AND 240), -- NULL = Source (mirror 0064)
  start_at                        TIMESTAMPTZ NOT NULL DEFAULT now(),
  end_at                          TIMESTAMPTZ,                         -- open-ended when NULL
  storage_destination_id          BIGINT NOT NULL REFERENCES storage_destinations(id) ON DELETE RESTRICT,
  delivery_storage_destination_id BIGINT REFERENCES storage_destinations(id) ON DELETE RESTRICT, -- nullable WebDAV delivery target
  status                          TEXT NOT NULL DEFAULT 'active'
                                    CHECK (status IN ('active','paused','canceled','completed')),
  created_at                      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at                      TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT recording_bundles_window_chk CHECK (end_at IS NULL OR end_at > start_at)
);

DROP TRIGGER IF EXISTS trg_recording_bundles_updated_at ON recording_bundles;
CREATE TRIGGER trg_recording_bundles_updated_at BEFORE UPDATE ON recording_bundles
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- name unique per account, case-insensitive (mirrors idx_recordings_account_name).
CREATE UNIQUE INDEX IF NOT EXISTS idx_recording_bundles_account_name
  ON recording_bundles (account_id, lower(name));
CREATE INDEX IF NOT EXISTS idx_recording_bundles_account_created
  ON recording_bundles (account_id, created_at DESC);

-- Link member recordings back to their bundle. ON DELETE SET NULL keeps member
-- recordings + their clips/billing intact if a bundle row is ever hard-deleted;
-- the cascade (pause/resume/cancel) is logical via status, not FK.
ALTER TABLE recordings
  ADD COLUMN IF NOT EXISTS bundle_id BIGINT NULL REFERENCES recording_bundles(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_recordings_bundle_id ON recordings (bundle_id);

COMMIT;
