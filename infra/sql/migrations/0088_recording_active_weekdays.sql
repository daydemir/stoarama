BEGIN;

ALTER TABLE recordings
  ADD COLUMN active_weekdays SMALLINT NOT NULL DEFAULT 127
  CHECK (active_weekdays BETWEEN 1 AND 127);

ALTER TABLE recording_bundles
  ADD COLUMN active_weekdays SMALLINT NOT NULL DEFAULT 127
  CHECK (active_weekdays BETWEEN 1 AND 127);

CREATE UNIQUE INDEX IF NOT EXISTS idx_recordings_account_stream_active
  ON recordings (account_id, stream_id)
  WHERE stream_id IS NOT NULL AND status <> 'canceled';

COMMIT;
