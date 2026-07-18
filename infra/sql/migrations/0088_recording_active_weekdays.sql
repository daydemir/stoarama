BEGIN;

ALTER TABLE recordings
  ADD COLUMN active_weekdays SMALLINT NOT NULL DEFAULT 127
  CHECK (active_weekdays BETWEEN 1 AND 127);

ALTER TABLE recording_bundles
  ADD COLUMN active_weekdays SMALLINT NOT NULL DEFAULT 127
  CHECK (active_weekdays BETWEEN 1 AND 127);

COMMIT;
