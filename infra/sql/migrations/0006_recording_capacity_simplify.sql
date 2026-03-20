BEGIN;

ALTER TABLE recording_mode_capacity
  DROP COLUMN IF EXISTS enabled;

ALTER TABLE recording_mode_capacity
  DROP CONSTRAINT IF EXISTS recording_mode_capacity_max_active_check;

ALTER TABLE recording_mode_capacity
  ADD CONSTRAINT recording_mode_capacity_max_active_check CHECK (max_active >= 0);

COMMIT;
