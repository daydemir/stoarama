ALTER TABLE recordings
  ADD COLUMN IF NOT EXISTS preferred_relay_group_id BIGINT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'recordings_preferred_relay_group_account_fk'
      AND conrelid = 'recordings'::regclass
  ) THEN
    ALTER TABLE recordings
      ADD CONSTRAINT recordings_preferred_relay_group_account_fk
      FOREIGN KEY (account_id, preferred_relay_group_id)
      REFERENCES relay_groups (account_id, id)
      ON DELETE SET NULL (preferred_relay_group_id);
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS recordings_preferred_relay_group_idx
  ON recordings (preferred_relay_group_id)
  WHERE preferred_relay_group_id IS NOT NULL;
