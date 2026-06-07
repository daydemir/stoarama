-- Allow source_kind='survey' on frames for the daily survey feature.
-- The original CHECK constraint is defined inline in 0001_init.sql:
--   source_kind TEXT NOT NULL CHECK (source_kind IN ('live', 'snapshot_url'))
-- An inline column CHECK constraint gets an auto-generated name; resolve it
-- dynamically so this migration is independent of that generated name.
DO $$
DECLARE
  con_name text;
BEGIN
  SELECT conname INTO con_name
  FROM pg_constraint
  WHERE conrelid = 'frames'::regclass
    AND contype = 'c'
    AND pg_get_constraintdef(oid) ILIKE '%source_kind%';
  IF con_name IS NOT NULL THEN
    EXECUTE format('ALTER TABLE frames DROP CONSTRAINT %I', con_name);
  END IF;
END $$;

ALTER TABLE frames
  ADD CONSTRAINT frames_source_kind_check
  CHECK (source_kind IN ('live', 'snapshot_url', 'survey'));
