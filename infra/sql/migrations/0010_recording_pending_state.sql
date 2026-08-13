DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_enum e
    JOIN pg_type t ON t.oid = e.enumtypid
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE t.typname = 'recording_state_enum'
      AND n.nspname = current_schema()
      AND e.enumlabel = 'pending'
  ) THEN
    ALTER TYPE recording_state_enum ADD VALUE 'pending';
  END IF;
END $$;
