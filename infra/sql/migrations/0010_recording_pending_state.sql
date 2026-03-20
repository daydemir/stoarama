DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_enum e
    JOIN pg_type t ON t.oid = e.enumtypid
    WHERE t.typname = 'recording_state_enum'
      AND e.enumlabel = 'pending'
  ) THEN
    ALTER TYPE recording_state_enum ADD VALUE 'pending';
  END IF;
END $$;
