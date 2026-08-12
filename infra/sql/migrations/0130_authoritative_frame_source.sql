-- Catalog-only frames captured from live sources for operator scene attestation.
-- These are deliberately not recording clips or recording heartbeats.
DO $$
DECLARE con_name text;
BEGIN
  SELECT conname INTO con_name FROM pg_constraint
  WHERE conrelid='frames'::regclass AND contype='c'
    AND pg_get_constraintdef(oid) ILIKE '%source_kind%';
  IF con_name IS NOT NULL THEN
    EXECUTE format('ALTER TABLE frames DROP CONSTRAINT %I',con_name);
  END IF;
END $$;
ALTER TABLE frames ADD CONSTRAINT frames_source_kind_check
  CHECK (source_kind IN ('live','snapshot_url','survey','authoritative_frame_refresh'));
CREATE UNIQUE INDEX idx_frames_authoritative_identity
  ON frames(stream_id,captured_at) WHERE source_kind='authoritative_frame_refresh';
