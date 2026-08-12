-- Catalog-only frames captured from live sources for operator scene attestation.
-- These are deliberately not recording clips or recording heartbeats.
ALTER TABLE frames DROP CONSTRAINT IF EXISTS frames_source_kind_check;
ALTER TABLE frames ADD CONSTRAINT frames_source_kind_check
  CHECK (source_kind IN ('live','snapshot_url','authoritative_frame_refresh'));
