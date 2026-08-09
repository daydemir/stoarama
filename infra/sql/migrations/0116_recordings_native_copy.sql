-- Recording clips are archival source footage. Fixed target FPS invokes the
-- FFmpeg encoding branch and can silently reduce quality, so retire historical
-- settings and make source-native capture a database invariant.
UPDATE recordings
SET target_fps = NULL,
    updated_at = now()
WHERE target_fps IS NOT NULL;

ALTER TABLE recordings
  DROP CONSTRAINT IF EXISTS recordings_target_fps_native_chk;

ALTER TABLE recordings
  ADD CONSTRAINT recordings_target_fps_native_chk CHECK (target_fps IS NULL);
