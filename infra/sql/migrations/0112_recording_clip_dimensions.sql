ALTER TABLE recording_clips
  ADD COLUMN IF NOT EXISTS video_width INTEGER,
  ADD COLUMN IF NOT EXISTS video_height INTEGER;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname='chk_recording_clips_video_dimensions'
  ) THEN
    ALTER TABLE recording_clips ADD CONSTRAINT chk_recording_clips_video_dimensions
      CHECK ((video_width IS NULL AND video_height IS NULL) OR
             (video_width > 0 AND video_height > 0)) NOT VALID;
  END IF;
END $$;
