ALTER TABLE recording_settings
  DROP CONSTRAINT IF EXISTS recording_settings_clip_duration_sec_check;

ALTER TABLE recording_settings
  ADD CONSTRAINT recording_settings_clip_duration_sec_check
  CHECK (clip_duration_sec IN (30, 90));
