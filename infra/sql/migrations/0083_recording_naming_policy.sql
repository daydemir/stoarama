BEGIN;

-- Per-recording naming is hard-cut and typed: no template strings. Existing rows
-- are migrated into the current Stoarama layout, and every clip snapshots the
-- display path used by user-facing delivery paths.

ALTER TABLE recordings
  ADD COLUMN IF NOT EXISTS naming_profile TEXT,
  ADD COLUMN IF NOT EXISTS folder_name TEXT,
  ADD COLUMN IF NOT EXISTS naming_metadata_jsonb JSONB;

UPDATE recordings
SET
  naming_profile = COALESCE(NULLIF(naming_profile, ''), 'stoarama_v1'),
  folder_name = COALESCE(NULLIF(folder_name, ''), 'recordings'),
  naming_metadata_jsonb = COALESCE(naming_metadata_jsonb, '{}'::jsonb)
WHERE naming_profile IS NULL
   OR folder_name IS NULL
   OR folder_name = ''
   OR naming_metadata_jsonb IS NULL;

ALTER TABLE recordings
  ALTER COLUMN naming_profile SET DEFAULT 'stoarama_v1',
  ALTER COLUMN folder_name SET DEFAULT 'recordings',
  ALTER COLUMN naming_metadata_jsonb SET DEFAULT '{}'::jsonb,
  ALTER COLUMN naming_profile SET NOT NULL,
  ALTER COLUMN folder_name SET NOT NULL,
  ALTER COLUMN naming_metadata_jsonb SET NOT NULL;

ALTER TABLE recordings
  DROP CONSTRAINT IF EXISTS recordings_naming_profile_chk,
  DROP CONSTRAINT IF EXISTS recordings_folder_name_nonempty_chk;

ALTER TABLE recordings
  ADD CONSTRAINT recordings_naming_profile_chk
    CHECK (naming_profile IN ('stoarama_v1', 'plaza_hourly_v1')),
  ADD CONSTRAINT recordings_folder_name_nonempty_chk
    CHECK (btrim(folder_name) <> '');

ALTER TABLE recording_upload_intents
  ADD COLUMN IF NOT EXISTS display_path TEXT;

CREATE OR REPLACE FUNCTION recording_display_path_from_object_key()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  prefix TEXT;
  cleaned TEXT;
BEGIN
  IF NEW.display_path IS NULL OR btrim(NEW.display_path) = '' THEN
    cleaned := btrim(NEW.object_key);
    SELECT btrim(COALESCE(key_prefix, ''), '/') INTO prefix
    FROM storage_destinations
    WHERE id = NEW.storage_destination_id;

    IF COALESCE(prefix, '') <> '' AND cleaned LIKE prefix || '/%' THEN
      cleaned := substr(cleaned, length(prefix) + 2);
    END IF;

    NEW.display_path := cleaned;
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS recording_upload_intents_display_path_default_trg ON recording_upload_intents;
CREATE TRIGGER recording_upload_intents_display_path_default_trg
BEFORE INSERT OR UPDATE ON recording_upload_intents
FOR EACH ROW
EXECUTE FUNCTION recording_display_path_from_object_key();

UPDATE recording_upload_intents ui
SET display_path = CASE
  WHEN btrim(COALESCE(sd.key_prefix, ''), '/') <> ''
   AND btrim(ui.object_key) LIKE btrim(sd.key_prefix, '/') || '/%'
    THEN substr(btrim(ui.object_key), length(btrim(sd.key_prefix, '/')) + 2)
  ELSE btrim(ui.object_key)
END
FROM storage_destinations sd
WHERE ui.storage_destination_id = sd.id
  AND (ui.display_path IS NULL OR ui.display_path = '');

ALTER TABLE recording_upload_intents
  DROP CONSTRAINT IF EXISTS recording_upload_intents_display_path_nonempty_chk;

ALTER TABLE recording_upload_intents
  ADD CONSTRAINT recording_upload_intents_display_path_nonempty_chk
    CHECK (display_path IS NOT NULL AND btrim(display_path) <> '') NOT VALID;

ALTER TABLE recording_upload_intents
  VALIDATE CONSTRAINT recording_upload_intents_display_path_nonempty_chk;

ALTER TABLE recording_clips
  ADD COLUMN IF NOT EXISTS display_path TEXT;

DROP TRIGGER IF EXISTS recording_clips_display_path_default_trg ON recording_clips;
CREATE TRIGGER recording_clips_display_path_default_trg
BEFORE INSERT OR UPDATE ON recording_clips
FOR EACH ROW
EXECUTE FUNCTION recording_display_path_from_object_key();

UPDATE recording_clips c
SET display_path = CASE
  WHEN btrim(COALESCE(sd.key_prefix, ''), '/') <> ''
   AND btrim(c.object_key) LIKE btrim(sd.key_prefix, '/') || '/%'
    THEN substr(btrim(c.object_key), length(btrim(sd.key_prefix, '/')) + 2)
  ELSE btrim(c.object_key)
END
FROM storage_destinations sd
WHERE c.storage_destination_id = sd.id
  AND (c.display_path IS NULL OR c.display_path = '');

ALTER TABLE recording_clips
  DROP CONSTRAINT IF EXISTS recording_clips_display_path_nonempty_chk;

ALTER TABLE recording_clips
  ADD CONSTRAINT recording_clips_display_path_nonempty_chk
    CHECK (display_path IS NOT NULL AND btrim(display_path) <> '') NOT VALID;

ALTER TABLE recording_clips
  VALIDATE CONSTRAINT recording_clips_display_path_nonempty_chk;

COMMIT;
