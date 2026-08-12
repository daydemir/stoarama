-- Immutable provenance for an authoritative frame decoded from one exact
-- landed recording clip. All fields are nullable for source-captured frames,
-- but clip-backed evidence must carry the complete identity.
ALTER TABLE frames ADD COLUMN source_recording_clip_id BIGINT REFERENCES recording_clips(id) ON DELETE RESTRICT;
ALTER TABLE frames ADD COLUMN source_recording_clip_sha256 TEXT;
ALTER TABLE frames ADD COLUMN source_recording_clip_etag TEXT;
ALTER TABLE frames ADD COLUMN source_recording_clip_version_id TEXT;

ALTER TABLE frames ADD CONSTRAINT frames_recording_clip_provenance_coherent CHECK (
  (source_recording_clip_id IS NULL AND source_recording_clip_sha256 IS NULL AND source_recording_clip_etag IS NULL AND source_recording_clip_version_id IS NULL)
  OR
  (source_recording_clip_id IS NOT NULL
   AND source_recording_clip_sha256 IS NOT NULL
   AND source_recording_clip_etag IS NOT NULL
   AND source_recording_clip_sha256 ~ '^[0-9a-f]{64}$'
   AND length(btrim(source_recording_clip_etag)) BETWEEN 1 AND 256)
) NOT VALID;
ALTER TABLE frames VALIDATE CONSTRAINT frames_recording_clip_provenance_coherent;

CREATE UNIQUE INDEX idx_frames_source_recording_clip
  ON frames(source_recording_clip_id) WHERE source_recording_clip_id IS NOT NULL;

-- Source-captured frames retain their timestamp identity. Clip-backed frames
-- use the stronger exact clip identity above; this also avoids a false conflict
-- if an earlier source refresh happened at the exact clip boundary.
DROP INDEX idx_frames_authoritative_identity;
CREATE UNIQUE INDEX idx_frames_authoritative_identity
  ON frames(stream_id,captured_at)
  WHERE source_kind='authoritative_frame_refresh' AND source_recording_clip_id IS NULL;

CREATE OR REPLACE FUNCTION prevent_frame_provenance_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF ROW(OLD.stream_id,OLD.captured_at,OLD.raw_media_object_id,OLD.source_kind,
         OLD.source_recording_clip_id,OLD.source_recording_clip_sha256,
         OLD.source_recording_clip_etag,OLD.source_recording_clip_version_id)
     IS DISTINCT FROM
     ROW(NEW.stream_id,NEW.captured_at,NEW.raw_media_object_id,NEW.source_kind,
         NEW.source_recording_clip_id,NEW.source_recording_clip_sha256,
         NEW.source_recording_clip_etag,NEW.source_recording_clip_version_id) THEN
    RAISE EXCEPTION 'frame media provenance is immutable';
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER trg_frames_provenance_immutable
BEFORE UPDATE ON frames FOR EACH ROW EXECUTE FUNCTION prevent_frame_provenance_mutation();
