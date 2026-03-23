BEGIN;

ALTER TABLE capture_segments
  ADD COLUMN IF NOT EXISTS thumbnail_media_object_id BIGINT REFERENCES media_objects(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_capture_segments_thumbnail_media
ON capture_segments(thumbnail_media_object_id)
WHERE thumbnail_media_object_id IS NOT NULL;

COMMIT;
