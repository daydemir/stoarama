BEGIN;

ALTER TABLE media_objects
  ADD COLUMN IF NOT EXISTS archive_provider TEXT NOT NULL DEFAULT 'none',
  ADD COLUMN IF NOT EXISTS archive_bucket TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS archive_object_key TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS archive_storage_class TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS archive_status TEXT NOT NULL DEFAULT 'none',
  ADD COLUMN IF NOT EXISTS archive_size_bytes BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS archive_sha256 TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS archive_etag TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS archive_copied_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS archive_verified_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS archive_source_deleted_at TIMESTAMPTZ;

ALTER TABLE media_objects
  DROP CONSTRAINT IF EXISTS media_objects_archive_provider_check,
  DROP CONSTRAINT IF EXISTS media_objects_archive_storage_class_check,
  DROP CONSTRAINT IF EXISTS media_objects_archive_status_check,
  DROP CONSTRAINT IF EXISTS media_objects_archive_size_bytes_check;

ALTER TABLE media_objects
  ADD CONSTRAINT media_objects_archive_provider_check
    CHECK (archive_provider IN ('none', 'aws_s3')),
  ADD CONSTRAINT media_objects_archive_storage_class_check
    CHECK (archive_storage_class IN ('', 'DEEP_ARCHIVE')),
  ADD CONSTRAINT media_objects_archive_status_check
    CHECK (archive_status IN ('none', 'copied', 'verified', 'source_deleted', 'error')),
  ADD CONSTRAINT media_objects_archive_size_bytes_check
    CHECK (archive_size_bytes >= 0);

CREATE INDEX IF NOT EXISTS idx_media_objects_archive_status
ON media_objects(archive_status, archive_provider);

CREATE INDEX IF NOT EXISTS idx_media_objects_archive_bucket_key
ON media_objects(archive_bucket, archive_object_key)
WHERE archive_provider <> 'none';

COMMIT;
