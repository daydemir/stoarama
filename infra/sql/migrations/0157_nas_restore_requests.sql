BEGIN;

-- Additive: NAS -> R2 restore queue. An operator (stoaramactl nas-restore
-- request) asks for specific clips to be uploaded back from the NAS. The NAS
-- pull client leases pending rows, verifies its local bytes against the
-- recorded sha256, and uploads through a presigned create-only PUT that signs
-- the key, content type, exact length, sha256 checksum and If-None-Match: *.
-- The API then HEADs the object and re-hashes it in full before a row becomes
-- 'verified'. An 'original' restore writes the clip's own object_key and then
-- clears recording_clips.purged_at; a 'test' restore writes under
-- nas-restore-test/ and the API deletes the object once it is verified.
--
-- No trigger or existing table changes: clearing purged_at is not a purge
-- transition, and the joined retention guard only constrains the other way.
CREATE TABLE IF NOT EXISTS nas_restore_requests (
  id BIGSERIAL PRIMARY KEY,
  connection_id BIGINT NOT NULL REFERENCES connections(id) ON DELETE RESTRICT,
  clip_id BIGINT NOT NULL,
  recording_id BIGINT NOT NULL,
  target TEXT NOT NULL CHECK (target IN ('original','test')),
  object_key TEXT NOT NULL CHECK (object_key <> '' AND left(object_key,1) <> '/'),
  source_object_key TEXT NOT NULL,
  relative_path TEXT NOT NULL CHECK (relative_path <> '' AND left(relative_path,1) <> '/'),
  content_type TEXT NOT NULL CHECK (content_type <> ''),
  size_bytes BIGINT NOT NULL CHECK (size_bytes > 0),
  sha256 TEXT NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
  label TEXT NOT NULL CHECK (label ~ '^[a-z0-9][a-z0-9._-]{0,79}$'),
  state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','leased','verified','failed','canceled')),
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  max_attempts INTEGER NOT NULL DEFAULT 5 CHECK (max_attempts BETWEEN 1 AND 20),
  requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  first_leased_at TIMESTAMPTZ,
  leased_at TIMESTAMPTZ,
  lease_expires_at TIMESTAMPTZ,
  put_expires_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  outcome TEXT NOT NULL DEFAULT '',
  upload_started_at TIMESTAMPTZ,
  upload_duration_ms BIGINT CHECK (upload_duration_ms >= 0),
  bytes_uploaded BIGINT CHECK (bytes_uploaded >= 0),
  verified_etag TEXT NOT NULL DEFAULT '',
  last_error TEXT NOT NULL DEFAULT '' CHECK (octet_length(last_error) <= 1024),
  clip_restored_at TIMESTAMPTZ,
  object_deleted_at TIMESTAMPTZ,
  client_version TEXT NOT NULL DEFAULT '',
  CHECK (target = 'original' OR object_key LIKE 'nas-restore-test/%'),
  CHECK (target = 'test' OR object_key = source_object_key),
  CHECK ((state = 'leased') = (lease_expires_at IS NOT NULL)),
  CHECK ((state IN ('verified','failed','canceled')) = (completed_at IS NOT NULL)),
  CHECK (clip_restored_at IS NULL OR (target = 'original' AND state = 'verified'))
);

-- One active request per target object: two leases can never race on a key.
CREATE UNIQUE INDEX IF NOT EXISTS nas_restore_requests_active_key_idx
  ON nas_restore_requests (object_key) WHERE state IN ('pending','leased');
CREATE INDEX IF NOT EXISTS nas_restore_requests_queue_idx
  ON nas_restore_requests (connection_id, id) WHERE state IN ('pending','leased');
CREATE INDEX IF NOT EXISTS nas_restore_requests_label_idx
  ON nas_restore_requests (label, id);
CREATE INDEX IF NOT EXISTS nas_restore_requests_clip_idx
  ON nas_restore_requests (clip_id);
CREATE INDEX IF NOT EXISTS nas_restore_requests_test_cleanup_idx
  ON nas_restore_requests (put_expires_at)
  WHERE target = 'test' AND object_deleted_at IS NULL AND state IN ('verified','failed','canceled');

COMMIT;
