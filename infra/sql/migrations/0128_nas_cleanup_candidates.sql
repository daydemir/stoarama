-- Qualification-only snapshots. Nothing in these tables authorizes mutation.
CREATE TABLE nas_cleanup_candidate_runs (
  id UUID PRIMARY KEY,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  connection_id BIGINT NOT NULL REFERENCES connections(id) ON DELETE RESTRICT,
  recording_ids BIGINT[] NOT NULL CHECK (cardinality(recording_ids)>0),
  inventory_generation TEXT NOT NULL,
  inventory_digest TEXT NOT NULL CHECK (inventory_digest~'^[0-9a-f]{64}$'),
  inventory_started_at TIMESTAMPTZ NOT NULL,
  inventory_completed_at TIMESTAMPTZ NOT NULL,
  state TEXT NOT NULL DEFAULT 'queued' CHECK(state IN('queued','verifying','ready','unknown','failed')),
  item_count BIGINT NOT NULL CHECK(item_count>0),
  total_bytes BIGINT NOT NULL CHECK(total_bytes>=0),
  unknown_count BIGINT NOT NULL DEFAULT 0 CHECK(unknown_count>=0),
  nas_rehash_required BOOLEAN NOT NULL DEFAULT true,
  canonical_digest TEXT NOT NULL CHECK(canonical_digest~'^[0-9a-f]{64}$'),
  error_code TEXT NOT NULL DEFAULT '',
  created_by_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  UNIQUE(account_id,canonical_digest)
);

CREATE TABLE r2_content_verifications (
  id BIGSERIAL PRIMARY KEY,
  storage_destination_id BIGINT NOT NULL REFERENCES storage_destinations(id) ON DELETE RESTRICT,
  endpoint_snapshot TEXT NOT NULL,
  bucket TEXT NOT NULL CHECK(bucket<>''),
  object_key TEXT NOT NULL CHECK(object_key<>''),
  expected_size_bytes BIGINT NOT NULL CHECK(expected_size_bytes>=0),
  expected_sha256 TEXT NOT NULL CHECK(expected_sha256~'^[0-9a-f]{64}$'),
  status TEXT NOT NULL DEFAULT 'queued' CHECK(status IN('queued','leased','verified','unknown')),
  observed_etag TEXT,
  observed_version_id TEXT,
  observed_size_bytes BIGINT,
  observed_sha256 TEXT,
  verified_at TIMESTAMPTZ,
  last_head_at TIMESTAMPTZ,
  lease_token UUID,
  lease_owner TEXT,
  lease_expires_at TIMESTAMPTZ,
  attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count>=0),
  max_attempts INTEGER NOT NULL DEFAULT 3 CHECK(max_attempts>0),
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  error_code TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(storage_destination_id,endpoint_snapshot,bucket,object_key,expected_size_bytes,expected_sha256),
  CHECK((status='verified')=(verified_at IS NOT NULL AND observed_size_bytes=expected_size_bytes AND observed_sha256=expected_sha256))
);
CREATE INDEX idx_r2_content_verifications_claim ON r2_content_verifications(status,next_attempt_at,id);

CREATE TABLE nas_cleanup_candidate_items (
  run_id UUID NOT NULL REFERENCES nas_cleanup_candidate_runs(id) ON DELETE RESTRICT,
  ordinal INTEGER NOT NULL CHECK(ordinal>0),
  clip_id BIGINT NOT NULL REFERENCES recording_clips(id) ON DELETE RESTRICT,
  recording_id BIGINT NOT NULL REFERENCES recordings(id) ON DELETE RESTRICT,
  recording_job_id BIGINT,
  window_start TIMESTAMPTZ NOT NULL,
  window_end TIMESTAMPTZ NOT NULL CHECK(window_end>window_start),
  relative_path TEXT NOT NULL CHECK(relative_path<>'' AND left(relative_path,1)<>'/'),
  size_bytes BIGINT NOT NULL CHECK(size_bytes>=0),
  content_sha256 TEXT NOT NULL CHECK(content_sha256~'^[0-9a-f]{64}$'),
  inventory_verified_at TIMESTAMPTZ NOT NULL,
  file_mtime_ns BIGINT NOT NULL,
  file_ctime_ns BIGINT NOT NULL CHECK(file_ctime_ns>0),
  file_inode BIGINT NOT NULL CHECK(file_inode>0),
  file_device BIGINT NOT NULL CHECK(file_device>0),
  sidecar_relative_path TEXT NOT NULL CHECK(sidecar_relative_path<>'' AND left(sidecar_relative_path,1)<>'/'),
  sidecar_size_bytes BIGINT NOT NULL CHECK(sidecar_size_bytes>0),
  sidecar_sha256 TEXT NOT NULL CHECK(sidecar_sha256~'^[0-9a-f]{64}$'),
  storage_destination_id BIGINT NOT NULL REFERENCES storage_destinations(id) ON DELETE RESTRICT,
  recovery_bucket TEXT NOT NULL,
  recovery_object_key TEXT NOT NULL,
  recovery_etag TEXT NOT NULL,
  verification_id BIGINT NOT NULL REFERENCES r2_content_verifications(id) ON DELETE RESTRICT,
  PRIMARY KEY(run_id,ordinal), UNIQUE(run_id,clip_id), UNIQUE(run_id,relative_path)
);
CREATE INDEX idx_nas_cleanup_candidate_items_verification ON nas_cleanup_candidate_items(verification_id,run_id);

-- Snapshot identity is immutable. Status/progress fields are worker-derived.
CREATE FUNCTION guard_nas_cleanup_candidate_run() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF (NEW.id,NEW.account_id,NEW.connection_id,NEW.recording_ids,NEW.inventory_generation,NEW.inventory_digest,
     NEW.inventory_started_at,NEW.inventory_completed_at,NEW.item_count,NEW.total_bytes,NEW.canonical_digest,
     NEW.created_by_user_id,NEW.created_at)
 IS DISTINCT FROM
    (OLD.id,OLD.account_id,OLD.connection_id,OLD.recording_ids,OLD.inventory_generation,OLD.inventory_digest,
     OLD.inventory_started_at,OLD.inventory_completed_at,OLD.item_count,OLD.total_bytes,OLD.canonical_digest,
     OLD.created_by_user_id,OLD.created_at) THEN RAISE EXCEPTION 'candidate snapshot is immutable'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER trg_nas_cleanup_candidate_run_guard BEFORE UPDATE OR DELETE ON nas_cleanup_candidate_runs FOR EACH ROW EXECUTE FUNCTION guard_nas_cleanup_candidate_run();
CREATE FUNCTION reject_nas_cleanup_candidate_item_mutation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'candidate items are immutable'; END $$;
CREATE TRIGGER trg_nas_cleanup_candidate_item_immutable BEFORE UPDATE OR DELETE ON nas_cleanup_candidate_items FOR EACH ROW EXECUTE FUNCTION reject_nas_cleanup_candidate_item_mutation();
