BEGIN;

ALTER TABLE connections
  ADD COLUMN joined_protocol_version SMALLINT NOT NULL DEFAULT 0,
  ADD COLUMN joined_files_pulled BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN joined_bytes_pulled BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN joined_last_attempt_artifact_id BIGINT,
  ADD COLUMN joined_last_blocker TEXT NOT NULL DEFAULT '',
  ADD COLUMN joined_last_attempt_at TIMESTAMPTZ,
  ADD COLUMN joined_retry_at TIMESTAMPTZ;

ALTER TABLE connections
  ADD CONSTRAINT connections_joined_protocol_version_chk CHECK (joined_protocol_version IN (0, 1)),
  ADD CONSTRAINT connections_joined_totals_chk CHECK (joined_files_pulled >= 0 AND joined_bytes_pulled >= 0),
  ADD CONSTRAINT connections_joined_attempt_chk CHECK (
    (joined_last_attempt_artifact_id IS NULL AND joined_last_blocker = '' AND joined_last_attempt_at IS NULL AND joined_retry_at IS NULL)
    OR (joined_last_attempt_artifact_id > 0 AND joined_last_blocker ~ '^[a-z][a-z0-9_]{0,79}$'
      AND joined_last_attempt_at IS NOT NULL AND (joined_retry_at IS NULL OR joined_retry_at >= joined_last_attempt_at))
  );

CREATE TABLE recording_joined_batches (
  id BIGSERIAL PRIMARY KEY,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  connection_id BIGINT NOT NULL REFERENCES connections(id) ON DELETE RESTRICT,
  batch_id TEXT NOT NULL CHECK (batch_id ~ '^[a-z0-9][a-z0-9-]{0,62}$'),
  generation INTEGER NOT NULL CHECK (generation > 0),
  source_endpoint TEXT NOT NULL CHECK (source_endpoint ~ '^https://[0-9a-f]{32}\.r2\.cloudflarestorage\.com$'),
  policy_version TEXT NOT NULL CHECK (policy_version = btrim(policy_version) AND policy_version <> '' AND octet_length(policy_version) <= 128),
  eligibility_cutoff TIMESTAMPTZ NOT NULL,
  media_tool JSONB NOT NULL CHECK (jsonb_typeof(media_tool) = 'object' AND media_tool <> '{}'::jsonb),
  media_tool_sha256 TEXT NOT NULL CHECK (media_tool_sha256 ~ '^[0-9a-f]{64}$'),
  freeze_request JSONB NOT NULL CHECK (jsonb_typeof(freeze_request) = 'object' AND freeze_request <> '{}'::jsonb),
  freeze_request_sha256 TEXT NOT NULL CHECK (freeze_request_sha256 ~ '^[0-9a-f]{64}$'),
  frozen_denominator_sha256 TEXT NOT NULL CHECK (frozen_denominator_sha256 ~ '^[0-9a-f]{64}$'),
  expected_recordings INTEGER NOT NULL CHECK (expected_recordings > 0),
  expected_stream_days INTEGER NOT NULL CHECK (expected_stream_days > 0),
  expected_scheduled_hours INTEGER NOT NULL CHECK (expected_scheduled_hours > 0),
  expected_source_clips BIGINT NOT NULL CHECK (expected_source_clips >= 0),
  expected_source_bytes BIGINT NOT NULL CHECK (expected_source_bytes >= 0),
  state TEXT NOT NULL DEFAULT 'building' CHECK (state IN ('building', 'frozen', 'index_sealed', 'published', 'terminal_failed')),
  index_artifact_id BIGINT,
  freeze_started_at TIMESTAMPTZ,
  frozen_at TIMESTAMPTZ,
  index_sealed_at TIMESTAMPTZ,
  published_at TIMESTAMPTZ,
  failure_reason_code TEXT NOT NULL DEFAULT '' CHECK (failure_reason_code = '' OR failure_reason_code ~ '^[a-z][a-z0-9_]{0,79}$'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (batch_id),
  UNIQUE (id, account_id, connection_id, batch_id),
  UNIQUE (id, source_endpoint),
  CHECK (expected_stream_days = expected_recordings * 14),
  CHECK (expected_scheduled_hours = expected_stream_days * 12),
  CHECK ((state = 'building' AND frozen_at IS NULL AND index_artifact_id IS NULL AND index_sealed_at IS NULL AND published_at IS NULL
      AND failure_reason_code = '')
    OR (state = 'frozen' AND freeze_started_at IS NOT NULL AND frozen_at IS NOT NULL AND freeze_started_at <= frozen_at
      AND index_artifact_id IS NULL AND index_sealed_at IS NULL AND published_at IS NULL
      AND failure_reason_code = '')
    OR (state = 'index_sealed' AND index_artifact_id IS NOT NULL AND index_sealed_at IS NOT NULL AND published_at IS NULL)
    OR (state = 'published' AND index_artifact_id IS NOT NULL AND index_sealed_at IS NOT NULL AND published_at IS NOT NULL)
    OR state = 'terminal_failed')
);

CREATE TABLE recording_joined_batch_recordings (
  id BIGSERIAL PRIMARY KEY,
  batch_record_id BIGINT NOT NULL REFERENCES recording_joined_batches(id) ON DELETE RESTRICT,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  connection_id BIGINT NOT NULL REFERENCES connections(id) ON DELETE RESTRICT,
  batch_id TEXT NOT NULL,
  recording_id BIGINT NOT NULL CHECK (recording_id > 0),
  priority_ordinal INTEGER NOT NULL CHECK (priority_ordinal > 0),
  timezone TEXT NOT NULL CHECK (timezone = btrim(timezone) AND timezone <> ''),
  folder_name TEXT NOT NULL CHECK (folder_name = btrim(folder_name) AND folder_name <> ''),
  naming_metadata JSONB NOT NULL CHECK (jsonb_typeof(naming_metadata) = 'object' AND naming_metadata <> '{}'::jsonb),
  first_local_date DATE NOT NULL,
  last_local_date DATE NOT NULL,
  qualification JSONB NOT NULL CHECK (jsonb_typeof(qualification) = 'object' AND qualification <> '{}'::jsonb),
  qualification_sha256 TEXT NOT NULL CHECK (qualification_sha256 ~ '^[0-9a-f]{64}$'),
  qualification_policy_version TEXT NOT NULL CHECK (qualification_policy_version = btrim(qualification_policy_version) AND qualification_policy_version <> ''),
  authoritative_job_ids BIGINT[] NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (batch_record_id, account_id, connection_id, batch_id)
    REFERENCES recording_joined_batches(id, account_id, connection_id, batch_id) ON DELETE RESTRICT,
  CHECK (last_local_date = first_local_date + 13),
  CHECK (array_ndims(authoritative_job_ids) = 1 AND array_lower(authoritative_job_ids, 1) = 1
    AND cardinality(authoritative_job_ids) = 14 AND array_position(authoritative_job_ids, NULL) IS NULL),
  UNIQUE (batch_record_id, recording_id),
  UNIQUE (batch_record_id, priority_ordinal),
  UNIQUE (batch_record_id, recording_id, priority_ordinal),
  UNIQUE (batch_record_id, id, recording_id)
);

CREATE FUNCTION guard_recording_joined_batch_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.state<>'building' OR NEW.freeze_started_at IS NOT NULL OR NEW.frozen_at IS NOT NULL
    OR NOT EXISTS(SELECT 1 FROM connections c
    WHERE c.id=NEW.connection_id AND c.account_id=NEW.account_id FOR KEY SHARE)
  THEN RAISE EXCEPTION 'joined batch must enter an owned building state'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_batch_insert_guard BEFORE INSERT ON recording_joined_batches
  FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_batch_insert();

CREATE TABLE recording_joined_freeze_exclusions (
  id BIGSERIAL PRIMARY KEY,
  batch_record_id BIGINT NOT NULL REFERENCES recording_joined_batches(id) ON DELETE RESTRICT,
  batch_recording_id BIGINT NOT NULL REFERENCES recording_joined_batch_recordings(id) ON DELETE RESTRICT,
  recording_id BIGINT NOT NULL CHECK (recording_id > 0),
  clip_id BIGINT,
  reason_code TEXT NOT NULL CHECK (reason_code ~ '^[a-z][a-z0-9_]{0,79}$'),
  evidence_sha256 TEXT NOT NULL CHECK (evidence_sha256 ~ '^[0-9a-f]{64}$'),
  canonical_evidence JSONB NOT NULL CHECK (jsonb_typeof(canonical_evidence) = 'object' AND canonical_evidence <> '{}'::jsonb),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (batch_record_id, batch_recording_id, recording_id)
    REFERENCES recording_joined_batch_recordings(batch_record_id, id, recording_id) ON DELETE RESTRICT,
  UNIQUE NULLS NOT DISTINCT (batch_record_id, recording_id, clip_id, reason_code, evidence_sha256)
);

CREATE TABLE recording_joined_stream_days (
  id BIGSERIAL PRIMARY KEY,
  batch_record_id BIGINT NOT NULL REFERENCES recording_joined_batches(id) ON DELETE RESTRICT,
  batch_recording_id BIGINT NOT NULL REFERENCES recording_joined_batch_recordings(id) ON DELETE RESTRICT,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  connection_id BIGINT NOT NULL REFERENCES connections(id) ON DELETE RESTRICT,
  batch_id TEXT NOT NULL,
  recording_id BIGINT NOT NULL CHECK (recording_id > 0),
  local_date DATE NOT NULL,
  date_ordinal SMALLINT NOT NULL CHECK (date_ordinal BETWEEN 1 AND 14),
  recording_job_id BIGINT NOT NULL REFERENCES recording_jobs(id) ON DELETE RESTRICT,
  scheduled_start_at TIMESTAMPTZ NOT NULL,
  scheduled_end_at TIMESTAMPTZ NOT NULL,
  source_clip_count INTEGER NOT NULL CHECK (source_clip_count >= 0),
  source_bytes BIGINT NOT NULL CHECK (source_bytes >= 0),
  source_manifest_sha256 TEXT NOT NULL CHECK (source_manifest_sha256 ~ '^[0-9a-f]{64}$'),
  ledger_sha256 TEXT NOT NULL CHECK (ledger_sha256 ~ '^[0-9a-f]{64}$'),
  ledger_bytes BYTEA NOT NULL CHECK (octet_length(ledger_bytes) BETWEEN 2 AND 16777216),
  ledger_artifact_sha256 TEXT NOT NULL CHECK (ledger_artifact_sha256 ~ '^[0-9a-f]{64}$'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (batch_record_id, account_id, connection_id, batch_id)
    REFERENCES recording_joined_batches(id, account_id, connection_id, batch_id) ON DELETE RESTRICT,
  FOREIGN KEY (batch_record_id, batch_recording_id, recording_id)
    REFERENCES recording_joined_batch_recordings(batch_record_id, id, recording_id) ON DELETE RESTRICT,
  CHECK (scheduled_end_at > scheduled_start_at),
  CHECK (ledger_artifact_sha256 = encode(sha256(ledger_bytes),'hex')),
  CHECK ((source_clip_count = 0 AND source_bytes = 0) OR (source_clip_count > 0 AND source_bytes > 0)),
  UNIQUE (batch_record_id, recording_id, local_date),
  UNIQUE (batch_recording_id, date_ordinal),
  UNIQUE (batch_record_id, id, recording_id)
);

CREATE TABLE recording_joined_day_boundaries (
  id BIGSERIAL PRIMARY KEY,
  stream_day_id BIGINT NOT NULL REFERENCES recording_joined_stream_days(id) ON DELETE RESTRICT,
  boundary_kind TEXT NOT NULL CHECK (boundary_kind IN ('cross_hour', 'cross_day')),
  ordinal SMALLINT NOT NULL,
  previous_delivery_hour SMALLINT,
  next_delivery_hour SMALLINT,
  previous_clip_id BIGINT,
  next_clip_id BIGINT,
  previous_presentation_end_at TIMESTAMPTZ,
  next_presentation_start_at TIMESTAMPTZ,
  signed_gap_nanoseconds BIGINT,
  scheduled_at TIMESTAMPTZ,
  scheduled_previous_end_at TIMESTAMPTZ,
  scheduled_next_start_at TIMESTAMPTZ,
  actual_seam_at TIMESTAMPTZ,
  boundary_skew_nanoseconds BIGINT,
  allocation_decision TEXT NOT NULL CHECK (allocation_decision = btrim(allocation_decision) AND allocation_decision <> ''),
  verdict TEXT NOT NULL CHECK (verdict IN ('allocated', 'absent_source', 'scheduled_gap', 'overlap')),
  reason TEXT NOT NULL CHECK (reason = btrim(reason) AND reason <> ''),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK ((boundary_kind = 'cross_hour' AND ordinal BETWEEN 1 AND 11
      AND previous_delivery_hour = ordinal AND next_delivery_hour = ordinal + 1
      AND scheduled_at IS NOT NULL AND scheduled_previous_end_at IS NULL AND scheduled_next_start_at IS NULL)
    OR (boundary_kind = 'cross_day' AND ordinal BETWEEN 1 AND 2
      AND previous_delivery_hour IS NULL AND next_delivery_hour IS NULL AND scheduled_at IS NULL
      AND scheduled_previous_end_at IS NOT NULL AND scheduled_next_start_at IS NOT NULL)),
  CHECK ((previous_clip_id IS NULL) = (previous_presentation_end_at IS NULL)),
  CHECK ((next_clip_id IS NULL) = (next_presentation_start_at IS NULL)),
  CHECK ((previous_clip_id IS NULL OR next_clip_id IS NULL) = (signed_gap_nanoseconds IS NULL)),
  UNIQUE (stream_day_id, boundary_kind, ordinal)
);

CREATE TABLE recording_joined_hours (
  id BIGSERIAL PRIMARY KEY,
  batch_record_id BIGINT NOT NULL REFERENCES recording_joined_batches(id) ON DELETE RESTRICT,
  stream_day_id BIGINT NOT NULL REFERENCES recording_joined_stream_days(id) ON DELETE RESTRICT,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  connection_id BIGINT NOT NULL REFERENCES connections(id) ON DELETE RESTRICT,
  batch_id TEXT NOT NULL,
  recording_id BIGINT NOT NULL CHECK (recording_id > 0),
  hour_id TEXT NOT NULL CHECK (hour_id ~ '^[a-z0-9][a-z0-9-]{0,62}__recording-[1-9][0-9]*__date-[0-9]{4}-[0-9]{2}-[0-9]{2}__hour-(0[1-9]|1[0-2])__generation-[1-9][0-9]*$'),
  local_date DATE NOT NULL,
  delivery_hour SMALLINT NOT NULL CHECK (delivery_hour BETWEEN 1 AND 12),
  clock_hour SMALLINT NOT NULL CHECK (clock_hour BETWEEN 8 AND 19),
  scheduled_start_at TIMESTAMPTZ NOT NULL,
  scheduled_end_at TIMESTAMPTZ NOT NULL,
  priority_ordinal BIGINT NOT NULL CHECK (priority_ordinal >= 0),
  source_clip_count INTEGER NOT NULL CHECK (source_clip_count >= 0),
  source_bytes BIGINT NOT NULL CHECK (source_bytes >= 0),
  source_claim_sha256 TEXT NOT NULL CHECK (source_claim_sha256 ~ '^[0-9a-f]{64}$'),
  state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'leased', 'sealed', 'terminal_failed')),
  attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count BETWEEN 0 AND 100),
  claim_token UUID,
  claimed_by TEXT CHECK (claimed_by IS NULL OR (claimed_by = btrim(claimed_by) AND claimed_by <> '' AND octet_length(claimed_by) <= 256)),
  lease_expires_at TIMESTAMPTZ,
  heartbeat_at TIMESTAMPTZ,
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_only_sha256 TEXT CHECK (source_only_sha256 ~ '^[0-9a-f]{64}$'),
  canonical_plan JSONB,
  manifest_bytes BYTEA,
  manifest_sha256 TEXT CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
  sealed_at TIMESTAMPTZ,
  failure_reason_code TEXT NOT NULL DEFAULT '' CHECK (failure_reason_code = '' OR failure_reason_code ~ '^[a-z][a-z0-9_]{0,79}$'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (batch_record_id, account_id, connection_id, batch_id)
    REFERENCES recording_joined_batches(id, account_id, connection_id, batch_id) ON DELETE RESTRICT,
  FOREIGN KEY (batch_record_id, stream_day_id, recording_id)
    REFERENCES recording_joined_stream_days(batch_record_id, id, recording_id) ON DELETE RESTRICT,
  CHECK (clock_hour = delivery_hour + 7 AND scheduled_end_at > scheduled_start_at),
  CHECK ((source_clip_count = 0 AND source_bytes = 0) OR (source_clip_count > 0 AND source_bytes > 0)),
  CHECK ((state = 'leased' AND claim_token IS NOT NULL AND claimed_by IS NOT NULL AND lease_expires_at IS NOT NULL AND heartbeat_at IS NOT NULL)
    OR (state <> 'leased' AND claim_token IS NULL AND claimed_by IS NULL AND lease_expires_at IS NULL AND heartbeat_at IS NULL)),
  CHECK ((state IN ('pending', 'leased') AND source_only_sha256 IS NULL AND canonical_plan IS NULL AND manifest_bytes IS NULL AND manifest_sha256 IS NULL AND sealed_at IS NULL)
    OR (state = 'sealed' AND source_only_sha256 IS NOT NULL AND jsonb_typeof(canonical_plan) = 'object'
      AND manifest_bytes IS NOT NULL AND manifest_sha256 IS NOT NULL
      AND manifest_sha256 = encode(sha256(manifest_bytes),'hex') AND sealed_at IS NOT NULL)
    OR state = 'terminal_failed'),
  UNIQUE (hour_id),
  UNIQUE (stream_day_id, delivery_hour),
  UNIQUE (batch_record_id, priority_ordinal),
  UNIQUE (batch_record_id, id, recording_id)
);

CREATE INDEX recording_joined_hours_claim_idx ON recording_joined_hours(batch_record_id, priority_ordinal, next_attempt_at, id)
  WHERE state IN ('pending', 'leased');

CREATE TABLE recording_joined_sources (
  id BIGSERIAL PRIMARY KEY,
  batch_record_id BIGINT NOT NULL REFERENCES recording_joined_batches(id) ON DELETE RESTRICT,
  stream_day_id BIGINT NOT NULL REFERENCES recording_joined_stream_days(id) ON DELETE RESTRICT,
  hour_record_id BIGINT NOT NULL REFERENCES recording_joined_hours(id) ON DELETE RESTRICT,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  connection_id BIGINT NOT NULL REFERENCES connections(id) ON DELETE RESTRICT,
  recording_id BIGINT NOT NULL CHECK (recording_id > 0),
  recording_job_id BIGINT NOT NULL REFERENCES recording_jobs(id) ON DELETE RESTRICT,
  clip_id BIGINT NOT NULL REFERENCES recording_clips(id) ON DELETE RESTRICT,
  storage_destination_id BIGINT NOT NULL REFERENCES storage_destinations(id) ON DELETE RESTRICT,
  day_ordinal INTEGER NOT NULL CHECK (day_ordinal > 0),
  hour_ordinal INTEGER NOT NULL CHECK (hour_ordinal > 0),
  provider TEXT NOT NULL CHECK (provider = btrim(provider) AND provider <> ''),
  endpoint TEXT NOT NULL,
  region TEXT NOT NULL,
  bucket TEXT NOT NULL CHECK (bucket = btrim(bucket) AND bucket <> ''),
  object_key TEXT NOT NULL CHECK (object_key = btrim(object_key) AND object_key <> ''),
  version_id TEXT NOT NULL DEFAULT '',
  etag TEXT NOT NULL DEFAULT '',
  size_bytes BIGINT NOT NULL CHECK (size_bytes > 0),
  sha256 TEXT NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
  start_at TIMESTAMPTZ NOT NULL,
  end_at TIMESTAMPTZ NOT NULL,
  audio_contract JSONB,
  seam_to_previous JSONB NOT NULL CHECK (jsonb_typeof(seam_to_previous) = 'object'),
  clip_created_at TIMESTAMPTZ NOT NULL,
  released_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (end_at > start_at AND end_at - start_at <= interval '15 minutes'),
  CHECK (audio_contract IS NULL OR jsonb_typeof(audio_contract) = 'object'),
  FOREIGN KEY (batch_record_id, endpoint)
    REFERENCES recording_joined_batches(id, source_endpoint) ON DELETE RESTRICT,
  UNIQUE (batch_record_id, clip_id),
  UNIQUE (stream_day_id, day_ordinal),
  UNIQUE (hour_record_id, hour_ordinal),
  CHECK (endpoint ~ '^https://[0-9a-f]{32}\.r2\.cloudflarestorage\.com$'
    AND region = btrim(region) AND octet_length(region) <= 128
    AND octet_length(bucket) <= 255 AND octet_length(object_key) <= 2048
    AND object_key NOT IN ('.','..') AND left(object_key,1)<>'/' AND object_key !~ E'\\\\'
    AND version_id = btrim(version_id) AND (version_id = '' OR (octet_length(version_id) <= 1024 AND version_id ~ '^[!-~]+$'))
    AND etag = btrim(etag) AND octet_length(etag) BETWEEN 1 AND 256 AND etag ~ '^[!-~]+$'
    AND etag !~ '^W/' AND etag !~ '"')
);

CREATE UNIQUE INDEX recording_joined_sources_storage_identity_uq ON recording_joined_sources(
  connection_id,batch_record_id,storage_destination_id,provider,endpoint,region,bucket,object_key,
  (CASE WHEN version_id <> '' THEN 'v:' || version_id ELSE 'e:' || etag END));

CREATE TABLE recording_joined_artifacts (
  id BIGSERIAL PRIMARY KEY,
  batch_record_id BIGINT NOT NULL REFERENCES recording_joined_batches(id) ON DELETE RESTRICT,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  connection_id BIGINT NOT NULL REFERENCES connections(id) ON DELETE RESTRICT,
  batch_id TEXT NOT NULL,
  scope_kind TEXT NOT NULL CHECK (scope_kind IN ('ledger', 'hour', 'batch_index')),
  scope_id TEXT NOT NULL CHECK (scope_id = btrim(scope_id) AND scope_id <> '' AND octet_length(scope_id) <= 1024),
  stream_day_id BIGINT REFERENCES recording_joined_stream_days(id) ON DELETE RESTRICT,
  hour_record_id BIGINT REFERENCES recording_joined_hours(id) ON DELETE RESTRICT,
  artifact_kind TEXT NOT NULL CHECK (artifact_kind IN ('allocation_ledger', 'hour_manifest', 'media', 'batch_index')),
  ordinal INTEGER NOT NULL DEFAULT 1 CHECK (ordinal > 0),
  relative_path TEXT NOT NULL CHECK (relative_path = btrim(relative_path) AND relative_path <> ''),
  object_key TEXT NOT NULL CHECK (object_key = btrim(object_key) AND object_key <> ''),
  content_type TEXT NOT NULL CHECK (content_type IN ('application/json', 'video/mp4')),
  content_id TEXT NOT NULL DEFAULT '' CHECK (content_id = '' OR content_id ~ '^[0-9a-f]{64}$'),
  expected_size_bytes BIGINT NOT NULL CHECK (expected_size_bytes > 0),
  expected_sha256 TEXT NOT NULL CHECK (expected_sha256 ~ '^[0-9a-f]{64}$'),
  canonical_bytes BYTEA,
  publication_state TEXT CHECK (publication_state IN ('sealed', 'publishing', 'published', 'terminal_failed')),
  publication_attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (publication_attempt_count BETWEEN 0 AND 100),
  publication_token UUID,
  publication_claimed_by TEXT CHECK (publication_claimed_by IS NULL OR
    (publication_claimed_by = btrim(publication_claimed_by) AND publication_claimed_by <> '' AND octet_length(publication_claimed_by) <= 256)),
  publication_lease_expires_at TIMESTAMPTZ,
  publication_heartbeat_at TIMESTAMPTZ,
  publication_next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finalized_token UUID,
  etag TEXT,
  version_id TEXT,
  published_at TIMESTAMPTZ,
  failure_reason_code TEXT NOT NULL DEFAULT '' CHECK (failure_reason_code = '' OR failure_reason_code ~ '^[a-z][a-z0-9_]{0,79}$'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (batch_record_id, account_id, connection_id, batch_id)
    REFERENCES recording_joined_batches(id, account_id, connection_id, batch_id) ON DELETE RESTRICT,
  CHECK ((artifact_kind = 'allocation_ledger' AND scope_kind = 'ledger' AND stream_day_id IS NOT NULL AND hour_record_id IS NULL
      AND content_type = 'application/json' AND canonical_bytes IS NOT NULL)
    OR (artifact_kind = 'hour_manifest' AND scope_kind = 'hour' AND stream_day_id IS NOT NULL AND hour_record_id IS NOT NULL
      AND content_type = 'application/json' AND canonical_bytes IS NOT NULL)
    OR (artifact_kind = 'media' AND scope_kind = 'hour' AND stream_day_id IS NOT NULL AND hour_record_id IS NOT NULL
      AND content_type = 'video/mp4' AND canonical_bytes IS NULL AND content_id = expected_sha256)
    OR (artifact_kind = 'batch_index' AND scope_kind = 'batch_index' AND stream_day_id IS NULL AND hour_record_id IS NULL
      AND content_type = 'application/json' AND canonical_bytes IS NOT NULL)),
  CHECK (canonical_bytes IS NULL OR octet_length(canonical_bytes) = expected_size_bytes),
  CHECK ((content_type = 'application/json' AND expected_size_bytes <= 16777216)
    OR (content_type = 'video/mp4' AND expected_size_bytes <= 5363466240)),
  CHECK ((artifact_kind <> 'media' AND publication_state IS NOT NULL)
    OR (artifact_kind = 'media' AND publication_state IS NULL AND publication_token IS NULL
      AND publication_claimed_by IS NULL AND publication_lease_expires_at IS NULL AND publication_heartbeat_at IS NULL)),
  CHECK ((publication_state = 'publishing' AND publication_token IS NOT NULL AND publication_claimed_by IS NOT NULL
      AND publication_lease_expires_at IS NOT NULL AND publication_heartbeat_at IS NOT NULL)
    OR (publication_state IS DISTINCT FROM 'publishing' AND publication_token IS NULL AND publication_claimed_by IS NULL
      AND publication_lease_expires_at IS NULL AND publication_heartbeat_at IS NULL)),
  CHECK ((published_at IS NULL AND etag IS NULL AND version_id IS NULL AND finalized_token IS NULL)
    OR (published_at IS NOT NULL AND etag IS NOT NULL AND version_id IS NOT NULL AND finalized_token IS NOT NULL)),
  UNIQUE (connection_id, batch_id, relative_path),
  UNIQUE (batch_record_id, scope_kind, scope_id, artifact_kind, ordinal)
);

CREATE UNIQUE INDEX recording_joined_one_ledger_root ON recording_joined_artifacts(stream_day_id)
  WHERE artifact_kind = 'allocation_ledger';
CREATE UNIQUE INDEX recording_joined_one_manifest_root ON recording_joined_artifacts(hour_record_id)
  WHERE artifact_kind = 'hour_manifest';
CREATE UNIQUE INDEX recording_joined_one_index_root ON recording_joined_artifacts(batch_record_id)
  WHERE artifact_kind = 'batch_index';
CREATE INDEX recording_joined_publication_claim_idx ON recording_joined_artifacts(batch_record_id, publication_next_attempt_at, id)
  WHERE artifact_kind <> 'media' AND publication_state IN ('sealed', 'publishing');
CREATE INDEX recording_joined_artifacts_object_idx ON recording_joined_artifacts(connection_id, object_key);

ALTER TABLE recording_joined_batches
  ADD CONSTRAINT recording_joined_batches_index_artifact_fk FOREIGN KEY (index_artifact_id)
    REFERENCES recording_joined_artifacts(id) ON DELETE RESTRICT;

CREATE TABLE recording_joined_media_sources (
  artifact_id BIGINT NOT NULL REFERENCES recording_joined_artifacts(id) ON DELETE RESTRICT,
  source_id BIGINT NOT NULL REFERENCES recording_joined_sources(id) ON DELETE RESTRICT,
  ordinal INTEGER NOT NULL CHECK (ordinal > 0),
  PRIMARY KEY (artifact_id, source_id),
  UNIQUE (artifact_id, ordinal),
  UNIQUE (source_id)
);

CREATE TABLE recording_joined_hour_dispositions (
  hour_record_id BIGINT NOT NULL REFERENCES recording_joined_hours(id) ON DELETE RESTRICT,
  source_id BIGINT NOT NULL REFERENCES recording_joined_sources(id) ON DELETE RESTRICT,
  disposition TEXT NOT NULL CHECK (disposition IN ('included', 'quarantined')),
  media_artifact_id BIGINT REFERENCES recording_joined_artifacts(id) ON DELETE RESTRICT,
  media_ordinal INTEGER NOT NULL DEFAULT 0 CHECK (media_ordinal >= 0),
  reason_code TEXT NOT NULL DEFAULT '' CHECK (reason_code = '' OR reason_code ~ '^[a-z][a-z0-9_]{0,79}$'),
  quarantine_evidence JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (hour_record_id, source_id),
  CHECK ((disposition = 'included' AND media_artifact_id IS NOT NULL AND media_ordinal > 0 AND reason_code = '' AND quarantine_evidence IS NULL)
    OR (disposition = 'quarantined' AND media_artifact_id IS NULL AND media_ordinal = 0 AND reason_code <> ''
      AND jsonb_typeof(quarantine_evidence) = 'object'))
);

CREATE TABLE recording_joined_batch_index_refs (
  batch_record_id BIGINT NOT NULL REFERENCES recording_joined_batches(id) ON DELETE RESTRICT,
  index_artifact_id BIGINT NOT NULL REFERENCES recording_joined_artifacts(id) ON DELETE RESTRICT,
  referenced_artifact_id BIGINT NOT NULL REFERENCES recording_joined_artifacts(id) ON DELETE RESTRICT,
  reference_kind TEXT NOT NULL CHECK (reference_kind IN ('allocation_ledger', 'hour_manifest')),
  ordinal INTEGER NOT NULL CHECK (ordinal > 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (index_artifact_id, referenced_artifact_id),
  UNIQUE (index_artifact_id, reference_kind, ordinal)
);

CREATE TABLE recording_joined_artifact_acks (
  artifact_id BIGINT NOT NULL REFERENCES recording_joined_artifacts(id) ON DELETE RESTRICT,
  connection_id BIGINT NOT NULL REFERENCES connections(id) ON DELETE RESTRICT,
  relative_path TEXT NOT NULL,
  size_bytes BIGINT NOT NULL CHECK (size_bytes > 0),
  sha256 TEXT NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
  verified_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (artifact_id, connection_id)
);

CREATE FUNCTION guard_recording_joined_source_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE clip recording_clips%ROWTYPE; destination storage_destinations%ROWTYPE; h recording_joined_hours%ROWTYPE;
  d recording_joined_stream_days%ROWTYPE; batch recording_joined_batches%ROWTYPE;
BEGIN
  SELECT * INTO STRICT clip FROM recording_clips WHERE id = NEW.clip_id FOR KEY SHARE;
  SELECT * INTO STRICT destination FROM storage_destinations WHERE id = NEW.storage_destination_id FOR KEY SHARE;
  SELECT * INTO STRICT h FROM recording_joined_hours WHERE id = NEW.hour_record_id FOR KEY SHARE;
  SELECT * INTO STRICT d FROM recording_joined_stream_days WHERE id = NEW.stream_day_id FOR KEY SHARE;
  SELECT * INTO STRICT batch FROM recording_joined_batches WHERE id = NEW.batch_record_id FOR KEY SHARE;
  IF clip.purged_at IS NOT NULL
    OR ROW(clip.recording_id, clip.recording_job_id, clip.storage_destination_id, clip.endpoint, clip.bucket,
      clip.object_key, clip.size_bytes, clip.sha256, clip.etag, clip.clip_start_at, clip.clip_end_at,
      clip.created_at, clip.released_at)
      IS DISTINCT FROM ROW(NEW.recording_id, NEW.recording_job_id, NEW.storage_destination_id, NEW.endpoint,
        NEW.bucket, NEW.object_key, NEW.size_bytes, NEW.sha256, NEW.etag, NEW.start_at, NEW.end_at,
        NEW.clip_created_at, NEW.released_at)
    OR ROW(destination.account_id, destination.provider, destination.endpoint, destination.region, destination.bucket)
      IS DISTINCT FROM ROW(NEW.account_id, NEW.provider, NEW.endpoint, NEW.region, NEW.bucket)
    OR NEW.endpoint IS DISTINCT FROM batch.source_endpoint
    OR ROW(h.batch_record_id, h.stream_day_id, h.account_id, h.connection_id, h.recording_id)
      IS DISTINCT FROM ROW(NEW.batch_record_id, NEW.stream_day_id, NEW.account_id, NEW.connection_id, NEW.recording_id)
    OR NEW.recording_job_id<>d.recording_job_id OR NEW.end_at<=d.scheduled_start_at OR NEW.start_at>=d.scheduled_end_at
    OR NEW.clip_created_at > batch.eligibility_cutoff
  THEN RAISE EXCEPTION 'joined source differs from frozen raw object'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_source_insert_guard BEFORE INSERT ON recording_joined_sources
FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_source_insert();

CREATE FUNCTION guard_recording_joined_freeze_child_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE batch_key BIGINT;
BEGIN
  IF TG_TABLE_NAME='recording_joined_day_boundaries' THEN
    SELECT batch_record_id INTO STRICT batch_key FROM recording_joined_stream_days
      WHERE id=(to_jsonb(NEW)->>'stream_day_id')::BIGINT;
  ELSE
    batch_key := (to_jsonb(NEW)->>'batch_record_id')::BIGINT;
  END IF;
  IF NOT EXISTS(SELECT 1 FROM recording_joined_batches WHERE id=batch_key AND state='building' FOR SHARE)
  THEN RAISE EXCEPTION 'joined frozen source scope is immutable'; END IF;
  IF TG_TABLE_NAME='recording_joined_batch_recordings' AND NOT EXISTS(SELECT 1 FROM recordings r
    WHERE r.id=(to_jsonb(NEW)->>'recording_id')::BIGINT AND r.account_id=(to_jsonb(NEW)->>'account_id')::BIGINT
      AND r.cron_timezone=to_jsonb(NEW)->>'timezone' AND r.mode='continuous' AND r.delivery='nas_pull'
      AND r.daily_window_start='08:00'::TIME AND r.daily_window_end='20:00'::TIME FOR KEY SHARE)
  THEN RAISE EXCEPTION 'joined recording scope differs'; END IF;
  IF TG_TABLE_NAME='recording_joined_hours' AND
    ((to_jsonb(NEW)->>'state')<>'pending' OR (to_jsonb(NEW)->>'attempt_count')::INTEGER<>0)
  THEN RAISE EXCEPTION 'joined hour must enter pending'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_batch_recording_insert_guard BEFORE INSERT ON recording_joined_batch_recordings
  FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_freeze_child_insert();
CREATE TRIGGER recording_joined_exclusion_insert_guard BEFORE INSERT ON recording_joined_freeze_exclusions
  FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_freeze_child_insert();
CREATE TRIGGER recording_joined_stream_day_insert_guard BEFORE INSERT ON recording_joined_stream_days
  FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_freeze_child_insert();
CREATE TRIGGER recording_joined_boundary_insert_guard BEFORE INSERT ON recording_joined_day_boundaries
  FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_freeze_child_insert();
CREATE TRIGGER recording_joined_hour_insert_guard BEFORE INSERT ON recording_joined_hours
  FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_freeze_child_insert();
CREATE TRIGGER recording_joined_source_freeze_guard BEFORE INSERT ON recording_joined_sources
  FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_freeze_child_insert();

CREATE FUNCTION validate_recording_joined_stream_day(p_stream_day_id BIGINT) RETURNS BOOLEAN LANGUAGE plpgsql AS $$
DECLARE d recording_joined_stream_days%ROWTYPE; b recording_joined_day_boundaries%ROWTYPE;
  source_count INTEGER; source_bytes BIGINT; last_position INTEGER := 0; expected_position INTEGER;
  expected_at TIMESTAMPTZ; expected_clip BIGINT; expected_prev BIGINT; expected_next BIGINT;
  expected_prev_end TIMESTAMPTZ; expected_next_start TIMESTAMPTZ; expected_gap BIGINT;
  expected_skew BIGINT; recording_timezone TEXT; expected_day_start TIMESTAMPTZ; expected_day_end TIMESTAMPTZ;
  ledger JSONB; batch recording_joined_batches%ROWTYPE; batch_recording recording_joined_batch_recordings%ROWTYPE;
BEGIN
  SELECT * INTO STRICT d FROM recording_joined_stream_days WHERE id = p_stream_day_id;
  SELECT * INTO STRICT batch FROM recording_joined_batches WHERE id=d.batch_record_id;
  SELECT * INTO STRICT batch_recording FROM recording_joined_batch_recordings WHERE id=d.batch_recording_id;
  recording_timezone := batch_recording.timezone;
  ledger := convert_from(d.ledger_bytes,'UTF8')::jsonb;
  expected_day_start := make_timestamptz(extract(year FROM d.local_date)::INTEGER,
    extract(month FROM d.local_date)::INTEGER,extract(day FROM d.local_date)::INTEGER,8,0,0,recording_timezone);
  expected_day_end := make_timestamptz(extract(year FROM d.local_date)::INTEGER,
    extract(month FROM d.local_date)::INTEGER,extract(day FROM d.local_date)::INTEGER,20,0,0,recording_timezone);
  SELECT count(*), COALESCE(sum(size_bytes), 0) INTO source_count, source_bytes
    FROM recording_joined_sources WHERE stream_day_id = d.id;
  IF ARRAY(SELECT jsonb_object_keys(ledger) ORDER BY 1) IS DISTINCT FROM ARRAY['batch_id','consecutive_pairs',
      'cross_day_boundaries','cross_hour_boundaries','first_clip_id','generation','hour_source_claim_sha256','hours',
      'last_clip_id','ledger_sha256','local_date','qualification_day','qualification_sha256','recording_id',
      'schema_version','source_bytes','source_claim_sha256','source_clip_count','sources','timezone']::TEXT[]
    OR ARRAY(SELECT jsonb_object_keys(ledger->'qualification_day') ORDER BY 1) IS DISTINCT FROM
      ARRAY['completed_at','job_id','local_date','quality_tier','window_end','window_start']::TEXT[]
    OR EXISTS(SELECT 1 FROM jsonb_array_elements(ledger->'sources') source WHERE
      ARRAY(SELECT jsonb_object_keys(source) ORDER BY 1) IS DISTINCT FROM ARRAY['bucket','clip_id','end_utc','endpoint',
        'object','provider','recording_id','recording_job_id','region','seam_to_previous','start_utc']::TEXT[]
      OR ARRAY(SELECT jsonb_object_keys(source->'object') ORDER BY 1) NOT IN
        (ARRAY['etag','key','sha256','size_bytes']::TEXT[],ARRAY['etag','key','sha256','size_bytes','version_id']::TEXT[])
      OR ARRAY(SELECT jsonb_object_keys(source->'seam_to_previous') ORDER BY 1) IS DISTINCT FROM
        ARRAY['reason','signed_gap_nanoseconds','verdict']::TEXT[])
    OR EXISTS(SELECT 1 FROM jsonb_array_elements(ledger->'hours') hour WHERE
      ARRAY(SELECT jsonb_object_keys(hour) ORDER BY 1) IS DISTINCT FROM
        ARRAY['clock_hour','delivery_hour','source_clip_ids']::TEXT[])
    OR EXISTS(SELECT 1 FROM jsonb_array_elements(ledger->'consecutive_pairs') pair WHERE
      ARRAY(SELECT jsonb_object_keys(pair) ORDER BY 1) IS DISTINCT FROM ARRAY['next_clip_id',
        'next_presentation_start_utc','previous_clip_id','previous_presentation_end_utc','signed_gap_nanoseconds']::TEXT[])
    OR EXISTS(SELECT 1 FROM jsonb_array_elements(ledger->'cross_hour_boundaries') boundary WHERE
      ARRAY(SELECT jsonb_object_keys(boundary) ORDER BY 1) IS DISTINCT FROM ARRAY['actual_seam_utc',
        'allocation_decision','boundary_skew_nanoseconds','next_clip_id','next_delivery_hour','next_presentation_start_utc',
        'previous_clip_id','previous_delivery_hour','previous_presentation_end_utc','reason','scheduled_utc',
        'signed_gap_nanoseconds','verdict']::TEXT[])
    OR EXISTS(SELECT 1 FROM jsonb_array_elements(ledger->'cross_day_boundaries') boundary WHERE
      ARRAY(SELECT jsonb_object_keys(boundary) ORDER BY 1) IS DISTINCT FROM ARRAY['allocation_decision',
        'boundary_skew_nanoseconds','next_clip_id','next_presentation_start_utc','previous_clip_id',
        'previous_presentation_end_utc','reason','scheduled_next_start_utc','scheduled_previous_end_utc',
        'signed_gap_nanoseconds','verdict']::TEXT[])
    OR (ledger->>'schema_version')::INTEGER IS DISTINCT FROM 1 OR ledger->>'batch_id' IS DISTINCT FROM d.batch_id
    OR (ledger->>'generation')::INTEGER IS DISTINCT FROM batch.generation
    OR (ledger->>'recording_id')::BIGINT IS DISTINCT FROM d.recording_id
    OR ledger->>'timezone' IS DISTINCT FROM recording_timezone OR (ledger->>'local_date')::DATE IS DISTINCT FROM d.local_date
    OR ledger->'qualification_day' IS DISTINCT FROM batch_recording.qualification->'days'->(d.date_ordinal-1)
    OR ledger->>'qualification_sha256' IS DISTINCT FROM batch_recording.qualification_sha256
    OR ledger->>'source_claim_sha256' IS DISTINCT FROM d.source_manifest_sha256
    OR (ledger->>'source_clip_count')::INTEGER IS DISTINCT FROM d.source_clip_count
    OR (ledger->>'source_bytes')::BIGINT IS DISTINCT FROM d.source_bytes
    OR ledger->>'ledger_sha256' IS DISTINCT FROM d.ledger_sha256
    OR jsonb_array_length(ledger->'sources') IS DISTINCT FROM d.source_clip_count
    OR jsonb_array_length(ledger->'hours') IS DISTINCT FROM 12
    OR jsonb_array_length(ledger->'hour_source_claim_sha256') IS DISTINCT FROM 12
    OR jsonb_array_length(ledger->'cross_hour_boundaries') IS DISTINCT FROM 11
    OR jsonb_array_length(ledger->'cross_day_boundaries') IS DISTINCT FROM 2
    OR jsonb_array_length(ledger->'consecutive_pairs') IS DISTINCT FROM GREATEST(d.source_clip_count-1,0)
    OR (d.source_clip_count=0 AND (ledger->'first_clip_id' IS DISTINCT FROM 'null'::jsonb OR ledger->'last_clip_id' IS DISTINCT FROM 'null'::jsonb))
    OR (d.source_clip_count>0 AND ((ledger->>'first_clip_id')::BIGINT IS DISTINCT FROM (SELECT clip_id FROM recording_joined_sources WHERE stream_day_id=d.id ORDER BY day_ordinal LIMIT 1)
      OR (ledger->>'last_clip_id')::BIGINT IS DISTINCT FROM (SELECT clip_id FROM recording_joined_sources WHERE stream_day_id=d.id ORDER BY day_ordinal DESC LIMIT 1)))
    OR source_count <> d.source_clip_count OR source_bytes <> d.source_bytes
    OR d.scheduled_start_at <> expected_day_start OR d.scheduled_end_at <> expected_day_end
    OR (source_count > 0 AND ((SELECT min(day_ordinal) FROM recording_joined_sources WHERE stream_day_id = d.id) <> 1
      OR (SELECT max(day_ordinal) FROM recording_joined_sources WHERE stream_day_id = d.id) <> source_count))
    OR EXISTS(SELECT 1 FROM (SELECT day_ordinal,row_number() OVER (ORDER BY start_at,clip_id) AS expected_ordinal
      FROM recording_joined_sources WHERE stream_day_id=d.id) ordered WHERE day_ordinal<>expected_ordinal)
    OR (SELECT count(*) FROM recording_joined_hours WHERE stream_day_id = d.id) <> 12
    OR EXISTS(SELECT 1 FROM recording_joined_hours h
      JOIN recording_joined_batch_recordings priority_recording ON priority_recording.id=d.batch_recording_id
      LEFT JOIN LATERAL (SELECT count(*)::INTEGER AS clip_count, COALESCE(sum(size_bytes), 0)::BIGINT AS bytes
        FROM recording_joined_sources s WHERE s.hour_record_id = h.id) actual ON TRUE
      WHERE h.stream_day_id = d.id AND (h.source_clip_count <> actual.clip_count OR h.source_bytes <> actual.bytes
        OR h.priority_ordinal<>(priority_recording.priority_ordinal-1)*168+(d.date_ordinal-1)*12+h.delivery_hour
        OR h.local_date<>d.local_date OR h.clock_hour<>h.delivery_hour+7
        OR h.scheduled_start_at<>make_timestamptz(extract(year FROM d.local_date)::INTEGER,
          extract(month FROM d.local_date)::INTEGER,extract(day FROM d.local_date)::INTEGER,h.clock_hour,0,0,recording_timezone)
        OR h.scheduled_end_at<>make_timestamptz(extract(year FROM d.local_date)::INTEGER,
          extract(month FROM d.local_date)::INTEGER,extract(day FROM d.local_date)::INTEGER,h.clock_hour+1,0,0,recording_timezone)))
    OR EXISTS(SELECT 1 FROM recording_joined_sources s WHERE s.stream_day_id=d.id
      AND ((s.start_at AT TIME ZONE recording_timezone)::DATE<>d.local_date
        OR (s.end_at AT TIME ZONE recording_timezone)::DATE<>d.local_date))
    OR (SELECT count(*) FROM recording_joined_day_boundaries WHERE stream_day_id = d.id AND boundary_kind = 'cross_hour') <> 11
    OR (SELECT count(*) FROM recording_joined_day_boundaries WHERE stream_day_id = d.id AND boundary_kind = 'cross_day') <> 2
  THEN RAISE EXCEPTION 'joined stream-day cardinality differs'; END IF;

  IF EXISTS(
    SELECT 1 FROM recording_joined_sources s
    LEFT JOIN LATERAL (
      SELECT value AS source FROM jsonb_array_elements(ledger->'sources') WITH ORDINALITY item(value, ordinal)
      WHERE ordinal=s.day_ordinal
    ) js ON TRUE
    WHERE s.stream_day_id=d.id AND (js.source IS NULL
      OR (js.source->>'clip_id')::BIGINT IS DISTINCT FROM s.clip_id
      OR (js.source->>'recording_id')::BIGINT IS DISTINCT FROM s.recording_id
      OR (js.source->>'recording_job_id')::BIGINT IS DISTINCT FROM s.recording_job_id
      OR js.source->>'provider' IS DISTINCT FROM s.provider OR js.source->>'endpoint' IS DISTINCT FROM s.endpoint
      OR js.source->>'region' IS DISTINCT FROM s.region OR js.source->>'bucket' IS DISTINCT FROM s.bucket
      OR (js.source->>'start_utc')::TIMESTAMPTZ IS DISTINCT FROM s.start_at
      OR (js.source->>'end_utc')::TIMESTAMPTZ IS DISTINCT FROM s.end_at
      OR js.source->'object'->>'key' IS DISTINCT FROM s.object_key
      OR js.source->'object'->>'etag' IS DISTINCT FROM s.etag
      OR COALESCE(js.source->'object'->>'version_id','') IS DISTINCT FROM s.version_id
      OR (js.source->'object'->>'size_bytes')::BIGINT IS DISTINCT FROM s.size_bytes
      OR js.source->'object'->>'sha256' IS DISTINCT FROM s.sha256
      OR js.source->'seam_to_previous' IS DISTINCT FROM s.seam_to_previous
      OR s.audio_contract IS NOT NULL)
  ) THEN RAISE EXCEPTION 'joined canonical ledger sources differ'; END IF;
  IF EXISTS(
    SELECT 1 FROM recording_joined_hours h
    LEFT JOIN LATERAL (
      SELECT value AS hour FROM jsonb_array_elements(ledger->'hours') WITH ORDINALITY item(value, ordinal)
      WHERE ordinal=h.delivery_hour
    ) jh ON TRUE
    WHERE h.stream_day_id=d.id AND (jh.hour IS NULL
      OR (jh.hour->>'delivery_hour')::INTEGER IS DISTINCT FROM h.delivery_hour
      OR (jh.hour->>'clock_hour')::INTEGER IS DISTINCT FROM h.clock_hour
      OR jh.hour->'source_clip_ids' IS DISTINCT FROM (SELECT COALESCE(jsonb_agg(s.clip_id ORDER BY s.day_ordinal),'[]'::jsonb)
        FROM recording_joined_sources s WHERE s.hour_record_id=h.id)
      OR ledger->'hour_source_claim_sha256'->>(h.delivery_hour-1) IS DISTINCT FROM h.source_claim_sha256)
  ) THEN RAISE EXCEPTION 'joined canonical ledger hours differ'; END IF;
  IF EXISTS(
    SELECT 1 FROM recording_joined_sources previous
    JOIN recording_joined_sources next ON next.stream_day_id=previous.stream_day_id AND next.day_ordinal=previous.day_ordinal+1
    LEFT JOIN LATERAL (
      SELECT value AS pair FROM jsonb_array_elements(ledger->'consecutive_pairs') WITH ORDINALITY item(value, ordinal)
      WHERE ordinal=previous.day_ordinal
    ) jp ON TRUE
    WHERE previous.stream_day_id=d.id AND (jp.pair IS NULL
      OR (jp.pair->>'previous_clip_id')::BIGINT IS DISTINCT FROM previous.clip_id
      OR (jp.pair->>'next_clip_id')::BIGINT IS DISTINCT FROM next.clip_id
      OR (jp.pair->>'previous_presentation_end_utc')::TIMESTAMPTZ IS DISTINCT FROM previous.end_at
      OR (jp.pair->>'next_presentation_start_utc')::TIMESTAMPTZ IS DISTINCT FROM next.start_at
      OR (jp.pair->>'signed_gap_nanoseconds')::BIGINT IS DISTINCT FROM (extract(epoch FROM (next.start_at-previous.end_at))*1000000000)::BIGINT)
  ) THEN RAISE EXCEPTION 'joined canonical ledger pairs differ'; END IF;

  FOR b IN SELECT * FROM recording_joined_day_boundaries
    WHERE stream_day_id = d.id AND boundary_kind = 'cross_hour' ORDER BY ordinal
  LOOP
    expected_at := make_timestamptz(extract(year FROM d.local_date)::INTEGER,
      extract(month FROM d.local_date)::INTEGER,extract(day FROM d.local_date)::INTEGER,8+b.ordinal,0,0,recording_timezone);
    IF b.scheduled_at <> expected_at THEN RAISE EXCEPTION 'joined hour boundary schedule differs'; END IF;
    IF NOT EXISTS(SELECT 1 FROM jsonb_array_elements(ledger->'cross_hour_boundaries') WITH ORDINALITY item(value,ordinal)
      WHERE ordinal=b.ordinal AND (value->>'previous_delivery_hour')::INTEGER=b.previous_delivery_hour
        AND (value->>'next_delivery_hour')::INTEGER=b.next_delivery_hour
        AND (value->>'previous_clip_id')::BIGINT IS NOT DISTINCT FROM b.previous_clip_id
        AND (value->>'next_clip_id')::BIGINT IS NOT DISTINCT FROM b.next_clip_id
        AND (value->>'previous_presentation_end_utc')::TIMESTAMPTZ IS NOT DISTINCT FROM b.previous_presentation_end_at
        AND (value->>'next_presentation_start_utc')::TIMESTAMPTZ IS NOT DISTINCT FROM b.next_presentation_start_at
        AND (value->>'signed_gap_nanoseconds')::BIGINT IS NOT DISTINCT FROM b.signed_gap_nanoseconds
        AND (value->>'scheduled_utc')::TIMESTAMPTZ=b.scheduled_at
        AND (value->>'actual_seam_utc')::TIMESTAMPTZ IS NOT DISTINCT FROM b.actual_seam_at
        AND (value->>'boundary_skew_nanoseconds')::BIGINT IS NOT DISTINCT FROM b.boundary_skew_nanoseconds
        AND value->>'allocation_decision'=b.allocation_decision AND value->>'verdict'=b.verdict AND value->>'reason'=b.reason)
    THEN RAISE EXCEPTION 'joined canonical hour boundary differs'; END IF;
    SELECT candidate_position, candidate_at, candidate_clip_id
      INTO expected_position, expected_at, expected_clip
      FROM (
        SELECT day_ordinal - 1 AS candidate_position, start_at AS candidate_at, clip_id AS candidate_clip_id
          FROM recording_joined_sources WHERE stream_day_id = d.id AND day_ordinal - 1 >= last_position
        UNION ALL
        SELECT day_ordinal, end_at, clip_id FROM recording_joined_sources
          WHERE stream_day_id = d.id AND day_ordinal >= last_position
      ) candidates
      ORDER BY abs((extract(epoch FROM (candidate_at - b.scheduled_at)) * 1000000)::BIGINT),
        candidate_at, candidate_position, candidate_clip_id LIMIT 1;
    IF expected_position IS NULL THEN expected_position := 0; END IF;
    IF expected_position <> (SELECT count(*) FROM recording_joined_sources s JOIN recording_joined_hours h
      ON h.id=s.hour_record_id WHERE s.stream_day_id=d.id AND h.delivery_hour<=b.ordinal)
    THEN RAISE EXCEPTION 'joined hour allocation position differs'; END IF;
    SELECT clip_id, end_at INTO expected_prev, expected_prev_end FROM recording_joined_sources
      WHERE stream_day_id = d.id AND day_ordinal = expected_position;
    SELECT clip_id, start_at INTO expected_next, expected_next_start FROM recording_joined_sources
      WHERE stream_day_id = d.id AND day_ordinal = expected_position + 1;
    IF expected_prev IS NOT NULL AND expected_next IS NOT NULL THEN
      expected_gap := (extract(epoch FROM (expected_next_start - expected_prev_end)) * 1000000000)::BIGINT;
    ELSE expected_gap := NULL; END IF;
    IF b.previous_clip_id IS DISTINCT FROM expected_prev OR b.next_clip_id IS DISTINCT FROM expected_next
      OR b.previous_presentation_end_at IS DISTINCT FROM expected_prev_end
      OR b.next_presentation_start_at IS DISTINCT FROM expected_next_start
      OR b.signed_gap_nanoseconds IS DISTINCT FROM expected_gap
      OR (expected_prev IS NOT NULL AND expected_next IS NOT NULL AND b.actual_seam_at IS DISTINCT FROM expected_at)
      OR (expected_prev IS NOT NULL AND expected_next IS NOT NULL AND b.boundary_skew_nanoseconds IS DISTINCT FROM
        (extract(epoch FROM (expected_at - b.scheduled_at)) * 1000000000)::BIGINT)
      OR ((expected_prev IS NULL OR expected_next IS NULL) AND
        (b.actual_seam_at IS NOT NULL OR b.boundary_skew_nanoseconds IS NOT NULL))
      OR (expected_prev IS NOT NULL AND expected_next IS NOT NULL AND
        ROW(b.allocation_decision, b.verdict, b.reason) IS DISTINCT FROM ROW('split_before_next_source', 'allocated', 'closest_source_boundary'))
      OR (expected_prev IS NULL AND expected_next IS NOT NULL AND
        ROW(b.allocation_decision, b.verdict, b.reason) IS DISTINCT FROM ROW('no_source_before_boundary', 'absent_source', 'previous_source_absent'))
      OR (expected_prev IS NOT NULL AND expected_next IS NULL AND
        ROW(b.allocation_decision, b.verdict, b.reason) IS DISTINCT FROM ROW('no_source_after_boundary', 'absent_source', 'next_source_absent'))
      OR (expected_prev IS NULL AND expected_next IS NULL AND
        ROW(b.allocation_decision, b.verdict, b.reason) IS DISTINCT FROM ROW('no_sources', 'absent_source', 'both_sources_absent'))
    THEN RAISE EXCEPTION 'joined closest hour boundary differs'; END IF;
    last_position := expected_position;
  END LOOP;

  IF EXISTS(
    SELECT 1 FROM recording_joined_sources s
    JOIN recording_joined_hours h ON h.id=s.hour_record_id
    LEFT JOIN recording_joined_day_boundaries lower_boundary
      ON lower_boundary.stream_day_id=d.id AND lower_boundary.boundary_kind='cross_hour'
        AND lower_boundary.ordinal=h.delivery_hour-1
    LEFT JOIN recording_joined_sources lower_source
      ON lower_source.stream_day_id=d.id AND lower_source.clip_id=lower_boundary.previous_clip_id
    LEFT JOIN recording_joined_day_boundaries upper_boundary
      ON upper_boundary.stream_day_id=d.id AND upper_boundary.boundary_kind='cross_hour'
        AND upper_boundary.ordinal=h.delivery_hour
    LEFT JOIN recording_joined_sources upper_source
      ON upper_source.stream_day_id=d.id AND upper_source.clip_id=upper_boundary.previous_clip_id
    WHERE s.stream_day_id=d.id AND (
      s.day_ordinal<=CASE WHEN h.delivery_hour=1 THEN 0 ELSE COALESCE(lower_source.day_ordinal,0) END
      OR s.day_ordinal>CASE WHEN h.delivery_hour=12 THEN source_count ELSE COALESCE(upper_source.day_ordinal,0) END
      OR s.hour_ordinal<>s.day_ordinal-CASE WHEN h.delivery_hour=1 THEN 0 ELSE COALESCE(lower_source.day_ordinal,0) END)
  ) THEN RAISE EXCEPTION 'joined hour source membership differs'; END IF;

  FOR b IN SELECT * FROM recording_joined_day_boundaries
    WHERE stream_day_id = d.id AND boundary_kind = 'cross_day' ORDER BY ordinal
  LOOP
    IF NOT EXISTS(SELECT 1 FROM jsonb_array_elements(ledger->'cross_day_boundaries') WITH ORDINALITY item(value,ordinal)
      WHERE ordinal=b.ordinal
        AND (value->>'previous_clip_id')::BIGINT IS NOT DISTINCT FROM b.previous_clip_id
        AND (value->>'next_clip_id')::BIGINT IS NOT DISTINCT FROM b.next_clip_id
        AND (value->>'previous_presentation_end_utc')::TIMESTAMPTZ IS NOT DISTINCT FROM b.previous_presentation_end_at
        AND (value->>'next_presentation_start_utc')::TIMESTAMPTZ IS NOT DISTINCT FROM b.next_presentation_start_at
        AND (value->>'signed_gap_nanoseconds')::BIGINT IS NOT DISTINCT FROM b.signed_gap_nanoseconds
        AND (value->>'scheduled_previous_end_utc')::TIMESTAMPTZ=b.scheduled_previous_end_at
        AND (value->>'scheduled_next_start_utc')::TIMESTAMPTZ=b.scheduled_next_start_at
        AND (value->>'boundary_skew_nanoseconds')::BIGINT IS NOT DISTINCT FROM b.boundary_skew_nanoseconds
        AND value->>'allocation_decision'=b.allocation_decision AND value->>'verdict'=b.verdict AND value->>'reason'=b.reason)
    THEN RAISE EXCEPTION 'joined canonical cross-day boundary differs'; END IF;
    expected_prev := NULL; expected_next := NULL; expected_prev_end := NULL; expected_next_start := NULL;
    IF b.ordinal = 1 THEN
      SELECT s.clip_id, s.end_at INTO expected_prev, expected_prev_end
        FROM recording_joined_stream_days previous_day
        JOIN recording_joined_sources s ON s.stream_day_id = previous_day.id
        WHERE previous_day.batch_record_id = d.batch_record_id AND previous_day.recording_id = d.recording_id
          AND previous_day.local_date = d.local_date - 1 ORDER BY s.day_ordinal DESC LIMIT 1;
      SELECT clip_id, start_at INTO expected_next, expected_next_start FROM recording_joined_sources
        WHERE stream_day_id = d.id ORDER BY day_ordinal LIMIT 1;
    ELSE
      SELECT clip_id, end_at INTO expected_prev, expected_prev_end FROM recording_joined_sources
        WHERE stream_day_id = d.id ORDER BY day_ordinal DESC LIMIT 1;
      SELECT s.clip_id, s.start_at INTO expected_next, expected_next_start
        FROM recording_joined_stream_days next_day
        JOIN recording_joined_sources s ON s.stream_day_id = next_day.id
        WHERE next_day.batch_record_id = d.batch_record_id AND next_day.recording_id = d.recording_id
          AND next_day.local_date = d.local_date + 1 ORDER BY s.day_ordinal LIMIT 1;
    END IF;
    IF b.ordinal=1 THEN
      expected_at := make_timestamptz(extract(year FROM d.local_date-1)::INTEGER,
        extract(month FROM d.local_date-1)::INTEGER,extract(day FROM d.local_date-1)::INTEGER,20,0,0,recording_timezone);
      expected_day_start := make_timestamptz(extract(year FROM d.local_date)::INTEGER,
        extract(month FROM d.local_date)::INTEGER,extract(day FROM d.local_date)::INTEGER,8,0,0,recording_timezone);
    ELSE
      expected_at := make_timestamptz(extract(year FROM d.local_date)::INTEGER,
        extract(month FROM d.local_date)::INTEGER,extract(day FROM d.local_date)::INTEGER,20,0,0,recording_timezone);
      expected_day_start := make_timestamptz(extract(year FROM d.local_date+1)::INTEGER,
        extract(month FROM d.local_date+1)::INTEGER,extract(day FROM d.local_date+1)::INTEGER,8,0,0,recording_timezone);
    END IF;
    IF b.scheduled_previous_end_at<>expected_at OR b.scheduled_next_start_at<>expected_day_start
    THEN RAISE EXCEPTION 'joined cross-day schedule differs'; END IF;
    IF expected_prev IS NOT NULL AND expected_next IS NOT NULL THEN
      expected_gap := (extract(epoch FROM (expected_next_start - expected_prev_end)) * 1000000000)::BIGINT;
      expected_skew := expected_gap - (extract(epoch FROM (b.scheduled_next_start_at - b.scheduled_previous_end_at)) * 1000000000)::BIGINT;
    ELSE expected_gap := NULL; expected_skew := NULL; END IF;
    IF b.previous_clip_id IS DISTINCT FROM expected_prev OR b.next_clip_id IS DISTINCT FROM expected_next
      OR b.previous_presentation_end_at IS DISTINCT FROM expected_prev_end
      OR b.next_presentation_start_at IS DISTINCT FROM expected_next_start
      OR b.signed_gap_nanoseconds IS DISTINCT FROM expected_gap
      OR b.boundary_skew_nanoseconds IS DISTINCT FROM expected_skew
      OR b.actual_seam_at IS NOT NULL
      OR (expected_gap IS NOT NULL AND b.verdict <> CASE WHEN expected_gap < 0 THEN 'overlap' ELSE 'scheduled_gap' END)
      OR (expected_gap IS NOT NULL AND ROW(b.allocation_decision, b.reason)
        IS DISTINCT FROM ROW('separate_local_days', 'scheduled_day_boundary'))
      OR (expected_gap IS NULL AND b.verdict <> 'absent_source')
      OR (expected_gap IS NULL AND b.ordinal=1 AND expected_prev IS NULL AND
        ROW(b.allocation_decision,b.reason) IS DISTINCT FROM ROW('no_previous_day_source','previous_source_absent'))
      OR (expected_gap IS NULL AND b.ordinal=1 AND expected_prev IS NOT NULL AND
        ROW(b.allocation_decision,b.reason) IS DISTINCT FROM ROW('empty_day_after_previous_source','next_source_absent'))
      OR (expected_gap IS NULL AND b.ordinal=2 AND expected_next IS NULL AND
        ROW(b.allocation_decision,b.reason) IS DISTINCT FROM ROW('no_next_day_source','next_source_absent'))
      OR (expected_gap IS NULL AND b.ordinal=2 AND expected_next IS NOT NULL AND
        ROW(b.allocation_decision,b.reason) IS DISTINCT FROM ROW('empty_day_before_next_source','previous_source_absent'))
    THEN RAISE EXCEPTION 'joined cross-day boundary differs'; END IF;
  END LOOP;
  RETURN TRUE;
END $$;

CREATE FUNCTION guard_recording_joined_artifact_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE root recording_joined_artifacts%ROWTYPE; h recording_joined_hours%ROWTYPE; d recording_joined_stream_days%ROWTYPE;
  batch recording_joined_batches%ROWTYPE;
BEGIN
  IF NOT EXISTS(SELECT 1 FROM recording_joined_batches b WHERE b.id=NEW.batch_record_id
      AND b.state IN ('frozen','index_sealed') FOR KEY SHARE)
    OR NEW.published_at IS NOT NULL OR NEW.etag IS NOT NULL OR NEW.version_id IS NOT NULL OR NEW.finalized_token IS NOT NULL
    OR NEW.failure_reason_code <> ''
    OR (NEW.artifact_kind<>'media' AND NEW.ordinal<>1)
    OR (NEW.artifact_kind = 'media' AND (NEW.publication_state IS NOT NULL OR NEW.publication_attempt_count <> 0
      OR NEW.publication_token IS NOT NULL OR NEW.publication_claimed_by IS NOT NULL
      OR NEW.publication_lease_expires_at IS NOT NULL OR NEW.publication_heartbeat_at IS NOT NULL))
    OR (NEW.artifact_kind IN ('allocation_ledger','batch_index') AND
      (NEW.publication_state <> 'sealed' OR NEW.publication_attempt_count <> 0 OR NEW.publication_token IS NOT NULL
        OR NEW.publication_claimed_by IS NOT NULL OR NEW.publication_lease_expires_at IS NOT NULL
        OR NEW.publication_heartbeat_at IS NOT NULL))
    OR (NEW.artifact_kind = 'hour_manifest' AND NOT (
      (NEW.publication_state='sealed' AND NEW.publication_attempt_count=0 AND NEW.publication_token IS NULL
        AND NEW.publication_claimed_by IS NULL AND NEW.publication_lease_expires_at IS NULL
        AND NEW.publication_heartbeat_at IS NULL)
      OR (NEW.publication_state='publishing' AND NEW.publication_attempt_count=1 AND NEW.publication_token IS NOT NULL
        AND NEW.publication_claimed_by IS NOT NULL AND NEW.publication_lease_expires_at>now()
        AND NEW.publication_heartbeat_at IS NOT NULL)))
    OR EXISTS(SELECT 1 FROM recording_joined_artifacts existing
      WHERE existing.connection_id=NEW.connection_id AND existing.object_key=NEW.object_key
        AND ROW(existing.expected_size_bytes,existing.expected_sha256,existing.content_type)
          IS DISTINCT FROM ROW(NEW.expected_size_bytes,NEW.expected_sha256,NEW.content_type))
  THEN RAISE EXCEPTION 'joined artifact initial publication identity differs'; END IF;

  IF (NEW.artifact_kind = 'allocation_ledger' AND
        (NEW.relative_path !~ '^coverage/ledgers/[1-9][0-9]*/[0-9]{4}-[0-9]{2}-[0-9]{2}\.json$'
          OR NEW.object_key <> 'joined/' || NEW.batch_id || '/' || NEW.relative_path))
    OR (NEW.artifact_kind = 'hour_manifest' AND
        (NEW.relative_path <> 'coverage/hours/' || NEW.scope_id || '.json'
          OR NEW.object_key <> 'joined/' || NEW.batch_id || '/' || NEW.relative_path))
    OR (NEW.artifact_kind = 'media' AND
        (NEW.object_key <> 'joined/' || NEW.batch_id || '/objects/' || NEW.expected_sha256 || '.mp4'))
    OR (NEW.artifact_kind = 'batch_index' AND
        (NEW.relative_path <> 'coverage/batch.json' OR NEW.object_key <> 'joined/' || NEW.batch_id || '/coverage/batch.json'))
  THEN RAISE EXCEPTION 'joined artifact canonical path differs'; END IF;

  IF NEW.artifact_kind = 'media' THEN
    SELECT * INTO STRICT h FROM recording_joined_hours WHERE id = NEW.hour_record_id FOR KEY SHARE;
    IF h.state <> 'leased' OR h.lease_expires_at <= now()
      OR ROW(h.batch_record_id, h.stream_day_id, h.account_id, h.connection_id, h.hour_id)
        IS DISTINCT FROM ROW(NEW.batch_record_id, NEW.stream_day_id, NEW.account_id, NEW.connection_id, NEW.scope_id)
    THEN RAISE EXCEPTION 'joined media scope differs'; END IF;
  ELSIF NEW.artifact_kind = 'allocation_ledger' THEN
    SELECT * INTO STRICT d FROM recording_joined_stream_days WHERE id = NEW.stream_day_id FOR KEY SHARE;
    SELECT * INTO STRICT batch FROM recording_joined_batches WHERE id = NEW.batch_record_id FOR KEY SHARE;
    IF NOT validate_recording_joined_stream_day(d.id)
      OR NEW.scope_id<>NEW.batch_id || '__recording-' || d.recording_id || '__date-' || d.local_date ||
        '__generation-' || batch.generation
      OR NEW.relative_path<>'coverage/ledgers/' || d.recording_id || '/' || d.local_date || '.json'
      OR NEW.object_key<>'joined/' || NEW.batch_id || '/' || NEW.relative_path
      OR ROW(d.batch_record_id, d.account_id, d.connection_id, d.ledger_artifact_sha256, octet_length(d.ledger_bytes), d.ledger_bytes)
        IS DISTINCT FROM ROW(NEW.batch_record_id, NEW.account_id, NEW.connection_id, NEW.expected_sha256,
          NEW.expected_size_bytes::INTEGER, NEW.canonical_bytes)
    THEN RAISE EXCEPTION 'joined allocation ledger artifact differs'; END IF;
  ELSIF NEW.artifact_kind = 'hour_manifest' THEN
    SELECT * INTO STRICT h FROM recording_joined_hours WHERE id = NEW.hour_record_id FOR KEY SHARE;
    IF h.state <> 'sealed' OR ROW(h.batch_record_id, h.stream_day_id, h.account_id, h.connection_id, h.hour_id,
        h.manifest_sha256, octet_length(h.manifest_bytes), h.manifest_bytes)
      IS DISTINCT FROM ROW(NEW.batch_record_id, NEW.stream_day_id, NEW.account_id, NEW.connection_id, NEW.scope_id,
        NEW.expected_sha256, NEW.expected_size_bytes::INTEGER, NEW.canonical_bytes)
    THEN RAISE EXCEPTION 'joined hour manifest artifact differs'; END IF;
  ELSIF NEW.artifact_kind = 'batch_index' THEN
    IF NEW.scope_id <> NEW.batch_id OR EXISTS(SELECT 1 FROM recording_joined_artifacts a
      WHERE a.batch_record_id = NEW.batch_record_id AND a.artifact_kind <> 'batch_index'
        AND (a.artifact_kind <> 'media' AND a.publication_state <> 'published'
          OR a.artifact_kind = 'media' AND a.published_at IS NULL))
    THEN RAISE EXCEPTION 'joined batch index sealed before cloud publication'; END IF;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_artifact_insert_guard BEFORE INSERT ON recording_joined_artifacts
FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_artifact_insert();

CREATE FUNCTION guard_recording_joined_media_source_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE a recording_joined_artifacts%ROWTYPE; s recording_joined_sources%ROWTYPE;
BEGIN
  SELECT * INTO STRICT a FROM recording_joined_artifacts WHERE id=NEW.artifact_id FOR KEY SHARE;
  SELECT * INTO STRICT s FROM recording_joined_sources WHERE id=NEW.source_id FOR KEY SHARE;
  IF a.artifact_kind <> 'media' OR a.hour_record_id <> s.hour_record_id
    OR NEW.ordinal <> (SELECT count(*)+1 FROM recording_joined_media_sources existing WHERE existing.artifact_id=a.id)
  THEN RAISE EXCEPTION 'joined media source membership differs'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_media_source_insert_guard BEFORE INSERT ON recording_joined_media_sources
FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_media_source_insert();

CREATE FUNCTION guard_recording_joined_disposition_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE s recording_joined_sources%ROWTYPE; a recording_joined_artifacts%ROWTYPE;
BEGIN
  SELECT * INTO STRICT s FROM recording_joined_sources WHERE id=NEW.source_id FOR KEY SHARE;
  IF s.hour_record_id <> NEW.hour_record_id THEN RAISE EXCEPTION 'joined disposition source differs'; END IF;
  IF NEW.disposition='included' THEN
    SELECT * INTO STRICT a FROM recording_joined_artifacts WHERE id=NEW.media_artifact_id FOR KEY SHARE;
    IF a.artifact_kind <> 'media' OR a.hour_record_id <> NEW.hour_record_id OR a.ordinal <> NEW.media_ordinal
      OR NOT EXISTS(SELECT 1 FROM recording_joined_media_sources ms
        WHERE ms.artifact_id=a.id AND ms.source_id=NEW.source_id)
    THEN RAISE EXCEPTION 'joined included disposition differs'; END IF;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_disposition_insert_guard BEFORE INSERT ON recording_joined_hour_dispositions
FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_disposition_insert();

CREATE FUNCTION guard_recording_joined_ack_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE a recording_joined_artifacts%ROWTYPE;
BEGIN
  SELECT * INTO STRICT a FROM recording_joined_artifacts WHERE id = NEW.artifact_id FOR UPDATE;
  IF a.connection_id <> NEW.connection_id
    OR ((a.artifact_kind <> 'media' AND a.publication_state IS DISTINCT FROM 'published')
      OR (a.artifact_kind = 'media' AND a.published_at IS NULL))
    OR ROW(a.relative_path, a.expected_size_bytes, a.expected_sha256)
      IS DISTINCT FROM ROW(NEW.relative_path, NEW.size_bytes, NEW.sha256)
    OR NEW.verified_at < a.published_at
    OR (a.artifact_kind='hour_manifest' AND NOT EXISTS(SELECT 1 FROM recording_joined_artifacts ledger
      JOIN recording_joined_artifact_acks ack ON ack.artifact_id=ledger.id AND ack.connection_id=a.connection_id
      WHERE ledger.stream_day_id=a.stream_day_id AND ledger.artifact_kind='allocation_ledger'))
    OR (a.artifact_kind='media' AND NOT EXISTS(SELECT 1 FROM recording_joined_artifacts manifest
      JOIN recording_joined_artifact_acks ack ON ack.artifact_id=manifest.id AND ack.connection_id=a.connection_id
      WHERE manifest.hour_record_id=a.hour_record_id AND manifest.artifact_kind='hour_manifest'))
    OR (a.artifact_kind='batch_index' AND EXISTS(SELECT 1 FROM recording_joined_artifacts prior
      LEFT JOIN recording_joined_artifact_acks ack ON ack.artifact_id=prior.id AND ack.connection_id=a.connection_id
      WHERE prior.batch_record_id=a.batch_record_id AND prior.artifact_kind<>'batch_index' AND ack.artifact_id IS NULL))
  THEN RAISE EXCEPTION 'joined artifact acknowledgment differs'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_ack_insert_guard BEFORE INSERT ON recording_joined_artifact_acks
FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_ack_insert();

ALTER TABLE connections ADD CONSTRAINT connections_joined_last_attempt_artifact_fk
  FOREIGN KEY (joined_last_attempt_artifact_id) REFERENCES recording_joined_artifacts(id) ON DELETE RESTRICT;

CREATE FUNCTION guard_recording_joined_hour_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF ROW(NEW.batch_record_id, NEW.stream_day_id, NEW.account_id, NEW.connection_id, NEW.batch_id, NEW.recording_id,
      NEW.hour_id, NEW.local_date, NEW.delivery_hour, NEW.clock_hour, NEW.scheduled_start_at, NEW.scheduled_end_at,
      NEW.priority_ordinal, NEW.source_clip_count, NEW.source_bytes, NEW.source_claim_sha256, NEW.created_at)
    IS DISTINCT FROM ROW(OLD.batch_record_id, OLD.stream_day_id, OLD.account_id, OLD.connection_id, OLD.batch_id, OLD.recording_id,
      OLD.hour_id, OLD.local_date, OLD.delivery_hour, OLD.clock_hour, OLD.scheduled_start_at, OLD.scheduled_end_at,
      OLD.priority_ordinal, OLD.source_clip_count, OLD.source_bytes, OLD.source_claim_sha256, OLD.created_at)
  THEN RAISE EXCEPTION 'joined hour identity is immutable'; END IF;

  IF OLD.state = 'pending' AND NEW.state = 'leased' THEN
    IF OLD.source_clip_count=0 OR NEW.attempt_count <> OLD.attempt_count + 1 OR NEW.lease_expires_at <= now()
      OR NEW.failure_reason_code<>OLD.failure_reason_code
      OR ROW(NEW.source_only_sha256, NEW.canonical_plan, NEW.manifest_bytes, NEW.manifest_sha256, NEW.sealed_at)
        IS DISTINCT FROM ROW(OLD.source_only_sha256, OLD.canonical_plan, OLD.manifest_bytes, OLD.manifest_sha256, OLD.sealed_at)
    THEN RAISE EXCEPTION 'invalid joined preflight claim'; END IF;
  ELSIF OLD.state='pending' AND NEW.state='sealed' THEN
    IF OLD.source_clip_count<>0 OR NEW.attempt_count<>0 OR NEW.source_only_sha256 IS NULL
      OR NEW.canonical_plan IS NULL OR NEW.manifest_bytes IS NULL OR NEW.manifest_sha256 IS NULL OR NEW.sealed_at IS NULL
      OR NEW.failure_reason_code<>'' OR NEW.source_only_sha256<>OLD.source_claim_sha256
      OR ARRAY(SELECT jsonb_object_keys(NEW.canonical_plan) ORDER BY 1) IS DISTINCT FROM ARRAY[
        'allocation_ledger_sha256','batch_id','coverage_object_key','expected_output_count','folder_name','gap_only',
        'gap_only_reason','gaps','generation','hour_id','local_date','local_hour','media_tool','naming_metadata','outputs',
        'policy_version','qualification_window','quarantine_reason_code','quarantined_sources','recording_id','schema_version',
        'source_claim_sha256','sources','timezone']::TEXT[]
      OR NEW.canonical_plan->>'hour_id' IS DISTINCT FROM OLD.hour_id
      OR NEW.canonical_plan->>'batch_id' IS DISTINCT FROM OLD.batch_id
      OR (NEW.canonical_plan->>'recording_id')::BIGINT IS DISTINCT FROM OLD.recording_id
      OR (NEW.canonical_plan->>'local_date')::DATE IS DISTINCT FROM OLD.local_date
      OR (NEW.canonical_plan->>'local_hour')::INTEGER IS DISTINCT FROM OLD.delivery_hour
      OR NEW.canonical_plan->>'source_claim_sha256' IS DISTINCT FROM OLD.source_claim_sha256
      OR NEW.canonical_plan->>'gap_only' IS DISTINCT FROM 'true'
      OR NEW.canonical_plan->>'gap_only_reason' IS DISTINCT FROM 'scheduled_source_gap'
      OR NEW.canonical_plan->>'quarantine_reason_code' IS DISTINCT FROM ''
      OR COALESCE((NEW.canonical_plan->>'expected_output_count')::INTEGER,-1)<>0
      OR COALESCE(jsonb_array_length(NEW.canonical_plan->'sources'),-1)<>0
      OR NEW.canonical_plan->'quarantined_sources' IS DISTINCT FROM 'null'::jsonb
      OR COALESCE(jsonb_array_length(NEW.canonical_plan->'outputs'),-1)<>0
      OR COALESCE(jsonb_array_length(NEW.canonical_plan->'gaps'),-1)<>0
      OR ARRAY(SELECT jsonb_object_keys(convert_from(NEW.manifest_bytes,'UTF8')::jsonb) ORDER BY 1) IS DISTINCT FROM ARRAY[
        'allocation','batch_id','clock_hour','delivery_hour','gaps','hour_id','local_date','media','media_tool',
        'policy_version','qualification_day','qualification_sha256','quarantine_evidence','quarantine_reason_code',
        'recording_id','scheduled_end_utc','scheduled_gap','scheduled_start_utc','schema_version','source_claim_sha256',
        'source_count','source_dispositions','sources','status','timezone']::TEXT[]
      OR ARRAY(SELECT jsonb_object_keys(convert_from(NEW.manifest_bytes,'UTF8')::jsonb->'scheduled_gap') ORDER BY 1)
        IS DISTINCT FROM ARRAY['no_allocatable_sources','reason_code','signed_gap_nanoseconds']::TEXT[]
      OR convert_from(NEW.manifest_bytes,'UTF8')::jsonb->>'status' IS DISTINCT FROM 'gap_only'
      OR convert_from(NEW.manifest_bytes,'UTF8')::jsonb->>'hour_id' IS DISTINCT FROM OLD.hour_id
      OR convert_from(NEW.manifest_bytes,'UTF8')::jsonb->>'batch_id' IS DISTINCT FROM OLD.batch_id
      OR (convert_from(NEW.manifest_bytes,'UTF8')::jsonb->>'recording_id')::BIGINT IS DISTINCT FROM OLD.recording_id
      OR (convert_from(NEW.manifest_bytes,'UTF8')::jsonb->>'local_date')::DATE IS DISTINCT FROM OLD.local_date
      OR (convert_from(NEW.manifest_bytes,'UTF8')::jsonb->>'delivery_hour')::INTEGER IS DISTINCT FROM OLD.delivery_hour
      OR convert_from(NEW.manifest_bytes,'UTF8')::jsonb->>'source_claim_sha256' IS DISTINCT FROM OLD.source_claim_sha256
      OR COALESCE((convert_from(NEW.manifest_bytes,'UTF8')::jsonb->>'source_count')::INTEGER,-1)<>0
      OR COALESCE(jsonb_array_length(convert_from(NEW.manifest_bytes,'UTF8')::jsonb->'sources'),-1)<>0
      OR COALESCE(jsonb_array_length(convert_from(NEW.manifest_bytes,'UTF8')::jsonb->'source_dispositions'),-1)<>0
      OR COALESCE(jsonb_array_length(convert_from(NEW.manifest_bytes,'UTF8')::jsonb->'media'),-1)<>0
      OR COALESCE(jsonb_array_length(convert_from(NEW.manifest_bytes,'UTF8')::jsonb->'gaps'),-1)<>0
      OR convert_from(NEW.manifest_bytes,'UTF8')::jsonb->'scheduled_gap'->>'reason_code' IS DISTINCT FROM 'scheduled_source_gap'
      OR convert_from(NEW.manifest_bytes,'UTF8')::jsonb->'scheduled_gap'->>'no_allocatable_sources' IS DISTINCT FROM 'true'
    THEN RAISE EXCEPTION 'invalid joined server gap-only seal'; END IF;
  ELSIF OLD.state = 'leased' AND NEW.state = 'leased' THEN
    IF OLD.source_clip_count=0 THEN RAISE EXCEPTION 'source-free hour cannot use worker lease';
    ELSIF OLD.lease_expires_at <= now() THEN
      IF NEW.claim_token = OLD.claim_token OR NEW.attempt_count <> OLD.attempt_count + 1
        OR NEW.heartbeat_at <= OLD.heartbeat_at OR NEW.lease_expires_at <= now()
        OR NEW.failure_reason_code<>OLD.failure_reason_code
      THEN RAISE EXCEPTION 'invalid joined preflight reclaim'; END IF;
    ELSIF NEW.claim_token <> OLD.claim_token OR NEW.claimed_by <> OLD.claimed_by
      OR NEW.attempt_count <> OLD.attempt_count OR NEW.heartbeat_at <= OLD.heartbeat_at
      OR NEW.lease_expires_at <= OLD.lease_expires_at
    THEN RAISE EXCEPTION 'joined preflight lease is fenced'; END IF;
  ELSIF OLD.state = 'leased' AND NEW.state = 'pending' THEN
    IF OLD.lease_expires_at > now() OR NEW.next_attempt_at <= now() OR NEW.attempt_count <> OLD.attempt_count
    THEN RAISE EXCEPTION 'invalid joined preflight release'; END IF;
  ELSIF OLD.state = 'leased' AND NEW.state = 'sealed' THEN
    IF OLD.source_clip_count=0 OR OLD.lease_expires_at <= now() OR NEW.attempt_count <> OLD.attempt_count OR NEW.source_only_sha256 IS NULL
      OR NEW.canonical_plan IS NULL OR NEW.manifest_bytes IS NULL OR NEW.manifest_sha256 IS NULL OR NEW.sealed_at IS NULL
      OR NEW.failure_reason_code<>OLD.failure_reason_code
    THEN RAISE EXCEPTION 'invalid joined hour seal'; END IF;
  ELSIF OLD.state = 'leased' AND NEW.state = 'terminal_failed' THEN
    IF OLD.lease_expires_at <= now() OR NEW.failure_reason_code = '' OR NEW.attempt_count <> OLD.attempt_count
    THEN RAISE EXCEPTION 'invalid joined terminal preflight failure'; END IF;
  ELSE RAISE EXCEPTION 'invalid joined hour transition'; END IF;
  NEW.updated_at := now(); RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_hour_update_guard BEFORE UPDATE ON recording_joined_hours
FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_hour_update();

CREATE FUNCTION validate_recording_joined_hour_seal(p_hour_record_id BIGINT) RETURNS BOOLEAN LANGUAGE plpgsql AS $$
DECLARE h recording_joined_hours%ROWTYPE; source_count INTEGER; media_count INTEGER; disposition_count INTEGER;
BEGIN
  SELECT * INTO STRICT h FROM recording_joined_hours WHERE id=p_hour_record_id;
  SELECT count(*) INTO media_count FROM recording_joined_artifacts
    WHERE hour_record_id=h.id AND artifact_kind='media';
  SELECT count(*) INTO disposition_count FROM recording_joined_hour_dispositions WHERE hour_record_id=h.id;
  IF h.state <> 'sealed' THEN
    IF media_count <> 0 OR disposition_count <> 0 OR EXISTS(SELECT 1 FROM recording_joined_artifacts
      WHERE hour_record_id=h.id AND artifact_kind='hour_manifest')
    THEN RAISE EXCEPTION 'joined hour children committed without a seal'; END IF;
    RETURN TRUE;
  END IF;
  SELECT count(*) INTO source_count FROM recording_joined_sources WHERE hour_record_id=h.id;
  IF (SELECT count(*) FROM recording_joined_artifacts WHERE hour_record_id=h.id AND artifact_kind='hour_manifest') <> 1
    OR media_count <> COALESCE((h.canonical_plan->>'expected_output_count')::INTEGER,-1)
    OR disposition_count <> source_count
    OR EXISTS(SELECT 1 FROM recording_joined_sources s WHERE s.hour_record_id=h.id
      AND NOT EXISTS(SELECT 1 FROM recording_joined_hour_dispositions d
        WHERE d.hour_record_id=h.id AND d.source_id=s.id))
    OR EXISTS(
      SELECT 1 FROM jsonb_array_elements(h.canonical_plan->'outputs') WITH ORDINALITY planned(output,ordinal)
      FULL JOIN (SELECT * FROM recording_joined_artifacts WHERE hour_record_id=h.id AND artifact_kind='media') a
        ON a.ordinal=planned.ordinal
      WHERE a.id IS NULL OR planned.output IS NULL
        OR ROW(a.relative_path,a.object_key,a.content_id,a.expected_size_bytes,a.expected_sha256)
          IS DISTINCT FROM ROW(planned.output->>'relative_path',planned.output->>'object_key',planned.output->>'content_id',
            (planned.output->>'expected_size_bytes')::BIGINT,planned.output->>'expected_sha256'))
    OR EXISTS(
      SELECT 1 FROM recording_joined_artifacts a
      JOIN LATERAL jsonb_array_elements(h.canonical_plan->'outputs') WITH ORDINALITY planned(output,ordinal)
        ON planned.ordinal=a.ordinal
      WHERE a.hour_record_id=h.id AND a.artifact_kind='media' AND
        ((SELECT count(*) FROM recording_joined_media_sources ms WHERE ms.artifact_id=a.id)
          <> jsonb_array_length(planned.output->'sources')
         OR EXISTS(SELECT 1 FROM jsonb_array_elements(planned.output->'sources') WITH ORDINALITY ps(source,ordinal)
           LEFT JOIN recording_joined_media_sources ms ON ms.artifact_id=a.id AND ms.ordinal=ps.ordinal
           LEFT JOIN recording_joined_sources s ON s.id=ms.source_id
           WHERE s.clip_id IS DISTINCT FROM (ps.source->>'clip_id')::BIGINT)))
  THEN RAISE EXCEPTION 'joined hour seal is incomplete or differs from canonical plan'; END IF;
  RETURN TRUE;
END $$;

CREATE FUNCTION check_recording_joined_hour_seal() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE hour_id BIGINT;
BEGIN
  hour_id := CASE TG_TABLE_NAME
    WHEN 'recording_joined_hours' THEN (to_jsonb(NEW)->>'id')::BIGINT
    WHEN 'recording_joined_artifacts' THEN (to_jsonb(NEW)->>'hour_record_id')::BIGINT
    WHEN 'recording_joined_media_sources' THEN (SELECT hour_record_id FROM recording_joined_artifacts
      WHERE id=(to_jsonb(NEW)->>'artifact_id')::BIGINT)
    ELSE (to_jsonb(NEW)->>'hour_record_id')::BIGINT END;
  IF hour_id IS NOT NULL THEN PERFORM validate_recording_joined_hour_seal(hour_id); END IF;
  RETURN NEW;
END $$;
CREATE CONSTRAINT TRIGGER recording_joined_hour_seal_complete AFTER UPDATE ON recording_joined_hours
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_recording_joined_hour_seal();
CREATE CONSTRAINT TRIGGER recording_joined_artifact_hour_seal_complete AFTER INSERT ON recording_joined_artifacts
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_recording_joined_hour_seal();
CREATE CONSTRAINT TRIGGER recording_joined_media_source_seal_complete AFTER INSERT ON recording_joined_media_sources
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_recording_joined_hour_seal();
CREATE CONSTRAINT TRIGGER recording_joined_disposition_seal_complete AFTER INSERT ON recording_joined_hour_dispositions
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_recording_joined_hour_seal();

CREATE FUNCTION guard_recording_joined_artifact_update() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE root recording_joined_artifacts%ROWTYPE;
BEGIN
  IF ROW(NEW.batch_record_id, NEW.account_id, NEW.connection_id, NEW.batch_id, NEW.scope_kind, NEW.scope_id,
      NEW.stream_day_id, NEW.hour_record_id, NEW.artifact_kind, NEW.ordinal, NEW.relative_path,
      NEW.object_key, NEW.content_type, NEW.content_id, NEW.expected_size_bytes, NEW.expected_sha256,
      NEW.canonical_bytes, NEW.created_at)
    IS DISTINCT FROM ROW(OLD.batch_record_id, OLD.account_id, OLD.connection_id, OLD.batch_id, OLD.scope_kind, OLD.scope_id,
      OLD.stream_day_id, OLD.hour_record_id, OLD.artifact_kind, OLD.ordinal, OLD.relative_path,
      OLD.object_key, OLD.content_type, OLD.content_id, OLD.expected_size_bytes, OLD.expected_sha256,
      OLD.canonical_bytes, OLD.created_at)
  THEN RAISE EXCEPTION 'joined artifact identity is immutable'; END IF;

  IF OLD.artifact_kind <> 'media' AND
    ((NEW.publication_state='published') IS DISTINCT FROM (NEW.published_at IS NOT NULL)
      OR (NEW.publication_state='published' AND
        (NEW.finalized_token IS NULL OR NEW.etag IS NULL OR NEW.version_id IS NULL))
      OR (NEW.publication_state<>'published' AND
        (NEW.finalized_token IS NOT NULL OR NEW.etag IS NOT NULL OR NEW.version_id IS NOT NULL)))
  THEN RAISE EXCEPTION 'joined publication result differs from state'; END IF;

  IF OLD.artifact_kind = 'media' THEN
    SELECT * INTO STRICT root FROM recording_joined_artifacts
      WHERE hour_record_id = OLD.hour_record_id AND artifact_kind = 'hour_manifest' FOR KEY SHARE;
    IF OLD.published_at IS NOT NULL OR NEW.publication_state IS NOT NULL OR root.publication_state <> 'publishing'
      OR root.publication_token IS DISTINCT FROM NEW.finalized_token OR root.publication_lease_expires_at <= now()
      OR NEW.etag IS NULL OR NEW.version_id IS NULL OR NEW.published_at IS NULL
    THEN RAISE EXCEPTION 'joined media publication is fenced'; END IF;
  ELSIF OLD.publication_state = 'sealed' AND NEW.publication_state = 'publishing' THEN
    IF NEW.publication_attempt_count <> OLD.publication_attempt_count + 1 OR NEW.publication_lease_expires_at <= now()
      OR NEW.etag IS NOT NULL OR NEW.version_id IS NOT NULL OR NEW.published_at IS NOT NULL
      OR NEW.failure_reason_code <> OLD.failure_reason_code
      OR (OLD.artifact_kind = 'batch_index' AND NOT EXISTS(SELECT 1 FROM recording_joined_batches b
        WHERE b.id = OLD.batch_record_id AND b.state = 'index_sealed' AND b.index_artifact_id = OLD.id))
    THEN RAISE EXCEPTION 'invalid joined publication claim'; END IF;
  ELSIF OLD.publication_state = 'publishing' AND NEW.publication_state = 'publishing' THEN
    IF OLD.publication_lease_expires_at <= now() THEN
      IF NEW.publication_token = OLD.publication_token OR NEW.publication_attempt_count <> OLD.publication_attempt_count + 1
        OR NEW.publication_heartbeat_at <= OLD.publication_heartbeat_at OR NEW.publication_lease_expires_at <= now()
        OR NEW.failure_reason_code <> OLD.failure_reason_code
      THEN RAISE EXCEPTION 'invalid joined publication reclaim'; END IF;
    ELSIF NEW.publication_token <> OLD.publication_token OR NEW.publication_claimed_by <> OLD.publication_claimed_by
      OR NEW.publication_attempt_count <> OLD.publication_attempt_count
      OR NEW.publication_heartbeat_at <= OLD.publication_heartbeat_at
      OR NEW.publication_lease_expires_at <= OLD.publication_lease_expires_at
      OR NEW.failure_reason_code <> OLD.failure_reason_code
    THEN RAISE EXCEPTION 'joined publication lease is fenced'; END IF;
  ELSIF OLD.publication_state = 'publishing' AND NEW.publication_state = 'sealed' THEN
    IF OLD.publication_lease_expires_at > now() OR NEW.publication_next_attempt_at <= now()
      OR NEW.publication_attempt_count <> OLD.publication_attempt_count
      OR NEW.failure_reason_code <> OLD.failure_reason_code
    THEN RAISE EXCEPTION 'invalid joined publication release'; END IF;
  ELSIF OLD.publication_state = 'publishing' AND NEW.publication_state = 'published' THEN
    IF OLD.publication_lease_expires_at <= now() OR NEW.publication_attempt_count <> OLD.publication_attempt_count
      OR NEW.finalized_token IS DISTINCT FROM OLD.publication_token OR NEW.etag IS NULL OR NEW.version_id IS NULL
      OR NEW.published_at IS NULL
      OR NEW.failure_reason_code <> OLD.failure_reason_code
      OR (OLD.artifact_kind = 'hour_manifest' AND EXISTS(SELECT 1 FROM recording_joined_artifacts child
        WHERE child.hour_record_id = OLD.hour_record_id AND child.artifact_kind = 'media' AND child.published_at IS NULL))
    THEN RAISE EXCEPTION 'invalid joined publication finalize'; END IF;
  ELSIF OLD.publication_state = 'publishing' AND NEW.publication_state = 'terminal_failed' THEN
    IF OLD.publication_lease_expires_at <= now() OR NEW.failure_reason_code = ''
    THEN RAISE EXCEPTION 'invalid joined terminal publication failure'; END IF;
  ELSE RAISE EXCEPTION 'invalid joined artifact transition'; END IF;
  NEW.updated_at := now(); RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_artifact_update_guard BEFORE UPDATE ON recording_joined_artifacts
FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_artifact_update();

CREATE FUNCTION guard_recording_joined_index_ref_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE idx recording_joined_artifacts%ROWTYPE; ref recording_joined_artifacts%ROWTYPE; batch recording_joined_batches%ROWTYPE;
BEGIN
  SELECT * INTO STRICT idx FROM recording_joined_artifacts WHERE id = NEW.index_artifact_id FOR KEY SHARE;
  SELECT * INTO STRICT ref FROM recording_joined_artifacts WHERE id = NEW.referenced_artifact_id FOR KEY SHARE;
  SELECT * INTO STRICT batch FROM recording_joined_batches WHERE id = NEW.batch_record_id FOR KEY SHARE;
  IF batch.state <> 'frozen' OR idx.artifact_kind <> 'batch_index' OR idx.batch_record_id <> batch.id
    OR ref.batch_record_id <> batch.id OR ref.artifact_kind <> NEW.reference_kind
    OR ref.publication_state <> 'published'
  THEN RAISE EXCEPTION 'joined batch index reference differs'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_index_ref_insert_guard BEFORE INSERT ON recording_joined_batch_index_refs
FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_index_ref_insert();

CREATE FUNCTION guard_recording_joined_batch_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF ROW(NEW.account_id, NEW.connection_id, NEW.batch_id, NEW.generation, NEW.source_endpoint, NEW.policy_version, NEW.eligibility_cutoff,
      NEW.media_tool, NEW.media_tool_sha256, NEW.freeze_request, NEW.freeze_request_sha256,
      NEW.frozen_denominator_sha256, NEW.expected_recordings, NEW.expected_stream_days,
      NEW.expected_scheduled_hours, NEW.expected_source_clips, NEW.expected_source_bytes, NEW.created_at)
    IS DISTINCT FROM ROW(OLD.account_id, OLD.connection_id, OLD.batch_id, OLD.generation, OLD.source_endpoint, OLD.policy_version, OLD.eligibility_cutoff,
      OLD.media_tool, OLD.media_tool_sha256, OLD.freeze_request, OLD.freeze_request_sha256,
      OLD.frozen_denominator_sha256, OLD.expected_recordings, OLD.expected_stream_days,
      OLD.expected_scheduled_hours, OLD.expected_source_clips, OLD.expected_source_bytes, OLD.created_at)
  THEN RAISE EXCEPTION 'joined batch identity is immutable'; END IF;
  IF OLD.state<>'building' AND NEW.freeze_started_at IS DISTINCT FROM OLD.freeze_started_at
  THEN RAISE EXCEPTION 'joined batch freeze evidence is immutable'; END IF;
  IF OLD.state='building' AND NEW.state='building' THEN
    IF current_setting('transaction_isolation') <> 'read committed' OR OLD.freeze_started_at IS NOT NULL
      OR NEW.freeze_started_at IS NULL OR NEW.freeze_started_at < OLD.created_at OR NEW.freeze_started_at > clock_timestamp()
    THEN RAISE EXCEPTION 'invalid joined batch freeze start'; END IF;
  ELSIF OLD.state='building' AND NEW.state='frozen' THEN
    IF current_setting('transaction_isolation') <> 'read committed'
      OR OLD.freeze_started_at IS NULL OR NEW.freeze_started_at IS DISTINCT FROM OLD.freeze_started_at
      OR NEW.frozen_at IS NULL OR NEW.frozen_at < OLD.eligibility_cutoff OR NEW.frozen_at < OLD.freeze_started_at
      OR NEW.frozen_at > clock_timestamp()
      OR (SELECT count(*) FROM recording_joined_batch_recordings br WHERE br.batch_record_id=OLD.id)<>OLD.expected_recordings
      OR (SELECT count(*) FROM recording_joined_stream_days d WHERE d.batch_record_id=OLD.id)<>OLD.expected_stream_days
      OR (SELECT count(*) FROM recording_joined_hours h WHERE h.batch_record_id=OLD.id)<>OLD.expected_scheduled_hours
      OR (SELECT count(*) FROM recording_joined_sources s WHERE s.batch_record_id=OLD.id)<>OLD.expected_source_clips
      OR (SELECT COALESCE(sum(size_bytes),0) FROM recording_joined_sources s WHERE s.batch_record_id=OLD.id)<>OLD.expected_source_bytes
      OR EXISTS(SELECT 1 FROM recording_joined_batch_recordings br
        LEFT JOIN LATERAL (SELECT count(*) AS days FROM recording_joined_stream_days d
          WHERE d.batch_recording_id=br.id) actual ON TRUE
        WHERE br.batch_record_id=OLD.id AND (actual.days<>14
          OR br.priority_ordinal<>(SELECT count(*) FROM recording_joined_batch_recordings earlier
            WHERE earlier.batch_record_id=br.batch_record_id AND earlier.priority_ordinal<=br.priority_ordinal)
          OR (br.qualification->>'recording_id')::BIGINT<>br.recording_id
          OR br.qualification->>'timezone'<>br.timezone
          OR br.qualification->>'evidence_sha256'<>br.qualification_sha256
          OR jsonb_array_length(br.qualification->'days')<>14))
      OR EXISTS(SELECT 1 FROM recording_joined_stream_days d
        JOIN recording_joined_batch_recordings br ON br.id=d.batch_recording_id
        JOIN recording_jobs j ON j.id=d.recording_job_id
        WHERE d.batch_record_id=OLD.id AND (d.local_date<>br.first_local_date+d.date_ordinal-1
          OR d.recording_job_id<>br.authoritative_job_ids[d.date_ordinal]
          OR ROW(j.recording_id,j.fire_at,j.scheduled_for,j.kind,j.window_end_at,j.status)
            IS DISTINCT FROM ROW(d.recording_id,d.scheduled_start_at,d.scheduled_start_at,'continuous_window',d.scheduled_end_at,'done')
          OR j.completed_at IS NULL OR j.completed_at<d.scheduled_end_at OR j.completed_at>NEW.frozen_at
          OR (br.qualification->'days'->(d.date_ordinal-1)->>'local_date')::DATE<>d.local_date
          OR (br.qualification->'days'->(d.date_ordinal-1)->>'job_id')::BIGINT<>d.recording_job_id
          OR (br.qualification->'days'->(d.date_ordinal-1)->>'window_start')::TIMESTAMPTZ<>d.scheduled_start_at
          OR (br.qualification->'days'->(d.date_ordinal-1)->>'window_end')::TIMESTAMPTZ<>d.scheduled_end_at
          OR br.qualification->'days'->(d.date_ordinal-1)->>'quality_tier'<>'good+'))
      OR EXISTS(SELECT 1 FROM recording_joined_artifacts a WHERE a.batch_record_id=OLD.id)
    THEN RAISE EXCEPTION 'joined batch freeze is incomplete'; END IF;
    PERFORM validate_recording_joined_stream_day(d.id) FROM recording_joined_stream_days d WHERE d.batch_record_id=OLD.id;
  ELSIF OLD.state = 'frozen' AND NEW.state = 'index_sealed' THEN
    IF NEW.index_artifact_id IS NULL OR NEW.index_sealed_at IS NULL
      OR (SELECT count(*) FROM recording_joined_batch_index_refs r
        WHERE r.index_artifact_id = NEW.index_artifact_id AND r.reference_kind = 'allocation_ledger') <> OLD.expected_stream_days
      OR (SELECT count(*) FROM recording_joined_batch_index_refs r
        WHERE r.index_artifact_id = NEW.index_artifact_id AND r.reference_kind = 'hour_manifest') <> OLD.expected_scheduled_hours
    THEN RAISE EXCEPTION 'joined batch index reference set is incomplete'; END IF;
  ELSIF OLD.state = 'index_sealed' AND NEW.state = 'published' THEN
    IF NEW.index_artifact_id <> OLD.index_artifact_id OR NEW.index_sealed_at <> OLD.index_sealed_at OR NEW.published_at IS NULL
      OR NOT EXISTS(SELECT 1 FROM recording_joined_artifacts a WHERE a.id = OLD.index_artifact_id AND a.publication_state = 'published')
    THEN RAISE EXCEPTION 'invalid joined batch publication'; END IF;
  ELSIF OLD.state IN ('frozen', 'index_sealed') AND NEW.state = 'terminal_failed' THEN
    IF NEW.failure_reason_code = '' THEN RAISE EXCEPTION 'joined terminal batch failure lacks reason'; END IF;
  ELSE RAISE EXCEPTION 'invalid joined batch transition'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_batch_update_guard BEFORE UPDATE ON recording_joined_batches
FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_batch_update();

CREATE FUNCTION reject_recording_joined_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'joined recording evidence is append-only'; END $$;

CREATE TRIGGER recording_joined_batch_no_delete BEFORE DELETE ON recording_joined_batches
  FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_mutation();
CREATE TRIGGER recording_joined_batch_recording_no_mutation BEFORE UPDATE OR DELETE ON recording_joined_batch_recordings
  FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_mutation();
CREATE TRIGGER recording_joined_exclusion_no_mutation BEFORE UPDATE OR DELETE ON recording_joined_freeze_exclusions
  FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_mutation();
CREATE TRIGGER recording_joined_stream_day_no_mutation BEFORE UPDATE OR DELETE ON recording_joined_stream_days
  FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_mutation();
CREATE TRIGGER recording_joined_boundary_no_mutation BEFORE UPDATE OR DELETE ON recording_joined_day_boundaries
  FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_mutation();
CREATE TRIGGER recording_joined_hour_no_delete BEFORE DELETE ON recording_joined_hours
  FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_mutation();
CREATE TRIGGER recording_joined_source_no_mutation BEFORE UPDATE OR DELETE ON recording_joined_sources
  FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_mutation();
CREATE TRIGGER recording_joined_artifact_no_delete BEFORE DELETE ON recording_joined_artifacts
  FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_mutation();
CREATE TRIGGER recording_joined_media_source_no_mutation BEFORE UPDATE OR DELETE ON recording_joined_media_sources
  FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_mutation();
CREATE TRIGGER recording_joined_disposition_no_mutation BEFORE UPDATE OR DELETE ON recording_joined_hour_dispositions
  FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_mutation();
CREATE TRIGGER recording_joined_index_ref_no_mutation BEFORE UPDATE OR DELETE ON recording_joined_batch_index_refs
  FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_mutation();
CREATE TRIGGER recording_joined_ack_no_mutation BEFORE UPDATE OR DELETE ON recording_joined_artifact_acks
  FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_mutation();

CREATE TRIGGER recording_joined_batch_no_truncate BEFORE TRUNCATE ON recording_joined_batches
  FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_mutation();
CREATE TRIGGER recording_joined_batch_recording_no_truncate BEFORE TRUNCATE ON recording_joined_batch_recordings
  FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_mutation();
CREATE TRIGGER recording_joined_exclusion_no_truncate BEFORE TRUNCATE ON recording_joined_freeze_exclusions
  FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_mutation();
CREATE TRIGGER recording_joined_stream_day_no_truncate BEFORE TRUNCATE ON recording_joined_stream_days
  FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_mutation();
CREATE TRIGGER recording_joined_boundary_no_truncate BEFORE TRUNCATE ON recording_joined_day_boundaries
  FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_mutation();
CREATE TRIGGER recording_joined_hour_no_truncate BEFORE TRUNCATE ON recording_joined_hours
  FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_mutation();
CREATE TRIGGER recording_joined_source_no_truncate BEFORE TRUNCATE ON recording_joined_sources
  FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_mutation();
CREATE TRIGGER recording_joined_artifact_no_truncate BEFORE TRUNCATE ON recording_joined_artifacts
  FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_mutation();
CREATE TRIGGER recording_joined_media_source_no_truncate BEFORE TRUNCATE ON recording_joined_media_sources
  FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_mutation();
CREATE TRIGGER recording_joined_disposition_no_truncate BEFORE TRUNCATE ON recording_joined_hour_dispositions
  FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_mutation();
CREATE TRIGGER recording_joined_index_ref_no_truncate BEFORE TRUNCATE ON recording_joined_batch_index_refs
  FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_mutation();
CREATE TRIGGER recording_joined_ack_no_truncate BEFORE TRUNCATE ON recording_joined_artifact_acks
  FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_mutation();

COMMIT;
