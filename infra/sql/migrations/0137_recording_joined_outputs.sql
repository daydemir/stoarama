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
  qualification_run_id BIGINT NOT NULL,
  qualification_cohort_sha256 TEXT NOT NULL CHECK (qualification_cohort_sha256 ~ '^[0-9a-f]{64}$'),
  qualification_windows_sha256 TEXT NOT NULL CHECK (qualification_windows_sha256 ~ '^[0-9a-f]{64}$'),
  selected_qualification_windows_sha256 TEXT NOT NULL CHECK (selected_qualification_windows_sha256 ~ '^[0-9a-f]{64}$'),
  qualification_jobs_sha256 TEXT NOT NULL CHECK (qualification_jobs_sha256 ~ '^[0-9a-f]{64}$'),
  qualification_frozen_at TIMESTAMPTZ NOT NULL,
  ordered_recording_ids_sha256 TEXT NOT NULL CHECK (ordered_recording_ids_sha256 ~ '^[0-9a-f]{64}$'),
  selection_basis TEXT NOT NULL CHECK (selection_basis = 'operator_approved_ordered_cohort_v1'),
  policy_version TEXT NOT NULL CHECK (policy_version = btrim(policy_version) AND policy_version <> '' AND octet_length(policy_version) <= 128),
  eligibility_cutoff TIMESTAMPTZ NOT NULL,
  media_tool JSONB NOT NULL CHECK (jsonb_typeof(media_tool) = 'object' AND media_tool <> '{}'::jsonb),
  media_tool_sha256 TEXT NOT NULL CHECK (media_tool_sha256 ~ '^[0-9a-f]{64}$'),
  freeze_request_bytes BYTEA NOT NULL CHECK (octet_length(freeze_request_bytes) BETWEEN 2 AND 16777216),
  freeze_request_sha256 TEXT NOT NULL CHECK (freeze_request_sha256 = encode(sha256(freeze_request_bytes),'hex')),
  frozen_denominator_sha256 TEXT NOT NULL CHECK (frozen_denominator_sha256 ~ '^[0-9a-f]{64}$'),
  freeze_exclusions_sha256 TEXT NOT NULL CHECK (freeze_exclusions_sha256 ~ '^[0-9a-f]{64}$'),
  expected_recordings INTEGER NOT NULL CHECK (expected_recordings > 0),
  expected_stream_days INTEGER NOT NULL CHECK (expected_stream_days > 0),
  expected_scheduled_hours INTEGER NOT NULL CHECK (expected_scheduled_hours > 0),
  expected_source_clips BIGINT NOT NULL CHECK (expected_source_clips >= 0),
  expected_source_bytes BIGINT NOT NULL CHECK (expected_source_bytes >= 0),
  expected_freeze_exclusions BIGINT NOT NULL CHECK (expected_freeze_exclusions >= 0),
  state TEXT NOT NULL DEFAULT 'snapshotting' CHECK (state IN ('snapshotting', 'building', 'frozen', 'index_sealed', 'published', 'terminal_failed')),
  index_artifact_id BIGINT,
  freeze_started_at TIMESTAMPTZ,
  frozen_at TIMESTAMPTZ,
  index_sealed_at TIMESTAMPTZ,
  published_at TIMESTAMPTZ,
  failure_reason_code TEXT NOT NULL DEFAULT '' CHECK (failure_reason_code = '' OR failure_reason_code ~ '^[a-z][a-z0-9_]{0,79}$'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (batch_id),
  UNIQUE (id, account_id, connection_id),
  UNIQUE (id, account_id, connection_id, batch_id),
  UNIQUE (id, source_endpoint),
  UNIQUE (id, qualification_run_id),
  FOREIGN KEY (qualification_run_id, account_id)
    REFERENCES recording_qualification_runs(id, account_id) ON DELETE RESTRICT,
  CHECK (expected_stream_days = expected_recordings * 14),
  CHECK (expected_scheduled_hours = expected_stream_days * 12),
  CHECK (expected_recordings = 33 AND expected_stream_days = 462 AND expected_scheduled_hours = 5544),
  CHECK (ordered_recording_ids_sha256 = '6038d4a23be9b0b5c2bb29ea933743a5ceb7f06b8875e417a3f16b44051ebd71'),
  CHECK (eligibility_cutoff = '2026-08-21T06:59:07.534131Z'::timestamptz),
  CHECK ((state IN ('snapshotting','building') AND frozen_at IS NULL
      AND index_artifact_id IS NULL AND index_sealed_at IS NULL AND published_at IS NULL
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
  qualification_run_id BIGINT NOT NULL,
  selection_tier TEXT NOT NULL CHECK (selection_tier = 'good+'),
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
  completed_at TIMESTAMPTZ NOT NULL,
  authoritative_job_ids BIGINT[] NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (batch_record_id, account_id, connection_id, batch_id)
    REFERENCES recording_joined_batches(id, account_id, connection_id, batch_id) ON DELETE RESTRICT,
  FOREIGN KEY (batch_record_id, qualification_run_id)
    REFERENCES recording_joined_batches(id, qualification_run_id) ON DELETE RESTRICT,
  FOREIGN KEY (qualification_run_id, recording_id)
    REFERENCES recording_qualification_members(run_id, recording_id) ON DELETE RESTRICT,
  CHECK (last_local_date = first_local_date + 13),
  CHECK (array_ndims(authoritative_job_ids) = 1 AND array_lower(authoritative_job_ids, 1) = 1
    AND cardinality(authoritative_job_ids) = 14 AND array_position(authoritative_job_ids, NULL) IS NULL),
  UNIQUE (batch_record_id, recording_id),
  UNIQUE (batch_record_id, priority_ordinal),
  UNIQUE (batch_record_id, recording_id, priority_ordinal),
  UNIQUE (batch_record_id, id, recording_id)
);

CREATE FUNCTION guard_recording_joined_batch_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE request JSONB;
BEGIN
  IF current_setting('transaction_isolation') <> 'read committed'
  THEN RAISE EXCEPTION 'joined snapshot apply requires read committed'; END IF;
  -- Freeze and purge share one transaction fence. This keeps the raw rows
  -- individually unlocked while making membership and retention linearizable.
  PERFORM pg_advisory_xact_lock(137, 1);
	request := convert_from(NEW.freeze_request_bytes,'UTF8')::JSONB;
  IF NEW.state<>'snapshotting' OR NEW.freeze_started_at IS NOT NULL OR NEW.frozen_at IS NOT NULL
    OR NOT EXISTS(SELECT 1 FROM connections c
    WHERE c.id=NEW.connection_id AND c.account_id=NEW.account_id AND c.joined_protocol_version=1 FOR UPDATE)
    OR NOT EXISTS(SELECT 1 FROM recording_qualification_runs q
    WHERE q.id=NEW.qualification_run_id AND q.account_id=NEW.account_id AND q.status='active'
      AND q.cohort_sha256=NEW.qualification_cohort_sha256
      AND q.windows_sha256=NEW.qualification_windows_sha256
      AND q.frozen_at=NEW.qualification_frozen_at FOR SHARE)
	OR ARRAY(SELECT key FROM jsonb_object_keys(request) AS object_keys(key) ORDER BY key COLLATE "C") IS DISTINCT FROM ARRAY['account_id','batch_id',
	  'connection_id','expected_scheduled_hours','expected_stream_days','freeze_exclusions_sha256',
	  'frozen_denominator_sha256','generation','media_tool','policy_version','provisional_exclusions',
	  'provisional_source_bytes','provisional_source_clips','qualification_jobs_sha256','recording_ids',
	  'recordings','schema_version','selection_authority','source_endpoint']::TEXT[]
	OR (request->>'schema_version')::INTEGER IS DISTINCT FROM 1
	OR request->>'batch_id' IS DISTINCT FROM NEW.batch_id
	OR (request->>'generation')::INTEGER IS DISTINCT FROM NEW.generation
	OR (request->>'account_id')::BIGINT IS DISTINCT FROM NEW.account_id
	OR (request->>'connection_id')::BIGINT IS DISTINCT FROM NEW.connection_id
	OR request->>'source_endpoint' IS DISTINCT FROM NEW.source_endpoint
	OR request->>'policy_version' IS DISTINCT FROM NEW.policy_version
	OR request->'media_tool' IS DISTINCT FROM NEW.media_tool
	OR request->'media_tool'->>'identity_sha256' IS DISTINCT FROM NEW.media_tool_sha256
	OR request->>'qualification_jobs_sha256' IS DISTINCT FROM NEW.qualification_jobs_sha256
	OR request->>'frozen_denominator_sha256' IS DISTINCT FROM NEW.frozen_denominator_sha256
	OR request->>'freeze_exclusions_sha256' IS DISTINCT FROM NEW.freeze_exclusions_sha256
	OR (request->>'expected_stream_days')::INTEGER IS DISTINCT FROM NEW.expected_stream_days
	OR (request->>'expected_scheduled_hours')::INTEGER IS DISTINCT FROM NEW.expected_scheduled_hours
	OR (request->>'provisional_source_clips')::BIGINT IS DISTINCT FROM NEW.expected_source_clips
	OR (request->>'provisional_source_bytes')::BIGINT IS DISTINCT FROM NEW.expected_source_bytes
	OR (request->>'provisional_exclusions')::BIGINT IS DISTINCT FROM NEW.expected_freeze_exclusions
	OR request->'recording_ids' IS DISTINCT FROM to_jsonb(ARRAY[
	  377,335,337,355,385,350,382,384,348,403,380,379,383,404,401,408,406,
	  409,422,418,419,413,420,428,423,425,416,421,437,440,429,431,439]::BIGINT[])
	OR jsonb_typeof(request->'recordings') IS DISTINCT FROM 'array'
	OR jsonb_array_length(request->'recordings') IS DISTINCT FROM 33
	OR EXISTS(
	  SELECT 1 FROM jsonb_array_elements(request->'recordings') WITH ORDINALITY frozen(item, ordinal)
	  WHERE CASE
	    WHEN jsonb_typeof(frozen.item) IS DISTINCT FROM 'object'
	      OR jsonb_typeof(frozen.item->'frozen_recording') IS DISTINCT FROM 'object'
	      OR jsonb_typeof(frozen.item->'qualification') IS DISTINCT FROM 'object' THEN TRUE
	    ELSE (frozen.item->'frozen_recording'->>'recording_id')::BIGINT IS DISTINCT FROM
	      (ARRAY[377,335,337,355,385,350,382,384,348,403,380,379,383,404,401,408,406,
	        409,422,418,419,413,420,428,423,425,416,421,437,440,429,431,439]::BIGINT[])[frozen.ordinal]
	      OR (frozen.item->'frozen_recording'->>'priority_ordinal')::BIGINT IS DISTINCT FROM frozen.ordinal
	      OR frozen.item->'frozen_recording'->>'selection_tier' IS DISTINCT FROM 'good+'
	  END)
	OR request->'selection_authority'->>'selection_basis' IS DISTINCT FROM NEW.selection_basis
	OR request->'selection_authority'->>'ordered_recording_ids_sha256' IS DISTINCT FROM NEW.ordered_recording_ids_sha256
	OR (request->'selection_authority'->>'cutoff')::TIMESTAMPTZ IS DISTINCT FROM NEW.eligibility_cutoff
	OR (request->'selection_authority'->>'qualification_run_id')::BIGINT IS DISTINCT FROM NEW.qualification_run_id
	OR (request->'selection_authority'->>'qualification_run_frozen_at')::TIMESTAMPTZ IS DISTINCT FROM NEW.qualification_frozen_at
	OR request->'selection_authority'->>'qualification_rule_version' IS DISTINCT FROM
	  (SELECT q.definition_version FROM recording_qualification_runs q WHERE q.id=NEW.qualification_run_id)
	OR request->'selection_authority'->>'qualification_cohort_sha256' IS DISTINCT FROM NEW.qualification_cohort_sha256
	OR request->'selection_authority'->>'qualification_windows_sha256' IS DISTINCT FROM NEW.qualification_windows_sha256
	OR request->'selection_authority'->>'selected_qualification_windows_sha256' IS DISTINCT FROM NEW.selected_qualification_windows_sha256
  THEN RAISE EXCEPTION 'joined batch must enter an owned snapshotting state'; END IF;
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
  completed_at TIMESTAMPTZ NOT NULL,
  source_clip_count INTEGER NOT NULL CHECK (source_clip_count >= 0),
  source_bytes BIGINT NOT NULL CHECK (source_bytes >= 0),
  source_snapshot_sha256 TEXT NOT NULL CHECK (source_snapshot_sha256 ~ '^[0-9a-f]{64}$'),
  state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','sealed')),
  source_manifest_sha256 TEXT CHECK (source_manifest_sha256 ~ '^[0-9a-f]{64}$'),
  head_manifest_sha256 TEXT CHECK (head_manifest_sha256 ~ '^[0-9a-f]{64}$'),
  seal_request_sha256 TEXT CHECK (seal_request_sha256 ~ '^[0-9a-f]{64}$'),
  ledger_sha256 TEXT CHECK (ledger_sha256 ~ '^[0-9a-f]{64}$'),
  ledger_bytes BYTEA CHECK (ledger_bytes IS NULL OR octet_length(ledger_bytes) BETWEEN 2 AND 16777216),
  ledger_artifact_sha256 TEXT CHECK (ledger_artifact_sha256 ~ '^[0-9a-f]{64}$'),
  sealed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (batch_record_id, account_id, connection_id, batch_id)
    REFERENCES recording_joined_batches(id, account_id, connection_id, batch_id) ON DELETE RESTRICT,
  FOREIGN KEY (batch_record_id, batch_recording_id, recording_id)
    REFERENCES recording_joined_batch_recordings(batch_record_id, id, recording_id) ON DELETE RESTRICT,
  CHECK (scheduled_end_at > scheduled_start_at AND completed_at >= scheduled_end_at),
  CHECK ((state='pending' AND source_manifest_sha256 IS NULL AND head_manifest_sha256 IS NULL
      AND seal_request_sha256 IS NULL AND ledger_sha256 IS NULL AND ledger_bytes IS NULL
      AND ledger_artifact_sha256 IS NULL AND sealed_at IS NULL)
    OR (state='sealed' AND source_manifest_sha256 IS NOT NULL AND head_manifest_sha256 IS NOT NULL
      AND seal_request_sha256 IS NOT NULL AND ledger_sha256 IS NOT NULL AND ledger_bytes IS NOT NULL
      AND ledger_artifact_sha256 = encode(sha256(ledger_bytes),'hex') AND sealed_at IS NOT NULL)),
  CHECK ((source_clip_count = 0 AND source_bytes = 0) OR (source_clip_count > 0 AND source_bytes > 0)),
  UNIQUE (batch_record_id, recording_id, local_date),
  UNIQUE (batch_recording_id, date_ordinal),
  UNIQUE (batch_record_id, id, recording_id),
  UNIQUE (batch_record_id, id, recording_id, recording_job_id)
);

-- Apply freezes the complete source-only denominator before any storage HEAD.
-- These rows retain exact raw/database evidence; observed object generations
-- live separately in recording_joined_sources after the day seals.
CREATE TABLE recording_joined_source_snapshots (
  id BIGSERIAL PRIMARY KEY,
  batch_record_id BIGINT NOT NULL REFERENCES recording_joined_batches(id) ON DELETE RESTRICT,
  stream_day_id BIGINT NOT NULL REFERENCES recording_joined_stream_days(id) ON DELETE RESTRICT,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  connection_id BIGINT NOT NULL REFERENCES connections(id) ON DELETE RESTRICT,
  recording_id BIGINT NOT NULL CHECK (recording_id > 0),
  recording_job_id BIGINT NOT NULL REFERENCES recording_jobs(id) ON DELETE RESTRICT,
  clip_id BIGINT NOT NULL CHECK (clip_id > 0),
  storage_destination_id BIGINT NOT NULL REFERENCES storage_destinations(id) ON DELETE RESTRICT,
  day_ordinal INTEGER NOT NULL CHECK (day_ordinal > 0),
  provider TEXT NOT NULL CHECK (provider = btrim(provider) AND provider <> ''),
  endpoint TEXT NOT NULL CHECK (endpoint ~ '^https://[0-9a-f]{32}\.r2\.cloudflarestorage\.com$'),
  region TEXT NOT NULL CHECK (region = btrim(region) AND octet_length(region) <= 128),
  bucket TEXT NOT NULL CHECK (bucket = btrim(bucket) AND bucket <> '' AND octet_length(bucket) <= 255),
  object_key TEXT NOT NULL CHECK (object_key = btrim(object_key) AND object_key <> '' AND octet_length(object_key) <= 2048),
  ingest_etag TEXT NOT NULL CHECK (ingest_etag = btrim(ingest_etag) AND octet_length(ingest_etag) BETWEEN 1 AND 256),
  size_bytes BIGINT NOT NULL CHECK (size_bytes > 0),
  sha256 TEXT NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
  start_at TIMESTAMPTZ NOT NULL,
  end_at TIMESTAMPTZ NOT NULL,
  clip_created_at TIMESTAMPTZ NOT NULL,
  released_at TIMESTAMPTZ,
  capture_lease_token UUID,
  capture_sequence BIGINT CHECK (capture_sequence IS NULL OR capture_sequence > 0),
  capture_attempt_id UUID,
  timestamp_contract_version TEXT,
  timestamp_contract JSONB,
  timestamp_contract_status TEXT,
  timestamp_contract_reason TEXT,
  db_retention_fenced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (batch_record_id, account_id, connection_id)
    REFERENCES recording_joined_batches(id, account_id, connection_id) ON DELETE RESTRICT,
  FOREIGN KEY (batch_record_id, endpoint)
    REFERENCES recording_joined_batches(id, source_endpoint) ON DELETE RESTRICT,
  FOREIGN KEY (batch_record_id, stream_day_id, recording_id)
    REFERENCES recording_joined_stream_days(batch_record_id, id, recording_id) ON DELETE RESTRICT,
  FOREIGN KEY (batch_record_id, stream_day_id, recording_id, recording_job_id)
    REFERENCES recording_joined_stream_days(batch_record_id, id, recording_id, recording_job_id) ON DELETE RESTRICT,
  CHECK (end_at > start_at AND end_at - start_at <= interval '15 minutes'),
  CHECK ((capture_lease_token IS NULL) = (capture_sequence IS NULL)),
  CHECK ((timestamp_contract IS NULL AND timestamp_contract_version IS NULL AND timestamp_contract_status IS NULL
      AND timestamp_contract_reason IS NULL)
    OR (jsonb_typeof(timestamp_contract)='object' AND timestamp_contract_version IS NOT NULL
      AND timestamp_contract_status IS NOT NULL AND timestamp_contract_reason IS NOT NULL)),
  UNIQUE (batch_record_id, clip_id),
  UNIQUE (stream_day_id, day_ordinal),
  UNIQUE (batch_record_id, id, recording_id)
);

CREATE UNIQUE INDEX recording_joined_source_snapshots_storage_identity_uq ON recording_joined_source_snapshots(
  connection_id,batch_record_id,storage_destination_id,provider,endpoint,region,bucket,object_key,ingest_etag);
CREATE INDEX recording_joined_source_snapshots_clip_idx ON recording_joined_source_snapshots(clip_id);
CREATE INDEX recording_joined_source_snapshots_destination_idx ON recording_joined_source_snapshots(storage_destination_id);

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
  source_snapshot_id BIGINT NOT NULL REFERENCES recording_joined_source_snapshots(id) ON DELETE RESTRICT,
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
  observed_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (end_at > start_at AND end_at - start_at <= interval '15 minutes'),
  CHECK (audio_contract IS NULL OR jsonb_typeof(audio_contract) = 'object'),
  FOREIGN KEY (batch_record_id, endpoint)
    REFERENCES recording_joined_batches(id, source_endpoint) ON DELETE RESTRICT,
  FOREIGN KEY (batch_record_id, source_snapshot_id, recording_id)
    REFERENCES recording_joined_source_snapshots(batch_record_id, id, recording_id) ON DELETE RESTRICT,
  UNIQUE (source_snapshot_id),
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

CREATE FUNCTION validate_recording_joined_batch_snapshot() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE batch recording_joined_batches%ROWTYPE; request JSONB;
BEGIN
  SELECT * INTO STRICT batch FROM recording_joined_batches WHERE id=NEW.id FOR UPDATE;
	request := convert_from(batch.freeze_request_bytes,'UTF8')::JSONB;
  IF batch.state<>'building' OR batch.freeze_started_at IS NOT NULL
    OR NOT EXISTS(SELECT 1 FROM connections c WHERE c.id=batch.connection_id AND c.account_id=batch.account_id
      AND c.joined_protocol_version=1 FOR UPDATE)
    OR NOT EXISTS(SELECT 1 FROM recording_qualification_runs q
      WHERE q.id=batch.qualification_run_id AND q.account_id=batch.account_id AND q.status='active'
        AND q.cohort_sha256=batch.qualification_cohort_sha256
        AND q.windows_sha256=batch.qualification_windows_sha256
        AND q.frozen_at=batch.qualification_frozen_at FOR SHARE)
    OR (SELECT count(*) FROM recording_joined_batch_recordings br WHERE br.batch_record_id=batch.id)<>batch.expected_recordings
    OR ARRAY(SELECT br.recording_id FROM recording_joined_batch_recordings br
      WHERE br.batch_record_id=batch.id ORDER BY br.priority_ordinal) IS DISTINCT FROM ARRAY[
        377,335,337,355,385,350,382,384,348,403,380,379,383,404,401,408,406,
        409,422,418,419,413,420,428,423,425,416,421,437,440,429,431,439]::BIGINT[]
    OR (SELECT count(*) FROM recording_joined_stream_days d WHERE d.batch_record_id=batch.id)<>batch.expected_stream_days
    OR (SELECT count(*) FROM recording_joined_stream_days d
      WHERE d.batch_record_id=batch.id AND d.state='pending')<>batch.expected_stream_days
    OR (SELECT count(*) FROM recording_joined_source_snapshots s WHERE s.batch_record_id=batch.id)<>batch.expected_source_clips
    OR (SELECT COALESCE(sum(size_bytes),0) FROM recording_joined_source_snapshots s
      WHERE s.batch_record_id=batch.id)<>batch.expected_source_bytes
    OR (SELECT count(*) FROM recording_joined_freeze_exclusions e
      WHERE e.batch_record_id=batch.id)<>batch.expected_freeze_exclusions
    OR (SELECT encode(sha256(convert_to(COALESCE(string_agg(format('%s\n%s\n%s\n%s\n',e.recording_id,
        COALESCE(e.clip_id::TEXT,''),e.reason_code,e.evidence_sha256),'' ORDER BY e.recording_id,e.clip_id,
        e.reason_code,e.evidence_sha256),''),'UTF8')),'hex') FROM recording_joined_freeze_exclusions e
      WHERE e.batch_record_id=batch.id)<>batch.freeze_exclusions_sha256
    OR EXISTS(SELECT 1 FROM recording_joined_batch_recordings br
      JOIN recording_qualification_runs q ON q.id=br.qualification_run_id AND q.account_id=batch.account_id
      LEFT JOIN LATERAL (SELECT count(*) AS days FROM recording_joined_stream_days d
        WHERE d.batch_recording_id=br.id) actual ON TRUE
		WHERE br.batch_record_id=batch.id AND (br.qualification_run_id IS DISTINCT FROM batch.qualification_run_id
		  OR br.selection_tier IS DISTINCT FROM 'good+' OR br.qualification_policy_version IS DISTINCT FROM q.definition_version
		  OR br.priority_ordinal IS DISTINCT FROM (SELECT count(*) FROM recording_joined_batch_recordings earlier
          WHERE earlier.batch_record_id=br.batch_record_id AND earlier.priority_ordinal<=br.priority_ordinal)
        OR br.first_local_date IS DISTINCT FROM (SELECT w.local_open_at::DATE
          FROM recording_qualification_windows w
          WHERE w.run_id=br.qualification_run_id AND w.recording_id=br.recording_id AND w.ordinal=1)
        OR br.last_local_date IS DISTINCT FROM (SELECT w.local_open_at::DATE
          FROM recording_qualification_windows w
          WHERE w.run_id=br.qualification_run_id AND w.recording_id=br.recording_id AND w.ordinal=14)
        OR br.completed_at>batch.eligibility_cutoff
        OR br.completed_at IS DISTINCT FROM (SELECT max(d.completed_at)
          FROM recording_joined_stream_days d WHERE d.batch_recording_id=br.id)
		OR actual.days IS DISTINCT FROM 14
        OR br.authoritative_job_ids IS DISTINCT FROM ARRAY(SELECT d.recording_job_id
          FROM recording_joined_stream_days d WHERE d.batch_recording_id=br.id ORDER BY d.date_ordinal)))
	OR jsonb_typeof(request->'recordings') IS DISTINCT FROM 'array'
	OR jsonb_array_length(request->'recordings') IS DISTINCT FROM batch.expected_recordings
	OR EXISTS(SELECT 1 FROM recording_joined_batch_recordings br
	  CROSS JOIN LATERAL (SELECT request->'recordings'->(br.priority_ordinal-1) item) frozen
	  WHERE br.batch_record_id=batch.id AND (
	    ARRAY(SELECT key FROM jsonb_object_keys(frozen.item) AS object_keys(key) ORDER BY key COLLATE "C") IS DISTINCT FROM ARRAY['frozen_recording','qualification']::TEXT[]
	    OR ARRAY(SELECT key FROM jsonb_object_keys(frozen.item->'frozen_recording') AS object_keys(key) ORDER BY key COLLATE "C") IS DISTINCT FROM
	      ARRAY['completed_at','folder_name','naming_metadata','priority_ordinal','qualification_sha256',
	        'recording_id','selection_tier','timezone']::TEXT[]
	    OR (frozen.item->'frozen_recording'->>'recording_id')::BIGINT IS DISTINCT FROM br.recording_id
	    OR (frozen.item->'frozen_recording'->>'priority_ordinal')::INTEGER IS DISTINCT FROM br.priority_ordinal
	    OR frozen.item->'frozen_recording'->>'selection_tier' IS DISTINCT FROM br.selection_tier
	    OR frozen.item->'frozen_recording'->>'qualification_sha256' IS DISTINCT FROM br.qualification_sha256
	    OR (frozen.item->'frozen_recording'->>'completed_at')::TIMESTAMPTZ IS DISTINCT FROM br.completed_at
	    OR frozen.item->'frozen_recording'->>'timezone' IS DISTINCT FROM br.timezone
	    OR frozen.item->'frozen_recording'->>'folder_name' IS DISTINCT FROM br.folder_name
	    OR frozen.item->'frozen_recording'->'naming_metadata' IS DISTINCT FROM br.naming_metadata
	    OR frozen.item->'qualification' IS DISTINCT FROM br.qualification))
    OR EXISTS(SELECT 1 FROM recording_joined_stream_days d
      JOIN recording_joined_batch_recordings br ON br.id=d.batch_recording_id
      JOIN recording_qualification_windows w ON w.run_id=br.qualification_run_id
        AND w.recording_id=br.recording_id AND w.ordinal=d.date_ordinal
      LEFT JOIN LATERAL (SELECT count(*) AS clips,COALESCE(sum(size_bytes),0) AS bytes
        FROM recording_joined_source_snapshots s WHERE s.stream_day_id=d.id) actual ON TRUE
      WHERE d.batch_record_id=batch.id AND (actual.clips<>d.source_clip_count OR actual.bytes<>d.source_bytes
        OR d.local_date<>br.first_local_date+d.date_ordinal-1
        OR ROW(d.scheduled_start_at,d.scheduled_end_at) IS DISTINCT FROM ROW(w.window_start_at,w.window_end_at)
        OR d.completed_at<d.scheduled_end_at OR d.completed_at>batch.eligibility_cutoff
        OR batch.qualification_frozen_at>d.scheduled_start_at
        OR EXISTS(SELECT 1 FROM (
          SELECT s.day_ordinal,row_number() OVER (ORDER BY s.start_at,s.clip_id) AS expected_ordinal
          FROM recording_joined_source_snapshots s WHERE s.stream_day_id=d.id
        ) ordered WHERE ordered.day_ordinal<>ordered.expected_ordinal)))
    OR EXISTS(SELECT 1 FROM recording_joined_hours h WHERE h.batch_record_id=batch.id)
    OR EXISTS(SELECT 1 FROM recording_joined_sources s WHERE s.batch_record_id=batch.id)
  THEN RAISE EXCEPTION 'joined building batch snapshot is incomplete'; END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER recording_joined_batch_snapshot_complete
AFTER INSERT ON recording_joined_batches DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_recording_joined_batch_snapshot();

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
	CHECK (canonical_bytes IS NULL OR expected_sha256 = encode(sha256(canonical_bytes),'hex')),
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
DECLARE h recording_joined_hours%ROWTYPE; d recording_joined_stream_days%ROWTYPE;
  batch recording_joined_batches%ROWTYPE;
  snapshot recording_joined_source_snapshots%ROWTYPE;
BEGIN
  SELECT * INTO STRICT snapshot FROM recording_joined_source_snapshots WHERE id=NEW.source_snapshot_id FOR KEY SHARE;
  SELECT * INTO STRICT h FROM recording_joined_hours WHERE id = NEW.hour_record_id FOR KEY SHARE;
  SELECT * INTO STRICT d FROM recording_joined_stream_days WHERE id = NEW.stream_day_id FOR KEY SHARE;
  SELECT * INTO STRICT batch FROM recording_joined_batches WHERE id = NEW.batch_record_id FOR KEY SHARE;
  IF d.state<>'pending'
    OR ROW(snapshot.batch_record_id,snapshot.stream_day_id,snapshot.account_id,snapshot.connection_id,snapshot.recording_id,
      snapshot.recording_job_id,snapshot.clip_id,snapshot.storage_destination_id,snapshot.day_ordinal,snapshot.provider,
      snapshot.endpoint,snapshot.region,snapshot.bucket,snapshot.object_key,snapshot.ingest_etag,snapshot.size_bytes,
      snapshot.sha256,snapshot.start_at,snapshot.end_at,snapshot.clip_created_at,snapshot.released_at)
      IS DISTINCT FROM ROW(NEW.batch_record_id,NEW.stream_day_id,NEW.account_id,NEW.connection_id,NEW.recording_id,
        NEW.recording_job_id,NEW.clip_id,NEW.storage_destination_id,NEW.day_ordinal,NEW.provider,NEW.endpoint,NEW.region,
        NEW.bucket,NEW.object_key,NEW.etag,NEW.size_bytes,NEW.sha256,NEW.start_at,NEW.end_at,NEW.clip_created_at,NEW.released_at)
    OR ROW(h.batch_record_id,h.stream_day_id,h.account_id,h.connection_id,h.recording_id)
      IS DISTINCT FROM ROW(NEW.batch_record_id,NEW.stream_day_id,NEW.account_id,NEW.connection_id,NEW.recording_id)
    OR NEW.endpoint IS DISTINCT FROM batch.source_endpoint OR NEW.observed_at<snapshot.created_at
    OR NEW.observed_at>clock_timestamp()+interval '5 minutes'
  THEN RAISE EXCEPTION 'joined source differs from frozen snapshot or HEAD'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_source_insert_guard BEFORE INSERT ON recording_joined_sources
FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_source_insert();

CREATE FUNCTION guard_recording_joined_clip_retention() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF current_setting('transaction_isolation') <> 'read committed'
  THEN RAISE EXCEPTION 'joined source purge requires read committed'; END IF;
  PERFORM pg_advisory_xact_lock_shared(137, 1);
  IF TG_OP='DELETE' THEN
    IF EXISTS(SELECT 1 FROM recording_joined_source_snapshots s WHERE s.clip_id=OLD.id)
    THEN RAISE EXCEPTION 'joined frozen source is retention protected'; END IF;
    RETURN OLD;
  END IF;
  IF NEW.id IS DISTINCT FROM OLD.id
    AND EXISTS(SELECT 1 FROM recording_joined_source_snapshots s WHERE s.clip_id=OLD.id)
  THEN RAISE EXCEPTION 'joined frozen source identity is immutable'; END IF;
  IF OLD.purged_at IS NULL AND NEW.purged_at IS NOT NULL
    AND EXISTS(SELECT 1 FROM recording_joined_source_snapshots s WHERE s.clip_id=OLD.id)
  THEN RAISE EXCEPTION 'joined frozen source is retention protected'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_clip_retention_update BEFORE UPDATE OF purged_at ON recording_clips
FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_clip_retention();
CREATE TRIGGER recording_joined_clip_identity_update BEFORE UPDATE OF id ON recording_clips
FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_clip_retention();
CREATE TRIGGER recording_joined_clip_retention_delete BEFORE DELETE ON recording_clips
FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_clip_retention();

CREATE FUNCTION guard_recording_joined_freeze_child_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE batch_key BIGINT; batch_state TEXT; batch_freeze_started_at TIMESTAMPTZ;
BEGIN
  IF TG_TABLE_NAME='recording_joined_day_boundaries' THEN
    SELECT batch_record_id INTO STRICT batch_key FROM recording_joined_stream_days
      WHERE id=(to_jsonb(NEW)->>'stream_day_id')::BIGINT;
  ELSE
    batch_key := (to_jsonb(NEW)->>'batch_record_id')::BIGINT;
  END IF;
  SELECT state,freeze_started_at INTO STRICT batch_state,batch_freeze_started_at
    FROM recording_joined_batches WHERE id=batch_key FOR SHARE;
  IF (TG_TABLE_NAME IN ('recording_joined_batch_recordings','recording_joined_freeze_exclusions',
      'recording_joined_stream_days','recording_joined_source_snapshots') AND batch_state<>'snapshotting')
    OR (TG_TABLE_NAME IN ('recording_joined_day_boundaries','recording_joined_hours','recording_joined_sources')
      AND (batch_state<>'building' OR batch_freeze_started_at IS NOT NULL))
  THEN RAISE EXCEPTION 'joined frozen source scope is immutable'; END IF;
  IF TG_TABLE_NAME='recording_joined_batch_recordings' AND NOT EXISTS(SELECT 1 FROM recordings r
    WHERE r.id=(to_jsonb(NEW)->>'recording_id')::BIGINT AND r.account_id=(to_jsonb(NEW)->>'account_id')::BIGINT
      AND r.cron_timezone=to_jsonb(NEW)->>'timezone' AND r.mode='continuous' AND r.delivery='nas_pull'
      AND r.daily_window_start='08:00'::TIME AND r.daily_window_end='20:00'::TIME FOR KEY SHARE)
  THEN RAISE EXCEPTION 'joined recording scope differs'; END IF;
  IF TG_TABLE_NAME='recording_joined_batch_recordings' AND NOT EXISTS(
    SELECT 1 FROM recording_joined_batches b
    JOIN recording_qualification_runs q ON q.id=b.qualification_run_id AND q.account_id=b.account_id
    JOIN recording_qualification_members m ON m.run_id=b.qualification_run_id
      AND m.recording_id=(to_jsonb(NEW)->>'recording_id')::BIGINT
      AND m.account_id=(to_jsonb(NEW)->>'account_id')::BIGINT
    WHERE b.id=batch_key AND b.qualification_run_id=(to_jsonb(NEW)->>'qualification_run_id')::BIGINT
      AND q.status='active' AND q.definition_version=to_jsonb(NEW)->>'qualification_policy_version'
      AND m.cron_timezone=to_jsonb(NEW)->>'timezone' FOR KEY SHARE OF b,q,m)
  THEN RAISE EXCEPTION 'joined qualification member differs'; END IF;
  IF TG_TABLE_NAME='recording_joined_stream_days' AND NOT EXISTS(
    SELECT 1 FROM recording_joined_batch_recordings br
    JOIN recording_joined_batches b ON b.id=br.batch_record_id
    JOIN recording_qualification_windows w ON w.run_id=br.qualification_run_id
      AND w.recording_id=br.recording_id
      AND w.ordinal=(to_jsonb(NEW)->>'date_ordinal')::INTEGER
    JOIN recording_jobs j ON j.id=(to_jsonb(NEW)->>'recording_job_id')::BIGINT
    WHERE br.id=(to_jsonb(NEW)->>'batch_recording_id')::BIGINT
      AND br.batch_record_id=batch_key
      AND w.local_open_at::DATE=(to_jsonb(NEW)->>'local_date')::DATE
      AND ROW(w.window_start_at,w.window_end_at)
        IS NOT DISTINCT FROM ROW((to_jsonb(NEW)->>'scheduled_start_at')::TIMESTAMPTZ,
          (to_jsonb(NEW)->>'scheduled_end_at')::TIMESTAMPTZ)
      AND ROW(j.recording_id,j.fire_at,j.scheduled_for,j.kind,j.window_end_at,j.status,j.completed_at)
        IS NOT DISTINCT FROM ROW(br.recording_id,w.window_start_at,w.window_start_at,'continuous_window',
          w.window_end_at,'done',(to_jsonb(NEW)->>'completed_at')::TIMESTAMPTZ)
      AND b.qualification_frozen_at<=w.window_start_at
      AND j.completed_at>=j.window_end_at AND j.completed_at<=b.eligibility_cutoff
    FOR KEY SHARE OF br,b,w FOR SHARE OF j)
  THEN RAISE EXCEPTION 'joined qualification window differs'; END IF;
  IF TG_TABLE_NAME='recording_joined_hours' AND
    ((to_jsonb(NEW)->>'state')<>'pending' OR (to_jsonb(NEW)->>'attempt_count')::INTEGER<>0)
  THEN RAISE EXCEPTION 'joined hour must enter pending'; END IF;
  IF TG_TABLE_NAME='recording_joined_stream_days' AND (to_jsonb(NEW)->>'state')<>'pending'
  THEN RAISE EXCEPTION 'joined stream day must enter pending'; END IF;
  IF TG_TABLE_NAME IN ('recording_joined_day_boundaries','recording_joined_hours','recording_joined_sources')
    AND NOT EXISTS(SELECT 1 FROM recording_joined_stream_days d
      WHERE d.id=(to_jsonb(NEW)->>'stream_day_id')::BIGINT AND d.state='pending' FOR SHARE)
  THEN RAISE EXCEPTION 'joined sealed stream day is immutable'; END IF;
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

CREATE FUNCTION guard_recording_joined_source_snapshot_statement() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF EXISTS(SELECT 1 FROM inserted source
    JOIN recording_joined_batches batch ON batch.id=source.batch_record_id
    JOIN recording_joined_stream_days day ON day.id=source.stream_day_id
    LEFT JOIN recording_clips clip ON clip.id=source.clip_id
    LEFT JOIN storage_destinations destination ON destination.id=source.storage_destination_id
    WHERE batch.state<>'snapshotting' OR batch.freeze_started_at IS NOT NULL OR day.state<>'pending'
      OR ROW(day.batch_record_id,day.account_id,day.connection_id,day.recording_id,day.recording_job_id)
        IS DISTINCT FROM ROW(source.batch_record_id,source.account_id,source.connection_id,source.recording_id,
          source.recording_job_id)
      OR source.endpoint IS DISTINCT FROM batch.source_endpoint
      OR source.end_at<=day.scheduled_start_at OR source.start_at>=day.scheduled_end_at
      OR source.clip_created_at>batch.eligibility_cutoff OR clip.id IS NULL OR destination.id IS NULL
      OR clip.purged_at IS NOT NULL OR destination.account_id IS DISTINCT FROM source.account_id
      OR ROW(clip.recording_id,clip.recording_job_id,clip.storage_destination_id,clip.endpoint,clip.bucket,
          clip.object_key,clip.etag,clip.size_bytes,clip.sha256,clip.clip_start_at,clip.clip_end_at,clip.created_at,
          clip.capture_lease_token,clip.capture_sequence,clip.capture_attempt_id,clip.timestamp_contract_version,
          clip.timestamp_contract,clip.timestamp_contract_status,clip.timestamp_contract_reason)
        IS DISTINCT FROM ROW(source.recording_id,source.recording_job_id,source.storage_destination_id,source.endpoint,
          source.bucket,source.object_key,source.ingest_etag,source.size_bytes,source.sha256,source.start_at,source.end_at,
          source.clip_created_at,source.capture_lease_token,source.capture_sequence,source.capture_attempt_id,
          source.timestamp_contract_version,source.timestamp_contract,source.timestamp_contract_status,
          source.timestamp_contract_reason)
      OR ROW(destination.provider,destination.endpoint,destination.region,destination.bucket)
        IS DISTINCT FROM ROW(source.provider,source.endpoint,source.region,source.bucket))
  THEN RAISE EXCEPTION 'joined source snapshot scope differs'; END IF;
  RETURN NULL;
END $$;
CREATE TRIGGER recording_joined_source_snapshot_freeze_guard AFTER INSERT ON recording_joined_source_snapshots
  REFERENCING NEW TABLE AS inserted FOR EACH STATEMENT EXECUTE FUNCTION guard_recording_joined_source_snapshot_statement();

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
  IF d.state<>'sealed'
    OR ARRAY(SELECT key FROM jsonb_object_keys(ledger) AS object_keys(key) ORDER BY key COLLATE "C") IS DISTINCT FROM ARRAY['batch_id','consecutive_pairs',
      'cross_day_boundaries','cross_hour_boundaries','first_clip_id','frozen_source_sha256','generation','hour_source_claim_sha256','hours',
      'last_clip_id','ledger_sha256','local_date','qualification_day','qualification_sha256','recording_id',
      'schema_version','source_bytes','source_claim_sha256','source_clip_count','sources','timezone']::TEXT[]
    OR ARRAY(SELECT key FROM jsonb_object_keys(ledger->'qualification_day') AS object_keys(key) ORDER BY key COLLATE "C") IS DISTINCT FROM
      ARRAY['completed_at','job_id','local_date','qualification_window_ordinal','window_end','window_start']::TEXT[]
    OR EXISTS(SELECT 1 FROM jsonb_array_elements(ledger->'sources') source WHERE
      ARRAY(SELECT key FROM jsonb_object_keys(source) AS object_keys(key) ORDER BY key COLLATE "C") IS DISTINCT FROM ARRAY['bucket','clip_id','end_utc','endpoint',
        'object','provider','recording_id','recording_job_id','region','released_at','seam_to_previous','start_utc',
        'storage_destination_id']::TEXT[]
      OR ARRAY(SELECT key FROM jsonb_object_keys(source->'object') AS object_keys(key) ORDER BY key COLLATE "C") NOT IN
        (ARRAY['etag','key','sha256','size_bytes']::TEXT[],ARRAY['etag','key','sha256','size_bytes','version_id']::TEXT[])
      OR ARRAY(SELECT key FROM jsonb_object_keys(source->'seam_to_previous') AS object_keys(key) ORDER BY key COLLATE "C") IS DISTINCT FROM
        ARRAY['reason','signed_gap_nanoseconds','verdict']::TEXT[])
    OR EXISTS(SELECT 1 FROM jsonb_array_elements(ledger->'hours') hour WHERE
      ARRAY(SELECT key FROM jsonb_object_keys(hour) AS object_keys(key) ORDER BY key COLLATE "C") IS DISTINCT FROM
        ARRAY['clock_hour','delivery_hour','source_clip_ids']::TEXT[])
    OR EXISTS(SELECT 1 FROM jsonb_array_elements(ledger->'consecutive_pairs') pair WHERE
      ARRAY(SELECT key FROM jsonb_object_keys(pair) AS object_keys(key) ORDER BY key COLLATE "C") IS DISTINCT FROM ARRAY['next_clip_id',
        'next_presentation_start_utc','previous_clip_id','previous_presentation_end_utc','signed_gap_nanoseconds']::TEXT[])
    OR EXISTS(SELECT 1 FROM jsonb_array_elements(ledger->'cross_hour_boundaries') boundary WHERE
      ARRAY(SELECT key FROM jsonb_object_keys(boundary) AS object_keys(key) ORDER BY key COLLATE "C") IS DISTINCT FROM ARRAY['actual_seam_utc',
        'allocation_decision','boundary_skew_nanoseconds','next_clip_id','next_delivery_hour','next_presentation_start_utc',
        'previous_clip_id','previous_delivery_hour','previous_presentation_end_utc','reason','scheduled_utc',
        'signed_gap_nanoseconds','verdict']::TEXT[])
    OR EXISTS(SELECT 1 FROM jsonb_array_elements(ledger->'cross_day_boundaries') boundary WHERE
      ARRAY(SELECT key FROM jsonb_object_keys(boundary) AS object_keys(key) ORDER BY key COLLATE "C") IS DISTINCT FROM ARRAY['allocation_decision',
        'boundary_skew_nanoseconds','next_clip_id','next_presentation_start_utc','previous_clip_id',
        'previous_presentation_end_utc','reason','scheduled_next_start_utc','scheduled_previous_end_utc',
        'signed_gap_nanoseconds','verdict']::TEXT[])
    OR (ledger->>'schema_version')::INTEGER IS DISTINCT FROM 1 OR ledger->>'batch_id' IS DISTINCT FROM d.batch_id
    OR (ledger->>'generation')::INTEGER IS DISTINCT FROM batch.generation
    OR (ledger->>'recording_id')::BIGINT IS DISTINCT FROM d.recording_id
    OR ledger->>'timezone' IS DISTINCT FROM recording_timezone OR (ledger->>'local_date')::DATE IS DISTINCT FROM d.local_date
    OR ledger->'qualification_day' IS DISTINCT FROM batch_recording.qualification->'days'->(d.date_ordinal-1)
    OR ledger->>'qualification_sha256' IS DISTINCT FROM batch_recording.qualification_sha256
    OR ledger->>'frozen_source_sha256' IS DISTINCT FROM d.source_snapshot_sha256
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
    OR (SELECT count(*) FROM recording_joined_source_snapshots s WHERE s.stream_day_id=d.id)<>d.source_clip_count
    OR (SELECT COALESCE(sum(size_bytes),0) FROM recording_joined_source_snapshots s
      WHERE s.stream_day_id=d.id)<>d.source_bytes
    OR EXISTS(SELECT 1 FROM recording_joined_source_snapshots snapshot
      LEFT JOIN recording_joined_sources observed ON observed.source_snapshot_id=snapshot.id
      WHERE snapshot.stream_day_id=d.id AND (observed.id IS NULL OR observed.stream_day_id<>d.id
        OR ROW(observed.clip_id,observed.recording_id,observed.recording_job_id,observed.storage_destination_id,
          observed.provider,observed.endpoint,observed.region,observed.bucket,observed.object_key,observed.etag,
          observed.size_bytes,observed.sha256,observed.start_at,observed.end_at,observed.clip_created_at,observed.released_at)
          IS DISTINCT FROM ROW(snapshot.clip_id,snapshot.recording_id,snapshot.recording_job_id,
          snapshot.storage_destination_id,snapshot.provider,snapshot.endpoint,snapshot.region,snapshot.bucket,
          snapshot.object_key,snapshot.ingest_etag,snapshot.size_bytes,snapshot.sha256,snapshot.start_at,
          snapshot.end_at,snapshot.clip_created_at,snapshot.released_at)))
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
      OR (js.source->>'storage_destination_id')::BIGINT IS DISTINCT FROM s.storage_destination_id
      OR js.source->>'provider' IS DISTINCT FROM s.provider OR js.source->>'endpoint' IS DISTINCT FROM s.endpoint
      OR js.source->>'region' IS DISTINCT FROM s.region OR js.source->>'bucket' IS DISTINCT FROM s.bucket
      OR (js.source->>'start_utc')::TIMESTAMPTZ IS DISTINCT FROM s.start_at
      OR (js.source->>'end_utc')::TIMESTAMPTZ IS DISTINCT FROM s.end_at
      OR (js.source->>'released_at')::TIMESTAMPTZ IS DISTINCT FROM s.released_at
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
  IF NEW.canonical_bytes IS NOT NULL AND NEW.expected_sha256 IS DISTINCT FROM
      encode(sha256(NEW.canonical_bytes),'hex')
  THEN RAISE EXCEPTION 'joined canonical artifact SHA differs'; END IF;
  IF NOT EXISTS(SELECT 1 FROM recording_joined_batches b WHERE b.id=NEW.batch_record_id AND
      ((NEW.artifact_kind='allocation_ledger' AND b.state='building' AND b.freeze_started_at IS NULL)
        OR (NEW.artifact_kind<>'allocation_ledger' AND b.state IN ('frozen','index_sealed'))) FOR KEY SHARE)
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
      OR ARRAY(SELECT key FROM jsonb_object_keys(NEW.canonical_plan) AS object_keys(key) ORDER BY key COLLATE "C") IS DISTINCT FROM ARRAY[
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
      OR ARRAY(SELECT key FROM jsonb_object_keys(convert_from(NEW.manifest_bytes,'UTF8')::jsonb) AS object_keys(key) ORDER BY key COLLATE "C") IS DISTINCT FROM ARRAY[
        'allocation','batch_id','clock_hour','delivery_hour','gaps','hour_id','local_date','media','media_tool',
        'policy_version','qualification_day','qualification_sha256','quarantine_evidence','quarantine_reason_code',
        'recording_id','scheduled_end_utc','scheduled_gap','scheduled_start_utc','schema_version','source_claim_sha256',
        'source_count','source_dispositions','sources','status','timezone']::TEXT[]
      OR ARRAY(SELECT key FROM jsonb_object_keys(convert_from(NEW.manifest_bytes,'UTF8')::jsonb->'scheduled_gap') AS object_keys(key) ORDER BY key COLLATE "C")
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
  IF ROW(NEW.account_id, NEW.connection_id, NEW.batch_id, NEW.generation, NEW.source_endpoint,
      NEW.qualification_run_id, NEW.qualification_cohort_sha256, NEW.qualification_windows_sha256,
      NEW.selected_qualification_windows_sha256, NEW.qualification_jobs_sha256, NEW.qualification_frozen_at,
      NEW.ordered_recording_ids_sha256, NEW.selection_basis,
      NEW.policy_version, NEW.eligibility_cutoff, NEW.media_tool, NEW.media_tool_sha256,
      NEW.freeze_request_bytes, NEW.freeze_request_sha256, NEW.frozen_denominator_sha256, NEW.freeze_exclusions_sha256,
      NEW.expected_recordings, NEW.expected_stream_days,
      NEW.expected_scheduled_hours, NEW.expected_source_clips, NEW.expected_source_bytes,
      NEW.expected_freeze_exclusions, NEW.created_at)
    IS DISTINCT FROM ROW(OLD.account_id, OLD.connection_id, OLD.batch_id, OLD.generation, OLD.source_endpoint,
      OLD.qualification_run_id, OLD.qualification_cohort_sha256, OLD.qualification_windows_sha256,
      OLD.selected_qualification_windows_sha256, OLD.qualification_jobs_sha256, OLD.qualification_frozen_at,
      OLD.ordered_recording_ids_sha256, OLD.selection_basis,
      OLD.policy_version, OLD.eligibility_cutoff, OLD.media_tool, OLD.media_tool_sha256,
      OLD.freeze_request_bytes, OLD.freeze_request_sha256, OLD.frozen_denominator_sha256, OLD.freeze_exclusions_sha256,
      OLD.expected_recordings, OLD.expected_stream_days,
      OLD.expected_scheduled_hours, OLD.expected_source_clips, OLD.expected_source_bytes,
      OLD.expected_freeze_exclusions, OLD.created_at)
  THEN RAISE EXCEPTION 'joined batch identity is immutable'; END IF;
  IF OLD.state<>'building' AND NEW.freeze_started_at IS DISTINCT FROM OLD.freeze_started_at
  THEN RAISE EXCEPTION 'joined batch freeze evidence is immutable'; END IF;
  IF OLD.state='snapshotting' AND NEW.state='building' THEN
    IF NEW.freeze_started_at IS NOT NULL OR NEW.frozen_at IS NOT NULL
    THEN RAISE EXCEPTION 'invalid joined batch snapshot transition'; END IF;
  ELSIF OLD.state='building' AND NEW.state='building' THEN
    IF current_setting('transaction_isolation') <> 'read committed' OR OLD.freeze_started_at IS NOT NULL
      OR NEW.freeze_started_at IS NULL OR NEW.freeze_started_at < OLD.created_at OR NEW.freeze_started_at > clock_timestamp()
    THEN RAISE EXCEPTION 'invalid joined batch freeze start'; END IF;
  ELSIF OLD.state='building' AND NEW.state='frozen' THEN
    IF current_setting('transaction_isolation') <> 'read committed'
      OR OLD.freeze_started_at IS NULL OR NEW.freeze_started_at IS DISTINCT FROM OLD.freeze_started_at
      OR NEW.frozen_at IS NULL OR NEW.frozen_at < OLD.eligibility_cutoff OR NEW.frozen_at < OLD.freeze_started_at
      OR NEW.frozen_at > clock_timestamp()
      OR NOT EXISTS(SELECT 1 FROM recording_qualification_runs q
        WHERE q.id=OLD.qualification_run_id AND q.account_id=OLD.account_id
          AND q.cohort_sha256=OLD.qualification_cohort_sha256
          AND q.windows_sha256=OLD.qualification_windows_sha256
          AND q.frozen_at=OLD.qualification_frozen_at FOR KEY SHARE)
      OR (SELECT count(*) FROM recording_joined_batch_recordings br WHERE br.batch_record_id=OLD.id)<>OLD.expected_recordings
      OR (SELECT count(*) FROM recording_joined_stream_days d WHERE d.batch_record_id=OLD.id)<>OLD.expected_stream_days
      OR (SELECT count(*) FROM recording_joined_stream_days d
        WHERE d.batch_record_id=OLD.id AND d.state='sealed')<>OLD.expected_stream_days
      OR (SELECT count(*) FROM recording_joined_source_snapshots s WHERE s.batch_record_id=OLD.id)<>OLD.expected_source_clips
      OR (SELECT COALESCE(sum(size_bytes),0) FROM recording_joined_source_snapshots s
        WHERE s.batch_record_id=OLD.id)<>OLD.expected_source_bytes
      OR (SELECT count(*) FROM recording_joined_hours h WHERE h.batch_record_id=OLD.id)<>OLD.expected_scheduled_hours
      OR (SELECT count(*) FROM recording_joined_sources s WHERE s.batch_record_id=OLD.id)<>OLD.expected_source_clips
      OR (SELECT COALESCE(sum(size_bytes),0) FROM recording_joined_sources s WHERE s.batch_record_id=OLD.id)<>OLD.expected_source_bytes
      OR EXISTS(SELECT 1 FROM recording_joined_batch_recordings br
        LEFT JOIN LATERAL (SELECT count(*) AS days FROM recording_joined_stream_days d
          WHERE d.batch_recording_id=br.id) actual ON TRUE
		WHERE br.batch_record_id=OLD.id AND (actual.days IS DISTINCT FROM 14
		  OR br.priority_ordinal IS DISTINCT FROM (SELECT count(*) FROM recording_joined_batch_recordings earlier
            WHERE earlier.batch_record_id=br.batch_record_id AND earlier.priority_ordinal<=br.priority_ordinal)
		  OR (br.qualification->>'recording_id')::BIGINT IS DISTINCT FROM br.recording_id
		  OR br.qualification->>'timezone' IS DISTINCT FROM br.timezone
		  OR br.qualification->>'evidence_sha256' IS DISTINCT FROM br.qualification_sha256
		  OR (br.qualification->>'frozen_at')::TIMESTAMPTZ IS DISTINCT FROM OLD.eligibility_cutoff
		  OR jsonb_array_length(br.qualification->'days') IS DISTINCT FROM 14))
      OR EXISTS(SELECT 1 FROM recording_joined_stream_days d
        JOIN recording_joined_batch_recordings br ON br.id=d.batch_recording_id
        JOIN recording_qualification_windows w ON w.run_id=br.qualification_run_id
          AND w.recording_id=br.recording_id AND w.ordinal=d.date_ordinal
		WHERE d.batch_record_id=OLD.id AND (d.local_date IS DISTINCT FROM br.first_local_date+d.date_ordinal-1
		  OR br.qualification_run_id IS DISTINCT FROM OLD.qualification_run_id
		  OR ROW(w.window_start_at,w.window_end_at) IS DISTINCT FROM ROW(d.scheduled_start_at,d.scheduled_end_at)
		  OR d.recording_job_id IS DISTINCT FROM br.authoritative_job_ids[d.date_ordinal]
		  OR (br.qualification->'days'->(d.date_ordinal-1)->>'local_date')::DATE IS DISTINCT FROM d.local_date
		  OR (br.qualification->'days'->(d.date_ordinal-1)->>'job_id')::BIGINT IS DISTINCT FROM d.recording_job_id
		  OR (br.qualification->'days'->(d.date_ordinal-1)->>'qualification_window_ordinal')::INTEGER IS DISTINCT FROM d.date_ordinal
		  OR (br.qualification->'days'->(d.date_ordinal-1)->>'window_start')::TIMESTAMPTZ IS DISTINCT FROM d.scheduled_start_at
		  OR (br.qualification->'days'->(d.date_ordinal-1)->>'window_end')::TIMESTAMPTZ IS DISTINCT FROM d.scheduled_end_at
		  OR (br.qualification->'days'->(d.date_ordinal-1)->>'completed_at')::TIMESTAMPTZ IS DISTINCT FROM d.completed_at))
      OR (SELECT count(*) FROM recording_joined_artifacts a
        WHERE a.batch_record_id=OLD.id AND a.artifact_kind='allocation_ledger')<>OLD.expected_stream_days
      OR EXISTS(SELECT 1 FROM recording_joined_artifacts a
        WHERE a.batch_record_id=OLD.id AND a.artifact_kind<>'allocation_ledger')
    THEN RAISE EXCEPTION 'joined batch freeze is incomplete'; END IF;
    PERFORM validate_recording_joined_stream_day(d.id) FROM recording_joined_stream_days d WHERE d.batch_record_id=OLD.id;
  ELSIF OLD.state = 'frozen' AND NEW.state = 'index_sealed' THEN
	IF OLD.frozen_at IS NULL OR NEW.frozen_at IS DISTINCT FROM OLD.frozen_at
	  OR NEW.index_artifact_id IS NULL OR NEW.index_sealed_at IS NULL
	  OR NOT EXISTS(SELECT 1 FROM recording_joined_artifacts a WHERE a.id=NEW.index_artifact_id
	    AND a.batch_record_id=OLD.id AND a.artifact_kind='batch_index')
      OR (SELECT count(*) FROM recording_joined_batch_index_refs r
		WHERE r.batch_record_id=OLD.id AND r.index_artifact_id = NEW.index_artifact_id
		  AND r.reference_kind = 'allocation_ledger') <> OLD.expected_stream_days
      OR (SELECT count(*) FROM recording_joined_batch_index_refs r
		WHERE r.batch_record_id=OLD.id AND r.index_artifact_id = NEW.index_artifact_id
		  AND r.reference_kind = 'hour_manifest') <> OLD.expected_scheduled_hours
    THEN RAISE EXCEPTION 'joined batch index reference set is incomplete'; END IF;
  ELSIF OLD.state = 'index_sealed' AND NEW.state = 'published' THEN
	IF OLD.frozen_at IS NULL OR NEW.frozen_at IS DISTINCT FROM OLD.frozen_at
	  OR NEW.index_artifact_id IS DISTINCT FROM OLD.index_artifact_id
	  OR NEW.index_sealed_at IS DISTINCT FROM OLD.index_sealed_at OR NEW.published_at IS NULL
      OR NOT EXISTS(SELECT 1 FROM recording_joined_artifacts a WHERE a.id = OLD.index_artifact_id AND a.publication_state = 'published')
    THEN RAISE EXCEPTION 'invalid joined batch publication'; END IF;
  ELSIF OLD.state IN ('building', 'frozen', 'index_sealed') AND NEW.state = 'terminal_failed' THEN
    IF NEW.failure_reason_code = '' OR OLD.failure_reason_code <> ''
      OR ROW(NEW.index_artifact_id,NEW.freeze_started_at,NEW.frozen_at,NEW.index_sealed_at,NEW.published_at)
        IS DISTINCT FROM ROW(OLD.index_artifact_id,OLD.freeze_started_at,OLD.frozen_at,OLD.index_sealed_at,OLD.published_at)
    THEN RAISE EXCEPTION 'joined terminal batch failure lacks reason or rewrites evidence'; END IF;
  ELSE RAISE EXCEPTION 'invalid joined batch transition'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_batch_update_guard BEFORE UPDATE ON recording_joined_batches
FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_batch_update();

CREATE FUNCTION validate_recording_joined_freeze_finished() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF EXISTS(SELECT 1 FROM recording_joined_batches b
    WHERE b.id=NEW.id AND b.state='building' AND b.freeze_started_at IS NOT NULL)
  THEN RAISE EXCEPTION 'joined batch freeze fence cannot commit unfinished'; END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER recording_joined_batch_freeze_finished
AFTER UPDATE OF freeze_started_at ON recording_joined_batches DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_recording_joined_freeze_finished();

CREATE FUNCTION reject_recording_joined_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'joined recording evidence is append-only'; END $$;

CREATE FUNCTION guard_recording_joined_stream_day_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.state<>'pending' OR NEW.state<>'sealed'
    OR ROW(NEW.batch_record_id,NEW.batch_recording_id,NEW.account_id,NEW.connection_id,NEW.batch_id,
      NEW.recording_id,NEW.local_date,NEW.date_ordinal,NEW.recording_job_id,NEW.scheduled_start_at,
      NEW.scheduled_end_at,NEW.completed_at,NEW.source_clip_count,NEW.source_bytes,NEW.source_snapshot_sha256,NEW.created_at)
      IS DISTINCT FROM ROW(OLD.batch_record_id,OLD.batch_recording_id,OLD.account_id,OLD.connection_id,OLD.batch_id,
      OLD.recording_id,OLD.local_date,OLD.date_ordinal,OLD.recording_job_id,OLD.scheduled_start_at,
      OLD.scheduled_end_at,OLD.completed_at,OLD.source_clip_count,OLD.source_bytes,OLD.source_snapshot_sha256,OLD.created_at)
    OR NOT EXISTS(SELECT 1 FROM recording_joined_batches b
      WHERE b.id=OLD.batch_record_id AND b.state='building' AND b.freeze_started_at IS NULL FOR SHARE)
    OR (SELECT count(*) FROM recording_joined_source_snapshots s WHERE s.stream_day_id=OLD.id)<>OLD.source_clip_count
    OR (SELECT COALESCE(sum(size_bytes),0) FROM recording_joined_source_snapshots s WHERE s.stream_day_id=OLD.id)<>OLD.source_bytes
    OR (SELECT count(*) FROM recording_joined_sources s WHERE s.stream_day_id=OLD.id)<>OLD.source_clip_count
    OR (SELECT COALESCE(sum(size_bytes),0) FROM recording_joined_sources s WHERE s.stream_day_id=OLD.id)<>OLD.source_bytes
    OR EXISTS(SELECT 1 FROM recording_joined_source_snapshots snapshot
      LEFT JOIN recording_joined_sources observed ON observed.source_snapshot_id=snapshot.id
      WHERE snapshot.stream_day_id=OLD.id AND (observed.id IS NULL OR observed.stream_day_id<>OLD.id))
    OR (SELECT count(*) FROM recording_joined_hours h WHERE h.stream_day_id=OLD.id)<>12
    OR (SELECT count(*) FROM recording_joined_day_boundaries b
      WHERE b.stream_day_id=OLD.id AND b.boundary_kind='cross_hour')<>11
    OR (SELECT count(*) FROM recording_joined_day_boundaries b
      WHERE b.stream_day_id=OLD.id AND b.boundary_kind='cross_day')<>2
  THEN RAISE EXCEPTION 'joined stream day seal differs from frozen snapshot'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_stream_day_update_guard BEFORE UPDATE ON recording_joined_stream_days
FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_stream_day_update();

CREATE FUNCTION validate_recording_joined_stream_day_after_seal() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  PERFORM validate_recording_joined_stream_day(NEW.id);
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER recording_joined_stream_day_validate_after_seal
AFTER UPDATE OF state ON recording_joined_stream_days DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW WHEN (NEW.state='sealed') EXECUTE FUNCTION validate_recording_joined_stream_day_after_seal();

CREATE TRIGGER recording_joined_batch_no_delete BEFORE DELETE ON recording_joined_batches
  FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_mutation();
CREATE TRIGGER recording_joined_batch_recording_no_mutation BEFORE UPDATE OR DELETE ON recording_joined_batch_recordings
  FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_mutation();
CREATE TRIGGER recording_joined_exclusion_no_mutation BEFORE UPDATE OR DELETE ON recording_joined_freeze_exclusions
  FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_mutation();
CREATE TRIGGER recording_joined_stream_day_no_delete BEFORE DELETE ON recording_joined_stream_days
  FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_mutation();
CREATE TRIGGER recording_joined_source_snapshot_no_mutation BEFORE UPDATE OR DELETE ON recording_joined_source_snapshots
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
CREATE TRIGGER recording_joined_source_snapshot_no_truncate BEFORE TRUNCATE ON recording_joined_source_snapshots
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
