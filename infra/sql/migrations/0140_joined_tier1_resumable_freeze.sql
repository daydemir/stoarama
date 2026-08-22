-- Resumable Tier-1 source freeze. The complete generation identity is approved
-- up front, while each recording's raw evidence is copied in its own short
-- transaction. Scope and receipts are append-only authority.

DO $$ BEGIN
  PERFORM pg_advisory_xact_lock(137,1);
  IF EXISTS(SELECT 1 FROM recording_joined_batches)
  THEN RAISE EXCEPTION 'cannot install denominator v2 while a v1 joined batch exists'; END IF;
END $$;

CREATE TABLE recording_joined_snapshot_scopes (
  id BIGSERIAL PRIMARY KEY,
  batch_record_id BIGINT NOT NULL REFERENCES recording_joined_batches(id) ON DELETE RESTRICT,
  batch_recording_id BIGINT NOT NULL REFERENCES recording_joined_batch_recordings(id) ON DELETE RESTRICT,
  recording_id BIGINT NOT NULL CHECK (recording_id > 0),
  priority_ordinal SMALLINT NOT NULL CHECK (priority_ordinal BETWEEN 1 AND 33),
  local_date DATE NOT NULL,
  date_ordinal SMALLINT NOT NULL CHECK (date_ordinal BETWEEN 1 AND 14),
  recording_job_id BIGINT NOT NULL REFERENCES recording_jobs(id) ON DELETE RESTRICT,
  scheduled_start_at TIMESTAMPTZ NOT NULL,
  scheduled_end_at TIMESTAMPTZ NOT NULL,
  completed_at TIMESTAMPTZ NOT NULL,
  high_water_clip_id BIGINT NOT NULL CHECK (high_water_clip_id >= 0),
  expected_source_clips INTEGER NOT NULL CHECK (expected_source_clips >= 0),
  expected_source_bytes BIGINT NOT NULL CHECK (expected_source_bytes >= 0),
  expected_source_sha256 TEXT NOT NULL CHECK (expected_source_sha256 ~ '^[0-9a-f]{64}$'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (batch_record_id,batch_recording_id,recording_id)
    REFERENCES recording_joined_batch_recordings(batch_record_id,id,recording_id) ON DELETE RESTRICT,
  CHECK (scheduled_end_at > scheduled_start_at AND completed_at >= scheduled_start_at),
  CHECK ((expected_source_clips=0 AND expected_source_bytes=0)
    OR (expected_source_clips>0 AND expected_source_bytes>0)),
  UNIQUE (batch_record_id,recording_job_id),
  UNIQUE (batch_record_id,recording_id,date_ordinal),
  UNIQUE (batch_recording_id,date_ordinal)
);

CREATE INDEX recording_joined_snapshot_scopes_retention_idx
  ON recording_joined_snapshot_scopes(recording_id,recording_job_id,high_water_clip_id);

CREATE TABLE recording_joined_snapshot_chunks (
  id BIGSERIAL PRIMARY KEY,
  batch_record_id BIGINT NOT NULL REFERENCES recording_joined_batches(id) ON DELETE RESTRICT,
  batch_recording_id BIGINT NOT NULL REFERENCES recording_joined_batch_recordings(id) ON DELETE RESTRICT,
  priority_ordinal SMALLINT NOT NULL CHECK (priority_ordinal BETWEEN 1 AND 33),
  recording_id BIGINT NOT NULL CHECK (recording_id > 0),
  expected_source_clips BIGINT NOT NULL CHECK (expected_source_clips >= 0),
  expected_source_bytes BIGINT NOT NULL CHECK (expected_source_bytes >= 0),
  expected_exclusions BIGINT NOT NULL CHECK (expected_exclusions >= 0),
  expected_exclusions_sha256 TEXT NOT NULL CHECK (expected_exclusions_sha256 ~ '^[0-9a-f]{64}$'),
  actual_source_clips BIGINT NOT NULL CHECK (actual_source_clips >= 0),
  actual_source_bytes BIGINT NOT NULL CHECK (actual_source_bytes >= 0),
  actual_exclusions BIGINT NOT NULL CHECK (actual_exclusions >= 0),
  actual_exclusions_sha256 TEXT NOT NULL CHECK (actual_exclusions_sha256 ~ '^[0-9a-f]{64}$'),
  receipt_sha256 TEXT NOT NULL CHECK (receipt_sha256 ~ '^[0-9a-f]{64}$'),
  completed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (batch_record_id,batch_recording_id,recording_id)
    REFERENCES recording_joined_batch_recordings(batch_record_id,id,recording_id) ON DELETE RESTRICT,
  CHECK (expected_source_clips=actual_source_clips AND expected_source_bytes=actual_source_bytes
    AND expected_exclusions=actual_exclusions
    AND expected_exclusions_sha256=actual_exclusions_sha256),
  UNIQUE (batch_record_id,priority_ordinal),
  UNIQUE (batch_record_id,recording_id),
  UNIQUE (batch_recording_id)
);

CREATE FUNCTION reject_recording_joined_snapshot_authority_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'joined snapshot authority is immutable'; END $$;
CREATE TRIGGER recording_joined_snapshot_scopes_immutable
  BEFORE UPDATE OR DELETE ON recording_joined_snapshot_scopes
  FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_snapshot_authority_mutation();
CREATE TRIGGER recording_joined_snapshot_chunks_immutable
  BEFORE UPDATE OR DELETE ON recording_joined_snapshot_chunks
  FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_snapshot_authority_mutation();
CREATE TRIGGER recording_joined_snapshot_scopes_no_truncate
  BEFORE TRUNCATE ON recording_joined_snapshot_scopes
  FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_snapshot_authority_mutation();
CREATE TRIGGER recording_joined_snapshot_chunks_no_truncate
  BEFORE TRUNCATE ON recording_joined_snapshot_chunks
  FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_snapshot_authority_mutation();

-- V1 validated completeness on the initial INSERT. V2 keeps snapshotting
-- intentionally partial and validates only the explicit visibility transition.
DROP TRIGGER recording_joined_batch_snapshot_complete ON recording_joined_batches;
DO $$
DECLARE definition TEXT;
BEGIN
  definition := pg_get_functiondef('validate_recording_joined_batch_snapshot()'::regprocedure);
  definition := replace(definition,
    'ARRAY[''frozen_recording'',''qualification'']::TEXT[]',
    'ARRAY[''expected_exclusions'',''expected_exclusions_sha256'',''expected_source_bytes'',''expected_source_clips'',''frozen_recording'',''qualification'',''snapshot_days'']::TEXT[]');
  IF definition NOT LIKE '%expected_exclusions_sha256%' THEN
    RAISE EXCEPTION 'joined v2 completeness validator rewrite failed';
  END IF;
  EXECUTE definition;
END $$;
CREATE CONSTRAINT TRIGGER recording_joined_batch_snapshot_complete
AFTER UPDATE ON recording_joined_batches DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW WHEN (NEW.state='building' AND OLD.state='snapshotting')
EXECUTE FUNCTION validate_recording_joined_batch_snapshot();

-- Replace the v1 wire guard. All detailed facts remain covered by the stored
-- byte hash; this guard binds those bytes to the owning live authorities.
CREATE OR REPLACE FUNCTION guard_recording_joined_batch_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE request JSONB;
BEGIN
  IF current_setting('transaction_isolation') <> 'read committed'
  THEN RAISE EXCEPTION 'joined snapshot apply requires read committed'; END IF;
  PERFORM pg_advisory_xact_lock(137,1);
  request := convert_from(NEW.freeze_request_bytes,'UTF8')::JSONB;
  IF NEW.state<>'snapshotting' OR NEW.freeze_started_at IS NOT NULL OR NEW.frozen_at IS NOT NULL
    OR ARRAY(SELECT key FROM jsonb_object_keys(request) object_keys(key) ORDER BY key COLLATE "C") IS DISTINCT FROM ARRAY[
      'account_id','batch_id','connection_id','expected_scheduled_hours','expected_stream_days','freeze_exclusions_sha256',
      'frozen_denominator_sha256','generation','media_tool','policy_version','provisional_exclusions',
      'provisional_source_bytes','provisional_source_clips','qualification_jobs_sha256','recording_ids','recordings',
      'schema_version','selection_authority','source_endpoint']::TEXT[]
    OR (request->>'schema_version')::INTEGER IS DISTINCT FROM 2
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
    OR jsonb_array_length(request->'recordings') IS DISTINCT FROM 33
    OR request->'recording_ids' IS DISTINCT FROM to_jsonb(ARRAY[
      377,335,337,355,385,350,382,384,348,403,380,379,383,404,401,408,406,
      409,422,418,419,413,420,428,423,425,416,421,437,440,429,431,439]::BIGINT[])
    OR request->'selection_authority'->>'selection_basis' IS DISTINCT FROM NEW.selection_basis
    OR ARRAY(SELECT key FROM jsonb_object_keys(request->'selection_authority') keys(key) ORDER BY key COLLATE "C")
      IS DISTINCT FROM ARRAY['cutoff','ordered_recording_ids_sha256','qualification_cohort_sha256',
        'qualification_rule_version','qualification_run_frozen_at','qualification_run_id',
        'qualification_windows_sha256','selected_qualification_windows_sha256','selection_basis']::TEXT[]
    OR request->'selection_authority'->>'ordered_recording_ids_sha256' IS DISTINCT FROM NEW.ordered_recording_ids_sha256
    OR (request->'selection_authority'->>'cutoff')::TIMESTAMPTZ IS DISTINCT FROM NEW.eligibility_cutoff
    OR (request->'selection_authority'->>'qualification_run_id')::BIGINT IS DISTINCT FROM NEW.qualification_run_id
    OR (request->'selection_authority'->>'qualification_run_frozen_at')::TIMESTAMPTZ IS DISTINCT FROM NEW.qualification_frozen_at
    OR request->'selection_authority'->>'qualification_cohort_sha256' IS DISTINCT FROM NEW.qualification_cohort_sha256
    OR request->'selection_authority'->>'qualification_windows_sha256' IS DISTINCT FROM NEW.qualification_windows_sha256
    OR request->'selection_authority'->>'selected_qualification_windows_sha256' IS DISTINCT FROM NEW.selected_qualification_windows_sha256
    OR request->'selection_authority'->>'qualification_rule_version' IS DISTINCT FROM
      (SELECT q.definition_version FROM recording_qualification_runs q WHERE q.id=NEW.qualification_run_id)
    OR EXISTS(SELECT 1 FROM jsonb_array_elements(request->'recordings') WITH ORDINALITY item(value,ordinal)
      WHERE ARRAY(SELECT key FROM jsonb_object_keys(value) keys(key) ORDER BY key COLLATE "C") IS DISTINCT FROM ARRAY[
          'expected_exclusions','expected_exclusions_sha256','expected_source_bytes','expected_source_clips',
          'frozen_recording','qualification','snapshot_days']::TEXT[]
        OR (value->'frozen_recording'->>'priority_ordinal')::INTEGER IS DISTINCT FROM ordinal
        OR ARRAY(SELECT key FROM jsonb_object_keys(value->'frozen_recording') keys(key) ORDER BY key COLLATE "C")
          IS DISTINCT FROM ARRAY['completed_at','folder_name','naming_metadata','priority_ordinal',
            'qualification_sha256','recording_id','selection_tier','timezone']::TEXT[]
        OR (value->'frozen_recording'->>'recording_id')::BIGINT IS DISTINCT FROM
          (request->'recording_ids'->>((ordinal-1)::INTEGER))::BIGINT
        OR jsonb_array_length(value->'snapshot_days') IS DISTINCT FROM 14
        OR EXISTS(SELECT 1 FROM jsonb_array_elements(value->'snapshot_days') WITH ORDINALITY day(fact,day_ordinal)
          WHERE ARRAY(SELECT key FROM jsonb_object_keys(fact) keys(key) ORDER BY key COLLATE "C") IS DISTINCT FROM ARRAY[
              'date_ordinal','expected_source_bytes','expected_source_clips','expected_source_sha256',
              'high_water_clip_id','local_date','recording_job_id']::TEXT[]
            OR (fact->>'date_ordinal')::INTEGER IS DISTINCT FROM day_ordinal))
    OR NOT EXISTS(SELECT 1 FROM connections c WHERE c.id=NEW.connection_id AND c.account_id=NEW.account_id
      AND c.joined_protocol_version=1 FOR UPDATE)
    OR NOT EXISTS(SELECT 1 FROM recording_qualification_runs q WHERE q.id=NEW.qualification_run_id
      AND q.account_id=NEW.account_id AND q.status='active' AND q.cohort_sha256=NEW.qualification_cohort_sha256
      AND q.windows_sha256=NEW.qualification_windows_sha256 AND q.frozen_at=NEW.qualification_frozen_at FOR SHARE)
  THEN RAISE EXCEPTION 'joined batch must enter an owned snapshotting state'; END IF;
  RETURN NEW;
END $$;

CREATE FUNCTION guard_recording_joined_snapshot_scope_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expected JSONB; expected_recording JSONB; batch_state TEXT; br recording_joined_batch_recordings%ROWTYPE; qualification_day JSONB;
BEGIN
  SELECT b.state,convert_from(b.freeze_request_bytes,'UTF8')::JSONB->'recordings'->(NEW.priority_ordinal-1)
    INTO STRICT batch_state,expected_recording
  FROM recording_joined_batches b WHERE b.id=NEW.batch_record_id FOR SHARE;
  expected := expected_recording->'snapshot_days'->(NEW.date_ordinal-1);
  SELECT * INTO STRICT br FROM recording_joined_batch_recordings
    WHERE id=NEW.batch_recording_id AND batch_record_id=NEW.batch_record_id FOR SHARE;
  qualification_day := br.qualification->'days'->(NEW.date_ordinal-1);
  IF batch_state<>'snapshotting' OR EXISTS(SELECT 1 FROM recording_joined_snapshot_chunks c
      WHERE c.batch_recording_id=NEW.batch_recording_id)
    OR ROW(br.recording_id,br.priority_ordinal) IS DISTINCT FROM ROW(NEW.recording_id,NEW.priority_ordinal)
    OR ROW((expected_recording->'frozen_recording'->>'recording_id')::BIGINT,
      (expected_recording->'frozen_recording'->>'priority_ordinal')::INTEGER,
      expected_recording->'frozen_recording'->>'qualification_sha256',
      (expected_recording->'frozen_recording'->>'completed_at')::TIMESTAMPTZ,
      expected_recording->'frozen_recording'->>'timezone',expected_recording->'frozen_recording'->>'folder_name',
      expected_recording->'frozen_recording'->'naming_metadata',expected_recording->'qualification')
      IS DISTINCT FROM ROW(br.recording_id,br.priority_ordinal,br.qualification_sha256,br.completed_at,
        br.timezone,br.folder_name,br.naming_metadata,br.qualification)
    OR (expected->>'recording_job_id')::BIGINT IS DISTINCT FROM NEW.recording_job_id
    OR (expected->>'high_water_clip_id')::BIGINT IS DISTINCT FROM NEW.high_water_clip_id
    OR (expected->>'expected_source_clips')::INTEGER IS DISTINCT FROM NEW.expected_source_clips
    OR (expected->>'expected_source_bytes')::BIGINT IS DISTINCT FROM NEW.expected_source_bytes
    OR expected->>'expected_source_sha256' IS DISTINCT FROM NEW.expected_source_sha256
    OR expected->>'local_date' IS DISTINCT FROM NEW.local_date::TEXT
    OR (expected->>'date_ordinal')::INTEGER IS DISTINCT FROM NEW.date_ordinal
    OR ROW((qualification_day->>'job_id')::BIGINT,(qualification_day->>'window_start')::TIMESTAMPTZ,
      (qualification_day->>'window_end')::TIMESTAMPTZ,(qualification_day->>'completed_at')::TIMESTAMPTZ)
      IS DISTINCT FROM ROW(NEW.recording_job_id,NEW.scheduled_start_at,NEW.scheduled_end_at,NEW.completed_at)
  THEN RAISE EXCEPTION 'joined snapshot scope differs from canonical v2 plan'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_snapshot_scope_insert_guard BEFORE INSERT ON recording_joined_snapshot_scopes
  FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_snapshot_scope_insert();

-- Reproduce encoding/json for the exact denominator-v2 source projection.
-- released_at remains in retained audit rows but is always null in identity.
CREATE FUNCTION recording_joined_snapshot_day_sha256(p_stream_day_id BIGINT)
RETURNS TEXT LANGUAGE SQL STABLE STRICT AS $$
  SELECT encode(sha256(convert_to(CASE WHEN count(*)=0 THEN 'null' ELSE '['||string_agg(
    '{"clip_id":'||s.clip_id::TEXT||
    ',"recording_id":'||s.recording_id::TEXT||
    ',"recording_job_id":'||s.recording_job_id::TEXT||
    ',"storage_destination_id":'||s.storage_destination_id::TEXT||
    ',"provider":'||recording_historical_go_string_json(s.provider)||
    ',"endpoint":'||recording_historical_go_string_json(s.endpoint)||
    ',"region":'||recording_historical_go_string_json(s.region)||
    ',"bucket":'||recording_historical_go_string_json(s.bucket)||
    ',"object_key":'||recording_historical_go_string_json(s.object_key)||
    ',"start_utc":'||recording_historical_go_utc_time_json(to_jsonb(s.start_at))||
    ',"end_utc":'||recording_historical_go_utc_time_json(to_jsonb(s.end_at))||
    ',"size_bytes":'||s.size_bytes::TEXT||
    ',"ingest_sha256":'||recording_historical_go_string_json(s.sha256)||
    ',"released_at":null}',',' ORDER BY s.day_ordinal)||']' END,'UTF8')),'hex')
  FROM recording_joined_source_snapshots s WHERE s.stream_day_id=p_stream_day_id;
$$;

CREATE FUNCTION guard_recording_joined_snapshot_chunk_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE batch_state TEXT; expected JSONB; exclusion_sha TEXT; expected_receipt TEXT;
BEGIN
  SELECT b.state,convert_from(b.freeze_request_bytes,'UTF8')::JSONB->'recordings'->(NEW.priority_ordinal-1)
    INTO STRICT batch_state,expected FROM recording_joined_batches b WHERE b.id=NEW.batch_record_id FOR SHARE;
  SELECT encode(sha256(convert_to(COALESCE(string_agg(format('%s\n%s\n%s\n%s\n',e.recording_id,
      COALESCE(e.clip_id::TEXT,''),e.reason_code,e.evidence_sha256),'' ORDER BY e.recording_id,e.clip_id,
      e.reason_code,e.evidence_sha256),''),'UTF8')),'hex') INTO exclusion_sha
    FROM recording_joined_freeze_exclusions e WHERE e.batch_recording_id=NEW.batch_recording_id;
  expected_receipt := encode(sha256(convert_to(format(
    '{"priority":%s,"recording_id":%s,"source_clips":%s,"source_bytes":%s,"exclusions":%s,"exclusions_sha256":"%s"}',
    NEW.priority_ordinal,NEW.recording_id,NEW.expected_source_clips,NEW.expected_source_bytes,
    NEW.expected_exclusions,NEW.expected_exclusions_sha256),'UTF8')),'hex');
  IF batch_state<>'snapshotting'
    OR (expected->'frozen_recording'->>'recording_id')::BIGINT IS DISTINCT FROM NEW.recording_id
    OR (expected->>'expected_source_clips')::BIGINT IS DISTINCT FROM NEW.expected_source_clips
    OR (expected->>'expected_source_bytes')::BIGINT IS DISTINCT FROM NEW.expected_source_bytes
    OR (expected->>'expected_exclusions')::BIGINT IS DISTINCT FROM NEW.expected_exclusions
    OR expected->>'expected_exclusions_sha256' IS DISTINCT FROM NEW.expected_exclusions_sha256
    OR (SELECT count(*) FROM recording_joined_stream_days d WHERE d.batch_recording_id=NEW.batch_recording_id)<>14
    OR (SELECT count(*) FROM recording_joined_source_snapshots s JOIN recording_joined_stream_days d
      ON d.id=s.stream_day_id WHERE d.batch_recording_id=NEW.batch_recording_id)<>NEW.actual_source_clips
    OR (SELECT COALESCE(sum(s.size_bytes),0) FROM recording_joined_source_snapshots s JOIN recording_joined_stream_days d
      ON d.id=s.stream_day_id WHERE d.batch_recording_id=NEW.batch_recording_id)<>NEW.actual_source_bytes
    OR EXISTS(SELECT 1 FROM recording_joined_snapshot_scopes scope
      LEFT JOIN recording_joined_stream_days day ON day.batch_recording_id=scope.batch_recording_id
        AND day.date_ordinal=scope.date_ordinal
      WHERE scope.batch_recording_id=NEW.batch_recording_id AND (day.id IS NULL
        OR ROW(day.source_clip_count,day.source_bytes,day.source_snapshot_sha256)
          IS DISTINCT FROM ROW(scope.expected_source_clips,scope.expected_source_bytes,scope.expected_source_sha256)
        OR recording_joined_snapshot_day_sha256(day.id) IS DISTINCT FROM scope.expected_source_sha256))
    OR (SELECT count(*) FROM recording_joined_freeze_exclusions e
      WHERE e.batch_recording_id=NEW.batch_recording_id)<>NEW.actual_exclusions
    OR exclusion_sha IS DISTINCT FROM NEW.actual_exclusions_sha256
    OR expected_receipt IS DISTINCT FROM NEW.receipt_sha256
  THEN RAISE EXCEPTION 'joined snapshot chunk receipt differs'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_snapshot_chunk_insert_guard BEFORE INSERT ON recording_joined_snapshot_chunks
  FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_snapshot_chunk_insert();

-- Terminal jobs should not receive new source facts. Only those jobs take the
-- shared fence, so current recording inserts never queue behind a freeze call.
CREATE FUNCTION fence_recording_joined_terminal_clip_membership() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE old_job_id BIGINT; new_job_id BIGINT;
BEGIN
  old_job_id := CASE WHEN TG_OP='UPDATE' THEN OLD.recording_job_id ELSE NULL END;
  new_job_id := NEW.recording_job_id;
  IF EXISTS(SELECT 1 FROM recording_jobs j WHERE j.id IN (old_job_id,new_job_id) AND j.status IN ('done','error'))
  THEN PERFORM pg_advisory_xact_lock_shared(137,1); END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_terminal_clip_insert_fence BEFORE INSERT ON recording_clips
  FOR EACH ROW EXECUTE FUNCTION fence_recording_joined_terminal_clip_membership();
CREATE TRIGGER recording_joined_terminal_clip_update_fence BEFORE UPDATE OF recording_id,recording_job_id,
  storage_destination_id,endpoint,bucket,object_key,etag,size_bytes,sha256,clip_start_at,clip_end_at,created_at
  ON recording_clips FOR EACH ROW EXECUTE FUNCTION fence_recording_joined_terminal_clip_membership();

CREATE FUNCTION guard_recording_joined_snapshot_source_scope() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF EXISTS(SELECT 1 FROM inserted source
    LEFT JOIN recording_joined_snapshot_scopes scope ON scope.batch_record_id=source.batch_record_id
      AND scope.recording_id=source.recording_id AND scope.recording_job_id=source.recording_job_id
    LEFT JOIN recording_joined_snapshot_chunks chunk ON chunk.batch_recording_id=scope.batch_recording_id
    WHERE scope.id IS NULL OR source.clip_id>scope.high_water_clip_id OR chunk.id IS NOT NULL)
  THEN RAISE EXCEPTION 'joined source is outside resumable snapshot scope'; END IF;
  RETURN NULL;
END $$;
CREATE TRIGGER recording_joined_snapshot_source_scope_guard
  AFTER INSERT ON recording_joined_source_snapshots REFERENCING NEW TABLE AS inserted
  FOR EACH STATEMENT EXECUTE FUNCTION guard_recording_joined_snapshot_source_scope();

-- Scope protects planned clips before their recording chunk is copied. IDs
-- above the captured watermark belong to a later generation and remain free.
CREATE OR REPLACE FUNCTION guard_recording_joined_clip_retention() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE target_clip_id BIGINT; clip_recording BIGINT; clip_job BIGINT; old_scoped BOOLEAN; new_scoped BOOLEAN := FALSE;
BEGIN
  IF current_setting('transaction_isolation') <> 'read committed'
  THEN RAISE EXCEPTION 'joined source purge requires read committed'; END IF;
  PERFORM pg_advisory_xact_lock_shared(137,1);
  target_clip_id:=OLD.id; clip_recording:=OLD.recording_id; clip_job:=OLD.recording_job_id;
  SELECT EXISTS(SELECT 1 FROM recording_joined_snapshot_scopes scope
    JOIN recording_joined_batches batch ON batch.id=scope.batch_record_id AND batch.state='snapshotting'
    WHERE scope.recording_id=clip_recording AND scope.recording_job_id=clip_job
      AND target_clip_id<=scope.high_water_clip_id) INTO old_scoped;
  IF TG_OP='UPDATE' THEN
    SELECT EXISTS(SELECT 1 FROM recording_joined_snapshot_scopes scope
      JOIN recording_joined_batches batch ON batch.id=scope.batch_record_id AND batch.state='snapshotting'
      WHERE scope.recording_id=NEW.recording_id AND scope.recording_job_id=NEW.recording_job_id
        AND NEW.id<=scope.high_water_clip_id) INTO new_scoped;
  END IF;
  IF (TG_OP='DELETE' OR NEW.id IS DISTINCT FROM OLD.id
      OR (OLD.purged_at IS NULL AND NEW.purged_at IS NOT NULL))
    AND (EXISTS(SELECT 1 FROM recording_joined_source_snapshots s WHERE s.clip_id=target_clip_id)
      OR old_scoped)
  THEN RAISE EXCEPTION 'joined frozen source is retention protected'; END IF;
  IF TG_OP='UPDATE' AND (old_scoped OR new_scoped) AND ROW(NEW.recording_id,NEW.recording_job_id,NEW.storage_destination_id,
      NEW.endpoint,NEW.bucket,NEW.object_key,NEW.etag,NEW.size_bytes,NEW.sha256,NEW.clip_start_at,NEW.clip_end_at,
      NEW.created_at)
    IS DISTINCT FROM ROW(OLD.recording_id,OLD.recording_job_id,OLD.storage_destination_id,
      OLD.endpoint,OLD.bucket,OLD.object_key,OLD.etag,OLD.size_bytes,OLD.sha256,OLD.clip_start_at,OLD.clip_end_at,
      OLD.created_at)
  THEN RAISE EXCEPTION 'joined scoped source identity is immutable'; END IF;
  IF TG_OP='DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_scoped_clip_identity_update BEFORE UPDATE OF recording_id,recording_job_id,
  storage_destination_id,endpoint,bucket,object_key,etag,size_bytes,sha256,clip_start_at,clip_end_at,created_at
  ON recording_clips FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_clip_retention();

-- Extra transition guard moves v1's all-at-insert assumption to the explicit
-- finalization call. It runs alongside the existing state-machine trigger.
CREATE FUNCTION guard_recording_joined_snapshot_finalize() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.state='snapshotting' AND NEW.state='building' AND (
    (SELECT count(*) FROM recording_joined_snapshot_scopes s WHERE s.batch_record_id=OLD.id)<>OLD.expected_stream_days
    OR (SELECT count(*) FROM recording_joined_snapshot_chunks c WHERE c.batch_record_id=OLD.id)<>OLD.expected_recordings
    OR (SELECT count(*) FROM recording_joined_batch_recordings r WHERE r.batch_record_id=OLD.id)<>OLD.expected_recordings
    OR (SELECT count(*) FROM recording_joined_stream_days d WHERE d.batch_record_id=OLD.id)<>OLD.expected_stream_days
    OR (SELECT count(*) FROM recording_joined_source_snapshots s WHERE s.batch_record_id=OLD.id)<>OLD.expected_source_clips
    OR (SELECT COALESCE(sum(s.size_bytes),0) FROM recording_joined_source_snapshots s WHERE s.batch_record_id=OLD.id)<>OLD.expected_source_bytes
    OR (SELECT count(*) FROM recording_joined_freeze_exclusions e WHERE e.batch_record_id=OLD.id)<>OLD.expected_freeze_exclusions
    OR EXISTS(SELECT 1 FROM recording_joined_snapshot_chunks c
      WHERE c.batch_record_id=OLD.id AND c.priority_ordinal IS DISTINCT FROM
        (SELECT count(*) FROM recording_joined_snapshot_chunks earlier
          WHERE earlier.batch_record_id=OLD.id AND earlier.priority_ordinal<=c.priority_ordinal))
    OR EXISTS(SELECT 1 FROM recording_joined_snapshot_scopes scope
      LEFT JOIN recording_joined_stream_days day ON day.batch_recording_id=scope.batch_recording_id
        AND day.date_ordinal=scope.date_ordinal
      WHERE scope.batch_record_id=OLD.id AND (day.id IS NULL
        OR ROW(day.recording_id,day.recording_job_id,day.source_clip_count,day.source_bytes,day.source_snapshot_sha256)
          IS DISTINCT FROM ROW(scope.recording_id,scope.recording_job_id,scope.expected_source_clips,
            scope.expected_source_bytes,scope.expected_source_sha256)))
  ) THEN RAISE EXCEPTION 'joined resumable snapshot is incomplete'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER aaa_recording_joined_snapshot_finalize_guard
  BEFORE UPDATE OF state ON recording_joined_batches
  FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_snapshot_finalize();
