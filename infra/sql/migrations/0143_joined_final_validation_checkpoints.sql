BEGIN;

-- Final-freeze validation is durable evidence, not a second source of truth.
-- The existing stream-day validator remains authoritative; these rows only
-- record its successful completion in short, restartable transactions.
CREATE TABLE recording_joined_final_validation_runs (
  id UUID PRIMARY KEY,
  batch_record_id BIGINT NOT NULL UNIQUE REFERENCES recording_joined_batches(id) ON DELETE RESTRICT,
  batch_id TEXT NOT NULL,
  generation INTEGER NOT NULL CHECK (generation > 0),
  validator_version TEXT NOT NULL CHECK (validator_version = 'stream-day-v1'),
  expected_denominator_sha256 TEXT NOT NULL CHECK (expected_denominator_sha256 ~ '^[0-9a-f]{64}$'),
  expected_stream_days INTEGER NOT NULL CHECK (expected_stream_days > 0),
  state TEXT NOT NULL DEFAULT 'running' CHECK (state IN ('running','ready')),
  completed_scopes INTEGER NOT NULL DEFAULT 0 CHECK (completed_scopes >= 0),
  receipt_set_sha256 TEXT CHECK (receipt_set_sha256 IS NULL OR receipt_set_sha256 ~ '^[0-9a-f]{64}$'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at TIMESTAMPTZ,
  FOREIGN KEY (batch_record_id) REFERENCES recording_joined_batches(id) ON DELETE RESTRICT,
  UNIQUE (id, batch_record_id),
  CHECK ((state='running' AND completed_at IS NULL AND receipt_set_sha256 IS NULL)
    OR (state='ready' AND completed_at IS NOT NULL AND receipt_set_sha256 IS NOT NULL
      AND completed_scopes = expected_stream_days))
);

CREATE TABLE recording_joined_final_validation_scopes (
  run_id UUID NOT NULL REFERENCES recording_joined_final_validation_runs(id) ON DELETE RESTRICT,
  ordinal INTEGER NOT NULL CHECK (ordinal > 0),
  batch_record_id BIGINT NOT NULL,
  stream_day_id BIGINT NOT NULL REFERENCES recording_joined_stream_days(id) ON DELETE RESTRICT,
  recording_id BIGINT NOT NULL CHECK (recording_id > 0),
  local_date DATE NOT NULL,
  date_ordinal SMALLINT NOT NULL CHECK (date_ordinal BETWEEN 1 AND 14),
  source_snapshot_sha256 TEXT NOT NULL CHECK (source_snapshot_sha256 ~ '^[0-9a-f]{64}$'),
  source_clip_count INTEGER NOT NULL CHECK (source_clip_count >= 0),
  source_bytes BIGINT NOT NULL CHECK (source_bytes >= 0),
  ledger_sha256 TEXT NOT NULL CHECK (ledger_sha256 ~ '^[0-9a-f]{64}$'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, ordinal),
  UNIQUE (run_id, stream_day_id),
  FOREIGN KEY (run_id, batch_record_id) REFERENCES recording_joined_final_validation_runs(id, batch_record_id) ON DELETE RESTRICT
);
CREATE INDEX recording_joined_final_validation_scopes_day_idx
  ON recording_joined_final_validation_scopes(stream_day_id, run_id);

CREATE TABLE recording_joined_final_validation_receipts (
  run_id UUID NOT NULL REFERENCES recording_joined_final_validation_runs(id) ON DELETE RESTRICT,
  ordinal INTEGER NOT NULL CHECK (ordinal > 0),
  stream_day_id BIGINT NOT NULL REFERENCES recording_joined_stream_days(id) ON DELETE RESTRICT,
  recording_id BIGINT NOT NULL CHECK (recording_id > 0),
  local_date DATE NOT NULL,
  date_ordinal SMALLINT NOT NULL CHECK (date_ordinal BETWEEN 1 AND 14),
  source_snapshot_sha256 TEXT NOT NULL CHECK (source_snapshot_sha256 ~ '^[0-9a-f]{64}$'),
  source_clip_count INTEGER NOT NULL CHECK (source_clip_count >= 0),
  source_bytes BIGINT NOT NULL CHECK (source_bytes >= 0),
  ledger_sha256 TEXT NOT NULL CHECK (ledger_sha256 ~ '^[0-9a-f]{64}$'),
  validator_version TEXT NOT NULL CHECK (validator_version = 'stream-day-v1'),
  receipt_sha256 TEXT NOT NULL CHECK (receipt_sha256 ~ '^[0-9a-f]{64}$'),
  validated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, ordinal),
  UNIQUE (run_id, stream_day_id),
  FOREIGN KEY (run_id, ordinal) REFERENCES recording_joined_final_validation_scopes(run_id, ordinal) ON DELETE RESTRICT
);
CREATE INDEX recording_joined_final_validation_receipts_day_idx
  ON recording_joined_final_validation_receipts(stream_day_id, run_id);

CREATE FUNCTION reject_recording_joined_final_validation_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'joined final-validation evidence is append-only';
END $$;
CREATE TRIGGER recording_joined_final_validation_scope_no_update
  BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_joined_final_validation_scopes
  FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_final_validation_mutation();
CREATE TRIGGER recording_joined_final_validation_receipt_no_update
  BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_joined_final_validation_receipts
  FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_final_validation_mutation();

CREATE FUNCTION guard_recording_joined_final_validation_run_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE b recording_joined_batches%ROWTYPE;
BEGIN
  SELECT * INTO STRICT b FROM recording_joined_batches WHERE id=NEW.batch_record_id FOR KEY SHARE;
  IF b.batch_id IS DISTINCT FROM NEW.batch_id OR b.generation IS DISTINCT FROM NEW.generation
    OR b.state <> 'building' OR b.freeze_started_at IS NOT NULL OR b.frozen_at IS NOT NULL
    OR b.frozen_denominator_sha256 IS DISTINCT FROM NEW.expected_denominator_sha256
    OR b.expected_stream_days IS DISTINCT FROM NEW.expected_stream_days
    OR (SELECT count(*) FROM recording_joined_stream_days d
        WHERE d.batch_record_id=b.id AND d.state='sealed') <> b.expected_stream_days
  THEN RAISE EXCEPTION 'joined final-validation run scope is not an exact sealed batch'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_final_validation_run_insert_guard
  BEFORE INSERT ON recording_joined_final_validation_runs
  FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_final_validation_run_insert();

CREATE FUNCTION guard_recording_joined_final_validation_scope_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r recording_joined_final_validation_runs%ROWTYPE; d recording_joined_stream_days%ROWTYPE;
BEGIN
  SELECT * INTO STRICT r FROM recording_joined_final_validation_runs WHERE id=NEW.run_id FOR KEY SHARE;
  IF r.state <> 'running' THEN RAISE EXCEPTION 'joined final-validation run is not running'; END IF;
  SELECT * INTO STRICT d FROM recording_joined_stream_days
    WHERE id=NEW.stream_day_id AND batch_record_id=r.batch_record_id AND state='sealed' FOR KEY SHARE;
  NEW.batch_record_id := r.batch_record_id;
  NEW.recording_id := d.recording_id;
  NEW.local_date := d.local_date;
  NEW.date_ordinal := d.date_ordinal;
  NEW.source_snapshot_sha256 := d.source_snapshot_sha256;
  NEW.source_clip_count := d.source_clip_count;
  NEW.source_bytes := d.source_bytes;
  NEW.ledger_sha256 := d.ledger_sha256;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_final_validation_scope_insert_guard
  BEFORE INSERT ON recording_joined_final_validation_scopes
  FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_final_validation_scope_insert();

CREATE FUNCTION guard_recording_joined_final_validation_receipt_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r recording_joined_final_validation_runs%ROWTYPE; s recording_joined_final_validation_scopes%ROWTYPE;
  expected TEXT;
BEGIN
  SELECT * INTO STRICT r FROM recording_joined_final_validation_runs WHERE id=NEW.run_id FOR KEY SHARE;
  IF r.state <> 'running' THEN RAISE EXCEPTION 'joined final-validation run is not running'; END IF;
  SELECT * INTO STRICT s FROM recording_joined_final_validation_scopes
    WHERE run_id=NEW.run_id AND ordinal=NEW.ordinal FOR KEY SHARE;
  IF NEW.stream_day_id IS DISTINCT FROM s.stream_day_id THEN
    RAISE EXCEPTION 'joined final-validation receipt scope differs';
  END IF;
  NEW.recording_id := s.recording_id;
  NEW.local_date := s.local_date;
  NEW.date_ordinal := s.date_ordinal;
  NEW.source_snapshot_sha256 := s.source_snapshot_sha256;
  NEW.source_clip_count := s.source_clip_count;
  NEW.source_bytes := s.source_bytes;
  NEW.ledger_sha256 := s.ledger_sha256;
  NEW.validator_version := r.validator_version;
  expected := encode(sha256(convert_to(concat_ws(E'\n',NEW.run_id::TEXT,NEW.ordinal::TEXT,
    NEW.stream_day_id::TEXT,NEW.recording_id::TEXT,NEW.local_date::TEXT,NEW.date_ordinal::TEXT,
    NEW.source_snapshot_sha256,NEW.source_clip_count::TEXT,NEW.source_bytes::TEXT,
    NEW.ledger_sha256,NEW.validator_version),'UTF8')),'hex');
  IF NEW.receipt_sha256 IS DISTINCT FROM expected THEN
    RAISE EXCEPTION 'joined final-validation receipt digest differs';
  END IF;
  NEW.validated_at := COALESCE(NEW.validated_at, clock_timestamp());
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_final_validation_receipt_insert_guard
  BEFORE INSERT ON recording_joined_final_validation_receipts
  FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_final_validation_receipt_insert();

CREATE FUNCTION guard_recording_joined_final_validation_run_update() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE count_receipts INTEGER; digest TEXT;
BEGIN
  IF ROW(NEW.id,NEW.batch_record_id,NEW.batch_id,NEW.generation,NEW.validator_version,
      NEW.expected_denominator_sha256,NEW.expected_stream_days,NEW.created_at)
    IS DISTINCT FROM ROW(OLD.id,OLD.batch_record_id,OLD.batch_id,OLD.generation,OLD.validator_version,
      OLD.expected_denominator_sha256,OLD.expected_stream_days,OLD.created_at)
  THEN RAISE EXCEPTION 'joined final-validation run identity is immutable'; END IF;
  SELECT count(*) INTO count_receipts FROM recording_joined_final_validation_receipts WHERE run_id=OLD.id;
  IF NEW.completed_scopes IS DISTINCT FROM count_receipts THEN
    RAISE EXCEPTION 'joined final-validation progress differs';
  END IF;
  IF NEW.state='running' THEN
    IF NEW.completed_at IS NOT NULL OR NEW.receipt_set_sha256 IS NOT NULL OR NEW.completed_scopes >= NEW.expected_stream_days
    THEN RAISE EXCEPTION 'invalid joined final-validation progress'; END IF;
  ELSIF OLD.state='running' AND NEW.state='ready' THEN
    IF NEW.completed_scopes <> NEW.expected_stream_days OR NEW.completed_at IS NULL THEN
      RAISE EXCEPTION 'joined final-validation run is incomplete';
    END IF;
    SELECT encode(sha256(convert_to(COALESCE(string_agg(receipt_sha256 || E'\n','' ORDER BY ordinal),''),'UTF8')),'hex')
      INTO digest FROM recording_joined_final_validation_receipts WHERE run_id=OLD.id;
    IF NEW.receipt_set_sha256 IS DISTINCT FROM digest THEN RAISE EXCEPTION 'joined final-validation receipt set differs'; END IF;
  ELSE RAISE EXCEPTION 'invalid joined final-validation run transition'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_final_validation_run_update_guard
  BEFORE UPDATE ON recording_joined_final_validation_runs
  FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_final_validation_run_update();

-- This function is called by the final-freeze trigger. It checks exact scope,
-- day identity, receipt identity, validator version, and the canonical receipt
-- set digest; a count alone is deliberately insufficient.
CREATE FUNCTION validate_recording_joined_final_validation(p_batch_record_id BIGINT) RETURNS BOOLEAN LANGUAGE plpgsql AS $$
DECLARE r recording_joined_final_validation_runs%ROWTYPE; n INTEGER; digest TEXT;
BEGIN
  SELECT run_row.* INTO r FROM recording_joined_final_validation_runs run_row
    JOIN recording_joined_batches b ON b.id=run_row.batch_record_id
    WHERE run_row.batch_record_id=p_batch_record_id AND run_row.state='ready' AND run_row.validator_version='stream-day-v1'
      AND run_row.batch_id=b.batch_id AND run_row.generation=b.generation
      AND run_row.expected_denominator_sha256=b.frozen_denominator_sha256
      AND run_row.expected_stream_days=b.expected_stream_days;
  IF NOT FOUND THEN RETURN FALSE; END IF;
  SELECT count(*) INTO n FROM recording_joined_final_validation_scopes WHERE run_id=r.id;
  IF n <> r.expected_stream_days THEN RETURN FALSE; END IF;
  SELECT count(*) INTO n FROM recording_joined_final_validation_receipts WHERE run_id=r.id;
  IF n <> r.expected_stream_days THEN RETURN FALSE; END IF;
  IF EXISTS(
    WITH expected AS (
      SELECT d.id AS stream_day_id,
        row_number() OVER (ORDER BY br.priority_ordinal,d.date_ordinal)::INTEGER AS ordinal,
        d.recording_id,d.local_date,d.date_ordinal,d.source_snapshot_sha256,d.source_clip_count,d.source_bytes,d.ledger_sha256
      FROM recording_joined_stream_days d
      JOIN recording_joined_batch_recordings br ON br.id=d.batch_recording_id
      WHERE d.batch_record_id=p_batch_record_id AND d.state='sealed'
    )
    SELECT 1 FROM expected e LEFT JOIN recording_joined_final_validation_scopes s
      ON s.run_id=r.id AND s.ordinal=e.ordinal
    WHERE s.stream_day_id IS NULL OR ROW(s.stream_day_id,s.recording_id,s.local_date,s.date_ordinal,
      s.source_snapshot_sha256,s.source_clip_count,s.source_bytes,s.ledger_sha256)
      IS DISTINCT FROM ROW(e.stream_day_id,e.recording_id,e.local_date,e.date_ordinal,e.source_snapshot_sha256,
      e.source_clip_count,e.source_bytes,e.ledger_sha256)
  ) THEN RETURN FALSE; END IF;
  IF EXISTS(
    SELECT 1 FROM recording_joined_final_validation_scopes s
    LEFT JOIN recording_joined_stream_days d ON d.id=s.stream_day_id AND d.batch_record_id=p_batch_record_id AND d.state='sealed'
    WHERE s.run_id=r.id AND (d.id IS NULL OR ROW(s.recording_id,s.local_date,s.date_ordinal,s.source_snapshot_sha256,
      s.source_clip_count,s.source_bytes,s.ledger_sha256)
      IS DISTINCT FROM ROW(d.recording_id,d.local_date,d.date_ordinal,d.source_snapshot_sha256,d.source_clip_count,
      d.source_bytes,d.ledger_sha256))
  ) THEN RETURN FALSE; END IF;
  IF EXISTS(
    SELECT 1 FROM recording_joined_final_validation_receipts x
    JOIN recording_joined_final_validation_scopes s ON s.run_id=x.run_id AND s.ordinal=x.ordinal
    WHERE x.run_id=r.id AND (x.stream_day_id IS DISTINCT FROM s.stream_day_id
      OR ROW(x.recording_id,x.local_date,x.date_ordinal,x.source_snapshot_sha256,x.source_clip_count,x.source_bytes,x.ledger_sha256)
        IS DISTINCT FROM ROW(s.recording_id,s.local_date,s.date_ordinal,s.source_snapshot_sha256,s.source_clip_count,s.source_bytes,s.ledger_sha256)
      OR x.validator_version IS DISTINCT FROM r.validator_version
      OR x.receipt_sha256 IS DISTINCT FROM encode(sha256(convert_to(concat_ws(E'\n',x.run_id::TEXT,x.ordinal::TEXT,
        x.stream_day_id::TEXT,x.recording_id::TEXT,x.local_date::TEXT,x.date_ordinal::TEXT,x.source_snapshot_sha256,
        x.source_clip_count::TEXT,x.source_bytes::TEXT,x.ledger_sha256,x.validator_version),'UTF8')),'hex')
  )) THEN RETURN FALSE; END IF;
  SELECT encode(sha256(convert_to(COALESCE(string_agg(receipt_sha256 || E'\n','' ORDER BY ordinal),''),'UTF8')),'hex')
    INTO digest FROM recording_joined_final_validation_receipts WHERE run_id=r.id;
  RETURN digest IS NOT DISTINCT FROM r.receipt_set_sha256;
END $$;

-- Re-declare the existing guarded transition, retaining all global checks but
-- replacing only the expensive 462-day validator loop with the certificate
-- check above. Promotion remains one atomic building -> frozen update.
CREATE OR REPLACE FUNCTION guard_recording_joined_batch_update() RETURNS trigger LANGUAGE plpgsql AS $$
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
      OR (SELECT count(*) FROM recording_joined_stream_days d WHERE d.batch_record_id=OLD.id AND d.state='sealed')<>OLD.expected_stream_days
      OR (SELECT count(*) FROM recording_joined_source_snapshots s WHERE s.batch_record_id=OLD.id)<>OLD.expected_source_clips
      OR (SELECT COALESCE(sum(size_bytes),0) FROM recording_joined_source_snapshots s WHERE s.batch_record_id=OLD.id)<>OLD.expected_source_bytes
      OR (SELECT count(*) FROM recording_joined_hours h WHERE h.batch_record_id=OLD.id)<>OLD.expected_scheduled_hours
      OR (SELECT count(*) FROM recording_joined_sources s WHERE s.batch_record_id=OLD.id)<>OLD.expected_source_clips
      OR (SELECT COALESCE(sum(size_bytes),0) FROM recording_joined_sources s WHERE s.batch_record_id=OLD.id)<>OLD.expected_source_bytes
      OR EXISTS(SELECT 1 FROM recording_joined_batch_recordings br
        LEFT JOIN LATERAL (SELECT count(*) AS days FROM recording_joined_stream_days d WHERE d.batch_recording_id=br.id) actual ON TRUE
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
      OR (SELECT count(*) FROM recording_joined_artifacts a WHERE a.batch_record_id=OLD.id AND a.artifact_kind='allocation_ledger')<>OLD.expected_stream_days
      OR EXISTS(SELECT 1 FROM recording_joined_artifacts a WHERE a.batch_record_id=OLD.id AND a.artifact_kind<>'allocation_ledger')
      OR NOT validate_recording_joined_final_validation(OLD.id)
    THEN RAISE EXCEPTION 'joined batch freeze is incomplete'; END IF;
  ELSIF OLD.state = 'frozen' AND NEW.state = 'index_sealed' THEN
    IF OLD.frozen_at IS NULL OR NEW.frozen_at IS DISTINCT FROM OLD.frozen_at
      OR NEW.index_artifact_id IS NULL OR NEW.index_sealed_at IS NULL
      OR NOT EXISTS(SELECT 1 FROM recording_joined_artifacts a WHERE a.id=NEW.index_artifact_id
        AND a.batch_record_id=OLD.id AND a.artifact_kind='batch_index')
      OR (SELECT count(*) FROM recording_joined_batch_index_refs r
        WHERE r.batch_record_id=OLD.id AND r.index_artifact_id=NEW.index_artifact_id
          AND r.reference_kind='allocation_ledger')<>OLD.expected_stream_days
      OR (SELECT count(*) FROM recording_joined_batch_index_refs r
        WHERE r.batch_record_id=OLD.id AND r.index_artifact_id=NEW.index_artifact_id
          AND r.reference_kind='hour_manifest')<>OLD.expected_scheduled_hours
    THEN RAISE EXCEPTION 'joined batch index reference set is incomplete'; END IF;
  ELSIF OLD.state = 'index_sealed' AND NEW.state = 'published' THEN
    IF OLD.frozen_at IS NULL OR NEW.frozen_at IS DISTINCT FROM OLD.frozen_at
      OR NEW.index_artifact_id IS DISTINCT FROM OLD.index_artifact_id
      OR NEW.index_sealed_at IS DISTINCT FROM OLD.index_sealed_at OR NEW.published_at IS NULL
      OR NOT EXISTS(SELECT 1 FROM recording_joined_artifacts a WHERE a.id=OLD.index_artifact_id AND a.publication_state='published')
    THEN RAISE EXCEPTION 'invalid joined batch publication'; END IF;
  ELSIF OLD.state IN ('building','frozen','index_sealed') AND NEW.state='terminal_failed' THEN
    IF NEW.failure_reason_code='' OR OLD.failure_reason_code<>''
      OR ROW(NEW.index_artifact_id,NEW.freeze_started_at,NEW.frozen_at,NEW.index_sealed_at,NEW.published_at)
        IS DISTINCT FROM ROW(OLD.index_artifact_id,OLD.freeze_started_at,OLD.frozen_at,OLD.index_sealed_at,OLD.published_at)
    THEN RAISE EXCEPTION 'joined terminal batch failure lacks reason or rewrites evidence'; END IF;
  ELSE RAISE EXCEPTION 'invalid joined batch transition'; END IF;
  RETURN NEW;
END $$;

COMMIT;
