-- Durable, operator-only construction of the Tier-1 denominator. These rows
-- are deliberately separate from recording_joined_batches, so an incomplete
-- plan is invisible to workers and NAS clients.

CREATE TABLE recording_joined_dry_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  connection_id BIGINT NOT NULL REFERENCES connections(id) ON DELETE RESTRICT,
  batch_id TEXT NOT NULL,
  generation INTEGER NOT NULL CHECK (generation > 0),
  qualification_run_id BIGINT NOT NULL REFERENCES recording_qualification_runs(id) ON DELETE RESTRICT,
  input_bytes BYTEA NOT NULL,
  input_sha256 TEXT NOT NULL CHECK (input_sha256 ~ '^[0-9a-f]{64}$'),
  skeleton_bytes BYTEA NOT NULL,
  skeleton_sha256 TEXT NOT NULL CHECK (skeleton_sha256 ~ '^[0-9a-f]{64}$'),
  state TEXT NOT NULL DEFAULT 'building' CHECK (state IN ('building','ready','invalidated')),
  completed_recordings SMALLINT NOT NULL DEFAULT 0 CHECK (completed_recordings BETWEEN 0 AND 33),
  final_plan_bytes BYTEA,
  final_plan_sha256 TEXT CHECK (final_plan_sha256 ~ '^[0-9a-f]{64}$'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ready_at TIMESTAMPTZ,
  invalidated_at TIMESTAMPTZ,
  UNIQUE (batch_id,generation),
  CHECK ((state='building' AND final_plan_bytes IS NULL AND final_plan_sha256 IS NULL
      AND ready_at IS NULL AND invalidated_at IS NULL)
    OR (state='ready' AND completed_recordings=33 AND final_plan_bytes IS NOT NULL
      AND final_plan_sha256 IS NOT NULL AND ready_at IS NOT NULL AND invalidated_at IS NULL)
    OR (state='invalidated' AND final_plan_bytes IS NULL AND final_plan_sha256 IS NULL
      AND ready_at IS NULL AND invalidated_at IS NOT NULL)),
  CHECK (encode(sha256(input_bytes),'hex')=input_sha256),
  CHECK (encode(sha256(skeleton_bytes),'hex')=skeleton_sha256),
  CHECK (final_plan_bytes IS NULL OR encode(sha256(final_plan_bytes),'hex')=final_plan_sha256)
);

CREATE TABLE recording_joined_dry_run_scopes (
  dry_run_id UUID NOT NULL REFERENCES recording_joined_dry_runs(id) ON DELETE RESTRICT,
  recording_id BIGINT NOT NULL,
  priority_ordinal SMALLINT NOT NULL CHECK (priority_ordinal BETWEEN 1 AND 33),
  local_date DATE NOT NULL,
  date_ordinal SMALLINT NOT NULL CHECK (date_ordinal BETWEEN 1 AND 14),
  recording_job_id BIGINT NOT NULL REFERENCES recording_jobs(id) ON DELETE RESTRICT,
  high_water_clip_id BIGINT NOT NULL CHECK (high_water_clip_id >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (dry_run_id,priority_ordinal,date_ordinal),
  UNIQUE (dry_run_id,recording_job_id)
);
CREATE INDEX recording_joined_dry_run_scopes_retention_idx
  ON recording_joined_dry_run_scopes(recording_id,recording_job_id,high_water_clip_id);

CREATE FUNCTION guard_recording_joined_dry_run_scope_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expected_recording JSONB; expected_day JSONB; run_state TEXT;
BEGIN
  SELECT r.state,convert_from(r.skeleton_bytes,'UTF8')::jsonb->'recordings'->(NEW.priority_ordinal-1)
    INTO STRICT run_state,expected_recording FROM recording_joined_dry_runs r WHERE r.id=NEW.dry_run_id FOR SHARE;
  expected_day:=expected_recording->'snapshot_days'->(NEW.date_ordinal-1);
  IF run_state<>'building'
    OR (expected_recording->'frozen_recording'->>'recording_id')::BIGINT IS DISTINCT FROM NEW.recording_id
    OR (expected_recording->'frozen_recording'->>'priority_ordinal')::INTEGER IS DISTINCT FROM NEW.priority_ordinal
    OR expected_day->>'local_date' IS DISTINCT FROM NEW.local_date::TEXT
    OR (expected_day->>'date_ordinal')::INTEGER IS DISTINCT FROM NEW.date_ordinal
    OR (expected_day->>'recording_job_id')::BIGINT IS DISTINCT FROM NEW.recording_job_id
    OR (expected_day->>'high_water_clip_id')::BIGINT IS DISTINCT FROM NEW.high_water_clip_id
  THEN RAISE EXCEPTION 'joined dry-run scope differs from start authority'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_dry_run_scope_insert_guard BEFORE INSERT ON recording_joined_dry_run_scopes
  FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_dry_run_scope_insert();

CREATE TABLE recording_joined_dry_run_recordings (
  dry_run_id UUID NOT NULL REFERENCES recording_joined_dry_runs(id) ON DELETE RESTRICT,
  priority_ordinal SMALLINT NOT NULL CHECK (priority_ordinal BETWEEN 1 AND 33),
  recording_id BIGINT NOT NULL,
  evidence_bytes BYTEA NOT NULL,
  evidence_sha256 TEXT NOT NULL CHECK (evidence_sha256 ~ '^[0-9a-f]{64}$'),
  source_clips BIGINT NOT NULL CHECK (source_clips >= 0),
  source_bytes BIGINT NOT NULL CHECK (source_bytes >= 0),
  exclusions BIGINT NOT NULL CHECK (exclusions >= 0),
  exclusions_sha256 TEXT NOT NULL CHECK (exclusions_sha256 ~ '^[0-9a-f]{64}$'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (dry_run_id,priority_ordinal),
  UNIQUE (dry_run_id,recording_id),
  CHECK (encode(sha256(evidence_bytes),'hex')=evidence_sha256),
  CHECK ((convert_from(evidence_bytes,'UTF8')::jsonb->'recording'->'frozen_recording'->>'recording_id')::BIGINT=recording_id),
  CHECK ((convert_from(evidence_bytes,'UTF8')::jsonb->'recording'->'frozen_recording'->>'priority_ordinal')::INTEGER=priority_ordinal),
  CHECK ((convert_from(evidence_bytes,'UTF8')::jsonb->'recording'->>'expected_source_clips')::BIGINT=source_clips),
  CHECK ((convert_from(evidence_bytes,'UTF8')::jsonb->'recording'->>'expected_source_bytes')::BIGINT=source_bytes),
  CHECK ((convert_from(evidence_bytes,'UTF8')::jsonb->'recording'->>'expected_exclusions')::BIGINT=exclusions),
  CHECK (convert_from(evidence_bytes,'UTF8')::jsonb->'recording'->>'expected_exclusions_sha256'=exclusions_sha256)
);

CREATE FUNCTION reject_recording_joined_dry_run_evidence_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'joined dry-run evidence is immutable'; END $$;
CREATE TRIGGER recording_joined_dry_run_scopes_immutable BEFORE UPDATE OR DELETE ON recording_joined_dry_run_scopes
  FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_dry_run_evidence_mutation();
CREATE TRIGGER recording_joined_dry_run_recordings_immutable BEFORE UPDATE OR DELETE ON recording_joined_dry_run_recordings
  FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_dry_run_evidence_mutation();
CREATE TRIGGER recording_joined_dry_run_scopes_no_truncate BEFORE TRUNCATE ON recording_joined_dry_run_scopes
  FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_dry_run_evidence_mutation();
CREATE TRIGGER recording_joined_dry_run_recordings_no_truncate BEFORE TRUNCATE ON recording_joined_dry_run_recordings
  FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_dry_run_evidence_mutation();
CREATE TRIGGER recording_joined_dry_runs_no_delete BEFORE DELETE ON recording_joined_dry_runs
  FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_dry_run_evidence_mutation();
CREATE TRIGGER recording_joined_dry_runs_no_truncate BEFORE TRUNCATE ON recording_joined_dry_runs
  FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_dry_run_evidence_mutation();

CREATE FUNCTION guard_recording_joined_dry_run_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF ROW(NEW.id,NEW.account_id,NEW.connection_id,NEW.batch_id,NEW.generation,NEW.qualification_run_id,
      NEW.input_bytes,NEW.input_sha256,NEW.skeleton_bytes,NEW.skeleton_sha256,NEW.created_at)
    IS DISTINCT FROM ROW(OLD.id,OLD.account_id,OLD.connection_id,OLD.batch_id,OLD.generation,OLD.qualification_run_id,
      OLD.input_bytes,OLD.input_sha256,OLD.skeleton_bytes,OLD.skeleton_sha256,OLD.created_at)
    OR (OLD.state IN ('ready','invalidated') AND ROW(NEW.state,NEW.completed_recordings,NEW.final_plan_bytes,NEW.final_plan_sha256,NEW.ready_at,NEW.invalidated_at)
      IS DISTINCT FROM ROW(OLD.state,OLD.completed_recordings,OLD.final_plan_bytes,OLD.final_plan_sha256,OLD.ready_at,OLD.invalidated_at))
    OR (OLD.state='building' AND (
      (NEW.state='building' AND (NEW.completed_recordings NOT IN (OLD.completed_recordings,OLD.completed_recordings+1)
        OR NEW.invalidated_at IS NOT NULL))
      OR (NEW.state='ready' AND (ROW(OLD.completed_recordings,NEW.completed_recordings) IS DISTINCT FROM ROW(32::SMALLINT,33::SMALLINT)
        OR NEW.invalidated_at IS NOT NULL))
      OR (NEW.state='ready' AND ((SELECT count(*) FROM recording_joined_dry_run_scopes s WHERE s.dry_run_id=OLD.id)<>462
        OR (SELECT count(*) FROM recording_joined_dry_run_recordings r WHERE r.dry_run_id=OLD.id)<>33
        OR EXISTS(SELECT 1 FROM recording_joined_dry_run_scopes s
          CROSS JOIN LATERAL (SELECT convert_from(NEW.final_plan_bytes,'UTF8')::jsonb->'recordings'->(s.priority_ordinal-1)
            ->'snapshot_days'->(s.date_ordinal-1) expected_day) expected
          WHERE s.dry_run_id=OLD.id AND ROW((expected.expected_day->>'recording_job_id')::BIGINT,
            (expected.expected_day->>'high_water_clip_id')::BIGINT,expected.expected_day->>'local_date',
            (expected.expected_day->>'date_ordinal')::INTEGER)
            IS DISTINCT FROM ROW(s.recording_job_id,s.high_water_clip_id,s.local_date::TEXT,s.date_ordinal))
        OR EXISTS(SELECT 1 FROM recording_joined_dry_run_recordings r
          CROSS JOIN LATERAL (SELECT convert_from(NEW.final_plan_bytes,'UTF8')::jsonb->'recordings'->(r.priority_ordinal-1) item) expected
            WHERE r.dry_run_id=OLD.id AND expected.item IS DISTINCT FROM convert_from(r.evidence_bytes,'UTF8')::jsonb->'recording')
        OR EXISTS(SELECT 1
          FROM jsonb_array_elements(convert_from(NEW.final_plan_bytes,'UTF8')::jsonb->'recordings') WITH ORDINALITY final(item,ord)
          WHERE (final.item->'frozen_recording') IS DISTINCT FROM
              (convert_from(OLD.skeleton_bytes,'UTF8')::jsonb->'recordings'->((final.ord-1)::INTEGER)->'frozen_recording')
            OR (final.item->'qualification') IS DISTINCT FROM
              (convert_from(OLD.skeleton_bytes,'UTF8')::jsonb->'recordings'->((final.ord-1)::INTEGER)->'qualification'))
        OR (convert_from(NEW.final_plan_bytes,'UTF8')::jsonb
          - ARRAY['recordings','provisional_source_clips','provisional_source_bytes','provisional_exclusions',
            'frozen_denominator_sha256','freeze_exclusions_sha256','request_sha256']::TEXT[])
          IS DISTINCT FROM (convert_from(OLD.skeleton_bytes,'UTF8')::jsonb
          - ARRAY['recordings','provisional_source_clips','provisional_source_bytes','provisional_exclusions',
            'frozen_denominator_sha256','freeze_exclusions_sha256','request_sha256']::TEXT[])))
      OR (NEW.state='invalidated' AND (NEW.completed_recordings<>OLD.completed_recordings OR NEW.invalidated_at IS NULL))
      OR NEW.state NOT IN ('building','ready','invalidated')))
  THEN RAISE EXCEPTION 'joined dry-run authority is immutable'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_dry_run_update_guard BEFORE UPDATE ON recording_joined_dry_runs
  FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_dry_run_update();

CREATE FUNCTION guard_recording_joined_dry_run_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE request JSONB; skeleton JSONB;
BEGIN
  request:=convert_from(NEW.input_bytes,'UTF8')::jsonb;
  skeleton:=convert_from(NEW.skeleton_bytes,'UTF8')::jsonb;
  IF NEW.state<>'building' OR NEW.completed_recordings<>0 OR NEW.final_plan_bytes IS NOT NULL
	OR NEW.final_plan_sha256 IS NOT NULL OR NEW.ready_at IS NOT NULL OR NEW.invalidated_at IS NOT NULL
    OR request->>'batch_id' IS DISTINCT FROM NEW.batch_id
    OR (request->>'generation')::INTEGER IS DISTINCT FROM NEW.generation
    OR (request->>'connection_id')::BIGINT IS DISTINCT FROM NEW.connection_id
    OR (request->>'qualification_run_id')::BIGINT IS DISTINCT FROM NEW.qualification_run_id
    OR (request->>'apply')::BOOLEAN IS DISTINCT FROM FALSE
    OR skeleton->>'batch_id' IS DISTINCT FROM NEW.batch_id
    OR (skeleton->>'generation')::INTEGER IS DISTINCT FROM NEW.generation
    OR (skeleton->>'connection_id')::BIGINT IS DISTINCT FROM NEW.connection_id
    OR (skeleton->'selection_authority'->>'qualification_run_id')::BIGINT IS DISTINCT FROM NEW.qualification_run_id
  THEN RAISE EXCEPTION 'joined dry-run must start building'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_dry_run_insert_guard BEFORE INSERT ON recording_joined_dry_runs
  FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_dry_run_insert();

-- A dry-run scope is retention authority as soon as start commits. released_at
-- remains mutable; all denominator fields and purge/delete stay fenced.
CREATE OR REPLACE FUNCTION guard_recording_joined_clip_retention() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE target_clip_id BIGINT; clip_recording BIGINT; clip_job BIGINT; old_scoped BOOLEAN; new_scoped BOOLEAN := FALSE;
BEGIN
  IF current_setting('transaction_isolation') <> 'read committed'
  THEN RAISE EXCEPTION 'joined source purge requires read committed'; END IF;
  PERFORM pg_advisory_xact_lock_shared(137,1);
  target_clip_id:=OLD.id; clip_recording:=OLD.recording_id; clip_job:=OLD.recording_job_id;
  SELECT EXISTS(
    SELECT 1 FROM recording_joined_snapshot_scopes scope
      JOIN recording_joined_batches batch ON batch.id=scope.batch_record_id AND batch.state='snapshotting'
      WHERE scope.recording_id=clip_recording AND scope.recording_job_id=clip_job AND target_clip_id<=scope.high_water_clip_id
    UNION ALL
    SELECT 1 FROM recording_joined_dry_run_scopes scope JOIN recording_joined_dry_runs run ON run.id=scope.dry_run_id
      WHERE scope.recording_id=clip_recording AND scope.recording_job_id=clip_job AND target_clip_id<=scope.high_water_clip_id
  ) INTO old_scoped;
  IF TG_OP='UPDATE' THEN
    SELECT EXISTS(
      SELECT 1 FROM recording_joined_snapshot_scopes scope
        JOIN recording_joined_batches batch ON batch.id=scope.batch_record_id AND batch.state='snapshotting'
        WHERE scope.recording_id=NEW.recording_id AND scope.recording_job_id=NEW.recording_job_id AND NEW.id<=scope.high_water_clip_id
      UNION ALL
      SELECT 1 FROM recording_joined_dry_run_scopes scope JOIN recording_joined_dry_runs run ON run.id=scope.dry_run_id
        WHERE scope.recording_id=NEW.recording_id AND scope.recording_job_id=NEW.recording_job_id AND NEW.id<=scope.high_water_clip_id
    ) INTO new_scoped;
  END IF;
  IF (TG_OP='DELETE' OR NEW.id IS DISTINCT FROM OLD.id OR (OLD.purged_at IS NULL AND NEW.purged_at IS NOT NULL))
    AND (EXISTS(SELECT 1 FROM recording_joined_source_snapshots s WHERE s.clip_id=target_clip_id) OR old_scoped)
  THEN RAISE EXCEPTION 'joined frozen source is retention protected'; END IF;
  IF TG_OP='UPDATE' AND (old_scoped OR new_scoped) AND ROW(NEW.recording_id,NEW.recording_job_id,NEW.storage_destination_id,
      NEW.endpoint,NEW.bucket,NEW.object_key,NEW.etag,NEW.size_bytes,NEW.sha256,NEW.clip_start_at,NEW.clip_end_at,NEW.created_at)
    IS DISTINCT FROM ROW(OLD.recording_id,OLD.recording_job_id,OLD.storage_destination_id,
      OLD.endpoint,OLD.bucket,OLD.object_key,OLD.etag,OLD.size_bytes,OLD.sha256,OLD.clip_start_at,OLD.clip_end_at,OLD.created_at)
  THEN RAISE EXCEPTION 'joined scoped source identity is immutable'; END IF;
  IF TG_OP='DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END $$;
