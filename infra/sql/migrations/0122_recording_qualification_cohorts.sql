-- Immutable current-frame scene identity attestation. The trigger binds an
-- accepted account member's visual/approved-automated assertion to a real,
-- successful catalog frame and its recorded media-object SHA. This is NOT a
-- fresh object-byte hash, decode result, NAS proof, or stitch certification;
-- those remain separate mandatory evidence axes.
CREATE TABLE IF NOT EXISTS recording_scene_frame_evidence (
  id                        BIGSERIAL PRIMARY KEY,
  account_id                BIGINT NOT NULL,
  stream_id                 BIGINT NOT NULL,
  frame_id                  BIGINT NOT NULL,
  media_object_id           BIGINT NOT NULL,
  captured_at               TIMESTAMPTZ NOT NULL,
  frame_sha256              TEXT NOT NULL CHECK (frame_sha256 ~ '^[0-9a-f]{64}$'),
  scene_identity_sha256     TEXT NOT NULL CHECK (scene_identity_sha256 ~ '^[0-9a-f]{64}$'),
  verification_method       TEXT NOT NULL CHECK (verification_method IN ('operator_visual','approved_automated')),
  verified_by_user_id       BIGINT NOT NULL,
  verified_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  evidence_sha256           TEXT NOT NULL CHECK (evidence_sha256 ~ '^[0-9a-f]{64}$'),
  UNIQUE (account_id, frame_id),
  UNIQUE (id, account_id, stream_id, scene_identity_sha256),
  CHECK (verified_at >= captured_at),
  CHECK (verified_at <= captured_at + interval '24 hours')
);

CREATE INDEX IF NOT EXISTS recording_scene_frame_evidence_account_verified_idx
  ON recording_scene_frame_evidence (account_id, verified_at DESC);

CREATE OR REPLACE FUNCTION validate_recording_scene_frame_evidence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  actual_stream_id BIGINT;
  actual_media_id BIGINT;
  actual_captured_at TIMESTAMPTZ;
  actual_sha TEXT;
BEGIN
  SELECT f.stream_id,f.raw_media_object_id,f.captured_at,lower(m.sha256)
    INTO actual_stream_id,actual_media_id,actual_captured_at,actual_sha
  FROM frames f
  JOIN media_objects m ON m.id=f.raw_media_object_id
  WHERE f.id=NEW.frame_id AND f.capture_status='success'
    AND m.sha256 ~ '^[0-9A-Fa-f]{64}$';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'scene evidence requires a successful SHA-verified frame';
  END IF;
  IF actual_stream_id<>NEW.stream_id OR actual_media_id<>NEW.media_object_id OR
     actual_captured_at<>NEW.captured_at OR actual_sha<>lower(NEW.frame_sha256) THEN
    RAISE EXCEPTION 'scene evidence does not match authoritative frame bytes';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM memberships
    WHERE org_id=NEW.account_id AND user_id=NEW.verified_by_user_id AND accepted_at IS NOT NULL
  ) THEN
    RAISE EXCEPTION 'scene verifier is not an accepted account member';
  END IF;
  IF NEW.verified_at>now()+interval '5 minutes' OR NEW.verified_at<now()-interval '24 hours' THEN
    RAISE EXCEPTION 'scene verification must be current';
  END IF;
  NEW.frame_sha256:=actual_sha;
  NEW.evidence_sha256:=encode(sha256(convert_to(jsonb_build_object(
    'account_id',NEW.account_id,'stream_id',NEW.stream_id,'frame_id',NEW.frame_id,
    'media_object_id',NEW.media_object_id,'captured_at_epoch',EXTRACT(EPOCH FROM NEW.captured_at),
    'frame_sha256',NEW.frame_sha256,'scene_identity_sha256',NEW.scene_identity_sha256,
    'verification_method',NEW.verification_method,'verified_by_user_id',NEW.verified_by_user_id,
    'verified_at_epoch',EXTRACT(EPOCH FROM NEW.verified_at)
  )::text,'UTF8')),'hex');
  RETURN NEW;
END;
$$;

CREATE TRIGGER recording_scene_frame_evidence_validate
BEFORE INSERT ON recording_scene_frame_evidence
FOR EACH ROW EXECUTE FUNCTION validate_recording_scene_frame_evidence();

CREATE OR REPLACE FUNCTION reject_recording_qualification_evidence_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'qualification evidence is append-only';
END;
$$;

CREATE TRIGGER recording_scene_frame_evidence_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_scene_frame_evidence
FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_qualification_evidence_mutation();

-- A run freezes the exact scene denominator and the first fourteen scheduled
-- occurrences at/after window_sequence_start_at.  The application generates
-- occurrences with the shared Go scheduler; PostgreSQL intentionally does not
-- reimplement DST ambiguity rules.
CREATE TABLE IF NOT EXISTS recording_qualification_runs (
  id                        BIGSERIAL PRIMARY KEY,
  account_id                BIGINT NOT NULL,
  definition_version        TEXT NOT NULL CHECK (length(definition_version) BETWEEN 1 AND 128),
  definition_jsonb          JSONB NOT NULL CHECK (jsonb_typeof(definition_jsonb)='object'),
  definition_sha256         TEXT CHECK (definition_sha256 ~ '^[0-9a-f]{64}$'),
  cohort_sha256             TEXT CHECK (cohort_sha256 ~ '^[0-9a-f]{64}$'),
  windows_sha256            TEXT CHECK (windows_sha256 ~ '^[0-9a-f]{64}$'),
  target_recording_count    INTEGER NOT NULL CHECK (target_recording_count>=50),
  target_window_count       INTEGER NOT NULL DEFAULT 14 CHECK (target_window_count=14),
  required_good_or_great    INTEGER NOT NULL DEFAULT 13 CHECK (required_good_or_great=13),
  max_acceptable            INTEGER NOT NULL DEFAULT 1 CHECK (max_acceptable=1),
  window_sequence_start_at  TIMESTAMPTZ NOT NULL,
  qualification_due_at      TIMESTAMPTZ,
  status                    TEXT NOT NULL DEFAULT 'building' CHECK (status IN ('building','active','canceled')),
  created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
  frozen_at                 TIMESTAMPTZ,
  canceled_at               TIMESTAMPTZ,
  CHECK (
    (status='building' AND definition_sha256 IS NULL AND cohort_sha256 IS NULL AND windows_sha256 IS NULL
      AND qualification_due_at IS NULL AND frozen_at IS NULL AND canceled_at IS NULL) OR
    (status='active' AND definition_sha256 IS NOT NULL AND cohort_sha256 IS NOT NULL AND windows_sha256 IS NOT NULL
      AND qualification_due_at IS NOT NULL AND frozen_at IS NOT NULL AND canceled_at IS NULL) OR
    (status='canceled' AND canceled_at IS NOT NULL)
  ),
  CHECK (frozen_at IS NULL OR frozen_at>=created_at),
  CHECK (canceled_at IS NULL OR canceled_at>=created_at)
);

CREATE UNIQUE INDEX IF NOT EXISTS recording_qualification_runs_id_account_idx
  ON recording_qualification_runs (id,account_id);
CREATE INDEX IF NOT EXISTS recording_qualification_runs_account_created_idx
  ON recording_qualification_runs (account_id,created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS recording_qualification_runs_one_active_idx
  ON recording_qualification_runs (account_id) WHERE status='active';

CREATE TABLE IF NOT EXISTS recording_qualification_members (
  run_id                    BIGINT NOT NULL,
  account_id                BIGINT NOT NULL,
  recording_id              BIGINT NOT NULL,
  ordinal                   INTEGER NOT NULL CHECK (ordinal>0),
  stream_id                 BIGINT NOT NULL,
  recording_name            TEXT NOT NULL,
  stream_name               TEXT NOT NULL,
  scene_identity_sha256     TEXT NOT NULL CHECK (scene_identity_sha256 ~ '^[0-9a-f]{64}$'),
  scene_frame_evidence_id   BIGINT NOT NULL,
  cron_timezone             TEXT NOT NULL,
  daily_window_start        TIME NOT NULL,
  daily_window_end          TIME NOT NULL,
  active_weekdays           SMALLINT NOT NULL CHECK (active_weekdays BETWEEN 1 AND 127),
  schedule_start_at         TIMESTAMPTZ NOT NULL,
  schedule_end_at           TIMESTAMPTZ,
  window_generator_version  TEXT NOT NULL CHECK (length(window_generator_version) BETWEEN 1 AND 128),
  schedule_config_sha256    TEXT CHECK (schedule_config_sha256 ~ '^[0-9a-f]{64}$'),
  window_sequence_sha256    TEXT CHECK (window_sequence_sha256 ~ '^[0-9a-f]{64}$'),
  PRIMARY KEY (run_id,recording_id),
  UNIQUE (run_id,ordinal),
  UNIQUE (run_id,scene_identity_sha256),
  FOREIGN KEY (run_id,account_id)
    REFERENCES recording_qualification_runs(id,account_id) ON DELETE RESTRICT,
  FOREIGN KEY (scene_frame_evidence_id,account_id,stream_id,scene_identity_sha256)
    REFERENCES recording_scene_frame_evidence(id,account_id,stream_id,scene_identity_sha256) ON DELETE RESTRICT,
  CHECK (schedule_end_at IS NULL OR schedule_end_at>schedule_start_at)
);

CREATE INDEX IF NOT EXISTS recording_qualification_members_account_recording_idx
  ON recording_qualification_members (account_id,recording_id,run_id);

CREATE TABLE IF NOT EXISTS recording_qualification_windows (
  run_id                    BIGINT NOT NULL,
  recording_id              BIGINT NOT NULL,
  ordinal                   INTEGER NOT NULL CHECK (ordinal BETWEEN 1 AND 14),
  local_open_at             TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  local_end_at              TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  open_utc_offset_seconds   INTEGER NOT NULL CHECK (open_utc_offset_seconds BETWEEN -50400 AND 50400),
  end_utc_offset_seconds    INTEGER NOT NULL CHECK (end_utc_offset_seconds BETWEEN -50400 AND 50400),
  window_start_at           TIMESTAMPTZ NOT NULL,
  window_end_at             TIMESTAMPTZ NOT NULL,
  expected_seconds          BIGINT NOT NULL CHECK (expected_seconds>0),
  PRIMARY KEY (run_id,recording_id,ordinal),
  UNIQUE (run_id,recording_id,window_start_at),
  FOREIGN KEY (run_id,recording_id)
    REFERENCES recording_qualification_members(run_id,recording_id) ON DELETE RESTRICT,
  CHECK (local_end_at>local_open_at),
  CHECK (window_end_at>window_start_at),
  CHECK (expected_seconds=EXTRACT(EPOCH FROM (window_end_at-window_start_at))::bigint)
);

CREATE INDEX IF NOT EXISTS recording_qualification_windows_end_idx
  ON recording_qualification_windows (run_id,window_end_at,recording_id);

CREATE OR REPLACE FUNCTION reject_recording_qualification_truncate()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'qualification audit tables cannot be truncated';
END;
$$;

CREATE TRIGGER recording_qualification_runs_no_truncate
BEFORE TRUNCATE ON recording_qualification_runs
FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_qualification_truncate();
CREATE TRIGGER recording_qualification_members_no_truncate
BEFORE TRUNCATE ON recording_qualification_members
FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_qualification_truncate();
CREATE TRIGGER recording_qualification_windows_no_truncate
BEFORE TRUNCATE ON recording_qualification_windows
FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_qualification_truncate();

-- Child keys are immutable, and both source and destination parents are locked
-- on UPDATE so a row cannot be moved out of an activated run.
CREATE OR REPLACE FUNCTION enforce_recording_qualification_child_mutability()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  old_status TEXT;
  new_status TEXT;
BEGIN
  IF TG_OP='INSERT' THEN
    SELECT status INTO new_status FROM recording_qualification_runs WHERE id=NEW.run_id FOR UPDATE;
    IF new_status IS DISTINCT FROM 'building' THEN RAISE EXCEPTION 'qualification run is not building'; END IF;
    RETURN NEW;
  END IF;
  SELECT status INTO old_status FROM recording_qualification_runs WHERE id=OLD.run_id FOR UPDATE;
  IF TG_OP='DELETE' THEN
    IF old_status IS DISTINCT FROM 'building' THEN RAISE EXCEPTION 'activated qualification run is immutable'; END IF;
    RETURN OLD;
  END IF;
  IF NEW.run_id<>OLD.run_id OR NEW.recording_id<>OLD.recording_id THEN
    RAISE EXCEPTION 'qualification child identity is immutable';
  END IF;
  SELECT status INTO new_status FROM recording_qualification_runs WHERE id=NEW.run_id FOR UPDATE;
  IF old_status IS DISTINCT FROM 'building' OR new_status IS DISTINCT FROM 'building' THEN
    RAISE EXCEPTION 'activated qualification run is immutable';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER recording_qualification_members_mutability
BEFORE INSERT OR UPDATE OR DELETE ON recording_qualification_members
FOR EACH ROW EXECUTE FUNCTION enforce_recording_qualification_child_mutability();
CREATE TRIGGER recording_qualification_windows_mutability
BEFORE INSERT OR UPDATE OR DELETE ON recording_qualification_windows
FOR EACH ROW EXECUTE FUNCTION enforce_recording_qualification_child_mutability();

CREATE OR REPLACE FUNCTION enforce_recording_qualification_run_lifecycle()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  member_count INTEGER;
  window_count INTEGER;
  invalid_count INTEGER;
  member_json JSONB;
  window_json JSONB;
  definition_sha TEXT;
  window_sha TEXT;
  authoritative_count INTEGER:=0;
  authoritative RECORD;
BEGIN
  IF TG_OP='INSERT' THEN
    IF NEW.status<>'building' THEN RAISE EXCEPTION 'qualification run must start building'; END IF;
    RETURN NEW;
  END IF;
  IF TG_OP='DELETE' THEN
    IF OLD.status<>'building' THEN RAISE EXCEPTION 'activated qualification run cannot be deleted'; END IF;
    RETURN OLD;
  END IF;
  IF OLD.status='building' AND NEW.status='active' THEN
    IF NEW.account_id IS DISTINCT FROM OLD.account_id OR NEW.definition_version IS DISTINCT FROM OLD.definition_version OR
       NEW.definition_jsonb IS DISTINCT FROM OLD.definition_jsonb OR NEW.target_recording_count IS DISTINCT FROM OLD.target_recording_count OR
       NEW.target_window_count IS DISTINCT FROM OLD.target_window_count OR NEW.required_good_or_great IS DISTINCT FROM OLD.required_good_or_great OR
       NEW.max_acceptable IS DISTINCT FROM OLD.max_acceptable OR NEW.window_sequence_start_at IS DISTINCT FROM OLD.window_sequence_start_at OR
       NEW.created_at IS DISTINCT FROM OLD.created_at THEN
      RAISE EXCEPTION 'activation cannot rewrite qualification definition';
    END IF;
    SELECT count(*)::int INTO member_count FROM recording_qualification_members WHERE run_id=OLD.id;
    SELECT count(*)::int INTO window_count FROM recording_qualification_windows WHERE run_id=OLD.id;
    IF member_count<>OLD.target_recording_count OR member_count<50 OR window_count<>member_count*14 THEN
      RAISE EXCEPTION 'qualification cohort is incomplete';
    END IF;
    -- Validate and lock the authoritative recording rows in deterministic ID
    -- order. SELECT FOR SHARE follows a concurrently updated row to its current
    -- committed version before returning, so an overlapping recording PATCH is
    -- either observed here or waits until this activation has frozen.
    FOR authoritative IN
      SELECT m.recording_id,m.account_id,m.stream_id,m.recording_name,m.cron_timezone,
             m.daily_window_start,m.daily_window_end,m.active_weekdays,m.schedule_start_at,m.schedule_end_at,
             r.id AS authoritative_id,r.account_id AS authoritative_account_id,
             r.stream_id AS authoritative_stream_id,r.name AS authoritative_name,
             r.mode AS authoritative_mode,r.status AS authoritative_status,
             r.cron_timezone AS authoritative_timezone,r.daily_window_start AS authoritative_window_start,
             r.daily_window_end AS authoritative_window_end,r.active_weekdays AS authoritative_weekdays,
             r.start_at AS authoritative_start_at,r.end_at AS authoritative_end_at
      FROM recording_qualification_members m
      JOIN recordings r ON r.id=m.recording_id
      WHERE m.run_id=OLD.id
      ORDER BY r.id
      FOR SHARE OF r
    LOOP
      authoritative_count:=authoritative_count+1;
      IF authoritative.authoritative_account_id<>authoritative.account_id OR
         authoritative.authoritative_stream_id IS DISTINCT FROM authoritative.stream_id OR
         authoritative.authoritative_name<>authoritative.recording_name OR
         authoritative.authoritative_mode<>'continuous' OR authoritative.authoritative_status<>'active' OR
         authoritative.authoritative_timezone<>authoritative.cron_timezone OR
         authoritative.authoritative_window_start IS DISTINCT FROM authoritative.daily_window_start OR
         authoritative.authoritative_window_end IS DISTINCT FROM authoritative.daily_window_end OR
         authoritative.authoritative_weekdays<>authoritative.active_weekdays OR
         authoritative.authoritative_start_at<>authoritative.schedule_start_at OR
         authoritative.authoritative_end_at IS DISTINCT FROM authoritative.schedule_end_at THEN
        RAISE EXCEPTION 'qualification member does not match authoritative recording %', authoritative.recording_id;
      END IF;
    END LOOP;
    IF authoritative_count<>member_count THEN
      RAISE EXCEPTION 'qualification cohort references missing authoritative recordings';
    END IF;
    SELECT count(*)::int INTO invalid_count
    FROM recording_qualification_members m
    JOIN recording_scene_frame_evidence e ON e.id=m.scene_frame_evidence_id
    LEFT JOIN LATERAL (
      SELECT count(*) n,min(ordinal) lo,max(ordinal) hi,
             bool_and(window_start_at>=OLD.window_sequence_start_at) starts_after_cutoff,
             bool_and(local_open_at=window_start_at AT TIME ZONE m.cron_timezone) opens_match,
             bool_and(local_end_at=window_end_at AT TIME ZONE m.cron_timezone) ends_match,
             bool_and(open_utc_offset_seconds=EXTRACT(EPOCH FROM (local_open_at-(window_start_at AT TIME ZONE 'UTC')))::int) open_offsets_match,
             bool_and(end_utc_offset_seconds=EXTRACT(EPOCH FROM (local_end_at-(window_end_at AT TIME ZONE 'UTC')))::int) end_offsets_match,
             bool_and(window_start_at>=m.schedule_start_at AND (m.schedule_end_at IS NULL OR window_end_at<=m.schedule_end_at)) envelope_match
      FROM recording_qualification_windows w WHERE w.run_id=m.run_id AND w.recording_id=m.recording_id
    ) c ON true
    WHERE m.run_id=OLD.id AND (
      e.account_id<>m.account_id OR e.stream_id<>m.stream_id OR e.scene_identity_sha256<>m.scene_identity_sha256 OR
      m.window_generator_version<>'recsched-next-full-v1' OR
      e.verified_at<now()-interval '24 hours' OR e.verified_at>now()+interval '5 minutes' OR
      c.n<>14 OR c.lo<>1 OR c.hi<>14 OR NOT c.starts_after_cutoff OR NOT c.opens_match OR NOT c.ends_match OR
      NOT c.open_offsets_match OR NOT c.end_offsets_match OR NOT c.envelope_match
    );
    IF invalid_count<>0 THEN RAISE EXCEPTION 'qualification evidence or window set is invalid'; END IF;

    UPDATE recording_qualification_members m SET
      schedule_config_sha256=encode(sha256(convert_to(jsonb_build_object(
        'cron_timezone',m.cron_timezone,'daily_window_start',m.daily_window_start,
        'daily_window_end',m.daily_window_end,'active_weekdays',m.active_weekdays,
        'schedule_start_epoch',EXTRACT(EPOCH FROM m.schedule_start_at),
        'schedule_end_epoch',EXTRACT(EPOCH FROM m.schedule_end_at)
      )::text,'UTF8')),'hex'),
      window_sequence_sha256=encode(sha256(convert_to((
        SELECT jsonb_agg(jsonb_build_object(
          'ordinal',w.ordinal,'local_open_at',w.local_open_at,'local_end_at',w.local_end_at,
          'open_offset',w.open_utc_offset_seconds,'end_offset',w.end_utc_offset_seconds,
          'window_start_epoch',EXTRACT(EPOCH FROM w.window_start_at),
          'window_end_epoch',EXTRACT(EPOCH FROM w.window_end_at),'expected_seconds',w.expected_seconds
        ) ORDER BY w.ordinal)::text
        FROM recording_qualification_windows w
        WHERE w.run_id=m.run_id AND w.recording_id=m.recording_id
      ),'UTF8')),'hex')
    WHERE m.run_id=OLD.id;

    SELECT jsonb_agg(jsonb_build_object(
      'ordinal',m.ordinal,'recording_id',m.recording_id,'stream_id',m.stream_id,
      'recording_name',m.recording_name,'stream_name',m.stream_name,
      'scene_identity_sha256',m.scene_identity_sha256,'scene_evidence_id',m.scene_frame_evidence_id,
      'cron_timezone',m.cron_timezone,'daily_window_start',m.daily_window_start,
      'daily_window_end',m.daily_window_end,'active_weekdays',m.active_weekdays,
      'schedule_start_epoch',EXTRACT(EPOCH FROM m.schedule_start_at),
      'schedule_end_epoch',EXTRACT(EPOCH FROM m.schedule_end_at),
      'window_generator_version',m.window_generator_version,
      'schedule_config_sha256',m.schedule_config_sha256,'window_sequence_sha256',m.window_sequence_sha256
    ) ORDER BY m.ordinal) INTO member_json
    FROM recording_qualification_members m WHERE m.run_id=OLD.id;
    SELECT jsonb_agg(jsonb_build_object(
      'recording_id',w.recording_id,'ordinal',w.ordinal,'local_open_at',w.local_open_at,
      'local_end_at',w.local_end_at,'open_offset',w.open_utc_offset_seconds,'end_offset',w.end_utc_offset_seconds,
      'window_start_epoch',EXTRACT(EPOCH FROM w.window_start_at),'window_end_epoch',EXTRACT(EPOCH FROM w.window_end_at),
      'expected_seconds',w.expected_seconds
    ) ORDER BY m.ordinal,w.ordinal) INTO window_json
    FROM recording_qualification_windows w
    JOIN recording_qualification_members m ON m.run_id=w.run_id AND m.recording_id=w.recording_id
    WHERE w.run_id=OLD.id;
    definition_sha:=encode(sha256(convert_to(OLD.definition_jsonb::text,'UTF8')),'hex');
    window_sha:=encode(sha256(convert_to(window_json::text,'UTF8')),'hex');
    NEW.definition_sha256:=definition_sha;
    NEW.windows_sha256:=window_sha;
    NEW.cohort_sha256:=encode(sha256(convert_to(jsonb_build_object(
      'account_id',OLD.account_id,'definition_version',OLD.definition_version,
      'definition_sha256',definition_sha,'members',member_json,'windows_sha256',window_sha
    )::text,'UTF8')),'hex');
    SELECT max(window_end_at) INTO NEW.qualification_due_at FROM recording_qualification_windows WHERE run_id=OLD.id;
    NEW.frozen_at:=now();
    NEW.canceled_at:=NULL;
    RETURN NEW;
  END IF;
  IF OLD.status IN ('building','active') AND NEW.status='canceled' THEN
    IF NEW.account_id IS DISTINCT FROM OLD.account_id OR NEW.definition_version IS DISTINCT FROM OLD.definition_version OR
       NEW.definition_jsonb IS DISTINCT FROM OLD.definition_jsonb OR NEW.definition_sha256 IS DISTINCT FROM OLD.definition_sha256 OR
       NEW.cohort_sha256 IS DISTINCT FROM OLD.cohort_sha256 OR NEW.windows_sha256 IS DISTINCT FROM OLD.windows_sha256 OR
       NEW.target_recording_count IS DISTINCT FROM OLD.target_recording_count OR NEW.target_window_count IS DISTINCT FROM OLD.target_window_count OR
       NEW.required_good_or_great IS DISTINCT FROM OLD.required_good_or_great OR NEW.max_acceptable IS DISTINCT FROM OLD.max_acceptable OR
       NEW.window_sequence_start_at IS DISTINCT FROM OLD.window_sequence_start_at OR NEW.qualification_due_at IS DISTINCT FROM OLD.qualification_due_at OR
       NEW.created_at IS DISTINCT FROM OLD.created_at OR NEW.frozen_at IS DISTINCT FROM OLD.frozen_at THEN
      RAISE EXCEPTION 'cancel cannot rewrite qualification definition';
    END IF;
    NEW.canceled_at:=now();
    RETURN NEW;
  END IF;
  IF NEW IS DISTINCT FROM OLD THEN RAISE EXCEPTION 'invalid qualification lifecycle transition'; END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER recording_qualification_run_lifecycle_insert_update
BEFORE INSERT OR UPDATE ON recording_qualification_runs
FOR EACH ROW EXECUTE FUNCTION enforce_recording_qualification_run_lifecycle();
CREATE TRIGGER recording_qualification_run_lifecycle_delete
BEFORE DELETE ON recording_qualification_runs
FOR EACH ROW EXECUTE FUNCTION enforce_recording_qualification_run_lifecycle();
