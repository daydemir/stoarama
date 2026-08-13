-- Campaign roster v2, R1: empty durable authority and database invariants only.
--
-- This migration intentionally adds no seed, writer API, runtime grant, or
-- production membership.  Completion is also deliberately unavailable until
-- a later migration binds typed delivery evidence.

DO $$
DECLARE ns TEXT:=current_schema();
BEGIN
  IF ns IS NULL OR ns='pg_temp' OR ns LIKE 'pg_temp_%' OR cardinality(current_schemas(false))<>1 THEN
    RAISE EXCEPTION 'campaign roster v2 requires exactly one controlled non-temporary schema';
  END IF;
  PERFORM set_config('search_path',quote_ident(ns)||',pg_catalog',true);
END $$;

-- Close the snapshot/trigger installation race. INSERT takes ROW EXCLUSIVE,
-- which conflicts with this lock; transactions that committed first are in
-- the snapshot, while later inserts wait until the baseline trigger exists.
-- MigrateUp applies its five-second lock timeout before executing this file.
LOCK TABLE streams IN SHARE ROW EXCLUSIVE MODE;

CREATE UNIQUE INDEX IF NOT EXISTS recordings_id_account_stream_v2_idx
  ON recordings(id,account_id,stream_id);
CREATE UNIQUE INDEX IF NOT EXISTS stream_source_revisions_id_stream_v2_idx
  ON stream_source_revisions(id,stream_id);

CREATE FUNCTION recording_campaign_v2_digest(p_domain TEXT, VARIADIC p_fields TEXT[])
RETURNS TEXT LANGUAGE sql IMMUTABLE STRICT AS $$
  SELECT encode(sha256(convert_to(
    p_domain||chr(10)||COALESCE((
      SELECT string_agg(octet_length(v)::text||':'||v||chr(10),'' ORDER BY ordinal)
      FROM unnest(p_fields) WITH ORDINALITY AS f(v,ordinal)
    ),''),'UTF8')),'hex')
$$;

-- Supported source writers and roster evidence/activation acquire this
-- transaction fence before their first stream/job/recording row lock. It
-- serializes the pre-existing stream->recording and job->recording->stream
-- writer orders against roster validation without relying on a row trigger
-- that would run only after UPDATE had already locked its target row.
CREATE FUNCTION recording_campaign_v2_lock_source_stream(p_stream BIGINT)
RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended(
    'recording-campaign-source-stream:'||p_stream::text,0));
END $$;

-- Trigger names fire alphabetically for the same timing/event. Prefixing this
-- admission pre-fence makes it run before C1's
-- recording_presentation_v2_admission_validate trigger, so the stream fence
-- precedes C1's recording and stream row locks.
CREATE FUNCTION recording_campaign_v2_fence_presentation_admission() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  PERFORM recording_campaign_v2_lock_source_stream(NEW.stream_id);
  RETURN NEW;
END $$;
CREATE TRIGGER a_recording_campaign_v2_admission_source_fence
BEFORE INSERT ON recording_presentation_v2_admissions FOR EACH ROW
EXECUTE FUNCTION recording_campaign_v2_fence_presentation_admission();

-- The source catalog predates append-only source revisions. Freeze the exact
-- source tuple once for every existing/new stream so the first later revision
-- has a database-authored predecessor instead of trusting caller text.
CREATE TABLE stream_source_v2_baselines (
  stream_id BIGINT PRIMARY KEY REFERENCES streams(id) ON DELETE RESTRICT,
  source_url TEXT NOT NULL,
  source_page_url TEXT NOT NULL,
  source_family TEXT NOT NULL,
  capture_type TEXT NOT NULL,
  execution_class TEXT NOT NULL,
  frozen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO stream_source_v2_baselines(stream_id,source_url,source_page_url,source_family,capture_type,execution_class)
SELECT id,source_url,source_page_url,source_family,capture_type,execution_class FROM streams;

CREATE FUNCTION freeze_stream_source_v2_baseline() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO stream_source_v2_baselines(stream_id,source_url,source_page_url,source_family,capture_type,execution_class,frozen_at)
  VALUES(NEW.id,NEW.source_url,NEW.source_page_url,NEW.source_family,NEW.capture_type,NEW.execution_class,now());
  RETURN NULL;
END $$;
CREATE TRIGGER streams_source_v2_baseline AFTER INSERT ON streams
FOR EACH ROW EXECUTE FUNCTION freeze_stream_source_v2_baseline();

DO $$
DECLARE stream_count BIGINT; baseline_count BIGINT;
BEGIN
  SELECT count(*) INTO stream_count FROM streams;
  SELECT count(*) INTO baseline_count FROM stream_source_v2_baselines;
  IF baseline_count<>stream_count THEN
    RAISE EXCEPTION 'stream source baseline snapshot is incomplete';
  END IF;
END $$;

CREATE TABLE recording_campaign_v2_campaigns (
  id BIGSERIAL PRIMARY KEY,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  campaign_key TEXT NOT NULL CHECK(campaign_key~'^[a-z0-9][a-z0-9._-]{0,63}$'),
  kind TEXT NOT NULL CHECK(kind IN('delivery30','repair21','reserve','strict50')),
  created_by_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(account_id,campaign_key),
  UNIQUE(id,account_id)
);

CREATE TABLE recording_campaign_v2_policy_versions (
  id BIGSERIAL PRIMARY KEY,
  campaign_id BIGINT NOT NULL,
  account_id BIGINT NOT NULL,
  version INTEGER NOT NULL CHECK(version>0),
  predecessor_policy_id BIGINT,
  policy_schema TEXT NOT NULL CHECK(policy_schema='campaign-roster-v2-r1'),
  target_count INTEGER NOT NULL CHECK(target_count BETWEEN 1 AND 1000),
  deadline_at TIMESTAMPTZ NOT NULL,
  grade_floor TEXT NOT NULL CHECK(grade_floor IN('GOOD','GREAT')),
  required_consecutive_windows INTEGER NOT NULL CHECK(required_consecutive_windows BETWEEN 0 AND 14),
  evidence_freshness_seconds INTEGER NOT NULL CHECK(evidence_freshness_seconds BETWEEN 60 AND 86400),
  selection_group TEXT NOT NULL CHECK(selection_group IN('operational-v1','strict-v1')),
  overlap_policy TEXT NOT NULL CHECK(overlap_policy IN('operational-disjoint-v1','strict-disjoint-operational-v1','strict-may-overlap-operational-v1')),
  definition_sha256 TEXT NOT NULL CHECK(definition_sha256~'^[0-9a-f]{64}$'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY(campaign_id,account_id) REFERENCES recording_campaign_v2_campaigns(id,account_id) ON DELETE RESTRICT,
  FOREIGN KEY(predecessor_policy_id) REFERENCES recording_campaign_v2_policy_versions(id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
  UNIQUE(campaign_id,version),
  UNIQUE(predecessor_policy_id),
  UNIQUE(id,campaign_id,account_id)
);

CREATE TABLE recording_campaign_v2_revisions (
  id BIGSERIAL PRIMARY KEY,
  campaign_id BIGINT NOT NULL,
  account_id BIGINT NOT NULL,
  revision_number INTEGER NOT NULL CHECK(revision_number>0),
  predecessor_revision_id BIGINT,
  decision_uuid UUID NOT NULL UNIQUE,
  event_type TEXT NOT NULL CHECK(event_type IN('create','enter_planning','roster_change','activate','complete','retire','policy_advance')),
  from_state TEXT CHECK(from_state IS NULL OR from_state IN('draft','planning','active','complete','retired')),
  to_state TEXT NOT NULL CHECK(to_state IN('draft','planning','active','complete','retired')),
  policy_version_id BIGINT NOT NULL,
  actor_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
  actor_role TEXT NOT NULL CHECK(actor_role IN('operator','owner','admin')),
  decided_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  request_sha256 TEXT NOT NULL CHECK(request_sha256~'^[0-9a-f]{64}$'),
  plan_sha256 TEXT NOT NULL CHECK(plan_sha256~'^[0-9a-f]{64}$'),
  idempotency_key TEXT NOT NULL CHECK(octet_length(idempotency_key) BETWEEN 1 AND 128),
  reason_codes TEXT[] NOT NULL CHECK(cardinality(reason_codes) BETWEEN 1 AND 16),
  FOREIGN KEY(campaign_id,account_id) REFERENCES recording_campaign_v2_campaigns(id,account_id) ON DELETE RESTRICT,
  FOREIGN KEY(predecessor_revision_id) REFERENCES recording_campaign_v2_revisions(id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY(policy_version_id,campaign_id,account_id) REFERENCES recording_campaign_v2_policy_versions(id,campaign_id,account_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
  UNIQUE(campaign_id,revision_number),
  UNIQUE(predecessor_revision_id),
  UNIQUE(id,campaign_id,account_id)
);

CREATE TABLE recording_campaign_v2_revision_commits (
  revision_id BIGINT PRIMARY KEY,
  campaign_id BIGINT NOT NULL,
  account_id BIGINT NOT NULL,
  roster_sha256 TEXT NOT NULL CHECK(roster_sha256~'^[0-9a-f]{64}$'),
  filled_count INTEGER NOT NULL CHECK(filled_count>=0),
  prospective_count INTEGER NOT NULL CHECK(prospective_count>=0),
  vacant_count INTEGER NOT NULL CHECK(vacant_count>=0),
  replace_count INTEGER NOT NULL CHECK(replace_count>=0),
  protected_count INTEGER NOT NULL CHECK(protected_count>=0),
  sealed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY(revision_id,campaign_id,account_id) REFERENCES recording_campaign_v2_revisions(id,campaign_id,account_id) ON DELETE RESTRICT,
  UNIQUE(revision_id,campaign_id)
);

CREATE TABLE recording_campaign_v2_slots (
  id BIGSERIAL PRIMARY KEY,
  campaign_id BIGINT NOT NULL,
  account_id BIGINT NOT NULL,
  ordinal INTEGER NOT NULL CHECK(ordinal>0),
  created_revision_id BIGINT NOT NULL,
  FOREIGN KEY(campaign_id,account_id) REFERENCES recording_campaign_v2_campaigns(id,account_id) ON DELETE RESTRICT,
  FOREIGN KEY(created_revision_id,campaign_id,account_id) REFERENCES recording_campaign_v2_revisions(id,campaign_id,account_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
  UNIQUE(campaign_id,ordinal),
  UNIQUE(id,campaign_id,account_id)
);

CREATE TABLE recording_campaign_v2_filled_evidence (
  id BIGSERIAL PRIMARY KEY,
  campaign_id BIGINT NOT NULL,
  account_id BIGINT NOT NULL,
  created_revision_id BIGINT NOT NULL,
  policy_version_id BIGINT NOT NULL,
  recording_id BIGINT NOT NULL,
  stream_id BIGINT NOT NULL,
  source_revision_id BIGINT,
  recording_source_sha256 TEXT NOT NULL CHECK(recording_source_sha256~'^[0-9a-f]{64}$'),
  stream_source_sha256 TEXT NOT NULL CHECK(stream_source_sha256~'^[0-9a-f]{64}$'),
  source_page_sha256 TEXT NOT NULL CHECK(source_page_sha256~'^[0-9a-f]{64}$'),
  provider TEXT NOT NULL CHECK(octet_length(provider) BETWEEN 1 AND 64),
  external_id TEXT NOT NULL CHECK(octet_length(external_id) BETWEEN 1 AND 256),
  source_family TEXT NOT NULL CHECK(octet_length(source_family) BETWEEN 1 AND 64),
  capture_type TEXT NOT NULL CHECK(octet_length(capture_type) BETWEEN 1 AND 64),
  execution_class TEXT NOT NULL CHECK(octet_length(execution_class) BETWEEN 1 AND 64),
  source_snapshot_sha256 TEXT NOT NULL CHECK(source_snapshot_sha256~'^[0-9a-f]{64}$'),
  scene_frame_evidence_id BIGINT NOT NULL,
  scene_identity_sha256 TEXT NOT NULL CHECK(scene_identity_sha256~'^[0-9a-f]{64}$'),
  scene_evidence_sha256 TEXT NOT NULL CHECK(scene_evidence_sha256~'^[0-9a-f]{64}$'),
  frame_id BIGINT NOT NULL,
  media_object_id BIGINT NOT NULL,
  frame_sha256 TEXT NOT NULL CHECK(frame_sha256~'^[0-9a-f]{64}$'),
  frame_captured_at TIMESTAMPTZ NOT NULL,
  scene_verified_at TIMESTAMPTZ NOT NULL,
  accepted_at_decision TIMESTAMPTZ NOT NULL DEFAULT now(),
  evidence_sha256 TEXT NOT NULL CHECK(evidence_sha256~'^[0-9a-f]{64}$'),
  FOREIGN KEY(campaign_id,account_id) REFERENCES recording_campaign_v2_campaigns(id,account_id) ON DELETE RESTRICT,
  FOREIGN KEY(created_revision_id,campaign_id,account_id) REFERENCES recording_campaign_v2_revisions(id,campaign_id,account_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY(policy_version_id,campaign_id,account_id) REFERENCES recording_campaign_v2_policy_versions(id,campaign_id,account_id) ON DELETE RESTRICT,
  FOREIGN KEY(recording_id,account_id,stream_id) REFERENCES recordings(id,account_id,stream_id) ON DELETE RESTRICT,
  FOREIGN KEY(source_revision_id,stream_id) REFERENCES stream_source_revisions(id,stream_id) ON DELETE RESTRICT,
  FOREIGN KEY(scene_frame_evidence_id,account_id,stream_id,scene_identity_sha256) REFERENCES recording_scene_frame_evidence(id,account_id,stream_id,scene_identity_sha256) ON DELETE RESTRICT,
  UNIQUE(id,campaign_id,account_id),
  UNIQUE(id,campaign_id,account_id,recording_id,stream_id,scene_identity_sha256)
);

CREATE TABLE recording_campaign_v2_prospective_evidence (
  id BIGSERIAL PRIMARY KEY,
  campaign_id BIGINT NOT NULL,
  account_id BIGINT NOT NULL,
  created_revision_id BIGINT NOT NULL,
  policy_version_id BIGINT NOT NULL,
  stream_id BIGINT NOT NULL,
  official_page_sha256 TEXT NOT NULL CHECK(official_page_sha256~'^[0-9a-f]{64}$'),
  source_sha256 TEXT NOT NULL CHECK(source_sha256~'^[0-9a-f]{64}$'),
  provider TEXT NOT NULL CHECK(octet_length(provider) BETWEEN 1 AND 64),
  scene_frame_evidence_id BIGINT NOT NULL,
  scene_identity_sha256 TEXT NOT NULL CHECK(scene_identity_sha256~'^[0-9a-f]{64}$'),
  scene_evidence_sha256 TEXT NOT NULL CHECK(scene_evidence_sha256~'^[0-9a-f]{64}$'),
  frame_id BIGINT NOT NULL,
  media_object_id BIGINT NOT NULL,
  frame_sha256 TEXT NOT NULL CHECK(frame_sha256~'^[0-9a-f]{64}$'),
  frame_captured_at TIMESTAMPTZ NOT NULL,
  scene_verified_at TIMESTAMPTZ NOT NULL,
  accepted_at_decision TIMESTAMPTZ NOT NULL DEFAULT now(),
  evidence_sha256 TEXT NOT NULL CHECK(evidence_sha256~'^[0-9a-f]{64}$'),
  FOREIGN KEY(campaign_id,account_id) REFERENCES recording_campaign_v2_campaigns(id,account_id) ON DELETE RESTRICT,
  FOREIGN KEY(created_revision_id,campaign_id,account_id) REFERENCES recording_campaign_v2_revisions(id,campaign_id,account_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY(policy_version_id,campaign_id,account_id) REFERENCES recording_campaign_v2_policy_versions(id,campaign_id,account_id) ON DELETE RESTRICT,
  FOREIGN KEY(scene_frame_evidence_id,account_id,stream_id,scene_identity_sha256) REFERENCES recording_scene_frame_evidence(id,account_id,stream_id,scene_identity_sha256) ON DELETE RESTRICT,
  UNIQUE(id,campaign_id,account_id),
  UNIQUE(id,campaign_id,account_id,stream_id,scene_identity_sha256)
);

CREATE TABLE recording_campaign_v2_slot_versions (
  id BIGSERIAL PRIMARY KEY,
  campaign_id BIGINT NOT NULL,
  account_id BIGINT NOT NULL,
  slot_id BIGINT NOT NULL,
  campaign_revision_id BIGINT NOT NULL,
  version INTEGER NOT NULL CHECK(version>0),
  predecessor_version_id BIGINT,
  role TEXT NOT NULL CHECK(role IN('primary','repair','backup','reserve')),
  rank INTEGER NOT NULL CHECK(rank>0),
  disposition TEXT NOT NULL CHECK(disposition IN('protect','probation','replace','prospective','vacant','removed')),
  filled_evidence_id BIGINT,
  prospective_evidence_id BIGINT,
  recording_id BIGINT,
  stream_id BIGINT,
  scene_identity_sha256 TEXT CHECK(scene_identity_sha256 IS NULL OR scene_identity_sha256~'^[0-9a-f]{64}$'),
  effective_from TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY(campaign_id,account_id) REFERENCES recording_campaign_v2_campaigns(id,account_id) ON DELETE RESTRICT,
  FOREIGN KEY(slot_id,campaign_id,account_id) REFERENCES recording_campaign_v2_slots(id,campaign_id,account_id) ON DELETE RESTRICT,
  FOREIGN KEY(campaign_revision_id,campaign_id,account_id) REFERENCES recording_campaign_v2_revisions(id,campaign_id,account_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY(predecessor_version_id) REFERENCES recording_campaign_v2_slot_versions(id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY(filled_evidence_id,campaign_id,account_id,recording_id,stream_id,scene_identity_sha256) REFERENCES recording_campaign_v2_filled_evidence(id,campaign_id,account_id,recording_id,stream_id,scene_identity_sha256) ON DELETE RESTRICT,
  FOREIGN KEY(prospective_evidence_id,campaign_id,account_id,stream_id,scene_identity_sha256) REFERENCES recording_campaign_v2_prospective_evidence(id,campaign_id,account_id,stream_id,scene_identity_sha256) ON DELETE RESTRICT,
  UNIQUE(slot_id,version),
  UNIQUE(predecessor_version_id),
  UNIQUE(id,campaign_id,account_id,slot_id),
  CHECK(
    (filled_evidence_id IS NOT NULL AND prospective_evidence_id IS NULL AND recording_id IS NOT NULL AND stream_id IS NOT NULL AND scene_identity_sha256 IS NOT NULL AND disposition IN('protect','probation','replace')) OR
    (filled_evidence_id IS NULL AND prospective_evidence_id IS NOT NULL AND recording_id IS NULL AND stream_id IS NOT NULL AND scene_identity_sha256 IS NOT NULL AND disposition='prospective') OR
    (filled_evidence_id IS NULL AND prospective_evidence_id IS NULL AND recording_id IS NULL AND stream_id IS NULL AND scene_identity_sha256 IS NULL AND disposition IN('vacant','removed'))
  )
);

CREATE TABLE recording_campaign_v2_revision_slot_changes (
  revision_id BIGINT NOT NULL,
  campaign_id BIGINT NOT NULL,
  account_id BIGINT NOT NULL,
  slot_id BIGINT NOT NULL,
  before_version_id BIGINT,
  after_version_id BIGINT NOT NULL,
  PRIMARY KEY(revision_id,slot_id),
  FOREIGN KEY(revision_id,campaign_id,account_id) REFERENCES recording_campaign_v2_revisions(id,campaign_id,account_id) ON DELETE RESTRICT,
  FOREIGN KEY(slot_id,campaign_id,account_id) REFERENCES recording_campaign_v2_slots(id,campaign_id,account_id) ON DELETE RESTRICT,
  FOREIGN KEY(before_version_id) REFERENCES recording_campaign_v2_slot_versions(id) ON DELETE RESTRICT,
  FOREIGN KEY(after_version_id,campaign_id,account_id,slot_id) REFERENCES recording_campaign_v2_slot_versions(id,campaign_id,account_id,slot_id) ON DELETE RESTRICT,
  UNIQUE(after_version_id)
);

CREATE TABLE recording_campaign_v2_recording_occupancy (
  account_id BIGINT NOT NULL,
  occupancy_scope TEXT NOT NULL CHECK(occupancy_scope IN('operational','strict')),
  recording_id BIGINT NOT NULL,
  campaign_id BIGINT NOT NULL,
  slot_id BIGINT NOT NULL,
  slot_version_id BIGINT NOT NULL,
  PRIMARY KEY(account_id,occupancy_scope,recording_id),
  FOREIGN KEY(campaign_id,account_id) REFERENCES recording_campaign_v2_campaigns(id,account_id) ON DELETE RESTRICT,
  FOREIGN KEY(slot_version_id,campaign_id,account_id,slot_id) REFERENCES recording_campaign_v2_slot_versions(id,campaign_id,account_id,slot_id) ON DELETE RESTRICT
);

CREATE TABLE recording_campaign_v2_scene_occupancy (
  account_id BIGINT NOT NULL,
  occupancy_scope TEXT NOT NULL CHECK(occupancy_scope IN('operational','strict')),
  scene_identity_sha256 TEXT NOT NULL CHECK(scene_identity_sha256~'^[0-9a-f]{64}$'),
  campaign_id BIGINT NOT NULL,
  slot_id BIGINT NOT NULL,
  slot_version_id BIGINT NOT NULL,
  PRIMARY KEY(account_id,occupancy_scope,scene_identity_sha256),
  FOREIGN KEY(campaign_id,account_id) REFERENCES recording_campaign_v2_campaigns(id,account_id) ON DELETE RESTRICT,
  FOREIGN KEY(slot_version_id,campaign_id,account_id,slot_id) REFERENCES recording_campaign_v2_slot_versions(id,campaign_id,account_id,slot_id) ON DELETE RESTRICT
);

CREATE TABLE recording_campaign_v2_idempotency (
  account_id BIGINT NOT NULL,
  campaign_id BIGINT NOT NULL,
  idempotency_key TEXT NOT NULL CHECK(octet_length(idempotency_key) BETWEEN 1 AND 128),
  request_sha256 TEXT NOT NULL CHECK(request_sha256~'^[0-9a-f]{64}$'),
  plan_sha256 TEXT NOT NULL CHECK(plan_sha256~'^[0-9a-f]{64}$'),
  response_sha256 TEXT NOT NULL CHECK(response_sha256~'^[0-9a-f]{64}$'),
  response_bytes BYTEA NOT NULL CHECK(octet_length(response_bytes) BETWEEN 1 AND 1048576),
  revision_id BIGINT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(account_id,campaign_id,idempotency_key),
  FOREIGN KEY(campaign_id,account_id) REFERENCES recording_campaign_v2_campaigns(id,account_id) ON DELETE RESTRICT,
  FOREIGN KEY(revision_id,campaign_id,account_id) REFERENCES recording_campaign_v2_revisions(id,campaign_id,account_id) ON DELETE RESTRICT
);

CREATE FUNCTION reject_recording_campaign_v2_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'campaign roster v2 authority is append-only';
END $$;

DO $$
DECLARE table_name TEXT;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'stream_source_v2_baselines',
    'recording_campaign_v2_campaigns','recording_campaign_v2_policy_versions',
    'recording_campaign_v2_revisions','recording_campaign_v2_revision_commits',
    'recording_campaign_v2_slots','recording_campaign_v2_filled_evidence',
    'recording_campaign_v2_prospective_evidence','recording_campaign_v2_slot_versions',
    'recording_campaign_v2_revision_slot_changes','recording_campaign_v2_idempotency'
  ] LOOP
    EXECUTE format('CREATE TRIGGER %I_append_only BEFORE UPDATE OR DELETE OR TRUNCATE ON %I FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_campaign_v2_mutation()',table_name,table_name);
  END LOOP;
END $$;

-- No supported source mutation path may update a stream or its active
-- recording snapshot without first entering the transaction-level stream
-- fence. Row triggers are the fail-closed DB backstop for direct writers; the
-- Go paths acquire it before their first row lock to avoid lock inversion.
CREATE FUNCTION enforce_recording_campaign_v2_source_stream_fence() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE stream_key BIGINT;
BEGIN
  IF TG_TABLE_NAME='streams' THEN stream_key:=NEW.id; ELSE stream_key:=NEW.stream_id; END IF;
  IF NOT pg_try_advisory_xact_lock(hashtextextended(
    'recording-campaign-source-stream:'||stream_key::text,0)) THEN
    RAISE EXCEPTION 'stream source mutation requires campaign source fence';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER streams_campaign_v2_source_fence
BEFORE UPDATE OF source_url,source_page_url,source_family,capture_type,execution_class ON streams
FOR EACH ROW EXECUTE FUNCTION enforce_recording_campaign_v2_source_stream_fence();
CREATE TRIGGER recordings_campaign_v2_source_fence
BEFORE UPDATE OF stream_url,stream_id ON recordings
FOR EACH ROW EXECUTE FUNCTION enforce_recording_campaign_v2_source_stream_fence();

-- Existing source revision writers only append.  Serialize each append behind
-- the stream row and make the historical lineage durable before roster
-- evidence can reference it.
DO $$
DECLARE broken_count INTEGER;
BEGIN
  WITH ordered AS (
    SELECT r.*,
      row_number() OVER(PARTITION BY stream_id ORDER BY created_at,id) AS rn,
      lag(new_source_url) OVER(PARTITION BY stream_id ORDER BY created_at,id) AS prior_url,
      lag(new_source_page_url) OVER(PARTITION BY stream_id ORDER BY created_at,id) AS prior_page,
      lag(new_source_family) OVER(PARTITION BY stream_id ORDER BY created_at,id) AS prior_family,
      lag(new_capture_type) OVER(PARTITION BY stream_id ORDER BY created_at,id) AS prior_capture,
      lag(new_execution_class) OVER(PARTITION BY stream_id ORDER BY created_at,id) AS prior_execution
    FROM stream_source_revisions r
  )
  SELECT count(*) INTO broken_count
  FROM ordered o LEFT JOIN streams s ON s.id=o.stream_id
  WHERE s.id IS NULL OR EXISTS(
    SELECT 1 FROM stream_source_revisions tied
    WHERE tied.stream_id=o.stream_id AND tied.id<>o.id AND tied.created_at=o.created_at
  ) OR (o.rn>1 AND
    (o.previous_source_url,o.previous_source_page_url,o.previous_source_family,o.previous_capture_type,o.previous_execution_class)
    IS DISTINCT FROM (o.prior_url,o.prior_page,o.prior_family,o.prior_capture,o.prior_execution));
  IF broken_count<>0 THEN
    RAISE EXCEPTION 'existing stream source revision lineage is structurally incoherent';
  END IF;
END $$;

CREATE FUNCTION validate_stream_source_revision_v2_append() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE s streams%ROWTYPE; prior stream_source_revisions%ROWTYPE; baseline stream_source_v2_baselines%ROWTYPE;
BEGIN
  SELECT * INTO s FROM streams WHERE id=NEW.stream_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'source revision stream missing'; END IF;
  SELECT * INTO prior FROM stream_source_revisions WHERE stream_id=NEW.stream_id ORDER BY created_at DESC,id DESC LIMIT 1;
  IF FOUND THEN
    IF (NEW.previous_source_url,NEW.previous_source_page_url,NEW.previous_source_family,NEW.previous_capture_type,NEW.previous_execution_class)
      IS DISTINCT FROM
      (prior.new_source_url,prior.new_source_page_url,prior.new_source_family,prior.new_capture_type,prior.new_execution_class) THEN
      RAISE EXCEPTION 'source revision does not extend current lineage terminal';
    END IF;
  ELSE
    SELECT * INTO baseline FROM stream_source_v2_baselines WHERE stream_id=NEW.stream_id FOR SHARE;
    IF NOT FOUND OR
      (NEW.previous_source_url,NEW.previous_source_page_url,NEW.previous_source_family,NEW.previous_capture_type,NEW.previous_execution_class)
        IS DISTINCT FROM (baseline.source_url,baseline.source_page_url,baseline.source_family,baseline.capture_type,baseline.execution_class)
    THEN
      RAISE EXCEPTION 'first source revision does not extend frozen baseline';
    END IF;
  END IF;
  IF (NEW.new_source_url,NEW.new_source_page_url,NEW.new_source_family,NEW.new_capture_type,NEW.new_execution_class)
    IS DISTINCT FROM (s.source_url,s.source_page_url,s.source_family,s.capture_type,s.execution_class) THEN
    RAISE EXCEPTION 'source revision terminal does not match locked stream';
  END IF;
  IF (NEW.new_source_url,NEW.new_source_page_url,NEW.new_source_family,NEW.new_capture_type,NEW.new_execution_class)
    IS NOT DISTINCT FROM
    (NEW.previous_source_url,NEW.previous_source_page_url,NEW.previous_source_family,NEW.previous_capture_type,NEW.previous_execution_class) THEN
    RAISE EXCEPTION 'source revision must record an actual source change';
  END IF;
  NEW.created_at:=clock_timestamp();
  IF prior.id IS NOT NULL AND NEW.created_at<=prior.created_at THEN
    RAISE EXCEPTION 'source revision database time did not strictly advance';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER stream_source_revisions_v2_append_validate
BEFORE INSERT ON stream_source_revisions FOR EACH ROW EXECUTE FUNCTION validate_stream_source_revision_v2_append();
CREATE TRIGGER stream_source_revisions_v2_no_update_delete
BEFORE UPDATE OR DELETE ON stream_source_revisions FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_campaign_v2_mutation();
CREATE TRIGGER stream_source_revisions_v2_no_truncate
BEFORE TRUNCATE ON stream_source_revisions FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_campaign_v2_mutation();

CREATE FUNCTION recording_campaign_v2_reason_codes_valid(p_event TEXT,p_codes TEXT[])
RETURNS BOOLEAN LANGUAGE sql IMMUTABLE STRICT AS $$
  SELECT cardinality(p_codes) BETWEEN 1 AND 16
    AND p_codes=ARRAY(SELECT DISTINCT x FROM unnest(p_codes) x ORDER BY x)
    AND NOT EXISTS(SELECT 1 FROM unnest(p_codes) x WHERE x NOT IN(
      'campaign_created','planning_manifest_frozen','prospective_added','member_admitted',
      'evidence_refreshed','member_probation','member_replaced','member_removed',
      'vacancy_recorded','rank_changed','activation_gates_met','completion_gates_met',
      'campaign_retired','policy_version_advanced'))
    AND CASE p_event
      WHEN 'create' THEN p_codes<@ARRAY['campaign_created']
      WHEN 'enter_planning' THEN p_codes<@ARRAY['planning_manifest_frozen','prospective_added','member_admitted','vacancy_recorded']
      WHEN 'roster_change' THEN p_codes<@ARRAY['prospective_added','member_admitted','evidence_refreshed','member_probation','member_replaced','member_removed','vacancy_recorded','rank_changed']
      WHEN 'activate' THEN p_codes<@ARRAY['activation_gates_met']
      WHEN 'complete' THEN p_codes<@ARRAY['completion_gates_met']
      WHEN 'retire' THEN p_codes<@ARRAY['campaign_retired']
      WHEN 'policy_advance' THEN p_codes<@ARRAY['policy_version_advanced']
      ELSE false END
$$;

CREATE FUNCTION validate_recording_campaign_v2_policy() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE c recording_campaign_v2_campaigns%ROWTYPE; prior recording_campaign_v2_policy_versions%ROWTYPE; expected TEXT;
BEGIN
  SELECT * INTO c FROM recording_campaign_v2_campaigns WHERE id=NEW.campaign_id AND account_id=NEW.account_id FOR SHARE;
  IF NOT FOUND OR NEW.created_at IS DISTINCT FROM now() THEN RAISE EXCEPTION 'campaign policy identity/time mismatch'; END IF;
  IF (c.kind='delivery30' AND NEW.target_count<>30) OR (c.kind='repair21' AND NEW.target_count<>21)
     OR (c.kind='strict50' AND NEW.target_count<>50) THEN RAISE EXCEPTION 'campaign kind target is fixed'; END IF;
  IF c.kind IN('delivery30','repair21','reserve') THEN
    IF (NEW.selection_group,NEW.overlap_policy) IS DISTINCT FROM ('operational-v1','operational-disjoint-v1') THEN
      RAISE EXCEPTION 'operational campaign selection policy mismatch';
    END IF;
  ELSE
    IF NEW.selection_group<>'strict-v1' OR NEW.overlap_policy NOT IN('strict-disjoint-operational-v1','strict-may-overlap-operational-v1') THEN
      RAISE EXCEPTION 'strict campaign selection policy mismatch';
    END IF;
  END IF;
  SELECT * INTO prior FROM recording_campaign_v2_policy_versions WHERE campaign_id=NEW.campaign_id ORDER BY version DESC LIMIT 1;
  IF NEW.version=1 THEN
    IF FOUND OR NEW.predecessor_policy_id IS NOT NULL THEN RAISE EXCEPTION 'first campaign policy chain mismatch'; END IF;
  ELSIF NOT FOUND OR NEW.version<>prior.version+1 OR NEW.predecessor_policy_id IS DISTINCT FROM prior.id
        OR (NEW.target_count,c.kind,NEW.selection_group) IS DISTINCT FROM (prior.target_count,c.kind,prior.selection_group) THEN
    RAISE EXCEPTION 'campaign policy must extend exact predecessor without denominator drift';
  END IF;
  expected:=recording_campaign_v2_digest('campaign-roster-v2-r1-policy',
    c.kind,NEW.version::text,NEW.target_count::text,
    ((extract(epoch from NEW.deadline_at)*1000000)::bigint)::text,NEW.grade_floor,
    NEW.required_consecutive_windows::text,NEW.evidence_freshness_seconds::text,
    NEW.selection_group,NEW.overlap_policy,NEW.policy_schema);
  IF NEW.definition_sha256 IS DISTINCT FROM expected THEN RAISE EXCEPTION 'campaign policy digest mismatch'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_v2_policy_validate BEFORE INSERT ON recording_campaign_v2_policy_versions
FOR EACH ROW EXECUTE FUNCTION validate_recording_campaign_v2_policy();

CREATE FUNCTION validate_recording_campaign_v2_campaign_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.created_at IS DISTINCT FROM now() OR NOT EXISTS(
    SELECT 1 FROM users u WHERE u.id=NEW.created_by_user_id AND (
      u.is_operator OR EXISTS(SELECT 1 FROM memberships m WHERE m.user_id=u.id AND m.org_id=NEW.account_id AND m.accepted_at IS NOT NULL AND m.role IN('owner','admin'))
    )
  ) THEN RAISE EXCEPTION 'campaign creation time/actor must be database-authoritative'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_v2_campaign_insert_validate BEFORE INSERT ON recording_campaign_v2_campaigns
FOR EACH ROW EXECUTE FUNCTION validate_recording_campaign_v2_campaign_insert();

CREATE FUNCTION validate_recording_campaign_v2_revision_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE c recording_campaign_v2_campaigns%ROWTYPE; prior recording_campaign_v2_revisions%ROWTYPE; authorized BOOLEAN;
BEGIN
  SELECT * INTO c FROM recording_campaign_v2_campaigns WHERE id=NEW.campaign_id AND account_id=NEW.account_id FOR SHARE;
  IF NOT FOUND OR NEW.decided_at IS DISTINCT FROM now() OR NOT recording_campaign_v2_reason_codes_valid(NEW.event_type,NEW.reason_codes) THEN
    RAISE EXCEPTION 'campaign revision identity/time/reason mismatch';
  END IF;
  SELECT (u.is_operator OR EXISTS(SELECT 1 FROM memberships m WHERE m.user_id=u.id AND m.org_id=NEW.account_id AND m.accepted_at IS NOT NULL AND m.role IN('owner','admin'))),
         CASE WHEN u.is_operator THEN 'operator' ELSE (SELECT m.role FROM memberships m WHERE m.user_id=u.id AND m.org_id=NEW.account_id AND m.accepted_at IS NOT NULL AND m.role IN('owner','admin') ORDER BY m.role LIMIT 1) END
    INTO authorized,NEW.actor_role FROM users u WHERE u.id=NEW.actor_user_id;
  IF authorized IS DISTINCT FROM true THEN RAISE EXCEPTION 'campaign revision actor is unauthorized'; END IF;
  SELECT * INTO prior FROM recording_campaign_v2_revisions WHERE campaign_id=NEW.campaign_id ORDER BY revision_number DESC LIMIT 1;
  IF NEW.revision_number=1 THEN
    IF FOUND OR NEW.predecessor_revision_id IS NOT NULL OR NEW.event_type<>'create' OR NEW.from_state IS NOT NULL OR NEW.to_state<>'draft' THEN
      RAISE EXCEPTION 'first campaign revision mismatch';
    END IF;
    IF (NEW.actor_user_id,NEW.decided_at) IS DISTINCT FROM (c.created_by_user_id,c.created_at) THEN
      RAISE EXCEPTION 'campaign creator and create revision must be one database decision';
    END IF;
  ELSIF NOT FOUND OR NEW.decided_at<=prior.decided_at
        OR NEW.revision_number<>prior.revision_number+1 OR NEW.predecessor_revision_id IS DISTINCT FROM prior.id
        OR NOT EXISTS(SELECT 1 FROM recording_campaign_v2_revision_commits WHERE revision_id=prior.id) THEN
    RAISE EXCEPTION 'campaign revision must extend the sealed exact predecessor';
  END IF;
  IF NEW.to_state='complete' OR NEW.event_type='complete' THEN
    RAISE EXCEPTION 'campaign completion requires a later typed evidence validator';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_v2_revision_insert_validate BEFORE INSERT ON recording_campaign_v2_revisions
FOR EACH ROW EXECUTE FUNCTION validate_recording_campaign_v2_revision_insert();

CREATE FUNCTION recording_campaign_v2_require_unsealed(p_revision BIGINT,p_campaign BIGINT) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
  IF NOT EXISTS(SELECT 1 FROM recording_campaign_v2_revisions WHERE id=p_revision AND campaign_id=p_campaign)
     OR EXISTS(SELECT 1 FROM recording_campaign_v2_revision_commits WHERE revision_id=p_revision) THEN
    RAISE EXCEPTION 'campaign revision is missing or already sealed';
  END IF;
END $$;

CREATE FUNCTION validate_recording_campaign_v2_slot_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r recording_campaign_v2_revisions%ROWTYPE; p recording_campaign_v2_policy_versions%ROWTYPE;
BEGIN
  PERFORM recording_campaign_v2_require_unsealed(NEW.created_revision_id,NEW.campaign_id);
  SELECT * INTO r FROM recording_campaign_v2_revisions WHERE id=NEW.created_revision_id;
  SELECT * INTO p FROM recording_campaign_v2_policy_versions WHERE id=r.policy_version_id;
  IF r.event_type<>'enter_planning' OR r.to_state<>'planning' OR NEW.ordinal>p.target_count OR NEW.account_id<>r.account_id THEN
    RAISE EXCEPTION 'campaign slots are created only by the exact planning revision';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_v2_slot_insert_validate BEFORE INSERT ON recording_campaign_v2_slots
FOR EACH ROW EXECUTE FUNCTION validate_recording_campaign_v2_slot_insert();

CREATE FUNCTION validate_recording_campaign_v2_filled_evidence() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  r recording_campaign_v2_revisions%ROWTYPE; p recording_campaign_v2_policy_versions%ROWTYPE;
  rec recordings%ROWTYPE; s streams%ROWTYPE; sr stream_source_revisions%ROWTYPE;
  baseline stream_source_v2_baselines%ROWTYPE;
  e recording_scene_frame_evidence%ROWTYPE; revision_count INTEGER; expected_snapshot TEXT; expected_evidence TEXT;
BEGIN
  PERFORM recording_campaign_v2_require_unsealed(NEW.created_revision_id,NEW.campaign_id);
  SELECT * INTO r FROM recording_campaign_v2_revisions WHERE id=NEW.created_revision_id;
  SELECT * INTO p FROM recording_campaign_v2_policy_versions WHERE id=NEW.policy_version_id AND campaign_id=NEW.campaign_id AND account_id=NEW.account_id;
  IF NOT FOUND OR r.policy_version_id<>p.id OR NEW.accepted_at_decision IS DISTINCT FROM now() THEN
    RAISE EXCEPTION 'filled evidence policy/revision/time mismatch';
  END IF;
  PERFORM recording_campaign_v2_lock_source_stream(NEW.stream_id);
  SELECT * INTO rec FROM recordings WHERE id=NEW.recording_id AND account_id=NEW.account_id AND stream_id=NEW.stream_id FOR UPDATE;
  SELECT * INTO s FROM streams WHERE id=NEW.stream_id FOR UPDATE;
  IF NOT FOUND OR rec.status<>'active' OR rec.paused_at IS NOT NULL OR rec.stream_url IS DISTINCT FROM s.source_url THEN
    RAISE EXCEPTION 'filled evidence recording/stream/source binding mismatch';
  END IF;
  SELECT count(*) INTO revision_count FROM stream_source_revisions WHERE stream_id=NEW.stream_id;
  SELECT * INTO sr FROM stream_source_revisions WHERE stream_id=NEW.stream_id ORDER BY created_at DESC,id DESC LIMIT 1 FOR SHARE;
  IF NEW.source_revision_id IS NULL THEN
    SELECT * INTO baseline FROM stream_source_v2_baselines WHERE stream_id=NEW.stream_id FOR SHARE;
    IF revision_count<>0 OR NOT FOUND OR
      (s.source_url,s.source_page_url,s.source_family,s.capture_type,s.execution_class)
        IS DISTINCT FROM (baseline.source_url,baseline.source_page_url,baseline.source_family,baseline.capture_type,baseline.execution_class) THEN
      RAISE EXCEPTION 'baseline source evidence requires zero history and exact frozen source';
    END IF;
  ELSIF revision_count=0 OR NOT FOUND OR NEW.source_revision_id<>sr.id OR
    (sr.new_source_url,sr.new_source_page_url,sr.new_source_family,sr.new_capture_type,sr.new_execution_class)
    IS DISTINCT FROM (s.source_url,s.source_page_url,s.source_family,s.capture_type,s.execution_class) THEN
    RAISE EXCEPTION 'filled evidence source revision is not the coherent current terminal';
  END IF;
  IF (NEW.provider,NEW.external_id,NEW.source_family,NEW.capture_type,NEW.execution_class)
    IS DISTINCT FROM (s.provider,s.external_id,s.source_family,s.capture_type,s.execution_class)
    OR NEW.recording_source_sha256 IS DISTINCT FROM encode(sha256(convert_to(rec.stream_url,'UTF8')),'hex')
    OR NEW.stream_source_sha256 IS DISTINCT FROM encode(sha256(convert_to(s.source_url,'UTF8')),'hex')
    OR NEW.source_page_sha256 IS DISTINCT FROM encode(sha256(convert_to(COALESCE(s.source_page_url,''),'UTF8')),'hex') THEN
    RAISE EXCEPTION 'filled evidence source snapshot fields mismatch';
  END IF;
  expected_snapshot:=recording_campaign_v2_digest('campaign-roster-v2-r1-source',
    NEW.account_id::text,NEW.recording_id::text,NEW.stream_id::text,COALESCE(NEW.source_revision_id,0)::text,
    NEW.recording_source_sha256,NEW.stream_source_sha256,NEW.source_page_sha256,
    NEW.provider,NEW.external_id,NEW.source_family,NEW.capture_type,NEW.execution_class);
  IF NEW.source_snapshot_sha256 IS DISTINCT FROM expected_snapshot THEN RAISE EXCEPTION 'filled evidence source snapshot digest mismatch'; END IF;
  SELECT * INTO e FROM recording_scene_frame_evidence WHERE id=NEW.scene_frame_evidence_id
    AND account_id=NEW.account_id AND stream_id=NEW.stream_id AND scene_identity_sha256=NEW.scene_identity_sha256 FOR SHARE;
  IF NOT FOUND OR (NEW.scene_evidence_sha256,NEW.frame_id,NEW.media_object_id,NEW.frame_sha256,NEW.frame_captured_at,NEW.scene_verified_at)
    IS DISTINCT FROM (e.evidence_sha256,e.frame_id,e.media_object_id,e.frame_sha256,e.captured_at,e.verified_at)
    OR e.captured_at<NEW.accepted_at_decision-make_interval(secs=>p.evidence_freshness_seconds)
    OR e.verified_at<NEW.accepted_at_decision-make_interval(secs=>p.evidence_freshness_seconds)
    OR e.captured_at>NEW.accepted_at_decision OR e.verified_at>NEW.accepted_at_decision THEN
    RAISE EXCEPTION 'filled evidence scene identity/freshness mismatch';
  END IF;
  expected_evidence:=recording_campaign_v2_digest('campaign-roster-v2-r1-filled-evidence',
    NEW.campaign_id::text,NEW.account_id::text,NEW.created_revision_id::text,NEW.policy_version_id::text,
    NEW.recording_id::text,NEW.stream_id::text,COALESCE(NEW.source_revision_id,0)::text,
    NEW.source_snapshot_sha256,NEW.scene_frame_evidence_id::text,NEW.scene_identity_sha256,
    NEW.scene_evidence_sha256,NEW.frame_id::text,NEW.media_object_id::text,NEW.frame_sha256,
    ((extract(epoch from NEW.frame_captured_at)*1000000)::bigint)::text,
    ((extract(epoch from NEW.scene_verified_at)*1000000)::bigint)::text,
    ((extract(epoch from NEW.accepted_at_decision)*1000000)::bigint)::text);
  IF NEW.evidence_sha256 IS DISTINCT FROM expected_evidence THEN RAISE EXCEPTION 'filled evidence digest mismatch'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_v2_filled_evidence_validate BEFORE INSERT ON recording_campaign_v2_filled_evidence
FOR EACH ROW EXECUTE FUNCTION validate_recording_campaign_v2_filled_evidence();

CREATE FUNCTION validate_recording_campaign_v2_prospective_evidence() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  r recording_campaign_v2_revisions%ROWTYPE; p recording_campaign_v2_policy_versions%ROWTYPE;
  s streams%ROWTYPE; e recording_scene_frame_evidence%ROWTYPE; expected_evidence TEXT;
BEGIN
  PERFORM recording_campaign_v2_require_unsealed(NEW.created_revision_id,NEW.campaign_id);
  SELECT * INTO r FROM recording_campaign_v2_revisions WHERE id=NEW.created_revision_id;
  SELECT * INTO p FROM recording_campaign_v2_policy_versions WHERE id=NEW.policy_version_id AND campaign_id=NEW.campaign_id AND account_id=NEW.account_id;
  IF NOT FOUND OR r.policy_version_id<>p.id OR NEW.accepted_at_decision IS DISTINCT FROM now() THEN
    RAISE EXCEPTION 'prospective evidence policy/revision/time mismatch';
  END IF;
  PERFORM recording_campaign_v2_lock_source_stream(NEW.stream_id);
  SELECT * INTO s FROM streams WHERE id=NEW.stream_id FOR UPDATE;
  IF NOT FOUND OR COALESCE(s.source_page_url,'')='' OR NEW.provider IS DISTINCT FROM s.provider
     OR NEW.official_page_sha256 IS DISTINCT FROM encode(sha256(convert_to(s.source_page_url,'UTF8')),'hex')
     OR NEW.source_sha256 IS DISTINCT FROM encode(sha256(convert_to(s.source_url,'UTF8')),'hex') THEN
    RAISE EXCEPTION 'prospective evidence official source mismatch';
  END IF;
  SELECT * INTO e FROM recording_scene_frame_evidence WHERE id=NEW.scene_frame_evidence_id
    AND account_id=NEW.account_id AND stream_id=NEW.stream_id AND scene_identity_sha256=NEW.scene_identity_sha256 FOR SHARE;
  IF NOT FOUND OR (NEW.scene_evidence_sha256,NEW.frame_id,NEW.media_object_id,NEW.frame_sha256,NEW.frame_captured_at,NEW.scene_verified_at)
    IS DISTINCT FROM (e.evidence_sha256,e.frame_id,e.media_object_id,e.frame_sha256,e.captured_at,e.verified_at)
    OR e.captured_at<NEW.accepted_at_decision-make_interval(secs=>p.evidence_freshness_seconds)
    OR e.verified_at<NEW.accepted_at_decision-make_interval(secs=>p.evidence_freshness_seconds)
    OR e.captured_at>NEW.accepted_at_decision OR e.verified_at>NEW.accepted_at_decision THEN
    RAISE EXCEPTION 'prospective evidence scene identity/freshness mismatch';
  END IF;
  expected_evidence:=recording_campaign_v2_digest('campaign-roster-v2-r1-prospective-evidence',
    NEW.campaign_id::text,NEW.account_id::text,NEW.created_revision_id::text,NEW.policy_version_id::text,
    NEW.stream_id::text,NEW.official_page_sha256,NEW.source_sha256,NEW.provider,
    NEW.scene_frame_evidence_id::text,NEW.scene_identity_sha256,NEW.scene_evidence_sha256,
    NEW.frame_id::text,NEW.media_object_id::text,NEW.frame_sha256,
    ((extract(epoch from NEW.frame_captured_at)*1000000)::bigint)::text,
    ((extract(epoch from NEW.scene_verified_at)*1000000)::bigint)::text,
    ((extract(epoch from NEW.accepted_at_decision)*1000000)::bigint)::text);
  IF NEW.evidence_sha256 IS DISTINCT FROM expected_evidence THEN RAISE EXCEPTION 'prospective evidence digest mismatch'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_v2_prospective_evidence_validate BEFORE INSERT ON recording_campaign_v2_prospective_evidence
FOR EACH ROW EXECUTE FUNCTION validate_recording_campaign_v2_prospective_evidence();

CREATE FUNCTION validate_recording_campaign_v2_slot_version() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r recording_campaign_v2_revisions%ROWTYPE; c recording_campaign_v2_campaigns%ROWTYPE; prior recording_campaign_v2_slot_versions%ROWTYPE;
BEGIN
  PERFORM recording_campaign_v2_require_unsealed(NEW.campaign_revision_id,NEW.campaign_id);
  SELECT * INTO r FROM recording_campaign_v2_revisions WHERE id=NEW.campaign_revision_id;
  SELECT * INTO c FROM recording_campaign_v2_campaigns WHERE id=NEW.campaign_id;
  IF NEW.account_id<>r.account_id OR (c.kind IN('delivery30','strict50') AND NEW.role<>'primary')
     OR (c.kind='repair21' AND NEW.role<>'repair') OR (c.kind='reserve' AND NEW.role NOT IN('backup','reserve')) THEN
    RAISE EXCEPTION 'slot role does not match campaign kind';
  END IF;
  SELECT * INTO prior FROM recording_campaign_v2_slot_versions WHERE slot_id=NEW.slot_id ORDER BY version DESC LIMIT 1;
  IF NEW.version=1 THEN
    IF FOUND OR NEW.predecessor_version_id IS NOT NULL OR NOT EXISTS(SELECT 1 FROM recording_campaign_v2_slots WHERE id=NEW.slot_id AND created_revision_id=NEW.campaign_revision_id) THEN
      RAISE EXCEPTION 'first slot version provenance mismatch';
    END IF;
  ELSIF NOT FOUND OR NEW.version<>prior.version+1 OR NEW.predecessor_version_id IS DISTINCT FROM prior.id THEN
    RAISE EXCEPTION 'slot version must extend exact predecessor';
  END IF;
  IF NEW.filled_evidence_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM recording_campaign_v2_filled_evidence WHERE id=NEW.filled_evidence_id AND created_revision_id=NEW.campaign_revision_id) THEN
    RAISE EXCEPTION 'new filled slot version requires fresh revision evidence';
  END IF;
  IF NEW.prospective_evidence_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM recording_campaign_v2_prospective_evidence WHERE id=NEW.prospective_evidence_id AND created_revision_id=NEW.campaign_revision_id) THEN
    RAISE EXCEPTION 'new prospective slot version requires fresh revision evidence';
  END IF;
  IF NEW.effective_from IS DISTINCT FROM r.decided_at THEN RAISE EXCEPTION 'slot version effective time must equal decision time'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_v2_slot_version_validate BEFORE INSERT ON recording_campaign_v2_slot_versions
FOR EACH ROW EXECUTE FUNCTION validate_recording_campaign_v2_slot_version();

CREATE FUNCTION validate_recording_campaign_v2_slot_change() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE a recording_campaign_v2_slot_versions%ROWTYPE; b recording_campaign_v2_slot_versions%ROWTYPE;
BEGIN
  PERFORM recording_campaign_v2_require_unsealed(NEW.revision_id,NEW.campaign_id);
  SELECT * INTO a FROM recording_campaign_v2_slot_versions WHERE id=NEW.after_version_id;
  IF NOT FOUND OR (a.campaign_id,a.account_id,a.slot_id,a.campaign_revision_id)
     IS DISTINCT FROM (NEW.campaign_id,NEW.account_id,NEW.slot_id,NEW.revision_id) THEN
    RAISE EXCEPTION 'slot change after-version mismatch';
  END IF;
  IF NEW.before_version_id IS NULL THEN
    IF a.version<>1 THEN RAISE EXCEPTION 'new slot change must begin at version one'; END IF;
  ELSE
    SELECT * INTO b FROM recording_campaign_v2_slot_versions WHERE id=NEW.before_version_id;
    IF NOT FOUND OR (b.campaign_id,b.account_id,b.slot_id) IS DISTINCT FROM (NEW.campaign_id,NEW.account_id,NEW.slot_id)
       OR b.version>=a.version THEN
      -- The deferred validator performs the complete contiguous-chain proof.
      RAISE EXCEPTION 'slot change before-version mismatch';
    END IF;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_v2_slot_change_validate BEFORE INSERT ON recording_campaign_v2_revision_slot_changes
FOR EACH ROW EXECUTE FUNCTION validate_recording_campaign_v2_slot_change();

CREATE FUNCTION validate_recording_campaign_v2_idempotency() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r recording_campaign_v2_revisions%ROWTYPE; canonical_response BYTEA;
BEGIN
  PERFORM recording_campaign_v2_require_unsealed(NEW.revision_id,NEW.campaign_id);
  SELECT * INTO r FROM recording_campaign_v2_revisions WHERE id=NEW.revision_id;
  canonical_response:=convert_to('{"campaign_id":'||r.campaign_id::text||',"revision":'||r.revision_number::text||'}','UTF8');
  IF (NEW.account_id,NEW.campaign_id,NEW.idempotency_key,NEW.request_sha256,NEW.plan_sha256)
     IS DISTINCT FROM (r.account_id,r.campaign_id,r.idempotency_key,r.request_sha256,r.plan_sha256)
     OR NEW.response_bytes IS DISTINCT FROM canonical_response
     OR NEW.response_sha256 IS DISTINCT FROM encode(sha256(canonical_response),'hex') OR NEW.created_at IS DISTINCT FROM now() THEN
    RAISE EXCEPTION 'campaign idempotency/revision/response mismatch';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_v2_idempotency_validate BEFORE INSERT ON recording_campaign_v2_idempotency
FOR EACH ROW EXECUTE FUNCTION validate_recording_campaign_v2_idempotency();

CREATE VIEW recording_campaign_v2_current_revisions AS
SELECT DISTINCT ON(r.campaign_id)
  r.campaign_id,r.account_id,r.id AS revision_id,r.revision_number,r.to_state AS state,r.policy_version_id,r.decided_at
FROM recording_campaign_v2_revisions r
JOIN recording_campaign_v2_revision_commits s ON s.revision_id=r.id
ORDER BY r.campaign_id,r.revision_number DESC;

CREATE VIEW recording_campaign_v2_current_slot_heads AS
SELECT v.*
FROM recording_campaign_v2_slot_versions v
JOIN recording_campaign_v2_revision_commits seal ON seal.revision_id=v.campaign_revision_id
WHERE NOT EXISTS(
  SELECT 1 FROM recording_campaign_v2_slot_versions later
  JOIN recording_campaign_v2_revision_commits later_seal ON later_seal.revision_id=later.campaign_revision_id
  WHERE later.slot_id=v.slot_id AND later.version>v.version
);

CREATE VIEW recording_campaign_v2_evidence_freshness AS
SELECT h.campaign_id,h.account_id,h.slot_id,h.id AS slot_version_id,
  CASE
    WHEN h.filled_evidence_id IS NOT NULL THEN
      f.frame_captured_at BETWEEN now()-make_interval(secs=>p.evidence_freshness_seconds) AND now()
      AND f.scene_verified_at BETWEEN now()-make_interval(secs=>p.evidence_freshness_seconds) AND now()
    WHEN h.prospective_evidence_id IS NOT NULL THEN
      e.frame_captured_at BETWEEN now()-make_interval(secs=>p.evidence_freshness_seconds) AND now()
      AND e.scene_verified_at BETWEEN now()-make_interval(secs=>p.evidence_freshness_seconds) AND now()
    ELSE false
  END AS evidence_current
FROM recording_campaign_v2_current_slot_heads h
JOIN recording_campaign_v2_current_revisions r ON r.campaign_id=h.campaign_id
JOIN recording_campaign_v2_policy_versions p ON p.id=r.policy_version_id
LEFT JOIN recording_campaign_v2_filled_evidence f ON f.id=h.filled_evidence_id
LEFT JOIN recording_campaign_v2_prospective_evidence e ON e.id=h.prospective_evidence_id;

CREATE VIEW recording_campaign_v2_expected_recording_occupancy AS
WITH base AS (
  SELECT h.account_id,h.recording_id,h.campaign_id,h.slot_id,h.id AS slot_version_id,c.kind,p.overlap_policy
  FROM recording_campaign_v2_current_slot_heads h
  JOIN recording_campaign_v2_current_revisions r ON r.campaign_id=h.campaign_id AND r.state IN('planning','active','complete')
  JOIN recording_campaign_v2_campaigns c ON c.id=h.campaign_id
  JOIN recording_campaign_v2_policy_versions p ON p.id=r.policy_version_id
  WHERE h.recording_id IS NOT NULL
)
SELECT account_id,'operational'::text AS occupancy_scope,recording_id,campaign_id,slot_id,slot_version_id FROM base WHERE kind IN('delivery30','repair21','reserve')
UNION ALL
SELECT account_id,'strict',recording_id,campaign_id,slot_id,slot_version_id FROM base WHERE kind='strict50'
UNION ALL
SELECT account_id,'operational',recording_id,campaign_id,slot_id,slot_version_id FROM base WHERE kind='strict50' AND overlap_policy='strict-disjoint-operational-v1';

CREATE VIEW recording_campaign_v2_expected_scene_occupancy AS
WITH base AS (
  SELECT h.account_id,h.scene_identity_sha256,h.campaign_id,h.slot_id,h.id AS slot_version_id,c.kind,p.overlap_policy
  FROM recording_campaign_v2_current_slot_heads h
  JOIN recording_campaign_v2_current_revisions r ON r.campaign_id=h.campaign_id AND r.state IN('planning','active','complete')
  JOIN recording_campaign_v2_campaigns c ON c.id=h.campaign_id
  JOIN recording_campaign_v2_policy_versions p ON p.id=r.policy_version_id
  WHERE h.scene_identity_sha256 IS NOT NULL
)
SELECT account_id,'operational'::text AS occupancy_scope,scene_identity_sha256,campaign_id,slot_id,slot_version_id FROM base WHERE kind IN('delivery30','repair21','reserve')
UNION ALL
SELECT account_id,'strict',scene_identity_sha256,campaign_id,slot_id,slot_version_id FROM base WHERE kind='strict50'
UNION ALL
SELECT account_id,'operational',scene_identity_sha256,campaign_id,slot_id,slot_version_id FROM base WHERE kind='strict50' AND overlap_policy='strict-disjoint-operational-v1';

CREATE FUNCTION recording_campaign_v2_roster_digest(p_campaign BIGINT) RETURNS TEXT LANGUAGE sql STABLE AS $$
  SELECT recording_campaign_v2_digest('campaign-roster-v2-r1-roster',VARIADIC COALESCE(array_agg(item ORDER BY ordinal),ARRAY[]::text[]))
  FROM (
    SELECT s.ordinal,
      recording_campaign_v2_digest('campaign-roster-v2-r1-slot',
        s.id::text,s.ordinal::text,v.version::text,v.role,v.rank::text,v.disposition,
        COALESCE(v.recording_id,0)::text,COALESCE(v.stream_id,0)::text,COALESCE(v.scene_identity_sha256,''),
        COALESCE(f.source_snapshot_sha256,e.official_page_sha256,''),
        COALESCE(f.scene_evidence_sha256,e.scene_evidence_sha256,''),
        COALESCE(f.source_revision_id,0)::text,
        COALESCE(e.source_sha256,'')) AS item
    FROM recording_campaign_v2_slots s
    JOIN LATERAL (SELECT x.* FROM recording_campaign_v2_slot_versions x WHERE x.slot_id=s.id ORDER BY x.version DESC LIMIT 1) v ON true
    LEFT JOIN recording_campaign_v2_filled_evidence f ON f.id=v.filled_evidence_id
    LEFT JOIN recording_campaign_v2_prospective_evidence e ON e.id=v.prospective_evidence_id
    WHERE s.campaign_id=p_campaign
  ) roster
$$;

CREATE FUNCTION recording_campaign_v2_plan_digest(p_revision BIGINT) RETURNS TEXT LANGUAGE sql STABLE AS $$
  SELECT recording_campaign_v2_digest('campaign-roster-v2-r1-plan',
    r.campaign_id::text,r.revision_number::text,r.event_type,COALESCE(r.from_state,''),r.to_state,
    p.definition_sha256,recording_campaign_v2_digest('campaign-roster-v2-r1-reasons',VARIADIC r.reason_codes),
    recording_campaign_v2_roster_digest(r.campaign_id))
  FROM recording_campaign_v2_revisions r JOIN recording_campaign_v2_policy_versions p ON p.id=r.policy_version_id
  WHERE r.id=p_revision
$$;

CREATE FUNCTION recording_campaign_v2_request_digest(p_revision BIGINT) RETURNS TEXT LANGUAGE sql STABLE AS $$
  SELECT recording_campaign_v2_digest('campaign-roster-v2-r1-request',
    r.campaign_id::text,r.account_id::text,r.revision_number::text,r.event_type,
    COALESCE(r.from_state,''),r.to_state,p.definition_sha256,r.idempotency_key,
    recording_campaign_v2_digest('campaign-roster-v2-r1-reasons',VARIADIC r.reason_codes),r.plan_sha256)
  FROM recording_campaign_v2_revisions r JOIN recording_campaign_v2_policy_versions p ON p.id=r.policy_version_id
  WHERE r.id=p_revision
$$;

CREATE FUNCTION recording_campaign_v2_assert_no_live_presentation(p_account BIGINT,p_recording BIGINT) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE bad BOOLEAN;
BEGIN
  -- Reuse the exact advisory/recording-row fence shared with C1. Existing
  -- protection is compatible (for example an explicitly allowed strict
  -- overlap); only a live presentation generation is a reciprocal veto.
  PERFORM lock_recording_campaign_protection(p_account,p_recording);
  -- Do not lock jobs here. UPDATE obtains its target row before its BEFORE
  -- trigger can enter the recording fence; waiting on that job while holding
  -- this fence would invert the order. MVCC reads the prior committed state;
  -- a crossing heartbeat then waits here and is rejected against the newly
  -- committed protection view by the job UPDATE trigger below.
  SELECT EXISTS(
    SELECT 1 FROM recording_presentation_v2_admissions a
    JOIN recording_jobs j ON j.id=a.recording_job_id
    WHERE a.account_id=p_account AND a.recording_id=p_recording
      AND a.deadline_at>transaction_timestamp()
      AND j.recording_id=a.recording_id AND j.status='leased' AND j.lease_token=a.lease_token
      AND j.lease_owner='node:'||a.node_id::text AND j.lease_expires_at>transaction_timestamp()
  ) INTO bad;
  IF bad THEN RAISE EXCEPTION 'live presentation admission vetoes campaign protection'; END IF;

  SELECT EXISTS(
    SELECT 1 FROM recording_presentation_v2_probe_tasks t
    WHERE t.account_id=p_account AND t.recording_id=p_recording
      AND t.absolute_deadline_at>transaction_timestamp()
      AND ((t.state='awaiting_retention' AND t.retention_state='awaiting')
        OR (t.state IN('pending','leased') AND t.retention_state='active'))
      AND NOT EXISTS(
        SELECT 1 FROM recording_presentation_v2_admissions a
        JOIN recording_presentation_v2_attempts p ON p.id=t.attempt_id AND p.admission_id=a.id
        JOIN recording_clips c ON c.id=t.clip_id AND c.recording_job_id=a.recording_job_id
        WHERE a.id=t.admission_id AND c.recording_id=a.recording_id
          AND (a.account_id,a.recording_id,a.stream_id,a.recording_job_id,a.lease_token,a.node_id)
            IS NOT DISTINCT FROM (t.account_id,t.recording_id,t.stream_id,t.recording_job_id,t.lease_token,t.node_id)
      )
  ) INTO bad;
  IF bad THEN RAISE EXCEPTION 'incoherent presentation task vetoes campaign protection'; END IF;

  SELECT EXISTS(
    SELECT 1 FROM recording_presentation_v2_probe_tasks t
    JOIN recording_presentation_v2_admissions a ON a.id=t.admission_id
    JOIN recording_presentation_v2_attempts p ON p.id=t.attempt_id AND p.admission_id=a.id
    JOIN recording_clips c ON c.id=t.clip_id AND c.recording_job_id=a.recording_job_id AND c.recording_id=a.recording_id
    WHERE t.account_id=p_account AND t.recording_id=p_recording AND t.absolute_deadline_at>transaction_timestamp()
      AND ((t.state='awaiting_retention' AND t.retention_state='awaiting')
        OR (t.state IN('pending','leased') AND t.retention_state='active'))
      AND (a.account_id,a.recording_id,a.stream_id,a.recording_job_id,a.lease_token,a.node_id)
        IS NOT DISTINCT FROM (t.account_id,t.recording_id,t.stream_id,t.recording_job_id,t.lease_token,t.node_id)
  ) INTO bad;
  IF bad THEN RAISE EXCEPTION 'live presentation task vetoes campaign protection'; END IF;
END $$;

CREATE FUNCTION fence_recording_presentation_job_against_campaign_v2() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE a RECORD;
BEGIN
  FOR a IN
    SELECT account_id,recording_id,node_id,lease_token,deadline_at
    FROM recording_presentation_v2_admissions
    WHERE recording_job_id=NEW.id
    ORDER BY id
  LOOP
    IF NEW.recording_id=a.recording_id AND NEW.status='leased' AND NEW.lease_token=a.lease_token
       AND NEW.lease_owner='node:'||a.node_id::text AND NEW.lease_expires_at>transaction_timestamp()
       AND a.deadline_at>transaction_timestamp()
       AND lock_recording_campaign_protection(a.account_id,a.recording_id) THEN
      RAISE EXCEPTION 'campaign-protected recording cannot revive or renew a presentation job';
    END IF;
  END LOOP;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_jobs_presentation_v2_campaign_fence
BEFORE UPDATE ON recording_jobs FOR EACH ROW
EXECUTE FUNCTION fence_recording_presentation_job_against_campaign_v2();

CREATE FUNCTION recording_campaign_v2_seal_preflight() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  r recording_campaign_v2_revisions%ROWTYPE; h RECORD; p recording_campaign_v2_policy_versions%ROWTYPE;
  rec recordings%ROWTYPE; s streams%ROWTYPE; sr stream_source_revisions%ROWTYPE;
  f recording_campaign_v2_filled_evidence%ROWTYPE; pe recording_campaign_v2_prospective_evidence%ROWTYPE;
  frame recording_scene_frame_evidence%ROWTYPE; baseline stream_source_v2_baselines%ROWTYPE;
BEGIN
  SELECT * INTO r FROM recording_campaign_v2_revisions WHERE id=NEW.revision_id FOR SHARE;
  IF NOT FOUND OR (NEW.campaign_id,NEW.account_id) IS DISTINCT FROM (r.campaign_id,r.account_id)
     OR NEW.sealed_at IS DISTINCT FROM now() THEN RAISE EXCEPTION 'campaign seal identity/time mismatch'; END IF;
  IF r.to_state IN('active','complete') THEN
    -- Lock every activation resource as a complete, globally ordered set
    -- before validating any one head. Opposite slot ordinals in concurrent
    -- campaigns must not create a recording/stream/frame lock inversion.
    PERFORM recording_campaign_v2_lock_source_stream(resource.id) FROM streams resource
    WHERE resource.id IN(
      SELECT v.stream_id FROM recording_campaign_v2_slots slot
      JOIN LATERAL (SELECT x.* FROM recording_campaign_v2_slot_versions x WHERE x.slot_id=slot.id ORDER BY x.version DESC LIMIT 1) v ON true
      WHERE slot.campaign_id=r.campaign_id AND (v.filled_evidence_id IS NOT NULL OR v.prospective_evidence_id IS NOT NULL)
    ) ORDER BY resource.id;
    FOR h IN
      SELECT DISTINCT recording_id FROM recording_campaign_v2_slots s
      JOIN LATERAL (SELECT v.* FROM recording_campaign_v2_slot_versions v WHERE v.slot_id=s.id ORDER BY version DESC LIMIT 1) v ON true
      WHERE s.campaign_id=r.campaign_id AND v.recording_id IS NOT NULL AND v.disposition IN('protect','probation')
      ORDER BY recording_id
    LOOP
      -- This shared C1 fence obtains the recording and matching live job /
      -- admission locks, also in ascending recording identity order.
      PERFORM recording_campaign_v2_assert_no_live_presentation(r.account_id,h.recording_id);
    END LOOP;
    PERFORM 1 FROM streams resource
    WHERE resource.id IN(
      SELECT v.stream_id FROM recording_campaign_v2_slots slot
      JOIN LATERAL (SELECT x.* FROM recording_campaign_v2_slot_versions x WHERE x.slot_id=slot.id ORDER BY x.version DESC LIMIT 1) v ON true
      WHERE slot.campaign_id=r.campaign_id AND (v.filled_evidence_id IS NOT NULL OR v.prospective_evidence_id IS NOT NULL)
    ) ORDER BY resource.id FOR UPDATE;
    PERFORM 1 FROM stream_source_revisions resource
    WHERE resource.id IN(
      SELECT f.source_revision_id FROM recording_campaign_v2_slots slot
      JOIN LATERAL (SELECT x.* FROM recording_campaign_v2_slot_versions x WHERE x.slot_id=slot.id ORDER BY x.version DESC LIMIT 1) v ON true
      JOIN recording_campaign_v2_filled_evidence f ON f.id=v.filled_evidence_id
      WHERE slot.campaign_id=r.campaign_id AND f.source_revision_id IS NOT NULL
    ) ORDER BY resource.id FOR SHARE;
    PERFORM 1 FROM stream_source_v2_baselines resource
    WHERE resource.stream_id IN(
      SELECT v.stream_id FROM recording_campaign_v2_slots slot
      JOIN LATERAL (SELECT x.* FROM recording_campaign_v2_slot_versions x WHERE x.slot_id=slot.id ORDER BY x.version DESC LIMIT 1) v ON true
      WHERE slot.campaign_id=r.campaign_id AND v.filled_evidence_id IS NOT NULL
    ) ORDER BY resource.stream_id FOR SHARE;
    PERFORM 1 FROM recording_scene_frame_evidence resource
    WHERE resource.id IN(
      SELECT COALESCE(f.scene_frame_evidence_id,pe.scene_frame_evidence_id)
      FROM recording_campaign_v2_slots slot
      JOIN LATERAL (SELECT x.* FROM recording_campaign_v2_slot_versions x WHERE x.slot_id=slot.id ORDER BY x.version DESC LIMIT 1) v ON true
      LEFT JOIN recording_campaign_v2_filled_evidence f ON f.id=v.filled_evidence_id
      LEFT JOIN recording_campaign_v2_prospective_evidence pe ON pe.id=v.prospective_evidence_id
      WHERE slot.campaign_id=r.campaign_id
    ) ORDER BY resource.id FOR SHARE;
    SELECT * INTO p FROM recording_campaign_v2_policy_versions WHERE id=r.policy_version_id;
    FOR h IN
      SELECT v.filled_evidence_id FROM recording_campaign_v2_slots slot
      JOIN LATERAL (SELECT x.* FROM recording_campaign_v2_slot_versions x WHERE x.slot_id=slot.id ORDER BY x.version DESC LIMIT 1) v ON true
      WHERE slot.campaign_id=r.campaign_id AND v.filled_evidence_id IS NOT NULL
      ORDER BY slot.ordinal
    LOOP
      SELECT * INTO f FROM recording_campaign_v2_filled_evidence WHERE id=h.filled_evidence_id;
      SELECT * INTO s FROM streams WHERE id=f.stream_id;
      SELECT * INTO rec FROM recordings WHERE id=f.recording_id AND account_id=f.account_id AND stream_id=f.stream_id;
      SELECT * INTO sr FROM stream_source_revisions WHERE stream_id=f.stream_id ORDER BY created_at DESC,id DESC LIMIT 1;
      SELECT * INTO baseline FROM stream_source_v2_baselines WHERE stream_id=f.stream_id;
      IF rec.id IS NULL OR rec.status<>'active' OR rec.paused_at IS NOT NULL OR rec.stream_url IS DISTINCT FROM s.source_url
        OR (f.provider,f.external_id,f.source_family,f.capture_type,f.execution_class)
          IS DISTINCT FROM (s.provider,s.external_id,s.source_family,s.capture_type,s.execution_class)
        OR f.recording_source_sha256 IS DISTINCT FROM encode(sha256(convert_to(rec.stream_url,'UTF8')),'hex')
        OR f.stream_source_sha256 IS DISTINCT FROM encode(sha256(convert_to(s.source_url,'UTF8')),'hex')
        OR f.source_page_sha256 IS DISTINCT FROM encode(sha256(convert_to(COALESCE(s.source_page_url,''),'UTF8')),'hex')
        OR (f.source_revision_id IS NULL AND (
          EXISTS(SELECT 1 FROM stream_source_revisions WHERE stream_id=f.stream_id) OR baseline.stream_id IS NULL OR
          (s.source_url,s.source_page_url,s.source_family,s.capture_type,s.execution_class)
            IS DISTINCT FROM (baseline.source_url,baseline.source_page_url,baseline.source_family,baseline.capture_type,baseline.execution_class)))
        OR (f.source_revision_id IS NOT NULL AND (sr.id IS DISTINCT FROM f.source_revision_id OR
          (sr.new_source_url,sr.new_source_page_url,sr.new_source_family,sr.new_capture_type,sr.new_execution_class)
          IS DISTINCT FROM (s.source_url,s.source_page_url,s.source_family,s.capture_type,s.execution_class)))
        OR f.frame_captured_at<transaction_timestamp()-make_interval(secs=>p.evidence_freshness_seconds)
        OR f.scene_verified_at<transaction_timestamp()-make_interval(secs=>p.evidence_freshness_seconds)
        OR f.frame_captured_at>transaction_timestamp() OR f.scene_verified_at>transaction_timestamp() THEN
        RAISE EXCEPTION 'campaign activation filled evidence is stale or source-divergent';
      END IF;
    END LOOP;
    FOR h IN
      SELECT v.prospective_evidence_id FROM recording_campaign_v2_slots slot
      JOIN LATERAL (SELECT x.* FROM recording_campaign_v2_slot_versions x WHERE x.slot_id=slot.id ORDER BY x.version DESC LIMIT 1) v ON true
      WHERE slot.campaign_id=r.campaign_id AND v.prospective_evidence_id IS NOT NULL
      ORDER BY slot.ordinal
    LOOP
      SELECT * INTO pe FROM recording_campaign_v2_prospective_evidence WHERE id=h.prospective_evidence_id;
      SELECT * INTO s FROM streams WHERE id=pe.stream_id;
      SELECT * INTO frame FROM recording_scene_frame_evidence WHERE id=pe.scene_frame_evidence_id
        AND account_id=pe.account_id AND stream_id=pe.stream_id AND scene_identity_sha256=pe.scene_identity_sha256;
      IF s.id IS NULL OR frame.id IS NULL OR COALESCE(s.source_page_url,'')=''
        OR pe.provider IS DISTINCT FROM s.provider
        OR pe.official_page_sha256 IS DISTINCT FROM encode(sha256(convert_to(s.source_page_url,'UTF8')),'hex')
        OR pe.source_sha256 IS DISTINCT FROM encode(sha256(convert_to(s.source_url,'UTF8')),'hex')
        OR (pe.scene_evidence_sha256,pe.frame_id,pe.media_object_id,pe.frame_sha256,pe.frame_captured_at,pe.scene_verified_at)
          IS DISTINCT FROM (frame.evidence_sha256,frame.frame_id,frame.media_object_id,frame.frame_sha256,frame.captured_at,frame.verified_at)
        OR pe.frame_captured_at<transaction_timestamp()-make_interval(secs=>p.evidence_freshness_seconds)
        OR pe.scene_verified_at<transaction_timestamp()-make_interval(secs=>p.evidence_freshness_seconds)
        OR pe.frame_captured_at>transaction_timestamp() OR pe.scene_verified_at>transaction_timestamp() THEN
        RAISE EXCEPTION 'campaign activation prospective evidence is stale or source-divergent';
      END IF;
    END LOOP;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_v2_seal_preflight BEFORE INSERT ON recording_campaign_v2_revision_commits
FOR EACH ROW EXECUTE FUNCTION recording_campaign_v2_seal_preflight();

CREATE FUNCTION validate_recording_campaign_v2_occupancy() RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE bad BOOLEAN;
BEGIN
  SELECT EXISTS(
    SELECT 1 FROM recording_campaign_v2_expected_recording_occupancy GROUP BY account_id,occupancy_scope,recording_id HAVING count(*)<>1
  ) OR EXISTS(
    SELECT 1 FROM recording_campaign_v2_expected_scene_occupancy GROUP BY account_id,occupancy_scope,scene_identity_sha256 HAVING count(*)<>1
  ) INTO bad;
  IF bad THEN RAISE EXCEPTION 'campaign occupancy has a forbidden duplicate recording or physical scene'; END IF;
  SELECT EXISTS(
    (SELECT * FROM recording_campaign_v2_expected_recording_occupancy EXCEPT SELECT * FROM recording_campaign_v2_recording_occupancy)
    UNION ALL
    (SELECT * FROM recording_campaign_v2_recording_occupancy EXCEPT SELECT * FROM recording_campaign_v2_expected_recording_occupancy)
  ) OR EXISTS(
    (SELECT * FROM recording_campaign_v2_expected_scene_occupancy EXCEPT SELECT * FROM recording_campaign_v2_scene_occupancy)
    UNION ALL
    (SELECT * FROM recording_campaign_v2_scene_occupancy EXCEPT SELECT * FROM recording_campaign_v2_expected_scene_occupancy)
  ) INTO bad;
  IF bad THEN RAISE EXCEPTION 'campaign occupancy registry does not equal current nonretired heads'; END IF;
END $$;

CREATE FUNCTION validate_recording_campaign_v2_revision(p_revision BIGINT) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
  r recording_campaign_v2_revisions%ROWTYPE; prior recording_campaign_v2_revisions%ROWTYPE;
  p recording_campaign_v2_policy_versions%ROWTYPE; pp recording_campaign_v2_policy_versions%ROWTYPE;
  c recording_campaign_v2_campaigns%ROWTYPE; seal recording_campaign_v2_revision_commits%ROWTYPE;
  slot_count INTEGER; head_count INTEGER; filled_count INTEGER; prospective_count INTEGER;
  vacant_count INTEGER; replace_count INTEGER; protected_count INTEGER; change_count INTEGER;
  distinct_ranks INTEGER; bad_count INTEGER; expected_roster TEXT; expected_plan TEXT; idem_count INTEGER;
BEGIN
  SELECT * INTO r FROM recording_campaign_v2_revisions WHERE id=p_revision;
  IF NOT FOUND THEN RAISE EXCEPTION 'campaign revision missing'; END IF;
  SELECT * INTO seal FROM recording_campaign_v2_revision_commits WHERE revision_id=r.id;
  IF NOT FOUND THEN RAISE EXCEPTION 'campaign revision is unsealed at commit'; END IF;
  SELECT * INTO c FROM recording_campaign_v2_campaigns WHERE id=r.campaign_id;
  SELECT * INTO p FROM recording_campaign_v2_policy_versions WHERE id=r.policy_version_id;
  IF NOT FOUND OR p.deadline_at<=c.created_at THEN RAISE EXCEPTION 'campaign revision policy missing or deadline invalid'; END IF;

  IF r.revision_number=1 THEN
    IF r.event_type<>'create' OR r.from_state IS NOT NULL OR r.to_state<>'draft' OR p.version<>1 OR p.predecessor_policy_id IS NOT NULL THEN
      RAISE EXCEPTION 'campaign create revision/state/policy mismatch';
    END IF;
    IF (r.actor_user_id,r.decided_at) IS DISTINCT FROM (c.created_by_user_id,c.created_at) THEN
      RAISE EXCEPTION 'campaign creator and create-revision authority mismatch';
    END IF;
  ELSE
    SELECT * INTO prior FROM recording_campaign_v2_revisions WHERE id=r.predecessor_revision_id;
    SELECT * INTO pp FROM recording_campaign_v2_policy_versions WHERE id=prior.policy_version_id;
    IF r.from_state IS DISTINCT FROM prior.to_state THEN RAISE EXCEPTION 'campaign lifecycle predecessor state mismatch'; END IF;
    CASE r.event_type
      WHEN 'enter_planning' THEN
        IF (r.from_state,r.to_state,r.policy_version_id) IS DISTINCT FROM ('draft','planning',prior.policy_version_id) THEN RAISE EXCEPTION 'invalid enter-planning transition'; END IF;
      WHEN 'roster_change' THEN
        IF r.from_state NOT IN('planning','active') OR r.to_state<>r.from_state OR r.policy_version_id<>prior.policy_version_id THEN RAISE EXCEPTION 'invalid roster-change transition'; END IF;
      WHEN 'activate' THEN
        IF (r.from_state,r.to_state,r.policy_version_id) IS DISTINCT FROM ('planning','active',prior.policy_version_id) THEN RAISE EXCEPTION 'invalid activation transition'; END IF;
      WHEN 'policy_advance' THEN
        IF r.from_state NOT IN('draft','planning') OR r.to_state<>r.from_state OR p.version<>pp.version+1 OR p.predecessor_policy_id<>pp.id THEN RAISE EXCEPTION 'invalid policy advance'; END IF;
      WHEN 'retire' THEN
        IF r.from_state NOT IN('draft','planning','active','complete') OR r.to_state<>'retired' OR r.policy_version_id<>prior.policy_version_id THEN RAISE EXCEPTION 'invalid retirement transition'; END IF;
      ELSE RAISE EXCEPTION 'invalid noninitial campaign lifecycle event';
    END CASE;
  END IF;

  SELECT count(*) INTO slot_count FROM recording_campaign_v2_slots WHERE campaign_id=r.campaign_id;
  SELECT count(*),count(DISTINCT rank),
    count(*) FILTER(WHERE filled_evidence_id IS NOT NULL),
    count(*) FILTER(WHERE prospective_evidence_id IS NOT NULL),
    count(*) FILTER(WHERE disposition='vacant'),
    count(*) FILTER(WHERE disposition='replace'),
    count(*) FILTER(WHERE disposition IN('protect','probation'))
  INTO head_count,distinct_ranks,filled_count,prospective_count,vacant_count,replace_count,protected_count
  FROM recording_campaign_v2_slots s
  JOIN LATERAL (SELECT v.* FROM recording_campaign_v2_slot_versions v WHERE v.slot_id=s.id ORDER BY version DESC LIMIT 1) h ON true
  WHERE s.campaign_id=r.campaign_id;

  IF r.to_state='draft' THEN
    IF slot_count<>0 OR head_count<>0 THEN RAISE EXCEPTION 'draft campaign must have zero slots'; END IF;
  ELSIF r.to_state IN('planning','active') THEN
    IF slot_count<>p.target_count OR head_count<>p.target_count OR distinct_ranks<>p.target_count
       OR EXISTS(SELECT 1 FROM recording_campaign_v2_slots s JOIN LATERAL(SELECT v.* FROM recording_campaign_v2_slot_versions v WHERE v.slot_id=s.id ORDER BY version DESC LIMIT 1) h ON true WHERE s.campaign_id=r.campaign_id AND (h.rank<1 OR h.rank>p.target_count OR h.disposition='removed')) THEN
      RAISE EXCEPTION 'campaign current heads/ranks do not equal frozen target';
    END IF;
    IF c.kind='repair21' AND r.to_state='planning' AND NOT(filled_count=17 AND protected_count=17 AND prospective_count=4 AND vacant_count=0 AND replace_count=0) THEN
      RAISE EXCEPTION 'repair21 planning requires exact 17 filled plus four prospective';
    ELSIF c.kind='reserve' AND (filled_count+prospective_count<>p.target_count OR protected_count<>filled_count OR vacant_count<>0 OR replace_count<>0) THEN
      RAISE EXCEPTION 'reserve campaign composition mismatch';
    ELSIF r.to_state='active' AND c.kind IN('delivery30','repair21','strict50')
      AND NOT(filled_count=p.target_count AND protected_count=p.target_count AND prospective_count=0 AND vacant_count=0 AND replace_count=0) THEN
      RAISE EXCEPTION 'active campaign requires exact filled protected denominator';
    END IF;
  ELSIF r.to_state='retired' THEN
    IF head_count<>slot_count THEN RAISE EXCEPTION 'retirement cannot alter or orphan roster heads'; END IF;
  ELSE
    RAISE EXCEPTION 'unsupported campaign terminal state in R1';
  END IF;

  SELECT count(*) INTO change_count FROM recording_campaign_v2_revision_slot_changes WHERE revision_id=r.id;
  IF (r.event_type='enter_planning' AND change_count<>p.target_count)
     OR (r.event_type='roster_change' AND change_count=0)
     OR (r.event_type IN('create','activate','policy_advance','retire') AND change_count<>0) THEN
    RAISE EXCEPTION 'campaign revision slot-change cardinality mismatch';
  END IF;
  SELECT count(*) INTO bad_count FROM recording_campaign_v2_slots s
  WHERE s.created_revision_id=r.id AND NOT EXISTS(
    SELECT 1 FROM recording_campaign_v2_revision_slot_changes x
    JOIN recording_campaign_v2_slot_versions v ON v.id=x.after_version_id
    WHERE x.revision_id=r.id AND x.slot_id=s.id AND x.before_version_id IS NULL AND v.version>=1
  );
  IF bad_count<>0 OR (r.event_type='enter_planning' AND (SELECT count(*) FROM recording_campaign_v2_slots WHERE created_revision_id=r.id)<>p.target_count) THEN
    RAISE EXCEPTION 'campaign stable slot creation lacks exact revision provenance';
  END IF;
  SELECT count(*) INTO bad_count FROM recording_campaign_v2_revision_slot_changes x
  JOIN recording_campaign_v2_slot_versions a ON a.id=x.after_version_id
  WHERE x.revision_id=r.id AND (
    a.campaign_revision_id<>r.id OR a.id<>(SELECT id FROM recording_campaign_v2_slot_versions WHERE slot_id=x.slot_id ORDER BY version DESC LIMIT 1)
    OR (x.before_version_id IS NULL AND a.version<>(SELECT count(*) FROM recording_campaign_v2_slot_versions WHERE slot_id=x.slot_id AND campaign_revision_id=r.id))
    OR (x.before_version_id IS NOT NULL AND NOT EXISTS(
      SELECT 1 FROM recording_campaign_v2_slot_versions b
      WHERE b.id=x.before_version_id AND b.slot_id=x.slot_id
        AND a.version=b.version+(SELECT count(*) FROM recording_campaign_v2_slot_versions WHERE slot_id=x.slot_id AND campaign_revision_id=r.id)
    ))
  );
  IF bad_count<>0 THEN RAISE EXCEPTION 'campaign slot change does not cover a contiguous final version chain'; END IF;
  SELECT count(*) INTO bad_count FROM recording_campaign_v2_slot_versions v
  WHERE v.campaign_revision_id=r.id AND NOT EXISTS(SELECT 1 FROM recording_campaign_v2_revision_slot_changes x WHERE x.revision_id=r.id AND x.slot_id=v.slot_id);
  IF bad_count<>0 THEN RAISE EXCEPTION 'campaign revision contains an unlisted slot version'; END IF;
  SELECT count(*) INTO bad_count FROM recording_campaign_v2_slot_versions v
  WHERE v.campaign_revision_id=r.id AND v.disposition='removed'
    AND NOT EXISTS(SELECT 1 FROM recording_campaign_v2_slot_versions n WHERE n.predecessor_version_id=v.id AND n.campaign_revision_id=r.id);
  IF bad_count<>0 THEN RAISE EXCEPTION 'removed is allowed only as a superseded historical version'; END IF;

  SELECT count(*) INTO bad_count FROM recording_campaign_v2_filled_evidence e WHERE e.created_revision_id=r.id
    AND NOT EXISTS(SELECT 1 FROM recording_campaign_v2_slot_versions v WHERE v.campaign_revision_id=r.id AND v.filled_evidence_id=e.id);
  bad_count:=bad_count+(SELECT count(*) FROM recording_campaign_v2_prospective_evidence e WHERE e.created_revision_id=r.id
    AND NOT EXISTS(SELECT 1 FROM recording_campaign_v2_slot_versions v WHERE v.campaign_revision_id=r.id AND v.prospective_evidence_id=e.id));
  IF bad_count<>0 THEN RAISE EXCEPTION 'campaign revision contains orphan decision evidence'; END IF;

  expected_roster:=recording_campaign_v2_roster_digest(r.campaign_id);
  expected_plan:=recording_campaign_v2_plan_digest(r.id);
  IF seal.roster_sha256 IS DISTINCT FROM expected_roster OR r.plan_sha256 IS DISTINCT FROM expected_plan
     OR r.request_sha256 IS DISTINCT FROM recording_campaign_v2_request_digest(r.id)
     OR (seal.filled_count,seal.prospective_count,seal.vacant_count,seal.replace_count,seal.protected_count)
       IS DISTINCT FROM (filled_count,prospective_count,vacant_count,replace_count,protected_count) THEN
    RAISE EXCEPTION 'campaign seal digest/count projection mismatch';
  END IF;
  SELECT count(*) INTO idem_count FROM recording_campaign_v2_idempotency i WHERE i.revision_id=r.id
    AND (i.account_id,i.campaign_id,i.idempotency_key,i.request_sha256,i.plan_sha256)
      IS NOT DISTINCT FROM (r.account_id,r.campaign_id,r.idempotency_key,r.request_sha256,r.plan_sha256);
  IF idem_count<>1 THEN RAISE EXCEPTION 'campaign revision requires exact immutable idempotency record'; END IF;
  PERFORM validate_recording_campaign_v2_occupancy();
END $$;

CREATE FUNCTION validate_recording_campaign_v2_deferred() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE j JSONB:=COALESCE(to_jsonb(NEW),to_jsonb(OLD)); target BIGINT; campaign BIGINT;
BEGIN
  target:=COALESCE(NULLIF(j->>'revision_id','')::bigint,NULLIF(j->>'campaign_revision_id','')::bigint,NULLIF(j->>'created_revision_id','')::bigint);
  campaign:=NULLIF(j->>'campaign_id','')::bigint;
  IF target IS NULL AND TG_TABLE_NAME='recording_campaign_v2_revisions' THEN
    target:=(j->>'id')::bigint;
  ELSIF target IS NULL AND TG_TABLE_NAME='recording_campaign_v2_policy_versions' THEN
    SELECT id INTO target FROM recording_campaign_v2_revisions WHERE policy_version_id=(j->>'id')::bigint ORDER BY revision_number DESC LIMIT 1;
  ELSIF target IS NULL AND TG_TABLE_NAME='recording_campaign_v2_campaigns' THEN
    SELECT id INTO target FROM recording_campaign_v2_revisions WHERE campaign_id=(j->>'id')::bigint ORDER BY revision_number DESC LIMIT 1;
  END IF;
  IF target IS NULL THEN RAISE EXCEPTION 'campaign authority row is orphaned from a revision'; END IF;
  PERFORM validate_recording_campaign_v2_revision(target);
  RETURN NULL;
END $$;

DO $$
DECLARE table_name TEXT;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'recording_campaign_v2_campaigns','recording_campaign_v2_policy_versions',
    'recording_campaign_v2_revisions','recording_campaign_v2_revision_commits',
    'recording_campaign_v2_slots','recording_campaign_v2_filled_evidence',
    'recording_campaign_v2_prospective_evidence','recording_campaign_v2_slot_versions',
    'recording_campaign_v2_revision_slot_changes','recording_campaign_v2_idempotency'
  ] LOOP
    EXECUTE format('CREATE CONSTRAINT TRIGGER %I_deferred_validate AFTER INSERT ON %I DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION validate_recording_campaign_v2_deferred()',table_name,table_name);
  END LOOP;
END $$;

CREATE FUNCTION validate_recording_campaign_v2_occupancy_deferred() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN PERFORM validate_recording_campaign_v2_occupancy(); RETURN NULL; END $$;
CREATE CONSTRAINT TRIGGER recording_campaign_v2_recording_occupancy_validate
AFTER INSERT OR UPDATE OR DELETE ON recording_campaign_v2_recording_occupancy DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_recording_campaign_v2_occupancy_deferred();
CREATE CONSTRAINT TRIGGER recording_campaign_v2_scene_occupancy_validate
AFTER INSERT OR UPDATE OR DELETE ON recording_campaign_v2_scene_occupancy DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_recording_campaign_v2_occupancy_deferred();
CREATE TRIGGER recording_campaign_v2_recording_occupancy_no_truncate
BEFORE TRUNCATE ON recording_campaign_v2_recording_occupancy FOR EACH STATEMENT
EXECUTE FUNCTION reject_recording_campaign_v2_mutation();
CREATE TRIGGER recording_campaign_v2_scene_occupancy_no_truncate
BEFORE TRUNCATE ON recording_campaign_v2_scene_occupancy FOR EACH STATEMENT
EXECUTE FUNCTION reject_recording_campaign_v2_mutation();

-- Preserve the stable protection interface and its OID/owner/ACL/options while
-- adding only sealed v2 heads. The set UNION intentionally deduplicates a
-- recording protected by more than one explicitly compatible campaign.
DO $$
DECLARE
  ns TEXT:=current_schema(); view_oid OID; post_oid OID; server_num INTEGER;
  old_owner OID; new_owner OID; old_acl ACLITEM[]; new_acl ACLITEM[];
  old_options TEXT[]; new_options TEXT[]; old_comment TEXT; new_comment TEXT;
  old_dependents TEXT[]; new_dependents TEXT[];
BEGIN
  server_num:=current_setting('server_version_num')::integer;
  IF server_num<150000 THEN RAISE EXCEPTION 'campaign roster v2 requires inspectable PostgreSQL 15+ view security options'; END IF;
  SELECT c.oid,c.relowner,c.relacl,c.reloptions,obj_description(c.oid),
    ARRAY(SELECT d.classid::regclass::text||':'||d.objid::text||':'||d.objsubid::text||':'||d.deptype
          FROM pg_depend d WHERE d.refobjid=c.oid
            AND NOT(d.classid='pg_rewrite'::regclass AND d.objid IN(SELECT rw.oid FROM pg_rewrite rw WHERE rw.ev_class=c.oid))
          ORDER BY 1)
  INTO view_oid,old_owner,old_acl,old_options,old_comment,old_dependents
  FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE n.nspname=ns AND c.relname='protected_campaign_recordings' AND c.relkind='v';
  IF view_oid IS NULL THEN RAISE EXCEPTION 'legacy protected_campaign_recordings view is missing'; END IF;
  EXECUTE format($view$
    CREATE OR REPLACE VIEW %1$I.protected_campaign_recordings AS
    SELECT t.account_id,e.recording_id
    FROM %1$I.recording_campaign_tracks t
    JOIN %1$I.recording_campaign_roster_entries e ON e.track_id=t.id
    WHERE t.state IN('active','complete') AND e.status IN('protect','probation')
    UNION
    SELECT r.account_id,h.recording_id
    FROM %1$I.recording_campaign_v2_current_revisions r
    JOIN %1$I.recording_campaign_v2_current_slot_heads h
      ON h.campaign_id=r.campaign_id AND h.account_id=r.account_id
    WHERE r.state IN('active','complete') AND h.recording_id IS NOT NULL
      AND h.disposition IN('protect','probation')
  $view$,ns);
  SELECT c.oid,c.relowner,c.relacl,c.reloptions,obj_description(c.oid),
    ARRAY(SELECT d.classid::regclass::text||':'||d.objid::text||':'||d.objsubid::text||':'||d.deptype
          FROM pg_depend d WHERE d.refobjid=c.oid
            AND NOT(d.classid='pg_rewrite'::regclass AND d.objid IN(SELECT rw.oid FROM pg_rewrite rw WHERE rw.ev_class=c.oid))
          ORDER BY 1)
  INTO post_oid,new_owner,new_acl,new_options,new_comment,new_dependents
  FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE n.nspname=ns AND c.relname='protected_campaign_recordings' AND c.relkind='v';
  IF (post_oid,new_owner,new_acl,new_options,new_comment,new_dependents)
    IS DISTINCT FROM (view_oid,old_owner,old_acl,old_options,old_comment,old_dependents) THEN
    RAISE EXCEPTION 'protected campaign view metadata or dependent identity changed';
  END IF;
END $$;

-- PL/pgSQL and SQL functions otherwise resolve unqualified relations against
-- the caller's search_path. Pin every R1 function (plus the shared C1 lock we
-- call) to the one controlled migration schema and pg_catalog.
DO $$
DECLARE ns TEXT:=current_schema(); fn RECORD;
BEGIN
  FOR fn IN
    SELECT p.oid::regprocedure::text AS signature
    FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
    WHERE n.nspname=ns AND (
      p.proname LIKE 'recording_campaign_v2_%'
      OR p.proname LIKE 'validate_recording_campaign_v2_%'
      OR p.proname IN(
        'reject_recording_campaign_v2_mutation','freeze_stream_source_v2_baseline',
        'validate_stream_source_revision_v2_append','enforce_recording_campaign_v2_source_stream_fence',
        'recording_campaign_v2_fence_presentation_admission',
        'fence_recording_presentation_job_against_campaign_v2',
        'lock_recording_campaign_protection'))
    ORDER BY p.oid
  LOOP
    EXECUTE format('ALTER FUNCTION %s SET search_path TO %I,pg_catalog',fn.signature,ns);
  END LOOP;
END $$;
