-- Separate from NAS-local tasks so an older API cannot lease Durham work.
-- The controller supplies the reviewed input set and enables it separately.
CREATE TYPE nas_stitch_r2_hour_state AS ENUM ('pending','leased','staged','held','certified');
CREATE TABLE nas_stitch_r2_hours (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 scope_id bigint NOT NULL REFERENCES nas_stitch_scopes(id),
 recording_id bigint NOT NULL REFERENCES recordings(id),
 hour_at timestamptz NOT NULL CHECK(hour_at=date_trunc('hour',hour_at,'UTC')),
 plan jsonb NOT NULL CHECK(jsonb_typeof(plan)='object'),
 plan_sha256 text NOT NULL CHECK(plan_sha256 ~ '^[a-f0-9]{64}$'),
 state nas_stitch_r2_hour_state NOT NULL DEFAULT 'pending',
 attempt integer NOT NULL DEFAULT 0 CHECK(attempt>=0),
 lease_api_key_id bigint REFERENCES account_api_keys(id),
 lease_token text CHECK(lease_token ~ '^[a-f0-9]{64}$'),
 lease_expires_at timestamptz,
 UNIQUE(recording_id,hour_at),
 CHECK(state<>'leased' OR (attempt>0 AND lease_api_key_id IS NOT NULL
       AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL))
);

CREATE TRIGGER nas_stitch_r2_hour_plan_immutable
 BEFORE UPDATE OF scope_id,recording_id,hour_at,plan,plan_sha256 OR DELETE ON nas_stitch_r2_hours
 FOR EACH ROW EXECUTE FUNCTION guard_nas_stitch_evidence();

CREATE TABLE nas_stitch_r2_input_holds (
 hour_id bigint NOT NULL REFERENCES nas_stitch_r2_hours(id),
 bucket text NOT NULL CHECK(bucket<>''),
 object_key text NOT NULL CHECK(object_key LIKE 'managed/%'),
 catalog_clip_id bigint NOT NULL REFERENCES recording_clips(id),
 sha256 text NOT NULL CHECK(sha256 ~ '^[a-f0-9]{64}$'),
 size_bytes bigint NOT NULL CHECK(size_bytes>0),
 etag text NOT NULL CHECK(etag<>''),
 released_at timestamptz,
 PRIMARY KEY(hour_id,catalog_clip_id)
);
CREATE INDEX nas_stitch_r2_input_holds_live_key ON nas_stitch_r2_input_holds(bucket,object_key)
 WHERE released_at IS NULL;

CREATE TABLE nas_stitch_r2_certificates (
 hour_id bigint PRIMARY KEY REFERENCES nas_stitch_r2_hours(id),
 connection_id bigint NOT NULL REFERENCES connections(id) CHECK(connection_id=13),
 attempt integer NOT NULL CHECK(attempt>0),
 plan_sha256 text NOT NULL CHECK(plan_sha256 ~ '^[a-f0-9]{64}$'),
 manifest_sha256 text NOT NULL CHECK(manifest_sha256 ~ '^[a-f0-9]{64}$'),
 receipt jsonb NOT NULL CHECK(jsonb_typeof(receipt)='object'),
 certified_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER nas_stitch_r2_certificate_immutable BEFORE UPDATE OR DELETE ON nas_stitch_r2_certificates
 FOR EACH ROW EXECUTE FUNCTION guard_nas_stitch_evidence();

-- Inverse apply owns the outer lock on a separate session. Its transactions
-- take the shared import lock rather than waiting on their own outer lock.
-- This also serializes independently issued legacy SQL with a bulk import.
CREATE FUNCTION guard_nas_stitch_r2_hold() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'stitch input hold is durable'; END IF;
 IF TG_OP='UPDATE' THEN
  IF ROW(NEW.hour_id,NEW.bucket,NEW.object_key,NEW.catalog_clip_id,NEW.sha256,NEW.size_bytes,NEW.etag)
     IS DISTINCT FROM ROW(OLD.hour_id,OLD.bucket,OLD.object_key,OLD.catalog_clip_id,OLD.sha256,OLD.size_bytes,OLD.etag)
   OR OLD.released_at IS NOT NULL OR NEW.released_at IS NULL
   OR NOT EXISTS(SELECT 1 FROM nas_stitch_r2_certificates c
    WHERE c.hour_id=OLD.hour_id AND c.certified_at=NEW.released_at) THEN
   RAISE EXCEPTION 'stitch input release requires the exact durable certificate';
  END IF;
  RETURN NEW;
 END IF;
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION 'stitch hold admission requires read committed' USING ERRCODE='55000';
 END IF;
 PERFORM pg_advisory_xact_lock(7343226011173);
 PERFORM pg_advisory_xact_lock(7343226011189);
 IF NEW.released_at IS NOT NULL OR NOT EXISTS(
  SELECT 1 FROM nas_stitch_r2_hours h
  JOIN nas_stitch_scopes s ON s.id=h.scope_id
  JOIN recordings r ON r.id=h.recording_id AND r.account_id=47 AND r.stream_id=s.stream_id
  JOIN recording_clips c ON c.id=NEW.catalog_clip_id AND c.recording_id=r.id
  JOIN nas_inventory_files f ON f.connection_id=13 AND f.clip_id=c.id AND f.state='present'
  WHERE h.id=NEW.hour_id AND h.state='pending' AND h.attempt=0 AND c.purged_at IS NULL
   AND c.bucket=NEW.bucket AND c.object_key=NEW.object_key AND c.etag=NEW.etag
   AND lower(c.sha256)=NEW.sha256 AND c.size_bytes=NEW.size_bytes
   AND f.sha256=NEW.sha256 AND f.size_bytes=NEW.size_bytes AND f.relative_path=c.display_path
   AND date_trunc('hour',c.clip_start_at,'UTC')=h.hour_at
   AND c.clip_start_at>=s.start_at AND c.clip_end_at<=s.end_at
 ) THEN
  RAISE EXCEPTION 'stitch input hold lacks exact R2 and NAS catalog identity';
 END IF;
 IF EXISTS(SELECT 1 FROM r2_retirement_reservations r WHERE r.bucket=NEW.bucket AND r.object_key=NEW.object_key)
  OR EXISTS(SELECT 1 FROM r2_inverse_intents r WHERE r.bucket=NEW.bucket AND r.object_key=NEW.object_key)
  OR EXISTS(SELECT 1 FROM r2_keep28_intents r WHERE r.bucket=NEW.bucket AND r.object_key=NEW.object_key)
  OR EXISTS(SELECT 1 FROM r2_object_retirements r WHERE r.bucket=NEW.bucket AND r.object_key=NEW.object_key) THEN
  RAISE EXCEPTION 'stitch input already has R2 retirement history';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER nas_stitch_r2_input_hold_guard BEFORE INSERT OR UPDATE OR DELETE ON nas_stitch_r2_input_holds
 FOR EACH ROW EXECUTE FUNCTION guard_nas_stitch_r2_hold();

CREATE FUNCTION guard_r2_retirement_stitch_input() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF current_setting('transaction_isolation')<>'read committed'
  OR NOT pg_try_advisory_xact_lock_shared(7343226011189) THEN
  RAISE EXCEPTION 'stitch input hold admission is busy' USING ERRCODE='55P03';
 END IF;
 IF EXISTS(SELECT 1 FROM nas_stitch_r2_input_holds h
  WHERE h.bucket=NEW.bucket AND h.object_key=NEW.object_key AND h.released_at IS NULL) THEN
  RAISE EXCEPTION 'active R2 stitch input hold' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END $$;

DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['r2_retirement_reservations','r2_object_retirements',
  'r2_inverse_intents','r2_inverse_attempts','r2_keep28_intents','r2_keep28_attempts'] LOOP
  EXECUTE format('CREATE TRIGGER r2_stitch_input_hold_guard BEFORE INSERT ON %I'
    ' FOR EACH ROW EXECUTE FUNCTION guard_r2_retirement_stitch_input()',t);
 END LOOP;
END $$;

-- This is the only writer class omitted from the legacy retirement predicate.
-- Its keys cannot overlap managed footage, and no legacy writer changes class.
CREATE TYPE nas_stitch_r2_stage_state AS ENUM ('starting','uploading','completing','staged','aborted');
CREATE TABLE nas_stitch_r2_stage_objects (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 hour_id bigint NOT NULL REFERENCES nas_stitch_r2_hours(id),
 attempt integer NOT NULL CHECK(attempt>0),
 api_key_id bigint NOT NULL REFERENCES account_api_keys(id),
 bucket text NOT NULL CHECK(bucket<>''),
 endpoint_sha256 text NOT NULL CHECK(endpoint_sha256 ~ '^[a-f0-9]{64}$'),
 generation uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
 object_key text GENERATED ALWAYS AS ('stitch-staging/'||generation::text||'/'||sha256||'.mp4') STORED,
 relative_path text NOT NULL CHECK(relative_path<>'' AND starts_with(relative_path,'/')=false),
 sha256 text NOT NULL CHECK(sha256 ~ '^[a-f0-9]{64}$'),
 size_bytes bigint NOT NULL CHECK(size_bytes BETWEEN 1 AND 2750000000),
 state nas_stitch_r2_stage_state NOT NULL DEFAULT 'starting',
 upload_id text CHECK(upload_id<>''),
 etag text CHECK(etag<>''),
 version_id text CHECK(version_id<>''),
 last_modified timestamptz,
 grants_drain_at timestamptz,
 UNIQUE(hour_id,attempt,relative_path), UNIQUE(bucket,object_key),
 CHECK(state NOT IN ('uploading','completing','staged') OR upload_id IS NOT NULL),
 CHECK(state<>'staged' OR (etag IS NOT NULL AND last_modified IS NOT NULL))
);
CREATE INDEX nas_stitch_r2_active_multipart ON nas_stitch_r2_stage_objects(bucket,object_key,upload_id)
 WHERE state IN ('uploading','completing');

CREATE FUNCTION guard_nas_stitch_r2_stage() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'stitch staging identity is durable'; END IF;
 IF TG_OP='UPDATE' THEN
  IF ROW(NEW.hour_id,NEW.attempt,NEW.api_key_id,NEW.bucket,NEW.endpoint_sha256,NEW.generation,
     NEW.relative_path,NEW.sha256,NEW.size_bytes)
     IS DISTINCT FROM ROW(OLD.hour_id,OLD.attempt,OLD.api_key_id,OLD.bucket,OLD.endpoint_sha256,OLD.generation,
     OLD.relative_path,OLD.sha256,OLD.size_bytes)
   OR (OLD.upload_id IS NOT NULL AND NEW.upload_id IS DISTINCT FROM OLD.upload_id)
   OR (OLD.grants_drain_at IS NOT NULL AND (NEW.grants_drain_at IS NULL OR NEW.grants_drain_at<OLD.grants_drain_at))
   OR NOT ((NEW.state=OLD.state AND OLD.state IN ('starting','uploading','completing'))
    OR (OLD.state='starting' AND NEW.state IN ('uploading','aborted'))
    OR (OLD.state='uploading' AND NEW.state IN ('completing','aborted'))
    OR (OLD.state='completing' AND NEW.state IN ('staged','aborted'))) THEN
   RAISE EXCEPTION 'stitch staging attempt or generation changed';
  END IF;
  RETURN NEW;
 END IF;
 PERFORM pg_advisory_xact_lock_shared(7343226011187);
 IF NEW.state<>'starting' OR NEW.upload_id IS NOT NULL OR NEW.etag IS NOT NULL
  OR NEW.version_id IS NOT NULL OR NEW.last_modified IS NOT NULL OR NEW.grants_drain_at IS NOT NULL
  OR NOT EXISTS(SELECT 1 FROM r2_retirement_writer_fences f
   WHERE f.bucket=NEW.bucket AND f.endpoint_sha256=NEW.endpoint_sha256)
  OR NOT EXISTS(SELECT 1 FROM nas_stitch_r2_hours h JOIN account_api_keys k ON k.id=h.lease_api_key_id
   WHERE h.id=NEW.hour_id AND h.state='leased' AND h.attempt=NEW.attempt
    AND h.lease_api_key_id=NEW.api_key_id AND h.lease_expires_at>clock_timestamp()
    AND k.revoked_at IS NULL AND (k.expires_at IS NULL OR k.expires_at>clock_timestamp())) THEN
  RAISE EXCEPTION 'R2 stitch staging requires its exact active writer attempt';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER nas_stitch_r2_stage_guard BEFORE INSERT OR UPDATE OR DELETE ON nas_stitch_r2_stage_objects
 FOR EACH ROW EXECUTE FUNCTION guard_nas_stitch_r2_stage();

CREATE FUNCTION guard_r2_stitch_fence_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT pg_try_advisory_xact_lock(7343226011187)
  OR EXISTS(SELECT 1 FROM nas_stitch_r2_stage_objects
   WHERE state IN ('starting','uploading','completing') OR grants_drain_at>=clock_timestamp()) THEN
  RAISE EXCEPTION 'R2 stitch staging writers have not drained';
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER r2_stitch_fence_transition BEFORE INSERT OR UPDATE OR DELETE ON r2_retirement_writer_fences
 FOR EACH ROW EXECUTE FUNCTION guard_r2_stitch_fence_transition();

ALTER TABLE nas_stitch_r2_hours ADD COLUMN staged_manifest jsonb CHECK(jsonb_typeof(staged_manifest)='object'),
 ADD COLUMN staged_manifest_sha256 text CHECK(staged_manifest_sha256 ~ '^[a-f0-9]{64}$');
ALTER TABLE nas_stitch_r2_hours ADD CHECK(state NOT IN ('staged','certified')
 OR (attempt>0 AND staged_manifest IS NOT NULL AND staged_manifest_sha256 IS NOT NULL));

CREATE FUNCTION add_nas_stitch_r2_certificate() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expected jsonb; covered boolean;
BEGIN
 PERFORM pg_advisory_xact_lock(7343226011173);
 SELECT jsonb_agg(jsonb_build_object('object_key',o.object_key,'etag',o.etag,'version_id',o.version_id,
    'last_modified',o.last_modified,'readback',q.stitch_receipt) ORDER BY o.id),
  bool_and(COALESCE(q.state='verified' AND q.stitch_receipt IS NOT NULL AND q.manifest_sha256=NEW.manifest_sha256,false))
 INTO expected,covered
 FROM nas_stitch_r2_stage_objects o LEFT JOIN nas_r2_copy_items q
  ON q.connection_id=13 AND q.kind='stitch_output' AND q.source_id=o.id
 WHERE o.hour_id=NEW.hour_id AND o.attempt=NEW.attempt AND o.state='staged';
 IF NOT COALESCE(covered,false) OR NEW.receipt IS DISTINCT FROM jsonb_build_object('objects',expected) THEN
  RAISE EXCEPTION 'NAS stitch certificate lacks exact pulled output coverage';
 END IF;
 UPDATE nas_stitch_r2_hours SET state='certified'
 WHERE id=NEW.hour_id AND state='staged' AND attempt=NEW.attempt
  AND plan_sha256=NEW.plan_sha256 AND staged_manifest_sha256=NEW.manifest_sha256;
 IF NOT FOUND THEN RAISE EXCEPTION 'NAS stitch certificate does not match staged result'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER nas_stitch_r2_certificate_admission BEFORE INSERT ON nas_stitch_r2_certificates
 FOR EACH ROW EXECUTE FUNCTION add_nas_stitch_r2_certificate();

CREATE FUNCTION release_nas_stitch_r2_inputs() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 UPDATE nas_stitch_r2_input_holds SET released_at=NEW.certified_at
 WHERE hour_id=NEW.hour_id AND released_at IS NULL;
 IF NOT FOUND THEN RAISE EXCEPTION 'NAS stitch certificate has no input holds'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER nas_stitch_r2_certificate_release AFTER INSERT ON nas_stitch_r2_certificates
 FOR EACH ROW EXECUTE FUNCTION release_nas_stitch_r2_inputs();

CREATE TABLE nas_stitch_r2_control (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 enabled boolean NOT NULL DEFAULT false,
 max_staging_bytes bigint NOT NULL DEFAULT 0 CHECK(max_staging_bytes>=0)
);
INSERT INTO nas_stitch_r2_control DEFAULT VALUES;

ALTER TABLE nas_stitch_r2_stage_objects ADD COLUMN start_issued_at timestamptz;
CREATE TABLE nas_stitch_r2_parts (
 object_id bigint NOT NULL REFERENCES nas_stitch_r2_stage_objects(id),
 part_number integer NOT NULL CHECK(part_number BETWEEN 1 AND 164),
 sha256 text NOT NULL CHECK(sha256 ~ '^[a-f0-9]{64}$'),
 PRIMARY KEY(object_id,part_number)
);
CREATE TRIGGER nas_stitch_r2_part_immutable BEFORE UPDATE OR DELETE ON nas_stitch_r2_parts
 FOR EACH ROW EXECUTE FUNCTION guard_nas_stitch_evidence();

ALTER TABLE nas_r2_copy_items DROP CONSTRAINT nas_r2_copy_items_kind_check,
 ADD CHECK(kind IN ('raw','collated','stitch_output'));
ALTER TABLE nas_r2_copy_items ADD COLUMN stitch_receipt jsonb CHECK(
 stitch_receipt IS NULL OR (kind='stitch_output' AND state='verified' AND jsonb_typeof(stitch_receipt)='object'));

-- A server rollback may still authenticate the dedicated key. Once the R2
-- phase is live it cannot use the old claim path to receive a NAS-local plan.
CREATE FUNCTION guard_nas_stitch_r2_phase() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.state='leased' AND NEW.lease_token IS DISTINCT FROM OLD.lease_token AND
  EXISTS(SELECT 1 FROM nas_stitch_r2_control WHERE singleton AND enabled) AND
  (NEW.lease_api_key_id IS NOT NULL OR EXISTS(SELECT 1 FROM nas_stitch_r2_hours WHERE state<>'certified')) THEN
  RAISE EXCEPTION 'Durham R2 phase excludes NAS-local claims';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER nas_stitch_r2_phase_guard BEFORE UPDATE OF lease_token ON nas_stitch_tasks
 FOR EACH ROW EXECUTE FUNCTION guard_nas_stitch_r2_phase();

CREATE TABLE nas_stitch_r2_stage_retirements (
 object_id bigint PRIMARY KEY REFERENCES nas_stitch_r2_stage_objects(id),
 issued_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 absence_observed_at timestamptz,
 CHECK(absence_observed_at IS NULL OR absence_observed_at>=issued_at)
);
