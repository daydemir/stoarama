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
 input_bytes bigint NOT NULL CHECK(input_bytes BETWEEN 1 AND 2500000000),
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
 BEFORE UPDATE OF scope_id,recording_id,hour_at,plan,plan_sha256,input_bytes OR DELETE ON nas_stitch_r2_hours
 FOR EACH ROW EXECUTE FUNCTION guard_nas_stitch_evidence();

CREATE INDEX nas_stitch_r2_hour_queue_order ON nas_stitch_r2_hours(input_bytes DESC,hour_at,id)
 WHERE state IN ('pending','leased');

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

CREATE FUNCTION nas_stitch_r2_has_exact_input_set(hour_id bigint) RETURNS boolean
LANGUAGE sql STABLE AS $$
 SELECT EXISTS (
  SELECT 1 FROM nas_stitch_r2_hours h
  JOIN nas_stitch_scopes s ON s.id=h.scope_id
  JOIN recordings r ON r.id=h.recording_id AND r.stream_id=s.stream_id AND r.account_id=47
  WHERE h.id=$1 AND h.hour_at>=s.start_at AND h.hour_at+interval '1 hour'<=s.end_at
   AND NOT EXISTS(SELECT 1 FROM recording_clips c WHERE c.recording_id=h.recording_id
    AND c.clip_start_at<h.hour_at AND c.clip_end_at>h.hour_at)
   AND (SELECT count(*) FROM recording_clips c WHERE c.recording_id=h.recording_id
    AND c.clip_start_at>=h.hour_at AND c.clip_start_at<h.hour_at+interval '1 hour')
     = jsonb_array_length(h.plan->'clips')
   AND (SELECT count(DISTINCT (p->>'source_id')::bigint) FROM jsonb_array_elements(h.plan->'clips') p
    WHERE jsonb_typeof(p->'source_id')='number')=jsonb_array_length(h.plan->'clips')
   AND EXISTS(SELECT 1 FROM nas_stitch_r2_input_holds i WHERE i.hour_id=h.id AND i.released_at IS NULL
    HAVING count(*)=jsonb_array_length(h.plan->'clips') AND sum(i.size_bytes)=h.input_bytes)
   AND NOT EXISTS(
    SELECT 1 FROM jsonb_array_elements(h.plan->'clips') p
    WHERE NOT EXISTS(
     SELECT 1 FROM nas_stitch_r2_input_holds i
     JOIN recording_clips c ON c.id=i.catalog_clip_id AND c.bucket=i.bucket AND c.object_key=i.object_key
     JOIN r2_retirement_writer_fences fence ON fence.bucket=c.bucket
     JOIN nas_inventory_files f ON f.connection_id=13 AND f.clip_id=c.id AND f.state='present'
     WHERE i.hour_id=h.id AND i.catalog_clip_id=(p->>'source_id')::bigint AND i.released_at IS NULL
      AND c.recording_id=h.recording_id AND c.clip_start_at>=h.hour_at
      AND c.clip_start_at<h.hour_at+interval '1 hour' AND c.clip_end_at<=h.hour_at+interval '1 hour'
      AND c.clip_start_at=(p->>'start_at')::timestamptz AND c.clip_end_at=(p->>'end_at')::timestamptz
      AND c.display_path=p->>'relative_path' AND c.purged_at IS NULL AND c.etag=i.etag
      AND encode(sha256(convert_to(rtrim(c.endpoint,'/'),'UTF8')),'hex')=fence.endpoint_sha256
      AND lower(c.sha256)=i.sha256 AND c.size_bytes=i.size_bytes
      AND f.relative_path=c.display_path AND f.size_bytes=i.size_bytes AND lower(f.sha256)=i.sha256
      AND i.sha256=p->>'sha256' AND i.size_bytes=(p->>'size_bytes')::bigint
    )
   )
 )
$$;

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

CREATE FUNCTION guard_nas_stitch_r2_truncate() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 RAISE EXCEPTION 'R2 stitch evidence is durable' USING ERRCODE='55000';
END $$;
CREATE TRIGGER nas_stitch_r2_input_hold_truncate BEFORE TRUNCATE ON nas_stitch_r2_input_holds
 FOR EACH STATEMENT EXECUTE FUNCTION guard_nas_stitch_r2_truncate();

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
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION 'R2 stitch fence transition requires READ COMMITTED' USING ERRCODE='55000';
 END IF;
 IF NOT pg_try_advisory_xact_lock(7343226011187)
  OR EXISTS(SELECT 1 FROM nas_stitch_r2_stage_objects
   WHERE state IN ('starting','uploading','completing') OR grants_drain_at>=clock_timestamp())
  OR EXISTS(SELECT 1 FROM nas_stitch_r2_stage_retirements WHERE absence_observed_at IS NULL) THEN
  RAISE EXCEPTION 'R2 stitch staging writers or unresolved retirements have not drained';
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
CREATE TRIGGER nas_stitch_r2_manifest_immutable
 BEFORE UPDATE OF staged_manifest,staged_manifest_sha256 ON nas_stitch_r2_hours
 FOR EACH ROW WHEN (OLD.staged_manifest IS NOT NULL AND
  ROW(OLD.staged_manifest,OLD.staged_manifest_sha256) IS DISTINCT FROM ROW(NEW.staged_manifest,NEW.staged_manifest_sha256))
 EXECUTE FUNCTION guard_nas_stitch_evidence();

ALTER TABLE nas_r2_copy_items DROP CONSTRAINT nas_r2_copy_items_kind_check,
 ADD CHECK(kind IN ('raw','collated','stitch_output'));
ALTER TABLE nas_r2_copy_items ADD COLUMN stitch_receipt jsonb CHECK(
 stitch_receipt IS NULL OR (kind='stitch_output' AND state='verified' AND jsonb_typeof(stitch_receipt)='object')),
 ADD COLUMN stitch_result_sha256 text CHECK(stitch_result_sha256 ~ '^[a-f0-9]{64}$'),
 ADD COLUMN stitch_read_grants_until timestamptz CHECK(kind='stitch_output' OR stitch_read_grants_until IS NULL);

CREATE FUNCTION guard_nas_r2_stitch_copy_evidence() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.kind='stitch_output' AND (TG_OP='DELETE' OR OLD.state='verified') THEN
  RAISE EXCEPTION 'NAS stitch copy evidence is durable' USING ERRCODE='55000';
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 IF OLD.stitch_read_grants_until IS NOT NULL AND
  (NEW.stitch_read_grants_until IS NULL OR NEW.stitch_read_grants_until<OLD.stitch_read_grants_until) THEN
  RAISE EXCEPTION 'NAS stitch read grant drain cannot regress' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER nas_r2_stitch_copy_evidence_guard BEFORE UPDATE OR DELETE ON nas_r2_copy_items
 FOR EACH ROW EXECUTE FUNCTION guard_nas_r2_stitch_copy_evidence();

CREATE FUNCTION nas_stitch_r2_exact_copy_receipt(q nas_r2_copy_items, o nas_stitch_r2_stage_objects) RETURNS boolean
LANGUAGE sql IMMUTABLE AS $$
 SELECT COALESCE(q.kind='stitch_output' AND q.state='verified' AND q.connection_id=13 AND q.source_id=o.id
  AND q.etag=o.etag AND coalesce(q.version_id,'')=coalesce(o.version_id,'')
  AND q.stitch_result_sha256 IS NOT NULL
  AND ((q.completion='downloaded' AND q.committed_bytes=o.size_bytes)
       OR (q.completion='verified_in_place' AND q.committed_bytes=0))
  AND q.item @> jsonb_build_object('kind','stitch_output','source_id',o.id,'bucket',o.bucket,'object_key',o.object_key,
   'relative_path',o.relative_path,'size_bytes',o.size_bytes,'sha256',o.sha256)
  AND q.stitch_receipt @> jsonb_build_object('relative_path',o.relative_path,'size_bytes',o.size_bytes,'sha256',o.sha256,'fsynced',true)
  AND CASE WHEN jsonb_typeof(q.stitch_receipt->'identity')='array' THEN
   jsonb_array_length(q.stitch_receipt->'identity')=5
   AND q.stitch_receipt#>'{identity,0}'=to_jsonb(o.size_bytes)
   AND q.stitch_receipt#>>'{identity,1}' ~ '^[1-9][0-9]{0,18}$'
   AND q.stitch_receipt#>>'{identity,2}' ~ '^[1-9][0-9]{0,18}$'
   AND q.stitch_receipt#>>'{identity,3}' ~ '^[1-9][0-9]{0,18}$'
   AND q.stitch_receipt#>>'{identity,4}' ~ '^[0-9]{1,19}$'
   AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(q.stitch_receipt->'identity') member
    WHERE jsonb_typeof(member)<>'number')
  ELSE false END, false);
$$;

CREATE FUNCTION nas_stitch_r2_certificate_receipt(hour_id bigint, attempt_number integer, plan_digest text, manifest_digest text)
RETURNS jsonb LANGUAGE sql STABLE AS $$
 WITH hour AS (
  SELECT h.* FROM nas_stitch_r2_hours h WHERE h.id=$1 AND h.attempt=$2 AND h.state='staged'
   AND h.plan_sha256=$3 AND h.staged_manifest_sha256=$4
   AND h.staged_manifest->>'plan_sha256'=$3 AND h.staged_manifest->>'lease_token'=h.lease_token
   AND h.staged_manifest->>'policy'=h.plan->>'policy' AND h.staged_manifest->>'error'=''
   AND jsonb_typeof(h.staged_manifest->'seams') IN ('array','null')
   AND jsonb_array_length(CASE WHEN jsonb_typeof(h.staged_manifest->'seams')='array'
    THEN h.staged_manifest->'seams' ELSE '[]'::jsonb END)=jsonb_array_length(h.plan->'clips')-1
   AND h.staged_manifest->'left_out' IN ('[]'::jsonb,'null'::jsonb)
   AND h.staged_manifest->'repairs' IN ('[]'::jsonb,'null'::jsonb)
   AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(CASE WHEN jsonb_typeof(h.staged_manifest->'seams')='array'
    THEN h.staged_manifest->'seams' ELSE '[]'::jsonb END) seam
    WHERE COALESCE(seam->>'decision','') NOT IN ('join','split'))
 ), outputs AS (
  SELECT value AS entry FROM hour h
  CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(h.staged_manifest->'outputs')='array'
   THEN h.staged_manifest->'outputs' ELSE '[]'::jsonb END)
 ), inputs AS (
  SELECT value AS proof FROM outputs
  CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(entry->'inputs')='array'
   THEN entry->'inputs' ELSE '[]'::jsonb END)
 )
 SELECT jsonb_build_object('objects',jsonb_agg(jsonb_build_object('object_key',o.object_key,'etag',o.etag,
  'version_id',o.version_id,'last_modified',o.last_modified,'readback',q.stitch_receipt) ORDER BY o.id))
 FROM hour h CROSS JOIN outputs
 JOIN nas_stitch_r2_stage_objects o ON o.hour_id=$1 AND o.attempt=$2 AND o.state='staged'
  AND entry @> jsonb_build_object('relative_path',o.relative_path,'size_bytes',o.size_bytes,'sha256',o.sha256)
 JOIN nas_r2_copy_items q ON q.connection_id=13 AND q.kind='stitch_output' AND q.source_id=o.id
  AND q.manifest_sha256=$4 AND nas_stitch_r2_exact_copy_receipt(q,o)
 WHERE (SELECT count(*) FROM inputs)=(SELECT count(*) FROM nas_stitch_r2_input_holds i
   WHERE i.hour_id=h.id AND i.released_at IS NULL)
  AND NOT EXISTS(SELECT 1 FROM nas_stitch_r2_input_holds i
   WHERE i.hour_id=h.id AND i.released_at IS NULL AND (SELECT count(*) FROM inputs
    WHERE proof @> jsonb_build_object('clip_id',i.catalog_clip_id,'size_bytes',i.size_bytes,
     'sha256',i.sha256,'repaired',false,'retirement_held',true))<>1)
 HAVING count(*)>0 AND count(DISTINCT o.id)=count(*) AND count(*)=(SELECT count(*) FROM outputs)
  AND count(*)=(SELECT count(*) FROM nas_stitch_r2_stage_objects o WHERE o.hour_id=$1 AND o.attempt=$2 AND o.state='staged')
$$;

CREATE FUNCTION add_nas_stitch_r2_certificate() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expected jsonb; locked_pairs bigint;
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION 'NAS stitch certification requires READ COMMITTED' USING ERRCODE='55000';
 END IF;
 PERFORM pg_advisory_xact_lock(7343226011173);
 PERFORM 1 FROM r2_inverse_ke_epoch FOR SHARE;
 PERFORM c.id FROM nas_stitch_r2_input_holds i
  JOIN recording_clips c ON c.id=i.catalog_clip_id
  JOIN nas_inventory_files f ON f.connection_id=13 AND f.clip_id=c.id
  WHERE i.hour_id=NEW.hour_id AND i.released_at IS NULL ORDER BY c.id FOR SHARE OF c,f;
 GET DIAGNOSTICS locked_pairs=ROW_COUNT;
 IF locked_pairs<>(SELECT count(*) FROM nas_stitch_r2_input_holds i
  WHERE i.hour_id=NEW.hour_id AND i.released_at IS NULL) THEN
  RAISE EXCEPTION 'NAS stitch input custody pairs could not be locked';
 END IF;
 IF NOT nas_stitch_r2_has_exact_input_set(NEW.hour_id) THEN
  RAISE EXCEPTION 'NAS stitch input coverage changed';
 END IF;
 expected:=nas_stitch_r2_certificate_receipt(NEW.hour_id,NEW.attempt,NEW.plan_sha256,NEW.manifest_sha256);
 IF expected IS NULL OR NEW.receipt IS DISTINCT FROM expected THEN
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


-- A server rollback may still authenticate the dedicated key. Once the R2
-- phase is live it cannot use the old claim path to receive a NAS-local plan.
CREATE FUNCTION guard_nas_stitch_r2_phase() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.state='leased' AND NEW.lease_token IS DISTINCT FROM OLD.lease_token AND
  ((NEW.lease_api_key_id IS NOT NULL AND NEW.lease_api_key_attempt=NEW.attempt)
   OR EXISTS(SELECT 1 FROM nas_stitch_r2_hours h
    WHERE h.state<>'certified' OR (h.recording_id=NEW.recording_id AND h.hour_at=NEW.hour_at))) THEN
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
CREATE INDEX nas_stitch_r2_stage_unresolved ON nas_stitch_r2_stage_retirements(object_id)
 WHERE absence_observed_at IS NULL;

CREATE FUNCTION guard_nas_stitch_r2_stage_retirement() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='INSERT' AND NEW.absence_observed_at IS NULL THEN RETURN NEW; END IF;
 IF TG_OP='UPDATE' AND NEW.object_id=OLD.object_id AND NEW.issued_at=OLD.issued_at
  AND OLD.absence_observed_at IS NULL AND NEW.absence_observed_at IS NOT NULL THEN RETURN NEW; END IF;
 RAISE EXCEPTION 'stitch staging retirement intent is one-shot' USING ERRCODE='55000';
END $$;
CREATE TRIGGER nas_stitch_r2_stage_retirement_guard BEFORE INSERT OR UPDATE OR DELETE ON nas_stitch_r2_stage_retirements
 FOR EACH ROW EXECUTE FUNCTION guard_nas_stitch_r2_stage_retirement();

-- The protection epoch fences these admissions during the wire attempt.
-- Its committed intent also protects the exact generation after timeout.
CREATE FUNCTION guard_nas_stitch_r2_stage_dependency() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE entry jsonb:=to_jsonb(NEW);
BEGIN
 IF EXISTS(SELECT 1 FROM nas_stitch_r2_stage_retirements d JOIN nas_stitch_r2_stage_objects o ON o.id=d.object_id
  WHERE d.absence_observed_at IS NULL AND (
   (o.object_key=entry->>'object_key' AND (entry->>'bucket' IS NULL OR o.bucket=entry->>'bucket'))
   OR o.relative_path=coalesce(entry->>'relative_path',entry->>'nas_relative_path',entry->>'display_path')
   OR (TG_TABLE_NAME<>'recording_clips' AND o.sha256=entry->>'sha256' AND o.size_bytes::text=entry->>'size_bytes'))) THEN
  RAISE EXCEPTION 'unresolved stitch staging retirement protects dependency admission' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER nas_stitch_stage_delivery_dependency BEFORE INSERT OR UPDATE ON delivery_keep_holds
 FOR EACH ROW EXECUTE FUNCTION guard_nas_stitch_r2_stage_dependency();
CREATE TRIGGER nas_stitch_stage_collated_dependency
 BEFORE INSERT OR UPDATE OF object_key,nas_relative_path,sha256,size_bytes ON recording_collation_outputs
 FOR EACH ROW EXECUTE FUNCTION guard_nas_stitch_r2_stage_dependency();
CREATE TRIGGER nas_stitch_stage_raw_dependency
 AFTER INSERT OR UPDATE OF bucket,object_key,display_path,storage_destination_id ON recording_clips
 FOR EACH ROW EXECUTE FUNCTION guard_nas_stitch_r2_stage_dependency();

DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['nas_stitch_r2_hours','nas_stitch_r2_stage_objects','nas_stitch_r2_parts',
  'nas_stitch_r2_certificates','nas_stitch_r2_stage_retirements','nas_r2_copy_items','r2_retirement_writer_fences'] LOOP
  EXECUTE format('CREATE TRIGGER nas_stitch_evidence_truncate BEFORE TRUNCATE ON %I'
    ' FOR EACH STATEMENT EXECUTE FUNCTION guard_nas_stitch_r2_truncate()',t);
 END LOOP;
END $$;
