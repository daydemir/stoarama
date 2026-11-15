-- Operator-only cancellation of a whole joined batch after independently
-- reviewed primary owner-stop evidence. No process death is inferred by SQL.
-- The CLI must verify every proof against its provider immediately before this
-- transaction. Proofs, every row digest and the final parent transition commit
-- together; ordinary worker transitions retain their existing guards.
CREATE TYPE r2_joined_retired_kind AS ENUM ('hour','artifact','admission','batch');
CREATE TABLE r2_joined_writer_retirement_runs (
 id uuid PRIMARY KEY,
 batch_record_id bigint NOT NULL UNIQUE REFERENCES recording_joined_batches(id),
 snapshot_sha256 text NOT NULL CHECK(snapshot_sha256 ~ '^[a-f0-9]{64}$'),
 proof_sha256 text NOT NULL CHECK(proof_sha256 ~ '^[a-f0-9]{64}$'),
 reviewer_reference text NOT NULL CHECK(length(btrim(reviewer_reference)) BETWEEN 1 AND 1024),
 expected_rows jsonb NOT NULL CHECK(jsonb_typeof(expected_rows)='array'),
 owner_evidence jsonb NOT NULL CHECK(jsonb_typeof(owner_evidence)='array'),
 transaction_id xid8 NOT NULL DEFAULT pg_current_xact_id(),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE r2_joined_writer_retirement_items (
 run_id uuid NOT NULL REFERENCES r2_joined_writer_retirement_runs(id),
 kind r2_joined_retired_kind NOT NULL,
 record_id bigint NOT NULL,
 before_sha256 text NOT NULL CHECK(before_sha256 ~ '^[a-f0-9]{64}$'),
 after_sha256 text NOT NULL CHECK(after_sha256 ~ '^[a-f0-9]{64}$'),
 PRIMARY KEY(run_id,kind,record_id)
);
CREATE TRIGGER r2_joined_writer_runs_immutable BEFORE UPDATE OR DELETE ON r2_joined_writer_retirement_runs
 FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_mutation();
CREATE TRIGGER r2_joined_writer_items_immutable BEFORE UPDATE OR DELETE ON r2_joined_writer_retirement_items
 FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_mutation();
CREATE TRIGGER r2_joined_writer_runs_no_truncate BEFORE TRUNCATE ON r2_joined_writer_retirement_runs
 FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_mutation();
CREATE TRIGGER r2_joined_writer_items_no_truncate BEFORE TRUNCATE ON r2_joined_writer_retirement_items
 FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_mutation();

CREATE FUNCTION r2_joined_writer_row_sha(body jsonb) RETURNS text LANGUAGE sql IMMUTABLE STRICT
 AS $$ SELECT encode(sha256(convert_to(body::text,'UTF8')),'hex') $$;

CREATE FUNCTION r2_joined_writer_snapshot(p_batch bigint) RETURNS text LANGUAGE sql STABLE SET timezone='UTC' AS $$
 SELECT r2_joined_writer_row_sha(jsonb_build_object('batch',r2_joined_writer_row_sha(to_jsonb(b)),
  'admission',(SELECT r2_joined_writer_row_sha(to_jsonb(c)) FROM recording_joined_admission_controls c WHERE c.batch_record_id=b.id),
  'hours',(SELECT coalesce(jsonb_agg(jsonb_build_array(h.id,r2_joined_writer_row_sha(to_jsonb(h))) ORDER BY h.id),'[]') FROM recording_joined_hours h WHERE h.batch_record_id=b.id),
  'days',(SELECT coalesce(jsonb_agg(jsonb_build_array(d.id,r2_joined_writer_row_sha(to_jsonb(d))) ORDER BY d.id),'[]') FROM recording_joined_stream_days d WHERE d.batch_record_id=b.id),
  'artifacts',(SELECT coalesce(jsonb_agg(jsonb_build_array(a.id,r2_joined_writer_row_sha(to_jsonb(a))) ORDER BY a.id),'[]') FROM recording_joined_artifacts a WHERE a.batch_record_id=b.id)))
 FROM recording_joined_batches b WHERE b.id=p_batch
$$;


-- Derive the complete allowed transition from locked source rows. Neither a
-- caller-supplied audit item nor an arbitrary same-transaction ID grants it.
CREATE FUNCTION r2_joined_writer_expected_rows(p_batch bigint) RETURNS jsonb LANGUAGE sql STABLE SET timezone='UTC' AS $$
 WITH source_rows AS (
  SELECT 'hour' kind,h.id,to_jsonb(h) body FROM recording_joined_hours h WHERE batch_record_id=p_batch AND state IN ('pending','leased')
  UNION ALL SELECT 'artifact',a.id,to_jsonb(a) FROM recording_joined_artifacts a WHERE batch_record_id=p_batch AND publication_state IN ('sealed','publishing')
  UNION ALL SELECT 'admission',c.batch_record_id,to_jsonb(c) FROM recording_joined_admission_controls c WHERE batch_record_id=p_batch
  UNION ALL SELECT 'batch',b.id,to_jsonb(b) FROM recording_joined_batches b WHERE id=p_batch
 ), transitions AS (
  SELECT kind,id,body,body || CASE kind
   WHEN 'hour' THEN jsonb_build_object('state','terminal_failed','failure_reason_code','owner_retired','claim_token',NULL,'claimed_by',NULL,'lease_expires_at',NULL,'heartbeat_at',NULL,'updated_at',transaction_timestamp())
   WHEN 'artifact' THEN jsonb_build_object('publication_state','terminal_failed','failure_reason_code','owner_retired','publication_token',NULL,'publication_claimed_by',NULL,'publication_lease_expires_at',NULL,'publication_heartbeat_at',NULL,'updated_at',transaction_timestamp())
   WHEN 'admission' THEN jsonb_build_object('claims_paused',true,'updated_at',transaction_timestamp())
   WHEN 'batch' THEN jsonb_build_object('state','terminal_failed','failure_reason_code','owner_retired') END after_body
  FROM source_rows
 ) SELECT jsonb_agg(jsonb_build_object('kind',kind,'record_id',id,'before_sha256',r2_joined_writer_row_sha(body),
  'after_sha256',r2_joined_writer_row_sha(after_body)) ORDER BY kind,id) FROM transitions
$$;

-- Old reclaim/release/completion overwrites owner identity. Until preserved
-- primary history accounts for every attempt, these batches must stay blocked.
-- The original live-worker failure guard permits rewriting its attempt count.
-- Preserve that history on every update, including ordinary terminal failure;
-- only the existing publishing claim/reclaim guard may increment it.
CREATE FUNCTION guard_r2_joined_publication_attempt_history() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.publication_attempt_count<OLD.publication_attempt_count
  OR (NEW.publication_state IS DISTINCT FROM 'publishing' AND NEW.publication_attempt_count<>OLD.publication_attempt_count)
 THEN RAISE EXCEPTION 'joined publication attempt history is immutable outside claim/reclaim'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER r2_joined_publication_attempt_history BEFORE UPDATE ON recording_joined_artifacts
 FOR EACH ROW EXECUTE FUNCTION guard_r2_joined_publication_attempt_history();

CREATE FUNCTION r2_joined_writer_owner_inventory_complete(p_batch bigint) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT NOT EXISTS(SELECT 1 FROM recording_joined_hours WHERE batch_record_id=p_batch AND attempt_count>0
  AND (attempt_count<>1 OR state<>'leased' OR claimed_by IS NULL))
 AND NOT EXISTS(SELECT 1 FROM recording_joined_artifacts WHERE batch_record_id=p_batch
  AND ((publication_attempt_count>0 AND (publication_attempt_count<>1 OR publication_state IS DISTINCT FROM 'publishing' OR publication_claimed_by IS NULL))
   -- Existing terminal/published roots with zero attempts may already have
   -- erased their owner. Installing this guard cannot reconstruct that proof.
   OR (publication_attempt_count=0 AND publication_state IN ('publishing','published','terminal_failed'))))
$$;

CREATE FUNCTION guard_r2_joined_writer_retirement_run() RETURNS trigger LANGUAGE plpgsql SET timezone='UTC' AS $$
DECLARE b recording_joined_batches%ROWTYPE; c recording_joined_admission_controls%ROWTYPE;
BEGIN
 PERFORM pg_advisory_xact_lock(7343226011187);
 PERFORM pg_advisory_xact_lock(137,1);
 IF NOT EXISTS(SELECT 1 FROM r2_retirement_writer_fences WHERE bucket='stoarama')
 THEN RAISE EXCEPTION 'active R2 writer fence required'; END IF;
 SELECT * INTO STRICT b FROM recording_joined_batches WHERE id=NEW.batch_record_id FOR UPDATE;
 SELECT * INTO STRICT c FROM recording_joined_admission_controls WHERE batch_record_id=NEW.batch_record_id FOR UPDATE;
 PERFORM 1 FROM recording_joined_hours WHERE batch_record_id=NEW.batch_record_id ORDER BY id FOR UPDATE;
 PERFORM 1 FROM recording_joined_artifacts WHERE batch_record_id=NEW.batch_record_id ORDER BY id FOR UPDATE;
 PERFORM 1 FROM recording_joined_stream_days WHERE batch_record_id=NEW.batch_record_id ORDER BY id FOR UPDATE;
 IF b.state NOT IN ('building','frozen','index_sealed') OR b.account_id<>47 OR b.connection_id<>13
  OR r2_joined_writer_snapshot(NEW.batch_record_id) IS DISTINCT FROM NEW.snapshot_sha256
  OR EXISTS(SELECT 1 FROM recording_joined_stream_days WHERE batch_record_id=NEW.batch_record_id AND state='pending')
  OR EXISTS(SELECT 1 FROM recording_joined_hours WHERE batch_record_id=NEW.batch_record_id AND state IN ('pending','leased')
   AND (lease_expires_at>clock_timestamp() OR heartbeat_at>clock_timestamp()-interval '15 minutes' OR updated_at>clock_timestamp()-interval '15 minutes'))
  OR EXISTS(SELECT 1 FROM recording_joined_artifacts WHERE batch_record_id=NEW.batch_record_id AND publication_state IN ('sealed','publishing')
   AND (publication_lease_expires_at>clock_timestamp() OR publication_heartbeat_at>clock_timestamp()-interval '15 minutes' OR updated_at>clock_timestamp()-interval '15 minutes'))
 THEN RAISE EXCEPTION 'joined snapshot drift, live/recent work or unsupported pending day'; END IF;
 IF NOT r2_joined_writer_owner_inventory_complete(NEW.batch_record_id)
 THEN RAISE EXCEPTION 'historical joined owner inventory is incomplete; current claims are not a complete roster'; END IF;
 IF jsonb_typeof(NEW.owner_evidence) IS DISTINCT FROM 'array'
  OR jsonb_array_length(NEW.owner_evidence)<>(SELECT count(DISTINCT e->>'owner') FROM jsonb_array_elements(NEW.owner_evidence)e) OR EXISTS(SELECT 1 FROM jsonb_array_elements(NEW.owner_evidence) e
  WHERE coalesce(e->>'kind','') NOT IN ('render_suspended','do_destroyed') OR coalesce(e->>'owner','')='' OR coalesce(e->>'resource_id','')=''
   OR coalesce(e->>'evidence_sha256','') !~ '^[a-f0-9]{64}$'
   OR e->>'observed_at' IS NULL OR (e->>'observed_at')::timestamptz NOT BETWEEN clock_timestamp()-interval '5 minutes' AND clock_timestamp()+interval '1 minute')
  OR ARRAY(SELECT DISTINCT owner FROM (
   SELECT claimed_by owner FROM recording_joined_hours WHERE batch_record_id=NEW.batch_record_id AND state='leased'
   UNION SELECT publication_claimed_by FROM recording_joined_artifacts WHERE batch_record_id=NEW.batch_record_id AND publication_state='publishing') x WHERE owner IS NOT NULL ORDER BY owner)
   IS DISTINCT FROM ARRAY(SELECT DISTINCT e->>'owner' FROM jsonb_array_elements(NEW.owner_evidence)e ORDER BY e->>'owner')
 THEN RAISE EXCEPTION 'fresh exact owner evidence roster required'; END IF;
 NEW.transaction_id:=pg_current_xact_id();
 NEW.created_at:=clock_timestamp();
 NEW.expected_rows:=r2_joined_writer_expected_rows(NEW.batch_record_id);
 RETURN NEW;
END $$;
CREATE TRIGGER r2_joined_writer_run_admission BEFORE INSERT ON r2_joined_writer_retirement_runs
 FOR EACH ROW EXECUTE FUNCTION guard_r2_joined_writer_retirement_run();

CREATE FUNCTION guard_r2_joined_writer_retirement_item() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM r2_joined_writer_retirement_runs r WHERE r.id=NEW.run_id
  AND r.transaction_id=pg_current_xact_id() AND r.expected_rows @> jsonb_build_array(to_jsonb(NEW)-'run_id'))
 THEN RAISE EXCEPTION 'joined audit item is not an exact admitted transition'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER r2_joined_writer_item_admission BEFORE INSERT ON r2_joined_writer_retirement_items
 FOR EACH ROW EXECUTE FUNCTION guard_r2_joined_writer_retirement_item();

-- Even direct INSERTs cannot commit partial or forged audit authorization.
-- This runs at commit, after all children and the parent have transitioned.
CREATE FUNCTION validate_r2_joined_writer_retirement_complete() RETURNS trigger LANGUAGE plpgsql SET timezone='UTC' AS $$
DECLARE actual jsonb;
BEGIN
 SELECT jsonb_agg(to_jsonb(i)-'run_id' ORDER BY kind::text,record_id) INTO actual
 FROM r2_joined_writer_retirement_items i WHERE run_id=NEW.id;
 IF actual IS DISTINCT FROM NEW.expected_rows
  OR NOT EXISTS(SELECT 1 FROM recording_joined_batches WHERE id=NEW.batch_record_id AND state='terminal_failed' AND failure_reason_code='owner_retired')
  OR NOT EXISTS(SELECT 1 FROM recording_joined_admission_controls WHERE batch_record_id=NEW.batch_record_id AND claims_paused)
  OR EXISTS(SELECT 1 FROM recording_joined_hours WHERE batch_record_id=NEW.batch_record_id AND state IN ('pending','leased'))
  OR EXISTS(SELECT 1 FROM recording_joined_artifacts WHERE batch_record_id=NEW.batch_record_id AND publication_state IN ('sealed','publishing'))
  OR EXISTS(SELECT 1 FROM recording_joined_stream_days WHERE batch_record_id=NEW.batch_record_id AND state='pending')
  OR EXISTS(SELECT 1 FROM r2_joined_writer_retirement_items i WHERE i.run_id=NEW.id AND i.after_sha256 IS DISTINCT FROM
   CASE i.kind
    WHEN 'hour' THEN (SELECT r2_joined_writer_row_sha(to_jsonb(h)) FROM recording_joined_hours h WHERE h.id=i.record_id)
    WHEN 'artifact' THEN (SELECT r2_joined_writer_row_sha(to_jsonb(a)) FROM recording_joined_artifacts a WHERE a.id=i.record_id)
    WHEN 'admission' THEN (SELECT r2_joined_writer_row_sha(to_jsonb(c)) FROM recording_joined_admission_controls c WHERE c.batch_record_id=i.record_id)
    WHEN 'batch' THEN (SELECT r2_joined_writer_row_sha(to_jsonb(b)) FROM recording_joined_batches b WHERE b.id=i.record_id)
   END)
 THEN RAISE EXCEPTION 'joined retirement must commit the entire exact audited batch'; END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER r2_joined_writer_run_complete AFTER INSERT ON r2_joined_writer_retirement_runs
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION validate_r2_joined_writer_retirement_complete();

-- Admission is the only ordinary mutable row after terminalizing the batch.
-- Keep its exact audited image even if deferred checks were run early, and
-- prevent later transactions from reopening retired admission.
CREATE FUNCTION guard_r2_joined_retired_admission() RETURNS trigger LANGUAGE plpgsql SET timezone='UTC' AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM r2_joined_writer_retirement_runs r WHERE r.batch_record_id=NEW.batch_record_id
  AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(r.expected_rows) e WHERE e->>'kind'='admission'
   AND (e->>'record_id')::bigint=NEW.batch_record_id AND e->>'after_sha256'=r2_joined_writer_row_sha(to_jsonb(NEW))))
 THEN RAISE EXCEPTION 'retired joined admission must retain its exact audited after-image'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER r2_joined_retired_admission_guard BEFORE UPDATE ON recording_joined_admission_controls
 FOR EACH ROW EXECUTE FUNCTION guard_r2_joined_retired_admission();

CREATE FUNCTION r2_joined_writer_retirement_matches(k r2_joined_retired_kind, record bigint, old_row jsonb, new_row jsonb)
 RETURNS boolean LANGUAGE plpgsql STABLE SET timezone='UTC' AS $$
DECLARE fields text[];
BEGIN
 IF NOT EXISTS(SELECT 1 FROM r2_joined_writer_retirement_items i JOIN r2_joined_writer_retirement_runs r ON r.id=i.run_id
  WHERE i.kind=k AND i.record_id=record AND r.transaction_id=pg_current_xact_id()
  AND r.batch_record_id=(old_row->>'batch_record_id')::bigint
  AND i.before_sha256=r2_joined_writer_row_sha(old_row) AND i.after_sha256=r2_joined_writer_row_sha(new_row))
 THEN RETURN false; END IF;
 IF k='hour' THEN
  IF old_row->>'state' NOT IN ('pending','leased') OR new_row->>'state'<>'terminal_failed'
   OR new_row->>'failure_reason_code'<>'owner_retired' OR new_row->>'claim_token' IS NOT NULL
   OR new_row->>'claimed_by' IS NOT NULL OR new_row->>'lease_expires_at' IS NOT NULL OR new_row->>'heartbeat_at' IS NOT NULL
   OR (old_row->>'lease_expires_at')::timestamptz>clock_timestamp()
   OR (old_row->>'heartbeat_at')::timestamptz>clock_timestamp()-interval '15 minutes'
   OR (old_row->>'updated_at')::timestamptz>clock_timestamp()-interval '15 minutes'
  THEN RETURN false; END IF;
  fields:=ARRAY['state','failure_reason_code','claim_token','claimed_by','lease_expires_at','heartbeat_at','updated_at'];
 ELSIF k='artifact' THEN
  IF old_row->>'artifact_kind'='media' OR old_row->>'publication_state' NOT IN ('sealed','publishing')
   OR new_row->>'publication_state'<>'terminal_failed' OR new_row->>'failure_reason_code'<>'owner_retired'
   OR new_row->>'publication_token' IS NOT NULL OR new_row->>'publication_claimed_by' IS NOT NULL
   OR new_row->>'publication_lease_expires_at' IS NOT NULL OR new_row->>'publication_heartbeat_at' IS NOT NULL
   OR (old_row->>'publication_lease_expires_at')::timestamptz>clock_timestamp()
   OR (old_row->>'publication_heartbeat_at')::timestamptz>clock_timestamp()-interval '15 minutes'
   OR (old_row->>'updated_at')::timestamptz>clock_timestamp()-interval '15 minutes'
  THEN RETURN false; END IF;
  fields:=ARRAY['publication_state','failure_reason_code','publication_token','publication_claimed_by','publication_lease_expires_at','publication_heartbeat_at','updated_at'];
 ELSE RETURN false; END IF;
 RETURN old_row-fields=new_row-fields AND (new_row->>'updated_at')::timestamptz=transaction_timestamp();
END $$;

-- Preserve the existing expired/exhausted worker path. Only exact same-
-- transaction audited cancellations use the new terminal path, without a
-- synthetic claim, lease refresh, disabled trigger or hidden child predicate.
DROP TRIGGER recording_joined_hour_update_guard ON recording_joined_hours;
CREATE TRIGGER recording_joined_hour_update_guard BEFORE UPDATE ON recording_joined_hours
 FOR EACH ROW WHEN (NOT (OLD.state='leased' AND NEW.state='terminal_failed'
  AND OLD.lease_expires_at<=CURRENT_TIMESTAMP AND OLD.attempt_count>=8 AND NEW.failure_reason_code='worker_lease_expired')
  AND NOT r2_joined_writer_retirement_matches('hour'::r2_joined_retired_kind,OLD.id,to_jsonb(OLD),to_jsonb(NEW)))
 EXECUTE FUNCTION guard_recording_joined_hour_update();
DROP TRIGGER recording_joined_artifact_update_guard ON recording_joined_artifacts;
CREATE TRIGGER recording_joined_artifact_update_guard BEFORE UPDATE ON recording_joined_artifacts
 FOR EACH ROW WHEN (NOT (OLD.publication_state='publishing' AND NEW.publication_state='terminal_failed'
  AND OLD.publication_lease_expires_at<=CURRENT_TIMESTAMP AND OLD.publication_attempt_count>=8 AND NEW.failure_reason_code='worker_lease_expired')
  AND NOT r2_joined_writer_retirement_matches('artifact'::r2_joined_retired_kind,OLD.id,to_jsonb(OLD),to_jsonb(NEW)))
 EXECUTE FUNCTION guard_recording_joined_artifact_update();

CREATE FUNCTION r2_retire_dead_joined_batch(p_run uuid,p_batch bigint,p_snapshot text,p_proof text,p_reviewer text,p_evidence jsonb)
 RETURNS integer LANGUAGE plpgsql SET timezone='UTC' AS $$
DECLARE b recording_joined_batches%ROWTYPE; c recording_joined_admission_controls%ROWTYPE; row record; old_body jsonb; new_body jsonb; total integer:=0;
BEGIN
 INSERT INTO r2_joined_writer_retirement_runs(id,batch_record_id,snapshot_sha256,proof_sha256,reviewer_reference,owner_evidence)
 VALUES(p_run,p_batch,p_snapshot,p_proof,p_reviewer,p_evidence);
 SELECT * INTO STRICT b FROM recording_joined_batches WHERE id=p_batch;
 SELECT * INTO STRICT c FROM recording_joined_admission_controls WHERE batch_record_id=p_batch;
 old_body:=to_jsonb(c);new_body:=old_body||jsonb_build_object('claims_paused',true,'updated_at',transaction_timestamp());
 INSERT INTO r2_joined_writer_retirement_items VALUES(p_run,'admission',p_batch,r2_joined_writer_row_sha(old_body),r2_joined_writer_row_sha(new_body));
 UPDATE recording_joined_admission_controls SET claims_paused=true,updated_at=transaction_timestamp() WHERE batch_record_id=p_batch;
 total:=total+1;
 FOR row IN SELECT * FROM recording_joined_hours WHERE batch_record_id=p_batch AND state IN ('pending','leased') ORDER BY id LOOP
  old_body:=to_jsonb(row); new_body:=old_body||jsonb_build_object('state','terminal_failed','failure_reason_code','owner_retired','claim_token',NULL,'claimed_by',NULL,'lease_expires_at',NULL,'heartbeat_at',NULL,'updated_at',transaction_timestamp());
  INSERT INTO r2_joined_writer_retirement_items VALUES(p_run,'hour',row.id,r2_joined_writer_row_sha(old_body),r2_joined_writer_row_sha(new_body));
  UPDATE recording_joined_hours SET state='terminal_failed',failure_reason_code='owner_retired',claim_token=NULL,claimed_by=NULL,lease_expires_at=NULL,heartbeat_at=NULL,updated_at=transaction_timestamp() WHERE id=row.id;
  total:=total+1;
 END LOOP;
 FOR row IN SELECT * FROM recording_joined_artifacts WHERE batch_record_id=p_batch AND publication_state IN ('sealed','publishing') ORDER BY id LOOP
  old_body:=to_jsonb(row); new_body:=old_body||jsonb_build_object('publication_state','terminal_failed','failure_reason_code','owner_retired','publication_token',NULL,'publication_claimed_by',NULL,'publication_lease_expires_at',NULL,'publication_heartbeat_at',NULL,'updated_at',transaction_timestamp());
  INSERT INTO r2_joined_writer_retirement_items VALUES(p_run,'artifact',row.id,r2_joined_writer_row_sha(old_body),r2_joined_writer_row_sha(new_body));
  UPDATE recording_joined_artifacts SET publication_state='terminal_failed',failure_reason_code='owner_retired',publication_token=NULL,publication_claimed_by=NULL,publication_lease_expires_at=NULL,publication_heartbeat_at=NULL,updated_at=transaction_timestamp() WHERE id=row.id;
  total:=total+1;
 END LOOP;
 IF EXISTS(SELECT 1 FROM recording_joined_hours WHERE batch_record_id=p_batch AND state IN ('pending','leased'))
  OR EXISTS(SELECT 1 FROM recording_joined_artifacts WHERE batch_record_id=p_batch AND publication_state IN ('sealed','publishing'))
 THEN RAISE EXCEPTION 'joined children did not all terminalize'; END IF;
 old_body:=to_jsonb(b);new_body:=old_body||jsonb_build_object('state','terminal_failed','failure_reason_code','owner_retired');
 INSERT INTO r2_joined_writer_retirement_items VALUES(p_run,'batch',p_batch,r2_joined_writer_row_sha(old_body),r2_joined_writer_row_sha(new_body));
 UPDATE recording_joined_batches SET state='terminal_failed',failure_reason_code='owner_retired' WHERE id=p_batch;
 IF EXISTS(SELECT 1 FROM r2_joined_writer_retirement_items i WHERE i.run_id=p_run AND i.after_sha256 IS DISTINCT FROM
  CASE i.kind
   WHEN 'hour' THEN (SELECT r2_joined_writer_row_sha(to_jsonb(h)) FROM recording_joined_hours h WHERE h.id=i.record_id)
   WHEN 'artifact' THEN (SELECT r2_joined_writer_row_sha(to_jsonb(a)) FROM recording_joined_artifacts a WHERE a.id=i.record_id)
   WHEN 'admission' THEN (SELECT r2_joined_writer_row_sha(to_jsonb(x)) FROM recording_joined_admission_controls x WHERE x.batch_record_id=i.record_id)
   WHEN 'batch' THEN (SELECT r2_joined_writer_row_sha(to_jsonb(x)) FROM recording_joined_batches x WHERE x.id=i.record_id)
  END)
 THEN RAISE EXCEPTION 'joined retirement audit does not match committed row'; END IF;
 RETURN total+1;
END $$;
