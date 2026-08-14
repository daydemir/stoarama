-- Fenced continuous-recording surrender transport and crash-recovery authority.
--
-- This migration is deliberately additive. Older recorder builds keep using the
-- legacy surrender body while v1 workers opt into the generation/producer ledger.

-- A SHA is not a clip identity: two camera-time units may legitimately contain
-- equal bytes. V1 binds acceptance to upload-intent/clip/capture-sequence instead.
-- Legacy workers retain their API hash preflight; the old unique SHA index must
-- no longer reject a distinct v1 intent at the final insert.
DROP INDEX uq_recording_clips_capture_sha256;

CREATE TABLE recording_job_lease_generations (
  recording_job_id BIGINT NOT NULL REFERENCES recording_jobs(id) ON DELETE RESTRICT,
  lease_token UUID NOT NULL,
  lease_owner TEXT NOT NULL CHECK(octet_length(lease_owner) BETWEEN 1 AND 255),
  node_id BIGINT REFERENCES nodes(id) ON DELETE RESTRICT,
  attempt_count INTEGER NOT NULL CHECK(attempt_count > 0),
  claimed_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  initial_lease_expires_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY(recording_job_id,lease_token),
  CHECK(initial_lease_expires_at > claimed_at)
);

CREATE TABLE recording_job_unique_heads (
  recording_job_id BIGINT NOT NULL,
  lease_token UUID NOT NULL,
  version BIGINT NOT NULL DEFAULT 0 CHECK(version >= 0),
  upload_intent_id UUID REFERENCES recording_upload_intents(id) ON DELETE RESTRICT,
  clip_id BIGINT REFERENCES recording_clips(id) ON DELETE RESTRICT,
  capture_sequence BIGINT,
  advanced_at TIMESTAMPTZ,
  PRIMARY KEY(recording_job_id,lease_token),
  FOREIGN KEY(recording_job_id,lease_token)
    REFERENCES recording_job_lease_generations(recording_job_id,lease_token) ON DELETE RESTRICT,
  CHECK((version=0 AND upload_intent_id IS NULL AND clip_id IS NULL AND capture_sequence IS NULL AND advanced_at IS NULL)
     OR (version>0 AND upload_intent_id IS NOT NULL AND clip_id IS NOT NULL AND capture_sequence IS NOT NULL AND capture_sequence>0 AND advanced_at IS NOT NULL))
);

CREATE TABLE recording_capture_producers (
  id UUID PRIMARY KEY,
  recording_job_id BIGINT NOT NULL,
  lease_token UUID NOT NULL,
  capture_ordinal BIGINT NOT NULL CHECK(capture_ordinal > 0),
  worker_id TEXT NOT NULL CHECK(octet_length(worker_id) BETWEEN 1 AND 255),
  node_id BIGINT NOT NULL REFERENCES nodes(id) ON DELETE RESTRICT,
  sealed_intent_limit INTEGER NOT NULL CHECK(sealed_intent_limit BETWEEN 1 AND 8),
  recovery_secret_sha256 TEXT NOT NULL CHECK(recovery_secret_sha256 ~ '^[0-9a-f]{64}$'),
  source_snapshot_sha256 TEXT NOT NULL CHECK(source_snapshot_sha256 ~ '^[0-9a-f]{64}$'),
  reserved_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  UNIQUE(recording_job_id,lease_token,capture_ordinal),
  FOREIGN KEY(recording_job_id,lease_token)
    REFERENCES recording_job_lease_generations(recording_job_id,lease_token) ON DELETE RESTRICT
);

CREATE TABLE recording_capture_producer_results (
  producer_id UUID PRIMARY KEY REFERENCES recording_capture_producers(id) ON DELETE RESTRICT,
  result TEXT NOT NULL CHECK(result IN ('completed','abandoned_empty','host_unreachable_unrecoverable','security_revoked')),
  result_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  detail_class TEXT NOT NULL DEFAULT '' CHECK(detail_class ~ '^[a-z0-9_]{0,64}$')
);

CREATE TABLE recording_capture_artifact_seals (
  upload_intent_id UUID PRIMARY KEY REFERENCES recording_upload_intents(id) ON DELETE RESTRICT,
  producer_id UUID NOT NULL REFERENCES recording_capture_producers(id) ON DELETE RESTRICT,
  capture_sequence BIGINT NOT NULL CHECK(capture_sequence > 0),
  segment_start_ms BIGINT NOT NULL CHECK(segment_start_ms > 0),
  size_bytes BIGINT NOT NULL CHECK(size_bytes > 0),
  sha256 TEXT NOT NULL CHECK(sha256 ~ '^[0-9a-f]{64}$'),
  sealed_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  UNIQUE(producer_id,capture_sequence),
  UNIQUE(producer_id,segment_start_ms)
);

CREATE TABLE recording_capture_artifact_results (
  upload_intent_id UUID PRIMARY KEY REFERENCES recording_capture_artifact_seals(upload_intent_id) ON DELETE RESTRICT,
  result TEXT NOT NULL CHECK(result IN ('accepted_unique','exact_replay','host_unreachable_unrecoverable','security_revoked')),
  clip_id BIGINT REFERENCES recording_clips(id) ON DELETE RESTRICT,
  head_version BIGINT,
  result_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  CHECK((result='accepted_unique' AND clip_id IS NOT NULL AND head_version IS NOT NULL AND head_version>0)
     OR (result='exact_replay' AND clip_id IS NOT NULL AND head_version IS NOT NULL AND head_version>=0)
     OR (result NOT IN ('accepted_unique','exact_replay') AND clip_id IS NULL AND head_version IS NULL))
);

CREATE TABLE recording_job_surrender_attempts (
  id UUID PRIMARY KEY,
  recording_job_id BIGINT NOT NULL,
  lease_token UUID NOT NULL,
  worker_id TEXT NOT NULL CHECK(octet_length(worker_id) BETWEEN 1 AND 255),
  node_id BIGINT NOT NULL REFERENCES nodes(id) ON DELETE RESTRICT,
  reason TEXT NOT NULL CHECK(reason IN ('no_progress','disk_pressure','self_update')),
  error_text TEXT NOT NULL DEFAULT '' CHECK(octet_length(error_text)<=512 AND error_text !~ '[[:cntrl:]]'),
  expected_head_version BIGINT NOT NULL CHECK(expected_head_version >= 0),
  expected_upload_intent_id UUID,
  expected_clip_id BIGINT,
  spool_count INTEGER NOT NULL CHECK(spool_count=0),
  spool_bytes BIGINT NOT NULL CHECK(spool_bytes=0),
  in_flight_count INTEGER NOT NULL CHECK(in_flight_count=0),
  request_sha256 TEXT NOT NULL CHECK(request_sha256 ~ '^[0-9a-f]{64}$'),
  requested_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  UNIQUE(recording_job_id,lease_token,id),
  FOREIGN KEY(recording_job_id,lease_token)
    REFERENCES recording_job_lease_generations(recording_job_id,lease_token) ON DELETE RESTRICT,
  CHECK((expected_head_version=0 AND expected_upload_intent_id IS NULL AND expected_clip_id IS NULL)
     OR (expected_head_version>0 AND expected_upload_intent_id IS NOT NULL AND expected_clip_id IS NOT NULL))
);

CREATE TABLE recording_job_surrender_results (
  attempt_id UUID PRIMARY KEY REFERENCES recording_job_surrender_attempts(id) ON DELETE RESTRICT,
  result TEXT NOT NULL CHECK(result IN ('committed','stale_progress','stale_fence','window_closed','ineligible_spool')),
  next_retry_at TIMESTAMPTZ,
  handoff_until TIMESTAMPTZ,
  had_clips BOOLEAN,
  alternate_available BOOLEAN,
  current_head_version BIGINT NOT NULL CHECK(current_head_version >= 0),
  current_upload_intent_id UUID REFERENCES recording_upload_intents(id) ON DELETE RESTRICT,
  current_clip_id BIGINT REFERENCES recording_clips(id) ON DELETE RESTRICT,
  result_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  CHECK((result='committed' AND next_retry_at IS NOT NULL AND handoff_until IS NOT NULL AND had_clips IS NOT NULL AND alternate_available IS NOT NULL)
     OR (result<>'committed' AND next_retry_at IS NULL AND handoff_until IS NULL AND had_clips IS NULL AND alternate_available IS NULL)),
  CHECK((current_head_version=0 AND current_upload_intent_id IS NULL AND current_clip_id IS NULL)
     OR (current_head_version>0 AND current_upload_intent_id IS NOT NULL AND current_clip_id IS NOT NULL))
);

CREATE TABLE recording_job_recovery_grants (
  id UUID PRIMARY KEY,
  recording_job_id BIGINT NOT NULL,
  lease_token UUID NOT NULL,
  producer_id UUID NOT NULL REFERENCES recording_capture_producers(id) ON DELETE RESTRICT,
  recovery_secret_sha256 TEXT NOT NULL CHECK(recovery_secret_sha256 ~ '^[0-9a-f]{64}$'),
  granted_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  upload_grace_until TIMESTAMPTZ NOT NULL,
  revoked_at TIMESTAMPTZ,
  revoke_reason TEXT CHECK(revoke_reason IS NULL OR revoke_reason IN ('security_incident','recovery_completed','recovery_grace_expired')),
  UNIQUE(recording_job_id,lease_token,producer_id),
  FOREIGN KEY(recording_job_id,lease_token)
    REFERENCES recording_job_lease_generations(recording_job_id,lease_token) ON DELETE RESTRICT,
  CHECK(upload_grace_until=granted_at+interval '30 minutes'),
  CHECK((revoked_at IS NULL AND revoke_reason IS NULL) OR (revoked_at IS NOT NULL AND revoke_reason IS NOT NULL))
);
CREATE INDEX recording_job_recovery_grants_producer_idx ON recording_job_recovery_grants(producer_id);

CREATE TABLE recording_job_recovery_grant_intents (
  grant_id UUID NOT NULL REFERENCES recording_job_recovery_grants(id) ON DELETE RESTRICT,
  upload_intent_id UUID NOT NULL REFERENCES recording_upload_intents(id) ON DELETE RESTRICT,
  PRIMARY KEY(grant_id,upload_intent_id),
  UNIQUE(upload_intent_id)
);

CREATE TABLE recording_job_lease_expiry_events (
  recording_job_id BIGINT NOT NULL,
  lease_token UUID NOT NULL,
  lease_owner TEXT NOT NULL,
  recovery_grant_count INTEGER NOT NULL CHECK(recovery_grant_count >= 0),
  alternate_available BOOLEAN NOT NULL,
  handoff_until TIMESTAMPTZ NOT NULL,
  reclaimed_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  PRIMARY KEY(recording_job_id,lease_token),
  FOREIGN KEY(recording_job_id,lease_token)
    REFERENCES recording_job_lease_generations(recording_job_id,lease_token) ON DELETE RESTRICT,
  CHECK(handoff_until=reclaimed_at+CASE WHEN alternate_available THEN interval '5 minutes' ELSE interval '0' END)
);

CREATE FUNCTION recording_surrender_freeze_rows() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'recording surrender authority is append-only';
END $$;

CREATE TRIGGER recording_job_lease_generations_freeze BEFORE UPDATE OR DELETE ON recording_job_lease_generations FOR EACH ROW EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_job_lease_generations_no_truncate BEFORE TRUNCATE ON recording_job_lease_generations FOR EACH STATEMENT EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_capture_producers_freeze BEFORE UPDATE OR DELETE ON recording_capture_producers FOR EACH ROW EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_capture_producers_no_truncate BEFORE TRUNCATE ON recording_capture_producers FOR EACH STATEMENT EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_capture_producer_results_freeze BEFORE UPDATE OR DELETE ON recording_capture_producer_results FOR EACH ROW EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_capture_producer_results_no_truncate BEFORE TRUNCATE ON recording_capture_producer_results FOR EACH STATEMENT EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_capture_artifact_seals_freeze BEFORE UPDATE OR DELETE ON recording_capture_artifact_seals FOR EACH ROW EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_capture_artifact_seals_no_truncate BEFORE TRUNCATE ON recording_capture_artifact_seals FOR EACH STATEMENT EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_capture_artifact_results_freeze BEFORE UPDATE OR DELETE ON recording_capture_artifact_results FOR EACH ROW EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_capture_artifact_results_no_truncate BEFORE TRUNCATE ON recording_capture_artifact_results FOR EACH STATEMENT EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_job_surrender_attempts_freeze BEFORE UPDATE OR DELETE ON recording_job_surrender_attempts FOR EACH ROW EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_job_surrender_attempts_no_truncate BEFORE TRUNCATE ON recording_job_surrender_attempts FOR EACH STATEMENT EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_job_surrender_results_freeze BEFORE UPDATE OR DELETE ON recording_job_surrender_results FOR EACH ROW EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_job_surrender_results_no_truncate BEFORE TRUNCATE ON recording_job_surrender_results FOR EACH STATEMENT EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_job_recovery_grant_intents_freeze BEFORE UPDATE OR DELETE ON recording_job_recovery_grant_intents FOR EACH ROW EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_job_recovery_grant_intents_no_truncate BEFORE TRUNCATE ON recording_job_recovery_grant_intents FOR EACH STATEMENT EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_job_lease_expiry_events_freeze BEFORE UPDATE OR DELETE ON recording_job_lease_expiry_events FOR EACH ROW EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_job_lease_expiry_events_no_truncate BEFORE TRUNCATE ON recording_job_lease_expiry_events FOR EACH STATEMENT EXECUTE FUNCTION recording_surrender_freeze_rows();

CREATE FUNCTION recording_surrender_validate_expiry_event_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE exact_owner TEXT; exact_grants INTEGER; capture_via TEXT; exact_alternate BOOLEAN; job_expired BOOLEAN;
BEGIN
	  SELECT generation.lease_owner,recording.capture_via,
	         job.status='leased' AND job.lease_token=NEW.lease_token
	           AND job.lease_owner=generation.lease_owner AND job.lease_expires_at<=transaction_timestamp()
	    INTO exact_owner,capture_via,job_expired
  FROM recording_job_lease_generations generation
  JOIN recording_jobs job ON job.id=generation.recording_job_id
  JOIN recordings recording ON recording.id=job.recording_id
  WHERE generation.recording_job_id=NEW.recording_job_id AND generation.lease_token=NEW.lease_token;
  IF NOT FOUND THEN RAISE EXCEPTION 'invalid recording lease expiry event generation'; END IF;
  SELECT count(*) INTO exact_grants FROM recording_job_recovery_grants grant_row
  WHERE grant_row.recording_job_id=NEW.recording_job_id AND grant_row.lease_token=NEW.lease_token;
	SELECT EXISTS(
	  SELECT 1 FROM recorder_droplets droplet
	  WHERE droplet.name<>NEW.lease_owner AND droplet.state IN('provisioning','active')
	    AND droplet.last_seen_at>=transaction_timestamp()-interval '2 minutes'
	    AND (SELECT count(*) FROM recording_jobs live
	         WHERE live.status='leased' AND live.lease_owner=droplet.name
	           AND live.lease_expires_at>transaction_timestamp())<droplet.capacity
	) INTO exact_alternate;
	  IF capture_via<>'cloud' OR NOT job_expired OR exact_owner<>NEW.lease_owner
     OR exact_grants<>NEW.recovery_grant_count
	 OR exact_alternate IS DISTINCT FROM NEW.alternate_available
     OR NEW.reclaimed_at IS DISTINCT FROM transaction_timestamp() THEN
    RAISE EXCEPTION 'invalid recording lease expiry event';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_job_lease_expiry_events_validate
BEFORE INSERT ON recording_job_lease_expiry_events FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_expiry_event_insert();

-- Grant revocation is the only mutable recovery fact. Its trigger permits one
-- DB-authored NULL -> terminal transition and freezes every identity field.
CREATE FUNCTION recording_surrender_validate_grant_update() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE producer_result TEXT; invalid_artifacts BOOLEAN;
BEGIN
  IF (NEW.id,NEW.recording_job_id,NEW.lease_token,NEW.producer_id,NEW.recovery_secret_sha256,NEW.granted_at,NEW.upload_grace_until)
       IS DISTINCT FROM
     (OLD.id,OLD.recording_job_id,OLD.lease_token,OLD.producer_id,OLD.recovery_secret_sha256,OLD.granted_at,OLD.upload_grace_until)
     OR OLD.revoked_at IS NOT NULL
     OR NEW.revoked_at IS DISTINCT FROM transaction_timestamp()
     OR NEW.revoke_reason IS NULL THEN
    RAISE EXCEPTION 'invalid recording recovery grant transition';
  END IF;
	SELECT result INTO producer_result FROM recording_capture_producer_results WHERE producer_id=NEW.producer_id;
	SELECT EXISTS(
	  SELECT 1 FROM recording_capture_artifact_seals seal
	  LEFT JOIN recording_capture_artifact_results result ON result.upload_intent_id=seal.upload_intent_id
	  WHERE seal.producer_id=NEW.producer_id AND (
	    result.upload_intent_id IS NULL OR
	    (NEW.revoke_reason='recovery_completed' AND result.result NOT IN('accepted_unique','exact_replay')) OR
	    (NEW.revoke_reason='security_incident' AND result.result NOT IN('accepted_unique','exact_replay','security_revoked')) OR
	    (NEW.revoke_reason='recovery_grace_expired' AND result.result NOT IN('accepted_unique','exact_replay','host_unreachable_unrecoverable'))
	  )
	) INTO invalid_artifacts;
	IF invalid_artifacts
	   OR (NEW.revoke_reason='recovery_completed' AND producer_result IS DISTINCT FROM 'completed' AND producer_result IS DISTINCT FROM 'abandoned_empty')
	   OR (NEW.revoke_reason='security_incident' AND producer_result IS DISTINCT FROM 'security_revoked')
	   OR (NEW.revoke_reason='recovery_grace_expired' AND (producer_result IS DISTINCT FROM 'host_unreachable_unrecoverable' OR NEW.upload_grace_until>transaction_timestamp())) THEN
	  RAISE EXCEPTION 'recording recovery grant terminal reason lacks exact producer evidence';
	END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_job_recovery_grants_validate_update BEFORE UPDATE ON recording_job_recovery_grants FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_grant_update();
CREATE TRIGGER recording_job_recovery_grants_no_delete BEFORE DELETE ON recording_job_recovery_grants FOR EACH ROW EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_job_recovery_grants_no_truncate BEFORE TRUNCATE ON recording_job_recovery_grants FOR EACH STATEMENT EXECUTE FUNCTION recording_surrender_freeze_rows();

CREATE FUNCTION recording_surrender_validate_grant_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE exact BOOLEAN;
BEGIN
  SELECT producer.recording_job_id=NEW.recording_job_id
         AND producer.lease_token=NEW.lease_token
         AND producer.recovery_secret_sha256=NEW.recovery_secret_sha256
	     AND job.status='leased' AND job.lease_token=producer.lease_token
	     AND job.lease_owner=producer.worker_id AND job.lease_expires_at<=transaction_timestamp()
         AND NOT EXISTS(SELECT 1 FROM recording_capture_producer_results result WHERE result.producer_id=producer.id)
    INTO exact
  FROM recording_capture_producers producer
  JOIN recording_jobs job ON job.id=producer.recording_job_id
  WHERE producer.id=NEW.producer_id FOR UPDATE OF producer,job;
  IF NOT FOUND OR NOT exact OR NEW.granted_at IS DISTINCT FROM transaction_timestamp()
	 OR NEW.upload_grace_until IS DISTINCT FROM transaction_timestamp()+interval '30 minutes'
     OR NEW.revoked_at IS NOT NULL OR NEW.revoke_reason IS NOT NULL THEN
    RAISE EXCEPTION 'invalid recording recovery grant insert';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_job_recovery_grants_validate_insert
BEFORE INSERT ON recording_job_recovery_grants FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_grant_insert();

CREATE FUNCTION recording_surrender_validate_grant_intent_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NOT EXISTS(
	    SELECT 1 FROM recording_job_recovery_grants grant_row
	    JOIN recording_capture_artifact_seals seal ON seal.producer_id=grant_row.producer_id
	    WHERE grant_row.id=NEW.grant_id AND seal.upload_intent_id=NEW.upload_intent_id
	      AND grant_row.revoked_at IS NULL AND grant_row.upload_grace_until>transaction_timestamp()
  ) THEN
    RAISE EXCEPTION 'recovery grant intent is not bound to its producer';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_job_recovery_grant_intents_validate
BEFORE INSERT ON recording_job_recovery_grant_intents FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_grant_intent_insert();

CREATE FUNCTION recording_surrender_revoke_recovery_grant(p_grant_id UUID, p_reason TEXT) RETURNS BOOLEAN LANGUAGE plpgsql AS $$
DECLARE target_job BIGINT; target_producer UUID;
BEGIN
  IF p_reason<>'security_incident' THEN RAISE EXCEPTION 'invalid recovery revocation reason'; END IF;
  SELECT recording_job_id,producer_id INTO target_job,target_producer
    FROM recording_job_recovery_grants WHERE id=p_grant_id;
  IF NOT FOUND THEN RETURN false; END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('recording-surrender-job:'||target_job::text,0));
  PERFORM 1 FROM recording_job_recovery_grants WHERE id=p_grant_id AND revoked_at IS NULL FOR UPDATE;
  IF NOT FOUND THEN RETURN false; END IF;
  INSERT INTO recording_capture_artifact_results(upload_intent_id,result)
  SELECT seal.upload_intent_id,'security_revoked'
  FROM recording_capture_artifact_seals seal
  WHERE seal.producer_id=target_producer
    AND NOT EXISTS(SELECT 1 FROM recording_capture_artifact_results result WHERE result.upload_intent_id=seal.upload_intent_id)
  ON CONFLICT DO NOTHING;
  INSERT INTO recording_capture_producer_results(producer_id,result,detail_class)
  VALUES(target_producer,'security_revoked','recovery_capability_revoked')
  ON CONFLICT DO NOTHING;
  UPDATE recording_job_recovery_grants SET revoked_at=transaction_timestamp(),revoke_reason=p_reason WHERE id=p_grant_id;
  RETURN true;
END $$;

CREATE FUNCTION recording_surrender_validate_producer_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expected_ordinal BIGINT; generation_owner TEXT; generation_node BIGINT; generation_current BOOLEAN; expected_snapshot TEXT; latest_reserved_at TIMESTAMPTZ; prior RECORD;
BEGIN
	  SELECT generation.lease_owner,generation.node_id,
	         job.status='leased' AND job.lease_token=NEW.lease_token
	           AND job.lease_owner=generation.lease_owner AND job.lease_expires_at>transaction_timestamp()
	           AND job.kind='continuous_window' AND job.window_end_at>transaction_timestamp(),
	         encode(sha256(convert_to(
	           'recording-capture-producer-v1'||chr(10)
	           ||octet_length(recording.id::text)::text||':'||recording.id::text||chr(10)
	           ||octet_length(COALESCE(recording.stream_id,0)::text)::text||':'||COALESCE(recording.stream_id,0)::text||chr(10)
	           ||octet_length(recording.stream_url)::text||':'||recording.stream_url||chr(10)
	           ||octet_length(COALESCE(stream.source_url,''))::text||':'||COALESCE(stream.source_url,'')||chr(10)
	           ||octet_length(COALESCE(stream.source_page_url,''))::text||':'||COALESCE(stream.source_page_url,'')||chr(10)
	           ||octet_length(COALESCE(stream.provider,''))::text||':'||COALESCE(stream.provider,'')||chr(10)
	           ||octet_length(COALESCE(stream.external_id,''))::text||':'||COALESCE(stream.external_id,'')||chr(10)
	         ,'UTF8')),'hex')
	    INTO generation_owner,generation_node,generation_current,expected_snapshot
	  FROM recording_job_lease_generations generation
	  JOIN recording_jobs job ON job.id=generation.recording_job_id
	  JOIN recordings recording ON recording.id=job.recording_id
	  LEFT JOIN streams stream ON stream.id=recording.stream_id
  WHERE generation.recording_job_id=NEW.recording_job_id AND generation.lease_token=NEW.lease_token
  FOR UPDATE OF generation,job;
	  IF NOT FOUND OR NOT generation_current OR generation_owner<>NEW.worker_id OR generation_node IS DISTINCT FROM NEW.node_id
	     OR NEW.source_snapshot_sha256 IS DISTINCT FROM expected_snapshot THEN
    RAISE EXCEPTION 'capture producer has no current matching lease generation';
  END IF;
  SELECT * INTO prior FROM recording_capture_producers p
  WHERE p.recording_job_id=NEW.recording_job_id AND p.lease_token=NEW.lease_token
    AND p.capture_ordinal=NEW.capture_ordinal;
  IF FOUND THEN
    IF (prior.id,prior.worker_id,prior.node_id,prior.sealed_intent_limit,prior.recovery_secret_sha256,prior.source_snapshot_sha256)
         IS DISTINCT FROM
       (NEW.id,NEW.worker_id,NEW.node_id,NEW.sealed_intent_limit,NEW.recovery_secret_sha256,NEW.source_snapshot_sha256) THEN
      RAISE EXCEPTION 'capture producer ordinal replay differs';
    END IF;
    RETURN NEW;
  END IF;
	  SELECT COALESCE(max(p.capture_ordinal),0)+1,max(p.reserved_at) INTO expected_ordinal,latest_reserved_at
	    FROM recording_capture_producers p
	    WHERE p.recording_job_id=NEW.recording_job_id AND p.lease_token=NEW.lease_token;
	  IF NEW.capture_ordinal<>expected_ordinal THEN RAISE EXCEPTION 'capture producer ordinal is not monotonic'; END IF;
	  IF latest_reserved_at IS NOT NULL AND NEW.reserved_at<=latest_reserved_at THEN
	    RAISE EXCEPTION 'capture producer decision time is not strictly monotonic';
	  END IF;
  IF EXISTS(
    SELECT 1 FROM recording_capture_producers p
    WHERE p.recording_job_id=NEW.recording_job_id AND p.lease_token=NEW.lease_token
      AND NOT EXISTS(SELECT 1 FROM recording_capture_producer_results r WHERE r.producer_id=p.id)
  ) THEN RAISE EXCEPTION 'capture generation already has an unsealed producer'; END IF;
  IF NEW.reserved_at IS DISTINCT FROM transaction_timestamp() THEN
    RAISE EXCEPTION 'capture producer reservation time is not DB-authored';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_capture_producers_validate BEFORE INSERT ON recording_capture_producers FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_producer_insert();

CREATE FUNCTION recording_surrender_validate_artifact_seal() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE lim INTEGER; outstanding INTEGER; exact_intent BOOLEAN; prior RECORD;
BEGIN
	  SELECT producer.sealed_intent_limit,
	         intent.recording_job_id=producer.recording_job_id
	           AND intent.recording_id=job.recording_id
	           AND intent.max_size_bytes>=NEW.size_bytes
	           AND ((job.status='leased' AND job.lease_token=producer.lease_token
	                 AND job.lease_owner=producer.worker_id AND job.lease_expires_at>transaction_timestamp())
	                OR EXISTS(SELECT 1 FROM recording_job_recovery_grants grant_row
	                          WHERE grant_row.producer_id=producer.id AND grant_row.revoked_at IS NULL
	                            AND grant_row.upload_grace_until>transaction_timestamp()))
	    INTO lim,exact_intent
	  FROM recording_capture_producers producer
	  JOIN recording_jobs job ON job.id=producer.recording_job_id
	  JOIN recording_upload_intents intent ON intent.id=NEW.upload_intent_id
	  WHERE producer.id=NEW.producer_id FOR UPDATE OF producer,intent,job;
	  IF NOT FOUND THEN RAISE EXCEPTION 'artifact seal has no producer'; END IF;
	  IF NOT exact_intent THEN RAISE EXCEPTION 'artifact seal intent crosses producer authority'; END IF;
  SELECT * INTO prior FROM recording_capture_artifact_seals seal
  WHERE seal.upload_intent_id=NEW.upload_intent_id
     OR (seal.producer_id=NEW.producer_id AND seal.capture_sequence=NEW.capture_sequence)
     OR (seal.producer_id=NEW.producer_id AND seal.segment_start_ms=NEW.segment_start_ms)
  LIMIT 1;
  IF FOUND THEN
    IF (prior.upload_intent_id,prior.producer_id,prior.capture_sequence,prior.segment_start_ms,prior.size_bytes,prior.sha256)
         IS DISTINCT FROM
       (NEW.upload_intent_id,NEW.producer_id,NEW.capture_sequence,NEW.segment_start_ms,NEW.size_bytes,NEW.sha256) THEN
      RAISE EXCEPTION 'capture artifact seal replay differs';
    END IF;
    RETURN NEW;
  END IF;
  IF EXISTS(SELECT 1 FROM recording_capture_producer_results WHERE producer_id=NEW.producer_id) THEN
    RAISE EXCEPTION 'artifact seal cannot extend a terminal producer';
  END IF;
  SELECT count(*) INTO outstanding FROM recording_capture_artifact_seals s
    WHERE s.producer_id=NEW.producer_id
      AND NOT EXISTS(SELECT 1 FROM recording_capture_artifact_results r WHERE r.upload_intent_id=s.upload_intent_id);
  IF outstanding>=lim THEN RAISE EXCEPTION 'capture producer sealed-intent limit exceeded'; END IF;
  IF NEW.sealed_at IS DISTINCT FROM transaction_timestamp() THEN
    RAISE EXCEPTION 'capture artifact seal time is not DB-authored';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_capture_artifact_seals_validate BEFORE INSERT ON recording_capture_artifact_seals FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_artifact_seal();

CREATE FUNCTION recording_surrender_validate_producer_result() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE seals INTEGER; unresolved INTEGER; current_lease BOOLEAN; recovery_reason TEXT;
BEGIN
  IF NEW.result_at IS DISTINCT FROM transaction_timestamp() THEN
    RAISE EXCEPTION 'capture producer result time is not DB-authored';
  END IF;
  SELECT count(*),count(*) FILTER(WHERE result.upload_intent_id IS NULL) INTO seals,unresolved
  FROM recording_capture_artifact_seals seal
  LEFT JOIN recording_capture_artifact_results result ON result.upload_intent_id=seal.upload_intent_id
  WHERE seal.producer_id=NEW.producer_id;
  IF (NEW.result='abandoned_empty' AND seals<>0)
     OR (NEW.result='completed' AND seals=0)
     OR (NEW.result<>'abandoned_empty' AND unresolved<>0) THEN
    RAISE EXCEPTION 'capture producer terminal result does not seal its artifacts';
  END IF;
	SELECT job.status='leased' AND job.lease_token=p.lease_token
	       AND job.lease_owner=p.worker_id AND job.lease_expires_at>transaction_timestamp()
	  INTO current_lease
	FROM recording_capture_producers p
	JOIN recording_jobs job ON job.id=p.recording_job_id
	WHERE p.id=NEW.producer_id;
	IF NOT FOUND THEN RAISE EXCEPTION 'capture producer result lacks producer authority'; END IF;
	SELECT grant_row.revoke_reason INTO recovery_reason
	FROM recording_job_recovery_grants grant_row
	WHERE grant_row.producer_id=NEW.producer_id;
	IF NEW.result IN('completed','abandoned_empty') THEN
	  IF NOT COALESCE(current_lease,false) AND recovery_reason IS DISTINCT FROM 'recovery_completed' THEN
	    RAISE EXCEPTION 'capture producer terminal result lacks current lease or completed recovery authority';
	  END IF;
	ELSIF NEW.result='security_revoked' THEN
	  IF recovery_reason IS DISTINCT FROM 'security_incident' OR NEW.detail_class<>'recovery_capability_revoked' THEN
	    RAISE EXCEPTION 'capture producer security result lacks exact recovery revocation';
	  END IF;
	ELSIF NEW.result='host_unreachable_unrecoverable' THEN
	  IF recovery_reason IS DISTINCT FROM 'recovery_grace_expired' OR NEW.detail_class<>'recovery_grace_expired' THEN
	    RAISE EXCEPTION 'capture producer expiry result lacks exact recovery revocation';
	  END IF;
	END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER recording_capture_producer_results_artifacts
AFTER INSERT ON recording_capture_producer_results DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_producer_result();

CREATE FUNCTION recording_surrender_validate_unique_head() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE matches INTEGER;
BEGIN
  IF TG_OP<>'UPDATE'
     OR NEW.recording_job_id<>OLD.recording_job_id
     OR NEW.lease_token<>OLD.lease_token
     OR NEW.version<>OLD.version+1
     OR NEW.upload_intent_id IS NULL OR NEW.clip_id IS NULL OR NEW.capture_sequence IS NULL
     OR NEW.advanced_at IS DISTINCT FROM transaction_timestamp() THEN
    RAISE EXCEPTION 'invalid accepted-unique head transition';
  END IF;
  SELECT count(*) INTO matches
  FROM recording_capture_artifact_results result
  JOIN recording_capture_artifact_seals seal ON seal.upload_intent_id=result.upload_intent_id
  JOIN recording_capture_producers producer ON producer.id=seal.producer_id
  JOIN recording_clips clip ON clip.id=result.clip_id
  WHERE result.upload_intent_id=NEW.upload_intent_id
    AND result.result='accepted_unique' AND result.clip_id=NEW.clip_id AND result.head_version=NEW.version
    AND seal.capture_sequence=NEW.capture_sequence
    AND producer.recording_job_id=NEW.recording_job_id AND producer.lease_token=NEW.lease_token
    AND clip.recording_job_id=NEW.recording_job_id
    AND clip.capture_sequence=NEW.capture_sequence AND clip.capture_lease_token=NEW.lease_token;
  IF matches<>1 THEN RAISE EXCEPTION 'accepted-unique head lacks exact artifact result'; END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER recording_job_unique_heads_transition
AFTER UPDATE ON recording_job_unique_heads DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_unique_head();
CREATE FUNCTION recording_surrender_validate_artifact_result() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE matches INTEGER;
BEGIN
  IF NEW.result_at IS DISTINCT FROM transaction_timestamp() THEN
    RAISE EXCEPTION 'capture artifact result time is not DB-authored';
  END IF;
	  IF NEW.result='accepted_unique' THEN
    SELECT count(*) INTO matches
	    FROM recording_capture_artifact_seals seal
	    JOIN recording_capture_producers producer ON producer.id=seal.producer_id
	    JOIN recording_job_unique_heads head ON head.recording_job_id=producer.recording_job_id AND head.lease_token=producer.lease_token
	    JOIN recording_upload_intents intent ON intent.id=seal.upload_intent_id
	    JOIN recording_clips clip ON clip.id=NEW.clip_id
	    WHERE seal.upload_intent_id=NEW.upload_intent_id
	      AND head.upload_intent_id=NEW.upload_intent_id AND head.clip_id=NEW.clip_id
	      AND head.version=NEW.head_version AND head.capture_sequence=seal.capture_sequence
	      AND clip.recording_job_id=producer.recording_job_id
	      AND clip.capture_sequence=seal.capture_sequence AND clip.capture_lease_token=producer.lease_token
	      AND clip.storage_destination_id=intent.storage_destination_id
	      AND clip.endpoint=intent.endpoint AND clip.bucket=intent.bucket AND clip.object_key=intent.object_key
	      AND clip.size_bytes=seal.size_bytes AND clip.sha256=seal.sha256
	      AND (extract(epoch FROM clip.clip_start_at)*1000)::bigint=seal.segment_start_ms;
	    IF matches<>1 THEN RAISE EXCEPTION 'accepted artifact result lacks exact generation head'; END IF;
	  ELSIF NEW.result='exact_replay' THEN
	    SELECT count(*) INTO matches
	    FROM recording_capture_artifact_seals seal
	    JOIN recording_capture_producers producer ON producer.id=seal.producer_id
	    JOIN recording_job_unique_heads head ON head.recording_job_id=producer.recording_job_id AND head.lease_token=producer.lease_token
	    JOIN recording_upload_intents intent ON intent.id=seal.upload_intent_id
	    JOIN recording_clips clip ON clip.id=NEW.clip_id
	    WHERE seal.upload_intent_id=NEW.upload_intent_id
	      AND head.version=NEW.head_version
	      AND clip.recording_job_id=producer.recording_job_id
	      AND clip.storage_destination_id=intent.storage_destination_id
	      AND clip.endpoint=intent.endpoint AND clip.bucket=intent.bucket AND clip.object_key=intent.object_key
	      AND clip.size_bytes=seal.size_bytes AND clip.sha256=seal.sha256
	      AND (extract(epoch FROM clip.clip_start_at)*1000)::bigint=seal.segment_start_ms;
	    IF matches<>1 THEN RAISE EXCEPTION 'replayed artifact result lacks exact prior clip bytes'; END IF;
	  ELSIF NEW.result='security_revoked' THEN
	    SELECT count(*) INTO matches
	    FROM recording_capture_artifact_seals seal
	    JOIN recording_job_recovery_grants grant_row ON grant_row.producer_id=seal.producer_id
	    WHERE seal.upload_intent_id=NEW.upload_intent_id
	      AND grant_row.revoke_reason='security_incident' AND grant_row.revoked_at=NEW.result_at;
	    IF matches<>1 THEN RAISE EXCEPTION 'artifact security result lacks exact recovery revocation'; END IF;
	  ELSIF NEW.result='host_unreachable_unrecoverable' THEN
	    SELECT count(*) INTO matches
	    FROM recording_capture_artifact_seals seal
	    JOIN recording_job_recovery_grants grant_row ON grant_row.producer_id=seal.producer_id
	    WHERE seal.upload_intent_id=NEW.upload_intent_id
	      AND grant_row.revoke_reason='recovery_grace_expired' AND grant_row.revoked_at=NEW.result_at
	      AND grant_row.upload_grace_until<=NEW.result_at;
	    IF matches<>1 THEN RAISE EXCEPTION 'artifact expiry result lacks exact recovery revocation'; END IF;
	  END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER recording_capture_artifact_results_head
AFTER INSERT ON recording_capture_artifact_results DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_artifact_result();
CREATE TRIGGER recording_job_unique_heads_no_delete BEFORE DELETE ON recording_job_unique_heads FOR EACH ROW EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_job_unique_heads_no_truncate BEFORE TRUNCATE ON recording_job_unique_heads FOR EACH STATEMENT EXECUTE FUNCTION recording_surrender_freeze_rows();

CREATE FUNCTION recording_surrender_request_sha(
  p_attempt UUID,p_reason TEXT,p_error_text TEXT,p_head BIGINT,p_intent UUID,p_clip BIGINT,
  p_spool_count INTEGER,p_spool_bytes BIGINT,p_in_flight INTEGER
) RETURNS TEXT LANGUAGE SQL IMMUTABLE AS $$
  SELECT encode(sha256(convert_to(
    'recording-surrender-request-v1'||chr(10)
    ||octet_length('1')::text||':1'||chr(10)
    ||octet_length(p_attempt::text)::text||':'||p_attempt::text||chr(10)
    ||octet_length(p_reason)::text||':'||p_reason||chr(10)
    ||octet_length(p_error_text)::text||':'||p_error_text||chr(10)
    ||octet_length(p_head::text)::text||':'||p_head::text||chr(10)
    ||octet_length(COALESCE(p_intent::text,''))::text||':'||COALESCE(p_intent::text,'')||chr(10)
    ||octet_length(p_clip::text)::text||':'||p_clip::text||chr(10)
    ||octet_length(p_spool_count::text)::text||':'||p_spool_count::text||chr(10)
    ||octet_length(p_spool_bytes::text)::text||':'||p_spool_bytes::text||chr(10)
    ||octet_length(p_in_flight::text)::text||':'||p_in_flight::text||chr(10)
  ,'UTF8')),'hex')
$$;

CREATE FUNCTION recording_surrender_validate_attempt_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE generation_owner TEXT; generation_node BIGINT; exact_kind TEXT; exact_capture TEXT;
BEGIN
	  SELECT generation.lease_owner,generation.node_id,job.kind,recording.capture_via
	    INTO generation_owner,generation_node,exact_kind,exact_capture
	  FROM recording_job_lease_generations generation
	  JOIN recording_jobs job ON job.id=generation.recording_job_id
	  JOIN recordings recording ON recording.id=job.recording_id
	  WHERE generation.recording_job_id=NEW.recording_job_id AND generation.lease_token=NEW.lease_token;
  IF NOT FOUND OR generation_owner<>NEW.worker_id OR generation_node IS DISTINCT FROM NEW.node_id
	 OR exact_kind<>'continuous_window' OR exact_capture<>'cloud'
     OR NEW.requested_at IS DISTINCT FROM transaction_timestamp()
     OR NEW.request_sha256 IS DISTINCT FROM recording_surrender_request_sha(
       NEW.id,NEW.reason,NEW.error_text,NEW.expected_head_version,NEW.expected_upload_intent_id,
       COALESCE(NEW.expected_clip_id,0),NEW.spool_count,NEW.spool_bytes,NEW.in_flight_count
     ) THEN
    RAISE EXCEPTION 'surrender attempt identity is not canonical';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_job_surrender_attempts_validate
BEFORE INSERT ON recording_job_surrender_attempts FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_attempt_insert();

CREATE FUNCTION recording_surrender_validate_attempt_result() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE target UUID; attempts INTEGER; results INTEGER; attempt_row RECORD; result_row RECORD; head_row RECORD; job_row RECORD; generation_attempt INTEGER; expected_retry TIMESTAMPTZ; current_generation BOOLEAN; head_matches BOOLEAN; spool_unsafe BOOLEAN;
BEGIN
  target:=COALESCE(NULLIF(to_jsonb(NEW)->>'attempt_id','')::uuid,NULLIF(to_jsonb(NEW)->>'id','')::uuid);
  SELECT count(*) INTO attempts FROM recording_job_surrender_attempts WHERE id=target;
  SELECT count(*) INTO results FROM recording_job_surrender_results WHERE attempt_id=target;
  IF attempts<>1 OR results<>1 THEN RAISE EXCEPTION 'surrender attempt requires exactly one terminal result'; END IF;
  SELECT * INTO attempt_row FROM recording_job_surrender_attempts WHERE id=target;
  SELECT * INTO result_row FROM recording_job_surrender_results WHERE attempt_id=target;
  IF result_row.result_at IS DISTINCT FROM transaction_timestamp() THEN
    RAISE EXCEPTION 'surrender result time is not DB-authored';
  END IF;
	  SELECT * INTO head_row FROM recording_job_unique_heads
    WHERE recording_job_id=attempt_row.recording_job_id AND lease_token=attempt_row.lease_token;
  IF NOT FOUND
     OR (result_row.current_head_version,result_row.current_upload_intent_id,result_row.current_clip_id)
          IS DISTINCT FROM
        (head_row.version,head_row.upload_intent_id,head_row.clip_id) THEN
	    RAISE EXCEPTION 'surrender result does not bind the current accepted-unique head';
	  END IF;
	  SELECT * INTO job_row FROM recording_jobs WHERE id=attempt_row.recording_job_id;
	  current_generation:=job_row.status='leased' AND job_row.lease_token=attempt_row.lease_token
	    AND job_row.lease_owner=attempt_row.worker_id;
	  head_matches:=(attempt_row.expected_head_version,attempt_row.expected_upload_intent_id,attempt_row.expected_clip_id)
	    IS NOT DISTINCT FROM (head_row.version,head_row.upload_intent_id,head_row.clip_id);
	  SELECT EXISTS(
	    SELECT 1 FROM recording_capture_producers producer
	    WHERE producer.recording_job_id=attempt_row.recording_job_id AND producer.lease_token=attempt_row.lease_token
	      AND NOT EXISTS(SELECT 1 FROM recording_capture_producer_results producer_result WHERE producer_result.producer_id=producer.id)
	    UNION ALL
	    SELECT 1 FROM recording_capture_artifact_seals seal
	    JOIN recording_capture_producers producer ON producer.id=seal.producer_id
	    WHERE producer.recording_job_id=attempt_row.recording_job_id AND producer.lease_token=attempt_row.lease_token
	      AND NOT EXISTS(SELECT 1 FROM recording_capture_artifact_results artifact_result WHERE artifact_result.upload_intent_id=seal.upload_intent_id)
	  ) INTO spool_unsafe;
	  IF result_row.result='committed' THEN
	    SELECT attempt_count INTO generation_attempt FROM recording_job_lease_generations
      WHERE recording_job_id=attempt_row.recording_job_id AND lease_token=attempt_row.lease_token;
    expected_retry:=result_row.result_at+CASE
      WHEN NOT result_row.alternate_available OR head_row.version>0 THEN interval '0'
      WHEN generation_attempt<=1 THEN interval '1 minute'
      WHEN generation_attempt=2 THEN interval '2 minutes'
      ELSE interval '5 minutes' END;
	    IF NOT head_matches OR spool_unsafe
	       OR job_row.status<>'pending' OR job_row.lease_owner IS NOT NULL OR job_row.lease_token IS NOT NULL
       OR job_row.handoff_owner IS DISTINCT FROM attempt_row.worker_id
       OR job_row.handoff_until IS DISTINCT FROM result_row.handoff_until
       OR job_row.scheduled_for IS DISTINCT FROM result_row.next_retry_at
       OR result_row.had_clips IS DISTINCT FROM (head_row.version>0)
       OR result_row.next_retry_at IS DISTINCT FROM expected_retry
       OR result_row.handoff_until IS DISTINCT FROM result_row.result_at+CASE WHEN result_row.alternate_available THEN interval '5 minutes' ELSE interval '0' END THEN
	      RAISE EXCEPTION 'committed surrender result does not seal the job transition';
	    END IF;
	  ELSIF result_row.result='stale_fence' THEN
	    IF current_generation THEN RAISE EXCEPTION 'stale-fence surrender result is not stale'; END IF;
	  ELSIF result_row.result='window_closed' THEN
	    IF NOT current_generation OR job_row.window_end_at IS NULL OR transaction_timestamp()<job_row.window_end_at THEN
	      RAISE EXCEPTION 'window-closed surrender result has an open or stale generation';
	    END IF;
	  ELSIF result_row.result='stale_progress' THEN
	    IF NOT current_generation OR job_row.window_end_at IS NULL OR transaction_timestamp()>=job_row.window_end_at OR head_matches THEN
	      RAISE EXCEPTION 'stale-progress surrender result does not bind changed progress';
	    END IF;
	  ELSIF result_row.result='ineligible_spool' THEN
	    IF NOT current_generation OR job_row.window_end_at IS NULL OR transaction_timestamp()>=job_row.window_end_at OR NOT head_matches OR NOT spool_unsafe THEN
	      RAISE EXCEPTION 'ineligible-spool surrender result has no unsafe durable producer';
	    END IF;
	  END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER recording_job_surrender_attempts_result_seal AFTER INSERT ON recording_job_surrender_attempts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_attempt_result();
CREATE CONSTRAINT TRIGGER recording_job_surrender_results_attempt_seal AFTER INSERT ON recording_job_surrender_results DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_attempt_result();

-- Backfill only currently fenced generations. Historical unfenced attempts are
-- deliberately not fabricated.
-- Hold writers from the snapshot through trigger installation. Without this
-- explicit fence, a lease could be created after the backfill SELECT but before
-- recording_jobs_surrender_generation exists and would have no generation row.
LOCK TABLE recording_jobs IN SHARE ROW EXCLUSIVE MODE;

INSERT INTO recording_job_lease_generations(recording_job_id,lease_token,lease_owner,node_id,attempt_count,claimed_at,initial_lease_expires_at)
SELECT j.id,j.lease_token,j.lease_owner,
       COALESCE(CASE WHEN j.lease_owner~'^node:[0-9]+$' THEN substring(j.lease_owner from 6)::bigint END,d.node_id),
       j.attempt_count,LEAST(j.updated_at,transaction_timestamp()),j.lease_expires_at
FROM recording_jobs j
LEFT JOIN recorder_droplets d ON d.name=j.lease_owner
WHERE j.status='leased' AND j.lease_token IS NOT NULL AND j.lease_owner IS NOT NULL AND j.lease_expires_at>transaction_timestamp()
ON CONFLICT DO NOTHING;

INSERT INTO recording_job_unique_heads(recording_job_id,lease_token)
SELECT g.recording_job_id,g.lease_token
FROM recording_job_lease_generations g
ON CONFLICT DO NOTHING;

CREATE FUNCTION recording_surrender_validate_generation_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expected_node BIGINT; exact BOOLEAN;
BEGIN
  SELECT CASE WHEN job.lease_owner~'^node:[0-9]+$' THEN substring(job.lease_owner from 6)::bigint ELSE droplet.node_id END,
         job.status='leased' AND job.lease_token=NEW.lease_token AND job.lease_owner=NEW.lease_owner
           AND job.attempt_count=NEW.attempt_count AND job.lease_expires_at=NEW.initial_lease_expires_at
    INTO expected_node,exact
  FROM recording_jobs job LEFT JOIN recorder_droplets droplet ON droplet.name=job.lease_owner
  WHERE job.id=NEW.recording_job_id FOR UPDATE OF job;
  IF NOT FOUND OR NOT exact OR expected_node IS DISTINCT FROM NEW.node_id
     OR NEW.claimed_at IS DISTINCT FROM transaction_timestamp() THEN
    RAISE EXCEPTION 'lease generation insert does not match current DB lease transition';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_job_lease_generations_validate_insert
BEFORE INSERT ON recording_job_lease_generations FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_generation_insert();

CREATE FUNCTION recording_surrender_validate_head_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.version<>0 OR NEW.upload_intent_id IS NOT NULL OR NEW.clip_id IS NOT NULL
     OR NEW.capture_sequence IS NOT NULL OR NEW.advanced_at IS NOT NULL
     OR NOT EXISTS(SELECT 1 FROM recording_job_lease_generations generation
                   WHERE generation.recording_job_id=NEW.recording_job_id AND generation.lease_token=NEW.lease_token) THEN
    RAISE EXCEPTION 'accepted-unique head must begin empty for an exact lease generation';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_job_unique_heads_validate_insert
BEFORE INSERT ON recording_job_unique_heads FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_head_insert();

-- Once a v1 producer may have created local bytes, no generic job writer may
-- clear its lease fence. The centralized expiry authority first creates one
-- active upload-only grant per nonterminal producer and seals the expiry event;
-- ordinary complete/fail/cancel/surrender paths must terminalize the producer
-- (and every artifact) before releasing the main lease.
CREATE FUNCTION recording_surrender_protect_durable_bytes() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE nonterminal INTEGER; granted INTEGER;
BEGIN
  IF OLD.status='leased' AND OLD.lease_token IS NOT NULL
     AND (NEW.status IS DISTINCT FROM 'leased' OR NEW.lease_token IS DISTINCT FROM OLD.lease_token) THEN
    SELECT count(*) INTO nonterminal
    FROM recording_capture_producers producer
    WHERE producer.recording_job_id=OLD.id AND producer.lease_token=OLD.lease_token
      AND NOT EXISTS(SELECT 1 FROM recording_capture_producer_results result WHERE result.producer_id=producer.id);
    IF nonterminal>0 THEN
      SELECT count(*) INTO granted
      FROM recording_capture_producers producer
      JOIN recording_job_recovery_grants grant_row ON grant_row.producer_id=producer.id
      WHERE producer.recording_job_id=OLD.id AND producer.lease_token=OLD.lease_token
        AND NOT EXISTS(SELECT 1 FROM recording_capture_producer_results result WHERE result.producer_id=producer.id)
        AND grant_row.revoked_at IS NULL AND grant_row.upload_grace_until>transaction_timestamp();
      IF granted<>nonterminal OR NOT EXISTS(
        SELECT 1 FROM recording_job_lease_expiry_events event
        WHERE event.recording_job_id=OLD.id AND event.lease_token=OLD.lease_token
          AND event.recovery_grant_count=nonterminal
      ) THEN
        RAISE EXCEPTION 'recording lease has nonterminal durable capture authority';
      END IF;
    END IF;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_jobs_surrender_durable_bytes
BEFORE UPDATE ON recording_jobs FOR EACH ROW EXECUTE FUNCTION recording_surrender_protect_durable_bytes();

CREATE FUNCTION recording_surrender_record_lease_generation() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE resolved_node BIGINT;
BEGIN
  IF NEW.status='leased' AND NEW.lease_token IS NOT NULL
     AND (OLD.status IS DISTINCT FROM 'leased' OR OLD.lease_token IS DISTINCT FROM NEW.lease_token) THEN
    IF NEW.lease_owner~'^node:[0-9]+$' THEN
      resolved_node:=substring(NEW.lease_owner from 6)::bigint;
    ELSE
      SELECT node_id INTO resolved_node FROM recorder_droplets WHERE name=NEW.lease_owner;
    END IF;
    INSERT INTO recording_job_lease_generations(recording_job_id,lease_token,lease_owner,node_id,attempt_count,initial_lease_expires_at)
      VALUES(NEW.id,NEW.lease_token,NEW.lease_owner,resolved_node,NEW.attempt_count,NEW.lease_expires_at);
    INSERT INTO recording_job_unique_heads(recording_job_id,lease_token) VALUES(NEW.id,NEW.lease_token);
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_jobs_surrender_generation
AFTER UPDATE ON recording_jobs FOR EACH ROW EXECUTE FUNCTION recording_surrender_record_lease_generation();

-- The scheduler and droplet controller call this one authority instead of
-- performing independent lease-clearing UPDATEs. It preserves generation facts,
-- opens upload-only recovery grants for every nonterminal producer, and chooses
-- old-owner exclusion from current capacity under the same global fence as claim.
CREATE FUNCTION recording_surrender_reclaim_expired() RETURNS BIGINT LANGUAGE plpgsql AS $$
DECLARE candidate RECORD; j RECORD; p RECORD; alternate BOOLEAN; grants INTEGER; changed BIGINT:=0; grant_id UUID;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('recording-surrender-cloud-capacity-v1',0));
  -- Expired upload-only capabilities close honestly. No later main-token or
  -- worker action may turn missing bytes into accepted footage.
  FOR p IN
    SELECT grant_row.id,grant_row.recording_job_id,grant_row.producer_id
    FROM recording_job_recovery_grants grant_row
    WHERE grant_row.revoked_at IS NULL AND grant_row.upload_grace_until<=transaction_timestamp()
    ORDER BY grant_row.recording_job_id,grant_row.id
  LOOP
    PERFORM pg_advisory_xact_lock(hashtextextended('recording-surrender-job:'||p.recording_job_id::text,0));
    PERFORM 1 FROM recording_job_recovery_grants grant_row
      WHERE grant_row.id=p.id AND grant_row.revoked_at IS NULL
        AND grant_row.upload_grace_until<=transaction_timestamp()
      FOR UPDATE;
    IF NOT FOUND THEN CONTINUE; END IF;
    INSERT INTO recording_capture_artifact_results(upload_intent_id,result)
    SELECT seal.upload_intent_id,'host_unreachable_unrecoverable'
    FROM recording_capture_artifact_seals seal
    WHERE seal.producer_id=p.producer_id
      AND NOT EXISTS(SELECT 1 FROM recording_capture_artifact_results result WHERE result.upload_intent_id=seal.upload_intent_id)
    ON CONFLICT DO NOTHING;
    INSERT INTO recording_capture_producer_results(producer_id,result,detail_class)
    VALUES(p.producer_id,'host_unreachable_unrecoverable','recovery_grace_expired')
    ON CONFLICT DO NOTHING;
    UPDATE recording_job_recovery_grants
    SET revoked_at=transaction_timestamp(),revoke_reason='recovery_grace_expired'
    WHERE id=p.id;
  END LOOP;
  FOR candidate IN
    SELECT jobs.id,jobs.lease_token,jobs.lease_owner,recording.capture_via
    FROM recording_jobs jobs JOIN recordings recording ON recording.id=jobs.recording_id
    WHERE jobs.status='leased' AND jobs.lease_expires_at<transaction_timestamp()
    ORDER BY jobs.id
  LOOP
    PERFORM pg_advisory_xact_lock(hashtextextended('recording-surrender-job:'||candidate.id::text,0));
    SELECT jobs.id,jobs.lease_token,jobs.lease_owner,recording.capture_via INTO j
    FROM recording_jobs jobs JOIN recordings recording ON recording.id=jobs.recording_id
    WHERE jobs.id=candidate.id AND jobs.status='leased' AND jobs.lease_expires_at<transaction_timestamp()
    FOR UPDATE OF jobs;
    IF NOT FOUND THEN CONTINUE; END IF;
    IF j.capture_via<>'cloud' THEN
      UPDATE recording_jobs SET status='pending',lease_owner=NULL,lease_expires_at=NULL,lease_token=NULL,
        relay_fairness_started_at=NULL,updated_at=transaction_timestamp() WHERE id=j.id;
      changed:=changed+1;
      CONTINUE;
    END IF;
    IF j.lease_token IS NULL OR NOT EXISTS(
      SELECT 1 FROM recording_job_lease_generations g
      WHERE g.recording_job_id=j.id AND g.lease_token=j.lease_token
    ) THEN
      UPDATE recording_jobs SET status='pending',lease_owner=NULL,lease_expires_at=NULL,lease_token=NULL,
        relay_fairness_started_at=NULL,updated_at=transaction_timestamp() WHERE id=j.id;
      changed:=changed+1;
      CONTINUE;
    END IF;
    SELECT EXISTS(
      SELECT 1 FROM recorder_droplets d
      WHERE d.name<>j.lease_owner AND d.state IN('provisioning','active')
        AND d.last_seen_at>=transaction_timestamp()-interval '2 minutes'
        AND (SELECT count(*) FROM recording_jobs live
             WHERE live.status='leased' AND live.lease_owner=d.name
               AND live.lease_expires_at>transaction_timestamp())<d.capacity
    ) INTO alternate;
    grants:=0;
    FOR p IN
      SELECT producer.id,producer.recovery_secret_sha256
      FROM recording_capture_producers producer
      WHERE producer.recording_job_id=j.id AND producer.lease_token=j.lease_token
        AND NOT EXISTS(SELECT 1 FROM recording_capture_producer_results result WHERE result.producer_id=producer.id)
      ORDER BY producer.capture_ordinal
    LOOP
      grant_id:=gen_random_uuid();
      INSERT INTO recording_job_recovery_grants(id,recording_job_id,lease_token,producer_id,recovery_secret_sha256,granted_at,upload_grace_until)
      VALUES(grant_id,j.id,j.lease_token,p.id,p.recovery_secret_sha256,transaction_timestamp(),transaction_timestamp()+interval '30 minutes')
      ON CONFLICT(recording_job_id,lease_token,producer_id) DO NOTHING;
      IF FOUND THEN grants:=grants+1; END IF;
      INSERT INTO recording_job_recovery_grant_intents(grant_id,upload_intent_id)
      SELECT g.id,seals.upload_intent_id
      FROM recording_job_recovery_grants g
      JOIN recording_capture_artifact_seals seals ON seals.producer_id=p.id
      LEFT JOIN recording_capture_artifact_results terminal ON terminal.upload_intent_id=seals.upload_intent_id
      WHERE g.recording_job_id=j.id AND g.lease_token=j.lease_token AND g.producer_id=p.id AND terminal.upload_intent_id IS NULL
      ON CONFLICT DO NOTHING;
    END LOOP;
    SELECT count(*) INTO grants FROM recording_job_recovery_grants grant_row
      WHERE grant_row.recording_job_id=j.id AND grant_row.lease_token=j.lease_token;
    INSERT INTO recording_job_lease_expiry_events(recording_job_id,lease_token,lease_owner,recovery_grant_count,alternate_available,handoff_until,reclaimed_at)
    VALUES(j.id,j.lease_token,j.lease_owner,grants,alternate,transaction_timestamp()+CASE WHEN alternate THEN interval '5 minutes' ELSE interval '0' END,transaction_timestamp())
    ON CONFLICT DO NOTHING;
    UPDATE recording_jobs SET status='pending',scheduled_for=transaction_timestamp(),lease_owner=NULL,lease_expires_at=NULL,lease_token=NULL,
      handoff_owner=j.lease_owner,handoff_until=transaction_timestamp()+CASE WHEN alternate THEN interval '5 minutes' ELSE interval '0' END,
      relay_fairness_started_at=NULL,updated_at=transaction_timestamp() WHERE id=j.id;
    changed:=changed+1;
  END LOOP;
  RETURN changed;
END $$;

-- Pin every trigger/authority function to the schema in which this migration
-- installed its objects. Explicit pg_temp-last prevents a caller-created temp
-- relation or helper from shadowing either catalog or authority objects.
DO $pin_recording_surrender_search_path$
DECLARE install_schema TEXT:=current_schema(); signature TEXT;
BEGIN
  FOREACH signature IN ARRAY ARRAY[
    'recording_surrender_freeze_rows()',
    'recording_surrender_validate_grant_update()',
    'recording_surrender_validate_grant_insert()',
    'recording_surrender_validate_grant_intent_insert()',
    'recording_surrender_validate_expiry_event_insert()',
    'recording_surrender_revoke_recovery_grant(uuid,text)',
    'recording_surrender_validate_producer_insert()',
    'recording_surrender_validate_artifact_seal()',
    'recording_surrender_validate_producer_result()',
    'recording_surrender_validate_unique_head()',
    'recording_surrender_validate_artifact_result()',
    'recording_surrender_request_sha(uuid,text,text,bigint,uuid,bigint,integer,bigint,integer)',
    'recording_surrender_validate_attempt_insert()',
    'recording_surrender_validate_attempt_result()',
    'recording_surrender_validate_generation_insert()',
	    'recording_surrender_validate_head_insert()',
	    'recording_surrender_protect_durable_bytes()',
    'recording_surrender_record_lease_generation()',
    'recording_surrender_reclaim_expired()'
  ] LOOP
    EXECUTE format('ALTER FUNCTION %I.%s SET search_path = %I, pg_catalog, pg_temp',install_schema,signature,install_schema);
  END LOOP;
END
$pin_recording_surrender_search_path$;
