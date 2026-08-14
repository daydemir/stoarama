-- Fenced continuous-recording surrender transport and crash-recovery authority.
--
-- This migration is deliberately additive. Older recorder builds keep using the
-- legacy surrender body while v1 workers opt into the generation/producer ledger.

-- A SHA is not a v1 clip identity: two camera-time units may legitimately
-- contain equal bytes. Preserve the legacy generation+SHA concurrency backstop
-- while giving explicitly typed v1 clips their intent/sequence identity.
ALTER TABLE recording_clips ADD COLUMN surrender_transport_version SMALLINT NOT NULL DEFAULT 0;
DROP INDEX uq_recording_clips_capture_sha256;
CREATE UNIQUE INDEX uq_recording_clips_capture_sha256
  ON recording_clips(recording_job_id,capture_lease_token,sha256)
  WHERE capture_lease_token IS NOT NULL AND sha256<>'' AND surrender_transport_version=0;

-- R10 claim credentials split new-claim authority from heartbeat and already-
-- fenced media authority. Existing live jobs are intentionally legacy_unknown:
-- their historical authenticating token was never persisted and is not guessed.
ALTER TABLE node_tokens
  ADD COLUMN recording_claim_generation BIGINT,
  ADD COLUMN recording_claim_purpose TEXT NOT NULL DEFAULT 'legacy_full'
    CHECK(recording_claim_purpose IN('legacy_full','claim_current','existing_fence_only'));
ALTER TABLE recording_jobs
  ADD COLUMN lease_node_token_id BIGINT REFERENCES node_tokens(id) ON DELETE RESTRICT,
  ADD COLUMN lease_claim_generation BIGINT,
  ADD COLUMN lease_credential_state TEXT NOT NULL DEFAULT 'legacy_unknown'
    CHECK(lease_credential_state IN('legacy_unknown','exact'));

ALTER TABLE recording_jobs ADD CONSTRAINT recording_jobs_lease_credential_identity_chk CHECK(
  (lease_credential_state='legacy_unknown' AND lease_node_token_id IS NULL AND lease_claim_generation IS NULL)
  OR (lease_credential_state='exact' AND lease_token IS NOT NULL
      AND lease_node_token_id IS NOT NULL AND lease_claim_generation>0)
);

CREATE TABLE recording_worker_claim_heads (
  node_id BIGINT PRIMARY KEY REFERENCES nodes(id) ON DELETE RESTRICT,
  generation BIGINT NOT NULL CHECK(generation>0),
  claim_token_id BIGINT NOT NULL UNIQUE REFERENCES node_tokens(id) ON DELETE RESTRICT,
  state TEXT NOT NULL CHECK(state IN('enabled','recovery_blocked','successor_pending')),
  blocked_at TIMESTAMPTZ,
  block_reason TEXT CHECK(block_reason IS NULL OR block_reason IN('durable_recovery','security_incident')),
  CHECK((state='enabled' AND blocked_at IS NULL AND block_reason IS NULL)
     OR (state<>'enabled' AND blocked_at IS NOT NULL AND block_reason IS NOT NULL))
);

CREATE TABLE recording_worker_claim_generation_events (
  node_id BIGINT NOT NULL REFERENCES nodes(id) ON DELETE RESTRICT,
  generation BIGINT NOT NULL CHECK(generation>0),
  predecessor_generation BIGINT,
  claim_token_id BIGINT NOT NULL REFERENCES node_tokens(id) ON DELETE RESTRICT,
  event_type TEXT NOT NULL CHECK(event_type IN('baseline','reenrolled','recovery_blocked','successor_proposed','enabled','retired','host_lost')),
  event_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  facts_sha256 TEXT NOT NULL CHECK(facts_sha256~'^[0-9a-f]{64}$'),
  PRIMARY KEY(node_id,generation,event_type),
  CHECK((generation=1 AND predecessor_generation IS NULL) OR (generation>1 AND predecessor_generation=generation-1))
);

CREATE FUNCTION recording_surrender_validate_claim_generation_event() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE token RECORD; expected TEXT;
BEGIN
  NEW.event_at:=transaction_timestamp();
  IF NEW.event_type IN('retired','host_lost') THEN
    SELECT * INTO token FROM node_tokens WHERE id=NEW.claim_token_id FOR SHARE;
		expected:=encode(sha256(convert_to(
		  CASE WHEN NEW.event_type='retired' THEN 'recording-worker-claim-retired-v1' ELSE 'recording-worker-host-lost-v1' END
		  ||chr(0)||NEW.node_id::text||chr(0)||NEW.generation::text||chr(0)||NEW.claim_token_id::text,'UTF8')),'hex');
    IF token.id IS NULL OR token.node_id<>NEW.node_id
       OR token.recording_claim_generation<>NEW.generation
       OR token.revoked_at IS DISTINCT FROM transaction_timestamp()
       OR NEW.facts_sha256<>expected
       OR EXISTS(SELECT 1 FROM recording_jobs job
				 WHERE job.lease_node_token_id=NEW.claim_token_id AND job.status='leased')
		 OR EXISTS(SELECT 1 FROM recording_capture_producers producer
		           WHERE producer.node_id=NEW.node_id
		             AND NOT EXISTS(SELECT 1 FROM recording_capture_producer_results result WHERE result.producer_id=producer.id))
		 OR EXISTS(
		   SELECT 1 FROM recording_capture_set_grants grant
		   JOIN recording_capture_reservation_sets capture_set ON capture_set.id=grant.set_id
		   JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
		   JOIN recording_job_lease_generations lease
		     ON lease.recording_job_id=plan.recording_job_id AND lease.lease_token=plan.lease_token
		   WHERE lease.node_id=NEW.node_id AND plan.origin_claim_generation=NEW.generation
		     AND NOT EXISTS(SELECT 1 FROM recording_capture_set_results result WHERE result.set_id=grant.set_id)
		 ) THEN
		  RAISE EXCEPTION 'claim terminal event lacks exact drained credential';
    END IF;
	ELSIF NEW.event_type='recovery_blocked' THEN
		IF NOT EXISTS(
		  SELECT 1
		  FROM recording_job_lease_generations lease
		  JOIN recording_jobs job ON job.id=lease.recording_job_id AND job.lease_token=lease.lease_token
		  JOIN recording_capture_set_plans plan
		    ON plan.recording_job_id=lease.recording_job_id AND plan.lease_token=lease.lease_token
		  JOIN recording_capture_reservation_sets capture_set ON capture_set.plan_id=plan.id
		  WHERE lease.node_id=NEW.node_id AND plan.origin_claim_generation=NEW.generation
		    AND job.status='leased' AND job.lease_expires_at<transaction_timestamp()
		    AND NOT EXISTS(SELECT 1 FROM recording_capture_set_results result WHERE result.set_id=capture_set.id)
		    AND NEW.facts_sha256=encode(sha256(convert_to(
		      'recording-worker-recovery-block-v1'||chr(0)||NEW.node_id::text||chr(0)||NEW.generation::text
		      ||chr(0)||job.id::text||chr(0)||lease.lease_token::text,'UTF8')),'hex')
		) THEN RAISE EXCEPTION 'claim recovery block event lacks exact expired capture authority'; END IF;
	ELSIF NEW.event_type='successor_proposed' THEN
		IF NOT EXISTS(
		  SELECT 1 FROM recording_worker_claim_successor_proposals proposal
		  WHERE proposal.node_id=NEW.node_id AND proposal.successor_generation=NEW.generation
		    AND proposal.predecessor_generation=NEW.predecessor_generation
		    AND proposal.successor_token_id=NEW.claim_token_id
		    AND NEW.facts_sha256=encode(sha256(convert_to(
		      'recording-claim-successor-v1'||chr(0)||proposal.node_id::text||chr(0)||proposal.predecessor_generation::text
		      ||chr(0)||proposal.successor_generation::text||chr(0)||proposal.id::text||chr(0)||proposal.successor_token_id::text
		      ||chr(0)||proposal.successor_secret_sha256,'UTF8')),'hex')
		) THEN RAISE EXCEPTION 'claim successor event lacks exact proposal'; END IF;
	ELSIF NEW.event_type='enabled' THEN
		IF NOT EXISTS(
		  SELECT 1 FROM recording_worker_claim_successor_proposals proposal
		  JOIN recording_worker_claim_successor_results result ON result.proposal_id=proposal.id AND result.result='enabled'
		  WHERE proposal.node_id=NEW.node_id AND proposal.successor_generation=NEW.generation
		    AND proposal.predecessor_generation=NEW.predecessor_generation
		    AND proposal.successor_token_id=NEW.claim_token_id
		    AND NEW.facts_sha256=encode(sha256(convert_to(
		      'recording-claim-successor-v1'||chr(0)||proposal.node_id::text||chr(0)||proposal.predecessor_generation::text
		      ||chr(0)||proposal.successor_generation::text||chr(0)||proposal.id::text||chr(0)||proposal.successor_token_id::text
		      ||chr(0)||proposal.successor_secret_sha256,'UTF8')),'hex')
		) THEN RAISE EXCEPTION 'claim enabled event lacks exact acknowledged proposal'; END IF;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_worker_claim_generation_events_validate
BEFORE INSERT ON recording_worker_claim_generation_events FOR EACH ROW
EXECUTE FUNCTION recording_surrender_validate_claim_generation_event();

CREATE FUNCTION recording_surrender_validate_claim_token_retirement_seal() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.revoked_at IS NULL AND NEW.revoked_at IS NOT NULL AND NOT EXISTS(
    SELECT 1 FROM recording_worker_claim_generation_events event
    WHERE event.node_id=NEW.node_id AND event.generation=NEW.recording_claim_generation
      AND event.claim_token_id=NEW.id AND event.event_type='retired'
  ) THEN RAISE EXCEPTION 'retired recording claim credential lacks append-only event'; END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER recording_node_tokens_retirement_seal
AFTER UPDATE OF revoked_at ON node_tokens DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION recording_surrender_validate_claim_token_retirement_seal();

CREATE TABLE recording_worker_claim_successor_proposals (
  id UUID PRIMARY KEY,
  node_id BIGINT NOT NULL REFERENCES nodes(id) ON DELETE RESTRICT,
  predecessor_generation BIGINT NOT NULL CHECK(predecessor_generation>0),
  successor_generation BIGINT NOT NULL CHECK(successor_generation=predecessor_generation+1),
  predecessor_token_id BIGINT NOT NULL REFERENCES node_tokens(id) ON DELETE RESTRICT,
  successor_token_id BIGINT NOT NULL UNIQUE REFERENCES node_tokens(id) ON DELETE RESTRICT,
  successor_key_prefix TEXT NOT NULL CHECK(octet_length(successor_key_prefix) BETWEEN 8 AND 32),
  successor_secret_sha256 TEXT NOT NULL CHECK(successor_secret_sha256~'^[0-9a-f]{64}$'),
  proposed_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  UNIQUE(node_id,successor_generation)
);

CREATE TABLE recording_worker_claim_successor_results (
  proposal_id UUID PRIMARY KEY REFERENCES recording_worker_claim_successor_proposals(id) ON DELETE RESTRICT,
  result TEXT NOT NULL CHECK(result='enabled'),
  result_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp()
);

DO $$
BEGIN
  IF EXISTS (
    SELECT node_id FROM node_tokens WHERE revoked_at IS NULL
    GROUP BY node_id HAVING count(*)<>1
  ) THEN
    RAISE EXCEPTION 'recording claim credential migration requires exactly one live token per enrolled node';
  END IF;
  IF EXISTS (
    SELECT 1
    FROM nodes node
    WHERE ((node.node_type='relay' AND node.status='active')
       OR (node.node_type='local_recorder' AND EXISTS(
         SELECT 1 FROM recorder_droplets droplet
         WHERE droplet.node_id=node.id AND droplet.state IN('provisioning','active'))))
      AND NOT EXISTS(SELECT 1 FROM node_tokens token WHERE token.node_id=node.id AND token.revoked_at IS NULL)
  ) THEN
    RAISE EXCEPTION 'recording claim credential migration found a claim-capable node without a live token';
  END IF;
END $$;

UPDATE node_tokens
SET recording_claim_generation=1,recording_claim_purpose='claim_current'
WHERE revoked_at IS NULL;

INSERT INTO recording_worker_claim_heads(node_id,generation,claim_token_id,state)
SELECT node_id,1,id,'enabled' FROM node_tokens WHERE revoked_at IS NULL;

INSERT INTO recording_worker_claim_generation_events(node_id,generation,predecessor_generation,claim_token_id,event_type,facts_sha256)
SELECT node_id,1,NULL,id,'baseline',
       encode(sha256(convert_to('recording-worker-claim-baseline-v1'||chr(0)||node_id::text||chr(0)||id::text,'UTF8')),'hex')
FROM node_tokens WHERE revoked_at IS NULL;

CREATE FUNCTION recording_surrender_initialize_claim_token() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE head RECORD; predecessor BIGINT; facts TEXT;
BEGIN
  IF TG_WHEN='BEFORE' THEN
    IF NEW.recording_claim_generation IS NOT NULL THEN
      IF NEW.recording_claim_purpose<>'existing_fence_only' THEN
        RAISE EXCEPTION 'explicit recording claim generation is reserved for a successor credential';
      END IF;
      SELECT * INTO head FROM recording_worker_claim_heads WHERE node_id=NEW.node_id FOR UPDATE;
      IF head.node_id IS NULL OR head.state<>'recovery_blocked' OR head.block_reason<>'durable_recovery'
         OR NEW.recording_claim_generation<>head.generation+1
         OR EXISTS(SELECT 1 FROM recording_capture_set_grants grant
                   JOIN recording_capture_reservation_sets capture_set ON capture_set.id=grant.set_id
                   JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
                   JOIN recording_job_lease_generations generation
                     ON generation.recording_job_id=plan.recording_job_id AND generation.lease_token=plan.lease_token
                   WHERE generation.node_id=NEW.node_id AND plan.origin_claim_generation=head.generation
                     AND NOT EXISTS(SELECT 1 FROM recording_capture_set_results result WHERE result.set_id=grant.set_id)) THEN
        RAISE EXCEPTION 'explicit recording claim successor lacks terminal blocked authority';
      END IF;
      RETURN NEW;
    END IF;
    SELECT * INTO head FROM recording_worker_claim_heads WHERE node_id=NEW.node_id FOR UPDATE;
    IF head.node_id IS NULL THEN
      NEW.recording_claim_generation:=1;
    ELSE
      IF EXISTS(SELECT 1 FROM node_tokens token WHERE token.id=head.claim_token_id AND token.revoked_at IS NULL)
         OR EXISTS(SELECT 1 FROM recording_jobs job WHERE job.lease_node_token_id=head.claim_token_id AND job.status='leased') THEN
        RAISE EXCEPTION 'node reenrollment cannot replace a live recording claim generation';
      END IF;
      NEW.recording_claim_generation:=head.generation+1;
    END IF;
    NEW.recording_claim_purpose:='claim_current';
    RETURN NEW;
  END IF;
  IF NEW.recording_claim_purpose<>'claim_current' THEN RETURN NULL; END IF;
  SELECT * INTO head FROM recording_worker_claim_heads WHERE node_id=NEW.node_id FOR UPDATE;
  facts:=encode(sha256(convert_to('recording-worker-enrollment-v1'||chr(0)||NEW.node_id::text||chr(0)||NEW.recording_claim_generation::text||chr(0)||NEW.id::text,'UTF8')),'hex');
  IF head.node_id IS NULL THEN
    INSERT INTO recording_worker_claim_generation_events(node_id,generation,predecessor_generation,claim_token_id,event_type,facts_sha256)
    VALUES(NEW.node_id,NEW.recording_claim_generation,NULL,NEW.id,'baseline',facts);
    INSERT INTO recording_worker_claim_heads(node_id,generation,claim_token_id,state)
    VALUES(NEW.node_id,NEW.recording_claim_generation,NEW.id,'enabled');
  ELSE
    INSERT INTO recording_worker_claim_generation_events(node_id,generation,predecessor_generation,claim_token_id,event_type,facts_sha256)
    VALUES(NEW.node_id,NEW.recording_claim_generation,head.generation,NEW.id,'reenrolled',facts);
    UPDATE recording_worker_claim_heads
    SET generation=NEW.recording_claim_generation,claim_token_id=NEW.id,state='enabled',blocked_at=NULL,block_reason=NULL
    WHERE node_id=NEW.node_id;
  END IF;
  RETURN NULL;
END $$;
CREATE TRIGGER recording_surrender_initialize_claim_token_before
BEFORE INSERT ON node_tokens FOR EACH ROW EXECUTE FUNCTION recording_surrender_initialize_claim_token();
CREATE TRIGGER recording_surrender_initialize_claim_token_after
AFTER INSERT ON node_tokens FOR EACH ROW EXECUTE FUNCTION recording_surrender_initialize_claim_token();

-- One DB-owned snapshot grammar is used by both orchestration and triggers.
-- jsonb::text is canonical for these typed facts and independent of session
-- TimeZone/DateStyle; nullable values retain their JSON null tag.
CREATE FUNCTION recording_surrender_source_snapshot(p_recording_id BIGINT) RETURNS JSONB
LANGUAGE sql STABLE AS $$
  SELECT jsonb_build_object('schema','recording-source-snapshot-v1','account_id',recording.account_id,
    'recording_id',recording.id,'stream_id',stream.id,'recording_stream_url',recording.stream_url,
    'capture_via',recording.capture_via,'mode',recording.mode,'source_kind',recording.source_kind,
    'target_fps',recording.target_fps,'stream_source_url',stream.source_url,
    'source_page_url',stream.source_page_url,'provider',stream.provider,'external_id',stream.external_id,
    'source_family',stream.source_family,'capture_type',stream.capture_type,'execution_class',stream.execution_class,
    'execution_config',COALESCE(stream.execution_config_jsonb,'{}'::jsonb),
    'revision',COALESCE((SELECT jsonb_build_object(
      'id',revision.id,'stream_id',revision.stream_id,'actor',revision.actor,'reason',revision.reason,
      'previous_source_url',revision.previous_source_url,'new_source_url',revision.new_source_url,
      'previous_source_page_url',revision.previous_source_page_url,'new_source_page_url',revision.new_source_page_url,
      'previous_source_family',revision.previous_source_family,'new_source_family',revision.new_source_family,
      'previous_capture_type',revision.previous_capture_type,'new_capture_type',revision.new_capture_type,
      'previous_execution_class',revision.previous_execution_class,'new_execution_class',revision.new_execution_class,
      'metadata',revision.metadata_jsonb,
      'created_at_epoch_microseconds',floor(extract(epoch FROM revision.created_at)*1000000)::bigint)
      FROM stream_source_revisions revision
      WHERE revision.stream_id=stream.id ORDER BY revision.id DESC LIMIT 1),'null'::jsonb))
  FROM recordings recording JOIN streams stream ON stream.id=recording.stream_id
  WHERE recording.id=p_recording_id
$$;

CREATE FUNCTION recording_surrender_destination_snapshot(p_recording_id BIGINT) RETURNS JSONB
LANGUAGE sql STABLE AS $$
  SELECT jsonb_build_object('schema','recording-destination-naming-v1','destination_id',destination.id,
    'endpoint',destination.endpoint,'region',destination.region,'bucket',destination.bucket,'key_prefix',destination.key_prefix,
    'naming_profile',recording.naming_profile,'folder_name',recording.folder_name,
    'naming_metadata',recording.naming_metadata_jsonb,'cron_timezone',recording.cron_timezone)
  FROM recordings recording JOIN storage_destinations destination ON destination.id=recording.storage_destination_id
  WHERE recording.id=p_recording_id
$$;

CREATE FUNCTION recording_surrender_capture_config_snapshot(p_recording_id BIGINT,p_job_id BIGINT,p_lease_token UUID) RETURNS JSONB
LANGUAGE sql STABLE AS $$
  SELECT jsonb_build_object('schema','recording-capture-config-v1',
    'recording_id',recording.id,'capture_via',recording.capture_via,'mode',recording.mode,
    'source_kind',recording.source_kind,'target_fps',recording.target_fps,
    'recording_clip_duration_seconds',recording.clip_duration_sec,
    'job_clip_duration_seconds',job.clip_duration_sec,'job_kind',job.kind,
    'window_end_epoch_microseconds',CASE WHEN job.window_end_at IS NULL THEN NULL
      ELSE floor(extract(epoch FROM job.window_end_at)*1000000)::bigint END,
    'timestamp_policy_version',(SELECT admission.policy_version FROM recording_timestamp_contract_admissions admission
      WHERE admission.recording_job_id=job.id AND admission.lease_token=p_lease_token),
    'source',recording_surrender_source_snapshot(recording.id),
    'destination',recording_surrender_destination_snapshot(recording.id))
  FROM recordings recording JOIN recording_jobs job ON job.recording_id=recording.id
  WHERE recording.id=p_recording_id AND job.id=p_job_id
$$;

CREATE FUNCTION recording_surrender_normalize_lease_credential() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
BEGIN
  IF NEW.lease_token IS NULL THEN
    NEW.lease_node_token_id:=NULL;
    NEW.lease_claim_generation:=NULL;
    NEW.lease_credential_state:='legacy_unknown';
  ELSIF NEW.lease_token IS DISTINCT FROM OLD.lease_token
        AND (NEW.lease_node_token_id IS NULL OR NEW.lease_claim_generation IS NULL) THEN
    NEW.lease_node_token_id:=NULL;
    NEW.lease_claim_generation:=NULL;
    NEW.lease_credential_state:='legacy_unknown';
  END IF;
  RETURN NEW;
END $$;

-- Every post-migration claim, including one issued by a rollback/v0 binary,
-- must pass the same enabled claim head.  Legacy clients may keep an unknown
-- lease credential shape so their existing request contract remains readable,
-- but a recovery-blocked host can never obtain a new fence.
CREATE FUNCTION recording_surrender_validate_lease_admission() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE claim_node_id BIGINT; head RECORD;
BEGIN
  IF OLD.status IS DISTINCT FROM 'leased' AND NEW.status='leased' THEN
    IF NEW.lease_owner LIKE 'node:%' THEN
      BEGIN claim_node_id:=substring(NEW.lease_owner FROM '^node:([0-9]+)$')::bigint;
      EXCEPTION WHEN invalid_text_representation THEN claim_node_id:=NULL; END;
    ELSE
      SELECT droplet.node_id INTO claim_node_id
      FROM recorder_droplets droplet WHERE droplet.name=NEW.lease_owner
      ORDER BY droplet.id LIMIT 1;
    END IF;
    SELECT claim.*,token.revoked_at,token.recording_claim_generation,token.recording_claim_purpose
      INTO head
    FROM recording_worker_claim_heads claim
    JOIN node_tokens token ON token.id=claim.claim_token_id
    WHERE claim.node_id=claim_node_id FOR SHARE OF claim,token;
    IF claim_node_id IS NULL OR head.node_id IS NULL OR head.state<>'enabled'
       OR head.revoked_at IS NOT NULL OR head.recording_claim_generation<>head.generation
       OR head.recording_claim_purpose<>'claim_current' THEN
      RAISE EXCEPTION 'recording lease admission is blocked by claim authority';
    END IF;
    IF NEW.lease_credential_state='exact' AND
       (NEW.lease_node_token_id IS DISTINCT FROM head.claim_token_id
        OR NEW.lease_claim_generation IS DISTINCT FROM head.generation) THEN
      RAISE EXCEPTION 'recording lease admission differs from enabled claim head';
    END IF;
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER recording_surrender_admission_veto_trg
BEFORE UPDATE OF status,lease_owner,lease_node_token_id,lease_claim_generation,lease_credential_state ON recording_jobs
FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_lease_admission();

CREATE TRIGGER recording_surrender_normalize_lease_credential_trg
BEFORE UPDATE OF lease_token,lease_node_token_id,lease_claim_generation,lease_credential_state ON recording_jobs
FOR EACH ROW EXECUTE FUNCTION recording_surrender_normalize_lease_credential();

-- A current successor bearer may service exact older fences on the same node
-- while also claiming new work.  The predecessor remains heartbeat/upload-only
-- until every old fence drains.  No other token or node crosses this relation.
CREATE FUNCTION recording_surrender_token_can_access_lease(
  p_presented_token_id BIGINT,p_node_id BIGINT,p_bound_token_id BIGINT,p_bound_generation BIGINT
) RETURNS BOOLEAN LANGUAGE sql STABLE AS $$
  SELECT EXISTS(
    SELECT 1
    FROM node_tokens presented
    JOIN node_tokens bound ON bound.id=p_bound_token_id AND bound.node_id=p_node_id
    WHERE presented.id=p_presented_token_id AND presented.node_id=p_node_id
      AND presented.revoked_at IS NULL AND bound.recording_claim_generation=p_bound_generation
      AND (
        (presented.id=bound.id AND presented.recording_claim_generation=p_bound_generation
          AND presented.recording_claim_purpose IN('claim_current','existing_fence_only'))
        OR EXISTS(
          SELECT 1 FROM recording_worker_claim_heads head
          WHERE head.node_id=p_node_id AND head.state='enabled'
            AND head.claim_token_id=presented.id
            AND head.generation=presented.recording_claim_generation
            AND presented.recording_claim_purpose='claim_current'
            AND head.generation>p_bound_generation
        )
      )
  )
$$;

-- One server-authored plan has one append-only outcome. The split-list digest
-- and integer-microsecond facts are independent of session TimeZone/DateStyle.
CREATE TABLE recording_capture_set_plans (
  id UUID PRIMARY KEY,
  set_id UUID NOT NULL UNIQUE,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  recording_id BIGINT NOT NULL REFERENCES recordings(id) ON DELETE RESTRICT,
  storage_destination_id BIGINT NOT NULL REFERENCES storage_destinations(id) ON DELETE RESTRICT,
  recording_job_id BIGINT NOT NULL REFERENCES recording_jobs(id) ON DELETE RESTRICT,
  lease_token UUID NOT NULL,
  origin_claim_generation BIGINT,
  producer_id UUID NOT NULL,
  capture_ordinal BIGINT NOT NULL CHECK(capture_ordinal>0),
  first_capture_sequence BIGINT NOT NULL CHECK(first_capture_sequence>0),
  snapshot_generation BIGINT NOT NULL CHECK(snapshot_generation>0),
  source_snapshot JSONB NOT NULL,
  source_snapshot_sha256 TEXT NOT NULL CHECK(source_snapshot_sha256~'^[0-9a-f]{64}$'),
  destination_naming_snapshot JSONB NOT NULL,
  destination_naming_sha256 TEXT NOT NULL CHECK(destination_naming_sha256~'^[0-9a-f]{64}$'),
  plan_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  window_end_at TIMESTAMPTZ NOT NULL,
  duration_microseconds BIGINT NOT NULL CHECK(duration_microseconds>0),
  clip_duration_seconds INTEGER NOT NULL CHECK(clip_duration_seconds BETWEEN 5 AND 900),
  artifact_count INTEGER NOT NULL CHECK(artifact_count BETWEEN 1 AND 12288),
  segment_times_argument TEXT NOT NULL CHECK(octet_length(segment_times_argument)<=65536),
  segment_times_sha256 TEXT NOT NULL CHECK(segment_times_sha256~'^[0-9a-f]{64}$'),
  max_artifact_bytes BIGINT NOT NULL CHECK(max_artifact_bytes=33554432),
  expires_at TIMESTAMPTZ NOT NULL,
  UNIQUE(recording_job_id,lease_token,capture_ordinal),
  CHECK(window_end_at>plan_at AND duration_microseconds=(extract(epoch FROM(window_end_at-plan_at))*1000000)::bigint),
  CHECK(expires_at=plan_at+interval '30 seconds')
);

CREATE FUNCTION recording_surrender_validate_set_plan() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expected_source JSONB; expected_destination JSONB; expected_generation BIGINT; expected_destination_id BIGINT;
BEGIN
  NEW.plan_at:=transaction_timestamp();
  NEW.expires_at:=NEW.plan_at+interval '30 seconds';
  SELECT recording_surrender_source_snapshot(NEW.recording_id),
         recording_surrender_destination_snapshot(NEW.recording_id),
         COALESCE((SELECT max(revision.id) FROM stream_source_revisions revision WHERE revision.stream_id=stream.id),0)+1,
         recording.storage_destination_id
    INTO expected_source,expected_destination,expected_generation,expected_destination_id
  FROM recordings recording JOIN streams stream ON stream.id=recording.stream_id
  WHERE recording.id=NEW.recording_id
  FOR SHARE OF recording,stream;
  IF expected_source IS NULL OR NEW.storage_destination_id IS DISTINCT FROM expected_destination_id
     OR NEW.source_snapshot IS DISTINCT FROM expected_source
     OR NEW.destination_naming_snapshot IS DISTINCT FROM expected_destination
     OR NEW.snapshot_generation<>expected_generation
     OR NEW.source_snapshot_sha256<>encode(sha256(convert_to(expected_source::text,'UTF8')),'hex')
     OR NEW.destination_naming_sha256<>encode(sha256(convert_to(expected_destination::text,'UTF8')),'hex') THEN
    RAISE EXCEPTION 'capture set plan differs from current DB-owned snapshot';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_capture_set_plans_validate
BEFORE INSERT ON recording_capture_set_plans FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_set_plan();

CREATE TABLE recording_capture_set_plan_results (
  plan_id UUID PRIMARY KEY REFERENCES recording_capture_set_plans(id) ON DELETE RESTRICT,
  result TEXT NOT NULL CHECK(result IN('accepted_set','expired_no_bytes','abandoned_no_bytes')),
  set_id UUID,
  result_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  CHECK((result='accepted_set' AND set_id IS NOT NULL) OR (result<>'accepted_set' AND set_id IS NULL))
);

CREATE TABLE recording_capture_reservation_sets (
  id UUID PRIMARY KEY,
  plan_id UUID NOT NULL UNIQUE REFERENCES recording_capture_set_plans(id) ON DELETE RESTRICT,
  merkle_root_sha256 TEXT NOT NULL CHECK(merkle_root_sha256~'^[0-9a-f]{64}$'),
  artifact_count INTEGER NOT NULL CHECK(artifact_count BETWEEN 1 AND 12288),
  committed_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  UNIQUE(id,artifact_count)
);

CREATE TABLE recording_capture_producer_stop_events (
  id UUID PRIMARY KEY,
  set_id UUID NOT NULL UNIQUE REFERENCES recording_capture_reservation_sets(id) ON DELETE RESTRICT,
  old_snapshot_generation BIGINT NOT NULL CHECK(old_snapshot_generation>0),
  new_snapshot_generation BIGINT NOT NULL CHECK(new_snapshot_generation>old_snapshot_generation),
  required_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  UNIQUE(set_id,new_snapshot_generation)
);

CREATE TABLE recording_capture_producer_stop_acks (
  id UUID PRIMARY KEY,
  stop_event_id UUID NOT NULL UNIQUE REFERENCES recording_capture_producer_stop_events(id) ON DELETE RESTRICT,
  set_id UUID NOT NULL REFERENCES recording_capture_reservation_sets(id) ON DELETE RESTRICT,
  inventory_sha256 TEXT NOT NULL CHECK(inventory_sha256~'^[0-9a-f]{64}$'),
  retained_directory_device BIGINT NOT NULL CHECK(retained_directory_device>=0),
  retained_directory_inode BIGINT NOT NULL CHECK(retained_directory_inode>0),
  acknowledged_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp()
);

CREATE FUNCTION recording_surrender_append_stream_stop_events() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
BEGIN
  IF (OLD.source_url,OLD.source_page_url,OLD.provider,OLD.external_id,OLD.source_family,
      OLD.capture_type,OLD.execution_class,OLD.execution_config_jsonb)
     IS NOT DISTINCT FROM
     (NEW.source_url,NEW.source_page_url,NEW.provider,NEW.external_id,NEW.source_family,
      NEW.capture_type,NEW.execution_class,NEW.execution_config_jsonb) THEN
    RETURN NEW;
  END IF;
  INSERT INTO recording_capture_producer_stop_events(id,set_id,old_snapshot_generation,new_snapshot_generation)
  SELECT gen_random_uuid(),capture_set.id,plan.snapshot_generation,
         plan.snapshot_generation+1+COALESCE((SELECT count(*) FROM recording_capture_producer_stop_events prior WHERE prior.set_id=capture_set.id),0)
  FROM recording_capture_reservation_sets capture_set
  JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
  WHERE (plan.source_snapshot->>'stream_id')::bigint=NEW.id
    AND NOT EXISTS(SELECT 1 FROM recording_capture_set_results result WHERE result.set_id=capture_set.id)
    AND NOT EXISTS(SELECT 1 FROM recording_capture_producer_stop_events prior WHERE prior.set_id=capture_set.id);
  RETURN NEW;
END $$;

CREATE TRIGGER recording_surrender_append_stream_stop_events_trg
AFTER UPDATE OF source_url,source_page_url,provider,external_id,source_family,capture_type,execution_class,execution_config_jsonb ON streams
FOR EACH ROW EXECUTE FUNCTION recording_surrender_append_stream_stop_events();

CREATE FUNCTION recording_surrender_append_recording_stop_events() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF (OLD.stream_url,OLD.capture_via,OLD.mode,OLD.source_kind,OLD.target_fps,
      OLD.storage_destination_id,OLD.naming_profile,OLD.folder_name,OLD.naming_metadata_jsonb,OLD.cron_timezone)
     IS NOT DISTINCT FROM
     (NEW.stream_url,NEW.capture_via,NEW.mode,NEW.source_kind,NEW.target_fps,
      NEW.storage_destination_id,NEW.naming_profile,NEW.folder_name,NEW.naming_metadata_jsonb,NEW.cron_timezone) THEN
    RETURN NEW;
  END IF;
  INSERT INTO recording_capture_producer_stop_events(id,set_id,old_snapshot_generation,new_snapshot_generation)
  SELECT gen_random_uuid(),capture_set.id,plan.snapshot_generation,
         plan.snapshot_generation+1+COALESCE((SELECT count(*) FROM recording_capture_producer_stop_events prior WHERE prior.set_id=capture_set.id),0)
  FROM recording_capture_reservation_sets capture_set
  JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
  WHERE plan.recording_id=NEW.id
    AND NOT EXISTS(SELECT 1 FROM recording_capture_set_results result WHERE result.set_id=capture_set.id)
    AND NOT EXISTS(SELECT 1 FROM recording_capture_producer_stop_events prior WHERE prior.set_id=capture_set.id);
  RETURN NEW;
END $$;
CREATE TRIGGER recording_surrender_append_recording_stop_events_trg
AFTER UPDATE OF stream_url,capture_via,mode,source_kind,target_fps,storage_destination_id,naming_profile,folder_name,naming_metadata_jsonb,cron_timezone ON recordings
FOR EACH ROW EXECUTE FUNCTION recording_surrender_append_recording_stop_events();

CREATE FUNCTION recording_surrender_append_destination_stop_events() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF (OLD.endpoint,OLD.region,OLD.bucket,OLD.key_prefix)
     IS NOT DISTINCT FROM (NEW.endpoint,NEW.region,NEW.bucket,NEW.key_prefix) THEN
    RETURN NEW;
  END IF;
  INSERT INTO recording_capture_producer_stop_events(id,set_id,old_snapshot_generation,new_snapshot_generation)
  SELECT gen_random_uuid(),capture_set.id,plan.snapshot_generation,
         plan.snapshot_generation+1+COALESCE((SELECT count(*) FROM recording_capture_producer_stop_events prior WHERE prior.set_id=capture_set.id),0)
  FROM recording_capture_reservation_sets capture_set
  JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
  WHERE (plan.destination_naming_snapshot->>'destination_id')::bigint=NEW.id
    AND NOT EXISTS(SELECT 1 FROM recording_capture_set_results result WHERE result.set_id=capture_set.id)
    AND NOT EXISTS(SELECT 1 FROM recording_capture_producer_stop_events prior WHERE prior.set_id=capture_set.id);
  RETURN NEW;
END $$;
CREATE TRIGGER recording_surrender_append_destination_stop_events_trg
AFTER UPDATE OF endpoint,region,bucket,key_prefix ON storage_destinations
FOR EACH ROW EXECUTE FUNCTION recording_surrender_append_destination_stop_events();

CREATE TABLE recording_capture_stop_ack_members (
  stop_ack_id UUID NOT NULL REFERENCES recording_capture_producer_stop_acks(id) ON DELETE RESTRICT,
  ordinal INTEGER NOT NULL CHECK(ordinal>0),
  artifact_id UUID NOT NULL,
  device BIGINT NOT NULL CHECK(device>=0),
  inode BIGINT NOT NULL CHECK(inode>0),
  size_bytes BIGINT NOT NULL CHECK(size_bytes BETWEEN 0 AND 33554432),
  relative_name TEXT NOT NULL CHECK(relative_name~'^seg-[0-9]{8}-[0-9]{6}\.mp4$'),
  PRIMARY KEY(stop_ack_id,ordinal),
  UNIQUE(stop_ack_id,artifact_id),
  UNIQUE(stop_ack_id,device,inode),
  UNIQUE(stop_ack_id,relative_name)
);

CREATE TABLE recording_capture_set_grants (
  id UUID PRIMARY KEY,
  set_id UUID NOT NULL UNIQUE REFERENCES recording_capture_reservation_sets(id) ON DELETE RESTRICT,
  origin_claim_generation BIGINT,
  recovery_block_generation BIGINT NOT NULL CHECK(recovery_block_generation>0),
  granted_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  upload_grace_until TIMESTAMPTZ NOT NULL,
  CHECK(upload_grace_until=granted_at+interval '30 minutes')
);

CREATE TABLE recording_capture_materialized_artifacts (
  set_id UUID NOT NULL REFERENCES recording_capture_reservation_sets(id) ON DELETE RESTRICT,
  ordinal INTEGER NOT NULL CHECK(ordinal>0),
  artifact_id UUID NOT NULL UNIQUE,
  recovery_secret_sha256 TEXT NOT NULL CHECK(recovery_secret_sha256~'^[0-9a-f]{64}$'),
  capture_sequence BIGINT NOT NULL CHECK(capture_sequence>0),
  proof JSONB NOT NULL,
  proof_sha256 TEXT NOT NULL CHECK(proof_sha256~'^[0-9a-f]{64}$'),
  materialized_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  PRIMARY KEY(set_id,ordinal),
  UNIQUE(set_id,capture_sequence)
);

CREATE TABLE recording_capture_materialized_artifact_seals (
  set_id UUID NOT NULL,
  ordinal INTEGER NOT NULL,
  artifact_id UUID NOT NULL UNIQUE,
  capture_sequence BIGINT NOT NULL CHECK(capture_sequence>0),
  segment_start_microseconds BIGINT NOT NULL,
  size_bytes BIGINT NOT NULL CHECK(size_bytes BETWEEN 1 AND 33554432),
  sha256 TEXT NOT NULL CHECK(sha256~'^[0-9a-f]{64}$'),
  storage_destination_id BIGINT NOT NULL REFERENCES storage_destinations(id) ON DELETE RESTRICT,
  object_key_root_id UUID NOT NULL UNIQUE,
  sealed_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  PRIMARY KEY(set_id,ordinal),
  FOREIGN KEY(set_id,ordinal) REFERENCES recording_capture_materialized_artifacts(set_id,ordinal) ON DELETE RESTRICT,
  FOREIGN KEY(artifact_id) REFERENCES recording_upload_intents(id) ON DELETE RESTRICT
);

CREATE TABLE recording_capture_recovery_reports (
  id UUID PRIMARY KEY,
  set_id UUID NOT NULL,
  ordinal INTEGER NOT NULL,
  report_type TEXT NOT NULL CHECK(report_type IN('no_bytes','partial_bytes','sealed_bytes')),
  size_bytes BIGINT,
  sha256 TEXT,
  local_observed_at TIMESTAMPTZ NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
	UNIQUE(id,set_id,ordinal),
	UNIQUE(set_id,ordinal),
  FOREIGN KEY(set_id,ordinal) REFERENCES recording_capture_materialized_artifacts(set_id,ordinal) ON DELETE RESTRICT,
  CHECK((report_type='no_bytes' AND size_bytes IS NULL AND sha256 IS NULL)
     OR (report_type<>'no_bytes' AND size_bytes>0 AND sha256~'^[0-9a-f]{64}$'))
);

-- A crash after set commitment but before the first file still owns durable
-- recovery authority.  The originating blocked node must explicitly attest
-- that the committed set has no local bytes before the journal/seed is removed.
CREATE TABLE recording_capture_empty_set_reports (
  set_id UUID PRIMARY KEY REFERENCES recording_capture_reservation_sets(id) ON DELETE RESTRICT,
  grant_id UUID NOT NULL UNIQUE REFERENCES recording_capture_set_grants(id) ON DELETE RESTRICT,
  node_id BIGINT NOT NULL REFERENCES nodes(id) ON DELETE RESTRICT,
  report_id UUID NOT NULL UNIQUE,
  result TEXT NOT NULL CHECK(result='no_bytes'),
  reported_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp()
);

CREATE FUNCTION recording_surrender_validate_empty_set_report() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.reported_at:=transaction_timestamp();
  IF EXISTS(SELECT 1 FROM recording_capture_materialized_artifacts artifact WHERE artifact.set_id=NEW.set_id)
     OR EXISTS(SELECT 1 FROM recording_capture_set_results result WHERE result.set_id=NEW.set_id)
     OR NOT EXISTS(
       SELECT 1
       FROM recording_capture_set_grants grant
       JOIN recording_capture_reservation_sets capture_set ON capture_set.id=grant.set_id
       JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
       JOIN recording_job_lease_generations lease
         ON lease.recording_job_id=plan.recording_job_id AND lease.lease_token=plan.lease_token
       JOIN recording_worker_claim_heads head ON head.node_id=lease.node_id
       WHERE grant.id=NEW.grant_id AND grant.set_id=NEW.set_id AND lease.node_id=NEW.node_id
         AND grant.upload_grace_until>transaction_timestamp()
         AND head.state IN('recovery_blocked','successor_pending','enabled')
         AND head.generation>=grant.recovery_block_generation
     ) THEN
    RAISE EXCEPTION 'empty capture set report lacks exact live recovery authority';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_capture_empty_set_reports_validate
BEFORE INSERT ON recording_capture_empty_set_reports FOR EACH ROW
EXECUTE FUNCTION recording_surrender_validate_empty_set_report();

CREATE TABLE recording_capture_artifact_grant_results (
  set_id UUID NOT NULL,
  ordinal INTEGER NOT NULL,
  result TEXT NOT NULL CHECK(result IN('accepted_unique','exact_replay','acknowledged_no_bytes','abandoned_no_bytes','unrecoverable_partial','host_unreachable','security_revoked')),
  report_id UUID REFERENCES recording_capture_recovery_reports(id) ON DELETE RESTRICT,
  clip_id BIGINT REFERENCES recording_clips(id) ON DELETE RESTRICT,
  stop_ack_id UUID,
  security_event_id UUID,
  result_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  PRIMARY KEY(set_id,ordinal),
	FOREIGN KEY(set_id,ordinal) REFERENCES recording_capture_materialized_artifacts(set_id,ordinal) ON DELETE RESTRICT,
	FOREIGN KEY(report_id,set_id,ordinal) REFERENCES recording_capture_recovery_reports(id,set_id,ordinal) ON DELETE RESTRICT,
	FOREIGN KEY(stop_ack_id,ordinal) REFERENCES recording_capture_stop_ack_members(stop_ack_id,ordinal) ON DELETE RESTRICT
);

CREATE TABLE recording_capture_set_results (
  set_id UUID PRIMARY KEY REFERENCES recording_capture_reservation_sets(id) ON DELETE RESTRICT,
  result TEXT NOT NULL CHECK(result IN('completed','abandoned','host_unreachable','security_revoked')),
  coverage_ranges JSONB NOT NULL,
  coverage_sha256 TEXT NOT NULL CHECK(coverage_sha256~'^[0-9a-f]{64}$'),
  result_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp()
);

CREATE TABLE recording_capture_security_events (
  id UUID PRIMARY KEY,
  set_id UUID NOT NULL REFERENCES recording_capture_reservation_sets(id) ON DELETE RESTRICT,
  ordinal INTEGER,
  actor_user_id BIGINT REFERENCES users(id) ON DELETE RESTRICT,
  actor_api_key_id BIGINT REFERENCES account_api_keys(id) ON DELETE RESTRICT,
  reason TEXT NOT NULL CHECK(reason IN('suspected_capability_compromise','suspected_seed_compromise','host_lost')),
  idempotency_key UUID NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  CHECK(ordinal IS NULL OR ordinal>0),
  CHECK((actor_user_id IS NOT NULL)::integer+(actor_api_key_id IS NOT NULL)::integer=1)
);

CREATE FUNCTION recording_surrender_validate_security_event() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE set_account BIGINT; actor_valid BOOLEAN;
BEGIN
  SELECT plan.account_id INTO set_account
  FROM recording_capture_reservation_sets capture_set
  JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
  WHERE capture_set.id=NEW.set_id FOR SHARE OF capture_set,plan;
  IF set_account IS NULL OR (NEW.reason='suspected_seed_compromise' AND NEW.ordinal IS NOT NULL)
     OR EXISTS(SELECT 1 FROM recording_capture_set_results result WHERE result.set_id=NEW.set_id)
     OR (NEW.ordinal IS NOT NULL AND NOT EXISTS(
       SELECT 1 FROM recording_capture_materialized_artifacts artifact
       WHERE artifact.set_id=NEW.set_id AND artifact.ordinal=NEW.ordinal
     )) THEN
    RAISE EXCEPTION 'recovery security event target is invalid';
  END IF;
  IF NEW.actor_user_id IS NOT NULL THEN
    SELECT (u.is_operator OR EXISTS(
      SELECT 1 FROM memberships membership
      WHERE membership.user_id=u.id AND membership.org_id=set_account
        AND membership.accepted_at IS NOT NULL AND membership.role IN('owner','admin')
    )) INTO actor_valid FROM users u WHERE u.id=NEW.actor_user_id;
  ELSE
    SELECT key.account_id=set_account AND key.revoked_at IS NULL
      AND (key.expires_at IS NULL OR key.expires_at>transaction_timestamp())
    INTO actor_valid FROM account_api_keys key WHERE key.id=NEW.actor_api_key_id;
  END IF;
  IF NOT COALESCE(actor_valid,false) THEN
    RAISE EXCEPTION 'recovery security event actor is unauthorized';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_capture_security_events_validate
BEFORE INSERT ON recording_capture_security_events FOR EACH ROW
EXECUTE FUNCTION recording_surrender_validate_security_event();

ALTER TABLE recording_capture_artifact_grant_results
  ADD CONSTRAINT recording_capture_artifact_grant_results_security_event_fk
  FOREIGN KEY(security_event_id) REFERENCES recording_capture_security_events(id) ON DELETE RESTRICT;

ALTER TABLE recording_capture_stop_ack_members
  ADD CONSTRAINT recording_capture_stop_ack_members_artifact_fk
  FOREIGN KEY(artifact_id) REFERENCES recording_capture_materialized_artifacts(artifact_id) ON DELETE RESTRICT;

CREATE TABLE recording_object_key_roots (
  id UUID PRIMARY KEY,
  storage_destination_id BIGINT NOT NULL REFERENCES storage_destinations(id) ON DELETE RESTRICT,
  bucket TEXT NOT NULL,
  object_key TEXT NOT NULL,
  owner_kind TEXT NOT NULL CHECK(owner_kind IN('legacy_intent','legacy_clip','capture_artifact')),
  owner_identity TEXT NOT NULL,
  semantic_identity_sha256 TEXT NOT NULL CHECK(semantic_identity_sha256~'^[0-9a-f]{64}$'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  UNIQUE(storage_destination_id,bucket,object_key)
);

ALTER TABLE recording_capture_materialized_artifact_seals
  ADD CONSTRAINT recording_capture_materialized_artifact_seals_key_root_fk
  FOREIGN KEY(object_key_root_id) REFERENCES recording_object_key_roots(id) ON DELETE RESTRICT;

-- Freeze every legacy writer while reconstructing global destination ownership.
-- A consumed intent and its exact clip are one lifecycle owner; ambiguity is a
-- migration error and is never guessed from SHA or object key alone.
LOCK TABLE recording_upload_intents,recording_clips IN SHARE ROW EXCLUSIVE MODE;
DO $$
BEGIN
  IF EXISTS (
    SELECT intent.id
    FROM recording_upload_intents intent
    WHERE intent.status='consumed'
      AND (SELECT count(*) FROM recording_clips clip
           WHERE clip.recording_id=intent.recording_id
             AND clip.recording_job_id=intent.recording_job_id
             AND clip.storage_destination_id=intent.storage_destination_id
             AND clip.endpoint=intent.endpoint AND clip.bucket=intent.bucket
             AND clip.object_key=intent.object_key)<>1
  ) THEN
    RAISE EXCEPTION 'consumed recording intent has ambiguous historical clip identity';
  END IF;
  IF EXISTS (
    SELECT clip.id
    FROM recording_clips clip
    WHERE (SELECT count(*) FROM recording_upload_intents intent
           WHERE intent.status='consumed' AND intent.recording_id=clip.recording_id
             AND intent.recording_job_id=clip.recording_job_id
             AND intent.storage_destination_id=clip.storage_destination_id
             AND intent.endpoint=clip.endpoint AND intent.bucket=clip.bucket
             AND intent.object_key=clip.object_key)>1
  ) THEN
    RAISE EXCEPTION 'historical clip has ambiguous consumed intent identity';
  END IF;
END $$;

DO $$
BEGIN
  IF EXISTS (
    SELECT storage_destination_id,bucket,object_key
    FROM (
      SELECT intent.storage_destination_id,intent.bucket,intent.object_key
      FROM recording_upload_intents intent
      UNION ALL
      SELECT clip.storage_destination_id,clip.bucket,clip.object_key
      FROM recording_clips clip
      WHERE clip.storage_destination_id IS NOT NULL AND NULLIF(clip.bucket,'') IS NOT NULL AND NULLIF(clip.object_key,'') IS NOT NULL
        AND NOT EXISTS(
        SELECT 1 FROM recording_upload_intents intent
        WHERE intent.status='consumed' AND intent.recording_id=clip.recording_id
          AND intent.recording_job_id=clip.recording_job_id
          AND intent.storage_destination_id=clip.storage_destination_id
          AND intent.endpoint=clip.endpoint AND intent.bucket=clip.bucket AND intent.object_key=clip.object_key)
    ) owner
    GROUP BY storage_destination_id,bucket,object_key HAVING count(*)<>1
  ) THEN
    RAISE EXCEPTION 'historical recording destination ownership is ambiguous';
  END IF;
END $$;

WITH owners AS (
  SELECT intent.storage_destination_id,intent.bucket,intent.object_key,
         CASE WHEN intent.status='consumed' THEN 'legacy_clip' ELSE 'legacy_intent' END owner_kind,
         CASE WHEN intent.status='consumed'
              THEN 'intent:'||intent.id::text||':clip:'||clip.id::text
              ELSE 'intent:'||intent.id::text END owner_identity
  FROM recording_upload_intents intent
  LEFT JOIN recording_clips clip ON intent.status='consumed'
    AND clip.recording_id=intent.recording_id AND clip.recording_job_id=intent.recording_job_id
    AND clip.storage_destination_id=intent.storage_destination_id
    AND clip.endpoint=intent.endpoint AND clip.bucket=intent.bucket AND clip.object_key=intent.object_key
  UNION ALL
  SELECT clip.storage_destination_id,clip.bucket,clip.object_key,'legacy_clip','clip:'||clip.id::text
  FROM recording_clips clip
  WHERE clip.storage_destination_id IS NOT NULL AND NULLIF(clip.bucket,'') IS NOT NULL AND NULLIF(clip.object_key,'') IS NOT NULL
    AND NOT EXISTS(
    SELECT 1 FROM recording_upload_intents intent
    WHERE intent.status='consumed' AND intent.recording_id=clip.recording_id
      AND intent.recording_job_id=clip.recording_job_id
      AND intent.storage_destination_id=clip.storage_destination_id
      AND intent.endpoint=clip.endpoint AND intent.bucket=clip.bucket AND intent.object_key=clip.object_key)
)
INSERT INTO recording_object_key_roots(id,storage_destination_id,bucket,object_key,owner_kind,owner_identity,semantic_identity_sha256)
SELECT gen_random_uuid(),storage_destination_id,bucket,object_key,owner_kind,owner_identity,
       encode(sha256(convert_to('recording-object-key-owner-v1'||chr(0)||owner_kind||chr(0)||owner_identity,'UTF8')),'hex')
FROM owners;

CREATE FUNCTION recording_surrender_validate_key_root_transition() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE intent_id UUID; clip_id BIGINT; expected_identity TEXT;
BEGIN
  IF TG_OP='DELETE' THEN RAISE EXCEPTION 'recording destination key authority is immutable'; END IF;
  IF (NEW.id,NEW.storage_destination_id,NEW.bucket,NEW.object_key,NEW.created_at)
     IS DISTINCT FROM (OLD.id,OLD.storage_destination_id,OLD.bucket,OLD.object_key,OLD.created_at) THEN
    RAISE EXCEPTION 'recording destination key identity is immutable';
  END IF;
  IF OLD.owner_kind='legacy_intent' AND NEW.owner_kind='legacy_clip' THEN
    intent_id:=substring(OLD.owner_identity FROM '^intent:([0-9a-f-]{36})$')::uuid;
    SELECT clip.id INTO STRICT clip_id
    FROM recording_upload_intents intent
    JOIN recording_clips clip ON clip.recording_id=intent.recording_id
      AND clip.recording_job_id=intent.recording_job_id
      AND clip.storage_destination_id=intent.storage_destination_id
      AND clip.endpoint=intent.endpoint AND clip.bucket=intent.bucket AND clip.object_key=intent.object_key
    WHERE intent.id=intent_id AND intent.status='pending'
      AND intent.storage_destination_id=OLD.storage_destination_id
      AND intent.bucket=OLD.bucket AND intent.object_key=OLD.object_key;
    expected_identity:='intent:'||intent_id::text||':clip:'||clip_id::text;
    IF NEW.owner_identity<>expected_identity
       OR NEW.semantic_identity_sha256<>encode(sha256(convert_to('recording-object-key-owner-v1'||chr(0)||'legacy_clip'||chr(0)||expected_identity,'UTF8')),'hex') THEN
      RAISE EXCEPTION 'recording destination key consumption identity differs';
    END IF;
    RETURN NEW;
  END IF;
  IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
  RAISE EXCEPTION 'recording destination key authority transition is not allowed';
EXCEPTION WHEN NO_DATA_FOUND OR TOO_MANY_ROWS THEN
  RAISE EXCEPTION 'recording destination key consumption is ambiguous';
END $$;

CREATE FUNCTION recording_surrender_validate_key_root_consumption() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE intent_id UUID;
BEGIN
  IF NEW.owner_kind<>'legacy_clip' OR OLD.owner_kind<>'legacy_intent' THEN RETURN NULL; END IF;
  intent_id:=substring(OLD.owner_identity FROM '^intent:([0-9a-f-]{36})$')::uuid;
  IF NOT EXISTS(SELECT 1 FROM recording_upload_intents intent WHERE intent.id=intent_id AND intent.status='consumed') THEN
    RAISE EXCEPTION 'recording destination key consumption lacks exact consumed intent';
  END IF;
  RETURN NULL;
END $$;

CREATE TRIGGER recording_object_key_roots_transition_trg
BEFORE UPDATE OR DELETE ON recording_object_key_roots FOR EACH ROW
EXECUTE FUNCTION recording_surrender_validate_key_root_transition();
CREATE CONSTRAINT TRIGGER recording_object_key_roots_consumption_seal
AFTER UPDATE ON recording_object_key_roots DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION recording_surrender_validate_key_root_consumption();

CREATE FUNCTION recording_surrender_reserve_legacy_intent_key() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE root RECORD; clip_id BIGINT; expected_identity TEXT:='intent:'||NEW.id::text; consumed_identity TEXT;
BEGIN
  IF TG_OP='UPDATE' THEN
    IF (NEW.id,NEW.recording_id,NEW.recording_job_id,NEW.storage_destination_id,NEW.endpoint,NEW.bucket,NEW.object_key)
       IS DISTINCT FROM (OLD.id,OLD.recording_id,OLD.recording_job_id,OLD.storage_destination_id,OLD.endpoint,OLD.bucket,OLD.object_key) THEN
      RAISE EXCEPTION 'recording upload destination identity is immutable';
    END IF;
    IF OLD.status='pending' AND NEW.status='consumed' THEN
	  SELECT * INTO root FROM recording_object_key_roots
	  WHERE storage_destination_id=NEW.storage_destination_id AND bucket=NEW.bucket AND object_key=NEW.object_key
	  FOR UPDATE;
	  IF NOT FOUND THEN RAISE EXCEPTION 'recording intent consumption lacks destination owner'; END IF;
	  IF root.owner_kind='capture_artifact' AND root.owner_identity LIKE '%:'||NEW.id::text THEN
	    RETURN NEW;
	  END IF;
      SELECT clip.id INTO STRICT clip_id FROM recording_clips clip
      WHERE clip.recording_id=NEW.recording_id AND clip.recording_job_id=NEW.recording_job_id
        AND clip.storage_destination_id=NEW.storage_destination_id AND clip.endpoint=NEW.endpoint
        AND clip.bucket=NEW.bucket AND clip.object_key=NEW.object_key;
      consumed_identity:=expected_identity||':clip:'||clip_id::text;
      UPDATE recording_object_key_roots
      SET owner_kind='legacy_clip',owner_identity=consumed_identity,
          semantic_identity_sha256=encode(sha256(convert_to('recording-object-key-owner-v1'||chr(0)||'legacy_clip'||chr(0)||consumed_identity,'UTF8')),'hex')
      WHERE storage_destination_id=NEW.storage_destination_id AND bucket=NEW.bucket AND object_key=NEW.object_key
        AND owner_kind='legacy_intent' AND owner_identity=expected_identity;
      IF NOT FOUND THEN RAISE EXCEPTION 'recording intent consumption lacks exact destination owner'; END IF;
    END IF;
    RETURN NEW;
  END IF;
  SELECT * INTO root FROM recording_object_key_roots
  WHERE storage_destination_id=NEW.storage_destination_id AND bucket=NEW.bucket AND object_key=NEW.object_key
  FOR UPDATE;
  IF NOT FOUND THEN
    INSERT INTO recording_object_key_roots(id,storage_destination_id,bucket,object_key,owner_kind,owner_identity,semantic_identity_sha256)
    VALUES(gen_random_uuid(),NEW.storage_destination_id,NEW.bucket,NEW.object_key,'legacy_intent',expected_identity,
      encode(sha256(convert_to('recording-object-key-owner-v1'||chr(0)||'legacy_intent'||chr(0)||expected_identity,'UTF8')),'hex'));
  ELSIF NOT ((root.owner_kind='legacy_intent' AND root.owner_identity=expected_identity)
          OR (root.owner_kind='legacy_clip' AND root.owner_identity LIKE expected_identity||':clip:%')
          OR (root.owner_kind='capture_artifact' AND root.owner_identity LIKE '%:'||NEW.id::text)) THEN
    RAISE EXCEPTION 'recording destination key belongs to another artifact';
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER recording_upload_intents_key_root_trg
BEFORE INSERT OR UPDATE OF id,recording_id,recording_job_id,storage_destination_id,endpoint,bucket,object_key,status ON recording_upload_intents
FOR EACH ROW EXECUTE FUNCTION recording_surrender_reserve_legacy_intent_key();

CREATE FUNCTION recording_surrender_validate_clip_key_root() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE root RECORD; intent_id UUID;
BEGIN
  IF TG_OP='UPDATE' THEN
    IF (NEW.storage_destination_id,NEW.bucket,NEW.object_key)
       IS DISTINCT FROM (OLD.storage_destination_id,OLD.bucket,OLD.object_key) THEN
      RAISE EXCEPTION 'recording clip destination identity is immutable';
    END IF;
    RETURN NEW;
  END IF;
	IF NEW.storage_destination_id IS NULL OR NULLIF(NEW.bucket,'') IS NULL OR NULLIF(NEW.object_key,'') IS NULL THEN
	  IF COALESCE(NEW.surrender_transport_version,0)<>0 THEN
	    RAISE EXCEPTION 'v1 recording clip requires exact destination key authority';
	  END IF;
	  RETURN NEW;
	END IF;
  SELECT * INTO root FROM recording_object_key_roots
  WHERE storage_destination_id=NEW.storage_destination_id AND bucket=NEW.bucket AND object_key=NEW.object_key
  FOR UPDATE;
  IF NOT FOUND THEN
	IF COALESCE(NEW.surrender_transport_version,0)<>0 THEN
	  RAISE EXCEPTION 'v1 recording clip lacks destination key authority';
	END IF;
	INSERT INTO recording_object_key_roots(id,storage_destination_id,bucket,object_key,owner_kind,owner_identity,semantic_identity_sha256)
	VALUES(gen_random_uuid(),NEW.storage_destination_id,NEW.bucket,NEW.object_key,'legacy_clip','clip:'||NEW.id::text,
	  encode(sha256(convert_to('recording-object-key-owner-v1'||chr(0)||'legacy_clip'||chr(0)||'clip:'||NEW.id::text,'UTF8')),'hex'));
	RETURN NEW;
  END IF;
  SELECT id INTO intent_id FROM recording_upload_intents
  WHERE recording_id=NEW.recording_id AND recording_job_id=NEW.recording_job_id
    AND storage_destination_id=NEW.storage_destination_id AND endpoint=NEW.endpoint
    AND bucket=NEW.bucket AND object_key=NEW.object_key AND status='pending'
  ORDER BY created_at DESC,id DESC LIMIT 1 FOR UPDATE;
  IF NOT FOUND THEN
	IF root.owner_kind='legacy_clip' AND root.owner_identity='clip:'||NEW.id::text THEN RETURN NEW; END IF;
	RAISE EXCEPTION 'recording clip destination key authority differs';
  END IF;
  IF NOT ((root.owner_kind='legacy_intent' AND root.owner_identity='intent:'||intent_id::text)
       OR (root.owner_kind='legacy_clip' AND root.owner_identity LIKE 'intent:'||intent_id::text||':clip:%')
       OR (root.owner_kind='capture_artifact' AND root.owner_identity LIKE '%:'||intent_id::text)) THEN
    RAISE EXCEPTION 'recording clip destination key authority differs';
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER recording_clips_key_root_trg
BEFORE INSERT OR UPDATE OF storage_destination_id,bucket,object_key ON recording_clips
FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_clip_key_root();

CREATE TABLE recording_recovery_upload_sessions (
  id UUID PRIMARY KEY,
  set_id UUID NOT NULL,
  ordinal INTEGER NOT NULL,
  revision INTEGER NOT NULL CHECK(revision>0),
  node_id BIGINT NOT NULL REFERENCES nodes(id) ON DELETE RESTRICT,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  declared_bytes BIGINT NOT NULL CHECK(declared_bytes BETWEEN 1 AND 33554432),
  quarantine_key TEXT NOT NULL UNIQUE,
  started_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  deadline_at TIMESTAMPTZ NOT NULL,
  UNIQUE(set_id,ordinal,revision),
  FOREIGN KEY(set_id,ordinal) REFERENCES recording_capture_materialized_artifacts(set_id,ordinal) ON DELETE RESTRICT,
  CHECK(deadline_at>started_at AND deadline_at<=started_at+interval '5 minutes')
);

CREATE TABLE recording_recovery_upload_session_results (
	  id UUID PRIMARY KEY,
	  session_id UUID NOT NULL REFERENCES recording_recovery_upload_sessions(id) ON DELETE RESTRICT,
	  phase TEXT NOT NULL CHECK(phase IN('upload','promotion')),
	  result TEXT NOT NULL CHECK(result IN('quarantined','promoted','disconnect','slow','timeout','hash_mismatch','storage_5xx','response_ambiguous','aborted','security_revoked')),
	  observed_size BIGINT,
	  observed_sha256 TEXT,
	  provider_etag TEXT,
	  provider_version_id TEXT,
	  provider_metadata_sha256 TEXT,
  result_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
	  CHECK(observed_sha256 IS NULL OR observed_sha256~'^[0-9a-f]{64}$'),
	  CHECK(provider_metadata_sha256 IS NULL OR provider_metadata_sha256~'^[0-9a-f]{64}$'),
	  UNIQUE(session_id,phase)
);

CREATE FUNCTION recording_surrender_author_db_times() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  CASE TG_TABLE_NAME
    WHEN 'recording_capture_reservation_sets' THEN NEW.committed_at:=transaction_timestamp();
    WHEN 'recording_capture_producer_stop_events' THEN NEW.required_at:=transaction_timestamp();
    WHEN 'recording_capture_set_grants' THEN
      NEW.granted_at:=transaction_timestamp(); NEW.upload_grace_until:=transaction_timestamp()+interval '30 minutes';
    WHEN 'recording_capture_materialized_artifact_seals' THEN NEW.sealed_at:=transaction_timestamp();
    WHEN 'recording_capture_security_events' THEN NEW.created_at:=transaction_timestamp();
    WHEN 'recording_recovery_upload_sessions' THEN
      NEW.started_at:=transaction_timestamp();
      NEW.deadline_at:=LEAST(NEW.deadline_at,transaction_timestamp()+interval '5 minutes');
    WHEN 'recording_recovery_upload_session_results' THEN NEW.result_at:=transaction_timestamp();
    WHEN 'recording_capture_recovery_alert_events' THEN NEW.event_at:=transaction_timestamp();
    WHEN 'recording_worker_claim_successor_proposals' THEN NEW.proposed_at:=transaction_timestamp();
    WHEN 'recording_worker_claim_successor_results' THEN NEW.result_at:=transaction_timestamp();
    ELSE RAISE EXCEPTION 'unsupported DB-time authority table';
  END CASE;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_capture_reservation_sets_db_time BEFORE INSERT ON recording_capture_reservation_sets FOR EACH ROW EXECUTE FUNCTION recording_surrender_author_db_times();
CREATE TRIGGER recording_capture_producer_stop_events_db_time BEFORE INSERT ON recording_capture_producer_stop_events FOR EACH ROW EXECUTE FUNCTION recording_surrender_author_db_times();
CREATE TRIGGER recording_capture_set_grants_db_time BEFORE INSERT ON recording_capture_set_grants FOR EACH ROW EXECUTE FUNCTION recording_surrender_author_db_times();
CREATE TRIGGER recording_capture_materialized_artifact_seals_db_time BEFORE INSERT ON recording_capture_materialized_artifact_seals FOR EACH ROW EXECUTE FUNCTION recording_surrender_author_db_times();
CREATE TRIGGER recording_capture_security_events_db_time BEFORE INSERT ON recording_capture_security_events FOR EACH ROW EXECUTE FUNCTION recording_surrender_author_db_times();
CREATE TRIGGER recording_recovery_upload_sessions_db_time BEFORE INSERT ON recording_recovery_upload_sessions FOR EACH ROW EXECUTE FUNCTION recording_surrender_author_db_times();
CREATE TRIGGER recording_recovery_upload_session_results_db_time BEFORE INSERT ON recording_recovery_upload_session_results FOR EACH ROW EXECUTE FUNCTION recording_surrender_author_db_times();

CREATE FUNCTION recording_surrender_validate_recovery_upload_session_result() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE session_row RECORD; latest_revision INTEGER; upload_result TEXT;
BEGIN
  NEW.result_at:=transaction_timestamp();
  SELECT * INTO session_row FROM recording_recovery_upload_sessions WHERE id=NEW.session_id FOR SHARE;
  IF NOT FOUND THEN RAISE EXCEPTION 'recovery upload session is missing'; END IF;
  SELECT max(revision) INTO latest_revision FROM recording_recovery_upload_sessions
  WHERE set_id=session_row.set_id AND ordinal=session_row.ordinal;
  IF NEW.phase='upload' THEN
    IF NEW.result NOT IN('quarantined','disconnect','slow','timeout','hash_mismatch','storage_5xx','response_ambiguous') THEN
      RAISE EXCEPTION 'recovery upload result is invalid';
    END IF;
    IF NEW.result='quarantined' AND (NEW.observed_size IS DISTINCT FROM session_row.declared_bytes OR NEW.observed_sha256 IS NULL) THEN
      RAISE EXCEPTION 'quarantined recovery upload lacks exact bytes';
    END IF;
  ELSE
    SELECT result INTO upload_result FROM recording_recovery_upload_session_results
    WHERE session_id=NEW.session_id AND phase='upload';
    IF upload_result IS DISTINCT FROM 'quarantined' OR session_row.revision<>latest_revision
       OR NEW.result NOT IN('promoted','storage_5xx','security_revoked','aborted') THEN
      RAISE EXCEPTION 'recovery promotion result is stale or lacks quarantine authority';
    END IF;
    IF NEW.result='promoted' AND (NEW.observed_size IS DISTINCT FROM session_row.declared_bytes OR NEW.observed_sha256 IS NULL
       OR NULLIF(NEW.provider_etag,'') IS NULL OR NEW.provider_version_id IS NULL
       OR NEW.provider_metadata_sha256 IS NULL) THEN
      RAISE EXCEPTION 'promoted recovery upload lacks exact bytes';
    END IF;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_recovery_upload_session_results_validate
BEFORE INSERT ON recording_recovery_upload_session_results FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_recovery_upload_session_result();

-- There is at most one retryable transport session per artifact.  A successor
-- revision is legal only after the exact previous session has an upload failure
-- or a terminal promotion result; it can never race or delete a live PUT.
CREATE FUNCTION recording_surrender_validate_recovery_upload_session() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE prior RECORD; prior_terminal BOOLEAN;
BEGIN
  SELECT session.* INTO prior
  FROM recording_recovery_upload_sessions session
  WHERE session.set_id=NEW.set_id AND session.ordinal=NEW.ordinal
  ORDER BY session.revision DESC LIMIT 1 FOR UPDATE;
  IF FOUND THEN
    SELECT EXISTS(
      SELECT 1 FROM recording_recovery_upload_session_results result
      WHERE result.session_id=prior.id
        AND ((result.phase='upload' AND result.result<>'quarantined') OR result.phase='promotion')
    ) INTO prior_terminal;
    IF NOT prior_terminal OR NEW.revision<>prior.revision+1 THEN
      RAISE EXCEPTION 'recovery upload session predecessor is still active or revision is noncanonical';
    END IF;
  ELSIF NEW.revision<>1 THEN
    RAISE EXCEPTION 'first recovery upload session revision must be one';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_recovery_upload_sessions_validate
BEFORE INSERT ON recording_recovery_upload_sessions FOR EACH ROW
EXECUTE FUNCTION recording_surrender_validate_recovery_upload_session();

-- A process or database outage after session creation but before its first
-- immutable result must not wedge the artifact forever.  DB time alone closes
-- only sessions whose hard deadline passed; a later exact/new replay can then
-- advance to the next canonical revision.
CREATE FUNCTION recording_surrender_reconcile_expired_upload_sessions() RETURNS BIGINT LANGUAGE plpgsql AS $$
DECLARE changed BIGINT:=0; inserted BIGINT;
BEGIN
  INSERT INTO recording_recovery_upload_session_results(id,session_id,phase,result)
  SELECT gen_random_uuid(),session.id,'upload','timeout'
  FROM recording_recovery_upload_sessions session
  WHERE session.deadline_at<=transaction_timestamp()
    AND NOT EXISTS(SELECT 1 FROM recording_recovery_upload_session_results result WHERE result.session_id=session.id)
  ORDER BY session.set_id,session.ordinal,session.revision
  ON CONFLICT(session_id,phase) DO NOTHING;
  GET DIAGNOSTICS inserted=ROW_COUNT;
  changed:=changed+inserted;
  INSERT INTO recording_recovery_upload_session_results(id,session_id,phase,result)
  SELECT gen_random_uuid(),session.id,'promotion','aborted'
  FROM recording_recovery_upload_sessions session
  JOIN recording_recovery_upload_session_results upload
    ON upload.session_id=session.id AND upload.phase='upload' AND upload.result='quarantined'
  WHERE session.deadline_at<=transaction_timestamp()
    AND NOT EXISTS(SELECT 1 FROM recording_recovery_upload_session_results result
                   WHERE result.session_id=session.id AND result.phase='promotion')
  ORDER BY session.set_id,session.ordinal,session.revision
  ON CONFLICT(session_id,phase) DO NOTHING;
  GET DIAGNOSTICS inserted=ROW_COUNT;
  changed:=changed+inserted;
  RETURN changed;
END $$;
CREATE TRIGGER recording_worker_claim_successor_proposals_db_time BEFORE INSERT ON recording_worker_claim_successor_proposals FOR EACH ROW EXECUTE FUNCTION recording_surrender_author_db_times();
CREATE TRIGGER recording_worker_claim_successor_results_db_time BEFORE INSERT ON recording_worker_claim_successor_results FOR EACH ROW EXECUTE FUNCTION recording_surrender_author_db_times();

CREATE FUNCTION recording_surrender_validate_set_grant() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE plan RECORD; generation RECORD;
BEGIN
  SELECT plan.origin_claim_generation,plan.recording_job_id,plan.lease_token INTO plan
  FROM recording_capture_reservation_sets capture_set
  JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
  WHERE capture_set.id=NEW.set_id FOR SHARE OF capture_set,plan;
  SELECT lease_generation.node_id INTO generation
  FROM recording_job_lease_generations lease_generation
  WHERE lease_generation.recording_job_id=plan.recording_job_id AND lease_generation.lease_token=plan.lease_token;
  IF NOT FOUND OR NEW.origin_claim_generation IS DISTINCT FROM plan.origin_claim_generation
     OR NEW.recovery_block_generation<>plan.origin_claim_generation
     OR NOT EXISTS(SELECT 1 FROM recording_worker_claim_heads head
                   WHERE head.node_id=generation.node_id AND head.generation=NEW.recovery_block_generation
                     AND head.state='recovery_blocked' AND head.block_reason='durable_recovery') THEN
    RAISE EXCEPTION 'capture set grant lacks exact worker recovery block';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_capture_set_grants_validate
BEFORE INSERT ON recording_capture_set_grants FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_set_grant();

CREATE FUNCTION recording_surrender_validate_set_grant_expiry_seal() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE exact_job BIGINT; exact_lease UUID;
BEGIN
  SELECT plan.recording_job_id,plan.lease_token INTO exact_job,exact_lease
  FROM recording_capture_reservation_sets capture_set
  JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
  WHERE capture_set.id=NEW.set_id;
  IF NOT EXISTS(
    SELECT 1 FROM recording_job_lease_expiry_events expiry
    WHERE expiry.recording_job_id=exact_job AND expiry.lease_token=exact_lease
      AND expiry.recovery_grant_count=(
        (SELECT count(*) FROM recording_capture_set_grants set_grant
         JOIN recording_capture_reservation_sets reservation ON reservation.id=set_grant.set_id
         JOIN recording_capture_set_plans set_plan ON set_plan.id=reservation.plan_id
         WHERE set_plan.recording_job_id=exact_job AND set_plan.lease_token=exact_lease)
        +(SELECT count(*) FROM recording_job_recovery_grants legacy_grant
          WHERE legacy_grant.recording_job_id=exact_job AND legacy_grant.lease_token=exact_lease)
      )
  ) THEN RAISE EXCEPTION 'capture set recovery grant lacks exact lease-expiry seal'; END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER recording_capture_set_grants_expiry_seal
AFTER INSERT ON recording_capture_set_grants DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION recording_surrender_validate_set_grant_expiry_seal();

CREATE FUNCTION recording_surrender_validate_claim_head_projection() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='DELETE' THEN RAISE EXCEPTION 'recording claim head cannot be deleted'; END IF;
  IF NEW.node_id<>OLD.node_id THEN RAISE EXCEPTION 'recording claim node identity is immutable'; END IF;
  IF OLD.state='enabled' AND NEW.state='recovery_blocked' AND NEW.block_reason='durable_recovery' AND NEW.blocked_at IS NOT NULL THEN
    RETURN NEW;
  END IF;
  IF OLD.state='recovery_blocked' AND NEW.state='successor_pending'
     AND NEW.generation=OLD.generation+1 AND NEW.claim_token_id<>OLD.claim_token_id
     AND NEW.blocked_at=OLD.blocked_at AND NEW.block_reason=OLD.block_reason
     AND EXISTS(SELECT 1 FROM recording_worker_claim_successor_proposals proposal
                WHERE proposal.node_id=OLD.node_id AND proposal.predecessor_generation=OLD.generation
                  AND proposal.successor_generation=NEW.generation
                  AND proposal.predecessor_token_id=OLD.claim_token_id
                  AND proposal.successor_token_id=NEW.claim_token_id) THEN
    RETURN NEW;
  END IF;
  IF OLD.state='successor_pending' AND NEW.state='enabled'
     AND NEW.node_id=OLD.node_id AND NEW.generation=OLD.generation
     AND NEW.claim_token_id=OLD.claim_token_id
     AND NEW.blocked_at IS NULL AND NEW.block_reason IS NULL
     AND EXISTS(SELECT 1 FROM recording_worker_claim_successor_proposals proposal
                JOIN recording_worker_claim_successor_results result ON result.proposal_id=proposal.id
                WHERE proposal.node_id=OLD.node_id AND proposal.successor_generation=OLD.generation
                  AND proposal.successor_token_id=OLD.claim_token_id AND result.result='enabled') THEN
    RETURN NEW;
  END IF;
  IF NEW.state='enabled' AND NEW.blocked_at IS NULL AND NEW.block_reason IS NULL
     AND NEW.generation=OLD.generation+1 AND NEW.claim_token_id<>OLD.claim_token_id
     AND EXISTS(SELECT 1 FROM node_tokens prior WHERE prior.id=OLD.claim_token_id AND prior.revoked_at IS NOT NULL)
     AND EXISTS(SELECT 1 FROM recording_worker_claim_generation_events event
                WHERE event.node_id=OLD.node_id AND event.generation=NEW.generation
                  AND event.predecessor_generation=OLD.generation AND event.claim_token_id=NEW.claim_token_id
                  AND event.event_type='reenrolled') THEN
    RETURN NEW;
  END IF;
  IF NEW.generation<>OLD.generation OR NEW.claim_token_id<>OLD.claim_token_id THEN
    RAISE EXCEPTION 'recording claim identity changes require a typed successor transition';
  END IF;
  IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
  RAISE EXCEPTION 'recording claim head transition is not allowed';
END $$;
CREATE TRIGGER recording_worker_claim_heads_validate
BEFORE UPDATE OR DELETE ON recording_worker_claim_heads FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_claim_head_projection();

CREATE FUNCTION recording_surrender_validate_claim_head_event_seal() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE required_event TEXT;
BEGIN
  IF OLD.state='enabled' AND NEW.state='recovery_blocked' THEN
    required_event:='recovery_blocked';
  ELSIF OLD.state='recovery_blocked' AND NEW.state='successor_pending' THEN
    required_event:='successor_proposed';
  ELSIF OLD.state='successor_pending' AND NEW.state='enabled' THEN
    required_event:='enabled';
  ELSIF NEW.generation=OLD.generation+1 AND NEW.state='enabled' THEN
    required_event:='reenrolled';
  ELSE
    RETURN NULL;
  END IF;
  IF NOT EXISTS(
    SELECT 1 FROM recording_worker_claim_generation_events event
    WHERE event.node_id=NEW.node_id AND event.generation=NEW.generation
      AND event.claim_token_id=NEW.claim_token_id AND event.event_type=required_event
  ) THEN RAISE EXCEPTION 'recording claim head transition lacks append-only event'; END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER recording_worker_claim_heads_event_seal
AFTER UPDATE ON recording_worker_claim_heads DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION recording_surrender_validate_claim_head_event_seal();

CREATE FUNCTION recording_surrender_validate_claim_token_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='DELETE' THEN RAISE EXCEPTION 'recording claim credential cannot be deleted'; END IF;
  IF NEW.id<>OLD.id OR NEW.node_id<>OLD.node_id OR NEW.key_prefix<>OLD.key_prefix OR NEW.secret_hash<>OLD.secret_hash
     OR NEW.recording_claim_generation IS DISTINCT FROM OLD.recording_claim_generation THEN
    RAISE EXCEPTION 'recording claim credential identity is immutable';
  END IF;
  IF NEW.recording_claim_purpose IS DISTINCT FROM OLD.recording_claim_purpose THEN
    IF OLD.recording_claim_purpose='claim_current' AND NEW.recording_claim_purpose='existing_fence_only'
       AND EXISTS(SELECT 1 FROM recording_worker_claim_heads head
                  WHERE head.node_id=OLD.node_id AND head.generation=OLD.recording_claim_generation
                    AND head.state='recovery_blocked') THEN
      NULL;
    ELSIF OLD.recording_claim_purpose='existing_fence_only' AND NEW.recording_claim_purpose='claim_current'
       AND EXISTS(SELECT 1 FROM recording_worker_claim_successor_proposals proposal
                  JOIN recording_worker_claim_successor_results result ON result.proposal_id=proposal.id
                  WHERE proposal.successor_token_id=OLD.id) THEN
      NULL;
    ELSE
      RAISE EXCEPTION 'recording claim credential purpose transition is not authorized';
    END IF;
  END IF;
  IF OLD.revoked_at IS NOT NULL AND NEW.revoked_at IS DISTINCT FROM OLD.revoked_at THEN
    RAISE EXCEPTION 'recording claim credential revocation is immutable';
  END IF;
  IF OLD.revoked_at IS NULL AND NEW.revoked_at IS NOT NULL AND (
       NEW.revoked_at<>transaction_timestamp()
       OR EXISTS(SELECT 1 FROM recording_worker_claim_heads head WHERE head.claim_token_id=OLD.id)
       OR EXISTS(SELECT 1 FROM recording_jobs job WHERE job.lease_node_token_id=OLD.id AND job.status='leased')
     ) THEN RAISE EXCEPTION 'recording claim credential cannot retire while authoritative'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_node_tokens_claim_validate
BEFORE UPDATE OR DELETE ON node_tokens FOR EACH ROW
WHEN (OLD.recording_claim_generation IS NOT NULL)
EXECUTE FUNCTION recording_surrender_validate_claim_token_update();

CREATE FUNCTION recording_surrender_validate_claim_successor_proposal() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE head RECORD; token RECORD;
BEGIN
  NEW.proposed_at:=transaction_timestamp();
  SELECT * INTO head FROM recording_worker_claim_heads WHERE node_id=NEW.node_id FOR UPDATE;
  SELECT * INTO token FROM node_tokens WHERE id=NEW.successor_token_id FOR UPDATE;
  IF head.node_id IS NULL OR token.id IS NULL OR head.state<>'recovery_blocked' OR head.block_reason<>'durable_recovery'
     OR head.generation<>NEW.predecessor_generation OR head.claim_token_id<>NEW.predecessor_token_id
     OR token.node_id<>NEW.node_id OR token.revoked_at IS NOT NULL
     OR token.recording_claim_generation<>NEW.successor_generation
     OR token.recording_claim_purpose<>'existing_fence_only'
     OR token.key_prefix<>NEW.successor_key_prefix
     OR token.secret_hash<>NEW.successor_secret_sha256
     OR EXISTS(SELECT 1 FROM recording_capture_set_grants grant
               JOIN recording_capture_set_plans plan ON plan.id=(SELECT capture_set.plan_id FROM recording_capture_reservation_sets capture_set WHERE capture_set.id=grant.set_id)
               WHERE plan.origin_claim_generation=NEW.predecessor_generation
                 AND EXISTS(SELECT 1 FROM recording_job_lease_generations generation
                            WHERE generation.recording_job_id=plan.recording_job_id AND generation.lease_token=plan.lease_token
                              AND generation.node_id=NEW.node_id)
                 AND NOT EXISTS(SELECT 1 FROM recording_capture_set_results result WHERE result.set_id=grant.set_id)) THEN
    RAISE EXCEPTION 'claim successor proposal lacks terminal recovery authority';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_worker_claim_successor_proposals_validate
BEFORE INSERT ON recording_worker_claim_successor_proposals FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_claim_successor_proposal();

CREATE FUNCTION recording_surrender_validate_claim_successor_result() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE proposal RECORD; head RECORD;
BEGIN
  NEW.result_at:=transaction_timestamp();
  SELECT * INTO proposal FROM recording_worker_claim_successor_proposals WHERE id=NEW.proposal_id FOR SHARE;
  SELECT * INTO head FROM recording_worker_claim_heads WHERE node_id=proposal.node_id FOR UPDATE;
  IF proposal.id IS NULL OR head.node_id IS NULL OR head.state<>'successor_pending' OR head.generation<>proposal.successor_generation
     OR head.claim_token_id<>proposal.successor_token_id THEN
    RAISE EXCEPTION 'claim successor result lacks exact pending head';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_worker_claim_successor_results_validate
BEFORE INSERT ON recording_worker_claim_successor_results FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_claim_successor_result();

-- R10 authority validators. HTTP handlers are orchestration only; a runtime
-- role issuing direct SQL must still be unable to forge a plan outcome, stop
-- inventory, recovery terminal, or incomplete set seal.
CREATE FUNCTION recording_surrender_validate_plan_result() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE plan RECORD;
BEGIN
  NEW.result_at:=transaction_timestamp();
  SELECT * INTO plan FROM recording_capture_set_plans WHERE id=NEW.plan_id FOR SHARE;
  IF NOT FOUND THEN RAISE EXCEPTION 'capture set plan is missing'; END IF;
  IF NEW.result='accepted_set' THEN
    IF NEW.set_id IS DISTINCT FROM plan.set_id OR NOT EXISTS(
      SELECT 1 FROM recording_capture_reservation_sets capture_set
      WHERE capture_set.id=NEW.set_id AND capture_set.plan_id=NEW.plan_id
    ) THEN RAISE EXCEPTION 'accepted plan result lacks its exact committed set'; END IF;
  ELSIF NEW.set_id IS NOT NULL OR EXISTS(
    SELECT 1 FROM recording_capture_reservation_sets capture_set WHERE capture_set.plan_id=NEW.plan_id
  ) THEN
    RAISE EXCEPTION 'no-byte plan result conflicts with a committed set';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_capture_set_plan_results_validate
BEFORE INSERT ON recording_capture_set_plan_results FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_plan_result();

CREATE FUNCTION recording_surrender_expire_set_plans() RETURNS BIGINT LANGUAGE plpgsql AS $$
DECLARE target RECORD; changed BIGINT:=0;
BEGIN
  FOR target IN
    SELECT plan.id
    FROM recording_capture_set_plans plan
    WHERE plan.expires_at<=transaction_timestamp()
      AND NOT EXISTS(SELECT 1 FROM recording_capture_set_plan_results result WHERE result.plan_id=plan.id)
      AND NOT EXISTS(SELECT 1 FROM recording_capture_reservation_sets capture_set WHERE capture_set.plan_id=plan.id)
    ORDER BY plan.id
    FOR UPDATE OF plan SKIP LOCKED
  LOOP
    INSERT INTO recording_capture_set_plan_results(plan_id,result)
    VALUES(target.id,'expired_no_bytes') ON CONFLICT(plan_id) DO NOTHING;
    IF FOUND THEN changed:=changed+1; END IF;
  END LOOP;
  RETURN changed;
END $$;

CREATE FUNCTION recording_surrender_validate_set_commit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NOT EXISTS(
    SELECT 1 FROM recording_capture_set_plan_results result
    WHERE result.plan_id=NEW.plan_id AND result.result='accepted_set' AND result.set_id=NEW.id
  ) THEN RAISE EXCEPTION 'capture set lacks exact accepted plan result'; END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER recording_capture_reservation_sets_validate
AFTER INSERT ON recording_capture_reservation_sets DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_set_commit();

CREATE FUNCTION recording_surrender_validate_materialized_artifact() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expected_count INTEGER; first_sequence BIGINT;
BEGIN
  NEW.materialized_at:=transaction_timestamp();
  SELECT capture_set.artifact_count,plan.first_capture_sequence INTO expected_count,first_sequence
  FROM recording_capture_reservation_sets capture_set
  JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
  WHERE capture_set.id=NEW.set_id FOR SHARE OF capture_set,plan;
  IF NOT FOUND OR NEW.ordinal>expected_count OR NEW.capture_sequence<>first_sequence+NEW.ordinal-1 THEN
    RAISE EXCEPTION 'materialized artifact is outside its committed set';
  END IF;
  IF EXISTS(SELECT 1 FROM recording_capture_set_results result WHERE result.set_id=NEW.set_id) THEN
    RAISE EXCEPTION 'terminal capture set cannot materialize another artifact';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_capture_materialized_artifacts_validate
BEFORE INSERT ON recording_capture_materialized_artifacts FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_materialized_artifact();

-- A stop event closes ordinary materialization immediately.  The stop-ACK
-- transaction may insert the exact inventoried artifact before its member row,
-- so the inverse is deferred and sealed at COMMIT rather than weakened with a
-- transaction-local bypass.
CREATE FUNCTION recording_surrender_validate_stopped_artifact_membership() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF EXISTS(SELECT 1 FROM recording_capture_producer_stop_events stop WHERE stop.set_id=NEW.set_id)
     AND NOT EXISTS(
       SELECT 1
       FROM recording_capture_producer_stop_events stop
       JOIN recording_capture_producer_stop_acks ack ON ack.stop_event_id=stop.id AND ack.set_id=stop.set_id
       JOIN recording_capture_stop_ack_members member
         ON member.stop_ack_id=ack.id AND member.ordinal=NEW.ordinal AND member.artifact_id=NEW.artifact_id
       WHERE stop.set_id=NEW.set_id
     ) THEN
    RAISE EXCEPTION 'stopped capture artifact is not present in the exact stop inventory';
  END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER recording_capture_materialized_artifacts_stop_seal
AFTER INSERT ON recording_capture_materialized_artifacts DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_stopped_artifact_membership();

CREATE FUNCTION recording_surrender_validate_stopped_artifact_seal() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF EXISTS(SELECT 1 FROM recording_capture_producer_stop_events stop WHERE stop.set_id=NEW.set_id)
     AND NOT EXISTS(
       SELECT 1
       FROM recording_capture_producer_stop_events stop
       JOIN recording_capture_producer_stop_acks ack ON ack.stop_event_id=stop.id AND ack.set_id=stop.set_id
       JOIN recording_capture_stop_ack_members member
         ON member.stop_ack_id=ack.id AND member.ordinal=NEW.ordinal AND member.artifact_id=NEW.artifact_id
       WHERE stop.set_id=NEW.set_id
     ) THEN
    RAISE EXCEPTION 'post-stop artifact seal is outside the acknowledged inventory';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_capture_materialized_artifact_seals_stop_guard
BEFORE INSERT ON recording_capture_materialized_artifact_seals
FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_stopped_artifact_seal();

CREATE FUNCTION recording_surrender_validate_stop_event() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE plan RECORD; prior_count BIGINT;
BEGIN
  NEW.required_at:=transaction_timestamp();
  SELECT plan.recording_id,plan.snapshot_generation,plan.source_snapshot,plan.destination_naming_snapshot
    INTO plan
  FROM recording_capture_reservation_sets capture_set
  JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
  WHERE capture_set.id=NEW.set_id FOR SHARE OF capture_set,plan;
  SELECT count(*) INTO prior_count FROM recording_capture_producer_stop_events prior WHERE prior.set_id=NEW.set_id;
  IF plan.recording_id IS NULL OR NEW.old_snapshot_generation<>plan.snapshot_generation
     OR NEW.new_snapshot_generation<>plan.snapshot_generation+prior_count+1
     OR (plan.source_snapshot IS NOT DISTINCT FROM recording_surrender_source_snapshot(plan.recording_id)
         AND plan.destination_naming_snapshot IS NOT DISTINCT FROM recording_surrender_destination_snapshot(plan.recording_id))
     OR EXISTS(SELECT 1 FROM recording_capture_set_results result WHERE result.set_id=NEW.set_id)
     OR EXISTS(SELECT 1 FROM recording_capture_producer_stop_events prior
               WHERE prior.set_id=NEW.set_id
                 AND NOT EXISTS(SELECT 1 FROM recording_capture_producer_stop_acks ack WHERE ack.stop_event_id=prior.id)) THEN
    RAISE EXCEPTION 'capture producer stop event lacks exact changed snapshot authority';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_capture_producer_stop_events_validate
BEFORE INSERT ON recording_capture_producer_stop_events FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_stop_event();

CREATE FUNCTION recording_surrender_validate_stop_ack() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.acknowledged_at:=transaction_timestamp();
  IF NOT EXISTS(
    SELECT 1 FROM recording_capture_producer_stop_events stop
    WHERE stop.id=NEW.stop_event_id AND stop.set_id=NEW.set_id
  ) OR EXISTS(SELECT 1 FROM recording_capture_set_results result WHERE result.set_id=NEW.set_id) THEN
    RAISE EXCEPTION 'capture stop acknowledgment differs from stop authority';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_capture_producer_stop_acks_validate
BEFORE INSERT ON recording_capture_producer_stop_acks FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_stop_ack();

CREATE FUNCTION recording_surrender_validate_stop_ack_member() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NOT EXISTS(
    SELECT 1 FROM recording_capture_producer_stop_acks ack
    JOIN recording_capture_materialized_artifacts artifact
      ON artifact.set_id=ack.set_id AND artifact.ordinal=NEW.ordinal AND artifact.artifact_id=NEW.artifact_id
    WHERE ack.id=NEW.stop_ack_id
  ) THEN RAISE EXCEPTION 'capture stop member differs from materialized set authority'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_capture_stop_ack_members_validate
BEFORE INSERT ON recording_capture_stop_ack_members FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_stop_ack_member();

CREATE FUNCTION recording_surrender_stop_inventory_sha(p_ack_id UUID) RETURNS TEXT LANGUAGE sql STABLE AS $$
  SELECT encode(sha256(convert_to(
    'recording-capture-stop-inventory-v3'||chr(10)
    ||octet_length(ack.retained_directory_device::text)::text||':'||ack.retained_directory_device::text||chr(10)
    ||octet_length(ack.retained_directory_inode::text)::text||':'||ack.retained_directory_inode::text||chr(10)
    ||COALESCE((SELECT string_agg(
        octet_length(member.ordinal::text)::text||':'||member.ordinal::text||chr(10)
        ||octet_length(member.artifact_id::text)::text||':'||member.artifact_id::text||chr(10)
        ||octet_length(artifact.capture_sequence::text)::text||':'||artifact.capture_sequence::text||chr(10)
        ||octet_length(artifact.recovery_secret_sha256)::text||':'||artifact.recovery_secret_sha256||chr(10)
        ||octet_length(artifact.proof_sha256)::text||':'||artifact.proof_sha256||chr(10)
        ||octet_length(member.device::text)::text||':'||member.device::text||chr(10)
        ||octet_length(member.inode::text)::text||':'||member.inode::text||chr(10)
        ||octet_length(member.size_bytes::text)::text||':'||member.size_bytes::text||chr(10)
        ||octet_length(member.relative_name)::text||':'||member.relative_name||chr(10),'' ORDER BY member.ordinal)
      FROM recording_capture_stop_ack_members member
      JOIN recording_capture_materialized_artifacts artifact
        ON artifact.set_id=ack.set_id AND artifact.ordinal=member.ordinal AND artifact.artifact_id=member.artifact_id
      WHERE member.stop_ack_id=ack.id),'')
    ,'UTF8')),'hex')
  FROM recording_capture_producer_stop_acks ack WHERE ack.id=p_ack_id
$$;

CREATE FUNCTION recording_surrender_validate_stop_inventory_seal() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE ack_id UUID; expected TEXT;
BEGIN
  ack_id:=CASE WHEN TG_TABLE_NAME='recording_capture_producer_stop_acks' THEN NEW.id ELSE NEW.stop_ack_id END;
  SELECT recording_surrender_stop_inventory_sha(ack_id) INTO expected;
  IF expected IS NULL OR NOT EXISTS(
       SELECT 1 FROM recording_capture_producer_stop_acks ack
       JOIN recording_capture_reservation_sets capture_set ON capture_set.id=ack.set_id
       WHERE ack.id=ack_id AND ack.inventory_sha256=expected
         AND (SELECT count(*) FROM recording_capture_stop_ack_members member WHERE member.stop_ack_id=ack.id)<=capture_set.artifact_count
     ) THEN RAISE EXCEPTION 'capture stop inventory seal is incomplete or noncanonical'; END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER recording_capture_producer_stop_acks_inventory_seal
AFTER INSERT ON recording_capture_producer_stop_acks DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_stop_inventory_seal();
CREATE CONSTRAINT TRIGGER recording_capture_stop_ack_members_inventory_seal
AFTER INSERT ON recording_capture_stop_ack_members DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_stop_inventory_seal();

CREATE FUNCTION recording_surrender_validate_recovery_report() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE grace_until TIMESTAMPTZ;
BEGIN
  NEW.received_at:=transaction_timestamp();
  SELECT grant.upload_grace_until INTO grace_until
  FROM recording_capture_set_grants grant WHERE grant.set_id=NEW.set_id FOR SHARE;
  IF NOT FOUND OR NEW.received_at>grace_until OR NEW.local_observed_at>NEW.received_at+interval '1 minute'
     OR NEW.local_observed_at<NEW.received_at-interval '30 minutes' THEN
    RAISE EXCEPTION 'recovery report is outside its exact grant window';
  END IF;
  IF (NEW.report_type='sealed_bytes') IS DISTINCT FROM EXISTS(
    SELECT 1 FROM recording_capture_materialized_artifact_seals seal
    WHERE seal.set_id=NEW.set_id AND seal.ordinal=NEW.ordinal
      AND seal.size_bytes=NEW.size_bytes AND seal.sha256=NEW.sha256
  ) AND NEW.report_type='sealed_bytes' THEN
    RAISE EXCEPTION 'sealed recovery report differs from artifact seal';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_capture_recovery_reports_validate
BEFORE INSERT ON recording_capture_recovery_reports FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_recovery_report();

CREATE FUNCTION recording_surrender_validate_artifact_grant_result() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE report_type TEXT; event_set UUID; event_ordinal INTEGER; grace_until TIMESTAMPTZ; ack_matches BOOLEAN;
BEGIN
  NEW.result_at:=transaction_timestamp();
  SELECT grant.upload_grace_until INTO grace_until FROM recording_capture_set_grants grant
  WHERE grant.set_id=NEW.set_id FOR SHARE;
  IF NEW.report_id IS NOT NULL THEN
    SELECT report.report_type INTO report_type FROM recording_capture_recovery_reports report
    WHERE report.id=NEW.report_id AND report.set_id=NEW.set_id AND report.ordinal=NEW.ordinal;
  END IF;
  IF (NEW.result='acknowledged_no_bytes') IS DISTINCT FROM (NEW.stop_ack_id IS NOT NULL) THEN
	RAISE EXCEPTION 'artifact result has invalid stop acknowledgment shape';
  END IF;
  IF NEW.result IN('accepted_unique','exact_replay') THEN
	IF grace_until IS NOT NULL AND report_type IS DISTINCT FROM 'sealed_bytes' THEN
	  RAISE EXCEPTION 'recovered accepted artifact lacks exact sealed local report';
	END IF;
    IF NEW.clip_id IS NULL OR (grace_until IS NULL AND NEW.report_id IS NOT NULL) OR NEW.security_event_id IS NOT NULL THEN
      RAISE EXCEPTION 'accepted artifact result has invalid evidence shape'; END IF;
  ELSIF NEW.result='acknowledged_no_bytes' THEN
	SELECT EXISTS(
	  SELECT 1
	  FROM recording_capture_producer_stop_acks ack
	  JOIN recording_capture_stop_ack_members member
	    ON member.stop_ack_id=ack.id AND member.ordinal=NEW.ordinal
	  JOIN recording_capture_materialized_artifacts artifact
	    ON artifact.set_id=ack.set_id AND artifact.ordinal=member.ordinal AND artifact.artifact_id=member.artifact_id
	  JOIN recording_capture_reservation_sets capture_set ON capture_set.id=ack.set_id
	  JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
	  JOIN recording_jobs job ON job.id=plan.recording_job_id
	  WHERE ack.id=NEW.stop_ack_id AND ack.set_id=NEW.set_id AND member.size_bytes=0
	    AND job.status='leased' AND job.lease_token=plan.lease_token
	    AND job.lease_expires_at>transaction_timestamp() AND job.lease_credential_state='exact'
	    AND NOT EXISTS(SELECT 1 FROM recording_capture_set_grants grant WHERE grant.set_id=NEW.set_id)
	) INTO ack_matches;
	IF NOT ack_matches OR NEW.report_id IS NOT NULL OR NEW.clip_id IS NOT NULL OR NEW.security_event_id IS NOT NULL THEN
	  RAISE EXCEPTION 'acknowledged no-bytes result lacks exact immutable zero-byte inventory'; END IF;
  ELSIF NEW.result='abandoned_no_bytes' THEN
	IF grace_until IS NULL THEN RAISE EXCEPTION 'no-bytes result lacks set recovery grant'; END IF;
    IF report_type IS DISTINCT FROM 'no_bytes' OR NEW.clip_id IS NOT NULL OR NEW.security_event_id IS NOT NULL THEN
      RAISE EXCEPTION 'no-bytes result lacks exact local report'; END IF;
  ELSIF NEW.result='unrecoverable_partial' THEN
	IF grace_until IS NULL THEN RAISE EXCEPTION 'partial result lacks set recovery grant'; END IF;
    IF report_type IS DISTINCT FROM 'partial_bytes' OR NEW.clip_id IS NOT NULL OR NEW.security_event_id IS NOT NULL THEN
      RAISE EXCEPTION 'partial result lacks exact local report'; END IF;
  ELSIF NEW.result='host_unreachable' THEN
	IF grace_until IS NULL THEN RAISE EXCEPTION 'host-unreachable result lacks set recovery grant'; END IF;
    IF NEW.report_id IS NOT NULL OR NEW.clip_id IS NOT NULL OR NEW.security_event_id IS NOT NULL
       OR transaction_timestamp()<grace_until THEN
      RAISE EXCEPTION 'host-unreachable result lacks DB deadline authority'; END IF;
  ELSIF NEW.result='security_revoked' THEN
	IF grace_until IS NULL THEN RAISE EXCEPTION 'security result lacks set recovery grant'; END IF;
    SELECT event.set_id,event.ordinal INTO event_set,event_ordinal FROM recording_capture_security_events event
    WHERE event.id=NEW.security_event_id;
    IF event_set IS DISTINCT FROM NEW.set_id OR (event_ordinal IS NOT NULL AND event_ordinal<>NEW.ordinal)
       OR NEW.report_id IS NOT NULL OR NEW.clip_id IS NOT NULL THEN
      RAISE EXCEPTION 'security result lacks exact immutable event'; END IF;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_capture_artifact_grant_results_validate
BEFORE INSERT ON recording_capture_artifact_grant_results FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_artifact_grant_result();

CREATE FUNCTION recording_surrender_unused_capture_ranges(p_count INTEGER,p_materialized INTEGER[]) RETURNS JSONB LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE output JSONB:='[]'::jsonb; ordinal INTEGER:=1; first_unused INTEGER;
BEGIN
  WHILE ordinal<=p_count LOOP
    IF ordinal=ANY(p_materialized) THEN ordinal:=ordinal+1; CONTINUE; END IF;
    first_unused:=ordinal;
    WHILE ordinal<=p_count AND NOT ordinal=ANY(p_materialized) LOOP ordinal:=ordinal+1; END LOOP;
    output:=output||jsonb_build_array(jsonb_build_array(first_unused,ordinal-1));
  END LOOP;
  RETURN output;
END $$;

CREATE FUNCTION recording_surrender_validate_set_result() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expected_count INTEGER; materialized INTEGER[]; expected_materialized JSONB; expected_unused JSONB; nonterminal BIGINT;
BEGIN
  NEW.result_at:=transaction_timestamp();
  SELECT capture_set.artifact_count INTO expected_count FROM recording_capture_reservation_sets capture_set
  WHERE capture_set.id=NEW.set_id FOR SHARE;
  SELECT COALESCE(array_agg(artifact.ordinal ORDER BY artifact.ordinal),'{}'::integer[]) INTO materialized
  FROM recording_capture_materialized_artifacts artifact WHERE artifact.set_id=NEW.set_id;
  SELECT count(*) INTO nonterminal FROM recording_capture_materialized_artifacts artifact
  LEFT JOIN recording_capture_artifact_grant_results result
    ON result.set_id=artifact.set_id AND result.ordinal=artifact.ordinal
  WHERE artifact.set_id=NEW.set_id AND result.set_id IS NULL;
  expected_materialized:=to_jsonb(materialized);
  expected_unused:=recording_surrender_unused_capture_ranges(expected_count,materialized);
  IF EXISTS(SELECT 1 FROM recording_capture_producer_stop_events stop WHERE stop.set_id=NEW.set_id)
     AND (NOT EXISTS(
       SELECT 1 FROM recording_capture_producer_stop_events stop
       JOIN recording_capture_producer_stop_acks ack ON ack.stop_event_id=stop.id
       WHERE stop.set_id=NEW.set_id
     ) OR EXISTS(
       SELECT 1 FROM recording_capture_materialized_artifacts artifact
       WHERE artifact.set_id=NEW.set_id AND NOT EXISTS(
         SELECT 1 FROM recording_capture_producer_stop_events stop
         JOIN recording_capture_producer_stop_acks ack ON ack.stop_event_id=stop.id
         JOIN recording_capture_stop_ack_members member
           ON member.stop_ack_id=ack.id AND member.ordinal=artifact.ordinal AND member.artifact_id=artifact.artifact_id
         WHERE stop.set_id=NEW.set_id
       )
     )) THEN
    RAISE EXCEPTION 'terminal stopped capture set does not consume its exact stop boundary';
  END IF;
  IF nonterminal<>0 OR NEW.coverage_ranges->'artifact_count'<>to_jsonb(expected_count)
     OR NEW.coverage_ranges->'materialized_ordinals'<>expected_materialized
     OR NEW.coverage_ranges->'unused_ranges'<>expected_unused
     OR NEW.coverage_sha256<>encode(sha256(convert_to(NEW.coverage_ranges::text,'UTF8')),'hex') THEN
    RAISE EXCEPTION 'capture set terminal coverage is incomplete or noncanonical';
  END IF;
  IF NEW.result='completed' AND (
       cardinality(materialized)=0
       OR EXISTS(
         SELECT 1 FROM recording_capture_artifact_grant_results result
         WHERE result.set_id=NEW.set_id AND result.result NOT IN('accepted_unique','exact_replay')
       )
     ) THEN RAISE EXCEPTION 'completed capture set must contain only accepted artifacts'; END IF;
  IF NEW.result='abandoned' AND (
       NOT EXISTS(SELECT 1 FROM recording_capture_artifact_grant_results result
                  WHERE result.set_id=NEW.set_id AND result.result IN('acknowledged_no_bytes','abandoned_no_bytes','unrecoverable_partial'))
       AND NOT (cardinality(materialized)=0 AND (
         EXISTS(SELECT 1 FROM recording_capture_producer_stop_acks ack WHERE ack.set_id=NEW.set_id)
         OR EXISTS(SELECT 1 FROM recording_capture_empty_set_reports empty_report WHERE empty_report.set_id=NEW.set_id)
         OR EXISTS(
           SELECT 1 FROM recording_capture_reservation_sets capture_set
           JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
           JOIN recording_jobs job ON job.id=plan.recording_job_id
           WHERE capture_set.id=NEW.set_id AND job.status='leased'
             AND job.lease_token=plan.lease_token AND job.lease_expires_at>transaction_timestamp()
             AND job.lease_credential_state='exact'
         )
       ))
       OR EXISTS(SELECT 1 FROM recording_capture_artifact_grant_results result
                 WHERE result.set_id=NEW.set_id AND result.result IN('host_unreachable','security_revoked'))
     ) THEN RAISE EXCEPTION 'abandoned capture set has invalid terminal outcomes'; END IF;
  IF NEW.result='host_unreachable' AND (
       (EXISTS(SELECT 1 FROM recording_capture_materialized_artifacts artifact WHERE artifact.set_id=NEW.set_id)
        AND NOT EXISTS(SELECT 1 FROM recording_capture_artifact_grant_results result
                       WHERE result.set_id=NEW.set_id AND result.result='host_unreachable'))
       OR NOT EXISTS(SELECT 1 FROM recording_capture_set_grants grant
                     WHERE grant.set_id=NEW.set_id AND grant.upload_grace_until<=transaction_timestamp())
       OR EXISTS(SELECT 1 FROM recording_capture_artifact_grant_results result
                 WHERE result.set_id=NEW.set_id AND result.result='security_revoked')
     ) THEN RAISE EXCEPTION 'host-unreachable capture set has invalid terminal outcomes'; END IF;
  IF NEW.result='security_revoked' AND (
		   NOT EXISTS(SELECT 1 FROM recording_capture_security_events event WHERE event.set_id=NEW.set_id)
     ) THEN RAISE EXCEPTION 'security-revoked capture set lacks exact security authority'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_capture_set_results_validate
BEFORE INSERT ON recording_capture_set_results FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_set_result();

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
  source_snapshot_sha256 TEXT NOT NULL CHECK(source_snapshot_sha256 ~ '^[0-9a-f]{64}$'),
  source_revision_head_id BIGINT REFERENCES stream_source_revisions(id) ON DELETE RESTRICT,
  capture_config_sha256 TEXT NOT NULL CHECK(capture_config_sha256 ~ '^[0-9a-f]{64}$'),
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

-- One exact upload intent and one exact recovery secret exist before the media
-- process can open its corresponding output file. A reserved_unsealed row is
-- durable authority too: expiry must grant or terminalize it, never forget it.
CREATE TABLE recording_capture_artifact_intents (
  upload_intent_id UUID PRIMARY KEY REFERENCES recording_upload_intents(id) ON DELETE RESTRICT,
  producer_id UUID NOT NULL REFERENCES recording_capture_producers(id) ON DELETE RESTRICT,
  capture_sequence BIGINT NOT NULL CHECK(capture_sequence > 0),
  recovery_secret_sha256 TEXT NOT NULL CHECK(recovery_secret_sha256 ~ '^[0-9a-f]{64}$'),
  max_size_bytes BIGINT NOT NULL CHECK(max_size_bytes > 0),
  reserved_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  UNIQUE(producer_id,capture_sequence),
  UNIQUE(recovery_secret_sha256)
);

-- Once an upload intent is bound before bytes, its destination/object/size
-- identity is immutable. Runtime may only refresh the exact presign lifetime,
-- consume the exact uploaded object, or expire a terminal unused reservation.
CREATE FUNCTION recording_surrender_validate_bound_upload_intent() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE terminal_result TEXT;
BEGIN
  IF NOT EXISTS(SELECT 1 FROM recording_capture_artifact_intents artifact WHERE artifact.upload_intent_id=OLD.id) THEN
    IF TG_OP='DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
  END IF;
  IF TG_OP='DELETE' THEN RAISE EXCEPTION 'bound capture upload intent is immutable'; END IF;
  IF (NEW.id,NEW.recording_id,NEW.recording_job_id,NEW.storage_destination_id,NEW.endpoint,NEW.bucket,
      NEW.object_key,NEW.display_path,NEW.mime_type,NEW.max_size_bytes,NEW.created_at)
       IS DISTINCT FROM
     (OLD.id,OLD.recording_id,OLD.recording_job_id,OLD.storage_destination_id,OLD.endpoint,OLD.bucket,
      OLD.object_key,OLD.display_path,OLD.mime_type,OLD.max_size_bytes,OLD.created_at) THEN
    RAISE EXCEPTION 'bound capture upload identity is immutable';
  END IF;
  IF OLD.status IN('pending','expired') AND NEW.status='pending' THEN
    IF NEW.expires_at IS DISTINCT FROM transaction_timestamp()+interval '15 minutes'
       OR NOT EXISTS(SELECT 1 FROM recording_capture_artifact_seals seal WHERE seal.upload_intent_id=OLD.id) THEN
      RAISE EXCEPTION 'bound capture presign refresh lacks an exact sealed artifact';
    END IF;
  ELSIF OLD.status='pending' AND NEW.status='consumed' THEN
    IF NEW.expires_at IS DISTINCT FROM OLD.expires_at OR NOT EXISTS(
      SELECT 1
      FROM recording_capture_artifact_seals seal
      JOIN recording_capture_producers producer ON producer.id=seal.producer_id
      JOIN recording_clips clip ON clip.recording_job_id=producer.recording_job_id
        AND clip.capture_lease_token=producer.lease_token AND clip.capture_sequence=seal.capture_sequence
      WHERE seal.upload_intent_id=OLD.id AND clip.recording_id=OLD.recording_id
        AND clip.storage_destination_id=OLD.storage_destination_id AND clip.endpoint=OLD.endpoint
        AND clip.bucket=OLD.bucket AND clip.object_key=OLD.object_key
        AND clip.size_bytes=seal.size_bytes AND clip.sha256=seal.sha256
    ) THEN
      RAISE EXCEPTION 'bound capture consume lacks its exact stored clip';
    END IF;
  ELSIF OLD.status='pending' AND NEW.status='expired' THEN
    SELECT result INTO terminal_result FROM recording_capture_artifact_results WHERE upload_intent_id=OLD.id;
    IF terminal_result IS NULL OR terminal_result IN('accepted_unique','exact_replay') OR NEW.expires_at IS DISTINCT FROM OLD.expires_at THEN
      RAISE EXCEPTION 'bound capture expiry lacks terminal unused authority';
    END IF;
  ELSE
    RAISE EXCEPTION 'invalid bound capture upload transition';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_surrender_bound_upload_intent_update
BEFORE UPDATE OR DELETE ON recording_upload_intents FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_bound_upload_intent();

CREATE TABLE recording_capture_artifact_seals (
  upload_intent_id UUID PRIMARY KEY REFERENCES recording_capture_artifact_intents(upload_intent_id) ON DELETE RESTRICT,
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
  upload_intent_id UUID PRIMARY KEY REFERENCES recording_capture_artifact_intents(upload_intent_id) ON DELETE RESTRICT,
  result TEXT NOT NULL CHECK(result IN ('accepted_unique','exact_replay','abandoned_unsealed','unrecoverable_partial','host_unreachable_unrecoverable','security_revoked')),
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
  upload_intent_id UUID NOT NULL UNIQUE REFERENCES recording_capture_artifact_intents(upload_intent_id) ON DELETE RESTRICT,
  recovery_secret_sha256 TEXT NOT NULL CHECK(recovery_secret_sha256 ~ '^[0-9a-f]{64}$'),
  granted_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  upload_grace_until TIMESTAMPTZ NOT NULL,
  UNIQUE(recording_job_id,lease_token,upload_intent_id),
  FOREIGN KEY(recording_job_id,lease_token)
    REFERENCES recording_job_lease_generations(recording_job_id,lease_token) ON DELETE RESTRICT,
  CHECK(upload_grace_until=granted_at+interval '30 minutes')
);
CREATE INDEX recording_job_recovery_grants_producer_idx ON recording_job_recovery_grants(producer_id);

CREATE TABLE recording_job_recovery_grant_results (
  grant_id UUID PRIMARY KEY REFERENCES recording_job_recovery_grants(id) ON DELETE RESTRICT,
  result TEXT NOT NULL CHECK(result IN('recovery_completed','security_incident','recovery_grace_expired')),
  result_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp()
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

CREATE TABLE recording_surrender_transport_observations (
  id UUID PRIMARY KEY,
  recording_job_id BIGINT NOT NULL,
  lease_token UUID NOT NULL,
  worker_id TEXT NOT NULL CHECK(octet_length(worker_id) BETWEEN 1 AND 255),
  node_id BIGINT NOT NULL REFERENCES nodes(id) ON DELETE RESTRICT,
  observation_type TEXT NOT NULL CHECK(observation_type IN
    ('request_started','request_transport_failed','request_result_received','transport_budget_exhausted','expired_reclaim')),
  attempt_id UUID,
  error_class TEXT NOT NULL DEFAULT '' CHECK(error_class ~ '^[a-z0-9_]{0,64}$'),
  observed_at TIMESTAMPTZ NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  request_sha256 TEXT NOT NULL CHECK(request_sha256 ~ '^[0-9a-f]{64}$'),
  FOREIGN KEY(recording_job_id,lease_token)
    REFERENCES recording_job_lease_generations(recording_job_id,lease_token) ON DELETE RESTRICT
);

CREATE TABLE recording_surrender_transport_episodes (
  recording_job_id BIGINT PRIMARY KEY REFERENCES recording_jobs(id) ON DELETE RESTRICT,
  episode_key UUID NOT NULL UNIQUE,
  lease_token UUID NOT NULL,
  state TEXT NOT NULL CHECK(state IN('open','resolved')),
  reason TEXT NOT NULL CHECK(reason IN('transport_budget_exhausted','expired_reclaim')),
  opened_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  last_observed_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  resolved_at TIMESTAMPTZ,
  CHECK((state='open' AND resolved_at IS NULL) OR (state='resolved' AND resolved_at IS NOT NULL))
);

CREATE TABLE recording_surrender_transport_episode_events (
  event_key UUID PRIMARY KEY,
  episode_key UUID NOT NULL REFERENCES recording_surrender_transport_episodes(episode_key) ON DELETE RESTRICT,
  recording_job_id BIGINT NOT NULL,
  lease_token UUID NOT NULL,
  event_type TEXT NOT NULL CHECK(event_type IN('opened','resolved')),
  reason TEXT NOT NULL CHECK(reason IN('transport_budget_exhausted','expired_reclaim','accepted_unique')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  FOREIGN KEY(recording_job_id,lease_token)
    REFERENCES recording_job_lease_generations(recording_job_id,lease_token) ON DELETE RESTRICT
);

CREATE FUNCTION recording_surrender_freeze_rows() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'recording surrender authority is append-only';
END $$;

CREATE TRIGGER recording_worker_claim_heads_no_truncate
BEFORE TRUNCATE ON recording_worker_claim_heads FOR EACH STATEMENT EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_node_tokens_claim_no_truncate
BEFORE TRUNCATE ON node_tokens FOR EACH STATEMENT EXECUTE FUNCTION recording_surrender_freeze_rows();

DO $freeze_r10_authority$
DECLARE table_name TEXT;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'recording_worker_claim_generation_events','recording_worker_claim_successor_proposals',
    'recording_worker_claim_successor_results','recording_capture_set_plans',
    'recording_capture_set_plan_results','recording_capture_reservation_sets',
    'recording_capture_producer_stop_events','recording_capture_producer_stop_acks',
    'recording_capture_stop_ack_members','recording_capture_set_grants',
    'recording_capture_materialized_artifacts','recording_capture_materialized_artifact_seals',
    'recording_capture_recovery_reports','recording_capture_empty_set_reports','recording_capture_artifact_grant_results',
    'recording_capture_set_results','recording_capture_security_events',
    'recording_recovery_upload_sessions',
    'recording_recovery_upload_session_results'
  ] LOOP
    EXECUTE format('CREATE TRIGGER %I_freeze BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION recording_surrender_freeze_rows()',table_name,table_name);
    EXECUTE format('CREATE TRIGGER %I_no_truncate BEFORE TRUNCATE ON %I FOR EACH STATEMENT EXECUTE FUNCTION recording_surrender_freeze_rows()',table_name,table_name);
  END LOOP;
END
$freeze_r10_authority$;
CREATE TRIGGER recording_object_key_roots_no_truncate
BEFORE TRUNCATE ON recording_object_key_roots FOR EACH STATEMENT EXECUTE FUNCTION recording_surrender_freeze_rows();

CREATE TRIGGER recording_job_lease_generations_freeze BEFORE UPDATE OR DELETE ON recording_job_lease_generations FOR EACH ROW EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_job_lease_generations_no_truncate BEFORE TRUNCATE ON recording_job_lease_generations FOR EACH STATEMENT EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_capture_producers_freeze BEFORE UPDATE OR DELETE ON recording_capture_producers FOR EACH ROW EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_capture_producers_no_truncate BEFORE TRUNCATE ON recording_capture_producers FOR EACH STATEMENT EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_capture_producer_results_freeze BEFORE UPDATE OR DELETE ON recording_capture_producer_results FOR EACH ROW EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_capture_producer_results_no_truncate BEFORE TRUNCATE ON recording_capture_producer_results FOR EACH STATEMENT EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_capture_artifact_intents_freeze BEFORE UPDATE OR DELETE ON recording_capture_artifact_intents FOR EACH ROW EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_capture_artifact_intents_no_truncate BEFORE TRUNCATE ON recording_capture_artifact_intents FOR EACH STATEMENT EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_capture_artifact_seals_freeze BEFORE UPDATE OR DELETE ON recording_capture_artifact_seals FOR EACH ROW EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_capture_artifact_seals_no_truncate BEFORE TRUNCATE ON recording_capture_artifact_seals FOR EACH STATEMENT EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_capture_artifact_results_freeze BEFORE UPDATE OR DELETE ON recording_capture_artifact_results FOR EACH ROW EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_capture_artifact_results_no_truncate BEFORE TRUNCATE ON recording_capture_artifact_results FOR EACH STATEMENT EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_job_recovery_grants_freeze BEFORE UPDATE OR DELETE ON recording_job_recovery_grants FOR EACH ROW EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_job_recovery_grants_no_truncate BEFORE TRUNCATE ON recording_job_recovery_grants FOR EACH STATEMENT EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_job_recovery_grant_results_freeze BEFORE UPDATE OR DELETE ON recording_job_recovery_grant_results FOR EACH ROW EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_job_recovery_grant_results_no_truncate BEFORE TRUNCATE ON recording_job_recovery_grant_results FOR EACH STATEMENT EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_job_surrender_attempts_freeze BEFORE UPDATE OR DELETE ON recording_job_surrender_attempts FOR EACH ROW EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_job_surrender_attempts_no_truncate BEFORE TRUNCATE ON recording_job_surrender_attempts FOR EACH STATEMENT EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_job_surrender_results_freeze BEFORE UPDATE OR DELETE ON recording_job_surrender_results FOR EACH ROW EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_job_surrender_results_no_truncate BEFORE TRUNCATE ON recording_job_surrender_results FOR EACH STATEMENT EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_job_lease_expiry_events_freeze BEFORE UPDATE OR DELETE ON recording_job_lease_expiry_events FOR EACH ROW EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_job_lease_expiry_events_no_truncate BEFORE TRUNCATE ON recording_job_lease_expiry_events FOR EACH STATEMENT EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_surrender_transport_observations_freeze BEFORE UPDATE OR DELETE ON recording_surrender_transport_observations FOR EACH ROW EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_surrender_transport_observations_no_truncate BEFORE TRUNCATE ON recording_surrender_transport_observations FOR EACH STATEMENT EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_surrender_transport_episode_events_freeze BEFORE UPDATE OR DELETE ON recording_surrender_transport_episode_events FOR EACH ROW EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_surrender_transport_episode_events_no_truncate BEFORE TRUNCATE ON recording_surrender_transport_episode_events FOR EACH STATEMENT EXECUTE FUNCTION recording_surrender_freeze_rows();

CREATE FUNCTION recording_surrender_validate_episode_write() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE matching_observation BOOLEAN; matching_ingest BOOLEAN;
BEGIN
  IF TG_OP='DELETE' THEN RAISE EXCEPTION 'recording surrender episode authority is append-only'; END IF;
  IF TG_OP='INSERT' THEN
    SELECT EXISTS(
      SELECT 1 FROM recording_surrender_transport_observations observation
      WHERE observation.id=NEW.episode_key AND observation.recording_job_id=NEW.recording_job_id
        AND observation.lease_token=NEW.lease_token
        AND ((NEW.reason='transport_budget_exhausted' AND observation.observation_type='transport_budget_exhausted')
          OR (NEW.reason='expired_reclaim' AND observation.observation_type='expired_reclaim'))
    ) INTO matching_observation;
    IF NEW.state<>'open' OR NEW.opened_at IS DISTINCT FROM transaction_timestamp()
       OR NEW.last_observed_at IS DISTINCT FROM transaction_timestamp() OR NOT matching_observation THEN
      RAISE EXCEPTION 'invalid surrender transport episode open';
    END IF;
    RETURN NEW;
  END IF;
  IF (NEW.recording_job_id,NEW.episode_key,NEW.opened_at) IS DISTINCT FROM (OLD.recording_job_id,OLD.episode_key,OLD.opened_at)
     OR NEW.last_observed_at IS DISTINCT FROM transaction_timestamp() THEN
    RAISE EXCEPTION 'surrender transport episode identity is immutable';
  END IF;
  IF NEW.state='open' THEN
    SELECT EXISTS(
      SELECT 1 FROM recording_surrender_transport_observations observation
      WHERE observation.recording_job_id=NEW.recording_job_id AND observation.lease_token=NEW.lease_token
        AND observation.received_at=transaction_timestamp()
        AND ((NEW.reason='transport_budget_exhausted' AND observation.observation_type='transport_budget_exhausted')
          OR (NEW.reason='expired_reclaim' AND observation.observation_type='expired_reclaim'))
    ) INTO matching_observation;
    IF NEW.resolved_at IS NOT NULL OR NOT matching_observation THEN
      RAISE EXCEPTION 'surrender transport episode reopen lacks typed observation';
    END IF;
  ELSIF NEW.state='resolved' THEN
    SELECT EXISTS(
      SELECT 1 FROM recording_capture_artifact_results result
      JOIN recording_capture_artifact_intents artifact ON artifact.upload_intent_id=result.upload_intent_id
      JOIN recording_capture_producers producer ON producer.id=artifact.producer_id
      WHERE producer.recording_job_id=NEW.recording_job_id
        AND result.result='accepted_unique' AND result.result_at=transaction_timestamp()
	  UNION ALL
	  SELECT 1 FROM recording_capture_artifact_grant_results result
	  JOIN recording_capture_reservation_sets capture_set ON capture_set.id=result.set_id
	  JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
	  WHERE plan.recording_job_id=NEW.recording_job_id
	    AND result.result='accepted_unique' AND result.result_at=transaction_timestamp()
    ) INTO matching_ingest;
    IF OLD.state<>'open' OR NEW.lease_token<>OLD.lease_token OR NEW.reason<>OLD.reason
       OR NEW.resolved_at IS DISTINCT FROM transaction_timestamp() OR NOT matching_ingest THEN
      RAISE EXCEPTION 'surrender transport episode resolve lacks accepted-unique evidence';
    END IF;
  ELSE
    RAISE EXCEPTION 'invalid surrender transport episode state';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_surrender_transport_episodes_validate
BEFORE INSERT OR UPDATE OR DELETE ON recording_surrender_transport_episodes FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_episode_write();
CREATE TRIGGER recording_surrender_transport_episodes_no_truncate BEFORE TRUNCATE ON recording_surrender_transport_episodes FOR EACH STATEMENT EXECUTE FUNCTION recording_surrender_freeze_rows();

CREATE FUNCTION recording_surrender_validate_episode_event() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE exact BOOLEAN;
BEGIN
  IF NEW.created_at IS DISTINCT FROM transaction_timestamp() THEN RAISE EXCEPTION 'episode event time is not DB-authored'; END IF;
  IF NEW.event_type='opened' THEN
    SELECT EXISTS(
      SELECT 1 FROM recording_surrender_transport_observations observation
      WHERE observation.id=NEW.event_key AND observation.recording_job_id=NEW.recording_job_id
        AND observation.lease_token=NEW.lease_token
        AND ((NEW.reason='transport_budget_exhausted' AND observation.observation_type='transport_budget_exhausted')
          OR (NEW.reason='expired_reclaim' AND observation.observation_type='expired_reclaim'))
    ) INTO exact;
  ELSE
    SELECT EXISTS(
      SELECT 1 FROM recording_surrender_transport_episodes episode
      WHERE episode.episode_key=NEW.episode_key AND episode.recording_job_id=NEW.recording_job_id
        AND episode.lease_token=NEW.lease_token AND episode.state='resolved'
        AND episode.resolved_at=transaction_timestamp() AND NEW.reason='accepted_unique'
    ) INTO exact;
  END IF;
  IF NOT exact OR NOT EXISTS(SELECT 1 FROM recording_surrender_transport_episodes episode WHERE episode.episode_key=NEW.episode_key AND episode.recording_job_id=NEW.recording_job_id) THEN
    RAISE EXCEPTION 'episode event lacks exact typed authority';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_surrender_transport_episode_events_validate
BEFORE INSERT ON recording_surrender_transport_episode_events FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_episode_event();

CREATE FUNCTION recording_surrender_validate_transport_observation() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE exact BOOLEAN; attempt_sha TEXT;
BEGIN
  SELECT generation.lease_owner=NEW.worker_id AND generation.node_id=NEW.node_id
         AND NEW.observed_at>=generation.claimed_at-interval '5 minutes'
         AND NEW.observed_at<=transaction_timestamp()+interval '1 minute'
    INTO exact
  FROM recording_job_lease_generations generation
  WHERE generation.recording_job_id=NEW.recording_job_id AND generation.lease_token=NEW.lease_token;
  IF NOT FOUND OR NOT exact OR NEW.received_at IS DISTINCT FROM transaction_timestamp() THEN
    RAISE EXCEPTION 'transport observation has no exact lease generation';
  END IF;
  IF ((NEW.observation_type IN('request_started','request_result_received','expired_reclaim')) <> (NEW.error_class='')) THEN
    RAISE EXCEPTION 'transport observation error class is not canonical';
  END IF;
  IF NEW.observation_type='expired_reclaim' THEN
    IF NEW.observed_at IS DISTINCT FROM transaction_timestamp() OR NEW.attempt_id IS NOT NULL
       OR NOT EXISTS(SELECT 1 FROM recording_job_lease_expiry_events event
                     WHERE event.recording_job_id=NEW.recording_job_id AND event.lease_token=NEW.lease_token) THEN
      RAISE EXCEPTION 'expired reclaim observation lacks DB expiry authority';
    END IF;
  ELSE
    IF NEW.attempt_id IS NULL THEN RAISE EXCEPTION 'worker transport observation lacks attempt identity'; END IF;
    SELECT request_sha256 INTO attempt_sha FROM recording_job_surrender_attempts WHERE id=NEW.attempt_id;
    IF FOUND AND attempt_sha<>NEW.request_sha256 THEN
      RAISE EXCEPTION 'transport observation request digest differs from surrender attempt';
    END IF;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_surrender_transport_observations_validate
BEFORE INSERT ON recording_surrender_transport_observations FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_transport_observation();

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
	SELECT
	  (SELECT count(*) FROM recording_job_recovery_grants grant_row
	   WHERE grant_row.recording_job_id=NEW.recording_job_id AND grant_row.lease_token=NEW.lease_token)
	  +(SELECT count(*) FROM recording_capture_set_grants set_grant
	    JOIN recording_capture_reservation_sets capture_set ON capture_set.id=set_grant.set_id
	    JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
	    WHERE plan.recording_job_id=NEW.recording_job_id AND plan.lease_token=NEW.lease_token)
	INTO exact_grants;
	IF capture_via='cloud' THEN
	  SELECT EXISTS(
	    SELECT 1 FROM recorder_droplets droplet
		    WHERE droplet.name<>NEW.lease_owner AND droplet.state IN('provisioning','active')
		      AND droplet.last_seen_at>=transaction_timestamp()-interval '2 minutes'
		      AND EXISTS(SELECT 1 FROM recording_worker_claim_heads claim
		                 JOIN node_tokens claim_token ON claim_token.id=claim.claim_token_id
		                 WHERE claim.node_id=droplet.node_id AND claim.state='enabled'
		                   AND claim_token.revoked_at IS NULL
		                   AND claim_token.recording_claim_generation=claim.generation
		                   AND claim_token.recording_claim_purpose='claim_current')
	      AND (SELECT count(*) FROM recording_jobs live
	           WHERE live.status='leased' AND live.lease_owner=droplet.name
	             AND live.lease_expires_at>transaction_timestamp())<droplet.capacity
	  ) INTO exact_alternate;
	ELSE
	  SELECT recording_surrender_relay_alternate(NEW.recording_job_id,NEW.lease_owner) INTO exact_alternate;
	END IF;
	  IF capture_via NOT IN('cloud','relay') OR NOT job_expired OR exact_owner<>NEW.lease_owner
     OR exact_grants<>NEW.recovery_grant_count
	 OR exact_alternate IS DISTINCT FROM NEW.alternate_available
     OR NEW.reclaimed_at IS DISTINCT FROM transaction_timestamp() THEN
    RAISE EXCEPTION 'invalid recording lease expiry event';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_job_lease_expiry_events_validate
BEFORE INSERT ON recording_job_lease_expiry_events FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_expiry_event_insert();

-- Grant issuance is immutable. Exactly one append-only terminal result closes
-- a grant and must be backed by the exact artifact outcome.
CREATE FUNCTION recording_surrender_validate_grant_result() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE artifact_result TEXT; grant_row RECORD;
BEGIN
  NEW.result_at:=transaction_timestamp();
  SELECT * INTO grant_row FROM recording_job_recovery_grants WHERE id=NEW.grant_id FOR SHARE;
  IF NOT FOUND THEN RAISE EXCEPTION 'recording recovery grant is missing'; END IF;
  SELECT result INTO artifact_result FROM recording_capture_artifact_results WHERE upload_intent_id=grant_row.upload_intent_id;
  IF artifact_result IS NULL
	 OR (NEW.result='recovery_completed' AND artifact_result NOT IN('accepted_unique','exact_replay','abandoned_unsealed','unrecoverable_partial'))
	 OR (NEW.result='security_incident' AND artifact_result<>'security_revoked')
	 OR (NEW.result='recovery_grace_expired' AND (artifact_result<>'host_unreachable_unrecoverable' OR grant_row.upload_grace_until>transaction_timestamp())) THEN
    RAISE EXCEPTION 'recording recovery grant terminal reason lacks exact intent evidence';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_job_recovery_grant_results_validate
BEFORE INSERT ON recording_job_recovery_grant_results FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_grant_result();

CREATE FUNCTION recording_surrender_validate_grant_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE exact BOOLEAN;
BEGIN
  SELECT producer.recording_job_id=NEW.recording_job_id
         AND producer.lease_token=NEW.lease_token
         AND artifact.producer_id=producer.id
         AND artifact.recovery_secret_sha256=NEW.recovery_secret_sha256
	     AND job.status='leased' AND job.lease_token=producer.lease_token
	     AND job.lease_owner=producer.worker_id AND job.lease_expires_at<=transaction_timestamp()
         AND NOT EXISTS(SELECT 1 FROM recording_capture_producer_results result WHERE result.producer_id=producer.id)
    INTO exact
  FROM recording_capture_producers producer
  JOIN recording_capture_artifact_intents artifact ON artifact.upload_intent_id=NEW.upload_intent_id
  JOIN recording_jobs job ON job.id=producer.recording_job_id
  WHERE producer.id=NEW.producer_id FOR UPDATE OF producer,job;
  IF NOT FOUND OR NOT exact OR NEW.granted_at IS DISTINCT FROM transaction_timestamp()
	 OR NEW.upload_grace_until IS DISTINCT FROM transaction_timestamp()+interval '30 minutes' THEN
    RAISE EXCEPTION 'invalid recording recovery grant insert';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_job_recovery_grants_validate_insert
BEFORE INSERT ON recording_job_recovery_grants FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_grant_insert();

CREATE FUNCTION recording_surrender_validate_producer_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expected_ordinal BIGINT; generation_owner TEXT; generation_node BIGINT; generation_current BOOLEAN; expected_snapshot TEXT; expected_revision BIGINT; expected_config TEXT; latest_reserved_at TIMESTAMPTZ; prior RECORD; locked_recording BIGINT; locked_stream BIGINT;
BEGIN
	  SELECT job.recording_id,recording.stream_id INTO locked_recording,locked_stream
	  FROM recording_jobs job JOIN recordings recording ON recording.id=job.recording_id
	  WHERE job.id=NEW.recording_job_id AND recording.stream_id IS NOT NULL;
	  IF NOT FOUND THEN RAISE EXCEPTION 'capture producer source binding is unavailable'; END IF;
	  PERFORM 1 FROM streams WHERE id=locked_stream FOR SHARE;
	  PERFORM 1 FROM recordings WHERE id=locked_recording AND stream_id=locked_stream FOR SHARE;
	  IF NOT FOUND THEN RAISE EXCEPTION 'capture producer source binding changed while locking'; END IF;
	  SELECT generation.lease_owner,generation.node_id,
	         job.status='leased' AND job.lease_token=NEW.lease_token
	           AND job.lease_owner=generation.lease_owner AND job.lease_expires_at>transaction_timestamp()
	           AND job.kind='continuous_window' AND job.window_end_at>transaction_timestamp(),
	         encode(sha256(convert_to(recording_surrender_source_snapshot(recording.id)::text,'UTF8')),'hex'),
	         (SELECT max(sr.id) FROM stream_source_revisions sr WHERE sr.stream_id=stream.id),
	         encode(sha256(convert_to(recording_surrender_capture_config_snapshot(recording.id,job.id,NEW.lease_token)::text,'UTF8')),'hex')
	    INTO generation_owner,generation_node,generation_current,expected_snapshot,expected_revision,expected_config
	  FROM recording_job_lease_generations generation
	  JOIN recording_jobs job ON job.id=generation.recording_job_id
	  JOIN recordings recording ON recording.id=job.recording_id
	  JOIN streams stream ON stream.id=recording.stream_id
  WHERE generation.recording_job_id=NEW.recording_job_id AND generation.lease_token=NEW.lease_token
	  FOR UPDATE OF generation,job;
	  IF NOT FOUND OR NOT generation_current OR generation_owner<>NEW.worker_id OR generation_node IS DISTINCT FROM NEW.node_id
	     OR NEW.source_snapshot_sha256 IS DISTINCT FROM expected_snapshot
	     OR NEW.source_revision_head_id IS DISTINCT FROM expected_revision
	     OR NEW.capture_config_sha256 IS DISTINCT FROM expected_config THEN
    RAISE EXCEPTION 'capture producer has no current matching lease generation';
  END IF;
  SELECT * INTO prior FROM recording_capture_producers p
  WHERE p.recording_job_id=NEW.recording_job_id AND p.lease_token=NEW.lease_token
    AND p.capture_ordinal=NEW.capture_ordinal;
  IF FOUND THEN
	    IF (prior.id,prior.worker_id,prior.node_id,prior.sealed_intent_limit,prior.source_snapshot_sha256,prior.source_revision_head_id,prior.capture_config_sha256)
	         IS DISTINCT FROM
	       (NEW.id,NEW.worker_id,NEW.node_id,NEW.sealed_intent_limit,NEW.source_snapshot_sha256,NEW.source_revision_head_id,NEW.capture_config_sha256) THEN
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

CREATE FUNCTION recording_surrender_validate_artifact_intent() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE exact BOOLEAN; prior RECORD; reserved_count INTEGER;
BEGIN
  SELECT upload.recording_job_id=producer.recording_job_id
         AND upload.recording_id=job.recording_id
         AND upload.max_size_bytes=NEW.max_size_bytes
         AND job.status='leased' AND job.lease_token=producer.lease_token
         AND job.lease_owner=producer.worker_id AND job.lease_expires_at>transaction_timestamp()
    INTO exact
  FROM recording_capture_producers producer
  JOIN recording_jobs job ON job.id=producer.recording_job_id
  JOIN recording_upload_intents upload ON upload.id=NEW.upload_intent_id
  WHERE producer.id=NEW.producer_id FOR UPDATE OF producer,job,upload;
  IF NOT FOUND OR NOT exact OR EXISTS(SELECT 1 FROM recording_capture_producer_results WHERE producer_id=NEW.producer_id)
     OR NEW.reserved_at IS DISTINCT FROM transaction_timestamp() THEN
    RAISE EXCEPTION 'capture artifact intent has no current exact producer authority';
  END IF;
  SELECT * INTO prior FROM recording_capture_artifact_intents
    WHERE upload_intent_id=NEW.upload_intent_id OR (producer_id=NEW.producer_id AND capture_sequence=NEW.capture_sequence)
    LIMIT 1;
  IF FOUND THEN
    IF (prior.upload_intent_id,prior.producer_id,prior.capture_sequence,prior.recovery_secret_sha256,prior.max_size_bytes)
       IS DISTINCT FROM
       (NEW.upload_intent_id,NEW.producer_id,NEW.capture_sequence,NEW.recovery_secret_sha256,NEW.max_size_bytes) THEN
      RAISE EXCEPTION 'capture artifact intent replay differs';
    END IF;
    RETURN NEW;
  END IF;
  SELECT count(*) INTO reserved_count FROM recording_capture_artifact_intents WHERE producer_id=NEW.producer_id;
  IF reserved_count>=2048 THEN RAISE EXCEPTION 'capture artifact reservation bound exceeded'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_capture_artifact_intents_validate BEFORE INSERT ON recording_capture_artifact_intents FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_artifact_intent();

CREATE FUNCTION recording_surrender_validate_artifact_seal() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE lim INTEGER; outstanding INTEGER; exact_intent BOOLEAN; prior RECORD;
BEGIN
	  SELECT producer.sealed_intent_limit,
	         intent.recording_job_id=producer.recording_job_id
	           AND intent.recording_id=job.recording_id
	           AND intent.max_size_bytes>=NEW.size_bytes
	           AND artifact.producer_id=producer.id AND artifact.capture_sequence=NEW.capture_sequence
	           AND ((job.status='leased' AND job.lease_token=producer.lease_token
	                 AND job.lease_owner=producer.worker_id AND job.lease_expires_at>transaction_timestamp())
	                OR EXISTS(SELECT 1 FROM recording_job_recovery_grants grant_row
	                          WHERE grant_row.upload_intent_id=NEW.upload_intent_id AND grant_row.producer_id=producer.id
	                            AND grant_row.upload_grace_until>transaction_timestamp()
	                            AND NOT EXISTS(SELECT 1 FROM recording_job_recovery_grant_results grant_result WHERE grant_result.grant_id=grant_row.id)))
	    INTO lim,exact_intent
	  FROM recording_capture_producers producer
	  JOIN recording_jobs job ON job.id=producer.recording_job_id
	  JOIN recording_upload_intents intent ON intent.id=NEW.upload_intent_id
	  JOIN recording_capture_artifact_intents artifact ON artifact.upload_intent_id=NEW.upload_intent_id
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
DECLARE intents INTEGER; unresolved INTEGER; accepted INTEGER; current_lease BOOLEAN; recovery_exact BOOLEAN; recovery_security INTEGER; recovery_expired INTEGER;
BEGIN
  IF NEW.result_at IS DISTINCT FROM transaction_timestamp() THEN
    RAISE EXCEPTION 'capture producer result time is not DB-authored';
  END IF;
  SELECT count(*),count(*) FILTER(WHERE result.upload_intent_id IS NULL),
         count(*) FILTER(WHERE result.result IN('accepted_unique','exact_replay'))
    INTO intents,unresolved,accepted
  FROM recording_capture_artifact_intents artifact
  LEFT JOIN recording_capture_artifact_results result ON result.upload_intent_id=artifact.upload_intent_id
  WHERE artifact.producer_id=NEW.producer_id;
  IF (intents=0 AND NOT (NEW.result='abandoned_empty' AND NEW.detail_class='no_artifact_reservation'))
     OR unresolved<>0 OR (NEW.result='abandoned_empty' AND accepted<>0)
     OR (NEW.result='completed' AND accepted=0) THEN
    RAISE EXCEPTION 'capture producer terminal result does not seal every reserved artifact intent';
  END IF;
	SELECT job.status='leased' AND job.lease_token=p.lease_token
	       AND job.lease_owner=p.worker_id AND job.lease_expires_at>transaction_timestamp()
	  INTO current_lease
	FROM recording_capture_producers p
	JOIN recording_jobs job ON job.id=p.recording_job_id
	WHERE p.id=NEW.producer_id;
	IF NOT FOUND THEN RAISE EXCEPTION 'capture producer result lacks producer authority'; END IF;
	SELECT bool_and(grant_result.result=CASE artifact_result.result
	         WHEN 'security_revoked' THEN 'security_incident'
	         WHEN 'host_unreachable_unrecoverable' THEN 'recovery_grace_expired'
	         ELSE 'recovery_completed' END),
	       count(*) FILTER(WHERE artifact_result.result='security_revoked'),
	       count(*) FILTER(WHERE artifact_result.result='host_unreachable_unrecoverable')
	  INTO recovery_exact,recovery_security,recovery_expired
	FROM recording_job_recovery_grants grant_row
	JOIN recording_capture_artifact_results artifact_result ON artifact_result.upload_intent_id=grant_row.upload_intent_id
	JOIN recording_job_recovery_grant_results grant_result ON grant_result.grant_id=grant_row.id
	WHERE grant_row.producer_id=NEW.producer_id;
	IF NEW.result IN('completed','abandoned_empty') THEN
	  IF NOT COALESCE(current_lease,false)
	     AND NOT (COALESCE(recovery_exact,false) AND recovery_security=0 AND recovery_expired=0)
	     AND NOT (NEW.result='abandoned_empty' AND NEW.detail_class='no_artifact_reservation' AND EXISTS(
	       SELECT 1 FROM recording_capture_producers producer
	       JOIN recording_job_lease_expiry_events event ON event.recording_job_id=producer.recording_job_id AND event.lease_token=producer.lease_token
	       WHERE producer.id=NEW.producer_id
	     )) THEN
	    RAISE EXCEPTION 'capture producer terminal result lacks current lease or completed recovery authority';
	  END IF;
	ELSIF NEW.result='security_revoked' THEN
	  IF NOT COALESCE(recovery_exact,false) OR recovery_security=0 OR NEW.detail_class<>'recovery_capability_revoked' THEN
	    RAISE EXCEPTION 'capture producer security result lacks exact recovery revocation';
	  END IF;
	ELSIF NEW.result='host_unreachable_unrecoverable' THEN
	  IF NOT COALESCE(recovery_exact,false) OR recovery_security<>0 OR recovery_expired=0 OR NEW.detail_class<>'recovery_grace_expired' THEN
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
	  ELSIF NEW.result='abandoned_unsealed' THEN
	    SELECT count(*) INTO matches
	    FROM recording_capture_artifact_intents artifact
	    JOIN recording_capture_producers producer ON producer.id=artifact.producer_id
	    JOIN recording_jobs job ON job.id=producer.recording_job_id
	    WHERE artifact.upload_intent_id=NEW.upload_intent_id
	      AND NOT EXISTS(SELECT 1 FROM recording_capture_artifact_seals seal WHERE seal.upload_intent_id=artifact.upload_intent_id)
	      AND ((job.status='leased' AND job.lease_token=producer.lease_token AND job.lease_owner=producer.worker_id)
	           OR EXISTS(SELECT 1 FROM recording_job_recovery_grants grant_row
	                     WHERE grant_row.upload_intent_id=artifact.upload_intent_id
	                       AND grant_row.upload_grace_until>NEW.result_at
	                       AND NOT EXISTS(SELECT 1 FROM recording_job_recovery_grant_results grant_result WHERE grant_result.grant_id=grant_row.id)));
	    IF matches<>1 THEN RAISE EXCEPTION 'abandoned artifact was not an exact reserved-unsealed intent'; END IF;
	  ELSIF NEW.result='unrecoverable_partial' THEN
	    SELECT count(*) INTO matches
	    FROM recording_capture_artifact_intents artifact
	    JOIN recording_job_recovery_grants grant_row ON grant_row.upload_intent_id=artifact.upload_intent_id
	    WHERE artifact.upload_intent_id=NEW.upload_intent_id
	      AND NOT EXISTS(SELECT 1 FROM recording_capture_artifact_seals seal WHERE seal.upload_intent_id=artifact.upload_intent_id)
	      AND grant_row.upload_grace_until>NEW.result_at
	      AND NOT EXISTS(SELECT 1 FROM recording_job_recovery_grant_results grant_result WHERE grant_result.grant_id=grant_row.id);
	    IF matches<>1 THEN RAISE EXCEPTION 'partial artifact lacks exact live recovery authority'; END IF;
	  ELSIF NEW.result='security_revoked' THEN
	    SELECT count(*) INTO matches
	    FROM recording_job_recovery_grants grant_row
	    JOIN recording_job_recovery_grant_results grant_result ON grant_result.grant_id=grant_row.id
	    WHERE grant_row.upload_intent_id=NEW.upload_intent_id
	      AND grant_result.result='security_incident' AND grant_result.result_at=NEW.result_at;
	    IF matches<>1 THEN RAISE EXCEPTION 'artifact security result lacks exact recovery revocation'; END IF;
	  ELSIF NEW.result='host_unreachable_unrecoverable' THEN
	    SELECT count(*) INTO matches
	    FROM recording_job_recovery_grants grant_row
	    JOIN recording_job_recovery_grant_results grant_result ON grant_result.grant_id=grant_row.id
	    WHERE grant_row.upload_intent_id=NEW.upload_intent_id
	      AND grant_result.result='recovery_grace_expired' AND grant_result.result_at=NEW.result_at
	      AND grant_row.upload_grace_until<=NEW.result_at;
	    IF matches<>1 THEN RAISE EXCEPTION 'artifact expiry result lacks exact recovery revocation'; END IF;
	  END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER recording_capture_artifact_results_head
AFTER INSERT ON recording_capture_artifact_results DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_artifact_result();

CREATE FUNCTION recording_surrender_validate_v1_clip() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE matches INTEGER;
BEGIN
  IF NEW.surrender_transport_version=0 THEN RETURN NULL; END IF;
  IF NEW.surrender_transport_version<>1 OR NEW.recording_job_id IS NULL OR NEW.capture_lease_token IS NULL OR NEW.capture_sequence IS NULL THEN
    RAISE EXCEPTION 'invalid typed surrender clip identity';
  END IF;
  SELECT count(*) INTO matches
  FROM recording_capture_artifact_results result
  JOIN recording_capture_artifact_seals seal ON seal.upload_intent_id=result.upload_intent_id
  JOIN recording_capture_producers producer ON producer.id=seal.producer_id
  JOIN recording_upload_intents intent ON intent.id=seal.upload_intent_id
  WHERE result.clip_id=NEW.id AND result.result IN('accepted_unique','exact_replay')
    AND producer.recording_job_id=NEW.recording_job_id AND producer.lease_token=NEW.capture_lease_token
    AND seal.capture_sequence=NEW.capture_sequence AND seal.size_bytes=NEW.size_bytes AND seal.sha256=NEW.sha256
	AND floor(extract(epoch FROM NEW.clip_start_at)*1000)::bigint=seal.segment_start_ms
    AND intent.storage_destination_id=NEW.storage_destination_id AND intent.endpoint=NEW.endpoint
    AND intent.bucket=NEW.bucket AND intent.object_key=NEW.object_key;
	IF matches=0 THEN
	  SELECT count(*) INTO matches
	  FROM recording_capture_artifact_grant_results result
	  JOIN recording_capture_materialized_artifact_seals seal
	    ON seal.set_id=result.set_id AND seal.ordinal=result.ordinal
	  JOIN recording_capture_reservation_sets capture_set ON capture_set.id=seal.set_id
	  JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
	  JOIN recording_upload_intents intent ON intent.id=seal.artifact_id
	  WHERE result.clip_id=NEW.id AND result.result IN('accepted_unique','exact_replay')
	    AND plan.recording_job_id=NEW.recording_job_id AND plan.lease_token=NEW.capture_lease_token
	    AND seal.capture_sequence=NEW.capture_sequence AND seal.size_bytes=NEW.size_bytes AND seal.sha256=NEW.sha256
	    AND floor(extract(epoch FROM NEW.clip_start_at)*1000000)::bigint=seal.segment_start_microseconds
	    AND intent.storage_destination_id=NEW.storage_destination_id AND intent.endpoint=NEW.endpoint
	    AND intent.bucket=NEW.bucket AND intent.object_key=NEW.object_key;
	END IF;
  IF matches<>1 THEN RAISE EXCEPTION 'typed surrender clip lacks exact artifact provenance'; END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER recording_clips_surrender_v1_provenance
AFTER INSERT ON recording_clips DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION recording_surrender_validate_v1_clip();

CREATE FUNCTION recording_surrender_freeze_v1_clip() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.surrender_transport_version<>OLD.surrender_transport_version THEN
    RAISE EXCEPTION 'recording clip surrender transport version is immutable';
  END IF;
  IF OLD.surrender_transport_version=0 THEN RETURN NEW; END IF;
  IF (to_jsonb(NEW)-'purged_at'-'released_at') IS DISTINCT FROM
     (to_jsonb(OLD)-'purged_at'-'released_at') THEN
    RAISE EXCEPTION 'typed surrender clip provenance is immutable';
  END IF;
  IF OLD.purged_at IS NOT NULL AND NEW.purged_at IS DISTINCT FROM OLD.purged_at
     OR OLD.released_at IS NOT NULL AND NEW.released_at IS DISTINCT FROM OLD.released_at THEN
    RAISE EXCEPTION 'typed surrender clip lifecycle is monotonic';
  END IF;
  IF OLD.purged_at IS NULL AND NEW.purged_at IS NOT NULL THEN NEW.purged_at:=transaction_timestamp(); END IF;
  IF OLD.released_at IS NULL AND NEW.released_at IS NOT NULL THEN NEW.released_at:=transaction_timestamp(); END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_clips_surrender_v1_freeze
BEFORE UPDATE ON recording_clips FOR EACH ROW EXECUTE FUNCTION recording_surrender_freeze_v1_clip();
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
	 OR exact_kind<>'continuous_window' OR exact_capture NOT IN('cloud','relay')
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
DECLARE target UUID; attempts INTEGER; results INTEGER; attempt_row RECORD; result_row RECORD; head_row RECORD; job_row RECORD; expected_retry TIMESTAMPTZ; current_generation BOOLEAN; head_matches BOOLEAN; spool_unsafe BOOLEAN;
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
	    UNION ALL
	    SELECT 1 FROM recording_capture_reservation_sets capture_set
	    JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
	    WHERE plan.recording_job_id=attempt_row.recording_job_id AND plan.lease_token=attempt_row.lease_token
	      AND NOT EXISTS(SELECT 1 FROM recording_capture_set_results set_result WHERE set_result.set_id=capture_set.id)
	  ) INTO spool_unsafe;
	  IF result_row.result='committed' THEN
	    expected_retry:=result_row.result_at;
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
DECLARE nonterminal INTEGER; granted INTEGER; set_nonterminal INTEGER; set_granted INTEGER;
BEGIN
  IF OLD.status='leased' AND OLD.lease_token IS NOT NULL
     AND (NEW.status IS DISTINCT FROM 'leased' OR NEW.lease_token IS DISTINCT FROM OLD.lease_token) THEN
    SELECT count(*) INTO nonterminal
    FROM recording_capture_artifact_intents artifact
    JOIN recording_capture_producers producer ON producer.id=artifact.producer_id
    WHERE producer.recording_job_id=OLD.id AND producer.lease_token=OLD.lease_token
      AND NOT EXISTS(SELECT 1 FROM recording_capture_producer_results result WHERE result.producer_id=producer.id);
		SELECT count(*) INTO set_nonterminal
		FROM recording_capture_reservation_sets capture_set
		JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
		WHERE plan.recording_job_id=OLD.id AND plan.lease_token=OLD.lease_token
		  AND NOT EXISTS(SELECT 1 FROM recording_capture_set_results result WHERE result.set_id=capture_set.id);
		IF nonterminal+set_nonterminal>0 THEN
      SELECT count(*) INTO granted
      FROM recording_capture_artifact_intents artifact
      JOIN recording_capture_producers producer ON producer.id=artifact.producer_id
      JOIN recording_job_recovery_grants grant_row ON grant_row.upload_intent_id=artifact.upload_intent_id
      WHERE producer.recording_job_id=OLD.id AND producer.lease_token=OLD.lease_token
        AND NOT EXISTS(SELECT 1 FROM recording_capture_producer_results result WHERE result.producer_id=producer.id)
	        AND grant_row.upload_grace_until>transaction_timestamp()
	        AND NOT EXISTS(SELECT 1 FROM recording_job_recovery_grant_results grant_result WHERE grant_result.grant_id=grant_row.id);
		  SELECT count(*) INTO set_granted
		  FROM recording_capture_reservation_sets capture_set
		  JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
		  JOIN recording_capture_set_grants grant ON grant.set_id=capture_set.id
		  WHERE plan.recording_job_id=OLD.id AND plan.lease_token=OLD.lease_token
		    AND NOT EXISTS(SELECT 1 FROM recording_capture_set_results result WHERE result.set_id=capture_set.id)
		    AND grant.upload_grace_until>transaction_timestamp();
		  IF granted<>nonterminal OR set_granted<>set_nonterminal OR NOT EXISTS(
        SELECT 1 FROM recording_job_lease_expiry_events event
        WHERE event.recording_job_id=OLD.id AND event.lease_token=OLD.lease_token
				  AND event.recovery_grant_count=nonterminal+set_nonterminal
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

-- Source lineage inserts take the same stream row fence as source/config
-- mutation. Producer reservation takes FOR SHARE on that row, so either the old
-- complete snapshot or the new complete snapshot wins; a mixed head is impossible.
CREATE FUNCTION recording_surrender_fence_source_revision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  PERFORM 1 FROM streams WHERE id=NEW.stream_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'source revision stream is missing'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_surrender_source_revision_fence
BEFORE INSERT ON stream_source_revisions FOR EACH ROW EXECUTE FUNCTION recording_surrender_fence_source_revision();

CREATE FUNCTION recording_surrender_append_source_revision_stop_events() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO recording_capture_producer_stop_events(id,set_id,old_snapshot_generation,new_snapshot_generation)
  SELECT gen_random_uuid(),capture_set.id,plan.snapshot_generation,
         plan.snapshot_generation+1+COALESCE((SELECT count(*) FROM recording_capture_producer_stop_events prior WHERE prior.set_id=capture_set.id),0)
  FROM recordings recording
  JOIN recording_capture_set_plans plan ON plan.recording_id=recording.id
  JOIN recording_capture_reservation_sets capture_set ON capture_set.plan_id=plan.id
  WHERE recording.stream_id=NEW.stream_id
    AND plan.source_snapshot IS DISTINCT FROM recording_surrender_source_snapshot(recording.id)
    AND NOT EXISTS(SELECT 1 FROM recording_capture_set_results result WHERE result.set_id=capture_set.id)
    AND NOT EXISTS(SELECT 1 FROM recording_capture_producer_stop_events prior
                   WHERE prior.set_id=capture_set.id);
  RETURN NEW;
END $$;
CREATE TRIGGER recording_surrender_source_revision_stop_events
AFTER INSERT ON stream_source_revisions FOR EACH ROW EXECUTE FUNCTION recording_surrender_append_source_revision_stop_events();

-- R10 does not depend on the unmerged roster lineage migration. Once a source
-- revision's full tuple is frozen into a capture plan it cannot be rewritten or
-- removed; unrelated legacy cleanup remains outside this narrow boundary.
CREATE FUNCTION recording_surrender_preserve_referenced_source_revision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='TRUNCATE' THEN
    IF EXISTS(SELECT 1 FROM recording_capture_set_plans plan WHERE plan.source_snapshot->'revision'<>'null'::jsonb) THEN
      RAISE EXCEPTION 'referenced capture source lineage cannot be truncated';
    END IF;
    RETURN NULL;
  END IF;
  IF EXISTS(
    SELECT 1 FROM recording_capture_set_plans plan
    WHERE (plan.source_snapshot->'revision'->>'id')::bigint=OLD.id
  ) THEN RAISE EXCEPTION 'referenced capture source revision is immutable'; END IF;
  RETURN OLD;
END $$;
CREATE TRIGGER recording_surrender_source_revision_preserve
BEFORE UPDATE OR DELETE ON stream_source_revisions FOR EACH ROW EXECUTE FUNCTION recording_surrender_preserve_referenced_source_revision();
CREATE TRIGGER recording_surrender_source_revision_no_truncate
BEFORE TRUNCATE ON stream_source_revisions FOR EACH STATEMENT EXECUTE FUNCTION recording_surrender_preserve_referenced_source_revision();

CREATE TABLE recording_capture_recovery_alert_events (
  id UUID PRIMARY KEY,
  set_id UUID NOT NULL REFERENCES recording_capture_reservation_sets(id) ON DELETE RESTRICT,
  event_type TEXT NOT NULL CHECK(event_type IN('reachable_stuck','host_unreachable','security_revoked','resolved')),
  dedupe_key TEXT NOT NULL CHECK(octet_length(dedupe_key) BETWEEN 1 AND 255),
  event_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  UNIQUE(dedupe_key,event_type)
);
CREATE TRIGGER recording_capture_recovery_alert_events_db_time
BEFORE INSERT ON recording_capture_recovery_alert_events FOR EACH ROW EXECUTE FUNCTION recording_surrender_author_db_times();
CREATE TRIGGER recording_capture_recovery_alert_events_freeze
BEFORE UPDATE OR DELETE ON recording_capture_recovery_alert_events FOR EACH ROW EXECUTE FUNCTION recording_surrender_freeze_rows();
CREATE TRIGGER recording_capture_recovery_alert_events_no_truncate
BEFORE TRUNCATE ON recording_capture_recovery_alert_events FOR EACH STATEMENT EXECUTE FUNCTION recording_surrender_freeze_rows();

CREATE FUNCTION recording_surrender_expire_set_grants() RETURNS BIGINT LANGUAGE plpgsql AS $$
DECLARE target RECORD; materialized INTEGER[]; coverage JSONB; changed BIGINT:=0; fresh_at TIMESTAMPTZ;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('recording-worker-claim-v1',0));
  PERFORM recording_surrender_reconcile_expired_upload_sessions();
  FOR target IN
    SELECT grant.id,grant.set_id,grant.upload_grace_until,plan.recording_job_id,plan.lease_token,
           generation.node_id,capture_set.artifact_count
    FROM recording_capture_set_grants grant
    JOIN recording_capture_reservation_sets capture_set ON capture_set.id=grant.set_id
    JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
    JOIN recording_job_lease_generations generation
      ON generation.recording_job_id=plan.recording_job_id AND generation.lease_token=plan.lease_token
    WHERE grant.upload_grace_until<=transaction_timestamp()
      AND NOT EXISTS(SELECT 1 FROM recording_capture_set_results result WHERE result.set_id=grant.set_id)
    ORDER BY generation.node_id,grant.set_id
  LOOP
    PERFORM 1 FROM nodes WHERE id=target.node_id FOR UPDATE;
    PERFORM 1 FROM recording_capture_set_grants grant WHERE grant.id=target.id FOR UPDATE;
    IF EXISTS(SELECT 1 FROM recording_capture_set_results result WHERE result.set_id=target.set_id) THEN CONTINUE; END IF;
    SELECT greatest(
      COALESCE((SELECT node.last_heartbeat_at FROM nodes node WHERE node.id=target.node_id),'-infinity'::timestamptz),
      COALESCE((SELECT max(droplet.last_seen_at) FROM recorder_droplets droplet WHERE droplet.node_id=target.node_id),'-infinity'::timestamptz),
      COALESCE((SELECT max(report.received_at) FROM recording_capture_recovery_reports report WHERE report.set_id=target.set_id),'-infinity'::timestamptz),
      COALESCE((SELECT max(session.started_at) FROM recording_recovery_upload_sessions session WHERE session.set_id=target.set_id),'-infinity'::timestamptz),
      COALESCE((SELECT max(result.result_at) FROM recording_recovery_upload_session_results result
                JOIN recording_recovery_upload_sessions session ON session.id=result.session_id
                WHERE session.set_id=target.set_id),'-infinity'::timestamptz)
    ) INTO fresh_at;
    IF fresh_at>=transaction_timestamp()-interval '2 minutes' THEN
      INSERT INTO recording_capture_recovery_alert_events(id,set_id,event_type,dedupe_key)
      VALUES(gen_random_uuid(),target.set_id,'reachable_stuck','capture-set:'||target.set_id::text)
      ON CONFLICT(dedupe_key,event_type) DO NOTHING;
      CONTINUE;
    END IF;
    INSERT INTO recording_capture_artifact_grant_results(set_id,ordinal,result)
    SELECT artifact.set_id,artifact.ordinal,'host_unreachable'
    FROM recording_capture_materialized_artifacts artifact
    WHERE artifact.set_id=target.set_id ON CONFLICT(set_id,ordinal) DO NOTHING;
    SELECT COALESCE(array_agg(artifact.ordinal ORDER BY artifact.ordinal),'{}'::integer[]) INTO materialized
    FROM recording_capture_materialized_artifacts artifact WHERE artifact.set_id=target.set_id;
    coverage:=jsonb_build_object('artifact_count',target.artifact_count,
      'materialized_ordinals',to_jsonb(materialized),
      'unused_ranges',recording_surrender_unused_capture_ranges(target.artifact_count,materialized));
    INSERT INTO recording_capture_set_results(set_id,result,coverage_ranges,coverage_sha256)
    VALUES(target.set_id,'host_unreachable',coverage,encode(sha256(convert_to(coverage::text,'UTF8')),'hex'));
    INSERT INTO recording_capture_recovery_alert_events(id,set_id,event_type,dedupe_key)
    VALUES(gen_random_uuid(),target.set_id,'host_unreachable','capture-set:'||target.set_id::text)
    ON CONFLICT(dedupe_key,event_type) DO NOTHING;
    changed:=changed+1;
  END LOOP;
  RETURN changed;
END $$;

CREATE FUNCTION recording_surrender_relay_candidate_eligible(p_job_id BIGINT,p_node_id BIGINT) RETURNS BOOLEAN
LANGUAGE sql STABLE AS $$
  SELECT EXISTS(
    SELECT 1
    FROM recording_jobs job
    JOIN recordings recording ON recording.id=job.recording_id
    LEFT JOIN streams stream ON stream.id=recording.stream_id
    JOIN nodes candidate ON candidate.id=p_node_id AND candidate.account_id=recording.account_id
      AND candidate.node_type='relay' AND candidate.status='active'
      AND candidate.last_heartbeat_at>=transaction_timestamp()-interval '120 seconds'
    LEFT JOIN relay_groups candidate_group ON candidate_group.id=candidate.relay_group_id
      AND candidate_group.account_id=candidate.account_id
    WHERE job.id=p_job_id
      AND EXISTS(SELECT 1 FROM recording_worker_claim_heads claim
                 JOIN node_tokens token ON token.id=claim.claim_token_id
                 WHERE claim.node_id=candidate.id AND claim.state='enabled'
                   AND token.revoked_at IS NULL AND token.recording_claim_generation=claim.generation
                   AND token.recording_claim_purpose='claim_current')
      AND (stream.id IS NULL OR stream.execution_class<>'youtube_direct'
           OR (jsonb_typeof(candidate.capabilities_jsonb->'youtube_ready')='boolean'
               AND (candidate.capabilities_jsonb->>'youtube_ready')::boolean))
      AND (SELECT count(*) FROM recording_jobs live
           WHERE live.status='leased' AND live.lease_owner='node:'||candidate.id::text
             AND live.lease_expires_at>transaction_timestamp())<candidate.relay_max_streams
      AND (candidate_group.id IS NULL OR
           (SELECT count(*) FROM recording_jobs group_live
            JOIN nodes group_node ON group_live.lease_owner='node:'||group_node.id::text
            WHERE group_live.status='leased' AND group_live.lease_expires_at>transaction_timestamp()
              AND group_node.account_id=candidate.account_id AND group_node.relay_group_id=candidate_group.id)<candidate_group.max_streams)
      AND (recording.preferred_relay_group_id IS NULL
           OR candidate.relay_group_id=recording.preferred_relay_group_id
           OR job.relay_fairness_started_at<=transaction_timestamp()-interval '12 seconds'
           OR NOT EXISTS(
             SELECT 1 FROM relay_groups preferred_group
             WHERE preferred_group.id=recording.preferred_relay_group_id
               AND preferred_group.account_id=recording.account_id
               AND (SELECT count(*) FROM recording_jobs preferred_jobs
                    JOIN nodes preferred_nodes ON preferred_jobs.lease_owner='node:'||preferred_nodes.id::text
                    WHERE preferred_nodes.account_id=recording.account_id
                      AND preferred_nodes.relay_group_id=preferred_group.id
                      AND preferred_jobs.status='leased' AND preferred_jobs.lease_expires_at>transaction_timestamp())<preferred_group.max_streams
               AND EXISTS(SELECT 1 FROM nodes preferred_node
                          WHERE preferred_node.account_id=recording.account_id
                            AND preferred_node.relay_group_id=preferred_group.id
                            AND preferred_node.node_type='relay' AND preferred_node.status='active'
                            AND preferred_node.last_heartbeat_at>=transaction_timestamp()-interval '120 seconds'
                            AND (SELECT count(*) FROM recording_jobs preferred_node_jobs
                                 WHERE preferred_node_jobs.status='leased'
                                   AND preferred_node_jobs.lease_owner='node:'||preferred_node.id::text
                                   AND preferred_node_jobs.lease_expires_at>transaction_timestamp())<preferred_node.relay_max_streams))
      AND (job.relay_fairness_started_at<=transaction_timestamp()-interval '12 seconds'
           OR candidate.relay_group_id IS NULL OR candidate.relay_group_id=recording.preferred_relay_group_id
           OR NOT EXISTS(
             SELECT 1 FROM relay_groups peer_group
             CROSS JOIN LATERAL (
               SELECT count(*) AS lease_count,
                      COALESCE(sum(GREATEST(COALESCE(peer_bw.observed_bandwidth_bps,0),4000000)),0) AS bandwidth_load
               FROM recording_jobs peer_jobs
               JOIN nodes peer_nodes ON peer_jobs.lease_owner='node:'||peer_nodes.id::text
               JOIN recordings peer_recordings ON peer_recordings.id=peer_jobs.recording_id
               LEFT JOIN recording_bandwidth_observations peer_bw ON peer_bw.recording_id=peer_recordings.id
               WHERE peer_nodes.account_id=candidate.account_id AND peer_nodes.relay_group_id=peer_group.id
                 AND peer_jobs.status='leased' AND peer_jobs.lease_expires_at>transaction_timestamp()
             ) peer_load
             WHERE peer_group.account_id=candidate.account_id AND peer_group.id<>candidate.relay_group_id
               AND peer_load.lease_count<peer_group.max_streams
               AND EXISTS(SELECT 1 FROM nodes peer_node
                          WHERE peer_node.account_id=candidate.account_id AND peer_node.relay_group_id=peer_group.id
                            AND peer_node.node_type='relay' AND peer_node.status='active'
                            AND peer_node.last_heartbeat_at>=transaction_timestamp()-interval '120 seconds'
                            AND (SELECT count(*) FROM recording_jobs peer_node_jobs
                                 WHERE peer_node_jobs.status='leased' AND peer_node_jobs.lease_owner='node:'||peer_node.id::text
                                   AND peer_node_jobs.lease_expires_at>transaction_timestamp())<peer_node.relay_max_streams)
               AND (peer_load.bandwidth_load+GREATEST(COALESCE((SELECT observed_bandwidth_bps FROM recording_bandwidth_observations WHERE recording_id=recording.id),0),4000000))::numeric
                     /COALESCE(peer_group.bandwidth_capacity_bps,peer_group.max_streams::bigint*4000000)
                   < ((SELECT COALESCE(sum(GREATEST(COALESCE(current_bw.observed_bandwidth_bps,0),4000000)),0)
                       FROM recording_jobs current_jobs
                       JOIN nodes current_nodes ON current_jobs.lease_owner='node:'||current_nodes.id::text
                       JOIN recordings current_recordings ON current_recordings.id=current_jobs.recording_id
                       LEFT JOIN recording_bandwidth_observations current_bw ON current_bw.recording_id=current_recordings.id
                       WHERE current_nodes.account_id=candidate.account_id AND current_nodes.relay_group_id=candidate.relay_group_id
                         AND current_jobs.status='leased' AND current_jobs.lease_expires_at>transaction_timestamp())
                      +GREATEST(COALESCE((SELECT observed_bandwidth_bps FROM recording_bandwidth_observations WHERE recording_id=recording.id),0),4000000))::numeric
                     /COALESCE(candidate_group.bandwidth_capacity_bps,candidate_group.max_streams::bigint*4000000)))
      AND (job.relay_fairness_started_at<=transaction_timestamp()-interval '12 seconds'
           OR candidate.relay_group_id IS NULL OR NOT EXISTS(
             SELECT 1 FROM nodes peer
             WHERE peer.account_id=candidate.account_id AND peer.relay_group_id=candidate.relay_group_id
               AND peer.node_type='relay' AND peer.status='active'
               AND peer.last_heartbeat_at>=transaction_timestamp()-interval '120 seconds'
               AND (SELECT count(*) FROM recording_jobs peer_jobs WHERE peer_jobs.status='leased'
                    AND peer_jobs.lease_owner='node:'||peer.id::text AND peer_jobs.lease_expires_at>transaction_timestamp())<peer.relay_max_streams
               AND (SELECT count(*) FROM recording_jobs peer_jobs WHERE peer_jobs.status='leased'
                    AND peer_jobs.lease_owner='node:'||peer.id::text AND peer_jobs.lease_expires_at>transaction_timestamp())
                   < (SELECT count(*) FROM recording_jobs current_jobs WHERE current_jobs.status='leased'
                      AND current_jobs.lease_owner='node:'||candidate.id::text AND current_jobs.lease_expires_at>transaction_timestamp())))
  )
$$;

CREATE FUNCTION recording_surrender_relay_alternate(p_job_id BIGINT,p_excluded_owner TEXT) RETURNS BOOLEAN
LANGUAGE sql STABLE AS $$
  SELECT EXISTS(
    SELECT 1 FROM nodes candidate
    WHERE 'node:'||candidate.id::text<>p_excluded_owner
      AND recording_surrender_relay_candidate_eligible(p_job_id,candidate.id)
  )
$$;

-- The scheduler and droplet controller call this one authority instead of
-- performing independent lease-clearing UPDATEs. It preserves generation facts,
-- opens upload-only recovery grants for every nonterminal producer, and chooses
-- old-owner exclusion from current capacity under the same global fence as claim.
CREATE FUNCTION recording_surrender_reclaim_expired() RETURNS BIGINT LANGUAGE plpgsql AS $$
DECLARE candidate RECORD; j RECORD; p RECORD; capture_set_row RECORD; lease_generation RECORD; alternate BOOLEAN; grants INTEGER; changed BIGINT:=0; grant_id UUID; observation_id UUID; episode_id UUID;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('recording-worker-claim-v1',0));
  PERFORM pg_advisory_xact_lock(hashtextextended('recording-surrender-cloud-capacity-v1',0));
  PERFORM recording_surrender_expire_set_plans();
  PERFORM recording_surrender_expire_set_grants();
  -- Expired upload-only capabilities close honestly. No later main-token or
  -- worker action may turn missing bytes into accepted footage.
  FOR p IN
    SELECT grant_row.id,grant_row.recording_job_id,grant_row.producer_id,grant_row.upload_intent_id,
           COALESCE((SELECT result.result FROM recording_capture_artifact_results result WHERE result.upload_intent_id=grant_row.upload_intent_id),'') AS artifact_result
    FROM recording_job_recovery_grants grant_row
	    WHERE grant_row.upload_grace_until<=transaction_timestamp()
	      AND NOT EXISTS(SELECT 1 FROM recording_job_recovery_grant_results grant_result WHERE grant_result.grant_id=grant_row.id)
    ORDER BY grant_row.recording_job_id,grant_row.id
  LOOP
    PERFORM pg_advisory_xact_lock(hashtextextended('recording-surrender-job:'||p.recording_job_id::text,0));
    PERFORM 1 FROM recording_job_recovery_grants grant_row
	      WHERE grant_row.id=p.id
	        AND grant_row.upload_grace_until<=transaction_timestamp()
	        AND NOT EXISTS(SELECT 1 FROM recording_job_recovery_grant_results grant_result WHERE grant_result.grant_id=grant_row.id)
      FOR UPDATE;
    IF NOT FOUND THEN CONTINUE; END IF;
	IF p.artifact_result IN('accepted_unique','exact_replay','abandoned_unsealed','unrecoverable_partial') THEN
	  INSERT INTO recording_job_recovery_grant_results(grant_id,result)
	  VALUES(p.id,'recovery_completed');
	ELSE
	  INSERT INTO recording_capture_artifact_results(upload_intent_id,result)
	  VALUES(p.upload_intent_id,'host_unreachable_unrecoverable') ON CONFLICT DO NOTHING;
	  INSERT INTO recording_job_recovery_grant_results(grant_id,result)
	  VALUES(p.id,'recovery_grace_expired');
	END IF;
    IF NOT EXISTS(
      SELECT 1 FROM recording_capture_artifact_intents artifact
      WHERE artifact.producer_id=p.producer_id
        AND NOT EXISTS(SELECT 1 FROM recording_capture_artifact_results result WHERE result.upload_intent_id=artifact.upload_intent_id)
    ) THEN
      INSERT INTO recording_capture_producer_results(producer_id,result,detail_class)
      VALUES(p.producer_id,'host_unreachable_unrecoverable','recovery_grace_expired') ON CONFLICT DO NOTHING;
    END IF;
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
    IF j.lease_token IS NULL OR NOT EXISTS(
      SELECT 1 FROM recording_job_lease_generations g
      WHERE g.recording_job_id=j.id AND g.lease_token=j.lease_token
    ) THEN
      UPDATE recording_jobs SET status='pending',lease_owner=NULL,lease_expires_at=NULL,lease_token=NULL,
        relay_fairness_started_at=NULL,updated_at=transaction_timestamp() WHERE id=j.id;
      changed:=changed+1;
      CONTINUE;
    END IF;
    IF j.capture_via='cloud' THEN
      SELECT EXISTS(
        SELECT 1 FROM recorder_droplets d
	        WHERE d.name<>j.lease_owner AND d.state IN('provisioning','active')
	          AND d.last_seen_at>=transaction_timestamp()-interval '2 minutes'
	          AND EXISTS(SELECT 1 FROM recording_worker_claim_heads claim
	                     JOIN node_tokens claim_token ON claim_token.id=claim.claim_token_id
	                     WHERE claim.node_id=d.node_id AND claim.state='enabled'
	                       AND claim_token.revoked_at IS NULL
	                       AND claim_token.recording_claim_generation=claim.generation
	                       AND claim_token.recording_claim_purpose='claim_current')
          AND (SELECT count(*) FROM recording_jobs live
               WHERE live.status='leased' AND live.lease_owner=d.name
                 AND live.lease_expires_at>transaction_timestamp())<d.capacity
      ) INTO alternate;
	ELSE
	  -- Lock every relay capacity row in identity order before deriving the
	  -- handoff fact. Claim takes the same rows/order and recomputes, so loss of
	  -- the alternate dynamically waives the old-owner exclusion.
	  PERFORM id FROM nodes WHERE account_id=(SELECT account_id FROM recordings recording JOIN recording_jobs job ON job.recording_id=recording.id WHERE job.id=j.id)
	    AND node_type='relay' ORDER BY id FOR UPDATE;
	  PERFORM id FROM relay_groups WHERE account_id=(SELECT account_id FROM recordings recording JOIN recording_jobs job ON job.recording_id=recording.id WHERE job.id=j.id)
	    ORDER BY id FOR UPDATE;
	  SELECT recording_surrender_relay_alternate(j.id,j.lease_owner) INTO alternate;
    END IF;
    grants:=0;
	-- A committed compact set is potential durable authority even when a crash
	-- happened before the first leaf was materialized. Block only this host's new
	-- claims; unrelated live fences and heartbeats remain valid. The originating
	-- claim token becomes existing-fence-only and can materialize exact Merkle
	-- leaves, but can never claim another job.
	SELECT generation.node_id,plan.origin_claim_generation AS claim_generation INTO lease_generation
	FROM recording_job_lease_generations generation
	JOIN recording_capture_set_plans plan
	  ON plan.recording_job_id=generation.recording_job_id AND plan.lease_token=generation.lease_token
	JOIN recording_capture_reservation_sets capture_set ON capture_set.plan_id=plan.id
	WHERE generation.recording_job_id=j.id AND generation.lease_token=j.lease_token
	  AND NOT EXISTS(SELECT 1 FROM recording_capture_set_results result WHERE result.set_id=capture_set.id)
	ORDER BY plan.capture_ordinal LIMIT 1;
	IF FOUND AND lease_generation.node_id IS NOT NULL AND lease_generation.claim_generation IS NOT NULL THEN
	  PERFORM 1 FROM recording_worker_claim_heads head WHERE head.node_id=lease_generation.node_id FOR UPDATE;
	  UPDATE recording_worker_claim_heads head
	  SET state='recovery_blocked',blocked_at=COALESCE(head.blocked_at,transaction_timestamp()),block_reason='durable_recovery'
	  WHERE head.node_id=lease_generation.node_id AND head.generation=lease_generation.claim_generation
	    AND head.state IN('enabled','recovery_blocked');
	  IF NOT FOUND THEN RAISE EXCEPTION 'expired set cannot acquire exact worker recovery block'; END IF;
	  UPDATE node_tokens token SET recording_claim_purpose='existing_fence_only'
	  WHERE token.id=(SELECT head.claim_token_id FROM recording_worker_claim_heads head WHERE head.node_id=lease_generation.node_id)
	    AND token.recording_claim_generation=lease_generation.claim_generation
	    AND token.revoked_at IS NULL AND token.recording_claim_purpose IN('claim_current','existing_fence_only');
	  INSERT INTO recording_worker_claim_generation_events(node_id,generation,predecessor_generation,claim_token_id,event_type,facts_sha256)
	  SELECT head.node_id,head.generation,CASE WHEN head.generation=1 THEN NULL ELSE head.generation-1 END,
	         head.claim_token_id,'recovery_blocked',
	         encode(sha256(convert_to('recording-worker-recovery-block-v1'||chr(0)||head.node_id::text||chr(0)||head.generation::text||chr(0)||j.id::text||chr(0)||j.lease_token::text,'UTF8')),'hex')
	  FROM recording_worker_claim_heads head WHERE head.node_id=lease_generation.node_id
	  ON CONFLICT DO NOTHING;
	  FOR capture_set_row IN
	    SELECT capture_set.id,plan.origin_claim_generation
	    FROM recording_capture_reservation_sets capture_set
	    JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
	    WHERE plan.recording_job_id=j.id AND plan.lease_token=j.lease_token
	      AND NOT EXISTS(SELECT 1 FROM recording_capture_set_results result WHERE result.set_id=capture_set.id)
	    ORDER BY plan.capture_ordinal
	  LOOP
	    INSERT INTO recording_capture_set_grants(id,set_id,origin_claim_generation,recovery_block_generation,granted_at,upload_grace_until)
	    VALUES(gen_random_uuid(),capture_set_row.id,capture_set_row.origin_claim_generation,lease_generation.claim_generation,
	           transaction_timestamp(),transaction_timestamp()+interval '30 minutes')
	    ON CONFLICT(set_id) DO NOTHING;
	  END LOOP;
	END IF;
    FOR p IN
      SELECT producer.id,artifact.upload_intent_id,artifact.recovery_secret_sha256
      FROM recording_capture_producers producer
      JOIN recording_capture_artifact_intents artifact ON artifact.producer_id=producer.id
      WHERE producer.recording_job_id=j.id AND producer.lease_token=j.lease_token
	    AND NOT EXISTS(SELECT 1 FROM recording_capture_producer_results result WHERE result.producer_id=producer.id)
	  ORDER BY producer.capture_ordinal,artifact.capture_sequence
    LOOP
      grant_id:=gen_random_uuid();
      INSERT INTO recording_job_recovery_grants(id,recording_job_id,lease_token,producer_id,upload_intent_id,recovery_secret_sha256,granted_at,upload_grace_until)
      VALUES(grant_id,j.id,j.lease_token,p.id,p.upload_intent_id,p.recovery_secret_sha256,transaction_timestamp(),transaction_timestamp()+interval '30 minutes')
      ON CONFLICT(recording_job_id,lease_token,upload_intent_id) DO NOTHING;
      IF FOUND THEN grants:=grants+1; END IF;
    END LOOP;
    SELECT (SELECT count(*) FROM recording_job_recovery_grants grant_row
	  WHERE grant_row.recording_job_id=j.id AND grant_row.lease_token=j.lease_token)
	      +(SELECT count(*) FROM recording_capture_set_grants set_grant
	         JOIN recording_capture_reservation_sets capture_set ON capture_set.id=set_grant.set_id
	         JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
	         WHERE plan.recording_job_id=j.id AND plan.lease_token=j.lease_token) INTO grants;
	    INSERT INTO recording_job_lease_expiry_events(recording_job_id,lease_token,lease_owner,recovery_grant_count,alternate_available,handoff_until,reclaimed_at)
    VALUES(j.id,j.lease_token,j.lease_owner,grants,alternate,transaction_timestamp()+CASE WHEN alternate THEN interval '5 minutes' ELSE interval '0' END,transaction_timestamp())
	    ON CONFLICT DO NOTHING;
	    INSERT INTO recording_capture_producer_results(producer_id,result,detail_class)
	    SELECT producer.id,'abandoned_empty','no_artifact_reservation'
	    FROM recording_capture_producers producer
	    WHERE producer.recording_job_id=j.id AND producer.lease_token=j.lease_token
	      AND NOT EXISTS(SELECT 1 FROM recording_capture_artifact_intents artifact WHERE artifact.producer_id=producer.id)
	      AND NOT EXISTS(SELECT 1 FROM recording_capture_producer_results result WHERE result.producer_id=producer.id)
	    ON CONFLICT DO NOTHING;
	    observation_id:=gen_random_uuid();
	    INSERT INTO recording_surrender_transport_observations(id,recording_job_id,lease_token,worker_id,node_id,observation_type,observed_at,request_sha256)
	    VALUES(observation_id,j.id,j.lease_token,j.lease_owner,
	      (SELECT node_id FROM recording_job_lease_generations WHERE recording_job_id=j.id AND lease_token=j.lease_token),
	      'expired_reclaim',transaction_timestamp(),
      encode(sha256(convert_to('expired-reclaim-v1:'||j.id::text||':'||j.lease_token::text,'UTF8')),'hex'))
    ON CONFLICT DO NOTHING;
	    INSERT INTO recording_surrender_transport_episodes(recording_job_id,episode_key,lease_token,state,reason)
	    VALUES(j.id,observation_id,j.lease_token,'open','expired_reclaim')
	    ON CONFLICT(recording_job_id) DO UPDATE SET
	      lease_token=EXCLUDED.lease_token,state='open',reason='expired_reclaim',last_observed_at=transaction_timestamp(),resolved_at=NULL
	    RETURNING episode_key INTO episode_id;
	    INSERT INTO recording_surrender_transport_episode_events(event_key,episode_key,recording_job_id,lease_token,event_type,reason)
	    VALUES(observation_id,episode_id,j.id,j.lease_token,'opened','expired_reclaim') ON CONFLICT DO NOTHING;
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
	'recording_surrender_initialize_claim_token()',
	'recording_surrender_validate_claim_generation_event()',
	'recording_surrender_validate_claim_token_retirement_seal()',
	'recording_surrender_source_snapshot(bigint)',
	'recording_surrender_destination_snapshot(bigint)',
	'recording_surrender_capture_config_snapshot(bigint,bigint,uuid)',
	'recording_surrender_validate_set_plan()',
	'recording_surrender_validate_plan_result()',
	'recording_surrender_expire_set_plans()',
	'recording_surrender_validate_set_commit()',
	'recording_surrender_validate_materialized_artifact()',
	'recording_surrender_validate_stopped_artifact_membership()',
	'recording_surrender_validate_stopped_artifact_seal()',
	'recording_surrender_validate_stop_event()',
	'recording_surrender_validate_stop_ack()',
	'recording_surrender_validate_stop_ack_member()',
	'recording_surrender_stop_inventory_sha(uuid)',
	'recording_surrender_validate_stop_inventory_seal()',
	'recording_surrender_validate_recovery_report()',
	'recording_surrender_validate_empty_set_report()',
	'recording_surrender_validate_artifact_grant_result()',
	'recording_surrender_unused_capture_ranges(integer,integer[])',
	'recording_surrender_validate_set_result()',
	'recording_surrender_validate_security_event()',
	'recording_surrender_author_db_times()',
	'recording_surrender_validate_recovery_upload_session_result()',
	'recording_surrender_validate_recovery_upload_session()',
	'recording_surrender_reconcile_expired_upload_sessions()',
	'recording_surrender_validate_set_grant()',
	'recording_surrender_validate_set_grant_expiry_seal()',
	'recording_surrender_validate_claim_head_projection()',
	'recording_surrender_validate_claim_head_event_seal()',
	'recording_surrender_validate_claim_successor_proposal()',
	'recording_surrender_validate_claim_successor_result()',
	'recording_surrender_validate_claim_token_update()',
	'recording_surrender_normalize_lease_credential()',
	'recording_surrender_validate_lease_admission()',
	'recording_surrender_token_can_access_lease(bigint,bigint,bigint,bigint)',
	'recording_surrender_append_stream_stop_events()',
	'recording_surrender_append_recording_stop_events()',
	'recording_surrender_append_destination_stop_events()',
	'recording_surrender_validate_key_root_transition()',
	'recording_surrender_validate_key_root_consumption()',
	'recording_surrender_reserve_legacy_intent_key()',
	'recording_surrender_validate_clip_key_root()',
	'recording_surrender_validate_grant_result()',
	'recording_surrender_validate_bound_upload_intent()',
    'recording_surrender_validate_grant_insert()',
	    'recording_surrender_validate_expiry_event_insert()',
	    'recording_surrender_validate_transport_observation()',
	    'recording_surrender_validate_episode_write()',
	    'recording_surrender_validate_episode_event()',
    'recording_surrender_validate_producer_insert()',
	'recording_surrender_validate_artifact_intent()',
    'recording_surrender_validate_artifact_seal()',
    'recording_surrender_validate_producer_result()',
    'recording_surrender_validate_unique_head()',
	    'recording_surrender_validate_artifact_result()',
	    'recording_surrender_validate_v1_clip()',
	    'recording_surrender_freeze_v1_clip()',
    'recording_surrender_request_sha(uuid,text,text,bigint,uuid,bigint,integer,bigint,integer)',
    'recording_surrender_validate_attempt_insert()',
    'recording_surrender_validate_attempt_result()',
    'recording_surrender_validate_generation_insert()',
	    'recording_surrender_validate_head_insert()',
	    'recording_surrender_protect_durable_bytes()',
    'recording_surrender_record_lease_generation()',
	'recording_surrender_fence_source_revision()',
	'recording_surrender_append_source_revision_stop_events()',
	'recording_surrender_preserve_referenced_source_revision()',
	'recording_surrender_expire_set_grants()',
	'recording_surrender_relay_alternate(bigint,text)',
	'recording_surrender_relay_candidate_eligible(bigint,bigint)',
    'recording_surrender_reclaim_expired()'
  ] LOOP
    EXECUTE format('ALTER FUNCTION %I.%s SET search_path = %I, pg_catalog, pg_temp',install_schema,signature,install_schema);
  END LOOP;
END
$pin_recording_surrender_search_path$;

-- PostgreSQL grants EXECUTE on new functions to PUBLIC by default. These are
-- mutation authorities/triggers, not public SQL APIs. The migration owner (the
-- authenticated API/control runtime role) retains execute; arbitrary roles do
-- not inherit it.
REVOKE ALL ON FUNCTION recording_surrender_reclaim_expired() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_source_snapshot(BIGINT) FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_destination_snapshot(BIGINT) FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_capture_config_snapshot(BIGINT,BIGINT,UUID) FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_set_plan() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_plan_result() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_expire_set_plans() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_set_commit() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_materialized_artifact() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_stopped_artifact_membership() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_stopped_artifact_seal() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_stop_event() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_stop_ack() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_stop_ack_member() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_stop_inventory_sha(UUID) FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_stop_inventory_seal() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_recovery_report() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_empty_set_report() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_artifact_grant_result() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_unused_capture_ranges(INTEGER,INTEGER[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_set_result() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_security_event() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_author_db_times() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_recovery_upload_session_result() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_recovery_upload_session() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_reconcile_expired_upload_sessions() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_set_grant() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_set_grant_expiry_seal() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_claim_head_projection() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_claim_head_event_seal() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_claim_successor_proposal() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_claim_successor_result() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_claim_token_update() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_freeze_rows() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_initialize_claim_token() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_claim_generation_event() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_claim_token_retirement_seal() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_normalize_lease_credential() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_lease_admission() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_token_can_access_lease(BIGINT,BIGINT,BIGINT,BIGINT) FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_append_stream_stop_events() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_append_recording_stop_events() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_append_destination_stop_events() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_key_root_transition() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_key_root_consumption() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_reserve_legacy_intent_key() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_clip_key_root() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_grant_result() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_bound_upload_intent() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_grant_insert() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_expiry_event_insert() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_transport_observation() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_episode_write() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_episode_event() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_producer_insert() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_artifact_intent() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_artifact_seal() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_producer_result() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_unique_head() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_artifact_result() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_v1_clip() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_freeze_v1_clip() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_request_sha(UUID,TEXT,TEXT,BIGINT,UUID,BIGINT,INTEGER,BIGINT,INTEGER) FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_attempt_insert() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_attempt_result() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_generation_insert() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_validate_head_insert() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_protect_durable_bytes() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_record_lease_generation() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_fence_source_revision() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_append_source_revision_stop_events() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_preserve_referenced_source_revision() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_expire_set_grants() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_relay_alternate(BIGINT,TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_surrender_relay_candidate_eligible(BIGINT,BIGINT) FROM PUBLIC;
