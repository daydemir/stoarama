-- Immutable, lease-fenced worker failure evidence for bounded joined retries.
-- The existing task counters retain their database cap of 100. The API stops
-- broad-rollout retries at attempt 8, well below that corruption guard.
CREATE TABLE recording_joined_worker_failures (
  id BIGSERIAL PRIMARY KEY,
  batch_record_id BIGINT NOT NULL REFERENCES recording_joined_batches(id) ON DELETE RESTRICT,
  hour_record_id BIGINT REFERENCES recording_joined_hours(id) ON DELETE RESTRICT,
  artifact_id BIGINT REFERENCES recording_joined_artifacts(id) ON DELETE RESTRICT,
  batch_id TEXT NOT NULL,
  scope_kind TEXT NOT NULL CHECK (scope_kind IN ('hour','ledger','batch_index')),
  scope_id TEXT NOT NULL CHECK (scope_id=btrim(scope_id) AND scope_id<>'' AND octet_length(scope_id)<=1024),
  claim_token UUID NOT NULL,
  attempt_count INTEGER NOT NULL CHECK (attempt_count BETWEEN 1 AND 99),
  failure_class TEXT NOT NULL CHECK (failure_class IN ('transient','resource','deterministic')),
  reason_code TEXT NOT NULL CHECK (reason_code ~ '^[a-z][a-z0-9_]{0,79}$'),
  disposition TEXT NOT NULL CHECK (disposition IN ('retry','terminal')),
  retry_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK ((hour_record_id IS NULL) <> (artifact_id IS NULL)),
  CHECK ((disposition='retry')=(retry_at IS NOT NULL)),
  CHECK ((failure_class='deterministic' OR attempt_count>=8)=(disposition='terminal')),
  UNIQUE (claim_token),
  UNIQUE (hour_record_id, attempt_count),
  UNIQUE (artifact_id, attempt_count)
);

CREATE INDEX recording_joined_worker_failures_hour_retry_idx
  ON recording_joined_worker_failures(hour_record_id,retry_at DESC) WHERE disposition='retry';
CREATE INDEX recording_joined_worker_failures_artifact_retry_idx
  ON recording_joined_worker_failures(artifact_id,retry_at DESC) WHERE disposition='retry';
CREATE INDEX recording_joined_hours_exhausted_lease_idx ON recording_joined_hours(batch_id,lease_expires_at,id)
  WHERE state='leased' AND attempt_count>=8;
CREATE INDEX recording_joined_artifacts_exhausted_lease_idx ON recording_joined_artifacts(batch_id,publication_lease_expires_at,id)
  WHERE publication_state='publishing' AND publication_attempt_count>=8;

CREATE FUNCTION guard_recording_joined_worker_failure_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.disposition='retry' AND NEW.retry_at<=NEW.created_at
  THEN RAISE EXCEPTION 'joined failure retry is not in the future'; END IF;
  IF NEW.hour_record_id IS NOT NULL THEN
    IF NEW.scope_kind<>'hour' OR NOT EXISTS(SELECT 1 FROM recording_joined_hours h
      WHERE h.id=NEW.hour_record_id AND h.batch_record_id=NEW.batch_record_id AND h.batch_id=NEW.batch_id
        AND h.hour_id=NEW.scope_id AND h.state='leased' AND h.claim_token=NEW.claim_token
        AND (h.lease_expires_at>now() OR (NEW.disposition='terminal' AND NEW.attempt_count>=8
          AND NEW.reason_code='worker_lease_expired' AND h.lease_expires_at<=now()))
        AND h.attempt_count=NEW.attempt_count FOR KEY SHARE)
    THEN RAISE EXCEPTION 'joined hour failure lease differs'; END IF;
  ELSE
    IF NOT EXISTS(SELECT 1 FROM recording_joined_artifacts a
      WHERE a.id=NEW.artifact_id AND a.batch_record_id=NEW.batch_record_id AND a.batch_id=NEW.batch_id
        AND a.scope_kind=NEW.scope_kind AND a.scope_id=NEW.scope_id AND a.publication_state='publishing'
        AND a.publication_token=NEW.claim_token
        AND (a.publication_lease_expires_at>now() OR (NEW.disposition='terminal' AND NEW.attempt_count>=8
          AND NEW.reason_code='worker_lease_expired' AND a.publication_lease_expires_at<=now()))
        AND a.publication_attempt_count=NEW.attempt_count FOR KEY SHARE)
    THEN RAISE EXCEPTION 'joined publication failure lease differs'; END IF;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_worker_failure_insert_guard BEFORE INSERT ON recording_joined_worker_failures
  FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_worker_failure_insert();

CREATE TRIGGER recording_joined_worker_failure_immutable BEFORE UPDATE OR DELETE ON recording_joined_worker_failures
  FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_mutation();

-- The original guards accept terminal failure only while a lease is live.
-- Preserve that rule for normal reports, but allow one narrow recovery for an
-- expired exhausted lease after its immutable terminal evidence exists.
DROP TRIGGER recording_joined_hour_update_guard ON recording_joined_hours;
CREATE TRIGGER recording_joined_hour_update_guard BEFORE UPDATE ON recording_joined_hours
  FOR EACH ROW WHEN (NOT (OLD.state='leased' AND NEW.state='terminal_failed'
    AND OLD.lease_expires_at<=CURRENT_TIMESTAMP AND OLD.attempt_count>=8
    AND NEW.failure_reason_code='worker_lease_expired'))
  EXECUTE FUNCTION guard_recording_joined_hour_update();

CREATE FUNCTION guard_recording_joined_hour_expired_terminal() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.state<>'leased' OR NEW.state<>'terminal_failed' OR OLD.lease_expires_at>now()
    OR OLD.attempt_count<8 OR NEW.attempt_count<>OLD.attempt_count
    OR NEW.failure_reason_code<>'worker_lease_expired'
    OR NEW.claim_token IS NOT NULL OR NEW.claimed_by IS NOT NULL OR NEW.lease_expires_at IS NOT NULL OR NEW.heartbeat_at IS NOT NULL
    OR (to_jsonb(NEW)-ARRAY['state','claim_token','claimed_by','lease_expires_at','heartbeat_at','failure_reason_code','updated_at'])
      IS DISTINCT FROM
       (to_jsonb(OLD)-ARRAY['state','claim_token','claimed_by','lease_expires_at','heartbeat_at','failure_reason_code','updated_at'])
    OR NOT EXISTS(SELECT 1 FROM recording_joined_worker_failures f WHERE f.hour_record_id=OLD.id
      AND f.claim_token=OLD.claim_token AND f.attempt_count=OLD.attempt_count AND f.disposition='terminal'
      AND f.reason_code='worker_lease_expired' FOR KEY SHARE)
  THEN RAISE EXCEPTION 'invalid joined expired preflight exhaustion'; END IF;
  NEW.updated_at:=now(); RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_hour_expired_terminal_guard BEFORE UPDATE ON recording_joined_hours
  FOR EACH ROW WHEN (OLD.state='leased' AND NEW.state='terminal_failed'
    AND OLD.lease_expires_at<=CURRENT_TIMESTAMP AND OLD.attempt_count>=8
    AND NEW.failure_reason_code='worker_lease_expired')
  EXECUTE FUNCTION guard_recording_joined_hour_expired_terminal();

DROP TRIGGER recording_joined_artifact_update_guard ON recording_joined_artifacts;
CREATE TRIGGER recording_joined_artifact_update_guard BEFORE UPDATE ON recording_joined_artifacts
  FOR EACH ROW WHEN (NOT (OLD.publication_state='publishing' AND NEW.publication_state='terminal_failed'
    AND OLD.publication_lease_expires_at<=CURRENT_TIMESTAMP AND OLD.publication_attempt_count>=8
    AND NEW.failure_reason_code='worker_lease_expired'))
  EXECUTE FUNCTION guard_recording_joined_artifact_update();

CREATE FUNCTION guard_recording_joined_artifact_expired_terminal() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.artifact_kind='media' OR OLD.publication_state<>'publishing' OR NEW.publication_state<>'terminal_failed'
    OR OLD.publication_lease_expires_at>now() OR OLD.publication_attempt_count<8
    OR NEW.publication_attempt_count<>OLD.publication_attempt_count OR NEW.failure_reason_code<>'worker_lease_expired'
    OR NEW.publication_token IS NOT NULL OR NEW.publication_claimed_by IS NOT NULL
    OR NEW.publication_lease_expires_at IS NOT NULL OR NEW.publication_heartbeat_at IS NOT NULL
    OR (to_jsonb(NEW)-ARRAY['publication_state','publication_token','publication_claimed_by','publication_lease_expires_at',
         'publication_heartbeat_at','failure_reason_code','updated_at'])
      IS DISTINCT FROM
       (to_jsonb(OLD)-ARRAY['publication_state','publication_token','publication_claimed_by','publication_lease_expires_at',
         'publication_heartbeat_at','failure_reason_code','updated_at'])
    OR NOT EXISTS(SELECT 1 FROM recording_joined_worker_failures f WHERE f.artifact_id=OLD.id
      AND f.claim_token=OLD.publication_token AND f.attempt_count=OLD.publication_attempt_count
      AND f.disposition='terminal' AND f.reason_code='worker_lease_expired' FOR KEY SHARE)
  THEN RAISE EXCEPTION 'invalid joined expired publication exhaustion'; END IF;
  NEW.updated_at:=now(); RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_artifact_expired_terminal_guard BEFORE UPDATE ON recording_joined_artifacts
  FOR EACH ROW WHEN (OLD.publication_state='publishing' AND NEW.publication_state='terminal_failed'
    AND OLD.publication_lease_expires_at<=CURRENT_TIMESTAMP AND OLD.publication_attempt_count>=8
    AND NEW.failure_reason_code='worker_lease_expired')
  EXECUTE FUNCTION guard_recording_joined_artifact_expired_terminal();
