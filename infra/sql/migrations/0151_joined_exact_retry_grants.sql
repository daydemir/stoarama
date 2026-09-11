-- Operator approval never removes old evidence or resets an attempt counter.
CREATE TABLE recording_joined_exact_retry_grants (
  hour_record_id BIGINT NOT NULL REFERENCES recording_joined_hours(id) ON DELETE RESTRICT,
  expected_attempt_count INTEGER NOT NULL CHECK (expected_attempt_count BETWEEN 1 AND 7),
  expected_claim_token UUID NOT NULL,
  failure_id BIGINT NOT NULL REFERENCES recording_joined_worker_failures(id) ON DELETE RESTRICT,
  reason TEXT NOT NULL CHECK (reason=btrim(reason) AND octet_length(reason) BETWEEN 1 AND 1024),
  granted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  consumed_at TIMESTAMPTZ,
  consumed_claim_token UUID,
  PRIMARY KEY (hour_record_id,expected_attempt_count),
  CHECK ((consumed_at IS NULL)=(consumed_claim_token IS NULL))
);

-- Shared by bootstrap discovery and both locked claim predicates, so a grant
-- can never bypass output evidence, a live lease or its exact failure identity.
CREATE FUNCTION joined_exact_retry_available(h recording_joined_hours) RETURNS boolean
LANGUAGE sql STABLE AS $$
  SELECT h.state='leased' AND h.lease_expires_at<=now() AND h.attempt_count BETWEEN 1 AND 7
    AND h.next_attempt_at<=now()
    AND h.source_clip_count>0 AND h.source_only_sha256 IS NULL AND h.canonical_plan IS NULL
    AND h.manifest_bytes IS NULL AND h.manifest_sha256 IS NULL AND h.sealed_at IS NULL
    AND NOT EXISTS(SELECT 1 FROM recording_joined_artifacts a WHERE a.hour_record_id=h.id)
    AND NOT EXISTS(SELECT 1 FROM recording_joined_hour_dispositions d WHERE d.hour_record_id=h.id)
    AND EXISTS(SELECT 1 FROM recording_joined_exact_retry_grants g
      JOIN recording_joined_worker_failures f ON f.id=g.failure_id
      WHERE g.hour_record_id=h.id AND g.expected_attempt_count=h.attempt_count
        AND g.expected_claim_token=h.claim_token AND g.consumed_at IS NULL
        AND f.hour_record_id=h.id AND f.attempt_count=h.attempt_count AND f.claim_token=h.claim_token
        AND f.disposition='retry' AND f.retry_at<=now()
        AND f.failure_class IN ('transient','resource'))
$$;

CREATE FUNCTION guard_joined_exact_retry_grant_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='DELETE' THEN RAISE EXCEPTION 'joined retry evidence is immutable'; END IF;
  IF OLD.consumed_at IS NOT NULL OR NEW.consumed_at IS NULL OR NEW.consumed_claim_token IS NULL
    OR (to_jsonb(NEW)-ARRAY['consumed_at','consumed_claim_token']) IS DISTINCT FROM
       (to_jsonb(OLD)-ARRAY['consumed_at','consumed_claim_token'])
    OR NOT EXISTS(SELECT 1 FROM recording_joined_hours h WHERE h.id=NEW.hour_record_id
      AND h.state='leased' AND h.attempt_count=NEW.expected_attempt_count+1
      AND h.claim_token=NEW.consumed_claim_token AND h.lease_expires_at>now())
  THEN RAISE EXCEPTION 'joined retry grant consumption differs'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_exact_retry_grant_update_guard
  BEFORE UPDATE OR DELETE ON recording_joined_exact_retry_grants
  FOR EACH ROW EXECUTE FUNCTION guard_joined_exact_retry_grant_update();
