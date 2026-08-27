CREATE TABLE recording_joined_gap_only_scope_authorizations (
  artifact_id BIGINT NOT NULL REFERENCES recording_joined_artifacts(id) ON DELETE RESTRICT,
  batch_record_id BIGINT NOT NULL REFERENCES recording_joined_batches(id) ON DELETE RESTRICT,
  batch_id TEXT NOT NULL,
  hour_record_id BIGINT NOT NULL REFERENCES recording_joined_hours(id) ON DELETE RESTRICT,
  hour_id TEXT NOT NULL,
  work_scope TEXT NOT NULL CHECK(work_scope IN ('canary','canary_single','frozen_batch')),
  work_scope_identity_sha256 TEXT NOT NULL CHECK(work_scope_identity_sha256 ~ '^[0-9a-f]{64}$'),
  work_scope_identity_bytes BYTEA NOT NULL,
  canary_hour_ids_sha256 TEXT,
  authorization_source TEXT NOT NULL CHECK(authorization_source IN ('server_seal','operator_frozen')),
  request_sha256 TEXT NOT NULL CHECK(request_sha256 ~ '^[0-9a-f]{64}$'),
  relative_path TEXT NOT NULL,
  object_key TEXT NOT NULL,
  expected_size_bytes BIGINT NOT NULL CHECK(expected_size_bytes > 0),
  expected_sha256 TEXT NOT NULL CHECK(expected_sha256 ~ '^[0-9a-f]{64}$'),
  review_evidence_sha256 TEXT NOT NULL CHECK(review_evidence_sha256 ~ '^[0-9a-f]{64}$'),
  incident_id TEXT,
  verification_policy_version TEXT NOT NULL CHECK(verification_policy_version='joined-gap-authorization-v1'),
  verified_publication_state TEXT NOT NULL CHECK(verified_publication_state IN ('sealed','published')),
  verified_etag TEXT,
  verified_version_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(artifact_id,work_scope_identity_sha256),
  CHECK ((work_scope IN ('canary','canary_single') AND canary_hour_ids_sha256 ~ '^[0-9a-f]{64}$')
    OR (work_scope='frozen_batch' AND canary_hour_ids_sha256 IS NULL)),
  CHECK (authorization_source<>'operator_frozen' OR work_scope='frozen_batch'),
  CHECK ((authorization_source='operator_frozen' AND incident_id ~ '^[a-z0-9][a-z0-9._-]{0,127}$')
    OR (authorization_source='server_seal' AND incident_id IS NULL))
);

CREATE FUNCTION validate_recording_joined_gap_only_scope_authorization() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE artifact recording_joined_artifacts%ROWTYPE; hour recording_joined_hours%ROWTYPE; scope_identity JSONB;
BEGIN
  SELECT * INTO STRICT artifact FROM recording_joined_artifacts WHERE id=NEW.artifact_id FOR SHARE;
  SELECT * INTO STRICT hour FROM recording_joined_hours WHERE id=NEW.hour_record_id FOR SHARE;
  BEGIN scope_identity:=convert_from(NEW.work_scope_identity_bytes,'UTF8')::jsonb;
  EXCEPTION WHEN OTHERS THEN RAISE EXCEPTION 'joined gap-only scope authorization encoding differs'; END;
  IF encode(sha256(NEW.work_scope_identity_bytes),'hex') IS DISTINCT FROM NEW.work_scope_identity_sha256
    OR scope_identity->>'work_scope' IS DISTINCT FROM NEW.work_scope
    OR (CASE WHEN NEW.work_scope IN ('canary','canary_single') THEN scope_identity->>'canary_hour_ids_sha256'
      ELSE NULL END) IS DISTINCT FROM NEW.canary_hour_ids_sha256
    OR (NEW.work_scope IN ('canary','canary_single') AND jsonb_typeof(scope_identity->'canary_hour_ids') IS DISTINCT FROM 'array')
    OR (NEW.work_scope IN ('canary','canary_single') AND NOT (scope_identity->'canary_hour_ids' @> to_jsonb(ARRAY[NEW.hour_id])))
    OR (NEW.work_scope='frozen_batch' AND scope_identity IS DISTINCT FROM '{"work_scope":"frozen_batch"}'::jsonb)
    OR artifact.artifact_kind IS DISTINCT FROM 'hour_manifest'
    OR artifact.hour_record_id IS DISTINCT FROM hour.id
    OR artifact.batch_record_id IS DISTINCT FROM NEW.batch_record_id
    OR artifact.batch_id IS DISTINCT FROM NEW.batch_id
    OR artifact.scope_kind IS DISTINCT FROM 'hour'
    OR artifact.scope_id IS DISTINCT FROM NEW.hour_id
    OR artifact.relative_path IS DISTINCT FROM NEW.relative_path
    OR artifact.object_key IS DISTINCT FROM NEW.object_key
    OR artifact.expected_size_bytes IS DISTINCT FROM NEW.expected_size_bytes
    OR artifact.expected_sha256 IS DISTINCT FROM NEW.expected_sha256
    OR artifact.publication_state IS DISTINCT FROM NEW.verified_publication_state
    OR (NEW.verified_publication_state='published' AND (artifact.etag IS DISTINCT FROM NEW.verified_etag
      OR artifact.version_id IS DISTINCT FROM NEW.verified_version_id))
    OR (NEW.verified_publication_state='sealed' AND (NEW.verified_etag IS NOT NULL OR NEW.verified_version_id IS NOT NULL))
    OR hour.batch_record_id IS DISTINCT FROM NEW.batch_record_id
    OR hour.batch_id IS DISTINCT FROM NEW.batch_id
    OR hour.hour_id IS DISTINCT FROM NEW.hour_id
    OR hour.source_clip_count IS DISTINCT FROM 0
    OR hour.state IS DISTINCT FROM 'sealed'
    OR artifact.expected_size_bytes IS DISTINCT FROM octet_length(artifact.canonical_bytes)
    OR artifact.expected_sha256 IS DISTINCT FROM encode(sha256(artifact.canonical_bytes),'hex')
    OR artifact.canonical_bytes IS NULL
    OR convert_from(artifact.canonical_bytes,'UTF8')::jsonb->>'status' IS DISTINCT FROM 'gap_only'
  THEN RAISE EXCEPTION 'joined gap-only scope authorization identity differs'; END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER recording_joined_gap_only_scope_authorization_validate
BEFORE INSERT ON recording_joined_gap_only_scope_authorizations
FOR EACH ROW EXECUTE FUNCTION validate_recording_joined_gap_only_scope_authorization();

CREATE FUNCTION reject_recording_joined_gap_only_scope_authorization_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'joined gap-only scope authorization is append-only'; END $$;

CREATE TRIGGER recording_joined_gap_only_scope_authorization_no_mutation
BEFORE UPDATE OR DELETE ON recording_joined_gap_only_scope_authorizations
FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_gap_only_scope_authorization_mutation();

CREATE TRIGGER recording_joined_gap_only_scope_authorization_no_truncate
BEFORE TRUNCATE ON recording_joined_gap_only_scope_authorizations
FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_gap_only_scope_authorization_mutation();

CREATE FUNCTION guard_recording_joined_gap_only_publication_transition() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE zero_source BOOLEAN;
BEGIN
  IF NEW.artifact_kind='batch_index' AND (NEW.publication_state IS DISTINCT FROM OLD.publication_state
      OR NEW.publication_token IS DISTINCT FROM OLD.publication_token) AND NEW.publication_state IN ('publishing','published')
    AND EXISTS(SELECT 1 FROM recording_joined_batch_index_refs ref
      JOIN recording_joined_artifacts target ON target.id=ref.referenced_artifact_id
      JOIN recording_joined_hours h ON h.id=target.hour_record_id
      WHERE ref.index_artifact_id=NEW.id AND ref.reference_kind='hour_manifest' AND h.source_clip_count=0
        AND NOT EXISTS(SELECT 1 FROM recording_joined_gap_only_scope_authorizations ga
          WHERE ga.artifact_id=target.id AND ga.batch_record_id=target.batch_record_id AND ga.batch_id=target.batch_id
            AND ga.hour_record_id=target.hour_record_id AND ga.hour_id=target.scope_id AND ga.work_scope='frozen_batch'
            AND ga.authorization_source IN ('server_seal','operator_frozen')))
  THEN RAISE EXCEPTION 'unauthorized joined gap-only batch-index publication transition'; END IF;
  IF NEW.artifact_kind='hour_manifest' AND NEW.hour_record_id IS NOT NULL
    AND (NEW.publication_state IS DISTINCT FROM OLD.publication_state
      OR NEW.publication_token IS DISTINCT FROM OLD.publication_token)
    AND NEW.publication_state IN ('publishing','published')
  THEN
    SELECT source_clip_count=0 INTO STRICT zero_source FROM recording_joined_hours WHERE id=NEW.hour_record_id FOR SHARE;
    -- This database guard proves a structurally valid authorization exists. API mutation
    -- paths separately require that authorization to match their exact current work scope.
    IF zero_source AND NOT EXISTS(SELECT 1 FROM recording_joined_gap_only_scope_authorizations ga
      WHERE ga.artifact_id=NEW.id AND ga.batch_record_id=NEW.batch_record_id AND ga.batch_id=NEW.batch_id
        AND ga.hour_record_id=NEW.hour_record_id AND ga.hour_id=NEW.scope_id)
    THEN RAISE EXCEPTION 'unauthorized joined gap-only publication transition'; END IF;
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER recording_joined_gap_only_publication_transition_guard
BEFORE UPDATE OF publication_state,publication_token ON recording_joined_artifacts
FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_gap_only_publication_transition();

CREATE FUNCTION guard_recording_joined_gap_only_batch_index_ref() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.reference_kind='hour_manifest' AND EXISTS(
    SELECT 1 FROM recording_joined_artifacts a JOIN recording_joined_hours h ON h.id=a.hour_record_id
    WHERE a.id=NEW.referenced_artifact_id AND h.source_clip_count=0
      AND NOT EXISTS(SELECT 1 FROM recording_joined_gap_only_scope_authorizations ga
        WHERE ga.artifact_id=a.id AND ga.batch_record_id=a.batch_record_id AND ga.batch_id=a.batch_id
          AND ga.hour_record_id=a.hour_record_id AND ga.hour_id=a.scope_id AND ga.work_scope='frozen_batch'
          AND ga.authorization_source IN ('server_seal','operator_frozen')))
  THEN RAISE EXCEPTION 'unauthorized joined gap-only batch-index reference'; END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER recording_joined_gap_only_batch_index_ref_guard
BEFORE INSERT ON recording_joined_batch_index_refs
FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_gap_only_batch_index_ref();
