DO $$
DECLARE constraint_name TEXT;
BEGIN
  FOR constraint_name IN
    SELECT conname FROM pg_constraint
    WHERE conrelid='recording_joined_gap_only_scope_authorizations'::regclass
      AND contype='c' AND pg_get_constraintdef(oid) LIKE '%work_scope%'
  LOOP
    EXECUTE format('ALTER TABLE recording_joined_gap_only_scope_authorizations DROP CONSTRAINT %I',constraint_name);
  END LOOP;
END $$;

ALTER TABLE recording_joined_gap_only_scope_authorizations
  ADD CONSTRAINT recording_joined_gap_authorization_work_scope_check
    CHECK(work_scope IN ('canary','canary_single','allowlist_50','frozen_batch')),
  ADD CONSTRAINT recording_joined_gap_authorization_canary_digest_check
    CHECK((work_scope IN ('canary','canary_single','allowlist_50') AND canary_hour_ids_sha256 ~ '^[0-9a-f]{64}$')
      OR (work_scope='frozen_batch' AND canary_hour_ids_sha256 IS NULL)),
  ADD CONSTRAINT recording_joined_gap_authorization_operator_source_check
    CHECK(authorization_source<>'operator_frozen' OR work_scope='frozen_batch');

CREATE OR REPLACE FUNCTION validate_recording_joined_gap_only_scope_authorization() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE artifact recording_joined_artifacts%ROWTYPE; hour recording_joined_hours%ROWTYPE; scope_identity JSONB;
BEGIN
  SELECT * INTO STRICT artifact FROM recording_joined_artifacts WHERE id=NEW.artifact_id FOR SHARE;
  SELECT * INTO STRICT hour FROM recording_joined_hours WHERE id=NEW.hour_record_id FOR SHARE;
  BEGIN scope_identity:=convert_from(NEW.work_scope_identity_bytes,'UTF8')::jsonb;
  EXCEPTION WHEN OTHERS THEN RAISE EXCEPTION 'joined gap-only scope authorization encoding differs'; END;
  IF encode(sha256(NEW.work_scope_identity_bytes),'hex') IS DISTINCT FROM NEW.work_scope_identity_sha256
    OR scope_identity->>'work_scope' IS DISTINCT FROM NEW.work_scope
    OR (CASE WHEN NEW.work_scope IN ('canary','canary_single','allowlist_50') THEN scope_identity->>'canary_hour_ids_sha256'
      ELSE NULL END) IS DISTINCT FROM NEW.canary_hour_ids_sha256
    OR (NEW.work_scope IN ('canary','canary_single','allowlist_50') AND jsonb_typeof(scope_identity->'canary_hour_ids') IS DISTINCT FROM 'array')
    OR (NEW.work_scope='canary' AND jsonb_array_length(scope_identity->'canary_hour_ids')<>3)
    OR (NEW.work_scope='canary_single' AND jsonb_array_length(scope_identity->'canary_hour_ids')<>1)
    OR (NEW.work_scope='allowlist_50' AND jsonb_array_length(scope_identity->'canary_hour_ids')<>50)
    OR (NEW.work_scope IN ('canary','canary_single','allowlist_50') AND NOT (scope_identity->'canary_hour_ids' @> to_jsonb(ARRAY[NEW.hour_id])))
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
