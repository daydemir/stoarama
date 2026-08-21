BEGIN;

ALTER TABLE connections ADD COLUMN joined_protocol_version SMALLINT NOT NULL DEFAULT 0,
  ADD COLUMN joined_files_pulled BIGINT NOT NULL DEFAULT 0, ADD COLUMN joined_bytes_pulled BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN joined_last_attempt_artifact_id BIGINT, ADD COLUMN joined_last_blocker TEXT NOT NULL DEFAULT '',
  ADD COLUMN joined_last_attempt_at TIMESTAMPTZ, ADD COLUMN joined_retry_at TIMESTAMPTZ;
ALTER TABLE connections ADD CONSTRAINT connections_joined_protocol_version_chk CHECK(joined_protocol_version IN(0,1)),
  ADD CONSTRAINT connections_joined_totals_chk CHECK(joined_files_pulled>=0 AND joined_bytes_pulled>=0),
  ADD CONSTRAINT connections_joined_attempt_chk CHECK(
    (joined_last_attempt_artifact_id IS NULL AND joined_last_blocker='' AND joined_last_attempt_at IS NULL AND joined_retry_at IS NULL)
    OR (joined_last_attempt_artifact_id>0 AND joined_last_blocker~'^[a-z][a-z0-9_]{0,79}$' AND joined_last_attempt_at IS NOT NULL
      AND (joined_retry_at IS NULL OR joined_retry_at>=joined_last_attempt_at)));

-- One local-hour row is the only cloud claim/lease boundary. Repeated batch
-- denominator facts make the frozen source/hour accounting independently checkable.
CREATE TABLE recording_joined_hours (
  id BIGSERIAL PRIMARY KEY, account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  connection_id BIGINT NOT NULL REFERENCES connections(id) ON DELETE RESTRICT, recording_id BIGINT NOT NULL CHECK(recording_id>0),
  batch_id TEXT NOT NULL CHECK(batch_id~'^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$'),
  batch_expected_hours INTEGER NOT NULL CHECK(batch_expected_hours BETWEEN 1 AND 100000),
  batch_expected_source_clips BIGINT NOT NULL CHECK(batch_expected_source_clips>=0),
  batch_expected_source_bytes BIGINT NOT NULL CHECK(batch_expected_source_bytes>=0),
  frozen_denominator_sha256 TEXT NOT NULL CHECK(frozen_denominator_sha256~'^[0-9a-f]{64}$'),
  source_frozen_at TIMESTAMPTZ NOT NULL,
  local_date DATE NOT NULL, delivery_hour SMALLINT NOT NULL CHECK(delivery_hour BETWEEN 1 AND 12),
  local_clock_hour SMALLINT NOT NULL CHECK(local_clock_hour BETWEEN 8 AND 19),
  local_timezone TEXT NOT NULL CHECK(btrim(local_timezone)<>''), hour_start_at TIMESTAMPTZ NOT NULL, hour_end_at TIMESTAMPTZ NOT NULL,
  batch_queue_order BIGINT NOT NULL CHECK(batch_queue_order>=0),
  priority_tier SMALLINT NOT NULL CHECK(priority_tier BETWEEN 1 AND 4), priority_order BIGINT NOT NULL CHECK(priority_order>=0),
  priority_facts JSONB NOT NULL CHECK(jsonb_typeof(priority_facts)='object'),
  source_clip_count INTEGER NOT NULL CHECK(source_clip_count BETWEEN 0 AND 4096), source_bytes BIGINT NOT NULL CHECK(source_bytes>=0),
  source_manifest_sha256 TEXT NOT NULL CHECK(source_manifest_sha256~'^[0-9a-f]{64}$'),
  generation INTEGER NOT NULL DEFAULT 1 CHECK(generation>0),
  canonical_hour_id TEXT NOT NULL CHECK(canonical_hour_id~'^[A-Za-z0-9][A-Za-z0-9._-]{0,255}$'),
  delivery_start_at TIMESTAMPTZ NOT NULL, delivery_end_at TIMESTAMPTZ NOT NULL,
  authoritative_local_dates DATE[] NOT NULL, authoritative_recording_job_ids BIGINT[] NOT NULL,
  qualification_facts JSONB NOT NULL CHECK(jsonb_typeof(qualification_facts)='object' AND qualification_facts<>'{}'::jsonb),
  qualification_sha256 TEXT NOT NULL CHECK(qualification_sha256~'^[0-9a-f]{64}$'),
  qualification_policy_version TEXT NOT NULL CHECK(qualification_policy_version=btrim(qualification_policy_version)
    AND qualification_policy_version<>'' AND octet_length(qualification_policy_version)<=128),
  state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN('pending','leased','sealed','publishing','published','failed')),
  attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count BETWEEN 0 AND 100), claim_token UUID,
  claimed_by TEXT CHECK(claimed_by IS NULL OR (claimed_by=btrim(claimed_by) AND claimed_by<>'' AND octet_length(claimed_by)<=256)),
  lease_expires_at TIMESTAMPTZ, heartbeat_at TIMESTAMPTZ, next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_reason_code TEXT NOT NULL DEFAULT '' CHECK(last_reason_code='' OR last_reason_code~'^[a-z][a-z0-9_]{0,79}$'),
  planned_output_count INTEGER CHECK(planned_output_count BETWEEN 0 AND 128),
  hour_manifest_sha256 TEXT CHECK(hour_manifest_sha256~'^[0-9a-f]{64}$'), seal_token UUID,
  quarantine_reason_code TEXT NOT NULL DEFAULT '' CHECK(quarantine_reason_code='' OR quarantine_reason_code~'^[a-z][a-z0-9_]{0,79}$'),
  publish_attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(publish_attempt_count BETWEEN 0 AND 100),
  publish_claim_token UUID,
  publish_claimed_by TEXT CHECK(publish_claimed_by IS NULL OR
    (publish_claimed_by=btrim(publish_claimed_by) AND publish_claimed_by<>'' AND octet_length(publish_claimed_by)<=256)),
  publish_lease_expires_at TIMESTAMPTZ, publish_heartbeat_at TIMESTAMPTZ,
  sealed_at TIMESTAMPTZ, published_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK(hour_end_at=hour_start_at+interval '1 hour'),
  CHECK(delivery_hour=local_clock_hour-7 AND ((source_clip_count=0 AND source_bytes=0) OR (source_clip_count>0 AND source_bytes>0))),
  CHECK(delivery_end_at>delivery_start_at AND array_ndims(authoritative_recording_job_ids)=1
    AND array_lower(authoritative_recording_job_ids,1)=1 AND cardinality(authoritative_recording_job_ids)=14
    AND array_position(authoritative_recording_job_ids,NULL) IS NULL AND array_ndims(authoritative_local_dates)=1
    AND array_lower(authoritative_local_dates,1)=1 AND cardinality(authoritative_local_dates)=14
    AND array_position(authoritative_local_dates,NULL) IS NULL),
  CHECK((planned_output_count IS NULL AND hour_manifest_sha256 IS NULL AND seal_token IS NULL AND sealed_at IS NULL)
    OR (planned_output_count IS NOT NULL AND hour_manifest_sha256 IS NOT NULL AND seal_token IS NOT NULL AND sealed_at IS NOT NULL)),
  CHECK((state='leased' AND claim_token IS NOT NULL AND claimed_by IS NOT NULL AND lease_expires_at IS NOT NULL AND heartbeat_at IS NOT NULL)
    OR (state<>'leased' AND claim_token IS NULL AND claimed_by IS NULL AND lease_expires_at IS NULL AND heartbeat_at IS NULL)),
  CHECK((state='publishing' AND publish_claim_token IS NOT NULL AND publish_claimed_by IS NOT NULL
      AND publish_lease_expires_at IS NOT NULL AND publish_heartbeat_at IS NOT NULL)
    OR (state<>'publishing' AND publish_claim_token IS NULL AND publish_claimed_by IS NULL
      AND publish_lease_expires_at IS NULL AND publish_heartbeat_at IS NULL)),
  CHECK(publish_claim_token IS NULL OR publish_claim_token<>seal_token),
  CHECK((state='published' AND sealed_at IS NOT NULL AND published_at IS NOT NULL) OR (state<>'published' AND published_at IS NULL)),
  UNIQUE(connection_id,batch_id,recording_id,local_date,delivery_hour,generation), UNIQUE(connection_id,canonical_hour_id));
CREATE INDEX recording_joined_hours_claim_idx ON recording_joined_hours(priority_tier,priority_order,next_attempt_at,id)
  WHERE state IN('pending','leased','sealed','publishing');
CREATE INDEX recording_joined_hours_batch_idx ON recording_joined_hours(connection_id,batch_id,id);

-- The immutable stream-day ledger is sealed before any hour can be claimed.
-- Its adjacency digest covers both within-day pairs and the edge from the
-- preceding delivery day, so hour workers never infer cross-hour/day seams.
CREATE TABLE recording_joined_stream_days (
  id BIGSERIAL PRIMARY KEY, account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  connection_id BIGINT NOT NULL REFERENCES connections(id) ON DELETE RESTRICT,
  recording_id BIGINT NOT NULL CHECK(recording_id>0), batch_id TEXT NOT NULL CHECK(batch_id~'^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$'),
  local_date DATE NOT NULL, recording_job_id BIGINT NOT NULL CHECK(recording_job_id>0),
  source_clip_count INTEGER NOT NULL CHECK(source_clip_count BETWEEN 0 AND 49152), source_bytes BIGINT NOT NULL CHECK(source_bytes>=0),
  source_manifest_sha256 TEXT NOT NULL CHECK(source_manifest_sha256~'^[0-9a-f]{64}$'),
  adjacency_manifest_sha256 TEXT NOT NULL CHECK(adjacency_manifest_sha256~'^[0-9a-f]{64}$'),
  first_source_clip_id BIGINT CHECK(first_source_clip_id>0), last_source_clip_id BIGINT CHECK(last_source_clip_id>0),
  previous_frozen_clip_id BIGINT CHECK(previous_frozen_clip_id>0),
  cross_day_facts JSONB NOT NULL CHECK(jsonb_typeof(cross_day_facts)='object'
    AND cross_day_facts->>'verdict' IN('no_sources','first','exact','gap')
    AND cross_day_facts->>'boundary'='cross_day'
    AND (cross_day_facts->>'verdict'<>'exact' OR cross_day_facts->>'certification_sha256'~'^[0-9a-f]{64}$')
    AND (cross_day_facts->>'verdict'<>'gap' OR cross_day_facts->>'reason_code'~'^[a-z][a-z0-9_]{0,79}$')),
  adjacency_facts JSONB NOT NULL CHECK(jsonb_typeof(adjacency_facts)='object'
    AND adjacency_facts->>'schema_version'='1' AND adjacency_facts?'pair_count'
    AND jsonb_typeof(adjacency_facts->'pair_count')='number'),
  state TEXT NOT NULL DEFAULT 'sealed' CHECK(state IN('sealed','publishing','published')),
  seal_token UUID NOT NULL DEFAULT gen_random_uuid(), publish_attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(publish_attempt_count BETWEEN 0 AND 100),
  publish_claim_token UUID, publish_claimed_by TEXT CHECK(publish_claimed_by IS NULL OR
    (publish_claimed_by=btrim(publish_claimed_by) AND publish_claimed_by<>'' AND octet_length(publish_claimed_by)<=256)),
  publish_lease_expires_at TIMESTAMPTZ, publish_heartbeat_at TIMESTAMPTZ, published_at TIMESTAMPTZ,
  sealed_at TIMESTAMPTZ NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(connection_id,batch_id,recording_id,local_date), UNIQUE(connection_id,batch_id,recording_job_id),
  CHECK (
    (source_clip_count=0 AND source_bytes=0 AND first_source_clip_id IS NULL AND last_source_clip_id IS NULL
      AND previous_frozen_clip_id IS NULL AND cross_day_facts->>'verdict'='no_sources')
    OR
    (source_clip_count>0 AND source_bytes>0 AND first_source_clip_id IS NOT NULL AND last_source_clip_id IS NOT NULL
      AND ((previous_frozen_clip_id IS NULL AND cross_day_facts->>'verdict'='first')
        OR (previous_frozen_clip_id IS NOT NULL AND cross_day_facts->>'verdict' IN('exact','gap'))))
  ),
  CHECK((state='publishing' AND publish_claim_token IS NOT NULL AND publish_claimed_by IS NOT NULL
      AND publish_lease_expires_at IS NOT NULL AND publish_heartbeat_at IS NOT NULL AND published_at IS NULL)
    OR (state='sealed' AND publish_claim_token IS NULL AND publish_claimed_by IS NULL AND publish_lease_expires_at IS NULL
      AND publish_heartbeat_at IS NULL AND published_at IS NULL)
    OR (state='published' AND publish_claim_token IS NULL AND publish_claimed_by IS NULL AND publish_lease_expires_at IS NULL
      AND publish_heartbeat_at IS NULL AND published_at IS NOT NULL)),
  CHECK(publish_claim_token IS NULL OR publish_claim_token<>seal_token)
);

-- Immutable stream-day allocation ledger: every frozen clip appears in exactly
-- one local hour per batch; allocation_facts carries cross-hour decisions.
CREATE TABLE recording_joined_hour_sources (
  hour_id BIGINT NOT NULL REFERENCES recording_joined_hours(id) ON DELETE RESTRICT,
  stream_day_id BIGINT NOT NULL REFERENCES recording_joined_stream_days(id) ON DELETE RESTRICT,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT, connection_id BIGINT NOT NULL REFERENCES connections(id) ON DELETE RESTRICT,
  batch_id TEXT NOT NULL CHECK(batch_id~'^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$'), ordinal INTEGER NOT NULL CHECK(ordinal>0),
  clip_id BIGINT NOT NULL CHECK(clip_id>0), recording_job_id BIGINT NOT NULL CHECK(recording_job_id>0),
  storage_destination_id BIGINT NOT NULL CHECK(storage_destination_id>0), provider TEXT NOT NULL CHECK(provider<>''),
  endpoint TEXT NOT NULL CHECK(endpoint<>''), region TEXT NOT NULL CHECK(region<>''),
  bucket TEXT NOT NULL CHECK(bucket<>''), object_key TEXT NOT NULL CHECK(object_key<>''), size_bytes BIGINT NOT NULL CHECK(size_bytes>0),
  sha256 TEXT NOT NULL CHECK(sha256~'^[0-9a-f]{64}$'),
  etag TEXT NOT NULL CHECK(etag=btrim(etag) AND etag<>'' AND position('"' in etag)=0 AND octet_length(etag)<=256),
  version_id TEXT NOT NULL DEFAULT '' CHECK(version_id=btrim(version_id) AND octet_length(version_id)<=1024),
  clip_start_at TIMESTAMPTZ NOT NULL, clip_end_at TIMESTAMPTZ NOT NULL, released_at TIMESTAMPTZ,
  stream_day_clip_ordinal INTEGER NOT NULL CHECK(stream_day_clip_ordinal>0),
  previous_clip_id BIGINT CHECK(previous_clip_id>0 AND previous_clip_id<>clip_id),
  adjacency_facts JSONB NOT NULL CHECK(jsonb_typeof(adjacency_facts)='object'
    AND adjacency_facts->>'schema_version'='1'
    AND adjacency_facts->>'verdict' IN('first','exact','gap')
    AND adjacency_facts->>'boundary' IN('first','within_hour','cross_hour','cross_day')
    AND adjacency_facts ?& ARRAY['previous_clip_id','next_clip_id','previous_presentation_end_utc',
      'next_presentation_start_utc','signed_gap_nanoseconds','reason']
    AND (adjacency_facts->>'verdict'<>'exact' OR adjacency_facts->>'certification_sha256'~'^[0-9a-f]{64}$')
    AND (adjacency_facts->>'verdict'<>'gap' OR adjacency_facts->>'reason_code'~'^[a-z][a-z0-9_]{0,79}$')),
  allocation_facts JSONB NOT NULL CHECK(jsonb_typeof(allocation_facts)='object'
    AND allocation_facts->>'boundary_rule'='closest_verified_seam'
    AND allocation_facts?'crosses_hour_start' AND jsonb_typeof(allocation_facts->'crosses_hour_start')='boolean'
    AND allocation_facts?'crosses_hour_end' AND jsonb_typeof(allocation_facts->'crosses_hour_end')='boolean'
    AND allocation_facts?'candidate_count' AND (allocation_facts->>'candidate_count')::integer>0
    AND allocation_facts?'winning_distance_us' AND (allocation_facts->>'winning_distance_us')::bigint>=0
    AND allocation_facts->>'tie_breaker' IN('earlier_seam','frozen_source_order','source_id')
    AND allocation_facts?'decision_sha256' AND allocation_facts->>'decision_sha256'~'^[0-9a-f]{64}$'),
  PRIMARY KEY(hour_id,ordinal), UNIQUE(hour_id,clip_id), UNIQUE(stream_day_id,stream_day_clip_ordinal),
  UNIQUE(connection_id,batch_id,clip_id), CHECK(clip_end_at>clip_start_at));
CREATE UNIQUE INDEX recording_joined_source_storage_identity_unique
  ON recording_joined_hour_sources(connection_id,batch_id,storage_destination_id,provider,endpoint,region,bucket,object_key,
    (CASE WHEN version_id<>'' THEN 'version:'||version_id ELSE 'etag:'||etag END));

-- Preflight inserts every part identity before upload. R2 media identity is
-- content-addressed and independent of the NAS-facing relative path.
CREATE TABLE recording_joined_outputs (
  id BIGSERIAL PRIMARY KEY, hour_id BIGINT REFERENCES recording_joined_hours(id) ON DELETE RESTRICT,
  stream_day_id BIGINT REFERENCES recording_joined_stream_days(id) ON DELETE RESTRICT,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT, connection_id BIGINT NOT NULL REFERENCES connections(id) ON DELETE RESTRICT,
  batch_id TEXT NOT NULL CHECK(batch_id~'^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$'),
  artifact_kind TEXT NOT NULL CHECK(artifact_kind IN('allocation_ledger','media','hour_manifest','batch_index')),
  content_type TEXT NOT NULL CHECK(content_type IN('video/mp4','application/json')),
  part_ordinal INTEGER CHECK(part_ordinal>0), hour_seal_token UUID,
  relative_path TEXT NOT NULL CHECK(relative_path<>'' AND left(relative_path,1)<>'/' AND right(relative_path,1)<>'/'
    AND position(E'\\' in relative_path)=0 AND position('//' in relative_path)=0 AND relative_path!~'(^|/)[.]{1,2}(/|$)'
    AND octet_length(relative_path)<=1024), content_id TEXT NOT NULL CHECK(content_id~'^[0-9a-f]{64}$'),
  expected_size_bytes BIGINT NOT NULL CHECK(expected_size_bytes>0 AND expected_size_bytes<=5363466240
    AND (content_type<>'application/json' OR expected_size_bytes<=16777216)),
  expected_sha256 TEXT NOT NULL CHECK(expected_sha256~'^[0-9a-f]{64}$' AND content_id=expected_sha256),
  object_key TEXT NOT NULL CHECK(
    (artifact_kind='media' AND object_key='joined/'||batch_id||'/objects/'||content_id||'.mp4')
    OR (artifact_kind='allocation_ledger' AND object_key='joined/'||batch_id||'/'||relative_path)
    OR (artifact_kind='hour_manifest' AND object_key='joined/'||batch_id||'/coverage/hours/'||(plan_facts->>'hour_id')||'.json')
    OR (artifact_kind='batch_index' AND object_key='joined/'||batch_id||'/coverage/batch.json')),
  source_claim_sha256 TEXT CHECK(source_claim_sha256~'^[0-9a-f]{64}$'),
  source_clip_count INTEGER NOT NULL CHECK(source_clip_count BETWEEN 0 AND 4096), source_bytes BIGINT NOT NULL CHECK(source_bytes>=0),
  coverage_start_at TIMESTAMPTZ, coverage_end_at TIMESTAMPTZ,
  plan_facts JSONB NOT NULL CHECK(jsonb_typeof(plan_facts)='object' AND plan_facts<>'{}'::jsonb),
  state TEXT NOT NULL DEFAULT 'planned' CHECK(state IN('planned','published')),
  size_bytes BIGINT CHECK(size_bytes>0 AND size_bytes<=5363466240), sha256 TEXT CHECK(sha256~'^[0-9a-f]{64}$'),
  r2_etag TEXT CHECK(r2_etag IS NULL OR (r2_etag=btrim(r2_etag) AND r2_etag<>'' AND position('"' in r2_etag)=0 AND octet_length(r2_etag)<=256)),
  r2_version_id TEXT CHECK(r2_version_id IS NULL OR (r2_version_id=btrim(r2_version_id) AND octet_length(r2_version_id)<=1024)),
  published_at TIMESTAMPTZ, nas_verified_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK((coverage_start_at IS NULL AND coverage_end_at IS NULL)
    OR (coverage_end_at>coverage_start_at
      AND (artifact_kind='media' OR coverage_end_at<=coverage_start_at+
        CASE WHEN artifact_kind='allocation_ledger' THEN interval '12 hours' ELSE interval '1 hour' END))),
  CHECK((artifact_kind='allocation_ledger' AND hour_id IS NULL AND stream_day_id IS NOT NULL AND hour_seal_token IS NOT NULL
      AND part_ordinal IS NULL AND content_type='application/json'
      AND relative_path='coverage/ledgers/'||(plan_facts->>'recording_id')||'/'||(plan_facts->>'local_date')||'.json'
      AND source_claim_sha256 IS NULL AND coverage_start_at IS NOT NULL AND coverage_end_at IS NOT NULL
      AND plan_facts->>'schema_version'='1')
    OR (artifact_kind='media' AND hour_id IS NOT NULL AND stream_day_id IS NULL AND hour_seal_token IS NOT NULL AND part_ordinal IS NOT NULL
      AND content_type='video/mp4' AND relative_path~'[.]mp4$' AND source_claim_sha256 IS NOT NULL
      AND source_clip_count>0 AND source_bytes>0 AND coverage_start_at IS NOT NULL AND coverage_end_at IS NOT NULL
      AND plan_facts?'policy_version' AND btrim(plan_facts->>'policy_version')<>''
      AND plan_facts?'ffmpeg_version' AND btrim(plan_facts->>'ffmpeg_version')<>''
      AND plan_facts?'ffprobe_version' AND btrim(plan_facts->>'ffprobe_version')<>'')
    OR (artifact_kind='hour_manifest' AND hour_id IS NOT NULL AND stream_day_id IS NULL AND hour_seal_token IS NOT NULL AND part_ordinal IS NULL
      AND content_type='application/json' AND relative_path='coverage/hours/'||(plan_facts->>'hour_id')||'.json' AND source_claim_sha256 IS NULL
      AND coverage_start_at IS NOT NULL AND coverage_end_at IS NOT NULL
      AND plan_facts->>'schema_version'='1')
    OR (artifact_kind='batch_index' AND hour_id IS NULL AND stream_day_id IS NULL AND hour_seal_token IS NULL AND part_ordinal IS NULL
      AND content_type='application/json' AND relative_path='coverage/batch.json' AND source_claim_sha256 IS NULL
      AND source_clip_count=0 AND source_bytes=0 AND coverage_start_at IS NULL AND coverage_end_at IS NULL
      AND plan_facts->>'schema_version'='1')),
  CHECK((state='published' AND size_bytes IS NOT NULL AND sha256 IS NOT NULL AND r2_etag IS NOT NULL AND r2_version_id IS NOT NULL
      AND published_at IS NOT NULL)
    OR (state='planned' AND size_bytes IS NULL AND sha256 IS NULL AND r2_etag IS NULL AND r2_version_id IS NULL
      AND published_at IS NULL AND nas_verified_at IS NULL)),
  UNIQUE(hour_id,artifact_kind,part_ordinal), UNIQUE(connection_id,batch_id,relative_path));
CREATE UNIQUE INDEX recording_joined_allocation_ledger_unique ON recording_joined_outputs(stream_day_id) WHERE artifact_kind='allocation_ledger';
CREATE UNIQUE INDEX recording_joined_hour_manifest_unique ON recording_joined_outputs(hour_id) WHERE artifact_kind='hour_manifest';
CREATE UNIQUE INDEX recording_joined_batch_index_unique ON recording_joined_outputs(connection_id,batch_id) WHERE artifact_kind='batch_index';
CREATE INDEX recording_joined_outputs_feed_idx ON recording_joined_outputs(connection_id,id)
  WHERE state='published' AND nas_verified_at IS NULL;

-- The compact final index binds exact first-class ledger and hour-manifest
-- artifacts without duplicating their potentially large canonical JSON.
CREATE TABLE recording_joined_batch_index_ledgers (
  batch_index_output_id BIGINT NOT NULL REFERENCES recording_joined_outputs(id) ON DELETE RESTRICT,
  ordinal INTEGER NOT NULL CHECK(ordinal>0), stream_day_id BIGINT NOT NULL REFERENCES recording_joined_stream_days(id) ON DELETE RESTRICT,
  ledger_output_id BIGINT NOT NULL REFERENCES recording_joined_outputs(id) ON DELETE RESTRICT,
  PRIMARY KEY(batch_index_output_id,ordinal), UNIQUE(batch_index_output_id,stream_day_id), UNIQUE(batch_index_output_id,ledger_output_id));
CREATE TABLE recording_joined_batch_index_hours (
  batch_index_output_id BIGINT NOT NULL REFERENCES recording_joined_outputs(id) ON DELETE RESTRICT,
  ordinal INTEGER NOT NULL CHECK(ordinal>0), hour_id BIGINT NOT NULL REFERENCES recording_joined_hours(id) ON DELETE RESTRICT,
  hour_manifest_output_id BIGINT NOT NULL REFERENCES recording_joined_outputs(id) ON DELETE RESTRICT,
  PRIMARY KEY(batch_index_output_id,ordinal), UNIQUE(batch_index_output_id,hour_id), UNIQUE(batch_index_output_id,hour_manifest_output_id));

CREATE FUNCTION guard_recording_joined_batch_index_ref_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE index_id BIGINT; DECLARE idx recording_joined_outputs%ROWTYPE;
BEGIN
  index_id=NEW.batch_index_output_id;
  SELECT * INTO idx FROM recording_joined_outputs WHERE id=index_id FOR SHARE;
  IF idx.id IS NULL OR idx.artifact_kind<>'batch_index' OR idx.state<>'planned'
    OR idx.created_at<>transaction_timestamp() THEN
    RAISE EXCEPTION 'joined batch index references must seal in the index creation transaction';
  END IF;
  IF TG_TABLE_NAME='recording_joined_batch_index_ledgers' THEN
    IF NOT EXISTS(SELECT 1 FROM recording_joined_stream_days d JOIN recording_joined_outputs ledger
        ON ledger.id=NEW.ledger_output_id
      WHERE d.id=NEW.stream_day_id AND d.connection_id=idx.connection_id AND d.batch_id=idx.batch_id
        AND ledger.stream_day_id=d.id AND ledger.artifact_kind='allocation_ledger' AND ledger.state='published') THEN
      RAISE EXCEPTION 'joined batch index ledger reference differs'; END IF;
  ELSE
    IF NOT EXISTS(SELECT 1 FROM recording_joined_hours h JOIN recording_joined_outputs manifest
        ON manifest.id=NEW.hour_manifest_output_id
      WHERE h.id=NEW.hour_id AND h.connection_id=idx.connection_id AND h.batch_id=idx.batch_id
        AND h.state='published' AND manifest.hour_id=h.id AND manifest.artifact_kind='hour_manifest'
        AND manifest.state='published') THEN
      RAISE EXCEPTION 'joined batch index hour reference differs'; END IF;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_batch_index_ledger_insert_guard BEFORE INSERT ON recording_joined_batch_index_ledgers
FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_batch_index_ref_insert();
CREATE TRIGGER recording_joined_batch_index_hour_insert_guard BEFORE INSERT ON recording_joined_batch_index_hours
FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_batch_index_ref_insert();

CREATE FUNCTION joined_batch_index_refs_complete(p_index BIGINT) RETURNS BOOLEAN LANGUAGE sql STABLE AS $$
  WITH idx AS (
    SELECT o.id,o.connection_id,o.batch_id,min(h.batch_expected_hours) expected_hours,
      min(h.batch_expected_source_clips) expected_clips,min(h.batch_expected_source_bytes) expected_bytes
    FROM recording_joined_outputs o JOIN recording_joined_hours h
      ON h.connection_id=o.connection_id AND h.batch_id=o.batch_id
    WHERE o.id=p_index AND o.artifact_kind='batch_index' GROUP BY o.id,o.connection_id,o.batch_id
  ), ordered_ledgers AS (
    SELECT d.id,row_number() OVER(ORDER BY min(h.priority_order),d.recording_id,d.local_date,d.id)::integer ordinal
    FROM idx JOIN recording_joined_stream_days d ON d.connection_id=idx.connection_id AND d.batch_id=idx.batch_id
    JOIN recording_joined_hours h ON h.connection_id=d.connection_id AND h.batch_id=d.batch_id
      AND h.recording_id=d.recording_id AND h.local_date=d.local_date
    GROUP BY d.id,d.recording_id,d.local_date
  ), ordered_hours AS (
    SELECT h.id,row_number() OVER(ORDER BY h.priority_order,h.recording_id,h.local_date,h.delivery_hour,h.id)::integer ordinal
    FROM idx JOIN recording_joined_hours h ON h.connection_id=idx.connection_id AND h.batch_id=idx.batch_id
  )
  SELECT
    (SELECT count(*) FROM recording_joined_batch_index_ledgers WHERE batch_index_output_id=idx.id)=idx.expected_hours/12
    AND (SELECT count(*) FROM recording_joined_batch_index_hours WHERE batch_index_output_id=idx.id)=idx.expected_hours
    AND (SELECT COALESCE(sum(h.source_clip_count),0) FROM recording_joined_batch_index_hours r
      JOIN recording_joined_hours h ON h.id=r.hour_id WHERE r.batch_index_output_id=idx.id)=idx.expected_clips
    AND (SELECT COALESCE(sum(h.source_bytes),0) FROM recording_joined_batch_index_hours r
      JOIN recording_joined_hours h ON h.id=r.hour_id WHERE r.batch_index_output_id=idx.id)=idx.expected_bytes
    AND (SELECT count(*) FROM recording_joined_batch_index_ledgers r JOIN ordered_ledgers o ON o.id=r.stream_day_id
      JOIN recording_joined_outputs artifact ON artifact.id=r.ledger_output_id
      WHERE r.batch_index_output_id=idx.id AND r.ordinal=o.ordinal AND artifact.stream_day_id=r.stream_day_id
        AND artifact.artifact_kind='allocation_ledger' AND artifact.state='published')=idx.expected_hours/12
    AND (SELECT count(*) FROM recording_joined_batch_index_hours r JOIN ordered_hours o ON o.id=r.hour_id
      JOIN recording_joined_outputs artifact ON artifact.id=r.hour_manifest_output_id
      WHERE r.batch_index_output_id=idx.id AND r.ordinal=o.ordinal AND artifact.hour_id=r.hour_id
        AND artifact.artifact_kind='hour_manifest' AND artifact.state='published')=idx.expected_hours
  FROM idx
$$;

-- Append-only media membership. One allocated clip can belong to only one
-- output part; the immutable hour source claim owns its canonical order.
CREATE TABLE recording_joined_output_sources (
  output_id BIGINT NOT NULL REFERENCES recording_joined_outputs(id) ON DELETE RESTRICT,
  hour_id BIGINT NOT NULL REFERENCES recording_joined_hours(id) ON DELETE RESTRICT,
  ordinal INTEGER NOT NULL CHECK(ordinal>0), clip_id BIGINT NOT NULL CHECK(clip_id>0),
  hour_source_ordinal INTEGER NOT NULL CHECK(hour_source_ordinal>0),
  seam_facts JSONB NOT NULL CHECK(jsonb_typeof(seam_facts)='object' AND seam_facts?'verdict'
    AND seam_facts->>'verdict' IN('exact','not_applicable')),
  PRIMARY KEY(output_id,ordinal), UNIQUE(output_id,hour_source_ordinal), UNIQUE(hour_id,clip_id));

-- Canonical manifest accounting: every frozen source is either included once
-- in a verified media artifact or immutably quarantined with typed repeatable
-- deterministic evidence. Infrastructure failures are never dispositions.
CREATE TABLE recording_joined_hour_source_dispositions (
  hour_id BIGINT NOT NULL REFERENCES recording_joined_hours(id) ON DELETE RESTRICT,
  clip_id BIGINT NOT NULL CHECK(clip_id>0), disposition TEXT NOT NULL CHECK(disposition IN('included','quarantined')),
  output_id BIGINT REFERENCES recording_joined_outputs(id) ON DELETE RESTRICT,
  reason_code TEXT NOT NULL DEFAULT '' CHECK(reason_code='' OR reason_code~'^[a-z][a-z0-9_]{0,79}$'),
  evidence_sha256 TEXT CHECK(evidence_sha256~'^[0-9a-f]{64}$'),
  evidence_facts JSONB NOT NULL CHECK(jsonb_typeof(evidence_facts)='object'),
  PRIMARY KEY(hour_id,clip_id),
  CHECK((disposition='included' AND output_id IS NOT NULL AND reason_code='' AND evidence_sha256 IS NULL
      AND evidence_facts='{}'::jsonb)
    OR (disposition='quarantined' AND output_id IS NULL AND reason_code<>'' AND evidence_sha256 IS NOT NULL
      AND evidence_facts ?& ARRAY['schema_version','classification','isolated_build_count','source_claim_sha256','policy_sha256',
        'media_tool_sha256','expected_facts_sha256','observed_facts_sha256','repeat_digest_sha256']
      AND evidence_facts->>'schema_version'='1'
      AND evidence_facts->>'classification' IN('deterministic_media_failure','frozen_manifest_failure')
      AND (evidence_facts->>'isolated_build_count')::integer>=2
      AND evidence_facts->>'source_claim_sha256'~'^[0-9a-f]{64}$'
      AND evidence_facts->>'policy_sha256'~'^[0-9a-f]{64}$'
      AND evidence_facts->>'media_tool_sha256'~'^[0-9a-f]{64}$'
      AND evidence_facts->>'expected_facts_sha256'~'^[0-9a-f]{64}$'
      AND evidence_facts->>'observed_facts_sha256'~'^[0-9a-f]{64}$'
      AND evidence_facts->>'repeat_digest_sha256'=evidence_sha256)));

CREATE FUNCTION guard_recording_joined_source_disposition() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE h recording_joined_hours%ROWTYPE;
BEGIN
  SELECT * INTO h FROM recording_joined_hours WHERE id=NEW.hour_id FOR SHARE;
  IF h.id IS NULL OR h.state<>'leased' OR h.sealed_at IS NOT NULL OR h.lease_expires_at<=now()
    OR NOT EXISTS(SELECT 1 FROM recording_joined_hour_sources src WHERE src.hour_id=NEW.hour_id AND src.clip_id=NEW.clip_id)
    OR (NEW.disposition='included' AND NOT EXISTS(SELECT 1 FROM recording_joined_output_sources os
      JOIN recording_joined_outputs o ON o.id=os.output_id WHERE os.hour_id=NEW.hour_id AND os.clip_id=NEW.clip_id
        AND o.id=NEW.output_id AND o.artifact_kind='media' AND o.hour_seal_token=h.claim_token)) THEN
    RAISE EXCEPTION 'joined source disposition differs from active frozen hour'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_source_disposition_guard BEFORE INSERT ON recording_joined_hour_source_dispositions
FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_source_disposition();

CREATE FUNCTION joined_batch_is_sealed(p_connection BIGINT,p_batch TEXT,p_hours INTEGER,p_clips BIGINT,p_bytes BIGINT,p_manifest TEXT)
RETURNS BOOLEAN LANGUAGE sql STABLE AS $$
  SELECT count(*)=p_hours AND COALESCE(sum(source_clip_count),0)=p_clips AND COALESCE(sum(source_bytes),0)=p_bytes
    AND count(*) FILTER(WHERE batch_expected_hours=p_hours AND batch_expected_source_clips=p_clips
      AND batch_expected_source_bytes=p_bytes AND frozen_denominator_sha256=p_manifest)=p_hours
  FROM recording_joined_hours WHERE connection_id=p_connection AND batch_id=p_batch
$$;

CREATE FUNCTION guard_recording_joined_hour_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE n BIGINT; DECLARE eh INTEGER; DECLARE ec BIGINT; DECLARE eb BIGINT; DECLARE m TEXT;
DECLARE distinct_jobs BIGINT; DECLARE distinct_dates BIGINT; DECLARE qualified_jobs BIGINT; DECLARE dates_consecutive BOOLEAN;
DECLARE scope recording_joined_hours%ROWTYPE;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('recording_joined_hour_planner',0));
  SELECT count(DISTINCT job_id) INTO distinct_jobs FROM unnest(NEW.authoritative_recording_job_ids) job_id;
  SELECT count(DISTINCT local_day) INTO distinct_dates FROM unnest(NEW.authoritative_local_dates) local_day;
  SELECT bool_and(local_day=NEW.authoritative_local_dates[1]+(ordinality-1)::integer) INTO dates_consecutive
    FROM unnest(NEW.authoritative_local_dates) WITH ORDINALITY dates(local_day,ordinality);
  SELECT count(*) INTO qualified_jobs
  FROM unnest(NEW.authoritative_recording_job_ids,NEW.authoritative_local_dates) paired(job_id,local_day)
  JOIN recording_jobs j ON j.id=paired.job_id
  WHERE j.recording_id=NEW.recording_id AND j.status='done' AND j.kind='continuous_window' AND j.completed_at IS NOT NULL
    AND j.fire_at=j.scheduled_for AND j.window_end_at=j.scheduled_for+interval '12 hours' AND j.completed_at>=j.window_end_at
    AND (j.scheduled_for AT TIME ZONE NEW.local_timezone)::date=paired.local_day
    AND (j.scheduled_for AT TIME ZONE NEW.local_timezone)::time=time '08:00';
  IF NEW.state<>'pending' OR NEW.attempt_count<>0 OR NEW.claim_token IS NOT NULL OR NEW.sealed_at IS NOT NULL THEN
    RAISE EXCEPTION 'joined hour must start canonical pending'; END IF;
  IF NEW.canonical_hour_id<>NEW.batch_id||'__recording-'||NEW.recording_id::text||'__date-'||NEW.local_date::text
      ||'__hour-'||lpad(NEW.delivery_hour::text,2,'0')||'__generation-'||NEW.generation::text THEN
    RAISE EXCEPTION 'joined hour canonical id differs from its frozen tuple'; END IF;
  IF distinct_jobs<>14 OR distinct_dates<>14 OR NOT dates_consecutive OR qualified_jobs<>14
    OR NEW.local_date<>ALL(NEW.authoritative_local_dates)
    OR NEW.authoritative_local_dates<>ARRAY(SELECT d FROM unnest(NEW.authoritative_local_dates) d ORDER BY d)
    OR (NEW.hour_start_at AT TIME ZONE NEW.local_timezone)::date<>NEW.local_date
    OR extract(hour FROM NEW.hour_start_at AT TIME ZONE NEW.local_timezone)::integer<>NEW.local_clock_hour
    OR (NEW.hour_start_at AT TIME ZONE NEW.local_timezone)::time<>make_time(NEW.local_clock_hour,0,0)
    OR NEW.delivery_start_at<>(NEW.authoritative_local_dates[1]::timestamp+time '08:00') AT TIME ZONE NEW.local_timezone
    OR NEW.delivery_end_at<>(NEW.authoritative_local_dates[14]::timestamp+time '20:00') AT TIME ZONE NEW.local_timezone
    OR NEW.hour_end_at<=NEW.delivery_start_at OR NEW.hour_start_at>=NEW.delivery_end_at THEN
    RAISE EXCEPTION 'joined hour is outside its authoritative delivery envelope'; END IF;
  SELECT * INTO scope FROM recording_joined_hours h WHERE h.connection_id=NEW.connection_id AND h.batch_id=NEW.batch_id
    AND h.recording_id=NEW.recording_id LIMIT 1;
  IF scope.id IS NOT NULL AND ROW(scope.delivery_start_at,scope.delivery_end_at,scope.local_timezone,
      scope.source_frozen_at,scope.authoritative_local_dates,scope.authoritative_recording_job_ids,scope.qualification_facts,
      scope.qualification_sha256,scope.qualification_policy_version)
    IS DISTINCT FROM ROW(NEW.delivery_start_at,NEW.delivery_end_at,NEW.local_timezone,
      NEW.source_frozen_at,NEW.authoritative_local_dates,NEW.authoritative_recording_job_ids,NEW.qualification_facts,
      NEW.qualification_sha256,NEW.qualification_policy_version) THEN
    RAISE EXCEPTION 'joined recording qualification scope differs'; END IF;
  SELECT count(*),min(h.batch_expected_hours),min(h.batch_expected_source_clips),min(h.batch_expected_source_bytes),min(h.frozen_denominator_sha256)
    INTO n,eh,ec,eb,m FROM recording_joined_hours h WHERE h.connection_id=NEW.connection_id AND h.batch_id=NEW.batch_id;
  IF n>0 AND (eh,ec,eb,m) IS DISTINCT FROM
    (NEW.batch_expected_hours,NEW.batch_expected_source_clips,NEW.batch_expected_source_bytes,NEW.frozen_denominator_sha256) THEN
    RAISE EXCEPTION 'joined batch denominator differs'; END IF;
  IF EXISTS(SELECT 1 FROM recording_joined_hours other WHERE other.connection_id=NEW.connection_id
      AND other.batch_id=NEW.batch_id AND other.batch_queue_order<>NEW.batch_queue_order) THEN
    RAISE EXCEPTION 'joined batch queue order differs'; END IF;
  IF EXISTS(SELECT 1 FROM recording_joined_hours other WHERE other.batch_id=NEW.batch_id
      AND (other.connection_id,other.account_id) IS DISTINCT FROM (NEW.connection_id,NEW.account_id)) THEN
    RAISE EXCEPTION 'joined batch id is already owned by another connection'; END IF;
  IF n>=NEW.batch_expected_hours THEN RAISE EXCEPTION 'joined batch is already sealed'; END IF;
  IF NOT EXISTS(SELECT 1 FROM connections c WHERE c.id=NEW.connection_id AND c.account_id=NEW.account_id AND c.kind='nas_pull')
    OR NOT EXISTS(SELECT 1 FROM recordings r WHERE r.id=NEW.recording_id AND r.account_id=NEW.account_id
      AND r.cron_timezone=NEW.local_timezone AND r.daily_window_start=time '08:00' AND r.daily_window_end=time '20:00') THEN
    RAISE EXCEPTION 'joined hour ownership differs'; END IF; RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_hour_insert_guard BEFORE INSERT ON recording_joined_hours FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_hour_insert();

CREATE FUNCTION guard_recording_joined_stream_day_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE h recording_joined_hours%ROWTYPE; DECLARE n BIGINT; DECLARE b BIGINT; DECLARE p BIGINT;
DECLARE first_id BIGINT; DECLARE first_start TIMESTAMPTZ; DECLARE last_id BIGINT;
DECLARE previous_id BIGINT; DECLARE previous_end TIMESTAMPTZ; DECLARE expected_verdict TEXT;
BEGIN
  SELECT * INTO h FROM recording_joined_hours WHERE connection_id=NEW.connection_id AND batch_id=NEW.batch_id
    AND recording_id=NEW.recording_id AND local_date=NEW.local_date LIMIT 1 FOR SHARE;
  SELECT count(*),COALESCE(sum(c.size_bytes),0),count(*) FILTER(WHERE c.purged_at IS NOT NULL) INTO n,b,p
    FROM recording_clips c WHERE c.recording_id=NEW.recording_id AND c.recording_job_id=NEW.recording_job_id AND c.created_at<=h.source_frozen_at
      AND c.clip_end_at>h.delivery_start_at AND c.clip_start_at<h.delivery_end_at;
  SELECT c.id,c.clip_start_at INTO first_id,first_start FROM recording_clips c WHERE c.recording_id=NEW.recording_id
    AND c.recording_job_id=NEW.recording_job_id AND c.created_at<=h.source_frozen_at AND c.clip_end_at>h.delivery_start_at AND c.clip_start_at<h.delivery_end_at
    ORDER BY c.clip_start_at,c.id LIMIT 1;
  SELECT c.id INTO last_id FROM recording_clips c WHERE c.recording_id=NEW.recording_id
    AND c.recording_job_id=NEW.recording_job_id AND c.created_at<=h.source_frozen_at AND c.clip_end_at>h.delivery_start_at AND c.clip_start_at<h.delivery_end_at
    ORDER BY c.clip_start_at DESC,c.id DESC LIMIT 1;
  SELECT c.id,c.clip_end_at INTO previous_id,previous_end FROM recording_clips c
    WHERE c.recording_id=NEW.recording_id AND c.created_at<=h.source_frozen_at AND c.recording_job_id=ANY(h.authoritative_recording_job_ids[1:
      array_position(h.authoritative_local_dates,NEW.local_date)-1])
      AND c.clip_end_at>h.delivery_start_at AND c.clip_start_at<h.delivery_end_at
    ORDER BY c.clip_start_at DESC,c.id DESC LIMIT 1;
  expected_verdict=CASE WHEN n=0 THEN 'no_sources' WHEN previous_id IS NULL THEN 'first'
    ELSE NEW.cross_day_facts->>'verdict' END;
  IF h.id IS NULL OR h.state<>'pending' OR (h.account_id,h.connection_id,h.batch_id) IS DISTINCT FROM
      (NEW.account_id,NEW.connection_id,NEW.batch_id) OR NEW.recording_job_id<>h.authoritative_recording_job_ids[
        array_position(h.authoritative_local_dates,NEW.local_date)]
    OR n<>NEW.source_clip_count OR b<>NEW.source_bytes OR p<>0
    OR ROW(NEW.first_source_clip_id,NEW.last_source_clip_id,NEW.previous_frozen_clip_id,NEW.cross_day_facts->>'verdict')
      IS DISTINCT FROM ROW(first_id,last_id,previous_id,expected_verdict)
    OR (NEW.cross_day_facts->>'verdict'='exact' AND previous_end<>first_start)
    OR (NEW.adjacency_facts->>'pair_count')::integer<0
    OR (NEW.adjacency_facts->>'pair_count')::integer<>greatest(NEW.source_clip_count-1,0) THEN
    RAISE EXCEPTION 'joined stream-day ledger differs from frozen recording day'; END IF; RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_stream_day_insert_guard BEFORE INSERT ON recording_joined_stream_days
FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_stream_day_insert();

CREATE FUNCTION guard_recording_joined_hour_source() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE h recording_joined_hours%ROWTYPE; DECLARE d recording_joined_stream_days%ROWTYPE;
DECLARE previous recording_joined_hour_sources%ROWTYPE; DECLARE expected_boundary TEXT;
BEGIN
  SELECT * INTO h FROM recording_joined_hours WHERE id=NEW.hour_id FOR SHARE;
  SELECT * INTO d FROM recording_joined_stream_days WHERE id=NEW.stream_day_id FOR SHARE;
  IF NEW.previous_clip_id IS NOT NULL THEN
    SELECT * INTO previous FROM recording_joined_hour_sources WHERE connection_id=NEW.connection_id
      AND batch_id=NEW.batch_id AND clip_id=NEW.previous_clip_id;
    IF previous.clip_id IS NULL THEN RAISE EXCEPTION 'joined source predecessor is missing'; END IF;
    expected_boundary=CASE WHEN previous.stream_day_id<>NEW.stream_day_id THEN 'cross_day'
      WHEN previous.hour_id<>NEW.hour_id THEN 'cross_hour' ELSE 'within_hour' END;
  ELSE expected_boundary='first'; END IF;
  IF h.id IS NULL OR h.state<>'pending' OR h.attempt_count<>0
    OR (h.account_id,h.connection_id,h.batch_id) IS DISTINCT FROM (NEW.account_id,NEW.connection_id,NEW.batch_id)
    OR d.id IS NULL OR (d.account_id,d.connection_id,d.recording_id,d.batch_id,d.local_date,d.recording_job_id)
      IS DISTINCT FROM (h.account_id,h.connection_id,h.recording_id,h.batch_id,h.local_date,NEW.recording_job_id)
    OR NEW.adjacency_facts->>'boundary'<>expected_boundary
    OR (NEW.allocation_facts->>'crosses_hour_start')::boolean<>(NEW.clip_start_at<h.hour_start_at)
    OR (NEW.allocation_facts->>'crosses_hour_end')::boolean<>(NEW.clip_end_at>h.hour_end_at)
    OR (NEW.stream_day_clip_ordinal=1 AND (NEW.clip_id<>d.first_source_clip_id
      OR NEW.previous_clip_id IS NOT NULL OR NEW.adjacency_facts->>'verdict'<>'first'))
    OR (NEW.stream_day_clip_ordinal>1 AND (NEW.previous_clip_id IS NULL
      OR previous.stream_day_id<>NEW.stream_day_id OR previous.stream_day_clip_ordinal<>NEW.stream_day_clip_ordinal-1))
    OR (NEW.adjacency_facts->>'verdict'='exact' AND previous.clip_end_at<>NEW.clip_start_at)
    OR (NEW.adjacency_facts->>'verdict'='first')<>(NEW.previous_clip_id IS NULL)
    OR (NEW.previous_clip_id IS NULL AND (NEW.adjacency_facts->'previous_clip_id'<>'null'::jsonb
      OR NEW.adjacency_facts->'previous_presentation_end_utc'<>'null'::jsonb
      OR NEW.adjacency_facts->'signed_gap_nanoseconds'<>'null'::jsonb
      OR (NEW.adjacency_facts->>'next_clip_id')::bigint<>NEW.clip_id
      OR (NEW.adjacency_facts->>'next_presentation_start_utc')::timestamptz<>NEW.clip_start_at))
    OR (NEW.previous_clip_id IS NOT NULL AND (
      (NEW.adjacency_facts->>'previous_clip_id')::bigint<>NEW.previous_clip_id
      OR (NEW.adjacency_facts->>'next_clip_id')::bigint<>NEW.clip_id
      OR (NEW.adjacency_facts->>'previous_presentation_end_utc')::timestamptz<>previous.clip_end_at
      OR (NEW.adjacency_facts->>'next_presentation_start_utc')::timestamptz<>NEW.clip_start_at
      OR (NEW.adjacency_facts->>'signed_gap_nanoseconds')::bigint<>
        round(extract(epoch FROM (NEW.clip_start_at-previous.clip_end_at))*1000000000)::bigint))
    OR NOT EXISTS(SELECT 1 FROM recording_clips c JOIN recordings r ON r.id=c.recording_id
      JOIN storage_destinations sd ON sd.id=c.storage_destination_id
      WHERE c.id=NEW.clip_id AND c.recording_id=h.recording_id AND r.account_id=h.account_id
        AND c.recording_job_id=NEW.recording_job_id AND NEW.recording_job_id=ANY(h.authoritative_recording_job_ids)
        AND c.storage_destination_id=NEW.storage_destination_id
        AND sd.provider=NEW.provider AND c.endpoint=NEW.endpoint AND sd.region=NEW.region
        AND c.bucket=NEW.bucket AND c.object_key=NEW.object_key
        AND c.size_bytes=NEW.size_bytes AND lower(c.sha256)=NEW.sha256 AND btrim(btrim(c.etag),'"')=NEW.etag
        AND c.clip_start_at=NEW.clip_start_at AND c.clip_end_at=NEW.clip_end_at AND c.released_at IS NOT DISTINCT FROM NEW.released_at
        AND c.created_at<=h.source_frozen_at AND c.clip_end_at>h.hour_start_at AND c.clip_start_at<h.hour_end_at
        AND c.clip_end_at>h.delivery_start_at AND c.clip_start_at<h.delivery_end_at) THEN
    RAISE EXCEPTION 'joined hour source differs from frozen clip'; END IF; RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_hour_source_guard BEFORE INSERT ON recording_joined_hour_sources FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_hour_source();

CREATE FUNCTION guard_recording_joined_output_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE h recording_joined_hours%ROWTYPE; DECLARE d recording_joined_stream_days%ROWTYPE;
BEGIN
  IF NEW.artifact_kind='allocation_ledger' THEN
    SELECT * INTO d FROM recording_joined_stream_days WHERE id=NEW.stream_day_id FOR SHARE;
    IF d.id IS NULL OR d.state<>'sealed' OR NEW.state<>'planned'
      OR (d.account_id,d.connection_id,d.batch_id) IS DISTINCT FROM (NEW.account_id,NEW.connection_id,NEW.batch_id)
      OR NEW.hour_seal_token<>d.seal_token OR NEW.expected_sha256<>d.adjacency_manifest_sha256 OR NEW.content_id<>d.adjacency_manifest_sha256
      OR NEW.source_clip_count<>d.source_clip_count OR NEW.source_bytes<>d.source_bytes
      OR NEW.plan_facts->>'recording_id'<>d.recording_id::text OR NEW.plan_facts->>'local_date'<>d.local_date::text
      OR NOT EXISTS(SELECT 1 FROM recording_joined_hours bounds WHERE bounds.connection_id=d.connection_id
        AND bounds.batch_id=d.batch_id AND bounds.recording_id=d.recording_id AND bounds.local_date=d.local_date
        GROUP BY bounds.connection_id HAVING min(bounds.hour_start_at)=NEW.coverage_start_at
          AND max(bounds.hour_end_at)=NEW.coverage_end_at) THEN
      RAISE EXCEPTION 'joined allocation ledger differs from sealed stream day'; END IF;
  ELSIF NEW.artifact_kind='batch_index' THEN
    IF NEW.state<>'planned' OR NOT EXISTS(SELECT 1 FROM recording_joined_hours scope
        WHERE scope.connection_id=NEW.connection_id AND scope.batch_id=NEW.batch_id
          AND scope.account_id=NEW.account_id)
      OR EXISTS(SELECT 1 FROM recording_joined_hours scope WHERE scope.connection_id=NEW.connection_id
        AND scope.batch_id=NEW.batch_id AND scope.state<>'published')
      OR NEW.plan_facts->>'frozen_denominator_sha256'<>(SELECT min(frozen_denominator_sha256) FROM recording_joined_hours
        WHERE connection_id=NEW.connection_id AND batch_id=NEW.batch_id)
      OR NEW.plan_facts->>'expected_hours'<>(SELECT min(batch_expected_hours)::text FROM recording_joined_hours
        WHERE connection_id=NEW.connection_id AND batch_id=NEW.batch_id)
      OR NOT joined_batch_is_sealed(NEW.connection_id,NEW.batch_id,
        (SELECT min(batch_expected_hours) FROM recording_joined_hours WHERE connection_id=NEW.connection_id AND batch_id=NEW.batch_id),
        (SELECT min(batch_expected_source_clips) FROM recording_joined_hours WHERE connection_id=NEW.connection_id AND batch_id=NEW.batch_id),
        (SELECT min(batch_expected_source_bytes) FROM recording_joined_hours WHERE connection_id=NEW.connection_id AND batch_id=NEW.batch_id),
        (SELECT min(frozen_denominator_sha256) FROM recording_joined_hours WHERE connection_id=NEW.connection_id AND batch_id=NEW.batch_id)) THEN
      RAISE EXCEPTION 'joined batch index requires a fully published sealed batch'; END IF;
  ELSE
    SELECT * INTO h FROM recording_joined_hours WHERE id=NEW.hour_id FOR UPDATE;
    IF h.id IS NULL OR h.state<>'leased' OR h.lease_expires_at<=now() OR h.sealed_at IS NOT NULL
      OR NEW.hour_seal_token<>h.claim_token
      OR (h.account_id,h.connection_id,h.batch_id) IS DISTINCT FROM (NEW.account_id,NEW.connection_id,NEW.batch_id)
      OR (NEW.artifact_kind='media' AND NEW.source_claim_sha256<>h.source_manifest_sha256)
      OR NEW.state<>'planned' THEN RAISE EXCEPTION 'joined artifact requires active unsealed hour lease'; END IF;
  END IF; RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_output_insert_guard BEFORE INSERT ON recording_joined_outputs FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_output_insert();

CREATE FUNCTION guard_recording_joined_output_source() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE o recording_joined_outputs%ROWTYPE; DECLARE h recording_joined_hours%ROWTYPE; DECLARE a recording_joined_hour_sources%ROWTYPE;
DECLARE prior_clip_id BIGINT;
BEGIN
  SELECT * INTO o FROM recording_joined_outputs WHERE id=NEW.output_id FOR SHARE;
  SELECT * INTO h FROM recording_joined_hours WHERE id=NEW.hour_id FOR SHARE;
  SELECT * INTO a FROM recording_joined_hour_sources WHERE hour_id=NEW.hour_id AND clip_id=NEW.clip_id;
  IF o.id IS NULL OR o.artifact_kind<>'media' OR h.id IS NULL OR o.hour_id<>h.id OR o.state<>'planned' OR h.state<>'leased'
    OR o.hour_seal_token IS DISTINCT FROM h.claim_token OR h.sealed_at IS NOT NULL OR h.lease_expires_at<=now()
    OR a.clip_id IS NULL OR a.ordinal<>NEW.hour_source_ordinal OR o.source_claim_sha256<>h.source_manifest_sha256 THEN
    RAISE EXCEPTION 'joined output source is not allocated under active hour lease'; END IF;
  SELECT clip_id INTO prior_clip_id FROM recording_joined_output_sources
    WHERE output_id=NEW.output_id AND ordinal=NEW.ordinal-1;
  IF (NEW.ordinal=1 AND NEW.seam_facts->>'verdict' IS DISTINCT FROM 'not_applicable')
    OR (NEW.ordinal>1 AND (NEW.seam_facts->>'verdict' IS DISTINCT FROM 'exact'
      OR prior_clip_id IS NULL OR a.previous_clip_id IS DISTINCT FROM prior_clip_id
      OR a.adjacency_facts->>'verdict' IS DISTINCT FROM 'exact')) THEN
    RAISE EXCEPTION 'joined output seam verdict differs from source position'; END IF; RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_output_source_guard BEFORE INSERT ON recording_joined_output_sources FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_output_source();

-- A planned hour artifact may never commit without the same transaction
-- sealing its complete hour manifest. This makes partial plans crash-safe.
CREATE FUNCTION guard_recording_joined_plan_commit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.artifact_kind NOT IN('allocation_ledger','batch_index') AND NOT EXISTS(SELECT 1 FROM recording_joined_hours h WHERE h.id=NEW.hour_id AND h.state='sealed'
      AND h.seal_token=NEW.hour_seal_token AND h.sealed_at IS NOT NULL
      AND h.planned_output_count IS NOT NULL AND h.hour_manifest_sha256 IS NOT NULL) THEN
    RAISE EXCEPTION 'joined output plan must commit with its token-fenced hour seal'; END IF;
  RETURN NEW;
END $$;
CREATE CONSTRAINT TRIGGER recording_joined_output_plan_atomic AFTER INSERT ON recording_joined_outputs
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_plan_commit();

CREATE FUNCTION guard_recording_joined_batch_index_commit() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expected_hours INTEGER; DECLARE expected_clips BIGINT; DECLARE expected_bytes BIGINT;
DECLARE ledger_count BIGINT; DECLARE hour_count BIGINT; DECLARE indexed_clips BIGINT; DECLARE indexed_bytes BIGINT;
BEGIN
  IF NEW.artifact_kind<>'batch_index' THEN RETURN NEW; END IF;
  SELECT min(batch_expected_hours),min(batch_expected_source_clips),min(batch_expected_source_bytes)
    INTO expected_hours,expected_clips,expected_bytes FROM recording_joined_hours
    WHERE connection_id=NEW.connection_id AND batch_id=NEW.batch_id;
  SELECT count(*) INTO ledger_count FROM recording_joined_batch_index_ledgers refs
    JOIN recording_joined_stream_days d ON d.id=refs.stream_day_id
    JOIN recording_joined_outputs ledger ON ledger.id=refs.ledger_output_id
    WHERE refs.batch_index_output_id=NEW.id AND d.connection_id=NEW.connection_id AND d.batch_id=NEW.batch_id
      AND d.state='published' AND ledger.stream_day_id=d.id AND ledger.artifact_kind='allocation_ledger'
      AND ledger.state='published';
  SELECT count(*),COALESCE(sum(h.source_clip_count),0),COALESCE(sum(h.source_bytes),0)
    INTO hour_count,indexed_clips,indexed_bytes FROM recording_joined_batch_index_hours refs
    JOIN recording_joined_hours h ON h.id=refs.hour_id
    JOIN recording_joined_outputs manifest ON manifest.id=refs.hour_manifest_output_id
    WHERE refs.batch_index_output_id=NEW.id AND h.connection_id=NEW.connection_id AND h.batch_id=NEW.batch_id
      AND h.state='published' AND manifest.hour_id=h.id AND manifest.artifact_kind='hour_manifest'
      AND manifest.state='published';
  IF expected_hours IS NULL OR NOT joined_batch_index_refs_complete(NEW.id)
    OR ledger_count<>expected_hours/12 OR hour_count<>expected_hours
    OR indexed_clips<>expected_clips OR indexed_bytes<>expected_bytes
    OR NEW.plan_facts->>'expected_ledgers'<>(expected_hours/12)::text
    OR NEW.plan_facts->>'expected_hours'<>expected_hours::text
    OR NEW.plan_facts->>'source_clip_count'<>expected_clips::text
    OR NEW.plan_facts->>'source_bytes'<>expected_bytes::text THEN
    RAISE EXCEPTION 'joined batch index does not bind the complete frozen artifact set'; END IF;
  RETURN NEW;
END $$;
CREATE CONSTRAINT TRIGGER recording_joined_batch_index_atomic AFTER INSERT ON recording_joined_outputs
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_batch_index_commit();

CREATE FUNCTION guard_recording_joined_hour_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE sc BIGINT; DECLARE sb BIGINT; DECLARE oc BIGINT; DECLARE hm BIGINT; DECLARE osc BIGINT; DECLARE osb BIGINT;
DECLARE first_part INTEGER; DECLARE last_part INTEGER;
DECLARE authoritative_clips BIGINT; DECLARE authoritative_bytes BIGINT; DECLARE purged_clips BIGINT;
DECLARE allocated_clips BIGINT; DECLARE allocated_bytes BIGINT;
DECLARE ledger_days BIGINT; DECLARE ledger_clips BIGINT; DECLARE ledger_bytes BIGINT;
DECLARE disposition_count BIGINT; DECLARE included_count BIGINT; DECLARE quarantined_count BIGINT;
BEGIN
  IF ROW(NEW.account_id,NEW.connection_id,NEW.recording_id,NEW.batch_id,NEW.batch_expected_hours,NEW.batch_expected_source_clips,
      NEW.batch_expected_source_bytes,NEW.frozen_denominator_sha256,NEW.source_frozen_at,NEW.local_date,NEW.delivery_hour,NEW.local_clock_hour,NEW.local_timezone,
      NEW.hour_start_at,NEW.hour_end_at,NEW.batch_queue_order,NEW.priority_tier,NEW.priority_order,NEW.priority_facts,
      NEW.source_clip_count,NEW.generation,NEW.canonical_hour_id,
      NEW.source_bytes,NEW.source_manifest_sha256,NEW.delivery_start_at,NEW.delivery_end_at,
      NEW.authoritative_local_dates,NEW.authoritative_recording_job_ids,NEW.qualification_facts,
      NEW.qualification_sha256,NEW.qualification_policy_version,NEW.created_at)
    IS DISTINCT FROM ROW(OLD.account_id,OLD.connection_id,OLD.recording_id,OLD.batch_id,OLD.batch_expected_hours,
      OLD.batch_expected_source_clips,OLD.batch_expected_source_bytes,OLD.frozen_denominator_sha256,OLD.source_frozen_at,OLD.local_date,OLD.delivery_hour,OLD.local_clock_hour,
      OLD.local_timezone,OLD.hour_start_at,OLD.hour_end_at,OLD.batch_queue_order,OLD.priority_tier,OLD.priority_order,OLD.priority_facts,
      OLD.source_clip_count,OLD.generation,OLD.canonical_hour_id,OLD.source_bytes,OLD.source_manifest_sha256,OLD.delivery_start_at,OLD.delivery_end_at,
      OLD.authoritative_local_dates,OLD.authoritative_recording_job_ids,OLD.qualification_facts,
      OLD.qualification_sha256,OLD.qualification_policy_version,OLD.created_at) THEN
    RAISE EXCEPTION 'joined hour identity is immutable'; END IF;
  IF OLD.state='pending' AND NEW.state='leased' THEN
    SELECT count(*),COALESCE(sum(size_bytes),0) INTO sc,sb FROM recording_joined_hour_sources WHERE hour_id=OLD.id;
    SELECT count(*),COALESCE(sum(c.size_bytes),0),count(*) FILTER(WHERE c.purged_at IS NOT NULL)
      INTO authoritative_clips,authoritative_bytes,purged_clips FROM recording_clips c
      WHERE c.recording_id=OLD.recording_id AND c.created_at<=OLD.source_frozen_at AND c.recording_job_id=ANY(OLD.authoritative_recording_job_ids)
        AND c.clip_end_at>OLD.delivery_start_at AND c.clip_start_at<OLD.delivery_end_at;
    SELECT count(*),COALESCE(sum(src.size_bytes),0) INTO allocated_clips,allocated_bytes
      FROM recording_joined_hour_sources src JOIN recording_joined_hours allocated_hour ON allocated_hour.id=src.hour_id
      WHERE allocated_hour.connection_id=OLD.connection_id AND allocated_hour.batch_id=OLD.batch_id
        AND allocated_hour.recording_id=OLD.recording_id;
    SELECT count(*),COALESCE(sum(source_clip_count),0),COALESCE(sum(source_bytes),0)
      INTO ledger_days,ledger_clips,ledger_bytes FROM recording_joined_stream_days d
      WHERE d.connection_id=OLD.connection_id AND d.batch_id=OLD.batch_id AND d.recording_id=OLD.recording_id
        AND d.local_date=ANY(OLD.authoritative_local_dates) AND d.recording_job_id=ANY(OLD.authoritative_recording_job_ids);
    IF (SELECT count(*) FROM recording_joined_hours scope_hours WHERE scope_hours.connection_id=OLD.connection_id
          AND scope_hours.batch_id=OLD.batch_id AND scope_hours.recording_id=OLD.recording_id)<>168
      OR ledger_days<>14 OR ledger_clips<>authoritative_clips OR ledger_bytes<>authoritative_bytes
      OR purged_clips<>0 OR allocated_clips<>authoritative_clips OR allocated_bytes<>authoritative_bytes
      OR EXISTS(SELECT 1 FROM recording_joined_stream_days d WHERE d.connection_id=OLD.connection_id
        AND d.batch_id=OLD.batch_id AND d.recording_id=OLD.recording_id AND (
          (SELECT count(*) FROM recording_joined_hour_sources src WHERE src.stream_day_id=d.id)<>d.source_clip_count
          OR (d.source_clip_count>0 AND NOT EXISTS(SELECT 1 FROM recording_joined_hour_sources src
            WHERE src.stream_day_id=d.id AND src.stream_day_clip_ordinal=1 AND src.clip_id=d.first_source_clip_id))
          OR (d.source_clip_count>0 AND NOT EXISTS(SELECT 1 FROM recording_joined_hour_sources src
            WHERE src.stream_day_id=d.id AND src.stream_day_clip_ordinal=d.source_clip_count AND src.clip_id=d.last_source_clip_id))))
      OR NOT joined_batch_is_sealed(OLD.connection_id,OLD.batch_id,OLD.batch_expected_hours,OLD.batch_expected_source_clips,
        OLD.batch_expected_source_bytes,OLD.frozen_denominator_sha256) OR sc<>OLD.source_clip_count OR sb<>OLD.source_bytes
      OR OLD.next_attempt_at>now() OR NEW.attempt_count<>OLD.attempt_count+1 OR NEW.lease_expires_at<=now()
      OR NEW.next_attempt_at<>OLD.next_attempt_at OR NEW.last_reason_code<>OLD.last_reason_code
      OR NEW.sealed_at IS DISTINCT FROM OLD.sealed_at OR NEW.planned_output_count IS DISTINCT FROM OLD.planned_output_count
      OR NEW.hour_manifest_sha256 IS DISTINCT FROM OLD.hour_manifest_sha256 OR NEW.seal_token IS DISTINCT FROM OLD.seal_token
      OR NEW.quarantine_reason_code<>OLD.quarantine_reason_code THEN RAISE EXCEPTION 'invalid joined hour claim'; END IF;
  ELSIF OLD.state='leased' AND NEW.state='sealed' THEN
      SELECT count(*),min(part_ordinal),max(part_ordinal) INTO oc,first_part,last_part FROM recording_joined_outputs
        WHERE hour_id=OLD.id AND artifact_kind='media';
      SELECT count(*) INTO hm FROM recording_joined_outputs manifest WHERE manifest.hour_id=OLD.id AND manifest.artifact_kind='hour_manifest'
        AND manifest.expected_sha256=NEW.hour_manifest_sha256 AND manifest.source_clip_count=OLD.source_clip_count
        AND manifest.source_bytes=OLD.source_bytes AND manifest.coverage_start_at=OLD.hour_start_at AND manifest.coverage_end_at=OLD.hour_end_at
        AND manifest.plan_facts->>'quarantine_reason_code'=NEW.quarantine_reason_code
        AND manifest.plan_facts->>'batch_id'=OLD.batch_id AND manifest.plan_facts->>'hour_id'=OLD.canonical_hour_id
        AND manifest.plan_facts->>'source_manifest_sha256'=OLD.source_manifest_sha256
        AND manifest.plan_facts->>'local_date'=OLD.local_date::text AND manifest.plan_facts->>'delivery_hour'=OLD.delivery_hour::text
        AND EXISTS(SELECT 1 FROM recording_joined_stream_days d JOIN recording_joined_outputs ledger
          ON ledger.stream_day_id=d.id AND ledger.artifact_kind='allocation_ledger'
          WHERE d.connection_id=OLD.connection_id AND d.batch_id=OLD.batch_id AND d.recording_id=OLD.recording_id
            AND d.local_date=OLD.local_date AND manifest.plan_facts->>'ledger_artifact_id'=ledger.id::text
            AND manifest.plan_facts->>'ledger_relative_path'=ledger.relative_path
            AND manifest.plan_facts->>'ledger_sha256'=ledger.expected_sha256);
      SELECT count(*),COALESCE(sum(s.size_bytes),0) INTO osc,osb FROM recording_joined_output_sources os
        JOIN recording_joined_hour_sources s ON s.hour_id=os.hour_id AND s.clip_id=os.clip_id WHERE os.hour_id=OLD.id;
      SELECT count(*),count(*) FILTER(WHERE disposition='included'),count(*) FILTER(WHERE disposition='quarantined')
        INTO disposition_count,included_count,quarantined_count FROM recording_joined_hour_source_dispositions WHERE hour_id=OLD.id;
      IF NEW.attempt_count<>OLD.attempt_count OR NEW.next_attempt_at<>OLD.next_attempt_at
        OR NEW.last_reason_code<>OLD.last_reason_code OR OLD.lease_expires_at<=now() OR NEW.planned_output_count<>oc OR hm<>1
        OR NEW.seal_token<>OLD.claim_token
        OR (oc=0 AND OLD.source_clip_count>0 AND NEW.quarantine_reason_code='')
        OR (oc>0 AND (OLD.source_clip_count=0 OR NEW.quarantine_reason_code<>''))
        OR (OLD.source_clip_count=0 AND NEW.quarantine_reason_code<>'')
        OR (oc>0 AND (first_part<>1 OR last_part<>oc))
        OR disposition_count<>OLD.source_clip_count OR (oc>0 AND (osc<>included_count OR included_count=0))
        OR (oc=0 AND OLD.source_clip_count>0 AND quarantined_count<>OLD.source_clip_count)
        OR EXISTS(SELECT 1 FROM recording_joined_outputs media WHERE media.hour_id=OLD.id AND media.artifact_kind='media'
          AND (NOT media.plan_facts?'maximality_evidence' OR jsonb_typeof(media.plan_facts->'maximality_evidence')<>'array'
            OR (media.part_ordinal<oc AND jsonb_array_length(media.plan_facts->'maximality_evidence')=0)))
        OR (oc=0 AND (osc<>0 OR osb<>0))
        OR EXISTS(SELECT 1 FROM recording_joined_outputs WHERE hour_id=OLD.id AND state<>'planned') THEN
        RAISE EXCEPTION 'invalid joined hour seal'; END IF;
  ELSIF OLD.state='leased' AND NEW.state='leased' THEN
    IF OLD.lease_expires_at<=now() THEN
      IF NEW.claim_token=OLD.claim_token OR NEW.attempt_count<>OLD.attempt_count+1 OR NEW.heartbeat_at<=OLD.heartbeat_at
        OR NEW.lease_expires_at<=now() OR NEW.sealed_at IS DISTINCT FROM OLD.sealed_at
        OR NEW.planned_output_count IS DISTINCT FROM OLD.planned_output_count
        OR NEW.hour_manifest_sha256 IS DISTINCT FROM OLD.hour_manifest_sha256 OR NEW.seal_token IS DISTINCT FROM OLD.seal_token
        OR NEW.quarantine_reason_code<>OLD.quarantine_reason_code THEN RAISE EXCEPTION 'invalid joined hour reclaim'; END IF;
    ELSIF (NEW.claim_token,NEW.claimed_by,NEW.attempt_count,NEW.sealed_at,NEW.planned_output_count,NEW.hour_manifest_sha256,
      NEW.seal_token,NEW.quarantine_reason_code)
      IS DISTINCT FROM (OLD.claim_token,OLD.claimed_by,OLD.attempt_count,OLD.sealed_at,OLD.planned_output_count,OLD.hour_manifest_sha256,
      OLD.seal_token,OLD.quarantine_reason_code)
	      OR NEW.heartbeat_at<=OLD.heartbeat_at OR NEW.lease_expires_at<OLD.lease_expires_at THEN
      RAISE EXCEPTION 'joined hour lease is fenced'; END IF;
  ELSIF OLD.state='sealed' AND NEW.state='publishing' THEN
    IF NEW.publish_attempt_count<>OLD.publish_attempt_count+1 OR NEW.publish_lease_expires_at<=now()
      OR NEW.attempt_count<>OLD.attempt_count OR NEW.sealed_at IS DISTINCT FROM OLD.sealed_at
      OR NEW.planned_output_count IS DISTINCT FROM OLD.planned_output_count
      OR NEW.hour_manifest_sha256 IS DISTINCT FROM OLD.hour_manifest_sha256 OR NEW.seal_token IS DISTINCT FROM OLD.seal_token
      OR NEW.quarantine_reason_code<>OLD.quarantine_reason_code OR NEW.last_reason_code<>OLD.last_reason_code THEN
      RAISE EXCEPTION 'invalid joined hour publication claim'; END IF;
  ELSIF OLD.state='publishing' AND NEW.state='publishing' THEN
    IF OLD.publish_lease_expires_at<=now() THEN
      IF NEW.publish_claim_token=OLD.publish_claim_token OR NEW.publish_attempt_count<>OLD.publish_attempt_count+1
        OR NEW.publish_heartbeat_at<=OLD.publish_heartbeat_at OR NEW.publish_lease_expires_at<=now() THEN
        RAISE EXCEPTION 'invalid joined hour publication reclaim'; END IF;
    ELSIF NEW.publish_claim_token<>OLD.publish_claim_token OR NEW.publish_claimed_by<>OLD.publish_claimed_by
      OR NEW.publish_attempt_count<>OLD.publish_attempt_count OR NEW.publish_heartbeat_at<=OLD.publish_heartbeat_at
	      OR NEW.publish_lease_expires_at<OLD.publish_lease_expires_at THEN
      RAISE EXCEPTION 'joined hour publication lease is fenced'; END IF;
    IF NEW.attempt_count<>OLD.attempt_count OR NEW.sealed_at IS DISTINCT FROM OLD.sealed_at
      OR NEW.planned_output_count IS DISTINCT FROM OLD.planned_output_count
      OR NEW.hour_manifest_sha256 IS DISTINCT FROM OLD.hour_manifest_sha256 OR NEW.seal_token IS DISTINCT FROM OLD.seal_token
      OR NEW.quarantine_reason_code<>OLD.quarantine_reason_code THEN
      RAISE EXCEPTION 'joined sealed hour identity changed'; END IF;
  ELSIF OLD.state='publishing' AND NEW.state='published' THEN
    SELECT count(*) INTO oc FROM recording_joined_outputs WHERE hour_id=OLD.id AND state='published';
    IF OLD.publish_lease_expires_at<=now() OR OLD.sealed_at IS NULL OR oc<>OLD.planned_output_count+1
      OR NEW.attempt_count<>OLD.attempt_count OR NEW.sealed_at IS DISTINCT FROM OLD.sealed_at
      OR NEW.planned_output_count IS DISTINCT FROM OLD.planned_output_count
      OR NEW.hour_manifest_sha256 IS DISTINCT FROM OLD.hour_manifest_sha256 OR NEW.seal_token IS DISTINCT FROM OLD.seal_token
      OR NEW.quarantine_reason_code<>OLD.quarantine_reason_code
      OR NEW.last_reason_code<>'' OR NEW.published_at<OLD.sealed_at THEN RAISE EXCEPTION 'invalid joined hour publish'; END IF;
  ELSIF OLD.state='publishing' AND NEW.state='sealed' THEN
    IF NEW.publish_attempt_count<>OLD.publish_attempt_count OR NEW.last_reason_code='' OR NEW.next_attempt_at<now()
      OR NEW.attempt_count<>OLD.attempt_count OR NEW.sealed_at IS DISTINCT FROM OLD.sealed_at
      OR NEW.planned_output_count IS DISTINCT FROM OLD.planned_output_count
      OR NEW.hour_manifest_sha256 IS DISTINCT FROM OLD.hour_manifest_sha256 OR NEW.seal_token IS DISTINCT FROM OLD.seal_token
      OR NEW.quarantine_reason_code<>OLD.quarantine_reason_code THEN
      RAISE EXCEPTION 'invalid joined hour publication release'; END IF;
  ELSIF OLD.state='leased' AND NEW.state IN('pending','failed') THEN
    IF NEW.attempt_count<>OLD.attempt_count OR NEW.last_reason_code='' OR NEW.next_attempt_at<now()
      OR (NEW.state='pending' AND OLD.attempt_count>=100) OR NEW.sealed_at IS DISTINCT FROM OLD.sealed_at
      OR NEW.planned_output_count IS DISTINCT FROM OLD.planned_output_count
      OR NEW.hour_manifest_sha256 IS DISTINCT FROM OLD.hour_manifest_sha256
      OR NEW.seal_token IS DISTINCT FROM OLD.seal_token OR NEW.quarantine_reason_code<>OLD.quarantine_reason_code
      THEN RAISE EXCEPTION 'invalid joined hour finalize'; END IF;
  ELSE RAISE EXCEPTION 'invalid joined hour state transition'; END IF;
  NEW.updated_at=now(); RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_hour_mutation_guard BEFORE UPDATE ON recording_joined_hours FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_hour_mutation();

CREATE FUNCTION guard_recording_joined_output_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE h recording_joined_hours%ROWTYPE; DECLARE n BIGINT; DECLARE b BIGINT; DECLARE s TIMESTAMPTZ; DECLARE e TIMESTAMPTZ;
DECLARE min_hour_source INTEGER; DECLARE max_hour_source INTEGER; DECLARE source_order INTEGER[]; DECLARE canonical_source_order INTEGER[];
DECLARE min_source INTEGER; DECLARE max_source INTEGER; DECLARE temporal_gaps BIGINT;
BEGIN
  IF ROW(NEW.hour_id,NEW.stream_day_id,NEW.account_id,NEW.connection_id,NEW.batch_id,NEW.artifact_kind,NEW.content_type,
      NEW.hour_seal_token,NEW.part_ordinal,NEW.relative_path,NEW.content_id,NEW.expected_size_bytes,NEW.expected_sha256,
      NEW.object_key,NEW.source_claim_sha256,NEW.source_clip_count,NEW.source_bytes,
      NEW.coverage_start_at,NEW.coverage_end_at,NEW.plan_facts,NEW.created_at)
    IS DISTINCT FROM ROW(OLD.hour_id,OLD.stream_day_id,OLD.account_id,OLD.connection_id,OLD.batch_id,OLD.artifact_kind,OLD.content_type,
      OLD.hour_seal_token,OLD.part_ordinal,OLD.relative_path,OLD.content_id,OLD.expected_size_bytes,OLD.expected_sha256,
      OLD.object_key,OLD.source_claim_sha256,OLD.source_clip_count,OLD.source_bytes,
      OLD.coverage_start_at,OLD.coverage_end_at,OLD.plan_facts,OLD.created_at) THEN RAISE EXCEPTION 'joined output identity is immutable'; END IF;
  IF OLD.state='planned' AND NEW.state='published' THEN
    IF NEW.nas_verified_at IS NOT NULL OR NEW.size_bytes<>OLD.expected_size_bytes OR NEW.sha256<>OLD.expected_sha256 THEN
      RAISE EXCEPTION 'published joined artifact differs from planned bytes'; END IF;
    IF OLD.artifact_kind IN('media','hour_manifest') THEN
      SELECT * INTO h FROM recording_joined_hours WHERE id=OLD.hour_id FOR SHARE;
      IF h.state<>'publishing' OR h.sealed_at IS NULL OR h.publish_lease_expires_at<=now() OR h.seal_token<>OLD.hour_seal_token THEN
        RAISE EXCEPTION 'joined hour artifact publish is not token fenced'; END IF;
    END IF;
    IF OLD.artifact_kind='media' THEN
      SELECT count(*),COALESCE(sum(src.size_bytes),0),min(src.clip_start_at),max(src.clip_end_at),
        min(os.hour_source_ordinal),max(os.hour_source_ordinal),min(os.ordinal),max(os.ordinal),
        array_agg(os.hour_source_ordinal ORDER BY os.ordinal),
        array_agg(os.hour_source_ordinal ORDER BY os.hour_source_ordinal)
        INTO n,b,s,e,min_hour_source,max_hour_source,min_source,max_source,source_order,canonical_source_order
        FROM recording_joined_output_sources os JOIN recording_joined_hour_sources src ON src.hour_id=os.hour_id AND src.clip_id=os.clip_id
        WHERE os.output_id=OLD.id;
      SELECT count(*) INTO temporal_gaps FROM (
        SELECT src.clip_start_at,lag(src.clip_end_at) OVER(ORDER BY os.ordinal) previous_end
        FROM recording_joined_output_sources os JOIN recording_joined_hour_sources src
          ON src.hour_id=os.hour_id AND src.clip_id=os.clip_id WHERE os.output_id=OLD.id) ordered
        WHERE previous_end IS NOT NULL AND clip_start_at<>previous_end;
      IF n<>OLD.source_clip_count OR b<>OLD.source_bytes OR s<>OLD.coverage_start_at OR e<>OLD.coverage_end_at
        OR min_source<>1 OR max_source<>n OR n<>max_hour_source-min_hour_source+1 OR source_order IS DISTINCT FROM canonical_source_order
        OR temporal_gaps<>0 THEN RAISE EXCEPTION 'invalid joined media publish proof'; END IF;
    ELSIF OLD.artifact_kind='hour_manifest' THEN
      IF OLD.expected_sha256<>h.hour_manifest_sha256 OR OLD.source_clip_count<>h.source_clip_count
        OR OLD.source_bytes<>h.source_bytes OR OLD.coverage_start_at<>h.hour_start_at OR OLD.coverage_end_at<>h.hour_end_at THEN
        RAISE EXCEPTION 'joined hour manifest differs from sealed hour'; END IF;
    ELSIF OLD.artifact_kind='allocation_ledger' THEN
      IF NOT EXISTS(SELECT 1 FROM recording_joined_stream_days d WHERE d.id=OLD.stream_day_id
          AND d.account_id=OLD.account_id AND d.connection_id=OLD.connection_id AND d.batch_id=OLD.batch_id
          AND d.state='publishing' AND d.publish_claim_token IS NOT NULL AND d.publish_lease_expires_at>now()
          AND d.adjacency_manifest_sha256=OLD.expected_sha256 AND d.source_clip_count=OLD.source_clip_count
          AND d.source_bytes=OLD.source_bytes) THEN
        RAISE EXCEPTION 'joined allocation ledger differs from sealed stream day'; END IF;
    ELSIF NOT joined_batch_index_refs_complete(OLD.id)
      OR EXISTS(SELECT 1 FROM recording_joined_hours scope WHERE scope.connection_id=OLD.connection_id
        AND scope.batch_id=OLD.batch_id AND scope.state<>'published') THEN
      RAISE EXCEPTION 'joined batch index published before the batch';
    END IF;
  ELSIF OLD.state='published' AND NEW.state='published' THEN
    IF OLD.nas_verified_at IS NOT NULL OR NEW.nas_verified_at IS NULL OR NEW.nas_verified_at<OLD.published_at
      OR ROW(NEW.size_bytes,NEW.sha256,NEW.r2_etag,NEW.r2_version_id,NEW.published_at)
      IS DISTINCT FROM ROW(OLD.size_bytes,OLD.sha256,OLD.r2_etag,OLD.r2_version_id,OLD.published_at)
      THEN RAISE EXCEPTION 'joined output permits one exact NAS acknowledgment'; END IF;
  ELSE RAISE EXCEPTION 'invalid joined output transition'; END IF;
  NEW.updated_at=now(); RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_output_mutation_guard BEFORE UPDATE ON recording_joined_outputs FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_output_mutation();

CREATE FUNCTION guard_recording_joined_stream_day_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF ROW(NEW.account_id,NEW.connection_id,NEW.recording_id,NEW.batch_id,NEW.local_date,NEW.recording_job_id,
      NEW.source_clip_count,NEW.source_bytes,NEW.source_manifest_sha256,NEW.adjacency_manifest_sha256,
      NEW.first_source_clip_id,NEW.last_source_clip_id,NEW.previous_frozen_clip_id,NEW.cross_day_facts,
      NEW.adjacency_facts,NEW.seal_token,NEW.sealed_at,NEW.created_at)
    IS DISTINCT FROM ROW(OLD.account_id,OLD.connection_id,OLD.recording_id,OLD.batch_id,OLD.local_date,OLD.recording_job_id,
      OLD.source_clip_count,OLD.source_bytes,OLD.source_manifest_sha256,OLD.adjacency_manifest_sha256,
      OLD.first_source_clip_id,OLD.last_source_clip_id,OLD.previous_frozen_clip_id,OLD.cross_day_facts,
      OLD.adjacency_facts,OLD.seal_token,OLD.sealed_at,OLD.created_at) THEN
    RAISE EXCEPTION 'joined stream-day identity is immutable'; END IF;
  IF OLD.state='sealed' AND NEW.state='publishing' THEN
    IF NEW.publish_attempt_count<>OLD.publish_attempt_count+1 OR NEW.publish_lease_expires_at<=now()
      OR NOT EXISTS(SELECT 1 FROM recording_joined_outputs o WHERE o.stream_day_id=OLD.id
        AND o.artifact_kind='allocation_ledger' AND o.state='planned' AND o.hour_seal_token=OLD.seal_token) THEN
      RAISE EXCEPTION 'invalid joined ledger publication claim'; END IF;
  ELSIF OLD.state='publishing' AND NEW.state='publishing' THEN
    IF OLD.publish_lease_expires_at<=now() THEN
      IF NEW.publish_claim_token=OLD.publish_claim_token OR NEW.publish_attempt_count<>OLD.publish_attempt_count+1
        OR NEW.publish_heartbeat_at<=OLD.publish_heartbeat_at OR NEW.publish_lease_expires_at<=now() THEN
        RAISE EXCEPTION 'invalid joined ledger publication reclaim'; END IF;
    ELSIF NEW.publish_claim_token<>OLD.publish_claim_token OR NEW.publish_claimed_by<>OLD.publish_claimed_by
      OR NEW.publish_attempt_count<>OLD.publish_attempt_count OR NEW.publish_heartbeat_at<=OLD.publish_heartbeat_at
      OR NEW.publish_lease_expires_at<=OLD.publish_lease_expires_at THEN
      RAISE EXCEPTION 'joined ledger publication lease is fenced'; END IF;
  ELSIF OLD.state='publishing' AND NEW.state='published' THEN
    IF OLD.publish_lease_expires_at<=now() OR NEW.publish_attempt_count<>OLD.publish_attempt_count OR NEW.published_at IS NULL
      OR NOT EXISTS(SELECT 1 FROM recording_joined_outputs o WHERE o.stream_day_id=OLD.id
        AND o.artifact_kind='allocation_ledger' AND o.state='published' AND o.sha256=OLD.adjacency_manifest_sha256) THEN
      RAISE EXCEPTION 'invalid joined ledger publication'; END IF;
  ELSE RAISE EXCEPTION 'invalid joined stream-day transition'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_stream_day_mutation_guard BEFORE UPDATE ON recording_joined_stream_days
FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_stream_day_mutation();

CREATE FUNCTION reject_recording_joined_fact_mutation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'joined facts are append-only'; END $$;
CREATE TRIGGER joined_hour_no_delete BEFORE DELETE ON recording_joined_hours FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_fact_mutation();
CREATE TRIGGER joined_hour_no_truncate BEFORE TRUNCATE ON recording_joined_hours FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_fact_mutation();
CREATE TRIGGER joined_stream_day_no_delete BEFORE DELETE ON recording_joined_stream_days FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_fact_mutation();
CREATE TRIGGER joined_stream_day_no_truncate BEFORE TRUNCATE ON recording_joined_stream_days FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_fact_mutation();
CREATE TRIGGER joined_hour_source_no_mutation BEFORE UPDATE OR DELETE ON recording_joined_hour_sources FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_fact_mutation();
CREATE TRIGGER joined_hour_source_no_truncate BEFORE TRUNCATE ON recording_joined_hour_sources FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_fact_mutation();
CREATE TRIGGER joined_output_no_delete BEFORE DELETE ON recording_joined_outputs FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_fact_mutation();
CREATE TRIGGER joined_output_no_truncate BEFORE TRUNCATE ON recording_joined_outputs FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_fact_mutation();
CREATE TRIGGER joined_output_source_no_mutation BEFORE UPDATE OR DELETE ON recording_joined_output_sources FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_fact_mutation();
CREATE TRIGGER joined_output_source_no_truncate BEFORE TRUNCATE ON recording_joined_output_sources FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_fact_mutation();
CREATE TRIGGER joined_source_disposition_no_mutation BEFORE UPDATE OR DELETE ON recording_joined_hour_source_dispositions FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_fact_mutation();
CREATE TRIGGER joined_source_disposition_no_truncate BEFORE TRUNCATE ON recording_joined_hour_source_dispositions FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_fact_mutation();
CREATE TRIGGER joined_batch_index_ledger_no_mutation BEFORE UPDATE OR DELETE ON recording_joined_batch_index_ledgers FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_fact_mutation();
CREATE TRIGGER joined_batch_index_ledger_no_truncate BEFORE TRUNCATE ON recording_joined_batch_index_ledgers FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_fact_mutation();
CREATE TRIGGER joined_batch_index_hour_no_mutation BEFORE UPDATE OR DELETE ON recording_joined_batch_index_hours FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_fact_mutation();
CREATE TRIGGER joined_batch_index_hour_no_truncate BEFORE TRUNCATE ON recording_joined_batch_index_hours FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_fact_mutation();

COMMIT;
