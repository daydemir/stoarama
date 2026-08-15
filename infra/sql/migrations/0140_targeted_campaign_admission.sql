-- Exact, user-authorized campaign admission. The reviewed decision rows below
-- are migration-owned: an operator session may execute a decision, but cannot
-- invent or widen Deniz's approved stream set.
--
-- All admission authority uses one transaction-stable clock function. The
-- production definition has no configurable input and always delegates to the
-- PostgreSQL transaction clock. Isolated integration tests may replace this
-- authority-owned function inside their disposable schema to advance time
-- deterministically; runtime and executor roles have no ALTER or EXECUTE grant.
CREATE FUNCTION recording_campaign_now() RETURNS TIMESTAMPTZ
LANGUAGE sql STABLE SET search_path=pg_catalog
AS $$ SELECT pg_catalog.transaction_timestamp() $$;

CREATE TABLE recording_campaign_authority_decisions (
  code TEXT PRIMARY KEY,
  campaign_key TEXT NOT NULL,
  authority_principal_snapshot TEXT NOT NULL,
  authority_source_snapshot TEXT NOT NULL,
  approved_at TIMESTAMPTZ NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  failure_domain_tag TEXT,
  permitted_stream_ids BIGINT[] NOT NULL,
  subset_allowed BOOLEAN NOT NULL,
  min_entries INTEGER NOT NULL,
  max_entries INTEGER NOT NULL,
  qualification_grade_floor TEXT NOT NULL CHECK(qualification_grade_floor='GOOD'),
  qualification_required_consecutive_windows INTEGER NOT NULL CHECK(qualification_required_consecutive_windows=14),
  reporting_grade_floor TEXT NOT NULL CHECK(reporting_grade_floor='ACCEPTABLE'),
  reporting_required_consecutive_windows INTEGER NOT NULL CHECK(reporting_required_consecutive_windows=14),
  decision_sha256 TEXT NOT NULL CHECK(decision_sha256~'^[0-9a-f]{64}$'),
  CHECK(campaign_key='delivery30-2026q3' AND expires_at>approved_at AND expires_at<=approved_at+interval '45 days'),
  CHECK(cardinality(permitted_stream_ids)>0 AND min_entries>0 AND max_entries>=min_entries AND max_entries<=cardinality(permitted_stream_ids)),
  CHECK((subset_allowed AND min_entries<=max_entries) OR (NOT subset_allowed AND min_entries=max_entries AND max_entries=cardinality(permitted_stream_ids)))
);

INSERT INTO recording_campaign_authority_decisions(
  code,campaign_key,authority_principal_snapshot,authority_source_snapshot,approved_at,expires_at,
  failure_domain_tag,permitted_stream_ids,subset_allowed,min_entries,max_entries,
  qualification_grade_floor,qualification_required_consecutive_windows,
  reporting_grade_floor,reporting_required_consecutive_windows,decision_sha256
) VALUES
  ('deniz_fd_restore_20260814','delivery30-2026q3','Deniz','explicit user instruction in the 2026-08-14 campaign operations thread',
   TIMESTAMPTZ '2026-08-14 00:00:00+00',TIMESTAMPTZ '2026-09-28 00:00:00+00','FD',ARRAY[78,94,158,175,178,179,182,195,293,295,415,469,487,2666,2667,2674,2675,2676,2677,2678,2680,2681,2693,2708,2713,2717,2718,2720,2726,2740,2747,2757,2963,2964,2965,2971,2973,2975,2994,2996,2997,3001,3002,3006,3007,3018,3021,3022,3023,3025,3027,3039,3040,3049,12672,12725,14303,14478,14554,14782,17186,17216,17219,17233,17234,17235,17237,17238,17239,17240,17241,17242,17243,17244,17245,17246,17247,17248,17249,17342,17343,17344,17345]::bigint[],true,1,83,'GOOD',14,'ACCEPTABLE',14,
   'd3c5f257acc5e79cfd8ba50a011fbaf0b7430ab3b7fd530946f32e44e90251f1'),
  ('deniz_scene_approval_20260814','delivery30-2026q3','Deniz','explicit user instruction in the 2026-08-14 campaign operations thread',
   TIMESTAMPTZ '2026-08-14 00:00:00+00',TIMESTAMPTZ '2026-09-28 00:00:00+00',NULL,ARRAY[16843,17200,17223]::bigint[],false,3,3,'GOOD',14,'ACCEPTABLE',14,
   encode(sha256(convert_to('deniz_scene_approval_20260814|Deniz|2026-08-14|none|16843,17200,17223|exact:3','UTF8')),'hex'));

UPDATE recording_campaign_authority_decisions decision SET decision_sha256=encode(sha256(convert_to(
  jsonb_build_object('code',decision.code,'campaign_key',decision.campaign_key,
    'authority_principal_snapshot',decision.authority_principal_snapshot,
    'authority_source_snapshot',decision.authority_source_snapshot,'approved_at',decision.approved_at,
    'expires_at',decision.expires_at,'failure_domain_tag',decision.failure_domain_tag,
    'permitted_stream_ids',decision.permitted_stream_ids,'subset_allowed',decision.subset_allowed,
    'min_entries',decision.min_entries,'max_entries',decision.max_entries,
    'qualification_grade_floor',decision.qualification_grade_floor,
    'qualification_required_consecutive_windows',decision.qualification_required_consecutive_windows,
    'reporting_grade_floor',decision.reporting_grade_floor,
    'reporting_required_consecutive_windows',decision.reporting_required_consecutive_windows)::text,'UTF8')),'hex');
ALTER TABLE recording_campaign_authority_decisions ADD CONSTRAINT recording_campaign_authority_decision_digest_exact CHECK(
  decision_sha256=encode(sha256(convert_to(jsonb_build_object('code',code,'campaign_key',campaign_key,
    'authority_principal_snapshot',authority_principal_snapshot,'authority_source_snapshot',authority_source_snapshot,
    'approved_at',approved_at,'expires_at',expires_at,'failure_domain_tag',failure_domain_tag,
    'permitted_stream_ids',permitted_stream_ids,'subset_allowed',subset_allowed,'min_entries',min_entries,
    'max_entries',max_entries,'qualification_grade_floor',qualification_grade_floor,
    'qualification_required_consecutive_windows',qualification_required_consecutive_windows,
    'reporting_grade_floor',reporting_grade_floor,
    'reporting_required_consecutive_windows',reporting_required_consecutive_windows)::text,'UTF8')),'hex'));

-- Admission and qualification remain separate authorities. The campaign track
-- carries the immutable governing streak plus the secondary reporting streak;
-- admission itself never claims that either streak has already been earned.
ALTER TABLE recording_campaign_tracks
  ADD COLUMN reporting_grade_floor TEXT,
  ADD COLUMN reporting_required_consecutive_windows INTEGER;
ALTER TABLE recording_campaign_tracks ADD CONSTRAINT recording_campaign_track_reporting_policy_exact CHECK(
  (campaign_key NOT LIKE 'targeted-admission-%' AND reporting_grade_floor IS NULL AND reporting_required_consecutive_windows IS NULL)
  OR (campaign_key LIKE 'targeted-admission-%' AND reporting_grade_floor='ACCEPTABLE' AND reporting_required_consecutive_windows=14)
);

-- This migration reserves an approved physical scene before probes begin,
-- accepts evidence only from a
-- fresh authenticated managed DO recorder, and seals scheduling + roster
-- protection in one transaction.
CREATE TABLE recording_campaign_admission_approvals (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  request_id UUID NOT NULL,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  actor_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
  actor_email_snapshot TEXT NOT NULL,
  authority_code TEXT NOT NULL REFERENCES recording_campaign_authority_decisions(code) ON DELETE RESTRICT,
  failure_domain_tag TEXT,
  deadline_at TIMESTAMPTZ NOT NULL,
  entries JSONB NOT NULL CHECK(jsonb_typeof(entries)='array' AND jsonb_array_length(entries) BETWEEN 1 AND 32),
  schedule_spec JSONB NOT NULL CHECK(jsonb_typeof(schedule_spec)='object'),
  request_sha256 TEXT NOT NULL CHECK(request_sha256~'^[0-9a-f]{64}$'),
  schedule_sha256 TEXT NOT NULL CHECK(schedule_sha256~'^[0-9a-f]{64}$'),
  approval_sha256 TEXT NOT NULL CHECK(approval_sha256~'^[0-9a-f]{64}$'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT recording_campaign_now(),
  UNIQUE(account_id,request_id),
  UNIQUE(account_id,approval_sha256),
  CHECK(deadline_at>created_at AND deadline_at<=created_at+interval '45 days'),
  CHECK((authority_code='deniz_fd_restore_20260814' AND failure_domain_tag='FD') OR
        (authority_code='deniz_scene_approval_20260814' AND failure_domain_tag IS NULL))
);

CREATE TABLE recording_campaign_admission_reservations (
  approval_id UUID NOT NULL REFERENCES recording_campaign_admission_approvals(id) ON DELETE RESTRICT,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE RESTRICT,
  recording_id BIGINT REFERENCES recordings(id) ON DELETE RESTRICT,
  source_revision_id BIGINT REFERENCES stream_source_revisions(id) ON DELETE RESTRICT,
  source_url_sha256 TEXT NOT NULL CHECK(source_url_sha256~'^[0-9a-f]{64}$'),
  source_page_url_sha256 TEXT NOT NULL CHECK(source_page_url_sha256~'^[0-9a-f]{64}$'),
  source_updated_at TIMESTAMPTZ NOT NULL,
  provider TEXT NOT NULL,
  external_id TEXT NOT NULL,
  normalized_label TEXT NOT NULL CHECK(normalized_label~'^[a-z0-9]+$'),
  scene_frame_evidence_id BIGINT NOT NULL,
  scene_identity_sha256 TEXT NOT NULL CHECK(scene_identity_sha256~'^[0-9a-f]{64}$'),
  reserved_at TIMESTAMPTZ NOT NULL DEFAULT recording_campaign_now(),
  PRIMARY KEY(approval_id,stream_id),
  FOREIGN KEY(scene_frame_evidence_id,account_id,stream_id,scene_identity_sha256)
    REFERENCES recording_scene_frame_evidence(id,account_id,stream_id,scene_identity_sha256) ON DELETE RESTRICT
);
CREATE INDEX recording_campaign_admission_pending_stream ON recording_campaign_admission_reservations(account_id,stream_id);
CREATE INDEX recording_campaign_admission_pending_scene ON recording_campaign_admission_reservations(account_id,scene_identity_sha256);

CREATE TABLE recording_campaign_admission_reservation_terminal_events (
  approval_id UUID PRIMARY KEY REFERENCES recording_campaign_admission_approvals(id) ON DELETE RESTRICT,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  request_id UUID NOT NULL,
  result TEXT NOT NULL CHECK(result='expired_unadmitted'),
  actor_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
  observed_at TIMESTAMPTZ NOT NULL DEFAULT recording_campaign_now(),
  event_sha256 TEXT NOT NULL CHECK(event_sha256~'^[0-9a-f]{64}$'),
  UNIQUE(account_id,request_id)
);

-- An approval never follows a mutable source row. Any authoritative source or
-- scene-identity field change after reservation permanently invalidates that
-- approval, including an A -> B -> A edit that restores the old bytes and
-- timestamp. A later approval can reserve the new head after this event.
CREATE TABLE recording_campaign_admission_source_fence_events (
  id BIGSERIAL PRIMARY KEY,
  stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE RESTRICT,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT recording_campaign_now(),
  prior_fence_sha256 TEXT NOT NULL CHECK(prior_fence_sha256~'^[0-9a-f]{64}$'),
  next_fence_sha256 TEXT NOT NULL CHECK(next_fence_sha256~'^[0-9a-f]{64}$')
);
CREATE INDEX recording_campaign_admission_source_fence_stream ON recording_campaign_admission_source_fence_events(stream_id,occurred_at);

CREATE TABLE recording_targeted_probe_orders (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  request_id UUID NOT NULL,
  approval_id UUID NOT NULL REFERENCES recording_campaign_admission_approvals(id) ON DELETE RESTRICT,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE RESTRICT,
  requested_by_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
  desired_attempts INTEGER NOT NULL DEFAULT 2 CHECK(desired_attempts=2),
  requested_at TIMESTAMPTZ NOT NULL DEFAULT recording_campaign_now(),
  UNIQUE(account_id,request_id),
  UNIQUE(approval_id,stream_id)
);

CREATE TABLE recording_targeted_provider_attestations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  node_id BIGINT NOT NULL REFERENCES nodes(id) ON DELETE RESTRICT,
  recorder_droplet_id BIGINT NOT NULL REFERENCES recorder_droplets(id) ON DELETE RESTRICT,
  do_droplet_id BIGINT NOT NULL,
  project_id_sha256 TEXT NOT NULL CHECK(project_id_sha256~'^[0-9a-f]{64}$'),
  firewall_id_sha256 TEXT NOT NULL CHECK(firewall_id_sha256~'^[0-9a-f]{64}$'),
  region TEXT NOT NULL,
  size_slug TEXT NOT NULL CHECK(size_slug~'^[a-z0-9-]{1,64}$'),
  pool_identity_sha256 TEXT NOT NULL CHECK(pool_identity_sha256~'^[0-9a-f]{64}$'),
  build_sha TEXT NOT NULL CHECK(build_sha~'^[0-9a-f]{40}$'),
  observed_at TIMESTAMPTZ NOT NULL DEFAULT recording_campaign_now(),
  attestation_sha256 TEXT NOT NULL CHECK(attestation_sha256~'^[0-9a-f]{64}$'),
  UNIQUE(node_id,observed_at)
);

CREATE OR REPLACE FUNCTION validate_recording_targeted_probe_order()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; authorized BOOLEAN; bound BOOLEAN;
BEGIN
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_tx_authorizations WHERE transaction_id=txid_current() AND action=''queue'' AND approval_id=$1 AND account_id=$2 AND actor_user_id=$3)',s)
    INTO authorized USING NEW.approval_id,NEW.account_id,NEW.requested_by_user_id;
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_approvals a JOIN %I.recording_campaign_admission_reservations r ON r.approval_id=a.id WHERE a.id=$1 AND a.account_id=$2 AND a.deadline_at>recording_campaign_now() AND r.stream_id=$3)',s,s)
    INTO bound USING NEW.approval_id,NEW.account_id,NEW.stream_id;
  IF authorized IS DISTINCT FROM true OR bound IS DISTINCT FROM true OR NEW.requested_at IS DISTINCT FROM recording_campaign_now() THEN
    RAISE EXCEPTION 'targeted probe order requires an exact live approval and typed operator authorization';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_targeted_probe_order_validate BEFORE INSERT ON recording_targeted_probe_orders FOR EACH ROW EXECUTE FUNCTION validate_recording_targeted_probe_order();

CREATE OR REPLACE FUNCTION validate_recording_targeted_provider_attestation()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE expected TEXT;
BEGIN
  expected:=encode(sha256(convert_to(jsonb_build_object('node_id',NEW.node_id,'recorder_droplet_id',NEW.recorder_droplet_id,'do_droplet_id',NEW.do_droplet_id,'project_id_sha256',NEW.project_id_sha256,'firewall_id_sha256',NEW.firewall_id_sha256,'region',NEW.region,'size_slug',NEW.size_slug,'pool_identity_sha256',NEW.pool_identity_sha256,'build_sha',NEW.build_sha,'observed_at_epoch',extract(epoch from NEW.observed_at))::text,'UTF8')),'hex');
  IF NEW.observed_at IS DISTINCT FROM recording_campaign_now() OR NEW.attestation_sha256<>expected THEN
    RAISE EXCEPTION 'managed DO provider attestation must be exact and database-timed';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_targeted_provider_attestation_validate BEFORE INSERT ON recording_targeted_provider_attestations FOR EACH ROW EXECUTE FUNCTION validate_recording_targeted_provider_attestation();

CREATE TABLE recording_targeted_probe_attempts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  request_id UUID NOT NULL,
  order_id UUID NOT NULL REFERENCES recording_targeted_probe_orders(id) ON DELETE RESTRICT,
  approval_id UUID NOT NULL REFERENCES recording_campaign_admission_approvals(id) ON DELETE RESTRICT,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE RESTRICT,
  attempt_no INTEGER NOT NULL CHECK(attempt_no BETWEEN 1 AND 8),
  node_id BIGINT NOT NULL REFERENCES nodes(id) ON DELETE RESTRICT,
  recorder_droplet_id BIGINT NOT NULL REFERENCES recorder_droplets(id) ON DELETE RESTRICT,
  provider_attestation_id UUID NOT NULL REFERENCES recording_targeted_provider_attestations(id) ON DELETE RESTRICT,
  do_droplet_id BIGINT NOT NULL,
  region TEXT NOT NULL,
  probe_build_sha TEXT NOT NULL CHECK(probe_build_sha~'^[0-9a-f]{40}$'),
  source_revision_id BIGINT REFERENCES stream_source_revisions(id) ON DELETE RESTRICT,
  source_url_sha256 TEXT NOT NULL CHECK(source_url_sha256~'^[0-9a-f]{64}$'),
  source_page_url_sha256 TEXT NOT NULL CHECK(source_page_url_sha256~'^[0-9a-f]{64}$'),
  source_updated_at TIMESTAMPTZ NOT NULL,
  challenge TEXT NOT NULL CHECK(challenge~'^[0-9a-f]{64}$'),
  object_bucket_sha256 TEXT NOT NULL CHECK(object_bucket_sha256~'^[0-9a-f]{64}$'),
  media_object_key TEXT NOT NULL UNIQUE CHECK(media_object_key~'^quarantine/campaign-probe/[0-9a-f-]{36}/media\.zip$'),
  frame_object_key TEXT NOT NULL UNIQUE CHECK(frame_object_key~'^quarantine/campaign-probe/[0-9a-f-]{36}/frame\.jpg$'),
  media_max_size_bytes BIGINT NOT NULL CHECK(media_max_size_bytes BETWEEN 1 AND 134217728),
  frame_max_size_bytes BIGINT NOT NULL CHECK(frame_max_size_bytes BETWEEN 1 AND 8388608),
  started_at TIMESTAMPTZ NOT NULL DEFAULT recording_campaign_now(),
  expires_at TIMESTAMPTZ NOT NULL,
  UNIQUE(account_id,request_id),
  UNIQUE(approval_id,stream_id,attempt_no),
  CHECK(expires_at=started_at+interval '15 minutes')
);

-- An abandoned presigned attempt is an immutable terminal outcome, not a
-- permanent queue poison. The next lease authors this event after expiry;
-- qualification still requires two later successful, advancing attempts.
CREATE TABLE recording_targeted_probe_attempt_terminal_events (
  attempt_id UUID PRIMARY KEY REFERENCES recording_targeted_probe_attempts(id) ON DELETE RESTRICT,
  result TEXT NOT NULL CHECK(result='expired_without_evidence'),
  observed_at TIMESTAMPTZ NOT NULL DEFAULT recording_campaign_now(),
  event_sha256 TEXT NOT NULL CHECK(event_sha256~'^[0-9a-f]{64}$')
);

CREATE FUNCTION validate_recording_targeted_probe_attempt_terminal_event()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; exact BOOLEAN; expected TEXT;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('campaign-admission-capacity-v1',0));
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_targeted_probe_attempts a LEFT JOIN %I.recording_targeted_probe_evidence e ON e.attempt_id=a.id WHERE a.id=$1 AND a.expires_at<=recording_campaign_now() AND e.id IS NULL FOR SHARE OF a)',s,s)
    INTO exact USING NEW.attempt_id;
  expected:=encode(sha256(convert_to('expired_without_evidence','UTF8')
    ||decode('00','hex')||convert_to(NEW.attempt_id::text,'UTF8')
    ||decode('00','hex')||convert_to(extract(epoch from recording_campaign_now())::text,'UTF8')),'hex');
  IF exact IS DISTINCT FROM true OR NEW.result<>'expired_without_evidence' OR
     NEW.observed_at IS DISTINCT FROM recording_campaign_now() OR NEW.event_sha256<>expected THEN
    RAISE EXCEPTION 'targeted attempt terminal must be exact, expired, and database-authored';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_targeted_probe_attempt_terminal_validate BEFORE INSERT ON recording_targeted_probe_attempt_terminal_events FOR EACH ROW EXECUTE FUNCTION validate_recording_targeted_probe_attempt_terminal_event();

CREATE TABLE recording_targeted_probe_evidence (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  attempt_id UUID NOT NULL UNIQUE REFERENCES recording_targeted_probe_attempts(id) ON DELETE RESTRICT,
  approval_id UUID NOT NULL REFERENCES recording_campaign_admission_approvals(id) ON DELETE RESTRICT,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE RESTRICT,
  result TEXT NOT NULL CHECK(result IN('ok','blocked','source_unstable','inconclusive')),
  valid_ratio DOUBLE PRECISION NOT NULL CHECK(valid_ratio>=0 AND valid_ratio<=1.10),
  duration_ms BIGINT NOT NULL CHECK(duration_ms>=0),
  segment_count INTEGER NOT NULL CHECK(segment_count>=0),
  frame_sha256 TEXT CHECK(frame_sha256 IS NULL OR frame_sha256~'^[0-9a-f]{64}$'),
  media_sha256 TEXT CHECK(media_sha256 IS NULL OR media_sha256~'^[0-9a-f]{64}$'),
  native_signature_sha256 TEXT CHECK(native_signature_sha256 IS NULL OR native_signature_sha256~'^[0-9a-f]{64}$'),
  challenge_proof_sha256 TEXT CHECK(challenge_proof_sha256 IS NULL OR challenge_proof_sha256~'^[0-9a-f]{64}$'),
  video_codec TEXT,
  audio_codec TEXT,
  audio_present BOOLEAN,
  video_width INTEGER,
  video_height INTEGER,
  actual_fps DOUBLE PRECISION,
  media_size_bytes BIGINT CHECK(media_size_bytes IS NULL OR media_size_bytes BETWEEN 1 AND 134217728),
  media_etag TEXT CHECK(media_etag IS NULL OR length(media_etag) BETWEEN 1 AND 256),
  media_version_id TEXT CHECK(media_version_id IS NULL OR length(media_version_id) BETWEEN 1 AND 512),
  frame_size_bytes BIGINT CHECK(frame_size_bytes IS NULL OR frame_size_bytes BETWEEN 1 AND 8388608),
  frame_etag TEXT CHECK(frame_etag IS NULL OR length(frame_etag) BETWEEN 1 AND 256),
  frame_version_id TEXT CHECK(frame_version_id IS NULL OR length(frame_version_id) BETWEEN 1 AND 512),
  archive_bucket_sha256 TEXT CHECK(archive_bucket_sha256 IS NULL OR archive_bucket_sha256~'^[0-9a-f]{64}$'),
  media_archive_object_key TEXT CHECK(media_archive_object_key IS NULL OR media_archive_object_key~'^protected/campaign-probe/[0-9a-f-]{36}/media\.zip$'),
  media_archive_sha256 TEXT CHECK(media_archive_sha256 IS NULL OR media_archive_sha256~'^[0-9a-f]{64}$'),
  media_archive_etag TEXT CHECK(media_archive_etag IS NULL OR length(media_archive_etag) BETWEEN 1 AND 256),
  media_archive_version_id TEXT CHECK(media_archive_version_id IS NULL OR length(media_archive_version_id) BETWEEN 1 AND 512),
  frame_archive_object_key TEXT CHECK(frame_archive_object_key IS NULL OR frame_archive_object_key~'^protected/campaign-probe/[0-9a-f-]{36}/frame\.jpg$'),
  frame_archive_sha256 TEXT CHECK(frame_archive_sha256 IS NULL OR frame_archive_sha256~'^[0-9a-f]{64}$'),
  frame_archive_etag TEXT CHECK(frame_archive_etag IS NULL OR length(frame_archive_etag) BETWEEN 1 AND 256),
  frame_archive_version_id TEXT CHECK(frame_archive_version_id IS NULL OR length(frame_archive_version_id) BETWEEN 1 AND 512),
  submission_request_sha256 TEXT NOT NULL CHECK(submission_request_sha256~'^[0-9a-f]{64}$'),
  retain_until TIMESTAMPTZ,
  retention_policy TEXT CHECK(retention_policy IS NULL OR retention_policy='qualification-evidence-campaign-plus-7d-v1'),
  detail TEXT NOT NULL CHECK(length(detail)<=1024 AND detail~'^(resolve_failed|image_source|ssrf_guard_rejected|temporary_storage_unavailable|parent_context_cancelled|valid_ratio=[0-9]+\.[0-9]{3} segments=[0-9]+ native_signature_stable=(true|false) frame=(true|false)( capture_exit=(network_cut|source_down|other))?)$'),
  observed_at TIMESTAMPTZ NOT NULL DEFAULT recording_campaign_now(),
  evidence_sha256 TEXT NOT NULL CHECK(evidence_sha256~'^[0-9a-f]{64}$'),
  CHECK(result<>'ok' OR (valid_ratio>=0.95 AND duration_ms BETWEEN 110000 AND 130000 AND segment_count=2 AND
    frame_sha256 IS NOT NULL AND media_sha256 IS NOT NULL AND native_signature_sha256 IS NOT NULL AND challenge_proof_sha256 IS NOT NULL AND
    lower(video_codec)='h264' AND video_width BETWEEN 1 AND 16384 AND video_height BETWEEN 1 AND 16384 AND actual_fps>0 AND actual_fps<=240 AND
    media_size_bytes IS NOT NULL AND media_etag IS NOT NULL AND frame_size_bytes IS NOT NULL AND frame_etag IS NOT NULL AND
    archive_bucket_sha256 IS NOT NULL AND media_archive_object_key IS NOT NULL AND media_archive_sha256 IS NOT NULL AND media_archive_etag IS NOT NULL AND
    frame_archive_object_key IS NOT NULL AND frame_archive_sha256 IS NOT NULL AND frame_archive_etag IS NOT NULL AND retain_until IS NOT NULL AND retention_policy IS NOT NULL AND
    audio_present IS NOT NULL AND ((audio_present AND length(btrim(COALESCE(audio_codec,'')))>0) OR (NOT audio_present AND btrim(COALESCE(audio_codec,''))=''))))
);

-- Narrow product bridge used by the ordinary cloud claim query. It exposes
-- only the current slot count, never attempt/object/evidence rows.
CREATE FUNCTION recording_worker_targeted_probe_occupancy(p_node_id BIGINT)
RETURNS INTEGER LANGUAGE sql STABLE SECURITY DEFINER SET search_path FROM CURRENT AS $$
  SELECT count(*)::integer FROM recording_targeted_probe_attempts attempt
  LEFT JOIN recording_targeted_probe_evidence evidence ON evidence.attempt_id=attempt.id
  LEFT JOIN recording_targeted_probe_attempt_terminal_events terminal ON terminal.attempt_id=attempt.id
  WHERE attempt.node_id=p_node_id AND attempt.expires_at>recording_campaign_now()
    AND evidence.id IS NULL AND terminal.attempt_id IS NULL
$$;

CREATE TABLE recording_targeted_probe_scene_presentations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  request_id UUID NOT NULL,
  approval_id UUID NOT NULL REFERENCES recording_campaign_admission_approvals(id) ON DELETE RESTRICT,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE RESTRICT,
  probe_evidence_id UUID NOT NULL REFERENCES recording_targeted_probe_evidence(id) ON DELETE RESTRICT,
  probe_frame_sha256 TEXT NOT NULL CHECK(probe_frame_sha256~'^[0-9a-f]{64}$'),
  frame_archive_object_key TEXT NOT NULL CHECK(frame_archive_object_key~'^protected/campaign-probe/[0-9a-f-]{36}/frame\.jpg$'),
  frame_archive_etag TEXT NOT NULL,
  frame_archive_version_id TEXT,
  frame_size_bytes BIGINT NOT NULL CHECK(frame_size_bytes BETWEEN 1 AND 8388608),
  presented_to_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
  presented_at TIMESTAMPTZ NOT NULL DEFAULT recording_campaign_now(),
  presentation_sha256 TEXT NOT NULL CHECK(presentation_sha256~'^[0-9a-f]{64}$'),
  UNIQUE(account_id,request_id),
  UNIQUE(id,account_id,approval_id,probe_evidence_id,presented_to_user_id)
);

CREATE TABLE recording_targeted_probe_scene_reviews (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  request_id UUID NOT NULL,
  approval_id UUID NOT NULL REFERENCES recording_campaign_admission_approvals(id) ON DELETE RESTRICT,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE RESTRICT,
  probe_evidence_id UUID NOT NULL UNIQUE REFERENCES recording_targeted_probe_evidence(id) ON DELETE RESTRICT,
  presentation_id UUID NOT NULL,
  probe_frame_sha256 TEXT NOT NULL CHECK(probe_frame_sha256~'^[0-9a-f]{64}$'),
  scene_frame_evidence_id BIGINT NOT NULL,
  scene_identity_sha256 TEXT NOT NULL CHECK(scene_identity_sha256~'^[0-9a-f]{64}$'),
  reviewed_by_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
  reviewed_at TIMESTAMPTZ NOT NULL DEFAULT recording_campaign_now(),
  review_sha256 TEXT NOT NULL CHECK(review_sha256~'^[0-9a-f]{64}$'),
  UNIQUE(account_id,request_id),
  FOREIGN KEY(presentation_id,account_id,approval_id,probe_evidence_id,reviewed_by_user_id)
    REFERENCES recording_targeted_probe_scene_presentations(id,account_id,approval_id,probe_evidence_id,presented_to_user_id) ON DELETE RESTRICT,
  FOREIGN KEY(scene_frame_evidence_id,account_id,stream_id,scene_identity_sha256)
    REFERENCES recording_scene_frame_evidence(id,account_id,stream_id,scene_identity_sha256) ON DELETE RESTRICT
);

-- Candidate/completed streams need a supported visual baseline before an
-- approval exists. This receipt binds the exact bytes presented to the exact
-- immutable decision and source head; scene attestation consumes it.
CREATE TABLE recording_campaign_baseline_scene_read_receipts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  request_id UUID NOT NULL,
  authority_code TEXT NOT NULL REFERENCES recording_campaign_authority_decisions(code) ON DELETE RESTRICT,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE RESTRICT,
  frame_id BIGINT NOT NULL REFERENCES frames(id) ON DELETE RESTRICT,
  media_object_id BIGINT NOT NULL REFERENCES media_objects(id) ON DELETE RESTRICT,
  frame_sha256 TEXT NOT NULL CHECK(frame_sha256~'^[0-9a-f]{64}$'),
  media_object_key TEXT NOT NULL,
  media_etag TEXT NOT NULL,
  media_size_bytes BIGINT NOT NULL CHECK(media_size_bytes BETWEEN 1 AND 8388608),
  source_revision_id BIGINT REFERENCES stream_source_revisions(id) ON DELETE RESTRICT,
  source_url_sha256 TEXT NOT NULL CHECK(source_url_sha256~'^[0-9a-f]{64}$'),
  source_page_url_sha256 TEXT NOT NULL CHECK(source_page_url_sha256~'^[0-9a-f]{64}$'),
  source_updated_at TIMESTAMPTZ NOT NULL,
  read_by_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
  read_at TIMESTAMPTZ NOT NULL DEFAULT recording_campaign_now(),
  expires_at TIMESTAMPTZ NOT NULL DEFAULT recording_campaign_now()+interval '5 minutes',
  receipt_sha256 TEXT NOT NULL CHECK(receipt_sha256~'^[0-9a-f]{64}$'),
  UNIQUE(account_id,request_id),
  UNIQUE(id,account_id,stream_id,frame_id,read_by_user_id),
  CHECK(expires_at=read_at+interval '5 minutes')
);

CREATE TABLE recording_campaign_baseline_scene_presentations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  request_id UUID NOT NULL,
  authority_code TEXT NOT NULL REFERENCES recording_campaign_authority_decisions(code) ON DELETE RESTRICT,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE RESTRICT,
  frame_id BIGINT NOT NULL REFERENCES frames(id) ON DELETE RESTRICT,
  read_receipt_id UUID NOT NULL,
  media_object_id BIGINT NOT NULL REFERENCES media_objects(id) ON DELETE RESTRICT,
  frame_sha256 TEXT NOT NULL CHECK(frame_sha256~'^[0-9a-f]{64}$'),
  media_object_key TEXT NOT NULL,
  media_etag TEXT NOT NULL,
  source_revision_id BIGINT REFERENCES stream_source_revisions(id) ON DELETE RESTRICT,
  source_url_sha256 TEXT NOT NULL CHECK(source_url_sha256~'^[0-9a-f]{64}$'),
  source_page_url_sha256 TEXT NOT NULL CHECK(source_page_url_sha256~'^[0-9a-f]{64}$'),
  source_updated_at TIMESTAMPTZ NOT NULL,
  presented_to_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
  presented_at TIMESTAMPTZ NOT NULL DEFAULT recording_campaign_now(),
  presentation_sha256 TEXT NOT NULL CHECK(presentation_sha256~'^[0-9a-f]{64}$'),
  UNIQUE(account_id,request_id),
  UNIQUE(id,account_id,stream_id,frame_id,presented_to_user_id),
  FOREIGN KEY(read_receipt_id,account_id,stream_id,frame_id,presented_to_user_id)
    REFERENCES recording_campaign_baseline_scene_read_receipts(id,account_id,stream_id,frame_id,read_by_user_id) ON DELETE RESTRICT
);

CREATE TABLE recording_campaign_capacity_observations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  approval_id UUID NOT NULL REFERENCES recording_campaign_admission_approvals(id) ON DELETE RESTRICT,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  observation_started_at TIMESTAMPTZ NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  build_sha TEXT NOT NULL CHECK(build_sha~'^[0-9a-f]{40}$'),
  size_slug TEXT NOT NULL CHECK(btrim(size_slug)<>''),
  pool_identity_sha256 TEXT NOT NULL CHECK(pool_identity_sha256~'^[0-9a-f]{64}$'),
  ready_workers INTEGER NOT NULL CHECK(ready_workers>=2),
  total_slots INTEGER NOT NULL CHECK(total_slots>0),
  largest_worker_slots INTEGER NOT NULL CHECK(largest_worker_slots>0),
  usable_after_worker_loss INTEGER NOT NULL CHECK(usable_after_worker_loss>0),
  largest_region TEXT NOT NULL,
  largest_region_slots INTEGER NOT NULL CHECK(largest_region_slots>0),
  relay_active_demand INTEGER NOT NULL CHECK(relay_active_demand>=0),
  relay_failure_domains INTEGER NOT NULL CHECK(relay_failure_domains>=0),
  relay_effective_capacity INTEGER NOT NULL CHECK(relay_effective_capacity>=0),
  relay_usable_after_largest_loss INTEGER NOT NULL CHECK(relay_usable_after_largest_loss>=0),
  provider_project_sha256 TEXT NOT NULL CHECK(provider_project_sha256~'^[0-9a-f]{64}$'),
  provider_firewall_sha256 TEXT NOT NULL CHECK(provider_firewall_sha256~'^[0-9a-f]{64}$'),
  facts_sha256 TEXT NOT NULL CHECK(facts_sha256~'^[0-9a-f]{64}$'),
  provider_observation_sha256 TEXT NOT NULL CHECK(provider_observation_sha256~'^[0-9a-f]{64}$'),
  UNIQUE(id,account_id,approval_id),
  CHECK(usable_after_worker_loss=total_slots-largest_worker_slots),
  CHECK(relay_usable_after_largest_loss<=relay_effective_capacity),
  CHECK(relay_active_demand=0 OR (relay_failure_domains>=2 AND relay_usable_after_largest_loss>=relay_active_demand)),
  CHECK(largest_region_slots<=total_slots),
  CHECK(observation_started_at<=observed_at AND observed_at-observation_started_at<=interval '120 seconds'),
  CHECK(expires_at=observed_at+interval '120 seconds')
);

CREATE TABLE recording_campaign_capacity_reservations (
  approval_id UUID PRIMARY KEY REFERENCES recording_campaign_admission_approvals(id) ON DELETE RESTRICT,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  observation_id UUID NOT NULL,
  forecast_peak_slots INTEGER NOT NULL CHECK(forecast_peak_slots>0),
  active_roster_after INTEGER NOT NULL CHECK(active_roster_after BETWEEN 1 AND 60),
  roster_cap INTEGER NOT NULL CHECK(roster_cap=60),
  reserved_at TIMESTAMPTZ NOT NULL DEFAULT recording_campaign_now(),
  FOREIGN KEY(observation_id,account_id,approval_id) REFERENCES recording_campaign_capacity_observations(id,account_id,approval_id) ON DELETE RESTRICT
);

-- Authority-side governed-horizon peak. Admission schedules are continuous
-- daily windows; any other cloud mode is conservatively charged as permanently live.
-- This is recomputed after the candidate rows are active while the shared
-- scheduler fence is held, so no executor forecast can increase authority.
CREATE FUNCTION recording_campaign_forecast_peak_slots(p_account_id BIGINT)
RETURNS INTEGER LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
  WITH configs AS (
    SELECT r.* FROM recordings r
    WHERE r.account_id=p_account_id AND r.status='active' AND r.capture_via='cloud'
  ), constant_load AS (
    SELECT count(*)::int n FROM configs WHERE mode<>'continuous' OR daily_window_start IS NULL OR daily_window_end IS NULL
  ), days AS (
    SELECT c.id,c.cron_timezone,c.daily_window_start,c.daily_window_end,c.active_weekdays,c.start_at,c.end_at,
      generate_series((recording_campaign_now() AT TIME ZONE c.cron_timezone)::date-1,
        (recording_campaign_now() AT TIME ZONE c.cron_timezone)::date+60,interval '1 day') AS schedule_day
    FROM configs c WHERE c.mode='continuous' AND c.daily_window_start IS NOT NULL AND c.daily_window_end IS NOT NULL
  ), windows AS (
    SELECT ((schedule_day::date+daily_window_start)::timestamp AT TIME ZONE cron_timezone) start_at,
      ((schedule_day::date+daily_window_end+CASE WHEN daily_window_end<=daily_window_start THEN interval '1 day' ELSE interval '0' END)::timestamp AT TIME ZONE cron_timezone) end_at
    FROM days
    WHERE (active_weekdays & (1 << (extract(isodow from schedule_day)::int-1)))<>0
      AND ((schedule_day::date+daily_window_start)::timestamp AT TIME ZONE cron_timezone)<COALESCE(end_at,'infinity'::timestamptz)
      AND ((schedule_day::date+daily_window_end+CASE WHEN daily_window_end<=daily_window_start THEN interval '1 day' ELSE interval '0' END)::timestamp AT TIME ZONE cron_timezone)>start_at
  ), events AS (
    SELECT start_at moment,1 delta FROM windows WHERE end_at>recording_campaign_now()
    UNION ALL SELECT end_at,-1 FROM windows WHERE end_at>recording_campaign_now()
  ), running AS (
    SELECT sum(delta) OVER(ORDER BY moment,delta) concurrent FROM events
  )
  SELECT COALESCE((SELECT max(concurrent)::int FROM running),0)+(SELECT n FROM constant_load)
$$;

-- Relay demand is unchanged by this cloud-only admission, but the campaign
-- promise is conjunctive: the fleet must already survive its largest eligible
-- relay failure domain while the cloud pool survives one worker loss.  Derive
-- that denominator from the same fresh-node/group cap used by relay alerts.
CREATE FUNCTION recording_campaign_relay_failure_capacity(p_account_id BIGINT)
RETURNS TABLE(active_demand INTEGER,failure_domains INTEGER,effective_capacity INTEGER,usable_after_largest_loss INTEGER)
LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
  WITH live_nodes AS (
    SELECT n.id,n.relay_group_id,n.relay_max_streams
    FROM nodes n
    WHERE n.account_id=p_account_id AND n.node_type='relay' AND n.status='active'
      AND n.last_heartbeat_at>recording_campaign_now()-interval '120 seconds'
  ), domains AS (
    SELECT 'group:'||ln.relay_group_id::text AS domain,
      LEAST(max(g.max_streams),sum(ln.relay_max_streams))::integer AS capacity
    FROM live_nodes ln JOIN relay_groups g ON g.id=ln.relay_group_id AND g.account_id=p_account_id
    WHERE ln.relay_group_id IS NOT NULL GROUP BY ln.relay_group_id
    UNION ALL
    SELECT 'node:'||id::text,relay_max_streams FROM live_nodes WHERE relay_group_id IS NULL
  ), demand AS (
    -- Conservative schedule peak: every active relay recording may overlap.
    -- This intentionally does not collapse to the off-window due-job count.
    SELECT count(*)::integer AS value FROM recordings r
    WHERE r.account_id=p_account_id AND r.capture_via='relay' AND r.status='active'
      -- Count every governed active schedule whose interval has not ended,
      -- including a future-start window. Admission is a campaign-horizon
      -- promise, not a point-in-time utilization check.
      AND (r.end_at IS NULL OR r.end_at>recording_campaign_now())
  ), totals AS (
    SELECT count(*)::integer AS domains,COALESCE(sum(capacity),0)::integer AS capacity,
      COALESCE(sum(capacity)-max(capacity),0)::integer AS remaining FROM domains
  )
  SELECT demand.value,totals.domains,totals.capacity,totals.remaining FROM demand CROSS JOIN totals
$$;

CREATE TABLE recording_campaign_storage_observations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  approval_id UUID NOT NULL REFERENCES recording_campaign_admission_approvals(id) ON DELETE RESTRICT,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  connection_id BIGINT NOT NULL REFERENCES connections(id) ON DELETE RESTRICT,
  observed_at TIMESTAMPTZ NOT NULL DEFAULT recording_campaign_now(),
  nas_reported_at TIMESTAMPTZ NOT NULL,
  nas_total_bytes BIGINT NOT NULL CHECK(nas_total_bytes>0),
  nas_free_bytes BIGINT NOT NULL CHECK(nas_free_bytes>=0 AND nas_free_bytes<=nas_total_bytes),
  measured_24h_bytes BIGINT NOT NULL CHECK(measured_24h_bytes>0),
  measured_streams INTEGER NOT NULL CHECK(measured_streams>0),
  active_roster_after INTEGER NOT NULL CHECK(active_roster_after BETWEEN 1 AND 60),
  projected_daily_bytes BIGINT NOT NULL CHECK(projected_daily_bytes>0),
  campaign_days_with_reserve INTEGER NOT NULL CHECK(campaign_days_with_reserve BETWEEN 8 AND 60),
  required_free_bytes BIGINT NOT NULL CHECK(required_free_bytes>0),
  projected_free_after_bytes BIGINT NOT NULL CHECK(projected_free_after_bytes>=0),
  warning_threshold_bytes BIGINT NOT NULL CHECK(warning_threshold_bytes>0),
  warning_after_reservation BOOLEAN NOT NULL,
  policy_version TEXT NOT NULL CHECK(policy_version='measured-active-linear-125pct-campaign-plus-7d-v1'),
  facts_sha256 TEXT NOT NULL CHECK(facts_sha256~'^[0-9a-f]{64}$'),
  UNIQUE(id,account_id,approval_id)
);

CREATE TABLE recording_campaign_storage_reservations (
  approval_id UUID PRIMARY KEY REFERENCES recording_campaign_admission_approvals(id) ON DELETE RESTRICT,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  observation_id UUID NOT NULL,
  reserved_bytes BIGINT NOT NULL CHECK(reserved_bytes>0),
  reserved_until TIMESTAMPTZ NOT NULL,
  reserved_at TIMESTAMPTZ NOT NULL DEFAULT recording_campaign_now(),
  FOREIGN KEY(observation_id,account_id,approval_id) REFERENCES recording_campaign_storage_observations(id,account_id,approval_id) ON DELETE RESTRICT
);

CREATE TABLE recording_campaign_admission_results (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  approval_id UUID NOT NULL REFERENCES recording_campaign_admission_approvals(id) ON DELETE RESTRICT,
  first_probe_evidence_id UUID NOT NULL REFERENCES recording_targeted_probe_evidence(id) ON DELETE RESTRICT,
  second_probe_evidence_id UUID NOT NULL REFERENCES recording_targeted_probe_evidence(id) ON DELETE RESTRICT,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  track_id BIGINT NOT NULL REFERENCES recording_campaign_tracks(id) ON DELETE RESTRICT,
  roster_entry_id BIGINT NOT NULL REFERENCES recording_campaign_roster_entries(id) ON DELETE RESTRICT,
  stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE RESTRICT,
  recording_id BIGINT NOT NULL REFERENCES recordings(id) ON DELETE RESTRICT,
  actor_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
  action TEXT NOT NULL CHECK(action IN('created','reactivated')),
  schedule_sha256 TEXT NOT NULL CHECK(schedule_sha256~'^[0-9a-f]{64}$'),
  recording_config_sha256 TEXT NOT NULL CHECK(recording_config_sha256~'^[0-9a-f]{64}$'),
  roster_entry_sha256 TEXT NOT NULL DEFAULT repeat('0',64) CHECK(roster_entry_sha256~'^[0-9a-f]{64}$'),
  admitted_at TIMESTAMPTZ NOT NULL DEFAULT recording_campaign_now(),
  UNIQUE(approval_id,stream_id),
  UNIQUE(track_id,roster_entry_id),
  CHECK(first_probe_evidence_id<>second_probe_evidence_id)
);

CREATE TABLE recording_campaign_admission_commits (
  approval_id UUID PRIMARY KEY REFERENCES recording_campaign_admission_approvals(id) ON DELETE RESTRICT,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  actor_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
  track_id BIGINT NOT NULL UNIQUE REFERENCES recording_campaign_tracks(id) ON DELETE RESTRICT,
  schedule_sha256 TEXT NOT NULL CHECK(schedule_sha256~'^[0-9a-f]{64}$'),
  response_json JSONB NOT NULL CHECK(jsonb_typeof(response_json)='object'),
  response_sha256 TEXT NOT NULL CHECK(response_sha256~'^[0-9a-f]{64}$'),
  committed_at TIMESTAMPTZ NOT NULL DEFAULT recording_campaign_now()
);

CREATE TABLE recording_campaign_admission_tx_authorizations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  transaction_id BIGINT NOT NULL,
  action TEXT NOT NULL CHECK(action IN('baseline_present','baseline_attest','approve','queue','present','review','attempt','evidence','admit','expire')),
  approval_id UUID REFERENCES recording_campaign_admission_approvals(id) ON DELETE RESTRICT,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  actor_user_id BIGINT REFERENCES users(id) ON DELETE RESTRICT,
  account_session_id BIGINT REFERENCES account_sessions(id) ON DELETE RESTRICT,
  node_id BIGINT REFERENCES nodes(id) ON DELETE RESTRICT,
  node_token_id BIGINT REFERENCES node_tokens(id) ON DELETE RESTRICT,
  node_claim_generation BIGINT,
  authorized_at TIMESTAMPTZ NOT NULL DEFAULT recording_campaign_now(),
  UNIQUE(transaction_id,action,account_id),
  CHECK((action IN('baseline_present','baseline_attest','approve','queue','present','review','admit','expire') AND actor_user_id IS NOT NULL AND account_session_id IS NOT NULL AND node_id IS NULL AND node_token_id IS NULL AND node_claim_generation IS NULL) OR
        (action IN('attempt','evidence') AND actor_user_id IS NULL AND account_session_id IS NULL AND node_id IS NOT NULL AND node_token_id IS NOT NULL AND node_claim_generation>0)),
  CHECK((action IN('approve','baseline_present','baseline_attest') AND approval_id IS NULL) OR (action NOT IN('approve','baseline_present','baseline_attest') AND approval_id IS NOT NULL))
);

-- Runtime code never writes authority tables directly. These narrow entry
-- functions are transferred to the NOLOGIN authority owner below and are the
-- only mutation surface granted to the runtime login. Row triggers remain the
-- final, transaction-local validators for every append.
CREATE OR REPLACE FUNCTION recording_campaign_create_approval(
  p_request_id UUID,p_account_id BIGINT,p_actor_user_id BIGINT,p_actor_email TEXT,
  p_authority_code TEXT,p_failure_domain_tag TEXT,p_deadline_at TIMESTAMPTZ,
  p_entries JSONB,p_schedule_spec JSONB,p_request_sha256 TEXT
) RETURNS TABLE(id UUID,approval_sha256 TEXT) LANGUAGE sql SET search_path FROM CURRENT AS $$
  WITH payload AS (
    SELECT p_account_id account_id,p_actor_user_id actor_user_id,p_actor_email actor_email,
      p_authority_code authority_code,NULLIF(p_failure_domain_tag,'') failure_domain_tag,
      p_deadline_at deadline_at,p_entries entries,p_schedule_spec schedule_spec
  ), hashed AS (
    SELECT *,encode(sha256(convert_to(schedule_spec::text,'UTF8')),'hex') schedule_sha256 FROM payload
  )
  INSERT INTO recording_campaign_admission_approvals(
    request_id,account_id,actor_user_id,actor_email_snapshot,authority_code,
    failure_domain_tag,deadline_at,entries,schedule_spec,request_sha256,
    schedule_sha256,approval_sha256
  )
  SELECT p_request_id,account_id,actor_user_id,actor_email,authority_code,
    failure_domain_tag,deadline_at,entries,schedule_spec,p_request_sha256,schedule_sha256,
    encode(sha256(convert_to(jsonb_build_object(
      'account_id',account_id,'actor_user_id',actor_user_id,'actor_email',lower(actor_email),
      'authority_code',authority_code,'failure_domain_tag',failure_domain_tag,
      'deadline_epoch',extract(epoch from deadline_at),'entries',entries,
      'schedule_sha256',schedule_sha256)::text,'UTF8')),'hex')
  FROM hashed ON CONFLICT DO NOTHING RETURNING recording_campaign_admission_approvals.id,
    recording_campaign_admission_approvals.approval_sha256
$$;

CREATE OR REPLACE FUNCTION recording_campaign_create_probe_order(
  p_request_id UUID,p_approval_id UUID,p_account_id BIGINT,p_stream_id BIGINT,p_user_id BIGINT
) RETURNS TABLE(id UUID) LANGUAGE sql SET search_path FROM CURRENT AS $$
  INSERT INTO recording_targeted_probe_orders(request_id,approval_id,account_id,stream_id,requested_by_user_id)
  VALUES(p_request_id,p_approval_id,p_account_id,p_stream_id,p_user_id)
  ON CONFLICT DO NOTHING
  RETURNING recording_targeted_probe_orders.id
$$;

CREATE OR REPLACE FUNCTION recording_campaign_create_provider_attestation(
  p_node_id BIGINT,p_recorder_droplet_id BIGINT,p_do_droplet_id BIGINT,
  p_project_sha256 TEXT,p_firewall_sha256 TEXT,p_region TEXT,p_size_slug TEXT,
  p_pool_identity_sha256 TEXT,p_build_sha TEXT
) RETURNS TABLE(id UUID) LANGUAGE sql SET search_path FROM CURRENT AS $$
  WITH facts AS (
    SELECT p_node_id node_id,p_recorder_droplet_id recorder_droplet_id,p_do_droplet_id do_droplet_id,
      p_project_sha256 project_hash,p_firewall_sha256 firewall_hash,p_region region,p_size_slug size_slug,
      p_pool_identity_sha256 pool_identity_sha256,p_build_sha build_sha
    FROM recorder_droplets d JOIN recording_worker_claim_heads head ON head.node_id=d.node_id
    JOIN node_tokens tok ON tok.id=head.claim_token_id AND tok.node_id=d.node_id
    WHERE d.id=p_recorder_droplet_id AND d.node_id=p_node_id AND d.do_droplet_id=p_do_droplet_id
      AND d.region=p_region AND d.size=p_size_slug AND d.build_sha=p_build_sha AND d.state='active'
      AND head.state='enabled' AND tok.revoked_at IS NULL AND tok.recording_claim_purpose='claim_current'
      AND p_pool_identity_sha256=encode(sha256(convert_to(
        p_size_slug||chr(10)||p_build_sha||chr(10)||d.capacity::text||chr(10)||
        p_project_sha256||chr(10)||p_firewall_sha256,'UTF8')),'hex')
  ), hashed AS (
    SELECT *,encode(sha256(convert_to(jsonb_build_object(
      'node_id',node_id,'recorder_droplet_id',recorder_droplet_id,'do_droplet_id',do_droplet_id,
      'project_id_sha256',project_hash,'firewall_id_sha256',firewall_hash,'region',region,
      'size_slug',size_slug,'pool_identity_sha256',pool_identity_sha256,'build_sha',build_sha,
      'observed_at_epoch',extract(epoch from recording_campaign_now()))::text,'UTF8')),'hex') digest
    FROM facts
  )
  INSERT INTO recording_targeted_provider_attestations(
    node_id,recorder_droplet_id,do_droplet_id,project_id_sha256,firewall_id_sha256,region,size_slug,
    pool_identity_sha256,build_sha,attestation_sha256
  ) SELECT node_id,recorder_droplet_id,do_droplet_id,project_hash,firewall_hash,region,size_slug,
      pool_identity_sha256,build_sha,digest FROM hashed
  RETURNING recording_targeted_provider_attestations.id
$$;

CREATE OR REPLACE FUNCTION recording_campaign_create_probe_attempt(
  p_id UUID,p_request_id UUID,p_order_id UUID,p_approval_id UUID,p_account_id BIGINT,p_stream_id BIGINT,
  p_node_id BIGINT,p_recorder_droplet_id BIGINT,p_provider_attestation_id UUID,p_do_droplet_id BIGINT,
  p_region TEXT,p_probe_build_sha TEXT,p_source_revision_id BIGINT,p_source_url_sha256 TEXT,
  p_source_page_url_sha256 TEXT,p_source_updated_at TIMESTAMPTZ,p_challenge TEXT,
  p_object_bucket_sha256 TEXT,p_media_object_key TEXT,p_frame_object_key TEXT,
  p_media_max_size_bytes BIGINT,p_frame_max_size_bytes BIGINT
) RETURNS TABLE(id UUID,challenge TEXT) LANGUAGE sql SET search_path FROM CURRENT AS $$
  INSERT INTO recording_targeted_probe_attempts(
    id,request_id,order_id,approval_id,account_id,stream_id,attempt_no,node_id,recorder_droplet_id,
    provider_attestation_id,do_droplet_id,region,probe_build_sha,source_revision_id,source_url_sha256,
    source_page_url_sha256,source_updated_at,challenge,object_bucket_sha256,media_object_key,
    frame_object_key,media_max_size_bytes,frame_max_size_bytes,expires_at
  )
  SELECT p_id,p_request_id,p_order_id,p_approval_id,p_account_id,p_stream_id,COALESCE(max(attempt_no),0)+1,
    p_node_id,p_recorder_droplet_id,p_provider_attestation_id,p_do_droplet_id,p_region,p_probe_build_sha,
    p_source_revision_id,p_source_url_sha256,p_source_page_url_sha256,p_source_updated_at,p_challenge,
    p_object_bucket_sha256,p_media_object_key,p_frame_object_key,p_media_max_size_bytes,p_frame_max_size_bytes,
    recording_campaign_now()+interval '15 minutes'
  FROM recording_targeted_probe_attempts WHERE approval_id=p_approval_id AND stream_id=p_stream_id
  RETURNING recording_targeted_probe_attempts.id,recording_targeted_probe_attempts.challenge
$$;

CREATE OR REPLACE FUNCTION recording_campaign_lease_probe(
  p_node_id BIGINT,p_node_token_id BIGINT,p_claim_generation BIGINT,p_credential_sha256 TEXT,
  p_recorder_droplet_id BIGINT,p_do_droplet_id BIGINT,p_region TEXT,p_build_sha TEXT,
  p_project_sha256 TEXT,p_firewall_sha256 TEXT,p_size_slug TEXT,p_pool_identity_sha256 TEXT,
  p_attempt_id UUID,p_request_id UUID,p_challenge TEXT,
  p_bucket_sha256 TEXT,p_media_key TEXT,p_frame_key TEXT,p_media_max BIGINT,p_frame_max BIGINT
) RETURNS TABLE(order_id UUID,approval_id UUID,account_id BIGINT,stream_id BIGINT,provider TEXT,
  source_url TEXT,source_page_url TEXT,attempt_id UUID,challenge TEXT) LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE selected RECORD; provider_id UUID; revision_id BIGINT; source_hash TEXT; page_hash TEXT; source_updated TIMESTAMPTZ;
BEGIN
  PERFORM pg_advisory_xact_lock_shared(hashtextextended('recording-worker-claim-v1',0));
  PERFORM pg_advisory_xact_lock(hashtextextended('recording-surrender-cloud-capacity-v1',0));
  -- Expiration, queueing, and leasing share one admission fence. This makes
  -- the approval terminal projection reciprocal with probe creation: an
  -- expiry cannot commit while a lease is being authorized, and a lease that
  -- starts after terminalization observes the terminal row before selecting
  -- an order.
  PERFORM pg_advisory_xact_lock(hashtextextended('campaign-admission-capacity-v1',0));
  -- Footage always wins. A probe cannot occupy a worker while it owns a live
  -- recording fence, or while an immediately claimable cloud job is waiting.
  IF EXISTS(SELECT 1 FROM recording_jobs j JOIN recorder_droplets d ON d.name=j.lease_owner
      WHERE d.node_id=p_node_id AND j.status='leased' AND j.lease_expires_at>recording_campaign_now()) OR
     EXISTS(SELECT 1 FROM recording_jobs j JOIN recordings r ON r.id=j.recording_id
      WHERE j.status='pending' AND r.capture_via='cloud' AND j.scheduled_for<=recording_campaign_now()) THEN
    RETURN;
  END IF;

  INSERT INTO recording_targeted_probe_attempt_terminal_events(attempt_id,result,event_sha256)
  SELECT pa.id,'expired_without_evidence',encode(sha256(
	convert_to('expired_without_evidence','UTF8')
	||decode('00','hex')||convert_to(pa.id::text,'UTF8')
	||decode('00','hex')||convert_to(extract(epoch from recording_campaign_now())::text,'UTF8')),'hex')
  FROM recording_targeted_probe_attempts pa
  LEFT JOIN recording_targeted_probe_evidence pe ON pe.attempt_id=pa.id
  LEFT JOIN recording_targeted_probe_attempt_terminal_events terminal ON terminal.attempt_id=pa.id
  WHERE pe.id IS NULL AND terminal.attempt_id IS NULL AND pa.expires_at<=recording_campaign_now();

  SELECT o.id,o.approval_id,o.account_id,o.stream_id INTO selected
  FROM recording_targeted_probe_orders o
  JOIN recording_campaign_admission_approvals a ON a.id=o.approval_id
  WHERE a.deadline_at>recording_campaign_now()
    AND NOT EXISTS(SELECT 1 FROM recording_campaign_admission_reservation_terminal_events terminal
      WHERE terminal.approval_id=o.approval_id)
    AND (SELECT count(*) FROM recording_targeted_probe_attempts pa WHERE pa.order_id=o.id)<8
    AND (SELECT count(*) FROM recording_targeted_probe_attempts pa JOIN recording_targeted_probe_evidence pe ON pe.attempt_id=pa.id WHERE pa.order_id=o.id AND pe.result='ok')<o.desired_attempts
    AND NOT EXISTS(SELECT 1 FROM recording_targeted_probe_attempts pa
      LEFT JOIN recording_targeted_probe_evidence pe ON pe.attempt_id=pa.id
      LEFT JOIN recording_targeted_probe_attempt_terminal_events terminal ON terminal.attempt_id=pa.id
      WHERE pa.order_id=o.id AND pe.id IS NULL AND terminal.attempt_id IS NULL)
    AND COALESCE((SELECT max(pe.observed_at) FROM recording_targeted_probe_attempts pa JOIN recording_targeted_probe_evidence pe ON pe.attempt_id=pa.id WHERE pa.order_id=o.id),TIMESTAMPTZ '-infinity')<=recording_campaign_now()-interval '60 seconds'
  ORDER BY o.requested_at,o.id FOR UPDATE OF o SKIP LOCKED LIMIT 1;
  IF selected.id IS NULL THEN RETURN; END IF;
  PERFORM account.id FROM accounts account
    WHERE account.id IN(selected.account_id,(SELECT node.account_id FROM nodes node WHERE node.id=p_node_id))
    ORDER BY account.id FOR SHARE;
  PERFORM 1 FROM nodes WHERE id=p_node_id FOR SHARE;
  PERFORM 1 FROM recorder_droplets WHERE id=p_recorder_droplet_id AND node_id=p_node_id FOR UPDATE;
  IF (SELECT count(*) FROM recording_jobs job JOIN recorder_droplets droplet ON droplet.name=job.lease_owner
        WHERE droplet.node_id=p_node_id AND job.status='leased' AND job.lease_expires_at>recording_campaign_now())+
     (SELECT count(*) FROM recording_targeted_probe_attempts attempt
        LEFT JOIN recording_targeted_probe_evidence evidence ON evidence.attempt_id=attempt.id
        LEFT JOIN recording_targeted_probe_attempt_terminal_events terminal ON terminal.attempt_id=attempt.id
        WHERE attempt.node_id=p_node_id AND attempt.expires_at>recording_campaign_now()
          AND evidence.id IS NULL AND terminal.attempt_id IS NULL)>=
     GREATEST((SELECT capacity FROM recorder_droplets WHERE id=p_recorder_droplet_id)-1,0) THEN
    RETURN;
  END IF;
  PERFORM recording_campaign_authorize_node('attempt',selected.approval_id,selected.account_id,
    p_node_id,p_node_token_id,p_claim_generation,p_credential_sha256);
  SELECT id INTO provider_id FROM recording_campaign_create_provider_attestation(p_node_id,
    p_recorder_droplet_id,p_do_droplet_id,p_project_sha256,p_firewall_sha256,p_region,p_size_slug,
    p_pool_identity_sha256,p_build_sha);
  IF provider_id IS NULL THEN RAISE EXCEPTION 'managed provider/claim-head attestation changed'; END IF;
  SELECT s.provider,s.source_url,s.source_page_url,r.source_revision_id,r.source_url_sha256,
    r.source_page_url_sha256,r.source_updated_at INTO provider,source_url,source_page_url,
    revision_id,source_hash,page_hash,source_updated
  FROM recording_campaign_admission_reservations r JOIN streams s ON s.id=r.stream_id
  WHERE r.approval_id=selected.approval_id AND r.stream_id=selected.stream_id
    AND NOT EXISTS(SELECT 1 FROM recording_campaign_admission_source_fence_events f
      WHERE f.stream_id=r.stream_id AND f.occurred_at>=r.reserved_at) FOR UPDATE OF r;
  IF source_url IS NULL THEN RAISE EXCEPTION 'targeted source fence changed'; END IF;
  PERFORM recording_campaign_create_probe_attempt(p_attempt_id,p_request_id,selected.id,selected.approval_id,
    selected.account_id,selected.stream_id,p_node_id,p_recorder_droplet_id,provider_id,p_do_droplet_id,
    p_region,p_build_sha,revision_id,source_hash,page_hash,source_updated,p_challenge,p_bucket_sha256,
    p_media_key,p_frame_key,p_media_max,p_frame_max);
  order_id:=selected.id; approval_id:=selected.approval_id; account_id:=selected.account_id;
  stream_id:=selected.stream_id; attempt_id:=p_attempt_id; challenge:=p_challenge;
  RETURN NEXT;
END $$;

CREATE OR REPLACE FUNCTION recording_campaign_create_probe_evidence(
  p_attempt_id UUID,p_approval_id UUID,p_account_id BIGINT,p_stream_id BIGINT,p_result TEXT,
  p_valid_ratio DOUBLE PRECISION,p_duration_ms BIGINT,p_segment_count INTEGER,p_frame_sha256 TEXT,
  p_media_sha256 TEXT,p_native_signature_sha256 TEXT,p_challenge_proof_sha256 TEXT,p_video_codec TEXT,
  p_audio_codec TEXT,p_audio_present BOOLEAN,p_video_width INTEGER,p_video_height INTEGER,
  p_actual_fps DOUBLE PRECISION,p_detail TEXT,p_media_size_bytes BIGINT,p_media_etag TEXT,
  p_media_version_id TEXT,p_frame_size_bytes BIGINT,p_frame_etag TEXT,p_frame_version_id TEXT,
  p_archive_bucket_sha256 TEXT,p_media_archive_object_key TEXT,p_media_archive_sha256 TEXT,
  p_media_archive_etag TEXT,p_media_archive_version_id TEXT,p_frame_archive_object_key TEXT,
  p_frame_archive_sha256 TEXT,p_frame_archive_etag TEXT,p_frame_archive_version_id TEXT,
  p_submission_request_sha256 TEXT,p_evidence_sha256 TEXT
) RETURNS TABLE(id UUID,evidence_sha256 TEXT) LANGUAGE sql SET search_path FROM CURRENT AS $$
  INSERT INTO recording_targeted_probe_evidence(
    attempt_id,approval_id,account_id,stream_id,result,valid_ratio,duration_ms,segment_count,
    frame_sha256,media_sha256,native_signature_sha256,challenge_proof_sha256,video_codec,audio_codec,
    audio_present,video_width,video_height,actual_fps,detail,media_size_bytes,media_etag,media_version_id,
    frame_size_bytes,frame_etag,frame_version_id,archive_bucket_sha256,media_archive_object_key,
    media_archive_sha256,media_archive_etag,media_archive_version_id,frame_archive_object_key,
    frame_archive_sha256,frame_archive_etag,frame_archive_version_id,submission_request_sha256,
    retain_until,retention_policy,evidence_sha256
  )
  SELECT p_attempt_id,p_approval_id,p_account_id,p_stream_id,p_result,p_valid_ratio,p_duration_ms,p_segment_count,
    NULLIF(p_frame_sha256,''),NULLIF(p_media_sha256,''),NULLIF(p_native_signature_sha256,''),
    NULLIF(p_challenge_proof_sha256,''),NULLIF(p_video_codec,''),NULLIF(p_audio_codec,''),p_audio_present,
    NULLIF(p_video_width,0),NULLIF(p_video_height,0),p_actual_fps,p_detail,NULLIF(p_media_size_bytes,0),
    NULLIF(p_media_etag,''),NULLIF(p_media_version_id,''),NULLIF(p_frame_size_bytes,0),NULLIF(p_frame_etag,''),
    NULLIF(p_frame_version_id,''),NULLIF(p_archive_bucket_sha256,''),NULLIF(p_media_archive_object_key,''),
    NULLIF(p_media_archive_sha256,''),NULLIF(p_media_archive_etag,''),NULLIF(p_media_archive_version_id,''),
    NULLIF(p_frame_archive_object_key,''),NULLIF(p_frame_archive_sha256,''),NULLIF(p_frame_archive_etag,''),
    NULLIF(p_frame_archive_version_id,''),p_submission_request_sha256,
    CASE WHEN p_result='ok' THEN (a.schedule_spec->>'end_at')::timestamptz+interval '7 days' END,
    CASE WHEN p_result='ok' THEN 'qualification-evidence-campaign-plus-7d-v1' END,p_evidence_sha256
  FROM recording_campaign_admission_approvals a WHERE a.id=p_approval_id
  ON CONFLICT(attempt_id) DO NOTHING
  RETURNING recording_targeted_probe_evidence.id,recording_targeted_probe_evidence.evidence_sha256
$$;

-- Evidence completion is one executor statement.  The API may inspect and
-- hash the immutable quarantine objects before this call, but it cannot split
-- node authorization from the append or write any authority table directly.
-- A committed terminal result is returned before mutable token, source, or
-- attempt-expiry checks so response-loss replay remains available.
CREATE OR REPLACE FUNCTION recording_campaign_submit_probe_evidence(
  p_node_id BIGINT,p_node_token_id BIGINT,p_claim_generation BIGINT,p_credential_sha256 TEXT,
  p_attempt_id UUID,p_request_id UUID,p_approval_id UUID,p_account_id BIGINT,p_stream_id BIGINT,
  p_observation JSONB
) RETURNS TABLE(evidence_id UUID,evidence_sha256 TEXT) LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE existing RECORD; attempt RECORD;
BEGIN
  IF jsonb_typeof(p_observation)<>'object' OR
     NOT (p_observation ?& ARRAY[
       'result','valid_ratio','duration_ms','segment_count','frame_sha256','media_sha256',
       'native_signature_sha256','challenge_proof_sha256','video_codec','audio_codec','audio_present',
       'video_width','video_height','actual_fps','detail','media_size_bytes','media_etag','media_version_id',
       'frame_size_bytes','frame_etag','frame_version_id','archive_bucket_sha256','media_archive_object_key',
       'media_archive_sha256','media_archive_etag','media_archive_version_id','frame_archive_object_key',
       'frame_archive_sha256','frame_archive_etag','frame_archive_version_id','submission_request_sha256','evidence_sha256'
     ]) OR (SELECT count(*) FROM jsonb_object_keys(p_observation))<>32 OR
     (p_observation->>'submission_request_sha256')!~'^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'targeted evidence observation has invalid exact shape';
  END IF;

  SELECT e.id,e.evidence_sha256,e.submission_request_sha256,a.request_id,a.approval_id,a.account_id,a.stream_id,a.node_id
    INTO existing
  FROM recording_targeted_probe_evidence e
  JOIN recording_targeted_probe_attempts a ON a.id=e.attempt_id
  WHERE e.attempt_id=p_attempt_id;
  IF existing.id IS NOT NULL THEN
    IF (existing.request_id,existing.approval_id,existing.account_id,existing.stream_id,existing.node_id) IS DISTINCT FROM
       (p_request_id,p_approval_id,p_account_id,p_stream_id,p_node_id) OR
       existing.submission_request_sha256 IS DISTINCT FROM p_observation->>'submission_request_sha256' THEN
      RAISE EXCEPTION 'targeted evidence request idempotency conflict';
    END IF;
    evidence_id:=existing.id; evidence_sha256:=existing.evidence_sha256;
    RETURN NEXT;
    RETURN;
  END IF;

  PERFORM pg_advisory_xact_lock_shared(hashtextextended('recording-worker-claim-v1',0));
  PERFORM pg_advisory_xact_lock(hashtextextended('recording-surrender-cloud-capacity-v1',0));
  PERFORM pg_advisory_xact_lock(hashtextextended('campaign-admission-capacity-v1',0));

  SELECT * INTO attempt FROM recording_targeted_probe_attempts a
  WHERE a.id=p_attempt_id AND a.request_id=p_request_id AND a.approval_id=p_approval_id
    AND a.account_id=p_account_id AND a.stream_id=p_stream_id AND a.node_id=p_node_id
  FOR UPDATE;
  IF attempt.id IS NULL THEN RAISE EXCEPTION 'targeted evidence attempt identity mismatch'; END IF;
  IF EXISTS(SELECT 1 FROM recording_targeted_probe_attempt_terminal_events terminal
      WHERE terminal.attempt_id=p_attempt_id FOR SHARE) THEN
    RAISE EXCEPTION 'targeted evidence attempt is already terminal without evidence';
  END IF;
  IF recording_campaign_now()>attempt.expires_at OR NOT EXISTS(
    SELECT 1 FROM recorder_droplets d
    WHERE d.id=attempt.recorder_droplet_id AND d.node_id=p_node_id
      AND d.do_droplet_id=attempt.do_droplet_id AND d.region=attempt.region
      AND d.build_sha=attempt.probe_build_sha AND d.state='active'
      AND d.last_seen_at BETWEEN recording_campaign_now()-interval '120 seconds' AND recording_campaign_now()+interval '30 seconds'
    FOR SHARE
  ) THEN RAISE EXCEPTION 'targeted evidence recorder attestation is no longer current'; END IF;

  PERFORM recording_campaign_authorize_node('evidence',p_approval_id,p_account_id,p_node_id,
    p_node_token_id,p_claim_generation,p_credential_sha256);
  RETURN QUERY
  SELECT created.id,created.evidence_sha256
  FROM recording_campaign_create_probe_evidence(
    p_attempt_id,p_approval_id,p_account_id,p_stream_id,p_observation->>'result',
    (p_observation->>'valid_ratio')::double precision,(p_observation->>'duration_ms')::bigint,
    (p_observation->>'segment_count')::integer,p_observation->>'frame_sha256',p_observation->>'media_sha256',
    p_observation->>'native_signature_sha256',p_observation->>'challenge_proof_sha256',
    p_observation->>'video_codec',p_observation->>'audio_codec',(p_observation->>'audio_present')::boolean,
    (p_observation->>'video_width')::integer,(p_observation->>'video_height')::integer,
    (p_observation->>'actual_fps')::double precision,p_observation->>'detail',
    (p_observation->>'media_size_bytes')::bigint,p_observation->>'media_etag',p_observation->>'media_version_id',
    (p_observation->>'frame_size_bytes')::bigint,p_observation->>'frame_etag',p_observation->>'frame_version_id',
    p_observation->>'archive_bucket_sha256',p_observation->>'media_archive_object_key',
    p_observation->>'media_archive_sha256',p_observation->>'media_archive_etag',p_observation->>'media_archive_version_id',
    p_observation->>'frame_archive_object_key',p_observation->>'frame_archive_sha256',
    p_observation->>'frame_archive_etag',p_observation->>'frame_archive_version_id',
    p_observation->>'submission_request_sha256',
    p_observation->>'evidence_sha256'
  ) created;
END $$;

CREATE OR REPLACE FUNCTION recording_campaign_create_scene_presentation(
  p_request_id UUID,p_presented_to BIGINT,p_probe_evidence_id UUID,p_approval_id UUID,p_account_id BIGINT
) RETURNS TABLE(id UUID,presentation_sha256 TEXT) LANGUAGE sql SET search_path FROM CURRENT AS $$
  WITH facts AS (
    SELECT p_request_id request_id,e.approval_id,e.account_id,e.stream_id,e.id probe_evidence_id,
      e.frame_sha256,e.frame_archive_object_key,e.frame_archive_etag,e.frame_archive_version_id,
      e.frame_size_bytes,p_presented_to presented_to,recording_campaign_now() presented_at
    FROM recording_targeted_probe_evidence e
    WHERE e.id=p_probe_evidence_id AND e.approval_id=p_approval_id AND e.account_id=p_account_id
      AND e.result='ok' AND e.frame_archive_sha256=e.frame_sha256
  ), hashed AS (
    SELECT *,encode(sha256(convert_to(jsonb_build_object(
      'request_id',request_id,'approval_id',approval_id,'account_id',account_id,'stream_id',stream_id,
      'probe_evidence_id',probe_evidence_id,'probe_frame_sha256',frame_sha256,
      'frame_archive_object_key',frame_archive_object_key,'frame_archive_etag',frame_archive_etag,
      'frame_archive_version_id',frame_archive_version_id,'frame_size_bytes',frame_size_bytes,
      'presented_to_user_id',presented_to,'presented_at_epoch',extract(epoch from presented_at))::text,'UTF8')),'hex') digest
    FROM facts
  )
  INSERT INTO recording_targeted_probe_scene_presentations(
    request_id,approval_id,account_id,stream_id,probe_evidence_id,probe_frame_sha256,
    frame_archive_object_key,frame_archive_etag,frame_archive_version_id,frame_size_bytes,
    presented_to_user_id,presented_at,presentation_sha256
  ) SELECT request_id,approval_id,account_id,stream_id,probe_evidence_id,frame_sha256,
      frame_archive_object_key,frame_archive_etag,frame_archive_version_id,frame_size_bytes,
      presented_to,presented_at,digest FROM hashed
  ON CONFLICT DO NOTHING
  RETURNING recording_targeted_probe_scene_presentations.id,recording_targeted_probe_scene_presentations.presentation_sha256
$$;

CREATE OR REPLACE FUNCTION recording_campaign_create_scene_review(
  p_request_id UUID,p_reviewer BIGINT,p_probe_evidence_id UUID,p_presentation_id UUID,p_approval_id UUID,p_account_id BIGINT
) RETURNS TABLE(id UUID,review_sha256 TEXT) LANGUAGE sql SET search_path FROM CURRENT AS $$
  WITH facts AS (
    SELECT p_request_id request_id,e.approval_id,e.account_id,e.stream_id,e.id probe_evidence_id,
      e.frame_sha256,p_presentation_id presentation_id,r.scene_frame_evidence_id,r.scene_identity_sha256,p_reviewer reviewer,
      recording_campaign_now() reviewed_at
    FROM recording_targeted_probe_evidence e
    JOIN recording_campaign_admission_reservations r ON r.approval_id=e.approval_id AND r.stream_id=e.stream_id
    WHERE e.id=p_probe_evidence_id AND e.approval_id=p_approval_id AND e.account_id=p_account_id AND e.result='ok'
  ), hashed AS (
    SELECT *,encode(sha256(convert_to(jsonb_build_object(
      'request_id',request_id,'approval_id',approval_id,'account_id',account_id,'stream_id',stream_id,
      'probe_evidence_id',probe_evidence_id,'presentation_id',presentation_id,'probe_frame_sha256',frame_sha256,
      'scene_frame_evidence_id',scene_frame_evidence_id,'scene_identity_sha256',scene_identity_sha256,
      'reviewed_by_user_id',reviewer,'reviewed_at_epoch',extract(epoch from reviewed_at))::text,'UTF8')),'hex') digest
    FROM facts
  )
  INSERT INTO recording_targeted_probe_scene_reviews(
    request_id,approval_id,account_id,stream_id,probe_evidence_id,presentation_id,probe_frame_sha256,
    scene_frame_evidence_id,scene_identity_sha256,reviewed_by_user_id,reviewed_at,review_sha256
  ) SELECT request_id,approval_id,account_id,stream_id,probe_evidence_id,presentation_id,frame_sha256,
      scene_frame_evidence_id,scene_identity_sha256,reviewer,reviewed_at,digest FROM hashed
  ON CONFLICT DO NOTHING
  RETURNING recording_targeted_probe_scene_reviews.id,recording_targeted_probe_scene_reviews.review_sha256
$$;

-- Public executor entry points.  Low-level create/authorize procedures remain
-- authority-private; each supported operation is one statement with terminal
-- replay before mutable credential/deadline/source checks.
CREATE FUNCTION recording_campaign_replay(
  p_approval_id UUID,p_account_id BIGINT,p_credential_sha256 TEXT
) RETURNS JSONB LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE response JSONB;
BEGIN
  IF p_credential_sha256!~'^[0-9a-f]{64}$' THEN RAISE EXCEPTION 'invalid campaign replay credential'; END IF;
  SELECT c.response_json INTO response
  FROM recording_campaign_admission_commits c
  JOIN recording_campaign_admission_tx_authorizations witness
    ON witness.approval_id=c.approval_id AND witness.account_id=c.account_id AND witness.action='admit'
  JOIN account_sessions session ON session.id=witness.account_session_id
  WHERE c.approval_id=p_approval_id AND c.account_id=p_account_id
    AND session.session_hash=p_credential_sha256
  ORDER BY witness.authorized_at DESC LIMIT 1;
  RETURN response;
END $$;

CREATE FUNCTION recording_campaign_replay_approval(
  p_account_id BIGINT,p_request_id UUID,p_request_sha256 TEXT,p_credential_sha256 TEXT
) RETURNS TABLE(approval_id UUID,approval_sha256 TEXT,entries JSONB)
LANGUAGE sql SET search_path FROM CURRENT AS $$
  SELECT a.id,a.approval_sha256,a.entries
  FROM recording_campaign_admission_approvals a
  WHERE a.account_id=p_account_id AND a.request_id=p_request_id AND a.request_sha256=p_request_sha256
    AND p_credential_sha256~'^[0-9a-f]{64}$'
    AND EXISTS(SELECT 1 FROM account_sessions session
      WHERE session.account_id=a.account_id AND session.user_id=a.actor_user_id
        AND session.session_hash=p_credential_sha256)
$$;

CREATE OR REPLACE FUNCTION recording_campaign_approve(
  p_request_id UUID,p_account_id BIGINT,p_user_id BIGINT,p_session_id BIGINT,p_credential_sha256 TEXT,
  p_actor_email TEXT,p_authority_code TEXT,p_failure_domain_tag TEXT,p_deadline_at TIMESTAMPTZ,
  p_entries JSONB,p_schedule_spec JSONB,p_request_sha256 TEXT
) RETURNS TABLE(approval_id UUID,approval_sha256 TEXT) LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE existing RECORD;
BEGIN
  SELECT a.id,a.approval_sha256,a.request_sha256,a.actor_user_id,a.actor_email_snapshot,
    a.authority_code,a.failure_domain_tag,a.deadline_at,a.entries,a.schedule_spec INTO existing
  FROM recording_campaign_admission_approvals a
  WHERE a.account_id=p_account_id AND a.request_id=p_request_id
    AND EXISTS(SELECT 1 FROM account_sessions session WHERE session.account_id=a.account_id
      AND session.user_id=a.actor_user_id AND session.session_hash=p_credential_sha256);
  IF existing.id IS NOT NULL THEN
    IF existing.request_sha256<>p_request_sha256 OR
       (existing.actor_user_id,existing.actor_email_snapshot,existing.authority_code,existing.failure_domain_tag,
        existing.deadline_at,existing.entries,existing.schedule_spec) IS DISTINCT FROM
       (p_user_id,lower(btrim(p_actor_email)),p_authority_code,NULLIF(p_failure_domain_tag,''),
        p_deadline_at,p_entries,p_schedule_spec) THEN
      RAISE EXCEPTION 'campaign approval idempotency conflict';
    END IF;
    approval_id:=existing.id; approval_sha256:=existing.approval_sha256; RETURN NEXT; RETURN;
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('campaign-admission-capacity-v1',0));
  PERFORM 1 FROM accounts WHERE id=p_account_id FOR UPDATE;
  PERFORM recording_campaign_authorize_account('approve',NULL,p_account_id,p_user_id,p_session_id,p_credential_sha256);
  RETURN QUERY SELECT created.id,created.approval_sha256
  FROM recording_campaign_create_approval(p_request_id,p_account_id,p_user_id,p_actor_email,
    p_authority_code,p_failure_domain_tag,p_deadline_at,p_entries,p_schedule_spec,p_request_sha256) created;
END $$;

CREATE FUNCTION recording_campaign_expire_approval(
  p_request_id UUID,p_approval_id UUID,p_account_id BIGINT,p_user_id BIGINT,p_session_id BIGINT,p_credential_sha256 TEXT
) RETURNS TABLE(event_sha256 TEXT) LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE existing RECORD; approval RECORD; digest TEXT;
BEGIN
  SELECT terminal.event_sha256,terminal.actor_user_id INTO existing
  FROM recording_campaign_admission_reservation_terminal_events terminal
  WHERE terminal.account_id=p_account_id AND terminal.request_id=p_request_id
    AND terminal.approval_id=p_approval_id
    AND EXISTS(SELECT 1 FROM account_sessions session WHERE session.id=p_session_id
      AND session.account_id=terminal.account_id AND session.user_id=terminal.actor_user_id
      AND session.session_hash=p_credential_sha256);
  IF existing.event_sha256 IS NOT NULL THEN
    IF existing.actor_user_id<>p_user_id THEN RAISE EXCEPTION 'campaign expiration idempotency conflict'; END IF;
    event_sha256:=existing.event_sha256; RETURN NEXT; RETURN;
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('campaign-admission-capacity-v1',0));
  PERFORM 1 FROM accounts WHERE id=p_account_id FOR UPDATE;
  PERFORM recording_campaign_authorize_account('expire',p_approval_id,p_account_id,p_user_id,p_session_id,p_credential_sha256);
  SELECT * INTO approval FROM recording_campaign_admission_approvals
    WHERE id=p_approval_id AND account_id=p_account_id FOR UPDATE;
  -- Probe orders are the concrete leasing heads. Locking them while holding
  -- the shared admission fence makes expiry and lease commit in one exact
  -- order; a later lease cannot author an attempt behind a terminal event.
  PERFORM 1 FROM recording_targeted_probe_orders
    WHERE approval_id=p_approval_id ORDER BY id FOR UPDATE;
  IF approval.id IS NULL OR approval.deadline_at>recording_campaign_now() OR
     EXISTS(SELECT 1 FROM recording_campaign_admission_commits WHERE approval_id=p_approval_id) THEN
    RAISE EXCEPTION 'campaign approval is not expired and unadmitted';
  END IF;
  INSERT INTO recording_targeted_probe_attempt_terminal_events(attempt_id,result,event_sha256)
  SELECT attempt.id,'expired_without_evidence',encode(sha256(
	convert_to('expired_without_evidence','UTF8')
	||decode('00','hex')||convert_to(attempt.id::text,'UTF8')
	||decode('00','hex')||convert_to(extract(epoch from recording_campaign_now())::text,'UTF8')),'hex')
  FROM recording_targeted_probe_attempts attempt
  LEFT JOIN recording_targeted_probe_evidence evidence ON evidence.attempt_id=attempt.id
  LEFT JOIN recording_targeted_probe_attempt_terminal_events terminal ON terminal.attempt_id=attempt.id
  WHERE attempt.approval_id=p_approval_id AND evidence.id IS NULL AND terminal.attempt_id IS NULL
    AND attempt.expires_at<=recording_campaign_now();
  IF EXISTS(SELECT 1 FROM recording_targeted_probe_attempts attempt
      LEFT JOIN recording_targeted_probe_evidence evidence ON evidence.attempt_id=attempt.id
      LEFT JOIN recording_targeted_probe_attempt_terminal_events terminal ON terminal.attempt_id=attempt.id
      WHERE attempt.approval_id=p_approval_id AND evidence.id IS NULL AND terminal.attempt_id IS NULL
        AND attempt.expires_at>recording_campaign_now() FOR UPDATE OF attempt) THEN
    RAISE EXCEPTION 'campaign approval still has an in-flight targeted probe';
  END IF;
  digest:=encode(sha256(convert_to(jsonb_build_object('approval_id',p_approval_id,'account_id',p_account_id,
    'request_id',p_request_id,'result','expired_unadmitted','actor_user_id',p_user_id,
    'observed_at',recording_campaign_now())::text,'UTF8')),'hex');
  INSERT INTO recording_campaign_admission_reservation_terminal_events(
    approval_id,account_id,request_id,result,actor_user_id,event_sha256
  ) VALUES(p_approval_id,p_account_id,p_request_id,'expired_unadmitted',p_user_id,digest);
  event_sha256:=digest; RETURN NEXT;
END $$;

CREATE FUNCTION recording_campaign_read_probe_attempt(
  p_node_id BIGINT,p_node_token_id BIGINT,p_claim_generation BIGINT,p_credential_sha256 TEXT,
  p_attempt_id UUID,p_request_id UUID,p_approval_id UUID,p_stream_id BIGINT
) RETURNS TABLE(account_id BIGINT,challenge TEXT,media_object_key TEXT,frame_object_key TEXT,
  media_max_size_bytes BIGINT,frame_max_size_bytes BIGINT,terminal BOOLEAN,
  evidence_id UUID,evidence_sha256 TEXT,submission_request_sha256 TEXT,result TEXT,detail TEXT)
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE attempt RECORD;
BEGIN
  SELECT a.account_id,a.challenge,a.media_object_key,a.frame_object_key,a.media_max_size_bytes,
    a.frame_max_size_bytes,e.id evidence_id,e.evidence_sha256,e.submission_request_sha256,e.result,e.detail INTO attempt
  FROM recording_targeted_probe_attempts a LEFT JOIN recording_targeted_probe_evidence e ON e.attempt_id=a.id
  WHERE a.id=p_attempt_id AND a.request_id=p_request_id AND a.approval_id=p_approval_id
    AND a.stream_id=p_stream_id AND a.node_id=p_node_id FOR SHARE;
  IF attempt.account_id IS NULL THEN RAISE EXCEPTION 'targeted attempt identity does not match'; END IF;
  IF attempt.evidence_id IS NULL THEN
    PERFORM recording_campaign_authorize_node('evidence',p_approval_id,attempt.account_id,p_node_id,
      p_node_token_id,p_claim_generation,p_credential_sha256);
  END IF;
  RETURN QUERY SELECT attempt.account_id,attempt.challenge,attempt.media_object_key,attempt.frame_object_key,
    attempt.media_max_size_bytes,attempt.frame_max_size_bytes,attempt.evidence_id IS NOT NULL,attempt.evidence_id,
    attempt.evidence_sha256,attempt.submission_request_sha256,attempt.result,attempt.detail;
END $$;

CREATE OR REPLACE FUNCTION recording_campaign_queue_probe(
  p_request_id UUID,p_approval_id UUID,p_account_id BIGINT,p_user_id BIGINT,p_session_id BIGINT,
  p_credential_sha256 TEXT,p_stream_id BIGINT
) RETURNS TABLE(order_id UUID) LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE existing RECORD;
BEGIN
  SELECT o.id,o.approval_id,o.stream_id,o.requested_by_user_id INTO existing
  FROM recording_targeted_probe_orders o WHERE o.account_id=p_account_id AND o.request_id=p_request_id
    AND EXISTS(SELECT 1 FROM account_sessions session WHERE session.account_id=o.account_id
      AND session.user_id=o.requested_by_user_id AND session.session_hash=p_credential_sha256);
  IF existing.id IS NOT NULL THEN
    IF (existing.approval_id,existing.stream_id,existing.requested_by_user_id) IS DISTINCT FROM
       (p_approval_id,p_stream_id,p_user_id) THEN RAISE EXCEPTION 'targeted probe order idempotency conflict'; END IF;
    order_id:=existing.id; RETURN NEXT; RETURN;
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('campaign-admission-capacity-v1',0));
  PERFORM 1 FROM accounts WHERE id=p_account_id FOR UPDATE;
  IF EXISTS(SELECT 1 FROM recording_campaign_admission_reservation_terminal_events
      WHERE approval_id=p_approval_id) THEN
    RAISE EXCEPTION 'expired campaign approval cannot queue a targeted probe';
  END IF;
  PERFORM recording_campaign_authorize_account('queue',p_approval_id,p_account_id,p_user_id,p_session_id,p_credential_sha256);
  RETURN QUERY SELECT created.id FROM recording_campaign_create_probe_order(
    p_request_id,p_approval_id,p_account_id,p_stream_id,p_user_id) created;
END $$;

CREATE OR REPLACE FUNCTION recording_campaign_review_probe_scene(
  p_request_id UUID,p_approval_id UUID,p_account_id BIGINT,p_user_id BIGINT,p_session_id BIGINT,
  p_credential_sha256 TEXT,p_probe_evidence_id UUID,p_presentation_id UUID
) RETURNS TABLE(review_id UUID,review_sha256 TEXT) LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE existing RECORD;
BEGIN
  SELECT r.id,r.review_sha256,r.approval_id,r.probe_evidence_id,r.presentation_id,r.reviewed_by_user_id INTO existing
  FROM recording_targeted_probe_scene_reviews r WHERE r.account_id=p_account_id AND r.request_id=p_request_id
    AND EXISTS(SELECT 1 FROM account_sessions session WHERE session.account_id=r.account_id
      AND session.user_id=r.reviewed_by_user_id AND session.session_hash=p_credential_sha256);
  IF existing.id IS NOT NULL THEN
    IF (existing.approval_id,existing.probe_evidence_id,existing.presentation_id,existing.reviewed_by_user_id) IS DISTINCT FROM
       (p_approval_id,p_probe_evidence_id,p_presentation_id,p_user_id) THEN RAISE EXCEPTION 'targeted scene review idempotency conflict'; END IF;
    review_id:=existing.id; review_sha256:=existing.review_sha256; RETURN NEXT; RETURN;
  END IF;
  PERFORM recording_campaign_authorize_account('review',p_approval_id,p_account_id,p_user_id,p_session_id,p_credential_sha256);
  RETURN QUERY SELECT created.id,created.review_sha256 FROM recording_campaign_create_scene_review(
    p_request_id,p_user_id,p_probe_evidence_id,p_presentation_id,p_approval_id,p_account_id) created;
END $$;

CREATE FUNCTION recording_campaign_read_probe_scene(
  p_account_id BIGINT,p_user_id BIGINT,p_session_id BIGINT,p_credential_sha256 TEXT,p_probe_evidence_id UUID
) RETURNS TABLE(approval_id UUID,frame_archive_object_key TEXT,frame_archive_etag TEXT,frame_archive_version_id TEXT,
  frame_archive_sha256 TEXT,frame_size_bytes BIGINT)
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE bound_approval UUID;
BEGIN
  SELECT e.approval_id INTO bound_approval FROM recording_targeted_probe_evidence e
    WHERE e.id=p_probe_evidence_id AND e.account_id=p_account_id AND e.result='ok';
  IF bound_approval IS NULL THEN RAISE EXCEPTION 'targeted scene evidence was not found'; END IF;
  PERFORM recording_campaign_authorize_account('present',bound_approval,p_account_id,p_user_id,p_session_id,p_credential_sha256);
  RETURN QUERY SELECT e.approval_id,e.frame_archive_object_key,e.frame_archive_etag,e.frame_archive_version_id,
    e.frame_archive_sha256,e.frame_size_bytes
  FROM recording_targeted_probe_evidence e
  WHERE e.id=p_probe_evidence_id AND e.approval_id=bound_approval AND e.account_id=p_account_id
    AND e.result='ok' AND e.frame_archive_sha256=e.frame_sha256 FOR SHARE;
  IF NOT FOUND THEN RAISE EXCEPTION 'targeted scene evidence was not found'; END IF;
END $$;

-- Read-only preparation returns the exact protected frame binding, but does
-- not create a presentation receipt. The API must successfully GET and hash
-- these bytes before calling recording_campaign_present_baseline_scene, which
-- rechecks the same source/frame fence and appends the immutable receipt.
CREATE FUNCTION recording_campaign_read_baseline_scene(
  p_request_id UUID,p_account_id BIGINT,p_user_id BIGINT,p_session_id BIGINT,p_credential_sha256 TEXT,
  p_authority_code TEXT,p_stream_id BIGINT,p_frame_id BIGINT
) RETURNS TABLE(read_receipt_id UUID,frame_sha256 TEXT,media_object_key TEXT,media_etag TEXT,media_size_bytes BIGINT)
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE existing RECORD; decision_ok BOOLEAN; revision BIGINT; source_hash TEXT; page_hash TEXT; source_updated TIMESTAMPTZ; frame RECORD; digest TEXT;
BEGIN
  SELECT * INTO existing FROM recording_campaign_baseline_scene_read_receipts
    WHERE account_id=p_account_id AND request_id=p_request_id AND
      (expires_at>recording_campaign_now() OR EXISTS(
        SELECT 1 FROM recording_campaign_baseline_scene_presentations presented
        WHERE presented.read_receipt_id=recording_campaign_baseline_scene_read_receipts.id
          AND presented.account_id=p_account_id AND presented.request_id=p_request_id))
      AND EXISTS(SELECT 1 FROM account_sessions session WHERE session.id=p_session_id
        AND session.account_id=recording_campaign_baseline_scene_read_receipts.account_id
        AND session.user_id=recording_campaign_baseline_scene_read_receipts.read_by_user_id
        AND session.session_hash=p_credential_sha256 AND session.revoked_at IS NULL
        AND session.expires_at>recording_campaign_now());
  IF existing.id IS NOT NULL THEN
    IF (existing.authority_code,existing.stream_id,existing.frame_id,existing.read_by_user_id) IS DISTINCT FROM
       (p_authority_code,p_stream_id,p_frame_id,p_user_id) THEN RAISE EXCEPTION 'baseline read receipt idempotency conflict'; END IF;
    RETURN QUERY SELECT existing.id,existing.frame_sha256,existing.media_object_key,existing.media_etag,existing.media_size_bytes;
    RETURN;
  END IF;
  PERFORM recording_campaign_authorize_account('baseline_present',NULL,p_account_id,p_user_id,p_session_id,p_credential_sha256);
  SELECT EXISTS(SELECT 1 FROM recording_campaign_authority_decisions
    WHERE code=p_authority_code AND campaign_key='delivery30-2026q3'
      AND expires_at>recording_campaign_now() AND p_stream_id=ANY(permitted_stream_ids))
    INTO decision_ok;
  PERFORM 1 FROM stream_source_revisions WHERE stream_id=p_stream_id ORDER BY id FOR SHARE;
  SELECT (SELECT max(id) FROM stream_source_revisions WHERE stream_id=s.id),
    encode(sha256(convert_to(source_url,'UTF8')),'hex'),
    encode(sha256(convert_to(COALESCE(source_page_url,''),'UTF8')),'hex'),updated_at
    INTO revision,source_hash,page_hash,source_updated FROM streams s
    WHERE s.id=p_stream_id AND s.deleted_at IS NULL FOR SHARE;
  IF decision_ok IS DISTINCT FROM true OR source_hash IS NULL THEN
    RAISE EXCEPTION 'baseline frame is not current and decision-authorized';
  END IF;
  SELECT f.raw_media_object_id,lower(m.sha256) sha,m.object_key,COALESCE(m.etag,'') etag,m.size_bytes
    INTO frame
    FROM frames f JOIN media_objects m ON m.id=f.raw_media_object_id
    WHERE f.id=p_frame_id AND f.stream_id=p_stream_id AND f.capture_status='success'
      AND f.captured_at BETWEEN recording_campaign_now()-interval '6 hours' AND recording_campaign_now()
      AND m.size_bytes BETWEEN 1 AND 8388608 FOR SHARE OF f,m;
  IF frame.raw_media_object_id IS NULL THEN RAISE EXCEPTION 'baseline frame bytes are not current'; END IF;
  digest:=encode(sha256(convert_to(jsonb_build_object('request_id',p_request_id,'authority_code',p_authority_code,
    'account_id',p_account_id,'stream_id',p_stream_id,'frame_id',p_frame_id,'media_object_id',frame.raw_media_object_id,
    'frame_sha256',frame.sha,'media_object_key',frame.object_key,'media_etag',frame.etag,'media_size_bytes',frame.size_bytes,
    'source_revision_id',revision,'source_url_sha256',source_hash,'source_page_url_sha256',page_hash,
    'source_updated_at_epoch_us',floor(extract(epoch from source_updated)*1000000)::bigint,
    'read_by_user_id',p_user_id,'read_at_epoch_us',floor(extract(epoch from recording_campaign_now())*1000000)::bigint)::text,'UTF8')),'hex');
  INSERT INTO recording_campaign_baseline_scene_read_receipts(request_id,authority_code,account_id,stream_id,frame_id,
    media_object_id,frame_sha256,media_object_key,media_etag,media_size_bytes,source_revision_id,source_url_sha256,
    source_page_url_sha256,source_updated_at,read_by_user_id,receipt_sha256)
  VALUES(p_request_id,p_authority_code,p_account_id,p_stream_id,p_frame_id,frame.raw_media_object_id,frame.sha,
    frame.object_key,frame.etag,frame.size_bytes,revision,source_hash,page_hash,source_updated,p_user_id,digest)
  RETURNING id,recording_campaign_baseline_scene_read_receipts.frame_sha256,
    recording_campaign_baseline_scene_read_receipts.media_object_key,
    recording_campaign_baseline_scene_read_receipts.media_etag,
    recording_campaign_baseline_scene_read_receipts.media_size_bytes
  INTO read_receipt_id,frame_sha256,media_object_key,media_etag,media_size_bytes;
  RETURN NEXT;
END $$;

CREATE FUNCTION recording_campaign_present_baseline_scene(
  p_request_id UUID,p_read_receipt_id UUID,p_account_id BIGINT,p_user_id BIGINT,p_session_id BIGINT,p_credential_sha256 TEXT,
  p_authority_code TEXT,p_stream_id BIGINT,p_frame_id BIGINT
) RETURNS TABLE(presentation_id UUID,frame_sha256 TEXT,media_object_key TEXT,media_etag TEXT) LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE existing RECORD; receipt RECORD; decision RECORD; revision BIGINT; source_hash TEXT; page_hash TEXT; source_updated TIMESTAMPTZ; digest TEXT;
BEGIN
  SELECT * INTO existing FROM recording_campaign_baseline_scene_presentations
    WHERE account_id=p_account_id AND request_id=p_request_id
      AND EXISTS(SELECT 1 FROM account_sessions session WHERE session.account_id=recording_campaign_baseline_scene_presentations.account_id
        AND session.user_id=recording_campaign_baseline_scene_presentations.presented_to_user_id
        AND session.session_hash=p_credential_sha256);
  IF existing.id IS NOT NULL THEN
    IF (existing.read_receipt_id,existing.authority_code,existing.stream_id,existing.frame_id,existing.presented_to_user_id) IS DISTINCT FROM
       (p_read_receipt_id,p_authority_code,p_stream_id,p_frame_id,p_user_id) THEN RAISE EXCEPTION 'baseline presentation idempotency conflict'; END IF;
    presentation_id:=existing.id; frame_sha256:=existing.frame_sha256;
    media_object_key:=existing.media_object_key; media_etag:=existing.media_etag; RETURN NEXT; RETURN;
  END IF;
  PERFORM recording_campaign_authorize_account('baseline_present',NULL,p_account_id,p_user_id,p_session_id,p_credential_sha256);
  SELECT * INTO decision FROM recording_campaign_authority_decisions
    WHERE code=p_authority_code AND campaign_key='delivery30-2026q3' AND expires_at>recording_campaign_now()
      AND p_stream_id=ANY(permitted_stream_ids) FOR SHARE;
  SELECT * INTO receipt FROM recording_campaign_baseline_scene_read_receipts
    WHERE id=p_read_receipt_id AND request_id=p_request_id AND account_id=p_account_id AND stream_id=p_stream_id
      AND frame_id=p_frame_id AND authority_code=p_authority_code AND read_by_user_id=p_user_id
      AND expires_at>recording_campaign_now() FOR SHARE;
  PERFORM 1 FROM stream_source_revisions WHERE stream_id=p_stream_id ORDER BY id FOR SHARE;
  SELECT (SELECT max(id) FROM stream_source_revisions WHERE stream_id=s.id),
    encode(sha256(convert_to(source_url,'UTF8')),'hex'),
    encode(sha256(convert_to(COALESCE(source_page_url,''),'UTF8')),'hex'),updated_at
    INTO revision,source_hash,page_hash,source_updated FROM streams s
    WHERE s.id=p_stream_id AND s.deleted_at IS NULL FOR SHARE;
  IF decision.code IS NULL OR receipt.id IS NULL OR source_hash IS NULL OR
     (receipt.source_revision_id,receipt.source_url_sha256,receipt.source_page_url_sha256,receipt.source_updated_at) IS DISTINCT FROM
     (revision,source_hash,page_hash,source_updated) THEN
    RAISE EXCEPTION 'baseline frame is not current and decision-authorized';
  END IF;
  digest:=encode(sha256(convert_to(jsonb_build_object('request_id',p_request_id,'authority_code',p_authority_code,
    'account_id',p_account_id,'stream_id',p_stream_id,'frame_id',p_frame_id,'read_receipt_id',p_read_receipt_id,
    'media_object_id',receipt.media_object_id,'frame_sha256',receipt.frame_sha256,
    'media_object_key',receipt.media_object_key,'media_etag',receipt.media_etag,
    'source_revision_id',revision,'source_url_sha256',source_hash,'source_page_url_sha256',page_hash,
    'source_updated_at_epoch',extract(epoch from source_updated),'presented_to_user_id',p_user_id,
    'presented_at_epoch',extract(epoch from recording_campaign_now()))::text,'UTF8')),'hex');
  INSERT INTO recording_campaign_baseline_scene_presentations(request_id,authority_code,account_id,stream_id,
    frame_id,read_receipt_id,media_object_id,frame_sha256,media_object_key,media_etag,source_revision_id,source_url_sha256,
    source_page_url_sha256,source_updated_at,presented_to_user_id,presentation_sha256)
  VALUES(p_request_id,p_authority_code,p_account_id,p_stream_id,p_frame_id,p_read_receipt_id,receipt.media_object_id,receipt.frame_sha256,
    receipt.media_object_key,receipt.media_etag,revision,source_hash,page_hash,source_updated,p_user_id,digest)
  RETURNING id,recording_campaign_baseline_scene_presentations.frame_sha256,
    recording_campaign_baseline_scene_presentations.media_object_key,
    recording_campaign_baseline_scene_presentations.media_etag
  INTO presentation_id,frame_sha256,media_object_key,media_etag;
  RETURN NEXT;
END $$;

CREATE FUNCTION recording_campaign_attest_baseline_scene(
  p_presentation_id UUID,p_account_id BIGINT,p_user_id BIGINT,p_session_id BIGINT,p_credential_sha256 TEXT,
  p_frame_id BIGINT,p_scene_identity_sha256 TEXT
) RETURNS TABLE(evidence_id BIGINT,evidence_sha256 TEXT) LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE presented RECORD; current_revision BIGINT; current_source TEXT; current_page TEXT; current_updated TIMESTAMPTZ;
BEGIN
  PERFORM recording_campaign_authorize_account('baseline_attest',NULL,p_account_id,p_user_id,p_session_id,p_credential_sha256);
  SELECT * INTO presented FROM recording_campaign_baseline_scene_presentations WHERE id=p_presentation_id
    AND account_id=p_account_id AND frame_id=p_frame_id AND presented_to_user_id=p_user_id
    AND presented_at>=recording_campaign_now()-interval '30 minutes' FOR SHARE;
  PERFORM 1 FROM stream_source_revisions WHERE stream_id=presented.stream_id ORDER BY id FOR SHARE;
  SELECT (SELECT max(id) FROM stream_source_revisions WHERE stream_id=s.id),
    encode(sha256(convert_to(s.source_url,'UTF8')),'hex'),
    encode(sha256(convert_to(COALESCE(s.source_page_url,''),'UTF8')),'hex'),s.updated_at
    INTO current_revision,current_source,current_page,current_updated
    FROM streams s WHERE s.id=presented.stream_id AND s.deleted_at IS NULL FOR SHARE;
  IF presented.id IS NULL OR p_scene_identity_sha256!~'^[0-9a-f]{64}$' OR
     (presented.source_revision_id,presented.source_url_sha256,presented.source_page_url_sha256,presented.source_updated_at) IS DISTINCT FROM
     (current_revision,current_source,current_page,current_updated) THEN
    RAISE EXCEPTION 'baseline presentation/source fence changed';
  END IF;
  INSERT INTO recording_scene_frame_evidence(account_id,stream_id,frame_id,media_object_id,captured_at,
    frame_sha256,scene_identity_sha256,verification_method,verified_by_user_id)
  SELECT p_account_id,presented.stream_id,presented.frame_id,presented.media_object_id,f.captured_at,
    presented.frame_sha256,p_scene_identity_sha256,'operator_visual',p_user_id
  FROM frames f WHERE f.id=presented.frame_id
  ON CONFLICT(account_id,frame_id) DO NOTHING
  RETURNING id,recording_scene_frame_evidence.evidence_sha256 INTO evidence_id,evidence_sha256;
  IF evidence_id IS NULL THEN
    SELECT id,recording_scene_frame_evidence.evidence_sha256 INTO evidence_id,evidence_sha256
    FROM recording_scene_frame_evidence WHERE account_id=p_account_id AND frame_id=presented.frame_id
      AND scene_identity_sha256=p_scene_identity_sha256 AND verified_by_user_id=p_user_id;
  END IF;
  IF evidence_id IS NULL THEN RAISE EXCEPTION 'baseline scene attestation conflicts with immutable evidence'; END IF;
  RETURN NEXT;
END $$;

CREATE OR REPLACE FUNCTION recording_campaign_present_probe_scene(
  p_request_id UUID,p_approval_id UUID,p_account_id BIGINT,p_user_id BIGINT,p_session_id BIGINT,
  p_credential_sha256 TEXT,p_probe_evidence_id UUID
) RETURNS TABLE(presentation_id UUID,presentation_sha256 TEXT) LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE existing RECORD;
BEGIN
  SELECT p.id,p.presentation_sha256,p.approval_id,p.probe_evidence_id,p.presented_to_user_id INTO existing
  FROM recording_targeted_probe_scene_presentations p WHERE p.account_id=p_account_id AND p.request_id=p_request_id
    AND EXISTS(SELECT 1 FROM account_sessions session WHERE session.account_id=p.account_id
      AND session.user_id=p.presented_to_user_id AND session.session_hash=p_credential_sha256);
  IF existing.id IS NOT NULL THEN
    IF (existing.approval_id,existing.probe_evidence_id,existing.presented_to_user_id) IS DISTINCT FROM
       (p_approval_id,p_probe_evidence_id,p_user_id) THEN RAISE EXCEPTION 'targeted scene presentation idempotency conflict'; END IF;
    presentation_id:=existing.id; presentation_sha256:=existing.presentation_sha256; RETURN NEXT; RETURN;
  END IF;
  PERFORM recording_campaign_authorize_account('present',p_approval_id,p_account_id,p_user_id,p_session_id,p_credential_sha256);
  RETURN QUERY SELECT created.id,created.presentation_sha256 FROM recording_campaign_create_scene_presentation(
    p_request_id,p_user_id,p_probe_evidence_id,p_approval_id,p_account_id) created;
END $$;

CREATE OR REPLACE FUNCTION recording_campaign_create_capacity_observation(
  p_approval_id UUID,p_account_id BIGINT,p_observation_started_at TIMESTAMPTZ,p_observed_at TIMESTAMPTZ,p_build_sha TEXT,
  p_size_slug TEXT,p_pool_identity_sha256 TEXT,
  p_ready_workers INTEGER,p_total_slots INTEGER,p_largest_worker_slots INTEGER,
  p_usable_after_worker_loss INTEGER,p_largest_region TEXT,p_largest_region_slots INTEGER,
  p_relay_active_demand INTEGER,p_relay_failure_domains INTEGER,p_relay_effective_capacity INTEGER,
  p_relay_usable_after_largest_loss INTEGER,
  p_project_sha256 TEXT,p_firewall_sha256 TEXT,p_facts_sha256 TEXT,p_provider_observation_sha256 TEXT
) RETURNS TABLE(id UUID) LANGUAGE sql SET search_path FROM CURRENT AS $$
  INSERT INTO recording_campaign_capacity_observations(
    approval_id,account_id,observation_started_at,observed_at,expires_at,build_sha,size_slug,pool_identity_sha256,ready_workers,total_slots,
    largest_worker_slots,usable_after_worker_loss,largest_region,largest_region_slots,relay_active_demand,
    relay_failure_domains,relay_effective_capacity,relay_usable_after_largest_loss,
    provider_project_sha256,provider_firewall_sha256,facts_sha256,provider_observation_sha256
  ) VALUES(p_approval_id,p_account_id,p_observation_started_at,p_observed_at,p_observed_at+interval '120 seconds',p_build_sha,
    p_size_slug,p_pool_identity_sha256,p_ready_workers,p_total_slots,p_largest_worker_slots,p_usable_after_worker_loss,p_largest_region,
    p_largest_region_slots,p_relay_active_demand,p_relay_failure_domains,p_relay_effective_capacity,
    p_relay_usable_after_largest_loss,p_project_sha256,p_firewall_sha256,p_facts_sha256,p_provider_observation_sha256)
  RETURNING recording_campaign_capacity_observations.id
$$;

CREATE OR REPLACE FUNCTION recording_campaign_create_capacity_reservation(
  p_approval_id UUID,p_account_id BIGINT,p_observation_id UUID,p_forecast_peak_slots INTEGER,
  p_active_roster_after INTEGER
) RETURNS void LANGUAGE sql SET search_path FROM CURRENT AS $$
  INSERT INTO recording_campaign_capacity_reservations(
    approval_id,account_id,observation_id,forecast_peak_slots,active_roster_after,roster_cap
  ) VALUES(p_approval_id,p_account_id,p_observation_id,p_forecast_peak_slots,p_active_roster_after,60)
$$;

CREATE OR REPLACE FUNCTION recording_campaign_create_storage_observation(
  p_approval_id UUID,p_account_id BIGINT,p_connection_id BIGINT,p_nas_reported_at TIMESTAMPTZ,
  p_nas_total_bytes BIGINT,p_nas_free_bytes BIGINT,p_measured_24h_bytes BIGINT,p_measured_streams INTEGER,
  p_active_roster_after INTEGER,p_projected_daily_bytes BIGINT,p_campaign_days_with_reserve INTEGER,
  p_required_free_bytes BIGINT,p_projected_free_after_bytes BIGINT,p_warning_threshold_bytes BIGINT,
  p_warning_after_reservation BOOLEAN
) RETURNS TABLE(id UUID) LANGUAGE sql SET search_path FROM CURRENT AS $$
  WITH facts AS (
    SELECT p_approval_id approval_id,p_account_id account_id,p_connection_id connection_id,
      recording_campaign_now() observed_at,p_nas_reported_at nas_reported_at,p_nas_total_bytes nas_total_bytes,
      p_nas_free_bytes nas_free_bytes,p_measured_24h_bytes measured_24h_bytes,p_measured_streams measured_streams,
      p_active_roster_after active_roster_after,p_projected_daily_bytes projected_daily_bytes,
      p_campaign_days_with_reserve campaign_days_with_reserve,p_required_free_bytes required_free_bytes,
      p_projected_free_after_bytes projected_free_after_bytes,p_warning_threshold_bytes warning_threshold_bytes,
      p_warning_after_reservation warning_after_reservation,
      'measured-active-linear-125pct-campaign-plus-7d-v1'::text policy_version
  ), hashed AS (
    SELECT *,encode(sha256(convert_to(jsonb_build_object(
      'approval_id',approval_id,'account_id',account_id,'connection_id',connection_id,
      'observed_at_epoch',extract(epoch from observed_at),'nas_reported_at_epoch',extract(epoch from nas_reported_at),
      'nas_total_bytes',nas_total_bytes,'nas_free_bytes',nas_free_bytes,'measured_24h_bytes',measured_24h_bytes,
      'measured_streams',measured_streams,'active_roster_after',active_roster_after,
      'projected_daily_bytes',projected_daily_bytes,'campaign_days_with_reserve',campaign_days_with_reserve,
      'required_free_bytes',required_free_bytes,'projected_free_after_bytes',projected_free_after_bytes,
      'warning_threshold_bytes',warning_threshold_bytes,'warning_after_reservation',warning_after_reservation,
      'policy_version',policy_version)::text,'UTF8')),'hex') digest FROM facts
  )
  INSERT INTO recording_campaign_storage_observations(
    approval_id,account_id,connection_id,observed_at,nas_reported_at,nas_total_bytes,nas_free_bytes,
    measured_24h_bytes,measured_streams,active_roster_after,projected_daily_bytes,campaign_days_with_reserve,
    required_free_bytes,projected_free_after_bytes,warning_threshold_bytes,warning_after_reservation,
    policy_version,facts_sha256
  ) SELECT approval_id,account_id,connection_id,observed_at,nas_reported_at,nas_total_bytes,nas_free_bytes,
      measured_24h_bytes,measured_streams,active_roster_after,projected_daily_bytes,campaign_days_with_reserve,
      required_free_bytes,projected_free_after_bytes,warning_threshold_bytes,warning_after_reservation,
      policy_version,digest FROM hashed
  RETURNING recording_campaign_storage_observations.id
$$;

CREATE OR REPLACE FUNCTION recording_campaign_create_storage_reservation(
  p_approval_id UUID,p_account_id BIGINT,p_observation_id UUID,p_reserved_bytes BIGINT,
  p_reserved_until TIMESTAMPTZ
) RETURNS void LANGUAGE sql SET search_path FROM CURRENT AS $$
  INSERT INTO recording_campaign_storage_reservations(
    approval_id,account_id,observation_id,reserved_bytes,reserved_until
  ) VALUES(p_approval_id,p_account_id,p_observation_id,p_reserved_bytes,p_reserved_until)
$$;

CREATE OR REPLACE FUNCTION recording_campaign_create_admission_result(
  p_approval_id UUID,p_first_evidence_id UUID,p_second_evidence_id UUID,p_account_id BIGINT,
  p_track_id BIGINT,p_roster_entry_id BIGINT,p_stream_id BIGINT,p_recording_id BIGINT,
  p_actor_user_id BIGINT,p_action TEXT,p_schedule_sha256 TEXT,p_recording_config_sha256 TEXT
) RETURNS void LANGUAGE sql SET search_path FROM CURRENT AS $$
  INSERT INTO recording_campaign_admission_results(
    approval_id,first_probe_evidence_id,second_probe_evidence_id,account_id,track_id,roster_entry_id,
    stream_id,recording_id,actor_user_id,action,schedule_sha256,recording_config_sha256
  ) VALUES(p_approval_id,p_first_evidence_id,p_second_evidence_id,p_account_id,p_track_id,p_roster_entry_id,
    p_stream_id,p_recording_id,p_actor_user_id,p_action,p_schedule_sha256,p_recording_config_sha256)
$$;

CREATE OR REPLACE FUNCTION recording_campaign_create_admission_commit(
  p_approval_id UUID,p_account_id BIGINT,p_actor_user_id BIGINT,p_track_id BIGINT,p_response_json JSONB
) RETURNS void LANGUAGE sql SET search_path FROM CURRENT AS $$
  INSERT INTO recording_campaign_admission_commits(
    approval_id,account_id,actor_user_id,track_id,schedule_sha256,response_json,response_sha256
  ) SELECT p_approval_id,p_account_id,p_actor_user_id,p_track_id,schedule_sha256,p_response_json,
      encode(sha256(convert_to(p_response_json::text,'UTF8')),'hex')
    FROM recording_campaign_admission_approvals WHERE id=p_approval_id AND account_id=p_account_id
$$;

-- The complete admission transition is one authority-owned transaction. The
-- caller supplies only an exact next-fire plan plus fresh external observation
-- facts; every durable recording/roster/result/reservation/response value is
-- derived and revalidated against the immutable approval and live database.
CREATE OR REPLACE FUNCTION recording_campaign_admit(
  p_approval_id UUID,p_account_id BIGINT,p_actor_user_id BIGINT,p_session_id BIGINT,
  p_credential_sha256 TEXT,p_schedule_spec JSONB,p_next_fires JSONB,p_capacity JSONB,p_storage JSONB
) RETURNS JSONB LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
  approval RECORD; reservation RECORD; stream_row RECORD; existing RECORD;
  v_first_evidence UUID; v_second_evidence UUID; v_roster_id BIGINT; v_recording_id BIGINT; v_track_id BIGINT;
  v_capacity_id UUID; v_storage_id UUID; v_action TEXT; v_timezone TEXT; v_next_fire TIMESTAMPTZ;
  v_expected_next TIMESTAMPTZ; v_weekdays SMALLINT; v_response JSONB; v_created_count INTEGER:=0;
  v_updated_count INTEGER:=0; v_rank INTEGER:=0; v_active_after INTEGER; v_plan JSONB;
  v_build_sha TEXT; v_project_sha TEXT; v_firewall_sha TEXT; v_size_slug TEXT; v_pool_identity_sha TEXT; v_capacity_facts_sha TEXT;
  v_capacity_started_at TIMESTAMPTZ; v_capacity_observed_at TIMESTAMPTZ; v_provider_observation_sha TEXT;
  v_ready_workers INTEGER; v_total_slots INTEGER; v_largest_worker_slots INTEGER;
  v_usable_after_worker_loss INTEGER; v_largest_region TEXT; v_largest_region_slots INTEGER; v_forecast_peak_slots INTEGER;
  v_relay_active_demand INTEGER; v_relay_failure_domains INTEGER; v_relay_effective_capacity INTEGER;
  v_relay_usable_after_largest_loss INTEGER;
  v_connection_id BIGINT; v_nas_reported_at TIMESTAMPTZ; v_nas_total BIGINT; v_nas_free BIGINT;
  v_measured_bytes BIGINT; v_measured_streams INTEGER; v_days INTEGER; v_projected_daily BIGINT;
  v_required_free BIGINT; v_projected_free BIGINT; v_warning_threshold BIGINT;
BEGIN
  SELECT c.response_json INTO v_response
  FROM recording_campaign_admission_commits c
  WHERE c.approval_id=p_approval_id AND c.account_id=p_account_id;
  IF v_response IS NOT NULL THEN
    -- Terminal replay is independent of current session/provider/source state,
    -- but never anonymous to the executor credential: it must present the exact
    -- historical session secret that sealed this commit.
    RETURN recording_campaign_replay(p_approval_id,p_account_id,p_credential_sha256);
  END IF;

  -- This is deliberately the same global fence used by ordinary batch
  -- scheduling.  A spelling variant here would make the two writers race.
  PERFORM pg_advisory_xact_lock(hashtextextended('campaign-admission-capacity-v1',0));
  PERFORM 1 FROM accounts WHERE id=p_account_id FOR UPDATE;
  PERFORM recording_campaign_authorize_account('admit',p_approval_id,p_account_id,p_actor_user_id,p_session_id,p_credential_sha256);
  SELECT a.* INTO approval FROM recording_campaign_admission_approvals a
    JOIN recording_campaign_authority_decisions decision ON decision.code=a.authority_code
    WHERE a.id=p_approval_id AND a.account_id=p_account_id AND a.actor_user_id=p_actor_user_id
      AND a.schedule_sha256=encode(sha256(convert_to(p_schedule_spec::text,'UTF8')),'hex')
      AND a.deadline_at>recording_campaign_now() AND decision.campaign_key='delivery30-2026q3'
      AND decision.expires_at>recording_campaign_now() AND a.deadline_at<=decision.expires_at FOR UPDATE OF a;
  IF approval.id IS NULL OR jsonb_typeof(p_capacity)<>'object' OR
     NOT (p_capacity ?& ARRAY['observation_started_at','observed_at','provider_observation_sha256','build_sha',
       'ready_workers','total_slots','largest_worker_slots','usable_after_worker_loss','largest_region',
       'largest_region_slots','provider_project_sha256','provider_firewall_sha256','size_slug',
       'pool_identity_sha256','facts_sha256','forecast_peak_slots']) OR
     (SELECT count(*) FROM jsonb_object_keys(p_capacity))<>16 OR
     jsonb_typeof(p_next_fires)<>'array' OR
     jsonb_array_length(p_next_fires)<>jsonb_array_length(approval.entries) OR
     EXISTS(SELECT 1 FROM jsonb_array_elements(p_next_fires) i
       WHERE jsonb_typeof(i)<>'object' OR (SELECT count(*) FROM jsonb_object_keys(i))<>2 OR
         NOT(i ?& ARRAY['stream_id','next_fire_at']) OR jsonb_typeof(i->'stream_id')<>'number' OR
         jsonb_typeof(i->'next_fire_at')<>'string' OR NOT pg_input_is_valid(i->>'next_fire_at','timestamptz')) THEN
    RAISE EXCEPTION 'campaign admission plan is not exact';
  END IF;
  SELECT sum(1 << ((d::text)::int-1))::smallint INTO v_weekdays
    FROM jsonb_array_elements(approval.schedule_spec->'active_weekdays') d;

  INSERT INTO recording_campaign_tracks(
    account_id,campaign_key,label,deadline_at,target_count,grade_floor,
    required_consecutive_windows,created_by_user_id,reporting_grade_floor,reporting_required_consecutive_windows
  ) VALUES(p_account_id,'targeted-admission-'||p_approval_id::text,
    'Admission-only; qualification requires 14 consecutive GOOD/GREAT, reports 14 ACCEPTABLE+ separately '||left(p_approval_id::text,8),approval.deadline_at,
    jsonb_array_length(approval.entries),
    (SELECT decision.qualification_grade_floor FROM recording_campaign_authority_decisions decision WHERE decision.code=approval.authority_code),
    (SELECT decision.qualification_required_consecutive_windows FROM recording_campaign_authority_decisions decision WHERE decision.code=approval.authority_code),
    p_actor_user_id,
    (SELECT decision.reporting_grade_floor FROM recording_campaign_authority_decisions decision WHERE decision.code=approval.authority_code),
    (SELECT decision.reporting_required_consecutive_windows FROM recording_campaign_authority_decisions decision WHERE decision.code=approval.authority_code)
  ) RETURNING id INTO v_track_id;

  FOR reservation IN SELECT * FROM recording_campaign_admission_reservations
    WHERE approval_id=p_approval_id ORDER BY stream_id FOR UPDATE
  LOOP
    v_rank:=v_rank+1;
    SELECT value INTO v_plan FROM jsonb_array_elements(p_next_fires) value
      WHERE (value->>'stream_id')::bigint=reservation.stream_id;
    IF v_plan IS NULL THEN RAISE EXCEPTION 'campaign admission plan omits stream %',reservation.stream_id; END IF;
    v_next_fire:=(v_plan->>'next_fire_at')::timestamptz;
    SELECT * INTO stream_row FROM streams WHERE id=reservation.stream_id AND deleted_at IS NULL FOR UPDATE;
    v_timezone:=(SELECT value->>'timezone' FROM jsonb_array_elements(approval.schedule_spec->'stream_timezones') value
      WHERE (value->>'stream_id')::bigint=reservation.stream_id);
    SELECT min(candidate) INTO v_expected_next FROM (
	      SELECT ((schedule_day::date+(approval.schedule_spec->>'daily_window_start')::time)::timestamp AT TIME ZONE v_timezone) candidate
      FROM generate_series(
        (greatest(recording_campaign_now(),(approval.schedule_spec->>'start_at')::timestamptz) AT TIME ZONE v_timezone)::date-1,
        (greatest(recording_campaign_now(),(approval.schedule_spec->>'start_at')::timestamptz) AT TIME ZONE v_timezone)::date+372,
	        interval '1 day') AS generated_days(schedule_day)
    ) candidates WHERE candidate>recording_campaign_now()
      AND candidate>=(approval.schedule_spec->>'start_at')::timestamptz
      AND candidate<(approval.schedule_spec->>'end_at')::timestamptz
      AND (v_weekdays & (1 << (extract(isodow from candidate AT TIME ZONE v_timezone)::int-1)))<>0;
    IF stream_row.id IS NULL OR v_timezone IS NULL OR v_expected_next IS DISTINCT FROM v_next_fire OR
       reservation.source_revision_id IS DISTINCT FROM (SELECT max(id) FROM stream_source_revisions WHERE stream_id=reservation.stream_id) OR
       reservation.source_url_sha256<>encode(sha256(convert_to(stream_row.source_url,'UTF8')),'hex') OR
       reservation.source_page_url_sha256<>encode(sha256(convert_to(COALESCE(stream_row.source_page_url,''),'UTF8')),'hex') OR
       reservation.source_updated_at<>stream_row.updated_at OR
       NOT EXISTS(SELECT 1 FROM storage_destinations sd WHERE sd.id=(approval.schedule_spec->>'storage_destination_id')::bigint
         AND sd.account_id=p_account_id AND sd.status='verified' AND sd.managed) OR
       approval.schedule_spec->>'delivery'<>'nas_pull' OR
       NOT EXISTS(SELECT 1 FROM connections WHERE account_id=p_account_id AND kind='nas_pull') THEN
      RAISE EXCEPTION 'campaign admission source, schedule, or managed destination changed';
    END IF;

    IF reservation.recording_id IS NOT NULL THEN
      SELECT id,status INTO existing FROM recordings WHERE id=reservation.recording_id AND account_id=p_account_id
        AND stream_id=reservation.stream_id FOR UPDATE;
      IF existing.id IS NULL OR existing.status<>'completed' OR EXISTS(
        SELECT 1 FROM recording_jobs job WHERE job.recording_id=existing.id AND
          (job.status='pending' OR (job.status='leased' AND job.lease_expires_at>recording_campaign_now())) FOR UPDATE
      ) THEN RAISE EXCEPTION 'reserved completed recording is not idle under its exact claim fence'; END IF;
      v_recording_id:=existing.id; v_action:='reactivated'; v_updated_count:=v_updated_count+1;
      UPDATE recordings SET
        name=stream_row.name||' ['||stream_row.id::text||']',stream_url=stream_row.source_url,
        source_kind=CASE WHEN lower(stream_row.source_url) LIKE '%.m3u8%' OR lower(stream_row.source_url) LIKE '%!hls%' THEN 'hls_live' ELSE 'ffmpeg_direct' END,
        mode=approval.schedule_spec->>'mode',cron_expr=NULL,cron_timezone=v_timezone,
        clip_duration_sec=(approval.schedule_spec->>'clip_duration_sec')::int,
        daily_window_start=(approval.schedule_spec->>'daily_window_start')::time,
        daily_window_end=(approval.schedule_spec->>'daily_window_end')::time,active_weekdays=v_weekdays,
        target_fps=NULL,start_at=(approval.schedule_spec->>'start_at')::timestamptz,
        end_at=(approval.schedule_spec->>'end_at')::timestamptz,next_fire_at=v_next_fire,
        storage_destination_id=(approval.schedule_spec->>'storage_destination_id')::bigint,
        delivery_storage_destination_id=NULL,delivery=approval.schedule_spec->>'delivery',capture_via='cloud',
        naming_profile='stoarama_v1',folder_name='recordings',naming_metadata_jsonb='{}'::jsonb,
        storage_retention_tier='monthly',last_enqueued_fire_at=NULL,status='active',paused_at=NULL,
        completed_captured_clip_count=NULL,completed_expected_clip_count=NULL,consecutive_failures=0,
        last_error_text='',last_error_at=NULL,updated_at=recording_campaign_now()
      WHERE id=v_recording_id;
    ELSE
      v_action:='created'; v_created_count:=v_created_count+1;
      INSERT INTO recordings(
        account_id,storage_destination_id,delivery_storage_destination_id,name,stream_url,stream_id,
        source_kind,mode,cron_expr,cron_timezone,clip_duration_sec,daily_window_start,daily_window_end,
        target_fps,status,next_fire_at,start_at,end_at,storage_retention_tier,delivery,capture_via,
        naming_profile,folder_name,naming_metadata_jsonb,active_weekdays
      ) VALUES(p_account_id,(approval.schedule_spec->>'storage_destination_id')::bigint,NULL,
        stream_row.name||' ['||stream_row.id::text||']',stream_row.source_url,stream_row.id,
        CASE WHEN lower(stream_row.source_url) LIKE '%.m3u8%' OR lower(stream_row.source_url) LIKE '%!hls%' THEN 'hls_live' ELSE 'ffmpeg_direct' END,
        approval.schedule_spec->>'mode',NULL,v_timezone,(approval.schedule_spec->>'clip_duration_sec')::int,
        (approval.schedule_spec->>'daily_window_start')::time,(approval.schedule_spec->>'daily_window_end')::time,
        NULL,'active',v_next_fire,(approval.schedule_spec->>'start_at')::timestamptz,
        (approval.schedule_spec->>'end_at')::timestamptz,'monthly',approval.schedule_spec->>'delivery','cloud',
        'stoarama_v1','recordings','{}'::jsonb,v_weekdays) RETURNING id INTO v_recording_id;
    END IF;

    SELECT older.id,newer.id INTO v_first_evidence,v_second_evidence FROM
      LATERAL (SELECT e.id,a.attempt_no FROM recording_targeted_probe_attempts a JOIN recording_targeted_probe_evidence e ON e.attempt_id=a.id WHERE a.approval_id=p_approval_id AND a.stream_id=reservation.stream_id ORDER BY a.attempt_no DESC LIMIT 1 OFFSET 1) older,
      LATERAL (SELECT e.id,a.attempt_no FROM recording_targeted_probe_attempts a JOIN recording_targeted_probe_evidence e ON e.attempt_id=a.id WHERE a.approval_id=p_approval_id AND a.stream_id=reservation.stream_id ORDER BY a.attempt_no DESC LIMIT 1) newer;
    INSERT INTO recording_campaign_roster_entries(
      track_id,recording_id,stream_id,scene_identity_sha256,role,rank,status,reason_codes,
      effective_at,decision_at,evidence_observed_at,evidence_sha256,updated_by_user_id
    ) SELECT v_track_id,v_recording_id,reservation.stream_id,reservation.scene_identity_sha256,'primary',v_rank,
      'probation',ARRAY['deniz_approved','targeted_do_two_pass','source_fenced'],recording_campaign_now(),
      recording_campaign_now(),e.observed_at,e.media_sha256,p_actor_user_id
      FROM recording_targeted_probe_evidence e WHERE e.id=v_second_evidence RETURNING id INTO v_roster_id;
    PERFORM recording_campaign_create_admission_result(p_approval_id,v_first_evidence,v_second_evidence,
      p_account_id,v_track_id,v_roster_id,reservation.stream_id,v_recording_id,p_actor_user_id,v_action,
      approval.schedule_sha256,repeat('0',64));
  END LOOP;

  PERFORM transition_recording_campaign_track(v_track_id,'active',
    ARRAY['deniz_approved','targeted_do_two_pass','atomic_schedule_protection'],p_actor_user_id,recording_campaign_now());
  SELECT count(*) INTO v_active_after FROM recordings WHERE account_id=p_account_id AND status='active';
  IF v_active_after>60 THEN RAISE EXCEPTION 'campaign admission exceeds roster cap'; END IF;

  -- Critical fleet facts are recomputed by the authority under the shared
  -- scheduler fence. The executor's JSON is only a consistency witness; it
  -- cannot increase ready workers or slots. Build/project/firewall authority
  -- comes from the immutable provider attestations of the exact successful
  -- probes, while liveness/capacity/regions come from locked product rows.
  SELECT min(pa.build_sha),min(pa.project_id_sha256),min(pa.firewall_id_sha256),min(pa.size_slug),min(pa.pool_identity_sha256)
    INTO v_build_sha,v_project_sha,v_firewall_sha,v_size_slug,v_pool_identity_sha
  FROM recording_targeted_probe_attempts attempt
  JOIN recording_targeted_probe_evidence evidence ON evidence.attempt_id=attempt.id AND evidence.result='ok'
  JOIN recording_targeted_provider_attestations pa ON pa.id=attempt.provider_attestation_id
  WHERE attempt.approval_id=p_approval_id;
  IF v_build_sha IS NULL OR EXISTS(
    SELECT 1 FROM recording_targeted_probe_attempts attempt
    JOIN recording_targeted_probe_evidence evidence ON evidence.attempt_id=attempt.id AND evidence.result='ok'
    JOIN recording_targeted_provider_attestations pa ON pa.id=attempt.provider_attestation_id
    WHERE attempt.approval_id=p_approval_id AND
      (pa.build_sha,pa.project_id_sha256,pa.firewall_id_sha256,pa.size_slug,pa.pool_identity_sha256) IS DISTINCT FROM
      (v_build_sha,v_project_sha,v_firewall_sha,v_size_slug,v_pool_identity_sha)
  ) THEN RAISE EXCEPTION 'campaign capacity probe provider facts are not exact'; END IF;
  PERFORM 1 FROM recorder_droplets d JOIN nodes n ON n.id=d.node_id
    JOIN recording_worker_claim_heads head ON head.node_id=n.id
    JOIN node_tokens tok ON tok.id=head.claim_token_id
    WHERE d.state='active' AND n.status='active' AND n.node_type='local_recorder'
      AND d.last_seen_at BETWEEN recording_campaign_now()-interval '120 seconds' AND recording_campaign_now()+interval '30 seconds'
      AND d.build_sha=v_build_sha AND d.size=v_size_slug AND d.do_droplet_id IS NOT NULL AND d.capacity>0
      AND head.state='enabled' AND tok.revoked_at IS NULL AND tok.recording_claim_purpose='claim_current'
      AND tok.recording_claim_generation=head.generation FOR SHARE OF d,n,head,tok;
  SELECT count(*)::int,COALESCE(sum(d.capacity),0)::int,COALESCE(max(d.capacity),0)::int,
    encode(sha256(convert_to(COALESCE(string_agg(
      d.do_droplet_id::text||chr(31)||d.node_id::text||chr(31)||d.region||chr(31)||d.size||chr(31)||
      d.capacity::text||chr(31)||d.build_sha||chr(31)||head.generation::text||chr(31)||head.claim_token_id::text,
      chr(10) ORDER BY d.id)||chr(10),chr(10)),'UTF8')),'hex')
    INTO v_ready_workers,v_total_slots,v_largest_worker_slots,v_capacity_facts_sha
  FROM recorder_droplets d JOIN nodes n ON n.id=d.node_id
  JOIN recording_worker_claim_heads head ON head.node_id=n.id
  JOIN node_tokens tok ON tok.id=head.claim_token_id
  WHERE d.state='active' AND n.status='active' AND n.node_type='local_recorder'
    AND d.last_seen_at BETWEEN recording_campaign_now()-interval '120 seconds' AND recording_campaign_now()+interval '30 seconds'
    AND d.build_sha=v_build_sha AND d.size=v_size_slug AND d.do_droplet_id IS NOT NULL AND d.capacity>0
    AND head.state='enabled' AND tok.revoked_at IS NULL AND tok.recording_claim_purpose='claim_current'
    AND tok.recording_claim_generation=head.generation;
  SELECT region,sum(capacity)::int INTO v_largest_region,v_largest_region_slots
  FROM recorder_droplets d JOIN nodes n ON n.id=d.node_id
  JOIN recording_worker_claim_heads head ON head.node_id=n.id
  JOIN node_tokens tok ON tok.id=head.claim_token_id
  WHERE d.state='active' AND n.status='active' AND n.node_type='local_recorder'
    AND d.last_seen_at BETWEEN recording_campaign_now()-interval '120 seconds' AND recording_campaign_now()+interval '30 seconds'
    AND d.build_sha=v_build_sha AND d.size=v_size_slug AND d.do_droplet_id IS NOT NULL AND d.capacity>0
    AND head.state='enabled' AND tok.revoked_at IS NULL AND tok.recording_claim_purpose='claim_current'
    AND tok.recording_claim_generation=head.generation
  GROUP BY region ORDER BY sum(capacity) DESC,region LIMIT 1;
  v_usable_after_worker_loss:=v_total_slots-v_largest_worker_slots;
  v_capacity_started_at:=(p_capacity->>'observation_started_at')::timestamptz;
  v_capacity_observed_at:=(p_capacity->>'observed_at')::timestamptz;
  v_provider_observation_sha:=encode(sha256(convert_to(
    floor(extract(epoch from v_capacity_started_at)*1000000)::bigint::text||chr(10)||
    floor(extract(epoch from v_capacity_observed_at)*1000000)::bigint::text||chr(10)||v_capacity_facts_sha||chr(10)||
    v_build_sha||chr(10)||v_size_slug||chr(10)||v_pool_identity_sha||chr(10)||v_project_sha||chr(10)||v_firewall_sha,'UTF8')),'hex');
  SELECT active_demand,failure_domains,effective_capacity,usable_after_largest_loss
    INTO v_relay_active_demand,v_relay_failure_domains,v_relay_effective_capacity,v_relay_usable_after_largest_loss
  FROM recording_campaign_relay_failure_capacity(p_account_id);
  IF v_ready_workers<2 OR v_usable_after_worker_loss<=0 OR
     (v_relay_active_demand>0 AND
       (v_relay_failure_domains<2 OR v_relay_usable_after_largest_loss<v_relay_active_demand)) OR
     (p_capacity->>'build_sha', (p_capacity->>'ready_workers')::int,
       (p_capacity->>'total_slots')::int,(p_capacity->>'largest_worker_slots')::int,
       (p_capacity->>'usable_after_worker_loss')::int,p_capacity->>'largest_region',
       (p_capacity->>'largest_region_slots')::int,p_capacity->>'provider_project_sha256',
       p_capacity->>'provider_firewall_sha256',p_capacity->>'size_slug',p_capacity->>'pool_identity_sha256',
       p_capacity->>'facts_sha256',p_capacity->>'provider_observation_sha256') IS DISTINCT FROM
     (v_build_sha,v_ready_workers,v_total_slots,v_largest_worker_slots,
       v_usable_after_worker_loss,v_largest_region,v_largest_region_slots,v_project_sha,v_firewall_sha,v_size_slug,v_pool_identity_sha,
       v_capacity_facts_sha,v_provider_observation_sha) OR
     v_capacity_started_at>v_capacity_observed_at OR
     v_capacity_observed_at-v_capacity_started_at>interval '120 seconds' OR
     v_capacity_observed_at<recording_campaign_now()-interval '30 seconds' OR
     v_capacity_observed_at>recording_campaign_now()+interval '5 seconds'
  THEN RAISE EXCEPTION 'campaign capacity witness differs from authority rows'; END IF;
  SELECT id INTO v_capacity_id FROM recording_campaign_create_capacity_observation(
    p_approval_id,p_account_id,v_capacity_started_at,v_capacity_observed_at,v_build_sha,
    v_size_slug,v_pool_identity_sha,
    v_ready_workers,v_total_slots,v_largest_worker_slots,v_usable_after_worker_loss,
    v_largest_region,v_largest_region_slots,v_relay_active_demand,v_relay_failure_domains,
    v_relay_effective_capacity,v_relay_usable_after_largest_loss,
    v_project_sha,v_firewall_sha,v_capacity_facts_sha,v_provider_observation_sha);
  v_forecast_peak_slots:=recording_campaign_forecast_peak_slots(p_account_id);
  IF v_forecast_peak_slots<=0 OR v_forecast_peak_slots>v_usable_after_worker_loss OR
     (p_capacity->>'forecast_peak_slots')::int<>v_forecast_peak_slots THEN
    RAISE EXCEPTION 'campaign DB-derived forecast exceeds one-worker-loss authority';
  END IF;
  PERFORM recording_campaign_create_capacity_reservation(p_approval_id,p_account_id,v_capacity_id,
    v_forecast_peak_slots,v_active_after);
  -- NAS telemetry and the measured 24-hour rate are likewise DB-derived at
  -- commit time. Caller arithmetic must be byte-for-byte equal but is never
  -- the source of authority.
  SELECT id,nas_storage_reported_at,nas_storage_total_bytes,nas_storage_free_bytes
    INTO v_connection_id,v_nas_reported_at,v_nas_total,v_nas_free
  FROM connections WHERE id=(p_storage->>'connection_id')::bigint AND account_id=p_account_id
    AND kind='nas_pull' AND nas_capacity_blocked=false
    AND last_seen_at>=recording_campaign_now()-interval '5 minutes'
    AND nas_storage_reported_at>=recording_campaign_now()-interval '5 minutes' FOR SHARE;
  SELECT COALESCE(sum(c.size_bytes),0),count(DISTINCT c.recording_id)::int
    INTO v_measured_bytes,v_measured_streams
  FROM recording_clips c JOIN recordings r ON r.id=c.recording_id
  WHERE r.account_id=p_account_id AND c.created_at>=recording_campaign_now()-interval '24 hours' AND c.purged_at IS NULL;
  v_days:=ceil(extract(epoch FROM ((approval.schedule_spec->>'end_at')::timestamptz-recording_campaign_now()))/86400)::int+7;
  IF v_connection_id IS NULL OR v_measured_bytes<=0 OR v_measured_streams<=0 OR v_days NOT BETWEEN 8 AND 60 THEN
    RAISE EXCEPTION 'campaign admission lacks DB-derived NAS runway facts';
  END IF;
  v_projected_daily:=ceil((v_measured_bytes::numeric*v_active_after*125)/(v_measured_streams*100))::bigint;
  v_required_free:=v_projected_daily*v_days;
  v_projected_free:=v_nas_free-v_required_free;
  v_warning_threshold:=(v_nas_total+9)/10;
  IF v_projected_free<0 OR
    ((p_storage->>'nas_reported_at')::timestamptz,(p_storage->>'nas_total_bytes')::bigint,
      (p_storage->>'nas_free_bytes')::bigint,(p_storage->>'measured_24h_bytes')::bigint,
      (p_storage->>'measured_streams')::int,(p_storage->>'projected_daily_bytes')::bigint,
      (p_storage->>'campaign_days_with_reserve')::int,(p_storage->>'required_free_bytes')::bigint,
      (p_storage->>'projected_free_after_bytes')::bigint,(p_storage->>'warning_threshold_bytes')::bigint,
      (p_storage->>'warning_after_reservation')::boolean) IS DISTINCT FROM
    (v_nas_reported_at,v_nas_total,v_nas_free,v_measured_bytes,v_measured_streams,
      v_projected_daily,v_days,v_required_free,v_projected_free,v_warning_threshold,
      v_projected_free<v_warning_threshold)
  THEN RAISE EXCEPTION 'campaign NAS witness differs from authority rows'; END IF;
  SELECT id INTO v_storage_id FROM recording_campaign_create_storage_observation(
    p_approval_id,p_account_id,v_connection_id,v_nas_reported_at,v_nas_total,v_nas_free,
    v_measured_bytes,v_measured_streams,v_active_after,v_projected_daily,v_days,
    v_required_free,v_projected_free,v_warning_threshold,v_projected_free<v_warning_threshold);
  PERFORM recording_campaign_create_storage_reservation(p_approval_id,p_account_id,v_storage_id,
    v_required_free,(approval.schedule_spec->>'end_at')::timestamptz+interval '7 days');
  SELECT jsonb_build_object(
    'items',jsonb_agg(jsonb_build_object('stream_id',ar.stream_id,'recording_id',ar.recording_id,
      'action',ar.action,'timezone',r.cron_timezone) ORDER BY ar.stream_id),
    'created',v_created_count,'updated',v_updated_count,'dry_run',false,'relay_streams',0,
    'online_relay_slots',0,'required_relay_slots',0,'campaign_track_id',v_track_id,
    'campaign_admission_approval_id',p_approval_id::text,
    'campaign_capacity_observation_id',v_capacity_id::text,
    'campaign_storage_observation_id',v_storage_id::text,
    'forecast_peak_slots',v_forecast_peak_slots,
    'usable_after_worker_loss',v_usable_after_worker_loss,
    'relay_active_demand',v_relay_active_demand,
    'relay_failure_domains',v_relay_failure_domains,
    'relay_effective_capacity',v_relay_effective_capacity,
    'relay_usable_after_largest_loss',v_relay_usable_after_largest_loss,
    'required_free_bytes',v_required_free,
    'projected_free_after_bytes',v_projected_free)
    INTO v_response FROM recording_campaign_admission_results ar JOIN recordings r ON r.id=ar.recording_id
    WHERE ar.approval_id=p_approval_id AND ar.track_id=v_track_id;
  PERFORM recording_campaign_create_admission_commit(p_approval_id,p_account_id,p_actor_user_id,v_track_id,v_response);
  RETURN v_response;
END $$;

-- The runtime role never writes an authorization witness directly. These two
-- narrow SECURITY DEFINER entry points require only a server-derived credential
-- digest; raw cookies and bearer tokens never enter SQL arguments or rows. The functions
-- are transferred to the NOLOGIN authority owner and granted only to the exact
-- runtime login by the role bootstrap at the end of this migration.
CREATE OR REPLACE FUNCTION recording_campaign_authorize_account(
  requested_action TEXT, requested_approval UUID, requested_account BIGINT,
  requested_user BIGINT, requested_session BIGINT, credential_sha256 TEXT
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE valid BOOLEAN;
BEGIN
  IF requested_action NOT IN('baseline_present','baseline_attest','approve','queue','present','review','admit','expire') OR
     (requested_action IN('baseline_present','baseline_attest','approve'))<>(requested_approval IS NULL) OR
     credential_sha256!~'^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'invalid campaign account authorization request';
  END IF;
  SELECT EXISTS(
    SELECT 1 FROM account_sessions rs
    JOIN users u ON u.id=rs.user_id
    JOIN memberships m ON m.user_id=u.id AND m.org_id=rs.current_org_id
    JOIN accounts a ON a.id=rs.current_org_id
    WHERE rs.id=requested_session AND rs.session_hash=credential_sha256
      AND rs.user_id=requested_user AND rs.current_org_id=requested_account
      AND rs.revoked_at IS NULL AND rs.expires_at>recording_campaign_now()
      AND u.is_operator AND m.accepted_at IS NOT NULL AND m.role IN('owner','admin') AND a.status='active'
    FOR SHARE OF rs,u,m,a
  ) INTO valid;
  IF valid IS DISTINCT FROM true THEN RAISE EXCEPTION 'campaign account credential is not current'; END IF;
  INSERT INTO recording_campaign_admission_tx_authorizations(transaction_id,action,approval_id,account_id,actor_user_id,account_session_id)
  VALUES(txid_current(),requested_action,requested_approval,requested_account,requested_user,requested_session);
END $$;

CREATE OR REPLACE FUNCTION recording_campaign_authorize_node(
  requested_action TEXT, requested_approval UUID, requested_account BIGINT,
  requested_node BIGINT, requested_token BIGINT, requested_generation BIGINT,
  credential_sha256 TEXT
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE valid BOOLEAN;
BEGIN
  IF requested_action NOT IN('attempt','evidence') OR requested_approval IS NULL OR
     requested_generation<=0 OR credential_sha256!~'^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'invalid campaign node authorization request';
  END IF;
  SELECT EXISTS(
    SELECT 1 FROM node_tokens tok
    JOIN nodes n ON n.id=tok.node_id
    JOIN accounts a ON a.id=n.account_id
    JOIN recorder_droplets d ON d.node_id=n.id
    JOIN recording_worker_claim_heads head ON head.node_id=n.id
    WHERE tok.id=requested_token AND tok.node_id=requested_node
      AND tok.secret_hash=credential_sha256
      AND tok.revoked_at IS NULL AND tok.recording_claim_generation=requested_generation
      AND tok.recording_claim_purpose='claim_current' AND head.generation=requested_generation
      AND head.claim_token_id=tok.id AND head.state='enabled'
      -- Managed cloud nodes belong to the operator infrastructure account,
      -- not to the customer account whose approved campaign they execute.
      -- The target account is instead bound by the immutable approval below.
      AND EXISTS(SELECT 1 FROM recording_campaign_admission_approvals approval
        WHERE approval.id=requested_approval AND approval.account_id=requested_account)
      AND n.node_type='local_recorder' AND n.status='active'
      AND a.status='active' AND d.state='active' AND d.last_seen_at>=recording_campaign_now()-interval '120 seconds'
    FOR SHARE OF tok,n,a,d,head
  ) INTO valid;
  IF valid IS DISTINCT FROM true THEN RAISE EXCEPTION 'campaign node credential is not current claim authority'; END IF;
  INSERT INTO recording_campaign_admission_tx_authorizations(transaction_id,action,approval_id,account_id,node_id,node_token_id,node_claim_generation)
  VALUES(txid_current(),requested_action,requested_approval,requested_account,requested_node,requested_token,requested_generation);
END $$;

CREATE OR REPLACE FUNCTION validate_recording_campaign_tx_authorization()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; valid BOOLEAN;
BEGIN
  IF NEW.transaction_id<>txid_current() OR NEW.authorized_at IS DISTINCT FROM recording_campaign_now() THEN RAISE EXCEPTION 'campaign authorization must be bound to the current database transaction'; END IF;
  IF NEW.action IN('baseline_present','baseline_attest','approve','queue','present','review','admit','expire') THEN
    EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.account_sessions sess JOIN %I.users u ON u.id=sess.user_id JOIN %I.accounts o ON o.id=$3 JOIN %I.memberships m ON m.user_id=u.id AND m.org_id=$3 AND m.accepted_at IS NOT NULL WHERE sess.id=$1 AND sess.user_id=$2 AND sess.current_org_id=$3 AND sess.revoked_at IS NULL AND sess.expires_at>recording_campaign_now() AND o.status=''active'' AND u.is_operator AND m.role IN(''owner'',''admin'') FOR SHARE OF sess,u,o,m)',s,s,s,s)
      INTO valid USING NEW.account_session_id,NEW.actor_user_id,NEW.account_id;
    IF valid AND NEW.action='admit' THEN
      EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_approvals WHERE id=$1 AND account_id=$2 AND actor_user_id=$3 AND deadline_at>recording_campaign_now() FOR SHARE)',s)
        INTO valid USING NEW.approval_id,NEW.account_id,NEW.actor_user_id;
    END IF;
  ELSE
    EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.node_tokens tok JOIN %I.nodes n ON n.id=tok.node_id JOIN %I.accounts o ON o.id=n.account_id JOIN %I.recorder_droplets d ON d.node_id=n.id JOIN %I.recording_worker_claim_heads head ON head.node_id=n.id JOIN %I.recording_campaign_admission_approvals approval ON approval.id=$5 AND approval.account_id=$3 WHERE tok.id=$1 AND tok.node_id=$2 AND tok.revoked_at IS NULL AND tok.recording_claim_generation=$4 AND tok.recording_claim_purpose=''claim_current'' AND head.generation=$4 AND head.claim_token_id=tok.id AND head.state=''enabled'' AND o.status=''active'' AND n.node_type=''local_recorder'' AND n.status=''active'' AND d.state=''active'' AND d.last_seen_at>=recording_campaign_now()-interval ''120 seconds'' FOR SHARE OF tok,n,o,d,head,approval)',s,s,s,s,s,s)
      INTO valid USING NEW.node_token_id,NEW.node_id,NEW.account_id,NEW.node_claim_generation,NEW.approval_id;
  END IF;
  IF valid IS DISTINCT FROM true THEN RAISE EXCEPTION 'campaign authorization principal is not current and exact'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_tx_authorization_validate BEFORE INSERT ON recording_campaign_admission_tx_authorizations FOR EACH ROW EXECUTE FUNCTION validate_recording_campaign_tx_authorization();
CREATE CONSTRAINT TRIGGER recording_campaign_tx_authorization_commit_seal AFTER INSERT ON recording_campaign_admission_tx_authorizations DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION validate_recording_campaign_tx_authorization();

-- Terminalizing an expired reservation is authority, not a projection bit an
-- owner can mint directly. Validate it both before insertion and again at
-- commit so same-transaction approval/commit/attempt changes cannot release a
-- reservation without the exact typed expiration operation.
CREATE OR REPLACE FUNCTION validate_recording_campaign_reservation_terminal_event()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; authorized BOOLEAN; approval RECORD; expected TEXT; has_inflight BOOLEAN; has_commit BOOLEAN;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('campaign-admission-capacity-v1',0));
  EXECUTE format('SELECT 1 FROM %I.accounts WHERE id=$1 FOR UPDATE',s) USING NEW.account_id;
  EXECUTE format('SELECT 1 FROM %I.recording_targeted_probe_orders WHERE approval_id=$1 ORDER BY id FOR UPDATE',s)
    USING NEW.approval_id;
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_tx_authorizations WHERE transaction_id=txid_current() AND action=''expire'' AND approval_id=$1 AND account_id=$2 AND actor_user_id=$3)',s)
    INTO authorized USING NEW.approval_id,NEW.account_id,NEW.actor_user_id;
  EXECUTE format('SELECT id,account_id,deadline_at FROM %I.recording_campaign_admission_approvals WHERE id=$1 FOR SHARE',s)
    INTO approval USING NEW.approval_id;
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_commits WHERE approval_id=$1)',s)
    INTO has_commit USING NEW.approval_id;
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_targeted_probe_attempts attempt LEFT JOIN %I.recording_targeted_probe_evidence evidence ON evidence.attempt_id=attempt.id LEFT JOIN %I.recording_targeted_probe_attempt_terminal_events terminal ON terminal.attempt_id=attempt.id WHERE attempt.approval_id=$1 AND evidence.id IS NULL AND terminal.attempt_id IS NULL AND attempt.expires_at>recording_campaign_now())',s,s,s)
    INTO has_inflight USING NEW.approval_id;
  expected:=encode(sha256(convert_to(jsonb_build_object(
    'approval_id',NEW.approval_id,'account_id',NEW.account_id,'request_id',NEW.request_id,
    'result','expired_unadmitted','actor_user_id',NEW.actor_user_id,
    'observed_at',NEW.observed_at)::text,'UTF8')),'hex');
  IF authorized IS DISTINCT FROM true OR approval.id IS NULL OR approval.account_id<>NEW.account_id OR
     approval.deadline_at>recording_campaign_now() OR has_commit OR has_inflight OR
     NEW.result<>'expired_unadmitted' OR NEW.observed_at IS DISTINCT FROM recording_campaign_now() OR
     NEW.event_sha256<>expected THEN
    RAISE EXCEPTION 'campaign reservation terminal event lacks exact expired-unadmitted authority';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_reservation_terminal_validate BEFORE INSERT ON recording_campaign_admission_reservation_terminal_events FOR EACH ROW EXECUTE FUNCTION validate_recording_campaign_reservation_terminal_event();
CREATE CONSTRAINT TRIGGER recording_campaign_reservation_terminal_commit_seal AFTER INSERT ON recording_campaign_admission_reservation_terminal_events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION validate_recording_campaign_reservation_terminal_event();

CREATE OR REPLACE FUNCTION validate_recording_targeted_probe_scene_presentation()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; authorized BOOLEAN; e RECORD; expected TEXT;
BEGIN
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_tx_authorizations WHERE transaction_id=txid_current() AND action=''present'' AND approval_id=$1 AND account_id=$2 AND actor_user_id=$3)',s)
    INTO authorized USING NEW.approval_id,NEW.account_id,NEW.presented_to_user_id;
  EXECUTE format('SELECT approval_id,account_id,stream_id,result,frame_sha256,frame_archive_object_key,frame_archive_etag,frame_archive_version_id,frame_size_bytes FROM %I.recording_targeted_probe_evidence WHERE id=$1 FOR SHARE',s)
    INTO e USING NEW.probe_evidence_id;
  expected:=encode(sha256(convert_to(jsonb_build_object(
    'request_id',NEW.request_id,'approval_id',NEW.approval_id,'account_id',NEW.account_id,'stream_id',NEW.stream_id,
    'probe_evidence_id',NEW.probe_evidence_id,'probe_frame_sha256',NEW.probe_frame_sha256,
    'frame_archive_object_key',NEW.frame_archive_object_key,'frame_archive_etag',NEW.frame_archive_etag,
    'frame_archive_version_id',NEW.frame_archive_version_id,'frame_size_bytes',NEW.frame_size_bytes,
    'presented_to_user_id',NEW.presented_to_user_id,'presented_at_epoch',extract(epoch from NEW.presented_at))::text,'UTF8')),'hex');
  IF authorized IS DISTINCT FROM true OR
     (e.approval_id,e.account_id,e.stream_id,e.result,e.frame_sha256,e.frame_archive_object_key,
      e.frame_archive_etag,e.frame_archive_version_id,e.frame_size_bytes) IS DISTINCT FROM
     (NEW.approval_id,NEW.account_id,NEW.stream_id,'ok',NEW.probe_frame_sha256,NEW.frame_archive_object_key,
      NEW.frame_archive_etag,NEW.frame_archive_version_id,NEW.frame_size_bytes) OR
     NEW.presented_at IS DISTINCT FROM recording_campaign_now() OR NEW.presentation_sha256<>expected THEN
    RAISE EXCEPTION 'targeted scene presentation is not exact and authorized';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_targeted_probe_scene_presentation_validate BEFORE INSERT ON recording_targeted_probe_scene_presentations FOR EACH ROW EXECUTE FUNCTION validate_recording_targeted_probe_scene_presentation();

CREATE OR REPLACE FUNCTION validate_recording_targeted_probe_scene_review()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; authorized BOOLEAN; e RECORD; reservation RECORD; presentation RECORD; scene_fresh BOOLEAN; expected TEXT;
BEGIN
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_tx_authorizations WHERE transaction_id=txid_current() AND action=''review'' AND approval_id=$1 AND account_id=$2 AND actor_user_id=$3)',s)
    INTO authorized USING NEW.approval_id,NEW.account_id,NEW.reviewed_by_user_id;
  EXECUTE format('SELECT approval_id,account_id,stream_id,result,frame_sha256 FROM %I.recording_targeted_probe_evidence WHERE id=$1 FOR SHARE',s)
    INTO e USING NEW.probe_evidence_id;
  EXECUTE format('SELECT scene_frame_evidence_id,scene_identity_sha256 FROM %I.recording_campaign_admission_reservations WHERE approval_id=$1 AND account_id=$2 AND stream_id=$3 FOR SHARE',s)
    INTO reservation USING NEW.approval_id,NEW.account_id,NEW.stream_id;
  EXECUTE format('SELECT approval_id,account_id,stream_id,probe_evidence_id,probe_frame_sha256,presented_to_user_id,presented_at FROM %I.recording_targeted_probe_scene_presentations WHERE id=$1 FOR SHARE',s)
    INTO presentation USING NEW.presentation_id;
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_scene_frame_evidence WHERE id=$1 AND account_id=$2 AND stream_id=$3 AND scene_identity_sha256=$4 AND captured_at BETWEEN recording_campaign_now()-interval ''6 hours'' AND recording_campaign_now() AND verified_at BETWEEN recording_campaign_now()-interval ''6 hours'' AND recording_campaign_now() FOR SHARE)',s)
    INTO scene_fresh USING NEW.scene_frame_evidence_id,NEW.account_id,NEW.stream_id,NEW.scene_identity_sha256;
  expected:=encode(sha256(convert_to(jsonb_build_object('request_id',NEW.request_id,'approval_id',NEW.approval_id,'account_id',NEW.account_id,'stream_id',NEW.stream_id,'probe_evidence_id',NEW.probe_evidence_id,'presentation_id',NEW.presentation_id,'probe_frame_sha256',NEW.probe_frame_sha256,'scene_frame_evidence_id',NEW.scene_frame_evidence_id,'scene_identity_sha256',NEW.scene_identity_sha256,'reviewed_by_user_id',NEW.reviewed_by_user_id,'reviewed_at_epoch',extract(epoch from NEW.reviewed_at))::text,'UTF8')),'hex');
  IF authorized IS DISTINCT FROM true OR e.approval_id<>NEW.approval_id OR e.account_id<>NEW.account_id OR e.stream_id<>NEW.stream_id OR e.result<>'ok' OR e.frame_sha256<>NEW.probe_frame_sha256 OR
     (presentation.approval_id,presentation.account_id,presentation.stream_id,presentation.probe_evidence_id,presentation.probe_frame_sha256,presentation.presented_to_user_id) IS DISTINCT FROM
       (NEW.approval_id,NEW.account_id,NEW.stream_id,NEW.probe_evidence_id,NEW.probe_frame_sha256,NEW.reviewed_by_user_id) OR
     presentation.presented_at<recording_campaign_now()-interval '30 minutes' OR presentation.presented_at>recording_campaign_now() OR
     reservation.scene_frame_evidence_id<>NEW.scene_frame_evidence_id OR reservation.scene_identity_sha256<>NEW.scene_identity_sha256 OR scene_fresh IS DISTINCT FROM true OR
     NEW.reviewed_at IS DISTINCT FROM recording_campaign_now() OR NEW.review_sha256<>expected THEN RAISE EXCEPTION 'targeted probe scene review is not exact, fresh, and authorized'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_targeted_probe_scene_review_validate BEFORE INSERT ON recording_targeted_probe_scene_reviews FOR EACH ROW EXECUTE FUNCTION validate_recording_targeted_probe_scene_review();

CREATE OR REPLACE FUNCTION validate_recording_campaign_capacity_observation()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; authorized BOOLEAN; expected_provider_digest TEXT;
BEGIN
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_tx_authorizations WHERE transaction_id=txid_current() AND action=''admit'' AND approval_id=$1 AND account_id=$2)',s)
    INTO authorized USING NEW.approval_id,NEW.account_id;
  expected_provider_digest:=encode(sha256(convert_to(
    floor(extract(epoch from NEW.observation_started_at)*1000000)::bigint::text||chr(10)||
    floor(extract(epoch from NEW.observed_at)*1000000)::bigint::text||chr(10)||NEW.facts_sha256||chr(10)||
    NEW.build_sha||chr(10)||NEW.size_slug||chr(10)||NEW.pool_identity_sha256||chr(10)||
    NEW.provider_project_sha256||chr(10)||NEW.provider_firewall_sha256,'UTF8')),'hex');
  IF authorized IS DISTINCT FROM true OR NEW.observation_started_at>NEW.observed_at OR
     NEW.observed_at-NEW.observation_started_at>interval '120 seconds' OR
     NEW.observed_at<recording_campaign_now()-interval '30 seconds' OR NEW.observed_at>recording_campaign_now()+interval '5 seconds' OR
     NEW.provider_observation_sha256<>expected_provider_digest THEN
    RAISE EXCEPTION 'campaign capacity observation is not fresh and transaction-authorized';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_capacity_observation_validate BEFORE INSERT ON recording_campaign_capacity_observations FOR EACH ROW EXECUTE FUNCTION validate_recording_campaign_capacity_observation();

CREATE OR REPLACE FUNCTION validate_recording_campaign_capacity_reservation()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; authorized BOOLEAN; observation RECORD; live_active INTEGER;
BEGIN
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_tx_authorizations WHERE transaction_id=txid_current() AND action=''admit'' AND approval_id=$1 AND account_id=$2)',s)
    INTO authorized USING NEW.approval_id,NEW.account_id;
  EXECUTE format('SELECT * FROM %I.recording_campaign_capacity_observations WHERE id=$1 AND account_id=$2 AND approval_id=$3 FOR SHARE',s)
    INTO observation USING NEW.observation_id,NEW.account_id,NEW.approval_id;
  EXECUTE format('SELECT count(*) FROM %I.recordings WHERE account_id=$1 AND status=''active''',s)
    INTO live_active USING NEW.account_id;
  IF authorized IS DISTINCT FROM true OR observation.id IS NULL OR observation.expires_at<=recording_campaign_now() OR NEW.forecast_peak_slots>observation.usable_after_worker_loss OR
     NEW.active_roster_after<>live_active OR NEW.reserved_at IS DISTINCT FROM recording_campaign_now() THEN
    RAISE EXCEPTION 'campaign capacity reservation exceeds fresh N+1 or roster authority';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_capacity_reservation_validate BEFORE INSERT ON recording_campaign_capacity_reservations FOR EACH ROW EXECUTE FUNCTION validate_recording_campaign_capacity_reservation();

CREATE OR REPLACE FUNCTION validate_recording_campaign_storage_observation()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; authorized BOOLEAN; connection_valid BOOLEAN; expected TEXT;
BEGIN
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_tx_authorizations WHERE transaction_id=txid_current() AND action=''admit'' AND approval_id=$1 AND account_id=$2)',s)
    INTO authorized USING NEW.approval_id,NEW.account_id;
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.connections WHERE id=$1 AND account_id=$2 AND kind=''nas_pull'' AND nas_capacity_blocked=false AND nas_storage_reported_at=$3 AND nas_storage_total_bytes=$4 AND nas_storage_free_bytes=$5 AND last_seen_at>=recording_campaign_now()-interval ''5 minutes'' FOR SHARE)',s)
    INTO connection_valid USING NEW.connection_id,NEW.account_id,NEW.nas_reported_at,NEW.nas_total_bytes,NEW.nas_free_bytes;
  expected:=encode(sha256(convert_to(jsonb_build_object('approval_id',NEW.approval_id,'account_id',NEW.account_id,'connection_id',NEW.connection_id,'observed_at_epoch',extract(epoch from NEW.observed_at),'nas_reported_at_epoch',extract(epoch from NEW.nas_reported_at),'nas_total_bytes',NEW.nas_total_bytes,'nas_free_bytes',NEW.nas_free_bytes,'measured_24h_bytes',NEW.measured_24h_bytes,'measured_streams',NEW.measured_streams,'active_roster_after',NEW.active_roster_after,'projected_daily_bytes',NEW.projected_daily_bytes,'campaign_days_with_reserve',NEW.campaign_days_with_reserve,'required_free_bytes',NEW.required_free_bytes,'projected_free_after_bytes',NEW.projected_free_after_bytes,'warning_threshold_bytes',NEW.warning_threshold_bytes,'warning_after_reservation',NEW.warning_after_reservation,'policy_version',NEW.policy_version)::text,'UTF8')),'hex');
  IF authorized IS DISTINCT FROM true OR connection_valid IS DISTINCT FROM true OR NEW.observed_at IS DISTINCT FROM recording_campaign_now() OR NEW.nas_reported_at<recording_campaign_now()-interval '5 minutes' OR
     NEW.projected_daily_bytes<>ceil((NEW.measured_24h_bytes::numeric/NEW.measured_streams::numeric)*NEW.active_roster_after*1.25)::bigint OR
     NEW.required_free_bytes<>NEW.projected_daily_bytes*NEW.campaign_days_with_reserve OR NEW.required_free_bytes>NEW.nas_free_bytes OR
     NEW.projected_free_after_bytes<>NEW.nas_free_bytes-NEW.required_free_bytes OR NEW.warning_threshold_bytes<>ceil(NEW.nas_total_bytes::numeric*0.10)::bigint OR
     NEW.warning_after_reservation<>(NEW.projected_free_after_bytes<NEW.warning_threshold_bytes) OR NEW.facts_sha256<>expected THEN
    RAISE EXCEPTION 'campaign NAS observation lacks exact fresh campaign-plus-reserve runway';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_storage_observation_validate BEFORE INSERT ON recording_campaign_storage_observations FOR EACH ROW EXECUTE FUNCTION validate_recording_campaign_storage_observation();

CREATE OR REPLACE FUNCTION validate_recording_campaign_storage_reservation()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; authorized BOOLEAN; observation RECORD;
BEGIN
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_tx_authorizations WHERE transaction_id=txid_current() AND action=''admit'' AND approval_id=$1 AND account_id=$2)',s)
    INTO authorized USING NEW.approval_id,NEW.account_id;
  EXECUTE format('SELECT * FROM %I.recording_campaign_storage_observations WHERE id=$1 AND account_id=$2 AND approval_id=$3 FOR SHARE',s)
    INTO observation USING NEW.observation_id,NEW.account_id,NEW.approval_id;
  IF authorized IS DISTINCT FROM true OR observation.id IS NULL OR NEW.reserved_bytes<>observation.required_free_bytes OR NEW.reserved_bytes>observation.nas_free_bytes OR
     NEW.reserved_until<>(SELECT (schedule_spec->>'end_at')::timestamptz+interval '7 days' FROM recording_campaign_admission_approvals WHERE id=NEW.approval_id) OR NEW.reserved_at IS DISTINCT FROM recording_campaign_now() THEN
    RAISE EXCEPTION 'campaign NAS reservation differs from exact observation and campaign-plus-7d window';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_storage_reservation_validate BEFORE INSERT ON recording_campaign_storage_reservations FOR EACH ROW EXECUTE FUNCTION validate_recording_campaign_storage_reservation();

CREATE OR REPLACE FUNCTION validate_recording_campaign_admission_commit()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; valid BOOLEAN; result_count INTEGER; entry_count INTEGER; canonical_response JSONB;
BEGIN
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_approvals a JOIN %I.recording_campaign_tracks t ON t.id=$4 WHERE a.id=$1 AND a.account_id=$2 AND a.actor_user_id=$3 AND a.schedule_sha256=$5 AND t.account_id=$2 AND t.state=''active'')',s,s)
    INTO valid USING NEW.approval_id,NEW.account_id,NEW.actor_user_id,NEW.track_id,NEW.schedule_sha256;
  EXECUTE format('SELECT count(*) FROM %I.recording_campaign_admission_results WHERE approval_id=$1',s) INTO result_count USING NEW.approval_id;
  EXECUTE format('SELECT jsonb_array_length(entries) FROM %I.recording_campaign_admission_approvals WHERE id=$1',s) INTO entry_count USING NEW.approval_id;
  EXECUTE format($q$
    SELECT jsonb_build_object(
      'items',COALESCE(jsonb_agg(jsonb_build_object('stream_id',ar.stream_id,'recording_id',ar.recording_id,'action',ar.action,'timezone',r.cron_timezone) ORDER BY ar.stream_id),'[]'::jsonb),
      'created',count(*) FILTER(WHERE ar.action='created'),
      'updated',count(*) FILTER(WHERE ar.action='reactivated'),
      'dry_run',false,
      'relay_streams',0,
      'online_relay_slots',0,
      'required_relay_slots',0,
      'campaign_track_id',$2::bigint,
      'campaign_admission_approval_id',$1::text,
      'campaign_capacity_observation_id',(SELECT cap.observation_id::text FROM %I.recording_campaign_capacity_reservations cap WHERE cap.approval_id=$1),
      'campaign_storage_observation_id',(SELECT storage.observation_id::text FROM %I.recording_campaign_storage_reservations storage WHERE storage.approval_id=$1),
      'forecast_peak_slots',(SELECT cap.forecast_peak_slots FROM %I.recording_campaign_capacity_reservations cap WHERE cap.approval_id=$1),
      'usable_after_worker_loss',(SELECT observation.usable_after_worker_loss FROM %I.recording_campaign_capacity_reservations cap JOIN %I.recording_campaign_capacity_observations observation ON observation.id=cap.observation_id WHERE cap.approval_id=$1),
      'relay_active_demand',(SELECT observation.relay_active_demand FROM %I.recording_campaign_capacity_reservations cap JOIN %I.recording_campaign_capacity_observations observation ON observation.id=cap.observation_id WHERE cap.approval_id=$1),
      'relay_failure_domains',(SELECT observation.relay_failure_domains FROM %I.recording_campaign_capacity_reservations cap JOIN %I.recording_campaign_capacity_observations observation ON observation.id=cap.observation_id WHERE cap.approval_id=$1),
      'relay_effective_capacity',(SELECT observation.relay_effective_capacity FROM %I.recording_campaign_capacity_reservations cap JOIN %I.recording_campaign_capacity_observations observation ON observation.id=cap.observation_id WHERE cap.approval_id=$1),
      'relay_usable_after_largest_loss',(SELECT observation.relay_usable_after_largest_loss FROM %I.recording_campaign_capacity_reservations cap JOIN %I.recording_campaign_capacity_observations observation ON observation.id=cap.observation_id WHERE cap.approval_id=$1),
      'required_free_bytes',(SELECT storage.reserved_bytes FROM %I.recording_campaign_storage_reservations storage WHERE storage.approval_id=$1),
      'projected_free_after_bytes',(SELECT observation.projected_free_after_bytes FROM %I.recording_campaign_storage_reservations storage JOIN %I.recording_campaign_storage_observations observation ON observation.id=storage.observation_id WHERE storage.approval_id=$1))
    FROM %I.recording_campaign_admission_results ar
    JOIN %I.recordings r ON r.id=ar.recording_id
    WHERE ar.approval_id=$1 AND ar.track_id=$2$q$,s,s,s,s,s,s,s,s,s,s,s,s,s,s,s,s,s,s)
    INTO canonical_response USING NEW.approval_id,NEW.track_id;
  IF valid IS DISTINCT FROM true OR result_count<>entry_count OR NEW.committed_at IS DISTINCT FROM recording_campaign_now() OR
     NEW.response_json IS DISTINCT FROM canonical_response OR
     NEW.response_sha256<>encode(sha256(convert_to(NEW.response_json::text,'UTF8')),'hex') THEN RAISE EXCEPTION 'campaign admission commit is not the exact sealed response'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_admission_commit_validate BEFORE INSERT ON recording_campaign_admission_commits FOR EACH ROW EXECUTE FUNCTION validate_recording_campaign_admission_commit();

CREATE OR REPLACE FUNCTION enforce_recording_campaign_result_has_commit()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; sealed BOOLEAN;
BEGIN
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_commits c JOIN %I.recording_campaign_capacity_reservations cap ON cap.approval_id=c.approval_id AND cap.account_id=c.account_id JOIN %I.recording_campaign_storage_reservations storage ON storage.approval_id=c.approval_id AND storage.account_id=c.account_id WHERE c.approval_id=$1 AND c.account_id=$2 AND c.track_id=$3)',s,s,s) INTO sealed USING NEW.approval_id,NEW.account_id,NEW.track_id;
  IF sealed IS DISTINCT FROM true THEN RAISE EXCEPTION 'campaign admission results require immutable response, N+1/roster, and NAS runway reservations'; END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER recording_campaign_admission_result_commit AFTER INSERT ON recording_campaign_admission_results DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION enforce_recording_campaign_result_has_commit();

CREATE OR REPLACE FUNCTION validate_recording_campaign_admission_approval()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; authorized BOOLEAN; unidentified_active BOOLEAN; entry JSONB; source_hash TEXT; page_hash TEXT; source_updated TIMESTAMPTZ; stream_provider TEXT; stream_external TEXT; stream_label TEXT; stream_timezone TEXT; expected_timezone TEXT; tags TEXT[]; latest_revision BIGINT; scene_bound BOOLEAN; recording_account BIGINT; recording_stream BIGINT; recording_status TEXT; expected_schedule TEXT; expected_approval TEXT; prior_stream BIGINT:=0; decision RECORD; requested_ids BIGINT[];
BEGIN
  EXECUTE format('SELECT u.is_operator AND lower(u.email)=lower($2) FROM %I.users u WHERE u.id=$1',s)
    INTO authorized USING NEW.actor_user_id,NEW.actor_email_snapshot;
  IF authorized IS DISTINCT FROM true OR NEW.actor_email_snapshot<>lower(btrim(NEW.actor_email_snapshot)) THEN RAISE EXCEPTION 'campaign approval requires the exact canonical authenticated operator snapshot'; END IF;
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_tx_authorizations WHERE transaction_id=txid_current() AND action=''approve'' AND account_id=$1 AND actor_user_id=$2)',s) INTO authorized USING NEW.account_id,NEW.actor_user_id;
  IF authorized IS DISTINCT FROM true THEN RAISE EXCEPTION 'campaign approval requires typed transaction authorization'; END IF;
  EXECUTE format('SELECT 1 FROM %I.accounts WHERE id=$1 FOR UPDATE',s) USING NEW.account_id;
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recordings recording WHERE recording.account_id=$1 AND recording.status=''active'' AND recording.stream_id IS NULL)',s)
    INTO unidentified_active USING NEW.account_id;
  IF unidentified_active THEN
    RAISE EXCEPTION 'campaign approval cannot prove occupancy while an active recording lacks stream identity';
  END IF;
  IF NEW.created_at IS DISTINCT FROM recording_campaign_now() THEN RAISE EXCEPTION 'campaign approval time is database-authored'; END IF;
  EXECUTE format('SELECT code,campaign_key,expires_at,failure_domain_tag,permitted_stream_ids,subset_allowed,min_entries,max_entries FROM %I.recording_campaign_authority_decisions WHERE code=$1',s)
    INTO decision USING NEW.authority_code;
  SELECT array_agg((value->>'stream_id')::bigint ORDER BY (value->>'stream_id')::bigint)
    INTO requested_ids FROM jsonb_array_elements(NEW.entries) value;
  IF decision.code IS NULL OR decision.campaign_key<>'delivery30-2026q3' OR recording_campaign_now()>=decision.expires_at OR
     NEW.deadline_at>decision.expires_at OR NEW.failure_domain_tag IS DISTINCT FROM decision.failure_domain_tag OR
     cardinality(requested_ids) NOT BETWEEN decision.min_entries AND decision.max_entries OR
     NOT(requested_ids<@decision.permitted_stream_ids) OR
     (NOT decision.subset_allowed AND requested_ids<>decision.permitted_stream_ids) THEN
    RAISE EXCEPTION 'campaign approval does not exactly implement an immutable Deniz authority decision';
  END IF;
  IF NOT(NEW.schedule_spec ?& ARRAY['target_account_id','stream_ids','stream_timezones','naming_profile','mode','cron_expr','clip_duration_sec','daily_window_start','daily_window_end','active_weekdays','target_fps','start_at','end_at','storage_destination_id','delivery_storage_destination_id','delivery','dry_run','required_relay_slots','campaign_admission_approval_id']) OR
     (SELECT count(*) FROM jsonb_object_keys(NEW.schedule_spec))<>19 OR
     jsonb_typeof(NEW.schedule_spec->'target_account_id')<>'number' OR (NEW.schedule_spec->>'target_account_id')!~'^[1-9][0-9]*$' OR (NEW.schedule_spec->>'target_account_id')::bigint<>NEW.account_id OR
     jsonb_typeof(NEW.schedule_spec->'stream_ids')<>'array' OR jsonb_typeof(NEW.schedule_spec->'stream_timezones')<>'array' OR
     jsonb_typeof(NEW.schedule_spec->'naming_profile')<>'string' OR NEW.schedule_spec->>'naming_profile'<>'stoarama_v1' OR
     jsonb_typeof(NEW.schedule_spec->'mode')<>'string' OR NEW.schedule_spec->>'mode'<>'continuous' OR
     jsonb_typeof(NEW.schedule_spec->'cron_expr')<>'string' OR NEW.schedule_spec->>'cron_expr'<>'' OR
     jsonb_typeof(NEW.schedule_spec->'clip_duration_sec')<>'number' OR (NEW.schedule_spec->>'clip_duration_sec')!~'^[1-9][0-9]*$' OR (NEW.schedule_spec->>'clip_duration_sec')::int NOT BETWEEN 5 AND 900 OR
     jsonb_typeof(NEW.schedule_spec->'daily_window_start')<>'string' OR (NEW.schedule_spec->>'daily_window_start')!~'^(0[0-9]|1[0-9]|2[0-3]):[0-5][0-9]$' OR
     jsonb_typeof(NEW.schedule_spec->'daily_window_end')<>'string' OR (NEW.schedule_spec->>'daily_window_end')!~'^(0[0-9]|1[0-9]|2[0-3]):[0-5][0-9]$' OR
     jsonb_typeof(NEW.schedule_spec->'active_weekdays')<>'array' OR jsonb_array_length(NEW.schedule_spec->'active_weekdays')=0 OR
     EXISTS(SELECT 1 FROM jsonb_array_elements(NEW.schedule_spec->'active_weekdays') d WHERE jsonb_typeof(d)<>'number' OR d::text!~'^[1-7]$') OR
     (SELECT count(*) FROM jsonb_array_elements(NEW.schedule_spec->'active_weekdays'))<>(SELECT count(DISTINCT d::text) FROM jsonb_array_elements(NEW.schedule_spec->'active_weekdays') d) OR
     jsonb_typeof(NEW.schedule_spec->'target_fps')<>'null' OR jsonb_typeof(NEW.schedule_spec->'start_at')<>'string' OR NOT pg_input_is_valid(NEW.schedule_spec->>'start_at','timestamptz') OR
     jsonb_typeof(NEW.schedule_spec->'end_at')<>'string' OR NOT pg_input_is_valid(NEW.schedule_spec->>'end_at','timestamptz') OR (NEW.schedule_spec->>'end_at')::timestamptz<=(NEW.schedule_spec->>'start_at')::timestamptz OR
     (NEW.schedule_spec->>'end_at')::timestamptz>NEW.deadline_at OR
     jsonb_typeof(NEW.schedule_spec->'storage_destination_id')<>'number' OR (NEW.schedule_spec->>'storage_destination_id')!~'^[0-9]+$' OR
     jsonb_typeof(NEW.schedule_spec->'delivery_storage_destination_id')<>'number' OR (NEW.schedule_spec->>'delivery_storage_destination_id')!~'^[0-9]+$' OR
     (NEW.schedule_spec->>'storage_destination_id')::bigint<=0 OR (NEW.schedule_spec->>'delivery_storage_destination_id')::bigint<>0 OR
     jsonb_typeof(NEW.schedule_spec->'delivery')<>'string' OR NEW.schedule_spec->>'delivery'<>'nas_pull' OR
     jsonb_typeof(NEW.schedule_spec->'required_relay_slots')<>'number' OR NEW.schedule_spec->>'required_relay_slots'<>'0' OR
     jsonb_typeof(NEW.schedule_spec->'dry_run')<>'boolean' OR (NEW.schedule_spec->>'dry_run')::boolean OR NEW.schedule_spec->>'campaign_admission_approval_id'<>'' OR
     EXISTS(SELECT 1 FROM jsonb_array_elements(NEW.schedule_spec->'stream_timezones') z WHERE jsonb_typeof(z)<>'object' OR (SELECT count(*) FROM jsonb_object_keys(z))<>2 OR NOT(z ?& ARRAY['stream_id','timezone']) OR jsonb_typeof(z->'stream_id')<>'number' OR (z->>'stream_id')!~'^[1-9][0-9]*$' OR jsonb_typeof(z->'timezone')<>'string' OR btrim(z->>'timezone')='') THEN
    RAISE EXCEPTION 'campaign approval schedule has invalid exact canonical shape';
  END IF;
  IF (SELECT count(*) FROM jsonb_array_elements(NEW.entries))<>(SELECT count(DISTINCT value->>'stream_id') FROM jsonb_array_elements(NEW.entries)) OR
     (SELECT count(*) FROM jsonb_array_elements(NEW.entries))<>(SELECT count(DISTINCT value->>'scene_identity_sha256') FROM jsonb_array_elements(NEW.entries)) THEN
    RAISE EXCEPTION 'campaign approval streams and scenes must be unique';
  END IF;
  IF ARRAY(SELECT value::bigint FROM jsonb_array_elements_text(NEW.schedule_spec->'stream_ids') value ORDER BY value::bigint) IS DISTINCT FROM
     ARRAY(SELECT (value->>'stream_id')::bigint FROM jsonb_array_elements(NEW.entries) ORDER BY (value->>'stream_id')::bigint) OR
     ARRAY(SELECT (value->>'stream_id')::bigint FROM jsonb_array_elements(NEW.schedule_spec->'stream_timezones') value ORDER BY (value->>'stream_id')::bigint) IS DISTINCT FROM
     ARRAY(SELECT (value->>'stream_id')::bigint FROM jsonb_array_elements(NEW.entries) ORDER BY (value->>'stream_id')::bigint) THEN
    RAISE EXCEPTION 'campaign approval schedule stream/timezone set mismatch';
  END IF;
  FOR entry IN SELECT value FROM jsonb_array_elements(NEW.entries) LOOP
    IF jsonb_typeof(entry)<>'object' OR entry<>jsonb_strip_nulls(entry) OR
       NOT(entry ?& ARRAY['stream_id','recording_id','source_revision_id','source_url_sha256','source_page_url_sha256','source_updated_at_unix_micros','provider','external_id','normalized_label','scene_frame_evidence_id','scene_identity_sha256']) OR
       (SELECT count(*) FROM jsonb_object_keys(entry))<>11 OR
       jsonb_typeof(entry->'stream_id')<>'number' OR (entry->>'stream_id')!~'^[1-9][0-9]*$' OR
       jsonb_typeof(entry->'recording_id')<>'number' OR (entry->>'recording_id')!~'^[0-9]+$' OR
       jsonb_typeof(entry->'source_revision_id')<>'number' OR (entry->>'source_revision_id')!~'^[0-9]+$' OR
       jsonb_typeof(entry->'source_url_sha256')<>'string' OR (entry->>'source_url_sha256')!~'^[0-9a-f]{64}$' OR
       jsonb_typeof(entry->'source_page_url_sha256')<>'string' OR (entry->>'source_page_url_sha256')!~'^[0-9a-f]{64}$' OR
       jsonb_typeof(entry->'source_updated_at_unix_micros')<>'number' OR (entry->>'source_updated_at_unix_micros')!~'^[1-9][0-9]*$' OR
       jsonb_typeof(entry->'provider')<>'string' OR entry->>'provider'<>btrim(entry->>'provider') OR
       jsonb_typeof(entry->'external_id')<>'string' OR entry->>'external_id'<>btrim(entry->>'external_id') OR
       jsonb_typeof(entry->'normalized_label')<>'string' OR (entry->>'normalized_label')!~'^[a-z0-9]+$' OR
       jsonb_typeof(entry->'scene_frame_evidence_id')<>'number' OR (entry->>'scene_frame_evidence_id')!~'^[1-9][0-9]*$' OR
       jsonb_typeof(entry->'scene_identity_sha256')<>'string' OR (entry->>'scene_identity_sha256')!~'^[0-9a-f]{64}$' THEN
      RAISE EXCEPTION 'campaign approval entry has invalid exact shape';
    END IF;
    IF (entry->>'stream_id')::bigint<=prior_stream THEN RAISE EXCEPTION 'campaign approval entries must be sorted by stream id'; END IF;
    prior_stream:=(entry->>'stream_id')::bigint;
    EXECUTE format('SELECT encode(sha256(convert_to(source_url,''UTF8'')),''hex''),encode(sha256(convert_to(COALESCE(source_page_url,''''),''UTF8'')),''hex''),updated_at,COALESCE(provider,''''),COALESCE(external_id,''''),COALESCE(NULLIF(regexp_replace(lower(name),''[^a-z0-9]'','''',''g''),''''),''stream''||id::text),COALESCE(local_timezone,''''),tags,(SELECT max(id) FROM %I.stream_source_revisions WHERE stream_id=st.id) FROM %I.streams st WHERE id=$1 AND deleted_at IS NULL FOR UPDATE',s,s)
      INTO source_hash,page_hash,source_updated,stream_provider,stream_external,stream_label,stream_timezone,tags,latest_revision USING (entry->>'stream_id')::bigint;
    SELECT value->>'timezone' INTO expected_timezone FROM jsonb_array_elements(NEW.schedule_spec->'stream_timezones') value WHERE (value->>'stream_id')::bigint=(entry->>'stream_id')::bigint;
    IF source_hash IS NULL OR source_hash<>(entry->>'source_url_sha256') OR page_hash<>(entry->>'source_page_url_sha256') OR
       floor(extract(epoch from source_updated)*1000000)::bigint<>(entry->>'source_updated_at_unix_micros')::bigint OR
       stream_provider<>(entry->>'provider') OR stream_external<>(entry->>'external_id') OR stream_label<>(entry->>'normalized_label') OR stream_timezone<>expected_timezone OR
       COALESCE(latest_revision,0)<>(entry->>'source_revision_id')::bigint THEN RAISE EXCEPTION 'campaign approval source fence mismatch'; END IF;
    IF NEW.failure_domain_tag IS NOT NULL AND NOT(NEW.failure_domain_tag=ANY(tags)) THEN RAISE EXCEPTION 'campaign approval stream is outside required failure domain'; END IF;
    EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_scene_frame_evidence WHERE id=$1 AND account_id=$2 AND stream_id=$3 AND scene_identity_sha256=$4 AND captured_at BETWEEN recording_campaign_now()-interval ''6 hours'' AND recording_campaign_now() AND verified_at BETWEEN recording_campaign_now()-interval ''6 hours'' AND recording_campaign_now())',s)
      INTO scene_bound USING (entry->>'scene_frame_evidence_id')::bigint,NEW.account_id,(entry->>'stream_id')::bigint,entry->>'scene_identity_sha256';
    IF scene_bound IS DISTINCT FROM true THEN RAISE EXCEPTION 'campaign approval scene is not bound to fresh authoritative frame evidence'; END IF;
    IF (entry->>'recording_id')::bigint>0 THEN
      EXECUTE format('SELECT account_id,stream_id,status FROM %I.recordings WHERE id=$1 FOR UPDATE',s) INTO recording_account,recording_stream,recording_status USING (entry->>'recording_id')::bigint;
      IF recording_account<>NEW.account_id OR recording_stream<>(entry->>'stream_id')::bigint OR recording_status<>'completed' THEN RAISE EXCEPTION 'campaign approval recording is not the exact completed recording'; END IF;
    END IF;
  END LOOP;
  expected_schedule:=encode(sha256(convert_to(NEW.schedule_spec::text,'UTF8')),'hex');
  expected_approval:=encode(sha256(convert_to(jsonb_build_object('account_id',NEW.account_id,'actor_user_id',NEW.actor_user_id,'actor_email',lower(NEW.actor_email_snapshot),'authority_code',NEW.authority_code,'failure_domain_tag',NEW.failure_domain_tag,'deadline_epoch',extract(epoch from NEW.deadline_at),'entries',NEW.entries,'schedule_sha256',expected_schedule)::text,'UTF8')),'hex');
  IF NEW.schedule_sha256<>expected_schedule OR NEW.approval_sha256<>expected_approval THEN RAISE EXCEPTION 'campaign approval digest mismatch'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_admission_approval_validate BEFORE INSERT ON recording_campaign_admission_approvals FOR EACH ROW EXECUTE FUNCTION validate_recording_campaign_admission_approval();

CREATE OR REPLACE FUNCTION reserve_recording_campaign_admission_entries()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; entry JSONB; collision BOOLEAN;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('campaign-admission-capacity-v1',0));
  EXECUTE format('SELECT 1 FROM %I.accounts WHERE id=$1 FOR UPDATE',s) USING NEW.account_id;
  FOR entry IN SELECT value FROM jsonb_array_elements(NEW.entries) LOOP
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recordings r LEFT JOIN %I.streams active_stream ON active_stream.id=r.stream_id WHERE r.account_id=$1 AND r.status<>''canceled'' AND NOT(r.id=NULLIF($4,0) AND r.status=''completed'') AND (r.stream_id=$2 OR encode(sha256(convert_to(active_stream.source_url,''UTF8'')),''hex'')=$5 OR (NULLIF(active_stream.provider,'''') IS NOT NULL AND active_stream.provider=$6 AND NULLIF(active_stream.external_id,'''') IS NOT NULL AND active_stream.external_id=$7) OR COALESCE(NULLIF(regexp_replace(lower(active_stream.name),''[^a-z0-9]'','''',''g''),''''),''stream''||active_stream.id::text)=$8) UNION ALL SELECT 1 FROM %I.recording_campaign_roster_entries e JOIN %I.recording_campaign_tracks t ON t.id=e.track_id JOIN %I.streams protected_stream ON protected_stream.id=e.stream_id WHERE t.account_id=$1 AND t.state IN(''active'',''complete'') AND e.status IN(''protect'',''probation'') AND (e.stream_id=$2 OR e.scene_identity_sha256=$3 OR encode(sha256(convert_to(protected_stream.source_url,''UTF8'')),''hex'')=$5 OR (NULLIF(protected_stream.provider,'''') IS NOT NULL AND protected_stream.provider=$6 AND NULLIF(protected_stream.external_id,'''') IS NOT NULL AND protected_stream.external_id=$7) OR COALESCE(NULLIF(regexp_replace(lower(protected_stream.name),''[^a-z0-9]'','''',''g''),''''),''stream''||protected_stream.id::text)=$8) UNION ALL SELECT 1 FROM %I.recording_campaign_admission_reservations pending WHERE pending.account_id=$1 AND NOT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_reservation_terminal_events terminal WHERE terminal.approval_id=pending.approval_id) AND (pending.stream_id=$2 OR pending.scene_identity_sha256=$3 OR pending.recording_id=NULLIF($4,0) OR pending.source_url_sha256=$5 OR (NULLIF(pending.provider,'''') IS NOT NULL AND pending.provider=$6 AND NULLIF(pending.external_id,'''') IS NOT NULL AND pending.external_id=$7) OR pending.normalized_label=$8) LIMIT 1)',s,s,s,s,s,s,s)
      INTO collision USING NEW.account_id,(entry->>'stream_id')::bigint,entry->>'scene_identity_sha256',(entry->>'recording_id')::bigint,entry->>'source_url_sha256',entry->>'provider',entry->>'external_id',entry->>'normalized_label';
    IF collision THEN RAISE EXCEPTION 'campaign approval collides with active/protected occupancy'; END IF;
    EXECUTE format('INSERT INTO %I.recording_campaign_admission_reservations(approval_id,account_id,stream_id,recording_id,source_revision_id,source_url_sha256,source_page_url_sha256,source_updated_at,provider,external_id,normalized_label,scene_frame_evidence_id,scene_identity_sha256) VALUES($1,$2,$3,NULLIF($4,0),NULLIF($5,0),$6,$7,TIMESTAMPTZ ''epoch''+$8*interval ''1 microsecond'',$9,$10,$11,$12,$13)',s)
      USING NEW.id,NEW.account_id,(entry->>'stream_id')::bigint,(entry->>'recording_id')::bigint,(entry->>'source_revision_id')::bigint,entry->>'source_url_sha256',entry->>'source_page_url_sha256',(entry->>'source_updated_at_unix_micros')::bigint,entry->>'provider',entry->>'external_id',entry->>'normalized_label',(entry->>'scene_frame_evidence_id')::bigint,entry->>'scene_identity_sha256';
  END LOOP;
  RETURN NEW;
END $$;
CREATE OR REPLACE FUNCTION validate_recording_campaign_admission_reservation()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; a_account BIGINT; entry JSONB;
BEGIN
  EXECUTE format('SELECT account_id,(SELECT value FROM jsonb_array_elements(entries) value WHERE (value->>''stream_id'')::bigint=$2) FROM %I.recording_campaign_admission_approvals WHERE id=$1',s)
    INTO a_account,entry USING NEW.approval_id,NEW.stream_id;
  IF a_account IS NULL OR a_account<>NEW.account_id OR entry IS NULL OR NEW.reserved_at IS DISTINCT FROM recording_campaign_now() OR
     NEW.recording_id IS DISTINCT FROM NULLIF((entry->>'recording_id')::bigint,0) OR NEW.source_revision_id IS DISTINCT FROM NULLIF((entry->>'source_revision_id')::bigint,0) OR
     NEW.source_url_sha256<>entry->>'source_url_sha256' OR NEW.source_page_url_sha256<>entry->>'source_page_url_sha256' OR
     floor(extract(epoch from NEW.source_updated_at)*1000000)::bigint<>(entry->>'source_updated_at_unix_micros')::bigint OR
     NEW.provider<>entry->>'provider' OR NEW.external_id<>entry->>'external_id' OR NEW.normalized_label<>entry->>'normalized_label' OR
     NEW.scene_frame_evidence_id<>(entry->>'scene_frame_evidence_id')::bigint OR
     NEW.scene_identity_sha256<>entry->>'scene_identity_sha256' THEN RAISE EXCEPTION 'campaign admission reservation does not match immutable approval entry'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_admission_reservation_validate BEFORE INSERT ON recording_campaign_admission_reservations FOR EACH ROW EXECUTE FUNCTION validate_recording_campaign_admission_reservation();
CREATE TRIGGER recording_campaign_admission_reserve AFTER INSERT ON recording_campaign_admission_approvals FOR EACH ROW EXECUTE FUNCTION reserve_recording_campaign_admission_entries();

CREATE OR REPLACE FUNCTION audit_recording_campaign_reserved_source_mutation()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; reserved BOOLEAN; collides BOOLEAN; prior_hash TEXT; next_hash TEXT;
BEGIN
  IF (NEW.source_url,NEW.source_page_url,NEW.provider,NEW.external_id,NEW.name,NEW.local_timezone,NEW.tags,NEW.deleted_at,NEW.updated_at)
     IS NOT DISTINCT FROM
     (OLD.source_url,OLD.source_page_url,OLD.provider,OLD.external_id,OLD.name,OLD.local_timezone,OLD.tags,OLD.deleted_at,OLD.updated_at) THEN RETURN NEW; END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('campaign-admission-capacity-v1',0));
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_reservations r WHERE r.stream_id=$1 AND NOT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_reservation_terminal_events terminal WHERE terminal.approval_id=r.approval_id))',s,s) INTO reserved USING NEW.id;
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_reservations r WHERE r.stream_id<>$1 AND NOT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_reservation_terminal_events terminal WHERE terminal.approval_id=r.approval_id) AND (r.source_url_sha256=encode(sha256(convert_to($2,''UTF8'')),''hex'') OR (NULLIF(r.provider,'''') IS NOT NULL AND r.provider=COALESCE($3,'''') AND NULLIF(r.external_id,'''') IS NOT NULL AND r.external_id=COALESCE($4,'''')) OR r.normalized_label=COALESCE(NULLIF(regexp_replace(lower($5),''[^a-z0-9]'','''',''g''),''''),''stream''||$1::text)))',s,s)
    INTO collides USING NEW.id,NEW.source_url,NEW.provider,NEW.external_id,NEW.name;
  IF collides THEN RAISE EXCEPTION 'stream identity mutation collides with protected campaign occupancy'; END IF;
  IF reserved THEN
    prior_hash:=encode(sha256(convert_to(jsonb_build_array(OLD.source_url,OLD.source_page_url,OLD.provider,OLD.external_id,OLD.name,OLD.local_timezone,OLD.tags,OLD.deleted_at,OLD.updated_at)::text,'UTF8')),'hex');
    next_hash:=encode(sha256(convert_to(jsonb_build_array(NEW.source_url,NEW.source_page_url,NEW.provider,NEW.external_id,NEW.name,NEW.local_timezone,NEW.tags,NEW.deleted_at,NEW.updated_at)::text,'UTF8')),'hex');
    EXECUTE format('INSERT INTO %I.recording_campaign_admission_source_fence_events(stream_id,prior_fence_sha256,next_fence_sha256) VALUES($1,$2,$3)',s) USING NEW.id,prior_hash,next_hash;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_admission_source_fence_audit
AFTER UPDATE OF source_url,source_page_url,provider,external_id,name,local_timezone,tags,deleted_at,updated_at ON streams
FOR EACH ROW EXECUTE FUNCTION audit_recording_campaign_reserved_source_mutation();

CREATE OR REPLACE FUNCTION audit_recording_campaign_reserved_revision_mutation()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; sid BIGINT:=CASE WHEN TG_OP='DELETE' THEN OLD.stream_id ELSE NEW.stream_id END; reserved BOOLEAN; prior_hash TEXT; next_hash TEXT;
BEGIN
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_reservations r WHERE r.stream_id=$1 AND NOT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_reservation_terminal_events terminal WHERE terminal.approval_id=r.approval_id))',s,s) INTO reserved USING sid;
  IF reserved THEN
    prior_hash:=encode(sha256(convert_to(jsonb_build_array(TG_OP,CASE WHEN TG_OP='INSERT' THEN NULL ELSE to_jsonb(OLD) END)::text,'UTF8')),'hex');
    next_hash:=encode(sha256(convert_to(jsonb_build_array(TG_OP,CASE WHEN TG_OP='DELETE' THEN NULL ELSE to_jsonb(NEW) END)::text,'UTF8')),'hex');
    EXECUTE format('INSERT INTO %I.recording_campaign_admission_source_fence_events(stream_id,prior_fence_sha256,next_fence_sha256) VALUES($1,$2,$3)',s) USING sid,prior_hash,next_hash;
  END IF;
  RETURN CASE WHEN TG_OP='DELETE' THEN OLD ELSE NEW END;
END $$;
CREATE TRIGGER recording_campaign_admission_revision_fence_audit
AFTER INSERT OR UPDATE OR DELETE ON stream_source_revisions
FOR EACH ROW EXECUTE FUNCTION audit_recording_campaign_reserved_revision_mutation();

CREATE OR REPLACE FUNCTION validate_recording_targeted_probe_attempt()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; authorized BOOLEAN; a_account BIGINT; a_deadline TIMESTAMPTZ; r_revision BIGINT; r_source TEXT; r_page TEXT; r_updated TIMESTAMPTZ; r_reserved TIMESTAMPTZ; source_clean BOOLEAN; current_revision BIGINT; current_source TEXT; current_page TEXT; current_updated TIMESTAMPTZ; d_node BIGINT; d_do BIGINT; d_region TEXT; d_size TEXT; d_build TEXT; d_state TEXT; d_seen TIMESTAMPTZ; order_valid BOOLEAN; provider_valid BOOLEAN; expected_attempt INTEGER; prior_completed TIMESTAMPTZ;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('campaign-admission-capacity-v1',0));
  EXECUTE format('SELECT 1 FROM %I.accounts WHERE id=$1 FOR UPDATE',s) USING NEW.account_id;
  EXECUTE format('SELECT 1 FROM %I.recording_targeted_probe_orders WHERE id=$1 FOR UPDATE',s) USING NEW.order_id;
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_tx_authorizations WHERE transaction_id=txid_current() AND action=''attempt'' AND approval_id=$1 AND account_id=$2 AND node_id=$3)',s) INTO authorized USING NEW.approval_id,NEW.account_id,NEW.node_id;
  IF authorized IS DISTINCT FROM true THEN RAISE EXCEPTION 'targeted attempt requires typed node transaction authorization'; END IF;
  EXECUTE format('SELECT account_id,deadline_at FROM %I.recording_campaign_admission_approvals WHERE id=$1',s) INTO a_account,a_deadline USING NEW.approval_id;
  EXECUTE format('SELECT source_revision_id,source_url_sha256,source_page_url_sha256,source_updated_at,reserved_at FROM %I.recording_campaign_admission_reservations WHERE approval_id=$1 AND stream_id=$2 FOR UPDATE',s) INTO r_revision,r_source,r_page,r_updated,r_reserved USING NEW.approval_id,NEW.stream_id;
  EXECUTE format('SELECT NOT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_source_fence_events WHERE stream_id=$1 AND occurred_at>= $2)',s) INTO source_clean USING NEW.stream_id,r_reserved;
  EXECUTE format('SELECT (SELECT max(id) FROM %I.stream_source_revisions WHERE stream_id=st.id),encode(sha256(convert_to(st.source_url,''UTF8'')),''hex''),encode(sha256(convert_to(COALESCE(st.source_page_url,''''),''UTF8'')),''hex''),st.updated_at FROM %I.streams st WHERE id=$1 AND deleted_at IS NULL FOR SHARE',s,s)
    INTO current_revision,current_source,current_page,current_updated USING NEW.stream_id;
  EXECUTE format('SELECT node_id,do_droplet_id,region,size,build_sha,state,last_seen_at FROM %I.recorder_droplets WHERE id=$1 FOR SHARE',s) INTO d_node,d_do,d_region,d_size,d_build,d_state,d_seen USING NEW.recorder_droplet_id;
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_targeted_probe_orders o WHERE o.id=$1 AND o.approval_id=$2 AND o.account_id=$3 AND o.stream_id=$4 AND NOT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_reservation_terminal_events terminal WHERE terminal.approval_id=o.approval_id))',s,s) INTO order_valid USING NEW.order_id,NEW.approval_id,NEW.account_id,NEW.stream_id;
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_targeted_provider_attestations WHERE id=$1 AND node_id=$2 AND recorder_droplet_id=$3 AND do_droplet_id=$4 AND region=$5 AND size_slug=$6 AND build_sha=$7 AND observed_at=recording_campaign_now())',s) INTO provider_valid USING NEW.provider_attestation_id,NEW.node_id,NEW.recorder_droplet_id,NEW.do_droplet_id,NEW.region,d_size,NEW.probe_build_sha;
  EXECUTE format('SELECT COALESCE(max(attempt_no),0)+1 FROM %I.recording_targeted_probe_attempts WHERE approval_id=$1 AND stream_id=$2',s) INTO expected_attempt USING NEW.approval_id,NEW.stream_id;
  IF expected_attempt>1 THEN
    EXECUTE format('SELECT e.observed_at FROM %I.recording_targeted_probe_attempts a JOIN %I.recording_targeted_probe_evidence e ON e.attempt_id=a.id WHERE a.approval_id=$1 AND a.stream_id=$2 AND a.attempt_no=$3 - 1',s,s) INTO prior_completed USING NEW.approval_id,NEW.stream_id,expected_attempt;
  END IF;
  IF a_account IS NULL OR a_account<>NEW.account_id OR recording_campaign_now()>=a_deadline OR NEW.started_at IS DISTINCT FROM recording_campaign_now() OR NEW.expires_at IS DISTINCT FROM recording_campaign_now()+interval '15 minutes' OR
     r_source IS NULL OR r_revision IS DISTINCT FROM NEW.source_revision_id OR r_source<>NEW.source_url_sha256 OR r_page<>NEW.source_page_url_sha256 OR r_updated<>NEW.source_updated_at OR
     current_revision IS DISTINCT FROM r_revision OR current_source<>r_source OR current_page<>r_page OR current_updated<>r_updated OR
     d_node<>NEW.node_id OR d_do<>NEW.do_droplet_id OR d_region<>NEW.region OR d_build<>NEW.probe_build_sha OR d_state<>'active' OR d_seen<recording_campaign_now()-interval '120 seconds' OR
     source_clean IS DISTINCT FROM true OR order_valid IS DISTINCT FROM true OR provider_valid IS DISTINCT FROM true OR NEW.attempt_no<>expected_attempt OR
     (expected_attempt>1 AND (prior_completed IS NULL OR prior_completed>recording_campaign_now()-interval '60 seconds')) THEN RAISE EXCEPTION 'targeted attempt is not a fresh server-issued managed-recorder/source challenge'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_targeted_probe_attempt_validate BEFORE INSERT ON recording_targeted_probe_attempts FOR EACH ROW EXECUTE FUNCTION validate_recording_targeted_probe_attempt();

CREATE OR REPLACE FUNCTION validate_recording_targeted_probe_evidence()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; authorized BOOLEAN; a RECORD; source_clean BOOLEAN; current_revision BIGINT; current_source TEXT; current_page TEXT; current_updated TIMESTAMPTZ; native_text TEXT; native_hash TEXT; proof_hash TEXT; expected_evidence TEXT;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('campaign-admission-capacity-v1',0));
  EXECUTE format('SELECT * FROM %I.recording_targeted_probe_attempts WHERE id=$1 FOR UPDATE',s) INTO a USING NEW.attempt_id;
  IF EXISTS(SELECT 1 FROM recording_targeted_probe_attempt_terminal_events terminal
      WHERE terminal.attempt_id=NEW.attempt_id FOR SHARE) THEN
    RAISE EXCEPTION 'targeted evidence and terminal-without-evidence are mutually exclusive';
  END IF;
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_tx_authorizations WHERE transaction_id=txid_current() AND action=''evidence'' AND approval_id=$1 AND account_id=$2 AND node_id=$3)',s) INTO authorized USING NEW.approval_id,NEW.account_id,a.node_id;
  IF authorized IS DISTINCT FROM true THEN RAISE EXCEPTION 'targeted evidence requires typed node transaction authorization'; END IF;
  EXECUTE format('SELECT (SELECT max(id) FROM %I.stream_source_revisions WHERE stream_id=st.id),encode(sha256(convert_to(st.source_url,''UTF8'')),''hex''),encode(sha256(convert_to(COALESCE(st.source_page_url,''''),''UTF8'')),''hex''),st.updated_at FROM %I.streams st WHERE id=$1 AND deleted_at IS NULL FOR SHARE',s,s)
    INTO current_revision,current_source,current_page,current_updated USING NEW.stream_id;
  EXECUTE format('SELECT NOT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_source_fence_events f JOIN %I.recording_campaign_admission_reservations r ON r.stream_id=f.stream_id WHERE r.approval_id=$1 AND r.stream_id=$2 AND f.occurred_at>=r.reserved_at)',s,s) INTO source_clean USING NEW.approval_id,NEW.stream_id;
  native_text:=format(E'v1\nvideo=%s\naudio=%s\naudio_present=%s\nwidth=%s\nheight=%s\nfps=%s\n',lower(btrim(COALESCE(NEW.video_codec,''))),lower(btrim(COALESCE(NEW.audio_codec,''))),COALESCE(NEW.audio_present,false)::text,COALESCE(NEW.video_width,0)::text,COALESCE(NEW.video_height,0)::text,COALESCE(NEW.actual_fps::text,''));
  native_hash:=encode(sha256(convert_to(length(native_text)::text||':'||native_text,'UTF8')),'hex');
  proof_hash:=encode(sha256(convert_to(length(a.challenge)::text||':'||a.challenge||length(a.id::text)::text||':'||a.id::text||length(NEW.media_etag)::text||':'||NEW.media_etag||length(COALESCE(NEW.media_version_id,''))::text||':'||COALESCE(NEW.media_version_id,'')||length(NEW.frame_etag)::text||':'||NEW.frame_etag||length(COALESCE(NEW.frame_version_id,''))::text||':'||COALESCE(NEW.frame_version_id,'')||length(COALESCE(NEW.media_sha256,''))::text||':'||COALESCE(NEW.media_sha256,'')||length(COALESCE(NEW.frame_sha256,''))::text||':'||COALESCE(NEW.frame_sha256,'')||length(native_hash)::text||':'||native_hash,'UTF8')),'hex');
  expected_evidence:=encode(sha256(convert_to(jsonb_build_object('attempt_id',NEW.attempt_id,'approval_id',NEW.approval_id,'account_id',NEW.account_id,'stream_id',NEW.stream_id,'result',NEW.result,'valid_ratio',NEW.valid_ratio,'duration_ms',NEW.duration_ms,'segment_count',NEW.segment_count,'frame_sha256',NEW.frame_sha256,'media_sha256',NEW.media_sha256,'native_signature_sha256',native_hash,'challenge_proof_sha256',proof_hash,'video_codec',lower(btrim(COALESCE(NEW.video_codec,''))),'audio_codec',lower(btrim(COALESCE(NEW.audio_codec,''))),'audio_present',NEW.audio_present,'video_width',NEW.video_width,'video_height',NEW.video_height,'actual_fps',NEW.actual_fps,'detail',NEW.detail,'media_size_bytes',NEW.media_size_bytes,'media_etag',NEW.media_etag,'media_version_id',NEW.media_version_id,'frame_size_bytes',NEW.frame_size_bytes,'frame_etag',NEW.frame_etag,'frame_version_id',NEW.frame_version_id,'archive_bucket_sha256',NEW.archive_bucket_sha256,'media_archive_object_key',NEW.media_archive_object_key,'media_archive_sha256',NEW.media_archive_sha256,'media_archive_etag',NEW.media_archive_etag,'media_archive_version_id',NEW.media_archive_version_id,'frame_archive_object_key',NEW.frame_archive_object_key,'frame_archive_sha256',NEW.frame_archive_sha256,'frame_archive_etag',NEW.frame_archive_etag,'frame_archive_version_id',NEW.frame_archive_version_id,'submission_request_sha256',NEW.submission_request_sha256,'retain_until',NEW.retain_until,'retention_policy',NEW.retention_policy)::text,'UTF8')),'hex');
  IF a.id IS NULL OR a.approval_id<>NEW.approval_id OR a.account_id<>NEW.account_id OR a.stream_id<>NEW.stream_id OR recording_campaign_now()>a.expires_at OR NEW.observed_at IS DISTINCT FROM recording_campaign_now() OR
     source_clean IS DISTINCT FROM true OR current_revision IS DISTINCT FROM a.source_revision_id OR current_source<>a.source_url_sha256 OR current_page<>a.source_page_url_sha256 OR current_updated<>a.source_updated_at OR
     NEW.native_signature_sha256<>native_hash OR NEW.challenge_proof_sha256<>proof_hash OR
     (NEW.result='ok' AND (NEW.archive_bucket_sha256<>a.object_bucket_sha256 OR NEW.media_archive_object_key<>'protected/campaign-probe/'||a.id::text||'/media.zip' OR NEW.frame_archive_object_key<>'protected/campaign-probe/'||a.id::text||'/frame.jpg' OR NEW.frame_archive_sha256<>NEW.frame_sha256 OR NEW.retain_until<>(SELECT (schedule_spec->>'end_at')::timestamptz+interval '7 days' FROM recording_campaign_admission_approvals WHERE id=NEW.approval_id) OR NEW.retention_policy<>'qualification-evidence-campaign-plus-7d-v1')) THEN RAISE EXCEPTION 'targeted evidence is not bound to the server challenge and current source fence'; END IF;
  NEW.video_codec:=lower(btrim(COALESCE(NEW.video_codec,''))); NEW.audio_codec:=lower(btrim(COALESCE(NEW.audio_codec,''))); NEW.native_signature_sha256:=native_hash; NEW.challenge_proof_sha256:=proof_hash; NEW.evidence_sha256:=expected_evidence;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_targeted_probe_evidence_validate BEFORE INSERT ON recording_targeted_probe_evidence FOR EACH ROW EXECUTE FUNCTION validate_recording_targeted_probe_evidence();
CREATE CONSTRAINT TRIGGER recording_targeted_probe_evidence_commit_seal AFTER INSERT ON recording_targeted_probe_evidence DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION validate_recording_targeted_probe_evidence();
CREATE CONSTRAINT TRIGGER recording_targeted_probe_attempt_terminal_commit_seal AFTER INSERT ON recording_targeted_probe_attempt_terminal_events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION validate_recording_targeted_probe_attempt_terminal_event();

CREATE OR REPLACE FUNCTION validate_recording_campaign_admission_result()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; authorized BOOLEAN; a_account BIGINT; a_actor BIGINT; a_schedule TEXT; a_deadline TIMESTAMPTZ; a_tag TEXT; r_stream BIGINT; r_recording BIGINT; r_revision BIGINT; r_source TEXT; r_page TEXT; r_updated TIMESTAMPTZ; r_scene BIGINT; r_scene_hash TEXT; source_clean BOOLEAN; current_revision BIGINT; current_source TEXT; current_page TEXT; current_updated TIMESTAMPTZ; current_tags TEXT[]; t_account BIGINT; e_recording BIGINT; e_stream BIGINT; e_actor BIGINT; e_scene TEXT; config_ok BOOLEAN; scene_fresh BOOLEAN; reviews_valid BOOLEAN; current_config_sha TEXT; current_roster_sha TEXT; newer_attempt BOOLEAN; p1 RECORD; p2 RECORD;
BEGIN
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_tx_authorizations WHERE transaction_id=txid_current() AND action=''admit'' AND approval_id=$1 AND account_id=$2 AND actor_user_id=$3)',s) INTO authorized USING NEW.approval_id,NEW.account_id,NEW.actor_user_id;
  IF authorized IS DISTINCT FROM true THEN RAISE EXCEPTION 'campaign admission result requires typed transaction authorization'; END IF;
  EXECUTE format('SELECT account_id,actor_user_id,schedule_sha256,deadline_at,failure_domain_tag FROM %I.recording_campaign_admission_approvals WHERE id=$1',s) INTO a_account,a_actor,a_schedule,a_deadline,a_tag USING NEW.approval_id;
  EXECUTE format('SELECT stream_id,recording_id,source_revision_id,source_url_sha256,source_page_url_sha256,source_updated_at,scene_frame_evidence_id,scene_identity_sha256 FROM %I.recording_campaign_admission_reservations WHERE approval_id=$1 AND stream_id=$2',s) INTO r_stream,r_recording,r_revision,r_source,r_page,r_updated,r_scene,r_scene_hash USING NEW.approval_id,NEW.stream_id;
  EXECUTE format('SELECT NOT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_source_fence_events f JOIN %I.recording_campaign_admission_reservations r ON r.stream_id=f.stream_id WHERE r.approval_id=$1 AND r.stream_id=$2 AND f.occurred_at>=r.reserved_at)',s,s) INTO source_clean USING NEW.approval_id,NEW.stream_id;
  EXECUTE format('SELECT (SELECT max(id) FROM %I.stream_source_revisions WHERE stream_id=st.id),encode(sha256(convert_to(st.source_url,''UTF8'')),''hex''),encode(sha256(convert_to(COALESCE(st.source_page_url,''''),''UTF8'')),''hex''),st.updated_at,st.tags FROM %I.streams st WHERE id=$1 AND deleted_at IS NULL FOR SHARE',s,s)
    INTO current_revision,current_source,current_page,current_updated,current_tags USING NEW.stream_id;
  EXECUTE format('SELECT account_id FROM %I.recording_campaign_tracks WHERE id=$1',s) INTO t_account USING NEW.track_id;
  EXECUTE format('SELECT recording_id,stream_id,updated_by_user_id,scene_identity_sha256 FROM %I.recording_campaign_roster_entries WHERE id=$1 AND track_id=$2',s) INTO e_recording,e_stream,e_actor,e_scene USING NEW.roster_entry_id,NEW.track_id;
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_scene_frame_evidence WHERE id=$1 AND account_id=$2 AND stream_id=$3 AND scene_identity_sha256=$4 AND captured_at BETWEEN recording_campaign_now()-interval ''6 hours'' AND recording_campaign_now() AND verified_at BETWEEN recording_campaign_now()-interval ''6 hours'' AND recording_campaign_now())',s)
    INTO scene_fresh USING r_scene,NEW.account_id,NEW.stream_id,r_scene_hash;
  EXECUTE format('SELECT e.approval_id,e.account_id,e.stream_id,e.result,e.observed_at,e.frame_sha256,e.media_sha256,e.native_signature_sha256,a.source_revision_id,a.source_url_sha256,a.source_page_url_sha256,a.source_updated_at,a.challenge,a.attempt_no FROM %I.recording_targeted_probe_evidence e JOIN %I.recording_targeted_probe_attempts a ON a.id=e.attempt_id WHERE e.id=$1',s,s) INTO p1 USING NEW.first_probe_evidence_id;
  EXECUTE format('SELECT e.approval_id,e.account_id,e.stream_id,e.result,e.observed_at,e.frame_sha256,e.media_sha256,e.native_signature_sha256,a.source_revision_id,a.source_url_sha256,a.source_page_url_sha256,a.source_updated_at,a.challenge,a.attempt_no FROM %I.recording_targeted_probe_evidence e JOIN %I.recording_targeted_probe_attempts a ON a.id=e.attempt_id WHERE e.id=$1',s,s) INTO p2 USING NEW.second_probe_evidence_id;
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_targeted_probe_scene_reviews r1 JOIN %I.recording_targeted_probe_scene_reviews r2 ON r2.probe_evidence_id=$2 WHERE r1.probe_evidence_id=$1 AND r1.approval_id=$3 AND r2.approval_id=$3 AND r1.scene_frame_evidence_id=$4 AND r2.scene_frame_evidence_id=$4 AND r1.scene_identity_sha256=$5 AND r2.scene_identity_sha256=$5 AND r1.probe_frame_sha256=$6 AND r2.probe_frame_sha256=$7 AND r1.reviewed_at>=recording_campaign_now()-interval ''6 hours'' AND r2.reviewed_at>=recording_campaign_now()-interval ''6 hours'')',s,s)
    INTO reviews_valid USING NEW.first_probe_evidence_id,NEW.second_probe_evidence_id,NEW.approval_id,r_scene,r_scene_hash,p1.frame_sha256,p2.frame_sha256;
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_targeted_probe_attempts WHERE approval_id=$1 AND stream_id=$2 AND attempt_no>$3)',s) INTO newer_attempt USING NEW.approval_id,NEW.stream_id,p2.attempt_no;
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_approvals a JOIN %I.recordings r ON r.id=$2 JOIN %I.streams st ON st.id=r.stream_id WHERE a.id=$1 AND r.account_id=a.account_id AND r.stream_id=$3 AND r.name=st.name||'' [''||st.id::text||'']'' AND r.stream_url=st.source_url AND r.source_kind=CASE WHEN lower(st.source_url) LIKE ''%%.m3u8%%'' OR lower(st.source_url) LIKE ''%%!hls%%'' THEN ''hls_live'' ELSE ''ffmpeg_direct'' END AND r.status=''active'' AND r.paused_at IS NULL AND r.capture_via=''cloud'' AND r.target_fps IS NULL AND r.mode=a.schedule_spec->>''mode'' AND COALESCE(r.cron_expr,'''')=COALESCE(a.schedule_spec->>''cron_expr'','''') AND r.cron_timezone=(SELECT value->>''timezone'' FROM jsonb_array_elements(a.schedule_spec->''stream_timezones'') value WHERE (value->>''stream_id'')::bigint=$3) AND r.clip_duration_sec=(a.schedule_spec->>''clip_duration_sec'')::int AND COALESCE(to_char(r.daily_window_start,''HH24:MI''),'''')=COALESCE(a.schedule_spec->>''daily_window_start'','''') AND COALESCE(to_char(r.daily_window_end,''HH24:MI''),'''')=COALESCE(a.schedule_spec->>''daily_window_end'','''') AND r.active_weekdays=(SELECT sum(1 << ((d::text)::int-1))::smallint FROM jsonb_array_elements(a.schedule_spec->''active_weekdays'') d) AND r.next_fire_at IS NOT NULL AND r.start_at=(a.schedule_spec->>''start_at'')::timestamptz AND r.end_at=(a.schedule_spec->>''end_at'')::timestamptz AND r.storage_destination_id=(a.schedule_spec->>''storage_destination_id'')::bigint AND r.delivery_storage_destination_id IS NULL AND r.delivery=a.schedule_spec->>''delivery'' AND r.naming_profile=a.schedule_spec->>''naming_profile'' AND r.folder_name=''recordings'' AND r.naming_metadata_jsonb=''{}''::jsonb AND r.storage_retention_tier=''monthly'')',s,s,s)
    INTO config_ok USING NEW.approval_id,NEW.recording_id,NEW.stream_id;
  EXECUTE format('SELECT encode(sha256(convert_to(jsonb_build_object(''account_id'',r.account_id,''stream_id'',r.stream_id,''name'',r.name,''stream_url'',r.stream_url,''source_kind'',r.source_kind,''mode'',r.mode,''cron_expr'',r.cron_expr,''cron_timezone'',r.cron_timezone,''clip_duration_sec'',r.clip_duration_sec,''daily_window_start_us'',CASE WHEN r.daily_window_start IS NULL THEN NULL ELSE floor(extract(epoch from r.daily_window_start)*1000000)::bigint END,''daily_window_end_us'',CASE WHEN r.daily_window_end IS NULL THEN NULL ELSE floor(extract(epoch from r.daily_window_end)*1000000)::bigint END,''active_weekdays'',r.active_weekdays,''target_fps'',r.target_fps,''start_at_us'',CASE WHEN r.start_at IS NULL THEN NULL ELSE floor(extract(epoch from r.start_at)*1000000)::bigint END,''end_at_us'',CASE WHEN r.end_at IS NULL THEN NULL ELSE floor(extract(epoch from r.end_at)*1000000)::bigint END,''storage_destination_id'',r.storage_destination_id,''delivery_storage_destination_id'',r.delivery_storage_destination_id,''delivery'',r.delivery,''capture_via'',r.capture_via,''naming_profile'',r.naming_profile,''folder_name'',r.folder_name,''naming_metadata_jsonb'',r.naming_metadata_jsonb,''storage_retention_tier'',r.storage_retention_tier)::text,''UTF8'')),''hex'') FROM %I.recordings r WHERE id=$1',s)
    INTO current_config_sha USING NEW.recording_id;
  -- Seal the full 0129 roster row, including decision/evidence timestamps and
  -- every optional source-provenance field. A later typed lifecycle extension
  -- must create a new authority event; generic audited row edits cannot rewrite
  -- admitted provenance.
  EXECUTE format('SELECT encode(sha256(convert_to(jsonb_build_object(''id'',e.id,''track_id'',e.track_id,''recording_id'',e.recording_id,''stream_id'',e.stream_id,''scene_identity_sha256'',e.scene_identity_sha256,''role'',e.role,''rank'',e.rank,''status'',e.status,''reason_codes'',e.reason_codes,''effective_at_us'',floor(extract(epoch from e.effective_at)*1000000)::bigint,''decision_at_us'',floor(extract(epoch from e.decision_at)*1000000)::bigint,''evidence_observed_at_us'',floor(extract(epoch from e.evidence_observed_at)*1000000)::bigint,''evidence_sha256'',e.evidence_sha256,''source_window_end_at_us'',CASE WHEN e.source_window_end_at IS NULL THEN NULL ELSE floor(extract(epoch from e.source_window_end_at)*1000000)::bigint END,''source_job_id'',e.source_job_id,''source_health_recording_id'',e.source_health_recording_id,''source_health_job_id'',e.source_health_job_id,''updated_by_user_id'',e.updated_by_user_id,''updated_at_us'',floor(extract(epoch from e.updated_at)*1000000)::bigint)::text,''UTF8'')),''hex'') FROM %I.recording_campaign_roster_entries e WHERE id=$1',s)
    INTO current_roster_sha USING NEW.roster_entry_id;
  IF a_account<>NEW.account_id OR a_schedule<>NEW.schedule_sha256 OR recording_campaign_now()>=a_deadline OR t_account<>NEW.account_id OR
     a_actor<>NEW.actor_user_id OR config_ok IS DISTINCT FROM true OR scene_fresh IS DISTINCT FROM true OR reviews_valid IS DISTINCT FROM true OR source_clean IS DISTINCT FROM true OR
     r_stream<>NEW.stream_id OR (r_recording IS NOT NULL AND r_recording<>NEW.recording_id) OR e_recording<>NEW.recording_id OR e_stream<>NEW.stream_id OR e_actor<>NEW.actor_user_id OR e_scene<>r_scene_hash OR
     current_revision IS DISTINCT FROM r_revision OR current_source<>r_source OR current_page<>r_page OR current_updated<>r_updated OR (a_tag IS NOT NULL AND NOT(a_tag=ANY(current_tags))) OR
     p1.approval_id<>NEW.approval_id OR p2.approval_id<>NEW.approval_id OR p1.account_id<>NEW.account_id OR p2.account_id<>NEW.account_id OR
     p1.stream_id<>NEW.stream_id OR p2.stream_id<>NEW.stream_id OR p1.result<>'ok' OR p2.result<>'ok' OR
     p2.attempt_no<>p1.attempt_no+1 OR newer_attempt OR
     p2.observed_at<p1.observed_at+interval '60 seconds' OR p1.observed_at<recording_campaign_now()-interval '6 hours' OR p2.observed_at<recording_campaign_now()-interval '6 hours' OR
     p1.challenge=p2.challenge OR p1.frame_sha256=p2.frame_sha256 OR p1.media_sha256=p2.media_sha256 OR
     (p1.native_signature_sha256,p1.source_revision_id,p1.source_url_sha256,p1.source_page_url_sha256,p1.source_updated_at) IS DISTINCT FROM
     (p2.native_signature_sha256,p2.source_revision_id,p2.source_url_sha256,p2.source_page_url_sha256,p2.source_updated_at) OR
     (p2.source_revision_id,p2.source_url_sha256,p2.source_page_url_sha256,p2.source_updated_at) IS DISTINCT FROM
     (r_revision,r_source,r_page,r_updated) THEN RAISE EXCEPTION 'campaign admission result evidence/binding mismatch'; END IF;
  NEW.recording_config_sha256:=current_config_sha;
  NEW.roster_entry_sha256:=current_roster_sha;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_admission_result_validate BEFORE INSERT ON recording_campaign_admission_results FOR EACH ROW EXECUTE FUNCTION validate_recording_campaign_admission_result();

-- Direct SQL and supported writers that do not pre-lock product rows still
-- enter the global admission order before PostgreSQL selects/locks any target
-- recording row. Supported writers that pre-lock account/stream/job rows call
-- the same advisory fence explicitly before those locks.
CREATE OR REPLACE FUNCTION recording_campaign_admission_statement_fence()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('campaign-admission-capacity-v1',0));
  RETURN NULL;
END $$;
CREATE TRIGGER recording_campaign_admission_insert_fence BEFORE INSERT ON recordings FOR EACH STATEMENT EXECUTE FUNCTION recording_campaign_admission_statement_fence();
CREATE TRIGGER recording_campaign_admission_demand_update_fence BEFORE UPDATE OF
  status,stream_id,stream_url,source_kind,mode,cron_expr,cron_timezone,clip_duration_sec,
  daily_window_start,daily_window_end,active_weekdays,target_fps,start_at,end_at,
  storage_destination_id,delivery_storage_destination_id,delivery,capture_via,preferred_relay_group_id
ON recordings FOR EACH STATEMENT EXECUTE FUNCTION recording_campaign_admission_statement_fence();

CREATE OR REPLACE FUNCTION guard_reserved_completed_recording_activation()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
  s TEXT:=TG_TABLE_SCHEMA; approval UUID; authorized BOOLEAN; active_count INTEGER; active_cloud INTEGER;
  has_campaign BOOLEAN; expected_build TEXT; expected_size TEXT; expected_pool TEXT; expected_project TEXT; expected_firewall TEXT; observed_expires TIMESTAMPTZ;
  current_total INTEGER; current_largest INTEGER; usable_slots INTEGER; nas_free BIGINT; required_free BIGINT; nas_connection BIGINT;
  relay_demand INTEGER; relay_domains INTEGER; relay_after_loss INTEGER; identity_collision BOOLEAN;
  activation_change BOOLEAN:=TG_OP='INSERT'; identity_change BOOLEAN:=false; demand_change BOOLEAN:=false;
BEGIN
  IF TG_OP='UPDATE' THEN
    activation_change:=OLD.status<>'active';
    identity_change:=(NEW.stream_id,NEW.stream_url,NEW.source_kind) IS DISTINCT FROM (OLD.stream_id,OLD.stream_url,OLD.source_kind);
    demand_change:=(NEW.mode,NEW.cron_expr,NEW.cron_timezone,NEW.clip_duration_sec,NEW.daily_window_start,
      NEW.daily_window_end,NEW.active_weekdays,NEW.target_fps,NEW.start_at,NEW.end_at,
      NEW.storage_destination_id,NEW.delivery_storage_destination_id,NEW.delivery,NEW.capture_via,NEW.preferred_relay_group_id) IS DISTINCT FROM
      (OLD.mode,OLD.cron_expr,OLD.cron_timezone,OLD.clip_duration_sec,OLD.daily_window_start,
      OLD.daily_window_end,OLD.active_weekdays,OLD.target_fps,OLD.start_at,OLD.end_at,
      OLD.storage_destination_id,OLD.delivery_storage_destination_id,OLD.delivery,OLD.capture_via,OLD.preferred_relay_group_id);
  END IF;
  IF NEW.status='active' AND (activation_change OR identity_change OR demand_change) THEN
    PERFORM pg_advisory_xact_lock(hashtextextended('campaign-admission-capacity-v1',0));
    EXECUTE format('SELECT 1 FROM %I.accounts WHERE id=$1 FOR UPDATE',s) USING NEW.account_id;
    IF NEW.stream_id IS NULL THEN
      EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_reservations WHERE account_id=$1)',s) INTO authorized USING NEW.account_id;
      IF authorized THEN RAISE EXCEPTION 'active recording with NULL stream cannot bypass campaign occupancy'; END IF;
    END IF;
    EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recordings other LEFT JOIN %I.streams occupied ON occupied.id=other.stream_id LEFT JOIN %I.streams candidate ON candidate.id=$3 WHERE other.account_id=$1 AND other.status=''active'' AND other.id<>COALESCE($2,0) AND (other.stream_id=$3 OR encode(sha256(convert_to(other.stream_url,''UTF8'')),''hex'')=encode(sha256(convert_to($4,''UTF8'')),''hex'') OR (candidate.id IS NOT NULL AND NULLIF(candidate.provider,'''') IS NOT NULL AND candidate.provider=COALESCE(occupied.provider,'''') AND NULLIF(candidate.external_id,'''') IS NOT NULL AND candidate.external_id=COALESCE(occupied.external_id,'''')) OR (candidate.id IS NOT NULL AND COALESCE(NULLIF(regexp_replace(lower(candidate.name),''[^a-z0-9]'','''',''g''),''''),''stream''||candidate.id::text)=COALESCE(NULLIF(regexp_replace(lower(occupied.name),''[^a-z0-9]'','''',''g''),''''),''stream''||occupied.id::text))) UNION ALL SELECT 1 FROM %I.recording_campaign_roster_entries entry JOIN %I.recording_campaign_tracks track ON track.id=entry.track_id JOIN %I.streams occupied ON occupied.id=entry.stream_id LEFT JOIN %I.streams candidate ON candidate.id=$3 WHERE track.account_id=$1 AND track.state IN(''active'',''complete'') AND entry.status IN(''protect'',''probation'') AND entry.recording_id<>COALESCE($2,0) AND (entry.stream_id=$3 OR encode(sha256(convert_to(occupied.source_url,''UTF8'')),''hex'')=encode(sha256(convert_to($4,''UTF8'')),''hex'') OR (candidate.id IS NOT NULL AND NULLIF(candidate.provider,'''') IS NOT NULL AND candidate.provider=COALESCE(occupied.provider,'''') AND NULLIF(candidate.external_id,'''') IS NOT NULL AND candidate.external_id=COALESCE(occupied.external_id,'''')) OR (candidate.id IS NOT NULL AND COALESCE(NULLIF(regexp_replace(lower(candidate.name),''[^a-z0-9]'','''',''g''),''''),''stream''||candidate.id::text)=COALESCE(NULLIF(regexp_replace(lower(occupied.name),''[^a-z0-9]'','''',''g''),''''),''stream''||occupied.id::text))) LIMIT 1)',s,s,s,s,s,s,s)
      INTO identity_collision USING NEW.account_id,NEW.id,NEW.stream_id,NEW.stream_url;
    IF identity_collision THEN RAISE EXCEPTION 'active recording identity collides with active or protected campaign occupancy'; END IF;
    EXECUTE format('SELECT ar.approval_id FROM %I.recording_campaign_admission_reservations ar LEFT JOIN %I.streams candidate ON candidate.id=$3 WHERE ar.account_id=$1 AND NOT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_reservation_terminal_events terminal WHERE terminal.approval_id=ar.approval_id) AND (ar.recording_id=$2 OR ar.stream_id=$3 OR ar.source_url_sha256=encode(sha256(convert_to($4,''UTF8'')),''hex'') OR (candidate.id IS NOT NULL AND ((NULLIF(ar.provider,'''') IS NOT NULL AND ar.provider=COALESCE(candidate.provider,'''') AND NULLIF(ar.external_id,'''') IS NOT NULL AND ar.external_id=COALESCE(candidate.external_id,'''')) OR ar.normalized_label=COALESCE(NULLIF(regexp_replace(lower(candidate.name),''[^a-z0-9]'','''',''g''),''''),''stream''||candidate.id::text)))) ORDER BY ar.reserved_at DESC LIMIT 1',s,s,s) INTO approval USING NEW.account_id,NEW.id,NEW.stream_id,NEW.stream_url;
    IF approval IS NOT NULL THEN
      EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_tx_authorizations WHERE transaction_id=txid_current() AND action=''admit'' AND approval_id=$1 AND account_id=$2)',s) INTO authorized USING approval,NEW.account_id;
      IF authorized IS DISTINCT FROM true THEN RAISE EXCEPTION 'reserved recording/stream requires typed campaign admission'; END IF;
    END IF;
    -- Source identity repair is occupancy-neutral and remains supported after
    -- the campaign begins. Admitted identities are separately sealed; all
    -- active/protected/reserved collisions were rejected above. Only a new
    -- activation or demand-affecting edit consumes fresh N+1/NAS authority.
    IF NOT activation_change AND NOT demand_change THEN RETURN NEW; END IF;
    -- Once the account has a sealed campaign, every later activation remains
    -- under the permanent roster/N+1/NAS policy; serialization alone is not a
    -- capacity predicate.
    EXECUTE format('SELECT count(*) FROM %I.recordings WHERE account_id=$1 AND status=''active'' AND id<>COALESCE($2,0)',s)
      INTO active_count USING NEW.account_id,NEW.id;
    IF active_count>=60 THEN RAISE EXCEPTION 'campaign roster cap is permanently enforced'; END IF;
    EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_commits WHERE account_id=$1)',s)
      INTO has_campaign USING NEW.account_id;
    -- After the first sealed campaign commit, every new activation or active
    -- demand change must use the typed admission transaction. This is the
    -- permanent reciprocal capacity/NAS fence: an ordinary writer cannot add
    -- demand after the last measured runway reservation and merely compare
    -- against that stale byte threshold.
    IF has_campaign THEN
      EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_tx_authorizations WHERE transaction_id=txid_current() AND action=''admit'' AND account_id=$1 AND ($2::uuid IS NULL OR approval_id=$2))',s)
        INTO authorized USING NEW.account_id,approval;
      IF authorized IS DISTINCT FROM true THEN
        RAISE EXCEPTION 'campaign account additions require typed admission capacity and NAS recomputation';
      END IF;
      -- The typed admission function recomputes and seals a new capacity/NAS
      -- observation later in this same transaction. Do not require the prior
      -- commit's 120-second observation here: doing so would make every
      -- subsequent legitimate admission impossible after that window. The
      -- deferred admission result/commit seals roll the activation back if
      -- the new observation or reservations are absent or insufficient.
      IF approval IS NOT NULL THEN RETURN NEW; END IF;
    END IF;
    EXECUTE format('SELECT o.build_sha,o.size_slug,o.pool_identity_sha256,o.provider_project_sha256,o.provider_firewall_sha256,o.expires_at FROM %I.recording_campaign_capacity_observations o JOIN %I.recording_campaign_admission_commits c ON c.approval_id=o.approval_id WHERE c.account_id=$1 ORDER BY o.observed_at DESC,o.id DESC LIMIT 1',s,s)
      INTO expected_build,expected_size,expected_pool,expected_project,expected_firewall,observed_expires USING NEW.account_id;
    EXECUTE format('SELECT count(*) FROM %I.recordings WHERE account_id=$1 AND status=''active'' AND capture_via=''cloud'' AND id<>COALESCE($2,0)',s)
      INTO active_cloud USING NEW.account_id,NEW.id;
    IF has_campaign THEN
      IF expected_build IS NULL OR expected_size IS NULL OR expected_pool IS NULL OR observed_expires<=recording_campaign_now() THEN
        RAISE EXCEPTION 'campaign activation requires a fresh typed capacity observation';
      END IF;
      -- Freeze exactly the current enabled R10 claim heads before deriving
      -- usable slots. recovery_blocked/rotated/stale workers never contribute.
      EXECUTE format('SELECT count(*) FROM (SELECT d.id FROM %I.recorder_droplets d JOIN %I.nodes n ON n.id=d.node_id JOIN %I.recording_worker_claim_heads head ON head.node_id=n.id JOIN %I.node_tokens tok ON tok.id=head.claim_token_id WHERE d.state=''active'' AND n.status=''active'' AND n.node_type=''local_recorder'' AND d.last_seen_at BETWEEN recording_campaign_now()-interval ''120 seconds'' AND recording_campaign_now()+interval ''30 seconds'' AND d.build_sha=$1 AND d.size=$2 AND d.do_droplet_id IS NOT NULL AND d.capacity>0 AND encode(sha256(convert_to(d.size||chr(10)||d.build_sha||chr(10)||d.capacity::text||chr(10)||$3||chr(10)||$4,''UTF8'')),''hex'')=$5 AND head.state=''enabled'' AND tok.revoked_at IS NULL AND tok.recording_claim_purpose=''claim_current'' AND tok.recording_claim_generation=head.generation ORDER BY d.id FOR SHARE OF d,n,head,tok) locked',s,s,s,s)
        USING expected_build,expected_size,expected_project,expected_firewall,expected_pool;
      EXECUTE format('SELECT COALESCE(sum(d.capacity),0)::int,COALESCE(max(d.capacity),0)::int FROM %I.recorder_droplets d JOIN %I.nodes n ON n.id=d.node_id JOIN %I.recording_worker_claim_heads head ON head.node_id=n.id JOIN %I.node_tokens tok ON tok.id=head.claim_token_id WHERE d.state=''active'' AND n.status=''active'' AND n.node_type=''local_recorder'' AND d.last_seen_at BETWEEN recording_campaign_now()-interval ''120 seconds'' AND recording_campaign_now()+interval ''30 seconds'' AND d.build_sha=$1 AND d.size=$2 AND d.do_droplet_id IS NOT NULL AND d.capacity>0 AND encode(sha256(convert_to(d.size||chr(10)||d.build_sha||chr(10)||d.capacity::text||chr(10)||$3||chr(10)||$4,''UTF8'')),''hex'')=$5 AND head.state=''enabled'' AND tok.revoked_at IS NULL AND tok.recording_claim_purpose=''claim_current'' AND tok.recording_claim_generation=head.generation',s,s,s,s)
        INTO current_total,current_largest USING expected_build,expected_size,expected_project,expected_firewall,expected_pool;
      usable_slots:=current_total-current_largest;
      IF active_cloud + (CASE WHEN NEW.capture_via='cloud' THEN 1 ELSE 0 END) > usable_slots THEN
        RAISE EXCEPTION 'campaign one-worker-loss capacity head is permanently enforced';
      END IF;
      EXECUTE format('SELECT active_demand,failure_domains,usable_after_largest_loss FROM %I.recording_campaign_relay_failure_capacity($1)',s)
        INTO relay_demand,relay_domains,relay_after_loss USING NEW.account_id;
      IF relay_demand>0 AND (relay_domains<2 OR relay_after_loss<relay_demand) THEN
        RAISE EXCEPTION 'campaign largest-relay-domain-loss capacity head is permanently enforced';
      END IF;
    END IF;
    EXECUTE format('SELECT observation.connection_id,reservation.reserved_bytes FROM %I.recording_campaign_storage_reservations reservation JOIN %I.recording_campaign_storage_observations observation ON observation.id=reservation.observation_id JOIN %I.recording_campaign_admission_commits committed ON committed.approval_id=reservation.approval_id WHERE reservation.account_id=$1 AND reservation.reserved_until>recording_campaign_now() ORDER BY committed.committed_at DESC,reservation.approval_id DESC LIMIT 1',s,s,s)
      INTO nas_connection,required_free USING NEW.account_id;
    EXECUTE format('SELECT nas_storage_free_bytes FROM %I.connections WHERE id=$1 AND account_id=$2 AND kind=''nas_pull'' AND nas_capacity_blocked=false AND last_seen_at>=recording_campaign_now()-interval ''5 minutes'' AND nas_storage_reported_at>=recording_campaign_now()-interval ''5 minutes'' FOR SHARE',s)
      INTO nas_free USING nas_connection,NEW.account_id;
    IF has_campaign AND (required_free IS NULL OR nas_free IS NULL OR nas_free<required_free) THEN
      RAISE EXCEPTION 'campaign NAS runway head is permanently enforced';
    END IF;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_admission_activation_guard BEFORE INSERT OR UPDATE ON recordings FOR EACH ROW EXECUTE FUNCTION guard_reserved_completed_recording_activation();

CREATE OR REPLACE FUNCTION enforce_reserved_activation_has_result()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; approval UUID; sealed BOOLEAN;
BEGIN
  IF NEW.status='active' AND (TG_OP='INSERT' OR OLD.status<>'active') THEN
    EXECUTE format('SELECT ar.approval_id FROM %I.recording_campaign_admission_reservations ar LEFT JOIN %I.streams candidate ON candidate.id=$3 WHERE ar.account_id=$1 AND NOT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_reservation_terminal_events terminal WHERE terminal.approval_id=ar.approval_id) AND (ar.recording_id=$2 OR ar.stream_id=$3 OR (candidate.id IS NOT NULL AND (ar.source_url_sha256=encode(sha256(convert_to(candidate.source_url,''UTF8'')),''hex'') OR (NULLIF(ar.provider,'''') IS NOT NULL AND ar.provider=COALESCE(candidate.provider,'''') AND NULLIF(ar.external_id,'''') IS NOT NULL AND ar.external_id=COALESCE(candidate.external_id,'''')) OR ar.normalized_label=COALESCE(NULLIF(regexp_replace(lower(candidate.name),''[^a-z0-9]'','''',''g''),''''),''stream''||candidate.id::text)))) ORDER BY ar.reserved_at DESC LIMIT 1',s,s,s) INTO approval USING NEW.account_id,NEW.id,NEW.stream_id;
    IF approval IS NOT NULL THEN
      EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_results WHERE approval_id=$1 AND recording_id=$2)',s) INTO sealed USING approval,NEW.id;
      IF sealed IS DISTINCT FROM true THEN RAISE EXCEPTION 'reserved activation must commit its immutable admission result'; END IF;
    END IF;
  END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER recording_campaign_admission_activation_seal AFTER INSERT OR UPDATE ON recordings DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION enforce_reserved_activation_has_result();

CREATE OR REPLACE FUNCTION enforce_admitted_recording_inverse_seal()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; rid BIGINT:=CASE WHEN TG_OP='DELETE' THEN OLD.id ELSE NEW.id END; expected TEXT; actual TEXT; bound BOOLEAN; lifecycle_ok BOOLEAN; expected_next TIMESTAMPTZ;
BEGIN
  EXECUTE format('SELECT recording_config_sha256 FROM %I.recording_campaign_admission_results WHERE recording_id=$1',s) INTO expected USING rid;
  IF expected IS NULL THEN RETURN NULL; END IF;
  IF TG_OP='DELETE' THEN RAISE EXCEPTION 'admitted recording is immutable without a typed release'; END IF;
  EXECUTE format('SELECT encode(sha256(convert_to(jsonb_build_object(''account_id'',r.account_id,''stream_id'',r.stream_id,''name'',r.name,''stream_url'',r.stream_url,''source_kind'',r.source_kind,''mode'',r.mode,''cron_expr'',r.cron_expr,''cron_timezone'',r.cron_timezone,''clip_duration_sec'',r.clip_duration_sec,''daily_window_start_us'',CASE WHEN r.daily_window_start IS NULL THEN NULL ELSE floor(extract(epoch from r.daily_window_start)*1000000)::bigint END,''daily_window_end_us'',CASE WHEN r.daily_window_end IS NULL THEN NULL ELSE floor(extract(epoch from r.daily_window_end)*1000000)::bigint END,''active_weekdays'',r.active_weekdays,''target_fps'',r.target_fps,''start_at_us'',CASE WHEN r.start_at IS NULL THEN NULL ELSE floor(extract(epoch from r.start_at)*1000000)::bigint END,''end_at_us'',CASE WHEN r.end_at IS NULL THEN NULL ELSE floor(extract(epoch from r.end_at)*1000000)::bigint END,''storage_destination_id'',r.storage_destination_id,''delivery_storage_destination_id'',r.delivery_storage_destination_id,''delivery'',r.delivery,''capture_via'',r.capture_via,''naming_profile'',r.naming_profile,''folder_name'',r.folder_name,''naming_metadata_jsonb'',r.naming_metadata_jsonb,''storage_retention_tier'',r.storage_retention_tier)::text,''UTF8'')),''hex''),r.paused_at IS NULL AND ((r.status=''active'' AND r.next_fire_at IS NOT NULL) OR (r.status=''completed'' AND r.end_at<=recording_campaign_now() AND r.next_fire_at IS NULL)) FROM %I.recordings r WHERE id=$1',s)
    INTO actual,lifecycle_ok USING rid;
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_results ar JOIN %I.recording_campaign_tracks t ON t.id=ar.track_id JOIN %I.recording_campaign_roster_entries e ON e.id=ar.roster_entry_id AND e.track_id=t.id WHERE ar.recording_id=$1 AND t.state=''active'' AND e.recording_id=$1 AND e.stream_id=ar.stream_id AND e.scene_identity_sha256=(SELECT scene_identity_sha256 FROM %I.recording_campaign_admission_reservations WHERE approval_id=ar.approval_id AND stream_id=ar.stream_id) AND e.status IN(''protect'',''probation''))',s,s,s,s)
    INTO bound USING rid;
  IF TG_OP='UPDATE' AND NEW.next_fire_at IS DISTINCT FROM OLD.next_fire_at AND OLD.status='active' THEN
    SELECT min(candidate) INTO expected_next FROM (
	      SELECT ((schedule_day::date+NEW.daily_window_start)::timestamp AT TIME ZONE NEW.cron_timezone) candidate
      FROM generate_series((OLD.next_fire_at AT TIME ZONE NEW.cron_timezone)::date,
	        (OLD.next_fire_at AT TIME ZONE NEW.cron_timezone)::date+372,interval '1 day') AS generated_days(schedule_day)
    ) q WHERE candidate>OLD.next_fire_at AND candidate<NEW.end_at
      AND (NEW.active_weekdays & (1 << (extract(isodow from candidate AT TIME ZONE NEW.cron_timezone)::int-1)))<>0;
    IF NEW.next_fire_at IS DISTINCT FROM expected_next THEN
      RAISE EXCEPTION 'admitted recording next-fire must advance by one exact scheduled window';
    END IF;
  END IF;
  IF actual<>expected OR lifecycle_ok IS DISTINCT FROM true OR bound IS DISTINCT FROM true THEN RAISE EXCEPTION 'admitted recording/config/lifecycle/roster inverse seal mismatch'; END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER recording_campaign_admission_recording_inverse AFTER INSERT OR UPDATE OR DELETE ON recordings DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION enforce_admitted_recording_inverse_seal();

-- Supersede the 0129 transition with the admission lock order and a complete
-- draft->active occupancy recheck. A draft roster is not protected occupancy;
-- therefore its final activation must compete atomically with reservations and
-- already-active/protected identities.
CREATE OR REPLACE FUNCTION recording_campaign_assert_track_activation_occupancy(p_track BIGINT,p_account BIGINT)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE entry RECORD; collision BOOLEAN;
BEGIN
  FOR entry IN
    SELECT e.recording_id,e.stream_id,e.scene_identity_sha256,
      encode(sha256(convert_to(st.source_url,'UTF8')),'hex') source_hash,
      COALESCE(st.provider,'') provider,COALESCE(st.external_id,'') external_id,
      COALESCE(NULLIF(regexp_replace(lower(st.name),'[^a-z0-9]','','g'),''),'stream'||st.id::text) normalized_label
    FROM recording_campaign_roster_entries e JOIN streams st ON st.id=e.stream_id
    WHERE e.track_id=p_track AND e.status IN('protect','probation') ORDER BY e.stream_id
  LOOP
    SELECT EXISTS(
      SELECT 1 FROM recordings r LEFT JOIN streams occupied ON occupied.id=r.stream_id
      WHERE r.account_id=p_account AND r.status='active' AND r.id<>entry.recording_id AND
        (r.stream_id=entry.stream_id OR encode(sha256(convert_to(occupied.source_url,'UTF8')),'hex')=entry.source_hash OR
         (NULLIF(entry.provider,'') IS NOT NULL AND entry.provider=COALESCE(occupied.provider,'') AND NULLIF(entry.external_id,'') IS NOT NULL AND entry.external_id=COALESCE(occupied.external_id,'')) OR
         entry.normalized_label=COALESCE(NULLIF(regexp_replace(lower(occupied.name),'[^a-z0-9]','','g'),''),'stream'||occupied.id::text))
      UNION ALL
      SELECT 1 FROM recording_campaign_roster_entries other JOIN recording_campaign_tracks other_track ON other_track.id=other.track_id JOIN streams occupied ON occupied.id=other.stream_id
      WHERE other.track_id<>p_track AND other_track.account_id=p_account AND other_track.state IN('active','complete') AND other.status IN('protect','probation') AND
        (other.stream_id=entry.stream_id OR other.scene_identity_sha256=entry.scene_identity_sha256 OR encode(sha256(convert_to(occupied.source_url,'UTF8')),'hex')=entry.source_hash OR
         (NULLIF(entry.provider,'') IS NOT NULL AND entry.provider=COALESCE(occupied.provider,'') AND NULLIF(entry.external_id,'') IS NOT NULL AND entry.external_id=COALESCE(occupied.external_id,'')) OR
         entry.normalized_label=COALESCE(NULLIF(regexp_replace(lower(occupied.name),'[^a-z0-9]','','g'),''),'stream'||occupied.id::text))
      UNION ALL
      SELECT 1 FROM recording_campaign_admission_reservations pending
      WHERE pending.account_id=p_account AND NOT EXISTS(SELECT 1 FROM recording_campaign_admission_reservation_terminal_events terminal WHERE terminal.approval_id=pending.approval_id) AND
        NOT EXISTS(SELECT 1 FROM recording_campaign_admission_tx_authorizations witness WHERE witness.transaction_id=txid_current() AND witness.action='admit' AND witness.approval_id=pending.approval_id AND witness.account_id=p_account) AND
        (pending.stream_id=entry.stream_id OR pending.scene_identity_sha256=entry.scene_identity_sha256 OR pending.recording_id=entry.recording_id OR pending.source_url_sha256=entry.source_hash OR
         (NULLIF(entry.provider,'') IS NOT NULL AND pending.provider=entry.provider AND NULLIF(entry.external_id,'') IS NOT NULL AND pending.external_id=entry.external_id) OR pending.normalized_label=entry.normalized_label)
      LIMIT 1
    ) INTO collision;
    IF collision THEN RAISE EXCEPTION 'campaign track activation collides with active/protected/reserved occupancy'; END IF;
  END LOOP;
END $$;

CREATE OR REPLACE FUNCTION guard_recording_campaign_track_activation_occupancy()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
BEGIN
  IF OLD.state='draft' AND NEW.state='active' THEN
    PERFORM recording_campaign_assert_track_activation_occupancy(NEW.id,NEW.account_id);
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_track_state_fence BEFORE UPDATE OF state ON recording_campaign_tracks FOR EACH STATEMENT EXECUTE FUNCTION recording_campaign_admission_statement_fence();
CREATE TRIGGER recording_campaign_track_activation_occupancy BEFORE UPDATE OF state ON recording_campaign_tracks FOR EACH ROW EXECUTE FUNCTION guard_recording_campaign_track_activation_occupancy();

CREATE OR REPLACE FUNCTION guard_recording_campaign_track_state()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; witnessed BOOLEAN;
BEGIN
  IF OLD.state<>'draft' AND (NEW.account_id,NEW.campaign_key,NEW.label,NEW.deadline_at,NEW.target_count,
      NEW.grade_floor,NEW.required_consecutive_windows,NEW.created_by_user_id)
    IS DISTINCT FROM (OLD.account_id,OLD.campaign_key,OLD.label,OLD.deadline_at,OLD.target_count,
      OLD.grade_floor,OLD.required_consecutive_windows,OLD.created_by_user_id) THEN
    RAISE EXCEPTION 'active campaign definition is immutable';
  END IF;
  IF NEW.state IS DISTINCT FROM OLD.state THEN
    EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_track_events event WHERE event.track_id=$1 AND event.from_state=$2 AND event.to_state=$3 AND (event.xmin::text)::bigint=txid_current())',s)
      INTO witnessed USING NEW.id,OLD.state,NEW.state;
    IF current_setting('stoarama.campaign_transition',true) IS DISTINCT FROM '1' OR witnessed IS DISTINCT FROM true THEN
      RAISE EXCEPTION 'use typed transition_recording_campaign_track authority';
    END IF;
  END IF;
  RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION transition_recording_campaign_track(p_track BIGINT,p_to TEXT,p_reasons TEXT[],p_actor BIGINT,p_decided TIMESTAMPTZ)
RETURNS VOID LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE old_state TEXT; expected_count INTEGER; actual_count INTEGER; track_account BIGINT; first_account BIGINT; authorized BOOLEAN;
BEGIN
  SELECT account_id INTO first_account FROM recording_campaign_tracks WHERE id=p_track;
  IF first_account IS NULL THEN RAISE EXCEPTION 'track transition requires track, reasons, and decision time'; END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('campaign-admission-capacity-v1',0));
  PERFORM 1 FROM accounts WHERE id=first_account FOR UPDATE;
  SELECT state,target_count,account_id INTO old_state,expected_count,track_account FROM recording_campaign_tracks WHERE id=p_track FOR UPDATE;
  IF track_account IS DISTINCT FROM first_account OR p_decided IS NULL OR cardinality(p_reasons)=0 THEN RAISE EXCEPTION 'track transition requires stable track, reasons, and decision time'; END IF;
  IF NOT ((old_state='draft' AND p_to='active') OR (old_state='active' AND p_to='complete') OR (old_state IN ('draft','active','complete') AND p_to='retired')) THEN RAISE EXCEPTION 'invalid campaign track transition'; END IF;
  SELECT (u.is_operator OR EXISTS(SELECT 1 FROM memberships m WHERE m.user_id=u.id AND m.org_id=track_account AND m.accepted_at IS NOT NULL AND m.role IN ('owner','admin'))) INTO authorized FROM users u WHERE u.id=p_actor;
  IF authorized IS DISTINCT FROM true THEN RAISE EXCEPTION 'campaign transition actor is not an account owner/operator'; END IF;
  IF p_to IN ('active','complete') THEN
    SELECT count(*) INTO actual_count FROM recording_campaign_roster_entries WHERE track_id=p_track AND role='primary' AND status IN ('protect','probation');
    IF actual_count<>expected_count THEN RAISE EXCEPTION 'active track requires exact target primary roster'; END IF;
  END IF;
  IF old_state='draft' AND p_to='active' THEN
    PERFORM recording_campaign_assert_track_activation_occupancy(p_track,track_account);
  END IF;
  INSERT INTO recording_campaign_track_events(track_id,from_state,to_state,reason_codes,actor_user_id,decided_at) VALUES(p_track,old_state,p_to,p_reasons,p_actor,p_decided);
  PERFORM set_config('stoarama.campaign_transition','1',true);
  UPDATE recording_campaign_tracks SET state=p_to,updated_at=recording_campaign_now() WHERE id=p_track;
  PERFORM set_config('stoarama.campaign_transition','0',true);
END $$;

CREATE OR REPLACE FUNCTION guard_reserved_campaign_roster_occupancy()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; account BIGINT; approval UUID; authorized BOOLEAN;
BEGIN
  IF NEW.status NOT IN('protect','probation') THEN RETURN NEW; END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('campaign-admission-capacity-v1',0));
  EXECUTE format('SELECT account_id FROM %I.recording_campaign_tracks WHERE id=$1',s) INTO account USING NEW.track_id;
  EXECUTE format('SELECT 1 FROM %I.accounts WHERE id=$1 FOR UPDATE',s) USING account;
  EXECUTE format('SELECT ar.approval_id FROM %I.recording_campaign_admission_reservations ar LEFT JOIN %I.streams candidate ON candidate.id=$2 WHERE ar.account_id=$1 AND NOT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_reservation_terminal_events terminal WHERE terminal.approval_id=ar.approval_id) AND (ar.stream_id=$2 OR ar.scene_identity_sha256=$3 OR (candidate.id IS NOT NULL AND (ar.source_url_sha256=encode(sha256(convert_to(candidate.source_url,''UTF8'')),''hex'') OR (NULLIF(ar.provider,'''') IS NOT NULL AND ar.provider=COALESCE(candidate.provider,'''') AND NULLIF(ar.external_id,'''') IS NOT NULL AND ar.external_id=COALESCE(candidate.external_id,'''')) OR ar.normalized_label=COALESCE(NULLIF(regexp_replace(lower(candidate.name),''[^a-z0-9]'','''',''g''),''''),''stream''||candidate.id::text)))) ORDER BY ar.reserved_at DESC LIMIT 1',s,s,s) INTO approval USING account,NEW.stream_id,NEW.scene_identity_sha256;
  IF approval IS NOT NULL THEN
    EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_tx_authorizations WHERE transaction_id=txid_current() AND action=''admit'' AND approval_id=$1 AND account_id=$2)',s) INTO authorized USING approval,account;
    IF authorized IS DISTINCT FROM true THEN RAISE EXCEPTION 'reserved stream/scene requires typed campaign admission roster'; END IF;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_admission_roster_guard BEFORE INSERT OR UPDATE ON recording_campaign_roster_entries FOR EACH ROW EXECUTE FUNCTION guard_reserved_campaign_roster_occupancy();

CREATE OR REPLACE FUNCTION enforce_admitted_roster_inverse_seal()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; eid BIGINT:=CASE WHEN TG_OP='DELETE' THEN OLD.id ELSE NEW.id END; admitted BOOLEAN; bound BOOLEAN; expected TEXT; actual TEXT;
BEGIN
  EXECUTE format('SELECT roster_entry_sha256 FROM %I.recording_campaign_admission_results WHERE roster_entry_id=$1',s) INTO expected USING eid;
  admitted:=expected IS NOT NULL;
  IF admitted IS DISTINCT FROM true THEN RETURN NULL; END IF;
  IF TG_OP='DELETE' THEN RAISE EXCEPTION 'admitted roster entry is immutable without a typed release'; END IF;
  EXECUTE format('SELECT encode(sha256(convert_to(jsonb_build_object(''id'',e.id,''track_id'',e.track_id,''recording_id'',e.recording_id,''stream_id'',e.stream_id,''scene_identity_sha256'',e.scene_identity_sha256,''role'',e.role,''rank'',e.rank,''status'',e.status,''reason_codes'',e.reason_codes,''effective_at_us'',floor(extract(epoch from e.effective_at)*1000000)::bigint,''decision_at_us'',floor(extract(epoch from e.decision_at)*1000000)::bigint,''evidence_observed_at_us'',floor(extract(epoch from e.evidence_observed_at)*1000000)::bigint,''evidence_sha256'',e.evidence_sha256,''source_window_end_at_us'',CASE WHEN e.source_window_end_at IS NULL THEN NULL ELSE floor(extract(epoch from e.source_window_end_at)*1000000)::bigint END,''source_job_id'',e.source_job_id,''source_health_recording_id'',e.source_health_recording_id,''source_health_job_id'',e.source_health_job_id,''updated_by_user_id'',e.updated_by_user_id,''updated_at_us'',floor(extract(epoch from e.updated_at)*1000000)::bigint)::text,''UTF8'')),''hex'') FROM %I.recording_campaign_roster_entries e WHERE id=$1',s)
    INTO actual USING eid;
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_results ar JOIN %I.recording_campaign_tracks t ON t.id=ar.track_id JOIN %I.recording_campaign_roster_entries e ON e.id=ar.roster_entry_id AND e.track_id=t.id JOIN %I.recording_campaign_admission_reservations r ON r.approval_id=ar.approval_id AND r.stream_id=ar.stream_id JOIN %I.recording_targeted_probe_evidence evidence ON evidence.id=ar.second_probe_evidence_id WHERE ar.roster_entry_id=$1 AND t.state=''active'' AND e.recording_id=ar.recording_id AND e.stream_id=ar.stream_id AND e.scene_identity_sha256=r.scene_identity_sha256 AND e.role=''primary'' AND e.rank=1+(SELECT count(*) FROM %I.recording_campaign_admission_results prior WHERE prior.approval_id=ar.approval_id AND prior.stream_id<ar.stream_id) AND e.status=''probation'' AND e.reason_codes=ARRAY[''deniz_approved'',''targeted_do_two_pass'',''source_fenced'']::text[] AND e.effective_at=e.decision_at AND e.evidence_observed_at=evidence.observed_at AND e.evidence_sha256=evidence.media_sha256 AND e.updated_by_user_id=ar.actor_user_id)',s,s,s,s,s,s)
    INTO bound USING eid;
  IF bound IS DISTINCT FROM true OR actual IS DISTINCT FROM expected THEN RAISE EXCEPTION 'admitted roster entry is immutable without a typed release'; END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER recording_campaign_admission_roster_inverse AFTER UPDATE OR DELETE ON recording_campaign_roster_entries DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION enforce_admitted_roster_inverse_seal();

CREATE OR REPLACE FUNCTION enforce_admitted_track_inverse_seal()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; tid BIGINT:=CASE WHEN TG_OP='DELETE' THEN OLD.id ELSE NEW.id END; admitted BOOLEAN; active BOOLEAN;
BEGIN
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_results WHERE track_id=$1)',s) INTO admitted USING tid;
  IF admitted IS DISTINCT FROM true THEN RETURN NULL; END IF;
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_tracks track JOIN %I.recording_campaign_admission_results result ON result.track_id=track.id JOIN %I.recording_campaign_admission_approvals approval ON approval.id=result.approval_id JOIN %I.recording_campaign_authority_decisions decision ON decision.code=approval.authority_code WHERE track.id=$1 AND (track.state=''active'' OR (track.state=''complete'' AND track.deadline_at<=recording_campaign_now())) AND track.grade_floor=decision.qualification_grade_floor AND track.required_consecutive_windows=decision.qualification_required_consecutive_windows AND track.reporting_grade_floor=decision.reporting_grade_floor AND track.reporting_required_consecutive_windows=decision.reporting_required_consecutive_windows)',s,s,s,s) INTO active USING tid;
  IF active IS DISTINCT FROM true THEN RAISE EXCEPTION 'admitted campaign track lifecycle or qualification policy is immutable without a typed release'; END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER recording_campaign_admission_track_inverse AFTER UPDATE OR DELETE ON recording_campaign_tracks DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION enforce_admitted_track_inverse_seal();

-- Worker lifecycle changes use the same claim -> cloud-capacity order as probe
-- leasing. A live server-owned probe is capture authority: no controller,
-- operator force-drain, direct SQL, or claim-head rotation may strand it.
CREATE OR REPLACE FUNCTION recording_campaign_worker_lifecycle_statement_fence()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('recording-worker-claim-v1',0));
  PERFORM pg_advisory_xact_lock(hashtextextended('recording-surrender-cloud-capacity-v1',0));
  RETURN NULL;
END $$;
CREATE OR REPLACE FUNCTION guard_recording_campaign_worker_probe_lifecycle()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
  IF (NEW.state,NEW.node_id,NEW.do_droplet_id,NEW.region,NEW.size,NEW.build_sha,NEW.capacity) IS DISTINCT FROM
     (OLD.state,OLD.node_id,OLD.do_droplet_id,OLD.region,OLD.size,OLD.build_sha,OLD.capacity) AND OLD.node_id IS NOT NULL AND
     recording_worker_targeted_probe_occupancy(OLD.node_id)>0 THEN
    RAISE EXCEPTION 'managed recorder has live targeted probe capture authority';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_droplet_lifecycle_fence BEFORE UPDATE OF state,node_id,do_droplet_id,region,size,build_sha,capacity ON recorder_droplets FOR EACH STATEMENT EXECUTE FUNCTION recording_campaign_worker_lifecycle_statement_fence();
CREATE TRIGGER recording_campaign_droplet_probe_guard BEFORE UPDATE OF state,node_id,do_droplet_id,region,size,build_sha,capacity ON recorder_droplets FOR EACH ROW EXECUTE FUNCTION guard_recording_campaign_worker_probe_lifecycle();

CREATE OR REPLACE FUNCTION guard_recording_campaign_node_probe_lifecycle()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
  IF (NEW.status,NEW.node_type,NEW.account_id) IS DISTINCT FROM (OLD.status,OLD.node_type,OLD.account_id) AND
     recording_worker_targeted_probe_occupancy(OLD.id)>0 THEN
    RAISE EXCEPTION 'managed recorder node has live targeted probe capture authority';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_node_lifecycle_fence BEFORE UPDATE OF status,node_type,account_id ON nodes FOR EACH STATEMENT EXECUTE FUNCTION recording_campaign_worker_lifecycle_statement_fence();
CREATE TRIGGER recording_campaign_node_probe_guard BEFORE UPDATE OF status,node_type,account_id ON nodes FOR EACH ROW EXECUTE FUNCTION guard_recording_campaign_node_probe_lifecycle();

CREATE OR REPLACE FUNCTION guard_recording_campaign_claim_head_probe_lifecycle()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
  IF (NEW.generation,NEW.claim_token_id,NEW.state) IS DISTINCT FROM (OLD.generation,OLD.claim_token_id,OLD.state) AND
     recording_worker_targeted_probe_occupancy(OLD.node_id)>0 THEN
    RAISE EXCEPTION 'claim authority cannot rotate or block while targeted probe is live';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_claim_head_lifecycle_fence BEFORE UPDATE OF generation,claim_token_id,state ON recording_worker_claim_heads FOR EACH STATEMENT EXECUTE FUNCTION recording_campaign_worker_lifecycle_statement_fence();
CREATE TRIGGER recording_campaign_claim_head_probe_guard BEFORE UPDATE OF generation,claim_token_id,state ON recording_worker_claim_heads FOR EACH ROW EXECUTE FUNCTION guard_recording_campaign_claim_head_probe_lifecycle();

CREATE OR REPLACE FUNCTION guard_recording_campaign_node_token_probe_lifecycle()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
  IF (NEW.node_id,NEW.key_prefix,NEW.secret_hash,NEW.revoked_at,NEW.recording_claim_purpose,NEW.recording_claim_generation) IS DISTINCT FROM
     (OLD.node_id,OLD.key_prefix,OLD.secret_hash,OLD.revoked_at,OLD.recording_claim_purpose,OLD.recording_claim_generation) AND
     recording_worker_targeted_probe_occupancy(OLD.node_id)>0 THEN
    RAISE EXCEPTION 'node token authority cannot revoke, repurpose, or rotate while targeted probe is live';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_node_token_lifecycle_fence BEFORE UPDATE OF node_id,key_prefix,secret_hash,revoked_at,recording_claim_purpose,recording_claim_generation ON node_tokens FOR EACH STATEMENT EXECUTE FUNCTION recording_campaign_worker_lifecycle_statement_fence();
CREATE TRIGGER recording_campaign_node_token_probe_guard BEFORE UPDATE OF node_id,key_prefix,secret_hash,revoked_at,recording_claim_purpose,recording_claim_generation ON node_tokens FOR EACH ROW EXECUTE FUNCTION guard_recording_campaign_node_token_probe_lifecycle();

CREATE OR REPLACE FUNCTION enforce_admitted_stream_inverse_seal()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; sid BIGINT:=CASE WHEN TG_OP='DELETE' THEN OLD.id ELSE NEW.id END; admitted BOOLEAN; bound BOOLEAN;
BEGIN
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_results WHERE stream_id=$1)',s) INTO admitted USING sid;
  IF admitted IS DISTINCT FROM true THEN RETURN NULL; END IF;
  IF TG_OP='DELETE' THEN RAISE EXCEPTION 'admitted stream is immutable without a typed release'; END IF;
  EXECUTE format('SELECT bool_and((SELECT max(id) FROM %I.stream_source_revisions WHERE stream_id=st.id) IS NOT DISTINCT FROM r.source_revision_id AND encode(sha256(convert_to(st.source_url,''UTF8'')),''hex'')=r.source_url_sha256 AND encode(sha256(convert_to(COALESCE(st.source_page_url,''''),''UTF8'')),''hex'')=r.source_page_url_sha256 AND st.updated_at=r.source_updated_at AND COALESCE(st.provider,'''')=r.provider AND COALESCE(st.external_id,'''')=r.external_id AND COALESCE(NULLIF(regexp_replace(lower(st.name),''[^a-z0-9]'','''',''g''),''''),''stream''||st.id::text)=r.normalized_label AND st.local_timezone=(SELECT value->>''timezone'' FROM jsonb_array_elements(a.schedule_spec->''stream_timezones'') value WHERE (value->>''stream_id'')::bigint=st.id) AND (a.failure_domain_tag IS NULL OR a.failure_domain_tag=ANY(st.tags)) AND NOT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_source_fence_events f WHERE f.stream_id=st.id AND f.occurred_at>=r.reserved_at)) FROM %I.recording_campaign_admission_results ar JOIN %I.recording_campaign_admission_reservations r ON r.approval_id=ar.approval_id AND r.stream_id=ar.stream_id JOIN %I.recording_campaign_admission_approvals a ON a.id=ar.approval_id JOIN %I.streams st ON st.id=ar.stream_id WHERE ar.stream_id=$1',s,s,s,s,s,s)
    INTO bound USING sid;
  IF bound IS DISTINCT FROM true THEN RAISE EXCEPTION 'admitted stream source/FD inverse seal mismatch'; END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER recording_campaign_admission_stream_inverse AFTER UPDATE OR DELETE ON streams DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION enforce_admitted_stream_inverse_seal();

CREATE OR REPLACE FUNCTION enforce_admitted_revision_inverse_seal()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; sid BIGINT:=CASE WHEN TG_OP='DELETE' THEN OLD.stream_id ELSE NEW.stream_id END; admitted BOOLEAN; bound BOOLEAN;
BEGIN
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_results WHERE stream_id=$1)',s) INTO admitted USING sid;
  IF admitted IS DISTINCT FROM true THEN RETURN NULL; END IF;
  EXECUTE format('SELECT bool_and((SELECT max(id) FROM %I.stream_source_revisions WHERE stream_id=$1) IS NOT DISTINCT FROM r.source_revision_id AND NOT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_source_fence_events f WHERE f.stream_id=$1 AND f.occurred_at>=r.reserved_at)) FROM %I.recording_campaign_admission_results ar JOIN %I.recording_campaign_admission_reservations r ON r.approval_id=ar.approval_id AND r.stream_id=ar.stream_id WHERE ar.stream_id=$1',s,s,s,s)
    INTO bound USING sid;
  IF bound IS DISTINCT FROM true THEN RAISE EXCEPTION 'admitted stream source revision inverse seal mismatch'; END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER recording_campaign_admission_revision_inverse AFTER INSERT OR UPDATE OR DELETE ON stream_source_revisions DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION enforce_admitted_revision_inverse_seal();

CREATE OR REPLACE FUNCTION reject_campaign_admission_evidence_mutation() RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$ BEGIN RAISE EXCEPTION 'campaign admission evidence is append-only'; END $$;
CREATE TRIGGER recording_campaign_admission_approvals_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_campaign_admission_approvals FOR EACH STATEMENT EXECUTE FUNCTION reject_campaign_admission_evidence_mutation();
CREATE TRIGGER recording_campaign_admission_reservations_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_campaign_admission_reservations FOR EACH STATEMENT EXECUTE FUNCTION reject_campaign_admission_evidence_mutation();
CREATE TRIGGER recording_campaign_admission_reservation_terminal_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_campaign_admission_reservation_terminal_events FOR EACH STATEMENT EXECUTE FUNCTION reject_campaign_admission_evidence_mutation();
CREATE TRIGGER recording_targeted_probe_attempts_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_targeted_probe_attempts FOR EACH STATEMENT EXECUTE FUNCTION reject_campaign_admission_evidence_mutation();
CREATE TRIGGER recording_targeted_probe_attempt_terminal_events_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_targeted_probe_attempt_terminal_events FOR EACH STATEMENT EXECUTE FUNCTION reject_campaign_admission_evidence_mutation();
CREATE TRIGGER recording_targeted_probe_evidence_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_targeted_probe_evidence FOR EACH STATEMENT EXECUTE FUNCTION reject_campaign_admission_evidence_mutation();
CREATE TRIGGER recording_campaign_admission_results_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_campaign_admission_results FOR EACH STATEMENT EXECUTE FUNCTION reject_campaign_admission_evidence_mutation();
CREATE TRIGGER recording_campaign_admission_commits_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_campaign_admission_commits FOR EACH STATEMENT EXECUTE FUNCTION reject_campaign_admission_evidence_mutation();
CREATE TRIGGER recording_campaign_admission_authorizations_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_campaign_admission_tx_authorizations FOR EACH STATEMENT EXECUTE FUNCTION reject_campaign_admission_evidence_mutation();
CREATE TRIGGER recording_campaign_admission_source_fence_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_campaign_admission_source_fence_events FOR EACH STATEMENT EXECUTE FUNCTION reject_campaign_admission_evidence_mutation();
CREATE TRIGGER recording_campaign_authority_decisions_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_campaign_authority_decisions FOR EACH STATEMENT EXECUTE FUNCTION reject_campaign_admission_evidence_mutation();
CREATE TRIGGER recording_targeted_probe_orders_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_targeted_probe_orders FOR EACH STATEMENT EXECUTE FUNCTION reject_campaign_admission_evidence_mutation();
CREATE TRIGGER recording_targeted_provider_attestations_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_targeted_provider_attestations FOR EACH STATEMENT EXECUTE FUNCTION reject_campaign_admission_evidence_mutation();
CREATE TRIGGER recording_targeted_probe_scene_reviews_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_targeted_probe_scene_reviews FOR EACH STATEMENT EXECUTE FUNCTION reject_campaign_admission_evidence_mutation();
CREATE TRIGGER recording_targeted_probe_scene_presentations_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_targeted_probe_scene_presentations FOR EACH STATEMENT EXECUTE FUNCTION reject_campaign_admission_evidence_mutation();
CREATE TRIGGER recording_campaign_baseline_scene_presentations_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_campaign_baseline_scene_presentations FOR EACH STATEMENT EXECUTE FUNCTION reject_campaign_admission_evidence_mutation();
CREATE TRIGGER recording_campaign_baseline_scene_read_receipts_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_campaign_baseline_scene_read_receipts FOR EACH STATEMENT EXECUTE FUNCTION reject_campaign_admission_evidence_mutation();
CREATE TRIGGER recording_campaign_capacity_observations_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_campaign_capacity_observations FOR EACH STATEMENT EXECUTE FUNCTION reject_campaign_admission_evidence_mutation();
CREATE TRIGGER recording_campaign_capacity_reservations_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_campaign_capacity_reservations FOR EACH STATEMENT EXECUTE FUNCTION reject_campaign_admission_evidence_mutation();
CREATE TRIGGER recording_campaign_storage_observations_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_campaign_storage_observations FOR EACH STATEMENT EXECUTE FUNCTION reject_campaign_admission_evidence_mutation();
CREATE TRIGGER recording_campaign_storage_reservations_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_campaign_storage_reservations FOR EACH STATEMENT EXECUTE FUNCTION reject_campaign_admission_evidence_mutation();

-- Pin every authority/trigger function to the schema selected by the migration.
-- pg_temp is deliberately last so hostile temporary relations/functions cannot
-- shadow the immutable admission graph. This also preserves isolated-schema PG
-- tests without hard-coding public.
DO $pin_recording_campaign_admission_search_path$
DECLARE install_schema TEXT:=current_schema(); signature TEXT;
BEGIN
  IF install_schema IS NULL OR install_schema LIKE 'pg_temp%' THEN
    RAISE EXCEPTION 'campaign admission must install in an explicit persistent schema';
  END IF;
  FOREACH signature IN ARRAY ARRAY[
    'recording_campaign_now()',
    'recording_campaign_authorize_account(text,uuid,bigint,bigint,bigint,text)',
    'recording_campaign_authorize_node(text,uuid,bigint,bigint,bigint,bigint,text)',
    'recording_campaign_create_approval(uuid,bigint,bigint,text,text,text,timestamp with time zone,jsonb,jsonb,text)',
    'recording_campaign_create_probe_order(uuid,uuid,bigint,bigint,bigint)',
    'recording_campaign_create_provider_attestation(bigint,bigint,bigint,text,text,text,text,text,text)',
    'recording_campaign_create_probe_attempt(uuid,uuid,uuid,uuid,bigint,bigint,bigint,bigint,uuid,bigint,text,text,bigint,text,text,timestamp with time zone,text,text,text,text,bigint,bigint)',
    'recording_campaign_lease_probe(bigint,bigint,bigint,text,bigint,bigint,text,text,text,text,text,text,uuid,uuid,text,text,text,text,bigint,bigint)',
    'recording_campaign_create_probe_evidence(uuid,uuid,bigint,bigint,text,double precision,bigint,integer,text,text,text,text,text,text,boolean,integer,integer,double precision,text,bigint,text,text,bigint,text,text,text,text,text,text,text,text,text,text,text,text,text)',
    'recording_campaign_submit_probe_evidence(bigint,bigint,bigint,text,uuid,uuid,uuid,bigint,bigint,jsonb)',
    'recording_campaign_read_probe_attempt(bigint,bigint,bigint,text,uuid,uuid,uuid,bigint)',
    'recording_campaign_create_scene_presentation(uuid,bigint,uuid,uuid,bigint)',
    'recording_campaign_create_scene_review(uuid,bigint,uuid,uuid,uuid,bigint)',
    'recording_campaign_approve(uuid,bigint,bigint,bigint,text,text,text,text,timestamp with time zone,jsonb,jsonb,text)',
    'recording_campaign_expire_approval(uuid,uuid,bigint,bigint,bigint,text)',
    'recording_campaign_queue_probe(uuid,uuid,bigint,bigint,bigint,text,bigint)',
    'recording_campaign_present_probe_scene(uuid,uuid,bigint,bigint,bigint,text,uuid)',
    'recording_campaign_review_probe_scene(uuid,uuid,bigint,bigint,bigint,text,uuid,uuid)',
    'recording_campaign_read_probe_scene(bigint,bigint,bigint,text,uuid)',
    'recording_campaign_read_baseline_scene(uuid,bigint,bigint,bigint,text,text,bigint,bigint)',
    'recording_campaign_present_baseline_scene(uuid,uuid,bigint,bigint,bigint,text,text,bigint,bigint)',
    'recording_campaign_attest_baseline_scene(uuid,bigint,bigint,bigint,text,bigint,text)',
    'recording_campaign_create_capacity_observation(uuid,bigint,timestamp with time zone,timestamp with time zone,text,text,text,integer,integer,integer,integer,text,integer,integer,integer,integer,integer,text,text,text,text)',
    'recording_campaign_create_capacity_reservation(uuid,bigint,uuid,integer,integer)',
    'recording_campaign_forecast_peak_slots(bigint)',
    'recording_campaign_relay_failure_capacity(bigint)',
    'recording_campaign_create_storage_observation(uuid,bigint,bigint,timestamp with time zone,bigint,bigint,bigint,integer,integer,bigint,integer,bigint,bigint,bigint,boolean)',
    'recording_campaign_create_storage_reservation(uuid,bigint,uuid,bigint,timestamp with time zone)',
    'recording_campaign_create_admission_result(uuid,uuid,uuid,bigint,bigint,bigint,bigint,bigint,bigint,text,text,text)',
    'recording_campaign_create_admission_commit(uuid,bigint,bigint,bigint,jsonb)',
    'recording_campaign_admit(uuid,bigint,bigint,bigint,text,jsonb,jsonb,jsonb,jsonb)',
    'recording_campaign_replay(uuid,bigint,text)',
    'recording_campaign_replay_approval(bigint,uuid,text,text)',
    'validate_recording_targeted_probe_order()',
    'validate_recording_targeted_probe_attempt_terminal_event()',
    'validate_recording_targeted_provider_attestation()',
    'validate_recording_campaign_tx_authorization()',
    'validate_recording_campaign_reservation_terminal_event()',
    'validate_recording_targeted_probe_scene_presentation()',
    'validate_recording_targeted_probe_scene_review()',
    'validate_recording_campaign_capacity_observation()',
    'validate_recording_campaign_capacity_reservation()',
    'validate_recording_campaign_storage_observation()',
    'validate_recording_campaign_storage_reservation()',
    'validate_recording_campaign_admission_commit()',
    'enforce_recording_campaign_result_has_commit()',
    'validate_recording_campaign_admission_approval()',
    'reserve_recording_campaign_admission_entries()',
    'validate_recording_campaign_admission_reservation()',
    'audit_recording_campaign_reserved_source_mutation()',
    'audit_recording_campaign_reserved_revision_mutation()',
    'validate_recording_targeted_probe_attempt()',
    'validate_recording_targeted_probe_evidence()',
    'validate_recording_campaign_admission_result()',
    'recording_campaign_admission_statement_fence()',
    'recording_campaign_assert_track_activation_occupancy(bigint,bigint)',
    'guard_recording_campaign_track_activation_occupancy()',
    'guard_reserved_completed_recording_activation()',
    'enforce_reserved_activation_has_result()',
    'enforce_admitted_recording_inverse_seal()',
    'guard_recording_campaign_track_state()',
    'transition_recording_campaign_track(bigint,text,text[],bigint,timestamp with time zone)',
    'guard_reserved_campaign_roster_occupancy()',
    'enforce_admitted_roster_inverse_seal()',
    'enforce_admitted_track_inverse_seal()',
    'recording_campaign_worker_lifecycle_statement_fence()',
    'guard_recording_campaign_worker_probe_lifecycle()',
    'guard_recording_campaign_node_probe_lifecycle()',
    'guard_recording_campaign_claim_head_probe_lifecycle()',
    'guard_recording_campaign_node_token_probe_lifecycle()',
    'enforce_admitted_stream_inverse_seal()',
    'enforce_admitted_revision_inverse_seal()',
    'reject_campaign_admission_evidence_mutation()'
  ] LOOP
    EXECUTE format('ALTER FUNCTION %I.%s SET search_path = %I, pg_catalog, pg_temp',install_schema,signature,install_schema);
  END LOOP;
END
$pin_recording_campaign_admission_search_path$;

REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON recording_campaign_authority_decisions,recording_campaign_admission_approvals,recording_campaign_admission_reservations,recording_campaign_admission_reservation_terminal_events,recording_targeted_probe_orders,recording_targeted_provider_attestations,recording_targeted_probe_attempts,recording_targeted_probe_attempt_terminal_events,recording_targeted_probe_evidence,recording_targeted_probe_scene_presentations,recording_targeted_probe_scene_reviews,recording_campaign_baseline_scene_read_receipts,recording_campaign_baseline_scene_presentations,recording_campaign_capacity_observations,recording_campaign_capacity_reservations,recording_campaign_storage_observations,recording_campaign_storage_reservations,recording_campaign_admission_results,recording_campaign_admission_commits,recording_campaign_admission_tx_authorizations,recording_campaign_admission_source_fence_events FROM PUBLIC;
REVOKE ALL ON FUNCTION validate_recording_targeted_probe_order(),validate_recording_targeted_probe_attempt_terminal_event(),validate_recording_targeted_provider_attestation(),validate_recording_campaign_tx_authorization(),validate_recording_campaign_reservation_terminal_event(),validate_recording_targeted_probe_scene_presentation(),validate_recording_targeted_probe_scene_review(),validate_recording_campaign_capacity_observation(),validate_recording_campaign_capacity_reservation(),validate_recording_campaign_storage_observation(),validate_recording_campaign_storage_reservation(),validate_recording_campaign_admission_commit(),enforce_recording_campaign_result_has_commit(),validate_recording_campaign_admission_approval(),reserve_recording_campaign_admission_entries(),validate_recording_campaign_admission_reservation(),audit_recording_campaign_reserved_source_mutation(),audit_recording_campaign_reserved_revision_mutation(),validate_recording_targeted_probe_attempt(),validate_recording_targeted_probe_evidence(),validate_recording_campaign_admission_result(),recording_campaign_admission_statement_fence(),recording_campaign_assert_track_activation_occupancy(BIGINT,BIGINT),guard_recording_campaign_track_activation_occupancy(),guard_reserved_completed_recording_activation(),enforce_reserved_activation_has_result(),enforce_admitted_recording_inverse_seal(),guard_recording_campaign_track_state(),transition_recording_campaign_track(BIGINT,TEXT,TEXT[],BIGINT,TIMESTAMPTZ),guard_reserved_campaign_roster_occupancy(),enforce_admitted_roster_inverse_seal(),enforce_admitted_track_inverse_seal(),recording_campaign_worker_lifecycle_statement_fence(),guard_recording_campaign_worker_probe_lifecycle(),guard_recording_campaign_node_probe_lifecycle(),guard_recording_campaign_claim_head_probe_lifecycle(),guard_recording_campaign_node_token_probe_lifecycle(),enforce_admitted_stream_inverse_seal(),enforce_admitted_revision_inverse_seal(),reject_campaign_admission_evidence_mutation() FROM PUBLIC;
REVOKE ALL ON FUNCTION recording_campaign_authorize_account(TEXT,UUID,BIGINT,BIGINT,BIGINT,TEXT),recording_campaign_authorize_node(TEXT,UUID,BIGINT,BIGINT,BIGINT,BIGINT,TEXT) FROM PUBLIC;

-- Structural role split. The migration-only login supplies these settings and
-- is the sole member of the NOLOGIN authority role; the Render-managed runtime
-- login is deliberately not a member and cannot RESET/SET ROLE into authority.
DO $recording_campaign_role_split$
DECLARE
  install_schema TEXT:=current_schema();
  runtime_role TEXT:=current_setting('stoarama.campaign_runtime_role',true);
  executor_role TEXT:=current_setting('stoarama.campaign_executor_role',true);
  authority_role TEXT:=current_setting('stoarama.campaign_authority_role',true);
  object_name TEXT; signature TEXT; runtime_member BOOLEAN; executor_member BOOLEAN;
  runtime_login BOOLEAN; executor_login BOOLEAN; authority_exact BOOLEAN;
  product_manifest_sha256 TEXT; authority_member_count INTEGER; migrator_member_count INTEGER;
  authority_tables TEXT[]:=ARRAY[
    'recording_campaign_authority_decisions','recording_campaign_admission_approvals','recording_campaign_admission_reservations',
    'recording_campaign_admission_reservation_terminal_events',
    'recording_campaign_admission_source_fence_events','recording_targeted_probe_orders','recording_targeted_provider_attestations',
    'recording_targeted_probe_attempts','recording_targeted_probe_evidence','recording_targeted_probe_scene_presentations','recording_targeted_probe_scene_reviews',
    'recording_targeted_probe_attempt_terminal_events',
    'recording_campaign_baseline_scene_read_receipts','recording_campaign_baseline_scene_presentations',
    'recording_campaign_capacity_observations','recording_campaign_capacity_reservations','recording_campaign_storage_observations',
    'recording_campaign_storage_reservations','recording_campaign_admission_results','recording_campaign_admission_commits',
    'recording_campaign_admission_tx_authorizations'
  ];
  authority_functions TEXT[]:=ARRAY[
    'recording_campaign_now()',
    'recording_campaign_authorize_account(text,uuid,bigint,bigint,bigint,text)',
    'recording_campaign_authorize_node(text,uuid,bigint,bigint,bigint,bigint,text)',
    'recording_campaign_create_approval(uuid,bigint,bigint,text,text,text,timestamp with time zone,jsonb,jsonb,text)',
    'recording_campaign_create_probe_order(uuid,uuid,bigint,bigint,bigint)',
    'recording_campaign_create_provider_attestation(bigint,bigint,bigint,text,text,text,text,text,text)',
    'recording_campaign_create_probe_attempt(uuid,uuid,uuid,uuid,bigint,bigint,bigint,bigint,uuid,bigint,text,text,bigint,text,text,timestamp with time zone,text,text,text,text,bigint,bigint)',
    'recording_campaign_lease_probe(bigint,bigint,bigint,text,bigint,bigint,text,text,text,text,text,text,uuid,uuid,text,text,text,text,bigint,bigint)',
    'recording_campaign_create_probe_evidence(uuid,uuid,bigint,bigint,text,double precision,bigint,integer,text,text,text,text,text,text,boolean,integer,integer,double precision,text,bigint,text,text,bigint,text,text,text,text,text,text,text,text,text,text,text,text,text)',
    'recording_campaign_submit_probe_evidence(bigint,bigint,bigint,text,uuid,uuid,uuid,bigint,bigint,jsonb)',
    'recording_campaign_read_probe_attempt(bigint,bigint,bigint,text,uuid,uuid,uuid,bigint)',
    'recording_campaign_create_scene_presentation(uuid,bigint,uuid,uuid,bigint)',
    'recording_campaign_create_scene_review(uuid,bigint,uuid,uuid,uuid,bigint)',
    'recording_campaign_approve(uuid,bigint,bigint,bigint,text,text,text,text,timestamp with time zone,jsonb,jsonb,text)',
    'recording_campaign_expire_approval(uuid,uuid,bigint,bigint,bigint,text)',
    'recording_campaign_queue_probe(uuid,uuid,bigint,bigint,bigint,text,bigint)',
    'recording_campaign_present_probe_scene(uuid,uuid,bigint,bigint,bigint,text,uuid)',
    'recording_campaign_review_probe_scene(uuid,uuid,bigint,bigint,bigint,text,uuid,uuid)',
    'recording_campaign_read_probe_scene(bigint,bigint,bigint,text,uuid)',
    'recording_campaign_read_baseline_scene(uuid,bigint,bigint,bigint,text,text,bigint,bigint)',
    'recording_campaign_present_baseline_scene(uuid,uuid,bigint,bigint,bigint,text,text,bigint,bigint)',
    'recording_campaign_attest_baseline_scene(uuid,bigint,bigint,bigint,text,bigint,text)',
    'recording_campaign_create_capacity_observation(uuid,bigint,timestamp with time zone,timestamp with time zone,text,text,text,integer,integer,integer,integer,text,integer,integer,integer,integer,integer,text,text,text,text)',
    'recording_campaign_create_capacity_reservation(uuid,bigint,uuid,integer,integer)',
    'recording_campaign_forecast_peak_slots(bigint)',
    'recording_campaign_relay_failure_capacity(bigint)',
    'recording_campaign_create_storage_observation(uuid,bigint,bigint,timestamp with time zone,bigint,bigint,bigint,integer,integer,bigint,integer,bigint,bigint,bigint,boolean)',
    'recording_campaign_create_storage_reservation(uuid,bigint,uuid,bigint,timestamp with time zone)',
    'recording_campaign_create_admission_result(uuid,uuid,uuid,bigint,bigint,bigint,bigint,bigint,bigint,text,text,text)',
    'recording_campaign_create_admission_commit(uuid,bigint,bigint,bigint,jsonb)',
    'recording_campaign_admit(uuid,bigint,bigint,bigint,text,jsonb,jsonb,jsonb,jsonb)',
    'recording_campaign_replay(uuid,bigint,text)',
    'recording_campaign_replay_approval(bigint,uuid,text,text)',
    'validate_recording_targeted_probe_order()','validate_recording_targeted_probe_attempt_terminal_event()','validate_recording_targeted_provider_attestation()',
    'validate_recording_campaign_tx_authorization()','validate_recording_campaign_reservation_terminal_event()','validate_recording_targeted_probe_scene_presentation()','validate_recording_targeted_probe_scene_review()',
    'validate_recording_campaign_capacity_observation()','validate_recording_campaign_capacity_reservation()',
    'validate_recording_campaign_storage_observation()','validate_recording_campaign_storage_reservation()',
    'validate_recording_campaign_admission_commit()','enforce_recording_campaign_result_has_commit()',
    'validate_recording_campaign_admission_approval()','reserve_recording_campaign_admission_entries()',
    'validate_recording_campaign_admission_reservation()','audit_recording_campaign_reserved_source_mutation()',
    'audit_recording_campaign_reserved_revision_mutation()','validate_recording_targeted_probe_attempt()',
    'validate_recording_targeted_probe_evidence()','validate_recording_campaign_admission_result()','recording_campaign_admission_statement_fence()',
    'recording_campaign_assert_track_activation_occupancy(bigint,bigint)','guard_recording_campaign_track_activation_occupancy()',
    'guard_reserved_completed_recording_activation()','enforce_reserved_activation_has_result()',
    'enforce_admitted_recording_inverse_seal()','guard_recording_campaign_track_state()',
    'transition_recording_campaign_track(bigint,text,text[],bigint,timestamp with time zone)',
    'guard_reserved_campaign_roster_occupancy()',
    'enforce_admitted_roster_inverse_seal()','enforce_admitted_track_inverse_seal()',
    'recording_campaign_worker_lifecycle_statement_fence()','guard_recording_campaign_worker_probe_lifecycle()',
    'guard_recording_campaign_node_probe_lifecycle()',
    'guard_recording_campaign_claim_head_probe_lifecycle()',
    'guard_recording_campaign_node_token_probe_lifecycle()',
    'enforce_admitted_stream_inverse_seal()','enforce_admitted_revision_inverse_seal()',
    'reject_campaign_admission_evidence_mutation()'
  ];
BEGIN
  IF runtime_role IS NULL OR runtime_role!~'^[a-z][a-z0-9_]{0,62}$' OR
     executor_role IS NULL OR executor_role!~'^[a-z][a-z0-9_]{0,62}$' OR
     authority_role IS NULL OR authority_role!~'^[a-z][a-z0-9_]{0,62}$' OR
     runtime_role=authority_role OR executor_role=authority_role OR runtime_role=executor_role THEN
    RAISE EXCEPTION '0140 requires exact distinct runtime, executor, and authority roles';
  END IF;
  SELECT rolcanlogin INTO runtime_login FROM pg_catalog.pg_roles WHERE rolname=runtime_role;
  SELECT rolcanlogin INTO executor_login FROM pg_catalog.pg_roles WHERE rolname=executor_role;
  SELECT NOT rolcanlogin AND NOT rolinherit INTO authority_exact FROM pg_catalog.pg_roles WHERE rolname=authority_role;
  SELECT pg_has_role(runtime_role,authority_role,'MEMBER') INTO runtime_member;
  SELECT pg_has_role(executor_role,authority_role,'MEMBER') INTO executor_member;
  SELECT count(*) INTO authority_member_count FROM pg_catalog.pg_auth_members membership
    JOIN pg_catalog.pg_roles role ON role.oid=membership.roleid WHERE role.rolname=authority_role;
  SELECT count(*) INTO migrator_member_count FROM pg_catalog.pg_auth_members membership
    JOIN pg_catalog.pg_roles role ON role.oid=membership.roleid
    JOIN pg_catalog.pg_roles member ON member.oid=membership.member
    WHERE role.rolname=authority_role AND member.rolname=current_user;
  IF runtime_login IS DISTINCT FROM true OR executor_login IS DISTINCT FROM true OR authority_exact IS DISTINCT FROM true OR
     runtime_member IS DISTINCT FROM false OR executor_member IS DISTINCT FROM false OR
     authority_member_count<>1 OR migrator_member_count<>1 OR
     current_user IN(runtime_role,executor_role) THEN
    RAISE EXCEPTION '0140 database role split preconditions are not exact';
  END IF;
  EXECUTE format('REVOKE CREATE ON SCHEMA %I FROM PUBLIC,%I,%I',install_schema,runtime_role,executor_role);
  EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I',install_schema,runtime_role);
  EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I',install_schema,executor_role);
  EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I',install_schema,authority_role);
  -- The migration ledger is owned and used only by the migrator. It is not a
  -- product table and must never be included in the runtime's broad product
  -- DML manifest.
  EXECUTE format('REVOKE ALL ON TABLE %I.schema_migrations FROM PUBLIC,%I,%I',install_schema,runtime_role,executor_role);
  FOREACH object_name IN ARRAY authority_tables LOOP
    EXECUTE format('ALTER TABLE %I.%I OWNER TO %I',install_schema,object_name,authority_role);
    EXECUTE format('REVOKE ALL ON TABLE %I.%I FROM PUBLIC,%I,%I',install_schema,object_name,runtime_role,executor_role);
  END LOOP;
  EXECUTE format('GRANT SELECT ON TABLE %I.recording_targeted_probe_attempts,%I.recording_targeted_probe_evidence,%I.recording_targeted_probe_attempt_terminal_events TO %I',install_schema,install_schema,install_schema,current_user);
  EXECUTE format('ALTER FUNCTION %I.recording_worker_targeted_probe_occupancy(bigint) SET search_path=%I,pg_catalog,pg_temp',install_schema,install_schema);
  EXECUTE format('REVOKE ALL ON FUNCTION %I.recording_worker_targeted_probe_occupancy(bigint) FROM PUBLIC,%I,%I',install_schema,runtime_role,executor_role);
  EXECUTE format('GRANT EXECUTE ON FUNCTION %I.recording_worker_targeted_probe_occupancy(bigint) TO %I',install_schema,runtime_role);
  EXECUTE format('GRANT EXECUTE ON FUNCTION %I.recording_worker_targeted_probe_occupancy(bigint) TO %I',install_schema,authority_role);
  SELECT encode(sha256(convert_to(string_agg(c.relname,E'\n' ORDER BY c.relname)||E'\n','UTF8')),'hex')
    INTO product_manifest_sha256
  FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
  WHERE n.nspname=install_schema AND c.relkind IN('r','p') AND c.relname<>'schema_migrations' AND NOT(c.relname=ANY(authority_tables));
  IF product_manifest_sha256<>'769af37338fc1a6775f1e7be93a55255fe21d8eebea9201e27f1ae4ddd6eabfb' THEN
    RAISE EXCEPTION '0140 reviewed product table manifest mismatch: %',product_manifest_sha256;
  END IF;
  FOR object_name IN
    SELECT c.relname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname=install_schema AND c.relkind IN('r','p') AND c.relname<>'schema_migrations' AND NOT(c.relname=ANY(authority_tables))
    ORDER BY c.relname
  LOOP
    EXECUTE format('GRANT SELECT,INSERT,UPDATE,DELETE ON TABLE %I.%I TO %I',install_schema,object_name,runtime_role);
  END LOOP;
  -- Campaign track events are the unforgeable transaction-local witness used
  -- by the state guard. Only the typed authority-owned transition function may
  -- append one; the general product runtime keeps read-only access.
  EXECUTE format('REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON TABLE %I.recording_campaign_track_events FROM %I',install_schema,runtime_role);
  FOREACH signature IN ARRAY authority_functions LOOP
    EXECUTE format('ALTER FUNCTION %I.%s OWNER TO %I',install_schema,signature,authority_role);
    EXECUTE format('ALTER FUNCTION %I.%s SECURITY DEFINER',install_schema,signature);
    EXECUTE format('REVOKE ALL ON FUNCTION %I.%s FROM PUBLIC,%I,%I',install_schema,signature,runtime_role,executor_role);
  END LOOP;
  EXECUTE format('GRANT EXECUTE ON FUNCTION %I.recording_campaign_approve(uuid,bigint,bigint,bigint,text,text,text,text,timestamp with time zone,jsonb,jsonb,text) TO %I',install_schema,executor_role);
  EXECUTE format('GRANT EXECUTE ON FUNCTION %I.recording_campaign_expire_approval(uuid,uuid,bigint,bigint,bigint,text) TO %I',install_schema,executor_role);
  EXECUTE format('GRANT EXECUTE ON FUNCTION %I.recording_campaign_queue_probe(uuid,uuid,bigint,bigint,bigint,text,bigint) TO %I',install_schema,executor_role);
  EXECUTE format('GRANT EXECUTE ON FUNCTION %I.recording_campaign_present_probe_scene(uuid,uuid,bigint,bigint,bigint,text,uuid) TO %I',install_schema,executor_role);
  EXECUTE format('GRANT EXECUTE ON FUNCTION %I.recording_campaign_review_probe_scene(uuid,uuid,bigint,bigint,bigint,text,uuid,uuid) TO %I',install_schema,executor_role);
  EXECUTE format('GRANT EXECUTE ON FUNCTION %I.recording_campaign_read_probe_scene(bigint,bigint,bigint,text,uuid) TO %I',install_schema,executor_role);
  EXECUTE format('GRANT EXECUTE ON FUNCTION %I.recording_campaign_read_baseline_scene(uuid,bigint,bigint,bigint,text,text,bigint,bigint) TO %I',install_schema,executor_role);
  EXECUTE format('GRANT EXECUTE ON FUNCTION %I.recording_campaign_present_baseline_scene(uuid,uuid,bigint,bigint,bigint,text,text,bigint,bigint) TO %I',install_schema,executor_role);
  EXECUTE format('GRANT EXECUTE ON FUNCTION %I.recording_campaign_attest_baseline_scene(uuid,bigint,bigint,bigint,text,bigint,text) TO %I',install_schema,executor_role);
  EXECUTE format('GRANT EXECUTE ON FUNCTION %I.recording_campaign_admit(uuid,bigint,bigint,bigint,text,jsonb,jsonb,jsonb,jsonb) TO %I',install_schema,executor_role);
  EXECUTE format('GRANT EXECUTE ON FUNCTION %I.recording_campaign_replay(uuid,bigint,text) TO %I',install_schema,executor_role);
  EXECUTE format('GRANT EXECUTE ON FUNCTION %I.recording_campaign_replay_approval(bigint,uuid,text,text) TO %I',install_schema,executor_role);
  EXECUTE format('GRANT EXECUTE ON FUNCTION %I.recording_campaign_lease_probe(bigint,bigint,bigint,text,bigint,bigint,text,text,text,text,text,text,uuid,uuid,text,text,text,text,bigint,bigint) TO %I',install_schema,executor_role);
  EXECUTE format('GRANT EXECUTE ON FUNCTION %I.recording_campaign_read_probe_attempt(bigint,bigint,bigint,text,uuid,uuid,uuid,bigint) TO %I',install_schema,executor_role);
  EXECUTE format('GRANT EXECUTE ON FUNCTION %I.recording_campaign_submit_probe_evidence(bigint,bigint,bigint,text,uuid,uuid,uuid,bigint,bigint,jsonb) TO %I',install_schema,executor_role);
  EXECUTE format('GRANT SELECT ON TABLE %I.accounts,%I.users,%I.memberships,%I.account_sessions,%I.nodes,%I.node_tokens,%I.recorder_droplets,%I.recording_worker_claim_heads,%I.streams,%I.stream_source_revisions,%I.recordings,%I.recording_scene_frame_evidence,%I.recording_campaign_tracks,%I.recording_campaign_roster_entries,%I.connections,%I.storage_destinations,%I.recording_jobs,%I.recording_clips,%I.relay_groups TO %I',install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,runtime_role);
  EXECUTE format('GRANT SELECT ON TABLE %I.accounts,%I.users,%I.memberships,%I.account_sessions,%I.nodes,%I.node_tokens,%I.recorder_droplets,%I.recording_worker_claim_heads,%I.streams,%I.stream_source_revisions,%I.recordings,%I.frames,%I.media_objects,%I.recording_scene_frame_evidence,%I.recording_campaign_tracks,%I.recording_campaign_roster_entries,%I.connections,%I.storage_destinations,%I.recording_jobs,%I.recording_clips,%I.relay_groups TO %I',install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,authority_role);
  -- PostgreSQL requires UPDATE privilege for FOR UPDATE and FOR SHARE.
  -- Admission locks these product identities to preserve the reviewed account,
  -- source, storage, auth, and worker order, so grant only immutable key columns.
  EXECUTE format('GRANT UPDATE(id) ON TABLE %I.accounts,%I.account_sessions,%I.connections,%I.frames,%I.media_objects,%I.node_tokens,%I.nodes,%I.recorder_droplets,%I.recording_scene_frame_evidence,%I.stream_source_revisions,%I.streams,%I.users TO %I',install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,authority_role);
  EXECUTE format('GRANT UPDATE(user_id) ON TABLE %I.memberships TO %I',install_schema,authority_role);
  EXECUTE format('GRANT UPDATE(node_id) ON TABLE %I.recording_worker_claim_heads TO %I',install_schema,authority_role);
  EXECUTE format('GRANT INSERT,UPDATE ON TABLE %I.recordings TO %I',install_schema,authority_role);
  EXECUTE format('GRANT INSERT ON TABLE %I.recording_scene_frame_evidence TO %I',install_schema,authority_role);
  EXECUTE format('GRANT UPDATE ON TABLE %I.recording_jobs TO %I',install_schema,authority_role);
  -- The 0129 roster INSERT trigger and typed track transition are invoker
  -- functions. Grant their exact append-only side effects to the authority;
  -- without these the split-role admission path cannot commit.
  EXECUTE format('GRANT INSERT ON TABLE %I.recording_campaign_tracks,%I.recording_campaign_roster_entries,%I.recording_campaign_roster_events,%I.recording_campaign_track_events TO %I',install_schema,install_schema,install_schema,install_schema,authority_role);
  EXECUTE format('GRANT UPDATE ON TABLE %I.recording_campaign_tracks TO %I',install_schema,authority_role);
  FOR object_name IN
    SELECT c.relname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname=install_schema AND c.relkind='S' AND pg_catalog.pg_get_userbyid(c.relowner)<>authority_role
    ORDER BY c.relname
  LOOP
    EXECUTE format('GRANT USAGE,SELECT ON SEQUENCE %I.%I TO %I',install_schema,object_name,runtime_role);
  END LOOP;
  EXECUTE format('REVOKE USAGE,SELECT,UPDATE ON SEQUENCE %I.recording_campaign_track_events_id_seq FROM %I',install_schema,runtime_role);
  EXECUTE format('GRANT USAGE,SELECT ON SEQUENCE %I.recordings_id_seq,%I.recording_scene_frame_evidence_id_seq,%I.recording_campaign_tracks_id_seq,%I.recording_campaign_roster_entries_id_seq,%I.recording_campaign_roster_events_id_seq,%I.recording_campaign_track_events_id_seq TO %I',install_schema,install_schema,install_schema,install_schema,install_schema,install_schema,authority_role);
  EXECUTE format('GRANT EXECUTE ON FUNCTION %I.transition_recording_campaign_track(bigint,text,text[],bigint,timestamp with time zone) TO %I',install_schema,authority_role);
  -- Migration 0139 revokes these product functions from PUBLIC. The distinct
  -- runtime login needs only the exact R10 calls made by current binaries.
  FOREACH signature IN ARRAY ARRAY[
    'recording_surrender_source_snapshot(bigint)',
    'recording_surrender_destination_snapshot(bigint)',
    'recording_surrender_capture_config_snapshot(bigint,bigint,uuid)',
    'recording_surrender_token_can_access_lease(bigint,bigint,bigint,bigint)',
    'recording_surrender_reconcile_expired_upload_sessions()',
    'recording_surrender_expire_set_plans()',
    'recording_surrender_reclaim_expired()',
    'recording_surrender_request_sha(uuid,text,text,bigint,uuid,bigint,integer,bigint,integer)',
    'recording_surrender_relay_candidate_eligible(bigint,bigint)',
    'recording_surrender_relay_alternate(bigint,text)',
    'recording_worker_targeted_probe_occupancy(bigint)'
  ] LOOP
    EXECUTE format('GRANT EXECUTE ON FUNCTION %I.%s TO %I',install_schema,signature,runtime_role);
  END LOOP;
END
$recording_campaign_role_split$;
