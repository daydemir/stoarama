-- Capture/stitch presentation evidence v2, C1: server-side lifecycle only.
--
-- This migration intentionally provides no admission creation surface and no
-- enabled claim path.  A later, separately reviewed rollout must add both.

CREATE FUNCTION lock_recording_campaign_protection(p_account_id BIGINT, p_recording_id BIGINT)
RETURNS BOOLEAN LANGUAGE plpgsql AS $$
DECLARE protected BOOLEAN;
BEGIN
  -- The recording-only key is always acquired first. It prevents lock-order
  -- inversion when a roster trigger must discover the campaign account from
  -- a track row while an admission is already fencing that same recording.
  PERFORM pg_advisory_xact_lock(hashtextextended(
    'recording-campaign-protection-recording:'||p_recording_id::text, 0));
  PERFORM pg_advisory_xact_lock(hashtextextended(
    'recording-campaign-protection:'||p_account_id::text||':'||p_recording_id::text, 0));
  PERFORM 1 FROM recordings WHERE id=p_recording_id AND account_id=p_account_id FOR UPDATE;
  SELECT EXISTS(
    SELECT 1 FROM protected_campaign_recordings
    WHERE account_id=p_account_id AND recording_id=p_recording_id
  ) INTO protected;
  RETURN protected;
END $$;

-- Campaign protection and presentation admission share this fence.  The
-- campaign triggers make the contract effective for the currently shipped
-- roster tables; roster v2 must take the same advisory lock before changing
-- its head/occupancy rows.
CREATE FUNCTION fence_recording_campaign_roster_protection() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE v_account BIGINT; first_recording BIGINT; second_recording BIGINT;
BEGIN
  first_recording:=LEAST(COALESCE(OLD.recording_id,NEW.recording_id),COALESCE(NEW.recording_id,OLD.recording_id));
  second_recording:=GREATEST(COALESCE(OLD.recording_id,NEW.recording_id),COALESCE(NEW.recording_id,OLD.recording_id));
  PERFORM pg_advisory_xact_lock(hashtextextended(
    'recording-campaign-protection-recording:'||first_recording::text, 0));
  IF second_recording IS DISTINCT FROM first_recording THEN
    PERFORM pg_advisory_xact_lock(hashtextextended(
      'recording-campaign-protection-recording:'||second_recording::text, 0));
  END IF;
  SELECT account_id INTO v_account FROM recording_campaign_tracks WHERE id=COALESCE(NEW.track_id,OLD.track_id) FOR SHARE;
  PERFORM lock_recording_campaign_protection(v_account,first_recording);
  IF second_recording IS DISTINCT FROM first_recording THEN
    PERFORM lock_recording_campaign_protection(v_account,second_recording);
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_roster_presentation_fence
BEFORE INSERT OR UPDATE ON recording_campaign_roster_entries FOR EACH ROW
EXECUTE FUNCTION fence_recording_campaign_roster_protection();

CREATE FUNCTION fence_recording_campaign_track_protection() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE entry RECORD;
BEGIN
  IF NEW.state IS DISTINCT FROM OLD.state THEN
    FOR entry IN SELECT recording_id FROM recording_campaign_roster_entries WHERE track_id=NEW.id ORDER BY recording_id LOOP
      PERFORM lock_recording_campaign_protection(NEW.account_id,entry.recording_id);
    END LOOP;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_track_presentation_fence
BEFORE UPDATE ON recording_campaign_tracks FOR EACH ROW
EXECUTE FUNCTION fence_recording_campaign_track_protection();

-- Cross-language semantic-tool identity v2. Every field is UTF-8 encoded as
-- its decimal octet length, a colon, its exact bytes, and a newline. The
-- domain tag prevents reuse by another digest contract.
CREATE FUNCTION recording_presentation_v2_tool_identity(
  p_ffmpeg TEXT,p_ffprobe TEXT,p_avformat TEXT,p_avcodec TEXT,p_avutil TEXT,
  p_build_sha TEXT,p_demuxer TEXT,p_video_decoder TEXT,p_audio_decoder TEXT,p_parser TEXT)
RETURNS TEXT LANGUAGE sql IMMUTABLE STRICT AS $$
  SELECT encode(sha256(convert_to(
    'presentation-semantic-tool-v2'||chr(10)||
    octet_length(p_ffmpeg)::text||':'||p_ffmpeg||chr(10)||
    octet_length(p_ffprobe)::text||':'||p_ffprobe||chr(10)||
    octet_length(p_avformat)::text||':'||p_avformat||chr(10)||
    octet_length(p_avcodec)::text||':'||p_avcodec||chr(10)||
    octet_length(p_avutil)::text||':'||p_avutil||chr(10)||
    octet_length(p_build_sha)::text||':'||p_build_sha||chr(10)||
    octet_length(p_demuxer)::text||':'||p_demuxer||chr(10)||
    octet_length(p_video_decoder)::text||':'||p_video_decoder||chr(10)||
    octet_length(p_audio_decoder)::text||':'||p_audio_decoder||chr(10)||
    octet_length(p_parser)::text||':'||p_parser||chr(10),'UTF8')),'hex')
$$;

-- Canonical retained-file identity. Device/inode are canonical unsigned
-- decimal strings so the contract is lossless across Go, PostgreSQL, and
-- platforms whose stat fields exceed signed 64-bit application types.
CREATE FUNCTION recording_presentation_v2_retention_identity(
  p_task UUID,p_node BIGINT,p_method TEXT,p_device TEXT,p_inode TEXT,p_clone TEXT,
  p_size BIGINT,p_file_sha TEXT,p_deadline TIMESTAMPTZ)
RETURNS TEXT LANGUAGE sql IMMUTABLE STRICT AS $$
  SELECT encode(sha256(convert_to(
    'presentation-retention-v2'||chr(10)||
    octet_length(p_task::text)::text||':'||p_task::text||chr(10)||
    octet_length(p_node::text)::text||':'||p_node::text||chr(10)||
    octet_length(p_method)::text||':'||p_method||chr(10)||
    octet_length(p_device)::text||':'||p_device||chr(10)||
    octet_length(p_inode)::text||':'||p_inode||chr(10)||
    octet_length(p_clone)::text||':'||p_clone||chr(10)||
    octet_length(p_size::text)::text||':'||p_size::text||chr(10)||
    octet_length(p_file_sha)::text||':'||p_file_sha||chr(10)||
    octet_length(((extract(epoch from p_deadline)*1000000)::bigint)::text)::text||':'||
      ((extract(epoch from p_deadline)*1000000)::bigint)::text||chr(10),'UTF8')),'hex')
$$;

CREATE TABLE recording_presentation_v2_admissions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  recording_id BIGINT NOT NULL REFERENCES recordings(id) ON DELETE RESTRICT,
  stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE RESTRICT,
  recording_job_id BIGINT NOT NULL REFERENCES recording_jobs(id) ON DELETE RESTRICT,
  lease_token UUID NOT NULL,
  node_id BIGINT NOT NULL REFERENCES nodes(id) ON DELETE RESTRICT,
  source_revision_id BIGINT REFERENCES stream_source_revisions(id) ON DELETE RESTRICT,
  recording_stream_url_sha256 TEXT NOT NULL CHECK(recording_stream_url_sha256~'^[0-9a-f]{64}$'),
  stream_source_url_sha256 TEXT NOT NULL CHECK(stream_source_url_sha256~'^[0-9a-f]{64}$'),
  stream_source_page_url_sha256 TEXT NOT NULL CHECK(stream_source_page_url_sha256~'^[0-9a-f]{64}$'),
  source_snapshot_sha256 TEXT NOT NULL CHECK(source_snapshot_sha256~'^[0-9a-f]{64}$'),
  provider TEXT NOT NULL CHECK(octet_length(provider) BETWEEN 1 AND 64),
  external_id TEXT NOT NULL CHECK(octet_length(external_id) BETWEEN 1 AND 256),
  source_family TEXT NOT NULL CHECK(octet_length(source_family) BETWEEN 1 AND 64),
  capture_type TEXT NOT NULL CHECK(octet_length(capture_type) BETWEEN 1 AND 64),
  execution_class TEXT NOT NULL CHECK(octet_length(execution_class) BETWEEN 1 AND 64),
  capture_mode TEXT NOT NULL CHECK(capture_mode='source_copy'),
  audio_selection TEXT NOT NULL CHECK(audio_selection='first_optional'),
  policy_version TEXT NOT NULL CHECK(policy_version='continuous-source-presentation-edge-v2'),
  parser_schema TEXT NOT NULL CHECK(parser_schema~'^[A-Za-z0-9._-]{1,64}$'),
  capture_tool_identity_sha256 TEXT NOT NULL CHECK(capture_tool_identity_sha256~'^[0-9a-f]{64}$'),
  admitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  deadline_at TIMESTAMPTZ NOT NULL,
  CHECK(deadline_at>admitted_at AND deadline_at<=admitted_at+interval '15 minutes'),
  UNIQUE(recording_job_id,lease_token,node_id,policy_version)
);

CREATE FUNCTION validate_recording_presentation_v2_admission() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  v_account BIGINT; v_recording BIGINT; v_stream BIGINT; v_lease UUID; v_owner TEXT;
  v_status TEXT; v_expires TIMESTAMPTZ; v_target_fps INTEGER; v_rec_status TEXT;
  v_capture_via TEXT; v_mode TEXT; v_rec_url TEXT; v_node_account BIGINT;
  v_node_type TEXT; v_node_status TEXT; v_source_url TEXT; v_page_url TEXT;
  v_provider TEXT; v_external TEXT; v_family TEXT; v_capture_type TEXT;
  v_execution TEXT; v_revision BIGINT; v_snapshot TEXT;
BEGIN
  IF NEW.admitted_at IS DISTINCT FROM now() THEN
    RAISE EXCEPTION 'presentation admission time must be transaction time';
  END IF;
  IF lock_recording_campaign_protection(NEW.account_id,NEW.recording_id) THEN
    RAISE EXCEPTION 'campaign-protected recording cannot be admitted';
  END IF;
  SELECT r.account_id,r.id,r.stream_id,j.lease_token,j.lease_owner,j.status,
         j.lease_expires_at,r.target_fps,r.status,r.capture_via,r.mode,r.stream_url,
         n.account_id,n.node_type,n.status,s.source_url,s.source_page_url,s.provider,
         s.external_id,s.source_family,s.capture_type,s.execution_class,
         (SELECT max(sr.id) FROM stream_source_revisions sr WHERE sr.stream_id=s.id)
  INTO v_account,v_recording,v_stream,v_lease,v_owner,v_status,v_expires,v_target_fps,
       v_rec_status,v_capture_via,v_mode,v_rec_url,v_node_account,v_node_type,v_node_status,
       v_source_url,v_page_url,v_provider,v_external,v_family,v_capture_type,v_execution,v_revision
  FROM recording_jobs j
  JOIN recordings r ON r.id=j.recording_id
  JOIN nodes n ON n.id=NEW.node_id
  JOIN streams s ON s.id=r.stream_id
  WHERE j.id=NEW.recording_job_id
  FOR UPDATE OF j,r,n,s;
  IF v_account IS DISTINCT FROM NEW.account_id OR v_recording IS DISTINCT FROM NEW.recording_id
     OR v_stream IS DISTINCT FROM NEW.stream_id OR v_lease IS DISTINCT FROM NEW.lease_token
     OR v_owner IS DISTINCT FROM 'node:'||NEW.node_id::text OR v_status IS DISTINCT FROM 'leased'
     OR v_expires<=NEW.admitted_at OR v_target_fps IS NOT NULL
     OR v_rec_status IS DISTINCT FROM 'active' OR v_capture_via IS DISTINCT FROM 'relay'
     OR v_mode IS DISTINCT FROM 'continuous' OR v_node_account IS DISTINCT FROM NEW.account_id
     OR v_node_type IS DISTINCT FROM 'relay' OR v_node_status IS DISTINCT FROM 'active' THEN
    RAISE EXCEPTION 'presentation admission ownership or native-copy mode mismatch';
  END IF;
  v_snapshot:=encode(sha256(convert_to(jsonb_build_array(
    NEW.account_id,NEW.recording_id,NEW.stream_id,NEW.recording_job_id,NEW.lease_token,
    NEW.node_id,v_rec_url,v_source_url,v_page_url,v_provider,v_external,v_family,
    v_capture_type,v_execution,COALESCE(v_revision,0),'source_copy','first_optional')::text,'UTF8')),'hex');
  IF NEW.recording_stream_url_sha256 IS DISTINCT FROM encode(sha256(convert_to(v_rec_url,'UTF8')),'hex')
     OR NEW.stream_source_url_sha256 IS DISTINCT FROM encode(sha256(convert_to(v_source_url,'UTF8')),'hex')
     OR NEW.stream_source_page_url_sha256 IS DISTINCT FROM encode(sha256(convert_to(v_page_url,'UTF8')),'hex')
     OR NEW.source_snapshot_sha256 IS DISTINCT FROM v_snapshot
     OR NEW.provider IS DISTINCT FROM v_provider OR NEW.external_id IS DISTINCT FROM v_external
     OR NEW.source_family IS DISTINCT FROM v_family OR NEW.capture_type IS DISTINCT FROM v_capture_type
     OR NEW.execution_class IS DISTINCT FROM v_execution OR NEW.source_revision_id IS DISTINCT FROM v_revision THEN
    RAISE EXCEPTION 'presentation admission source snapshot mismatch';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_presentation_v2_admission_validate
BEFORE INSERT ON recording_presentation_v2_admissions FOR EACH ROW
EXECUTE FUNCTION validate_recording_presentation_v2_admission();

CREATE TABLE recording_presentation_v2_attempts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  admission_id UUID NOT NULL REFERENCES recording_presentation_v2_admissions(id) ON DELETE RESTRICT,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  recording_id BIGINT NOT NULL REFERENCES recordings(id) ON DELETE RESTRICT,
  stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE RESTRICT,
  recording_job_id BIGINT NOT NULL REFERENCES recording_jobs(id) ON DELETE RESTRICT,
  lease_token UUID NOT NULL,
  node_id BIGINT NOT NULL REFERENCES nodes(id) ON DELETE RESTRICT,
  idempotency_key TEXT NOT NULL CHECK(octet_length(idempotency_key) BETWEEN 1 AND 128),
  ffmpeg_version TEXT NOT NULL CHECK(octet_length(ffmpeg_version) BETWEEN 1 AND 256),
  ffprobe_version TEXT NOT NULL CHECK(octet_length(ffprobe_version) BETWEEN 1 AND 256),
  libavformat_version TEXT NOT NULL CHECK(octet_length(libavformat_version) BETWEEN 1 AND 64),
  libavcodec_version TEXT NOT NULL CHECK(octet_length(libavcodec_version) BETWEEN 1 AND 64),
  libavutil_version TEXT NOT NULL CHECK(octet_length(libavutil_version) BETWEEN 1 AND 64),
  build_flags_sha256 TEXT NOT NULL CHECK(build_flags_sha256~'^[0-9a-f]{64}$'),
  demuxer_name TEXT NOT NULL CHECK(demuxer_name~'^[A-Za-z0-9._-]{1,64}$'),
  video_decoder_name TEXT NOT NULL CHECK(video_decoder_name~'^[A-Za-z0-9._-]{1,64}$'),
  audio_decoder_name TEXT CHECK(audio_decoder_name IS NULL OR audio_decoder_name~'^[A-Za-z0-9._-]{1,64}$'),
  parser_schema TEXT NOT NULL CHECK(parser_schema~'^[A-Za-z0-9._-]{1,64}$'),
  request_sha256 TEXT NOT NULL CHECK(request_sha256~'^[0-9a-f]{64}$'),
  response_sha256 TEXT NOT NULL CHECK(response_sha256~'^[0-9a-f]{64}$'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(admission_id,idempotency_key)
);

CREATE FUNCTION validate_recording_presentation_v2_attempt() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE a recording_presentation_v2_admissions%ROWTYPE; j recording_jobs%ROWTYPE;
BEGIN
  SELECT * INTO a FROM recording_presentation_v2_admissions WHERE id=NEW.admission_id FOR SHARE;
  IF NOT FOUND OR (NEW.account_id,NEW.recording_id,NEW.stream_id,NEW.recording_job_id,NEW.lease_token,NEW.node_id,NEW.parser_schema)
    IS DISTINCT FROM (a.account_id,a.recording_id,a.stream_id,a.recording_job_id,a.lease_token,a.node_id,a.parser_schema) THEN
    RAISE EXCEPTION 'presentation attempt admission identity mismatch';
  END IF;
  IF NEW.created_at IS DISTINCT FROM now() OR NEW.created_at>=a.deadline_at THEN
    RAISE EXCEPTION 'presentation attempt time outside admission';
  END IF;
  IF NEW.response_sha256 IS DISTINCT FROM encode(sha256(convert_to('attempt:'||NEW.id::text,'UTF8')),'hex') THEN
    RAISE EXCEPTION 'presentation attempt response binding mismatch';
  END IF;
  IF NEW.build_flags_sha256 IS DISTINCT FROM lower(NEW.build_flags_sha256)
     OR a.capture_tool_identity_sha256 IS DISTINCT FROM recording_presentation_v2_tool_identity(
       NEW.ffmpeg_version,NEW.ffprobe_version,NEW.libavformat_version,NEW.libavcodec_version,
       NEW.libavutil_version,NEW.build_flags_sha256,NEW.demuxer_name,NEW.video_decoder_name,
       COALESCE(NEW.audio_decoder_name,''),NEW.parser_schema) THEN
    RAISE EXCEPTION 'presentation attempt semantic tool identity mismatch';
  END IF;
  IF lock_recording_campaign_protection(NEW.account_id,NEW.recording_id) THEN
    RAISE EXCEPTION 'campaign-protected recording cannot start presentation attempt';
  END IF;
  SELECT * INTO j FROM recording_jobs WHERE id=NEW.recording_job_id FOR UPDATE;
  IF j.status<>'leased' OR j.lease_token IS DISTINCT FROM NEW.lease_token
     OR j.lease_owner IS DISTINCT FROM 'node:'||NEW.node_id::text OR j.lease_expires_at<=now() THEN
    RAISE EXCEPTION 'presentation attempt lease mismatch';
  END IF;
  IF NOT EXISTS(
    SELECT 1 FROM recordings r JOIN streams s ON s.id=r.stream_id JOIN nodes n ON n.id=a.node_id
    WHERE r.id=a.recording_id AND r.account_id=a.account_id AND r.status='active'
      AND r.capture_via='relay' AND r.mode='continuous' AND r.target_fps IS NULL
      AND r.stream_id=a.stream_id AND n.account_id=a.account_id AND n.node_type='relay' AND n.status='active'
      AND encode(sha256(convert_to(r.stream_url,'UTF8')),'hex')=a.recording_stream_url_sha256
      AND encode(sha256(convert_to(s.source_url,'UTF8')),'hex')=a.stream_source_url_sha256
      AND encode(sha256(convert_to(s.source_page_url,'UTF8')),'hex')=a.stream_source_page_url_sha256
      AND (s.provider,s.external_id,s.source_family,s.capture_type,s.execution_class)
          IS NOT DISTINCT FROM (a.provider,a.external_id,a.source_family,a.capture_type,a.execution_class)
      AND (SELECT max(sr.id) FROM stream_source_revisions sr WHERE sr.stream_id=s.id) IS NOT DISTINCT FROM a.source_revision_id
  ) THEN RAISE EXCEPTION 'presentation attempt source snapshot diverged'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_presentation_v2_attempt_validate
BEFORE INSERT ON recording_presentation_v2_attempts FOR EACH ROW
EXECUTE FUNCTION validate_recording_presentation_v2_attempt();

CREATE TABLE recording_presentation_v2_probe_tasks (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  admission_id UUID NOT NULL REFERENCES recording_presentation_v2_admissions(id) ON DELETE RESTRICT,
  attempt_id UUID NOT NULL REFERENCES recording_presentation_v2_attempts(id) ON DELETE RESTRICT,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  recording_id BIGINT NOT NULL REFERENCES recordings(id) ON DELETE RESTRICT,
  stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE RESTRICT,
  recording_job_id BIGINT NOT NULL REFERENCES recording_jobs(id) ON DELETE RESTRICT,
  clip_id BIGINT NOT NULL REFERENCES recording_clips(id) ON DELETE RESTRICT,
  upload_intent_id UUID NOT NULL REFERENCES recording_upload_intents(id) ON DELETE RESTRICT,
  lease_token UUID NOT NULL,
  node_id BIGINT NOT NULL REFERENCES nodes(id) ON DELETE RESTRICT,
  capture_sequence BIGINT NOT NULL CHECK(capture_sequence>0),
  clip_size_bytes BIGINT NOT NULL CHECK(clip_size_bytes BETWEEN 1 AND 536870912),
  clip_sha256 TEXT NOT NULL CHECK(clip_sha256~'^[0-9a-f]{64}$'),
  local_upload_identity_sha256 TEXT NOT NULL CHECK(local_upload_identity_sha256~'^[0-9a-f]{64}$'),
  staging_identity_sha256 TEXT CHECK(staging_identity_sha256 IS NULL OR staging_identity_sha256~'^[0-9a-f]{64}$'),
  staging_method TEXT CHECK(staging_method IS NULL OR staging_method IN('hardlink','clone')),
  staging_device_id TEXT CHECK(staging_device_id IS NULL OR CASE WHEN staging_device_id~'^(0|[1-9][0-9]{0,19})$' THEN staging_device_id::numeric<=18446744073709551615 ELSE false END),
  staging_inode_id TEXT CHECK(staging_inode_id IS NULL OR CASE WHEN staging_inode_id~'^[1-9][0-9]{0,19}$' THEN staging_inode_id::numeric<=18446744073709551615 ELSE false END),
  staging_clone_identity_sha256 TEXT CHECK(staging_clone_identity_sha256 IS NULL OR staging_clone_identity_sha256~'^[0-9a-f]{64}$'),
  request_sha256 TEXT NOT NULL CHECK(request_sha256~'^[0-9a-f]{64}$'),
  response_sha256 TEXT NOT NULL CHECK(response_sha256~'^[0-9a-f]{64}$'),
  initial_disposition TEXT NOT NULL CHECK(initial_disposition IN('retained','unavailable')),
  state TEXT NOT NULL CHECK(state IN('awaiting_retention','pending','leased','completed','expired','unavailable')),
  retention_state TEXT NOT NULL CHECK(retention_state IN('none','awaiting','active','release_pending','released')),
  unavailable_reason TEXT,
  retention_identity_sha256 TEXT CHECK(retention_identity_sha256 IS NULL OR retention_identity_sha256~'^[0-9a-f]{64}$'),
  retention_method TEXT CHECK(retention_method IS NULL OR retention_method IN('hardlink','clone')),
  retention_device_id TEXT CHECK(retention_device_id IS NULL OR CASE WHEN retention_device_id~'^(0|[1-9][0-9]{0,19})$' THEN retention_device_id::numeric<=18446744073709551615 ELSE false END),
  retention_inode_id TEXT CHECK(retention_inode_id IS NULL OR CASE WHEN retention_inode_id~'^[1-9][0-9]{0,19}$' THEN retention_inode_id::numeric<=18446744073709551615 ELSE false END),
  retention_clone_identity_sha256 TEXT CHECK(retention_clone_identity_sha256 IS NULL OR retention_clone_identity_sha256~'^[0-9a-f]{64}$'),
  revision BIGINT NOT NULL DEFAULT 1 CHECK(revision>0),
  attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count BETWEEN 0 AND 5),
  claim_token UUID,
  terminal_claim_token UUID,
  lease_expires_at TIMESTAMPTZ,
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  absolute_deadline_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(attempt_id,capture_sequence),
  UNIQUE(upload_intent_id),
  UNIQUE(clip_id),
  CHECK(absolute_deadline_at>created_at AND absolute_deadline_at<=created_at+interval '10 minutes'),
  CHECK(
    (initial_disposition='unavailable' AND state='unavailable' AND retention_state='none'
      AND staging_identity_sha256 IS NULL AND retention_identity_sha256 IS NULL
      AND staging_method IS NULL AND retention_method IS NULL
      AND staging_device_id IS NULL AND staging_inode_id IS NULL AND staging_clone_identity_sha256 IS NULL
      AND retention_device_id IS NULL AND retention_inode_id IS NULL AND retention_clone_identity_sha256 IS NULL
      AND unavailable_reason IS NOT NULL)
    OR initial_disposition='retained'),
  CHECK(unavailable_reason IS NULL OR unavailable_reason IN(
    'retention_unavailable','state_reserve','link_unavailable','retention_deadline',
    'probe_timeout','probe_resource_limit','probe_unavailable','retention_lost')),
  CHECK((state='leased')=(claim_token IS NOT NULL AND lease_expires_at IS NOT NULL)),
  CHECK(state='leased' OR (claim_token IS NULL AND lease_expires_at IS NULL)),
  CHECK(
    (retention_state='none' AND staging_identity_sha256 IS NULL AND staging_method IS NULL
      AND staging_device_id IS NULL AND staging_inode_id IS NULL AND staging_clone_identity_sha256 IS NULL
      AND retention_identity_sha256 IS NULL AND retention_method IS NULL
      AND retention_device_id IS NULL AND retention_inode_id IS NULL AND retention_clone_identity_sha256 IS NULL)
    OR (retention_state='awaiting' AND staging_identity_sha256 IS NOT NULL AND staging_method IS NOT NULL
      AND ((staging_method='hardlink' AND staging_device_id IS NOT NULL AND staging_inode_id IS NOT NULL AND staging_clone_identity_sha256 IS NULL)
        OR (staging_method='clone' AND staging_device_id IS NULL AND staging_inode_id IS NULL AND staging_clone_identity_sha256 IS NOT NULL))
      AND retention_identity_sha256 IS NULL AND retention_method IS NULL
      AND retention_device_id IS NULL AND retention_inode_id IS NULL AND retention_clone_identity_sha256 IS NULL)
    OR (retention_state='active' AND staging_identity_sha256 IS NOT NULL AND staging_method IS NOT NULL
      AND retention_identity_sha256 IS NOT NULL AND retention_method=staging_method
      AND ((retention_method='hardlink' AND retention_device_id=staging_device_id AND retention_inode_id=staging_inode_id
          AND retention_clone_identity_sha256 IS NULL AND staging_clone_identity_sha256 IS NULL)
        OR (retention_method='clone' AND retention_device_id IS NULL AND retention_inode_id IS NULL
          AND retention_clone_identity_sha256=staging_clone_identity_sha256 AND retention_clone_identity_sha256 IS NOT NULL)))
    OR (retention_state IN('release_pending','released') AND staging_identity_sha256 IS NOT NULL
      AND staging_method IS NOT NULL
      AND ((retention_identity_sha256 IS NULL AND retention_method IS NULL
          AND retention_device_id IS NULL AND retention_inode_id IS NULL AND retention_clone_identity_sha256 IS NULL)
        OR (retention_identity_sha256 IS NOT NULL AND retention_method=staging_method
          AND ((retention_method='hardlink' AND retention_device_id=staging_device_id AND retention_inode_id=staging_inode_id
              AND retention_clone_identity_sha256 IS NULL AND staging_clone_identity_sha256 IS NULL)
            OR (retention_method='clone' AND retention_device_id IS NULL AND retention_inode_id IS NULL
              AND retention_clone_identity_sha256=staging_clone_identity_sha256 AND retention_clone_identity_sha256 IS NOT NULL)))))),
  CHECK(
    (state='awaiting_retention' AND retention_state='awaiting')
    OR (state IN('pending','leased') AND retention_state='active')
    OR (state IN('completed','expired') AND retention_state IN('release_pending','released'))
    OR (state='unavailable' AND retention_state IN('none','release_pending','released'))),
  CHECK(retention_identity_sha256 IS NULL OR (retention_method IS NOT NULL AND retention_identity_sha256=
    recording_presentation_v2_retention_identity(id,node_id,retention_method,
      COALESCE(retention_device_id,''),COALESCE(retention_inode_id,''),
      COALESCE(retention_clone_identity_sha256,''),clip_size_bytes,clip_sha256,absolute_deadline_at)))
);
CREATE INDEX recording_presentation_v2_probe_claim_idx
ON recording_presentation_v2_probe_tasks(account_id,node_id,next_attempt_at,id)
WHERE state IN('pending','leased') AND retention_state='active';

CREATE TABLE recording_presentation_v2_authored_facts (
  id BIGSERIAL PRIMARY KEY,
  task_id UUID NOT NULL REFERENCES recording_presentation_v2_probe_tasks(id) ON DELETE RESTRICT,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  node_id BIGINT NOT NULL REFERENCES nodes(id) ON DELETE RESTRICT,
  claim_token UUID NOT NULL,
  request_sha256 TEXT NOT NULL CHECK(request_sha256~'^[0-9a-f]{64}$'),
  report_sha256 TEXT NOT NULL CHECK(report_sha256~'^[0-9a-f]{64}$'),
  authored_status TEXT NOT NULL CHECK(authored_status IN('complete','partial','unknown')),
  terminal_reason TEXT NOT NULL CHECK(terminal_reason IN('all_axes_complete','some_axes_unknown','all_axes_unknown')),
  completed_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(task_id), UNIQUE(task_id,claim_token)
);

CREATE TABLE recording_presentation_v2_fact_axes (
  fact_id BIGINT NOT NULL REFERENCES recording_presentation_v2_authored_facts(id) ON DELETE RESTRICT,
  axis TEXT NOT NULL CHECK(axis IN('demux_video','raw_video','video_presentation','demux_audio','raw_audio','audio_sample')),
  status TEXT NOT NULL CHECK(status IN('complete','unknown','not_present')),
  reason TEXT CHECK(reason IS NULL OR reason IN('probe_timeout','probe_resource_limit','tool_incompatible','probe_unavailable','presentation_ambiguous','raw_extent_unavailable','audio_not_present')),
  stream_index INTEGER,
  unit_count BIGINT,
  canonical_sha256 TEXT CHECK(canonical_sha256 IS NULL OR canonical_sha256~'^[0-9a-f]{64}$'),
  time_base_num BIGINT,
  time_base_den BIGINT,
  first_ordinal BIGINT,
  first_timestamp BIGINT,
  end_ordinal BIGINT,
  end_timestamp BIGINT,
  nonmonotonic_count BIGINT,
  duplicate_count BIGINT,
  hole_count BIGINT,
  overlap_count BIGINT,
  sample_rate INTEGER,
  channel_count INTEGER,
  channel_layout TEXT,
  normalization_profile TEXT,
  edit_list_sha256 TEXT CHECK(edit_list_sha256 IS NULL OR edit_list_sha256~'^[0-9a-f]{64}$'),
  edit_list_kind TEXT CHECK(edit_list_kind IS NULL OR edit_list_kind IN('none','identity')),
  skip_samples BIGINT,
  discard_padding BIGINT,
  codec_delay BIGINT,
  initial_padding BIGINT,
  trailing_padding BIGINT,
  PRIMARY KEY(fact_id,axis),
  CHECK(
    (status='complete' AND reason IS NULL AND stream_index>=0 AND unit_count BETWEEN 1 AND 100000
      AND canonical_sha256 IS NOT NULL AND first_ordinal>=0 AND end_ordinal>=first_ordinal
      AND end_ordinal-first_ordinal+1=unit_count
      AND nonmonotonic_count=0 AND duplicate_count=0 AND hole_count=0 AND overlap_count=0
      AND ((axis IN('raw_video','raw_audio') AND time_base_num IS NULL AND time_base_den IS NULL
        AND first_timestamp IS NULL AND end_timestamp IS NULL)
       OR (axis NOT IN('raw_video','raw_audio') AND time_base_num BETWEEN 1 AND 1000000000
        AND time_base_den BETWEEN 1 AND 1000000000 AND first_timestamp IS NOT NULL
        AND end_timestamp>first_timestamp)))
    OR (status='unknown' AND reason IS NOT NULL AND stream_index IS NULL AND unit_count IS NULL
      AND canonical_sha256 IS NULL AND time_base_num IS NULL AND time_base_den IS NULL
      AND first_ordinal IS NULL AND first_timestamp IS NULL AND end_ordinal IS NULL AND end_timestamp IS NULL
      AND nonmonotonic_count IS NULL AND duplicate_count IS NULL AND hole_count IS NULL AND overlap_count IS NULL
      AND sample_rate IS NULL AND channel_count IS NULL AND channel_layout IS NULL)
    OR (status='not_present' AND axis IN('demux_audio','raw_audio','audio_sample') AND reason='audio_not_present'
      AND stream_index IS NULL AND unit_count IS NULL AND canonical_sha256 IS NULL
      AND time_base_num IS NULL AND time_base_den IS NULL AND first_ordinal IS NULL
      AND first_timestamp IS NULL AND end_ordinal IS NULL AND end_timestamp IS NULL
      AND nonmonotonic_count IS NULL AND duplicate_count IS NULL AND hole_count IS NULL
      AND overlap_count IS NULL AND sample_rate IS NULL AND channel_count IS NULL AND channel_layout IS NULL)),
  CHECK(
    (status='complete' AND axis='video_presentation'
      AND normalization_profile='continuous-rational-presentation-v2.0'
      AND edit_list_sha256 IS NOT NULL AND edit_list_kind IN('none','identity')
      AND skip_samples IS NULL AND discard_padding IS NULL AND codec_delay IS NULL
      AND initial_padding IS NULL AND trailing_padding IS NULL)
    OR (status='complete' AND axis='audio_sample'
      AND normalization_profile='decoder-output-effective-samples-v2.0'
      AND edit_list_sha256 IS NOT NULL AND edit_list_kind IN('none','identity')
      AND skip_samples>=0 AND discard_padding>=0 AND codec_delay>=0
      AND initial_padding>=0 AND trailing_padding>=0)
    OR (status='complete' AND axis NOT IN('video_presentation','audio_sample')
      AND normalization_profile IS NULL AND edit_list_sha256 IS NULL AND edit_list_kind IS NULL
      AND skip_samples IS NULL AND discard_padding IS NULL AND codec_delay IS NULL
      AND initial_padding IS NULL AND trailing_padding IS NULL)
    OR (status<>'complete' AND normalization_profile IS NULL AND edit_list_sha256 IS NULL
      AND edit_list_kind IS NULL AND skip_samples IS NULL AND discard_padding IS NULL
      AND codec_delay IS NULL AND initial_padding IS NULL AND trailing_padding IS NULL)),
  CHECK(axis='audio_sample' OR (sample_rate IS NULL AND channel_count IS NULL AND channel_layout IS NULL)),
  CHECK(axis<>'audio_sample' OR status<>'complete' OR
    (sample_rate BETWEEN 8000 AND 768000 AND channel_count BETWEEN 1 AND 32
      AND octet_length(channel_layout) BETWEEN 1 AND 128
      AND time_base_num=1 AND time_base_den=sample_rate))
);

CREATE TABLE recording_presentation_v2_packet_edges (
  fact_id BIGINT NOT NULL,
  axis TEXT NOT NULL CHECK(axis IN('demux_video','demux_audio')),
  edge_side TEXT NOT NULL CHECK(edge_side IN('leading','trailing')),
  edge_rank INTEGER NOT NULL CHECK(edge_rank BETWEEN 1 AND 4),
  demux_ordinal BIGINT NOT NULL CHECK(demux_ordinal>=0),
  pts BIGINT NOT NULL, dts BIGINT NOT NULL, duration BIGINT NOT NULL CHECK(duration>0),
  time_base_num BIGINT NOT NULL CHECK(time_base_num BETWEEN 1 AND 1000000000),
  time_base_den BIGINT NOT NULL CHECK(time_base_den BETWEEN 1 AND 1000000000),
  flags INTEGER NOT NULL,
  side_data_sha256 TEXT NOT NULL CHECK(side_data_sha256~'^[0-9a-f]{64}$'),
  payload_sha256 TEXT NOT NULL CHECK(payload_sha256~'^[0-9a-f]{64}$'),
  PRIMARY KEY(fact_id,axis,edge_side,edge_rank),
  UNIQUE(fact_id,axis,edge_side,demux_ordinal),
  FOREIGN KEY(fact_id,axis) REFERENCES recording_presentation_v2_fact_axes(fact_id,axis) ON DELETE RESTRICT
);

CREATE TABLE recording_presentation_v2_raw_extents (
  fact_id BIGINT NOT NULL,
  axis TEXT NOT NULL CHECK(axis IN('raw_video','raw_audio')),
  edge_side TEXT NOT NULL CHECK(edge_side IN('leading','trailing')),
  edge_rank INTEGER NOT NULL CHECK(edge_rank BETWEEN 1 AND 4),
  demux_ordinal BIGINT NOT NULL CHECK(demux_ordinal>=0),
  byte_position BIGINT NOT NULL CHECK(byte_position>=0),
  byte_size BIGINT NOT NULL CHECK(byte_size>0 AND byte_size<=536870912),
  raw_extent_sha256 TEXT NOT NULL CHECK(raw_extent_sha256~'^[0-9a-f]{64}$'),
  PRIMARY KEY(fact_id,axis,edge_side,edge_rank),
  UNIQUE(fact_id,axis,edge_side,demux_ordinal),
  FOREIGN KEY(fact_id,axis) REFERENCES recording_presentation_v2_fact_axes(fact_id,axis) ON DELETE RESTRICT
);

CREATE TABLE recording_presentation_v2_video_frame_edges (
  fact_id BIGINT NOT NULL,
  axis TEXT NOT NULL DEFAULT 'video_presentation' CHECK(axis='video_presentation'),
  edge_side TEXT NOT NULL CHECK(edge_side IN('leading','trailing')),
  edge_rank INTEGER NOT NULL CHECK(edge_rank BETWEEN 1 AND 4),
  presentation_ordinal BIGINT NOT NULL CHECK(presentation_ordinal>=0),
  pts BIGINT NOT NULL, duration BIGINT NOT NULL CHECK(duration>0),
  time_base_num BIGINT NOT NULL CHECK(time_base_num BETWEEN 1 AND 1000000000),
  time_base_den BIGINT NOT NULL CHECK(time_base_den BETWEEN 1 AND 1000000000),
  pixel_sha256 TEXT CHECK(pixel_sha256 IS NULL OR pixel_sha256~'^[0-9a-f]{64}$'),
  PRIMARY KEY(fact_id,edge_side,edge_rank),
  UNIQUE(fact_id,edge_side,presentation_ordinal),
  FOREIGN KEY(fact_id,axis) REFERENCES recording_presentation_v2_fact_axes(fact_id,axis) ON DELETE RESTRICT
);

CREATE TABLE recording_presentation_v2_audio_block_edges (
  fact_id BIGINT NOT NULL,
  axis TEXT NOT NULL DEFAULT 'audio_sample' CHECK(axis='audio_sample'),
  edge_side TEXT NOT NULL CHECK(edge_side IN('leading','trailing')),
  edge_rank INTEGER NOT NULL CHECK(edge_rank BETWEEN 1 AND 4),
  block_ordinal BIGINT NOT NULL CHECK(block_ordinal>=0),
  pts BIGINT NOT NULL, sample_count INTEGER NOT NULL CHECK(sample_count BETWEEN 1 AND 1048576),
  time_base_num BIGINT NOT NULL CHECK(time_base_num BETWEEN 1 AND 1000000000),
  time_base_den BIGINT NOT NULL CHECK(time_base_den BETWEEN 1 AND 1000000000),
  sample_rate INTEGER NOT NULL CHECK(sample_rate BETWEEN 8000 AND 768000),
  channel_count INTEGER NOT NULL CHECK(channel_count BETWEEN 1 AND 32),
  channel_layout TEXT NOT NULL CHECK(octet_length(channel_layout) BETWEEN 1 AND 128),
  pcm_sha256 TEXT CHECK(pcm_sha256 IS NULL OR pcm_sha256~'^[0-9a-f]{64}$'),
  PRIMARY KEY(fact_id,edge_side,edge_rank),
  UNIQUE(fact_id,edge_side,block_ordinal),
  FOREIGN KEY(fact_id,axis) REFERENCES recording_presentation_v2_fact_axes(fact_id,axis) ON DELETE RESTRICT
);

CREATE TABLE recording_presentation_v2_release_authorizations (
  task_id UUID PRIMARY KEY REFERENCES recording_presentation_v2_probe_tasks(id) ON DELETE RESTRICT,
  release_version BIGINT NOT NULL CHECK(release_version=1),
  node_id BIGINT NOT NULL REFERENCES nodes(id) ON DELETE RESTRICT,
  binding_sha256 TEXT NOT NULL CHECK(binding_sha256~'^[0-9a-f]{64}$'),
  terminal_state TEXT NOT NULL CHECK(terminal_state IN('completed','expired','unavailable')),
  authorized_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE recording_presentation_v2_release_acknowledgements (
  task_id UUID PRIMARY KEY REFERENCES recording_presentation_v2_release_authorizations(task_id) ON DELETE RESTRICT,
  release_version BIGINT NOT NULL CHECK(release_version=1),
  node_id BIGINT NOT NULL REFERENCES nodes(id) ON DELETE RESTRICT,
  binding_sha256 TEXT NOT NULL CHECK(binding_sha256~'^[0-9a-f]{64}$'),
  acknowledged_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE recording_presentation_v2_task_events (
  id BIGSERIAL PRIMARY KEY,
  task_id UUID NOT NULL REFERENCES recording_presentation_v2_probe_tasks(id) ON DELETE RESTRICT,
  revision BIGINT NOT NULL,
  from_state TEXT,
  to_state TEXT NOT NULL,
  from_retention_state TEXT,
  to_retention_state TEXT NOT NULL,
  reason TEXT NOT NULL CHECK(octet_length(reason) BETWEEN 1 AND 128),
  claim_token UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(task_id,revision)
);
CREATE TABLE recording_presentation_v2_tool_compatibility (
  id BIGSERIAL PRIMARY KEY,
  capture_identity_sha256 TEXT NOT NULL CHECK(capture_identity_sha256~'^[0-9a-f]{64}$'),
  nas_identity_sha256 TEXT NOT NULL CHECK(nas_identity_sha256~'^[0-9a-f]{64}$'),
  parser_schema TEXT NOT NULL CHECK(parser_schema~'^[A-Za-z0-9._-]{1,64}$'),
  axis TEXT NOT NULL CHECK(axis IN('demux_video','raw_video','video_presentation','demux_audio','raw_audio','audio_sample')),
  enabled BOOLEAN NOT NULL DEFAULT false CHECK(enabled=false),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(capture_identity_sha256,nas_identity_sha256,parser_schema,axis)
);

CREATE FUNCTION reject_recording_presentation_v2_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'presentation evidence is append-only'; END $$;

CREATE TRIGGER recording_presentation_v2_admissions_append_only BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_presentation_v2_admissions FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_presentation_v2_mutation();
CREATE TRIGGER recording_presentation_v2_attempts_append_only BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_presentation_v2_attempts FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_presentation_v2_mutation();
CREATE TRIGGER recording_presentation_v2_facts_append_only BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_presentation_v2_authored_facts FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_presentation_v2_mutation();
CREATE TRIGGER recording_presentation_v2_axes_append_only BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_presentation_v2_fact_axes FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_presentation_v2_mutation();
CREATE TRIGGER recording_presentation_v2_packet_edges_append_only BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_presentation_v2_packet_edges FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_presentation_v2_mutation();
CREATE TRIGGER recording_presentation_v2_raw_extents_append_only BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_presentation_v2_raw_extents FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_presentation_v2_mutation();
CREATE TRIGGER recording_presentation_v2_video_edges_append_only BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_presentation_v2_video_frame_edges FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_presentation_v2_mutation();
CREATE TRIGGER recording_presentation_v2_audio_edges_append_only BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_presentation_v2_audio_block_edges FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_presentation_v2_mutation();
CREATE TRIGGER recording_presentation_v2_release_auth_append_only BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_presentation_v2_release_authorizations FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_presentation_v2_mutation();
CREATE TRIGGER recording_presentation_v2_release_ack_append_only BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_presentation_v2_release_acknowledgements FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_presentation_v2_mutation();
CREATE TRIGGER recording_presentation_v2_events_append_only BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_presentation_v2_task_events FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_presentation_v2_mutation();
CREATE TRIGGER recording_presentation_v2_compat_append_only BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_presentation_v2_tool_compatibility FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_presentation_v2_mutation();

CREATE FUNCTION guard_recording_presentation_v2_task() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE a recording_presentation_v2_admissions%ROWTYPE; p recording_presentation_v2_attempts%ROWTYPE;
  c RECORD; i RECORD; j recording_jobs%ROWTYPE;
BEGIN
  IF TG_OP='INSERT' THEN
    IF NEW.revision<>1 OR NEW.attempt_count<>0 OR NEW.claim_token IS NOT NULL OR NEW.terminal_claim_token IS NOT NULL THEN
      RAISE EXCEPTION 'invalid initial presentation task lifecycle';
    END IF;
    IF NEW.response_sha256 IS DISTINCT FROM encode(sha256(convert_to(
      'task:'||NEW.id::text||':'||NEW.clip_id::text||':'||NEW.state,'UTF8')),'hex') THEN
      RAISE EXCEPTION 'presentation task response binding mismatch';
    END IF;
    IF NEW.initial_disposition='retained' AND
       (NEW.state<>'awaiting_retention' OR NEW.retention_state<>'awaiting' OR NEW.staging_identity_sha256 IS NULL
        OR NEW.staging_method IS NULL OR NEW.unavailable_reason IS NOT NULL
        OR (NEW.staging_method='hardlink' AND (NEW.staging_device_id IS NULL OR NEW.staging_inode_id IS NULL OR NEW.staging_clone_identity_sha256 IS NOT NULL))
        OR (NEW.staging_method='clone' AND (NEW.staging_device_id IS NOT NULL OR NEW.staging_inode_id IS NOT NULL OR NEW.staging_clone_identity_sha256 IS NULL))) THEN
      RAISE EXCEPTION 'retained presentation task must await retention';
    END IF;
    SELECT * INTO a FROM recording_presentation_v2_admissions WHERE id=NEW.admission_id FOR SHARE;
    SELECT * INTO p FROM recording_presentation_v2_attempts WHERE id=NEW.attempt_id FOR SHARE;
    SELECT recording_id,recording_job_id,storage_destination_id,endpoint,bucket,object_key,
           size_bytes,lower(sha256) AS sha256,capture_lease_token,capture_sequence
      INTO c FROM recording_clips WHERE id=NEW.clip_id FOR SHARE;
    SELECT recording_id,recording_job_id,storage_destination_id,endpoint,bucket,object_key,max_size_bytes,status
      INTO i FROM recording_upload_intents WHERE id=NEW.upload_intent_id FOR SHARE;
    SELECT * INTO j FROM recording_jobs WHERE id=NEW.recording_job_id FOR UPDATE;
    IF NOT FOUND OR p.admission_id IS DISTINCT FROM a.id
       OR (NEW.account_id,NEW.recording_id,NEW.stream_id,NEW.recording_job_id,NEW.lease_token,NEW.node_id)
          IS DISTINCT FROM (a.account_id,a.recording_id,a.stream_id,a.recording_job_id,a.lease_token,a.node_id)
       OR (p.account_id,p.recording_id,p.stream_id,p.recording_job_id,p.lease_token,p.node_id)
          IS DISTINCT FROM (a.account_id,a.recording_id,a.stream_id,a.recording_job_id,a.lease_token,a.node_id)
       OR (c.recording_id,c.recording_job_id,c.size_bytes,c.sha256,c.capture_lease_token,c.capture_sequence)
          IS DISTINCT FROM (NEW.recording_id,NEW.recording_job_id,NEW.clip_size_bytes,NEW.clip_sha256,NEW.lease_token,NEW.capture_sequence)
       OR (i.recording_id,i.recording_job_id,i.status) IS DISTINCT FROM (NEW.recording_id,NEW.recording_job_id,'consumed'::text)
       OR (c.storage_destination_id,c.endpoint,c.bucket,c.object_key)
          IS DISTINCT FROM (i.storage_destination_id,i.endpoint,i.bucket,i.object_key)
       OR c.size_bytes>i.max_size_bytes
       OR j.status<>'leased' OR j.lease_token IS DISTINCT FROM NEW.lease_token
       OR j.lease_owner IS DISTINCT FROM 'node:'||NEW.node_id::text OR j.lease_expires_at<=now()
       OR NEW.created_at>=a.deadline_at OR NEW.absolute_deadline_at>a.deadline_at THEN
      RAISE EXCEPTION 'presentation task ownership, clip, intent, or lease mismatch';
    END IF;
    IF lock_recording_campaign_protection(NEW.account_id,NEW.recording_id) THEN
      RAISE EXCEPTION 'campaign-protected recording cannot create presentation task';
    END IF;
    IF NOT EXISTS(
      SELECT 1 FROM recordings r JOIN streams s ON s.id=r.stream_id JOIN nodes n ON n.id=a.node_id
      WHERE r.id=a.recording_id AND r.account_id=a.account_id AND r.status='active'
        AND r.capture_via='relay' AND r.mode='continuous' AND r.target_fps IS NULL
        AND r.stream_id=a.stream_id AND n.account_id=a.account_id AND n.node_type='relay' AND n.status='active'
        AND encode(sha256(convert_to(r.stream_url,'UTF8')),'hex')=a.recording_stream_url_sha256
        AND encode(sha256(convert_to(s.source_url,'UTF8')),'hex')=a.stream_source_url_sha256
        AND encode(sha256(convert_to(s.source_page_url,'UTF8')),'hex')=a.stream_source_page_url_sha256
        AND (s.provider,s.external_id,s.source_family,s.capture_type,s.execution_class)
            IS NOT DISTINCT FROM (a.provider,a.external_id,a.source_family,a.capture_type,a.execution_class)
        AND (SELECT max(sr.id) FROM stream_source_revisions sr WHERE sr.stream_id=s.id) IS NOT DISTINCT FROM a.source_revision_id
    ) THEN RAISE EXCEPTION 'presentation task source snapshot diverged'; END IF;
  ELSE
    IF (NEW.admission_id,NEW.attempt_id,NEW.account_id,NEW.recording_id,NEW.stream_id,NEW.recording_job_id,
        NEW.clip_id,NEW.upload_intent_id,NEW.lease_token,NEW.node_id,NEW.capture_sequence,NEW.clip_size_bytes,
        NEW.clip_sha256,NEW.local_upload_identity_sha256,NEW.staging_identity_sha256,NEW.staging_method,
        NEW.staging_device_id,NEW.staging_inode_id,NEW.staging_clone_identity_sha256,NEW.request_sha256,
        NEW.response_sha256,NEW.initial_disposition,NEW.created_at,NEW.absolute_deadline_at)
      IS DISTINCT FROM
       (OLD.admission_id,OLD.attempt_id,OLD.account_id,OLD.recording_id,OLD.stream_id,OLD.recording_job_id,
        OLD.clip_id,OLD.upload_intent_id,OLD.lease_token,OLD.node_id,OLD.capture_sequence,OLD.clip_size_bytes,
        OLD.clip_sha256,OLD.local_upload_identity_sha256,OLD.staging_identity_sha256,OLD.staging_method,
        OLD.staging_device_id,OLD.staging_inode_id,OLD.staging_clone_identity_sha256,OLD.request_sha256,
        OLD.response_sha256,OLD.initial_disposition,OLD.created_at,OLD.absolute_deadline_at) THEN
      RAISE EXCEPTION 'presentation task identity is immutable';
    END IF;
    IF NEW.revision<>OLD.revision+1 OR NEW.attempt_count<OLD.attempt_count OR NEW.attempt_count>OLD.attempt_count+1 THEN
      RAISE EXCEPTION 'invalid presentation task revision or attempt counter';
    END IF;
    IF OLD.state IN('completed','expired','unavailable') AND OLD.retention_state IN('none','released') THEN
      RAISE EXCEPTION 'terminal presentation task is immutable';
    END IF;
    IF OLD.state='awaiting_retention' AND NEW.state='pending' THEN
      IF OLD.retention_state<>'awaiting' OR NEW.retention_state<>'active'
         OR NEW.retention_method IS DISTINCT FROM OLD.staging_method
         OR (NEW.retention_method='hardlink' AND
           (NEW.retention_device_id,NEW.retention_inode_id,NEW.retention_clone_identity_sha256)
             IS DISTINCT FROM (OLD.staging_device_id,OLD.staging_inode_id,NULL::text))
         OR (NEW.retention_method='clone' AND
           (NEW.retention_device_id,NEW.retention_inode_id,NEW.retention_clone_identity_sha256)
             IS DISTINCT FROM (NULL::text,NULL::text,OLD.staging_clone_identity_sha256))
         OR NEW.retention_identity_sha256 IS DISTINCT FROM recording_presentation_v2_retention_identity(
           NEW.id,NEW.node_id,NEW.retention_method,COALESCE(NEW.retention_device_id,''),
           COALESCE(NEW.retention_inode_id,''),COALESCE(NEW.retention_clone_identity_sha256,''),
           NEW.clip_size_bytes,NEW.clip_sha256,NEW.absolute_deadline_at)
      THEN RAISE EXCEPTION 'invalid retention activation'; END IF;
    ELSIF OLD.state='pending' AND NEW.state='leased' THEN
      IF OLD.retention_state<>'active' OR NEW.retention_state<>'active' OR NEW.attempt_count<>OLD.attempt_count+1 THEN RAISE EXCEPTION 'invalid presentation claim'; END IF;
    ELSIF OLD.state='leased' AND NEW.state='pending' THEN
      IF NEW.retention_state<>'active' OR NEW.attempt_count<>OLD.attempt_count THEN RAISE EXCEPTION 'invalid presentation retry'; END IF;
    ELSIF OLD.state IN('awaiting_retention','pending','leased') AND NEW.state IN('completed','expired','unavailable') THEN
      IF OLD.initial_disposition<>'retained' OR NEW.retention_state<>'release_pending' THEN RAISE EXCEPTION 'retained terminal task requires release'; END IF;
      IF NEW.state='completed' AND (OLD.state<>'leased' OR NEW.terminal_claim_token IS DISTINCT FROM OLD.claim_token) THEN RAISE EXCEPTION 'completion claim mismatch'; END IF;
    ELSIF OLD.retention_state='release_pending' AND NEW.retention_state='released' AND NEW.state=OLD.state THEN
      NULL;
    ELSE RAISE EXCEPTION 'invalid presentation task transition';
    END IF;
  END IF;
  NEW.updated_at=now();
  RETURN NEW;
END $$;
CREATE TRIGGER recording_presentation_v2_task_guard BEFORE INSERT OR UPDATE ON recording_presentation_v2_probe_tasks FOR EACH ROW EXECUTE FUNCTION guard_recording_presentation_v2_task();
CREATE TRIGGER recording_presentation_v2_task_no_delete BEFORE DELETE OR TRUNCATE ON recording_presentation_v2_probe_tasks FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_presentation_v2_mutation();

CREATE FUNCTION audit_recording_presentation_v2_task() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO recording_presentation_v2_task_events(task_id,revision,from_state,to_state,from_retention_state,to_retention_state,reason,claim_token)
  VALUES(NEW.id,NEW.revision,CASE WHEN TG_OP='UPDATE' THEN OLD.state END,NEW.state,
    CASE WHEN TG_OP='UPDATE' THEN OLD.retention_state END,NEW.retention_state,
    CASE WHEN TG_OP='INSERT' THEN 'created' ELSE COALESCE(NEW.unavailable_reason,'transition') END,
    COALESCE(NEW.claim_token,NEW.terminal_claim_token,
      CASE WHEN TG_OP='UPDATE' AND OLD.state='leased' THEN OLD.claim_token END));
  RETURN NEW;
END $$;
CREATE TRIGGER recording_presentation_v2_task_audit AFTER INSERT OR UPDATE ON recording_presentation_v2_probe_tasks FOR EACH ROW EXECUTE FUNCTION audit_recording_presentation_v2_task();

CREATE FUNCTION validate_recording_presentation_v2_fact(p_fact_id BIGINT) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE f recording_presentation_v2_authored_facts%ROWTYPE; t recording_presentation_v2_probe_tasks%ROWTYPE;
  v_axes INTEGER; v_complete INTEGER; v_unknown INTEGER; v_not_present INTEGER; v_audio_not_present INTEGER;
  axis_fact RECORD; expected_edges INTEGER; actual_edges INTEGER; corresponding TEXT;
BEGIN
  SELECT * INTO f FROM recording_presentation_v2_authored_facts WHERE id=p_fact_id;
  IF NOT FOUND THEN RETURN; END IF;
  SELECT * INTO t FROM recording_presentation_v2_probe_tasks WHERE id=f.task_id;
  IF NOT FOUND OR t.state<>'completed' OR t.terminal_claim_token IS DISTINCT FROM f.claim_token
     OR t.account_id<>f.account_id OR t.node_id<>f.node_id OR t.request_sha256<>f.request_sha256 THEN
    RAISE EXCEPTION 'presentation fact task/claim/request mismatch';
  END IF;
  IF f.completed_at IS DISTINCT FROM f.created_at OR f.created_at IS DISTINCT FROM now() THEN
    RAISE EXCEPTION 'presentation fact completion time must be transaction time';
  END IF;
  SELECT count(*),count(*) FILTER(WHERE status='complete'),count(*) FILTER(WHERE status='unknown'),
         count(*) FILTER(WHERE status='not_present'),
         count(*) FILTER(WHERE axis IN('demux_audio','raw_audio','audio_sample') AND status='not_present')
  INTO v_axes,v_complete,v_unknown,v_not_present,v_audio_not_present
  FROM recording_presentation_v2_fact_axes WHERE fact_id=f.id;
  IF v_axes<>6 OR (v_not_present NOT IN(0,3)) THEN RAISE EXCEPTION 'presentation fact requires exact coherent six axes'; END IF;
  IF (f.authored_status='complete') IS DISTINCT FROM (v_unknown=0 AND (v_complete=6 OR (v_complete=3 AND v_not_present=3)))
     OR (f.authored_status='unknown') IS DISTINCT FROM (v_complete=0 AND (v_unknown=6 OR (v_unknown=3 AND v_not_present=3)))
     OR (f.authored_status='partial') IS DISTINCT FROM (v_complete>0 AND v_unknown>0) THEN
    RAISE EXCEPTION 'presentation authored status differs from axes';
  END IF;
  IF (f.authored_status='complete' AND f.terminal_reason<>'all_axes_complete')
     OR (f.authored_status='partial' AND f.terminal_reason<>'some_axes_unknown')
     OR (f.authored_status='unknown' AND f.terminal_reason<>'all_axes_unknown') THEN
    RAISE EXCEPTION 'presentation terminal reason differs from axes';
  END IF;
  FOR axis_fact IN SELECT * FROM recording_presentation_v2_fact_axes WHERE fact_id=f.id LOOP
    SELECT count(*) INTO actual_edges FROM recording_presentation_v2_packet_edges WHERE fact_id=f.id AND axis=axis_fact.axis;
    IF axis_fact.axis IN('demux_video','demux_audio') AND axis_fact.status='complete' THEN
      expected_edges:=2*LEAST(4,axis_fact.unit_count)::integer;
      IF actual_edges<>expected_edges THEN RAISE EXCEPTION 'demux edge cardinality mismatch'; END IF;
      IF EXISTS(
        SELECT 1 FROM recording_presentation_v2_packet_edges e
        WHERE e.fact_id=f.id AND e.axis=axis_fact.axis AND
          (e.time_base_num<>axis_fact.time_base_num OR e.time_base_den<>axis_fact.time_base_den
           OR (e.edge_side='leading' AND e.demux_ordinal<>axis_fact.first_ordinal+e.edge_rank-1)
           OR (e.edge_side='trailing' AND e.demux_ordinal<>axis_fact.end_ordinal-e.edge_rank+1)))
        OR NOT EXISTS(SELECT 1 FROM recording_presentation_v2_packet_edges e WHERE e.fact_id=f.id AND e.axis=axis_fact.axis AND e.edge_side='leading' AND e.edge_rank=1 AND e.pts=axis_fact.first_timestamp)
        OR NOT EXISTS(SELECT 1 FROM recording_presentation_v2_packet_edges e WHERE e.fact_id=f.id AND e.axis=axis_fact.axis AND e.edge_side='trailing' AND e.edge_rank=1 AND e.pts+e.duration=axis_fact.end_timestamp) THEN
        RAISE EXCEPTION 'demux edge ordering or endpoint mismatch';
      END IF;
    ELSIF actual_edges<>0 THEN RAISE EXCEPTION 'non-complete demux axis has children'; END IF;
    SELECT count(*) INTO actual_edges FROM recording_presentation_v2_raw_extents WHERE fact_id=f.id AND axis=axis_fact.axis;
    IF axis_fact.axis IN('raw_video','raw_audio') AND axis_fact.status='complete' THEN
      expected_edges:=2*LEAST(4,axis_fact.unit_count)::integer;
      IF actual_edges<>expected_edges THEN RAISE EXCEPTION 'raw extent cardinality mismatch'; END IF;
      corresponding:=CASE axis_fact.axis WHEN 'raw_video' THEN 'demux_video' ELSE 'demux_audio' END;
      IF NOT EXISTS(SELECT 1 FROM recording_presentation_v2_fact_axes d WHERE d.fact_id=f.id AND d.axis=corresponding AND d.status='complete' AND d.unit_count=axis_fact.unit_count)
         OR EXISTS(SELECT 1 FROM recording_presentation_v2_raw_extents x WHERE x.fact_id=f.id AND x.axis=axis_fact.axis AND NOT EXISTS(SELECT 1 FROM recording_presentation_v2_packet_edges p WHERE p.fact_id=f.id AND p.axis=corresponding AND p.edge_side=x.edge_side AND p.edge_rank=x.edge_rank AND p.demux_ordinal=x.demux_ordinal)) THEN
        RAISE EXCEPTION 'raw extent does not match demux edges';
      END IF;
    ELSIF actual_edges<>0 THEN RAISE EXCEPTION 'non-complete raw axis has children'; END IF;
  END LOOP;
  SELECT count(*) INTO actual_edges FROM recording_presentation_v2_video_frame_edges WHERE fact_id=f.id;
  SELECT 2*LEAST(4,unit_count)::integer INTO expected_edges FROM recording_presentation_v2_fact_axes WHERE fact_id=f.id AND axis='video_presentation' AND status='complete';
  IF actual_edges<>COALESCE(expected_edges,0) THEN RAISE EXCEPTION 'video edge cardinality mismatch'; END IF;
  IF expected_edges IS NOT NULL AND (
     NOT EXISTS(SELECT 1 FROM recording_presentation_v2_fact_axes v JOIN recording_presentation_v2_fact_axes d ON d.fact_id=v.fact_id AND d.axis='demux_video' AND d.status='complete' WHERE v.fact_id=f.id AND v.axis='video_presentation' AND v.status='complete')
     OR EXISTS(SELECT 1 FROM recording_presentation_v2_video_frame_edges e JOIN recording_presentation_v2_fact_axes v ON v.fact_id=e.fact_id AND v.axis='video_presentation' WHERE e.fact_id=f.id AND (e.time_base_num<>v.time_base_num OR e.time_base_den<>v.time_base_den OR (e.edge_side='leading' AND e.presentation_ordinal<>v.first_ordinal+e.edge_rank-1) OR (e.edge_side='trailing' AND e.presentation_ordinal<>v.end_ordinal-e.edge_rank+1)))
     OR NOT EXISTS(SELECT 1 FROM recording_presentation_v2_video_frame_edges e JOIN recording_presentation_v2_fact_axes v ON v.fact_id=e.fact_id AND v.axis='video_presentation' WHERE e.fact_id=f.id AND e.edge_side='leading' AND e.edge_rank=1 AND e.pts=v.first_timestamp)
     OR NOT EXISTS(SELECT 1 FROM recording_presentation_v2_video_frame_edges e JOIN recording_presentation_v2_fact_axes v ON v.fact_id=e.fact_id AND v.axis='video_presentation' WHERE e.fact_id=f.id AND e.edge_side='trailing' AND e.edge_rank=1 AND e.pts+e.duration=v.end_timestamp)) THEN
    RAISE EXCEPTION 'video presentation dependency or edge mismatch';
  END IF;
  SELECT count(*) INTO actual_edges FROM recording_presentation_v2_audio_block_edges WHERE fact_id=f.id;
  SELECT 2*LEAST(4,unit_count)::integer INTO expected_edges FROM recording_presentation_v2_fact_axes WHERE fact_id=f.id AND axis='audio_sample' AND status='complete';
  IF actual_edges<>COALESCE(expected_edges,0) THEN RAISE EXCEPTION 'audio edge cardinality mismatch'; END IF;
  IF expected_edges IS NOT NULL AND (
     NOT EXISTS(SELECT 1 FROM recording_presentation_v2_fact_axes a JOIN recording_presentation_v2_fact_axes d ON d.fact_id=a.fact_id AND d.axis='demux_audio' AND d.status='complete' WHERE a.fact_id=f.id AND a.axis='audio_sample' AND a.status='complete')
     OR EXISTS(SELECT 1 FROM recording_presentation_v2_audio_block_edges e JOIN recording_presentation_v2_fact_axes a ON a.fact_id=e.fact_id AND a.axis='audio_sample' WHERE e.fact_id=f.id AND (e.time_base_num<>a.time_base_num OR e.time_base_den<>a.time_base_den OR e.sample_rate<>a.sample_rate OR e.channel_count<>a.channel_count OR e.channel_layout<>a.channel_layout OR (e.edge_side='leading' AND e.block_ordinal<>a.first_ordinal+e.edge_rank-1) OR (e.edge_side='trailing' AND e.block_ordinal<>a.end_ordinal-e.edge_rank+1)))
     OR NOT EXISTS(SELECT 1 FROM recording_presentation_v2_audio_block_edges e JOIN recording_presentation_v2_fact_axes a ON a.fact_id=e.fact_id AND a.axis='audio_sample' WHERE e.fact_id=f.id AND e.edge_side='leading' AND e.edge_rank=1 AND e.pts=a.first_timestamp)
     OR NOT EXISTS(SELECT 1 FROM recording_presentation_v2_audio_block_edges e JOIN recording_presentation_v2_fact_axes a ON a.fact_id=e.fact_id AND a.axis='audio_sample' WHERE e.fact_id=f.id AND e.edge_side='trailing' AND e.edge_rank=1 AND e.pts+e.sample_count=a.end_timestamp)) THEN
    RAISE EXCEPTION 'audio sample dependency or edge mismatch';
  END IF;
  IF EXISTS(SELECT 1 FROM recording_presentation_v2_raw_extents e WHERE e.fact_id=f.id AND (e.byte_size>t.clip_size_bytes OR e.byte_position>t.clip_size_bytes-e.byte_size)) THEN
    RAISE EXCEPTION 'raw extent exceeds exact clip bytes';
  END IF;
END $$;

CREATE FUNCTION recording_presentation_v2_fact_deferred_check() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE target BIGINT;
BEGIN
  target:=COALESCE(NULLIF(to_jsonb(NEW)->>'fact_id','')::bigint,NULLIF(to_jsonb(NEW)->>'id','')::bigint,
                   NULLIF(to_jsonb(OLD)->>'fact_id','')::bigint,NULLIF(to_jsonb(OLD)->>'id','')::bigint);
  PERFORM validate_recording_presentation_v2_fact(target);
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER recording_presentation_v2_fact_validate AFTER INSERT ON recording_presentation_v2_authored_facts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION recording_presentation_v2_fact_deferred_check();
CREATE CONSTRAINT TRIGGER recording_presentation_v2_axis_validate AFTER INSERT ON recording_presentation_v2_fact_axes DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION recording_presentation_v2_fact_deferred_check();
CREATE CONSTRAINT TRIGGER recording_presentation_v2_packet_validate AFTER INSERT ON recording_presentation_v2_packet_edges DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION recording_presentation_v2_fact_deferred_check();
CREATE CONSTRAINT TRIGGER recording_presentation_v2_extent_validate AFTER INSERT ON recording_presentation_v2_raw_extents DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION recording_presentation_v2_fact_deferred_check();
CREATE CONSTRAINT TRIGGER recording_presentation_v2_video_validate AFTER INSERT ON recording_presentation_v2_video_frame_edges DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION recording_presentation_v2_fact_deferred_check();
CREATE CONSTRAINT TRIGGER recording_presentation_v2_audio_validate AFTER INSERT ON recording_presentation_v2_audio_block_edges DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION recording_presentation_v2_fact_deferred_check();

CREATE FUNCTION validate_recording_presentation_v2_release() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE t recording_presentation_v2_probe_tasks%ROWTYPE;
BEGIN
  SELECT * INTO t FROM recording_presentation_v2_probe_tasks WHERE id=NEW.task_id FOR SHARE;
  IF NOT FOUND OR t.initial_disposition<>'retained' OR t.retention_state<>'release_pending'
     OR t.state<>NEW.terminal_state OR t.node_id<>NEW.node_id
     OR NEW.authorized_at IS DISTINCT FROM now()
     OR NEW.binding_sha256 IS DISTINCT FROM COALESCE(t.retention_identity_sha256,t.staging_identity_sha256) THEN
    RAISE EXCEPTION 'release authorization task mismatch';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_presentation_v2_release_validate BEFORE INSERT ON recording_presentation_v2_release_authorizations FOR EACH ROW EXECUTE FUNCTION validate_recording_presentation_v2_release();

CREATE FUNCTION validate_recording_presentation_v2_release_ack() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE a recording_presentation_v2_release_authorizations%ROWTYPE;
BEGIN
  SELECT * INTO a FROM recording_presentation_v2_release_authorizations WHERE task_id=NEW.task_id FOR SHARE;
  IF NOT FOUND OR (NEW.release_version,NEW.node_id,NEW.binding_sha256) IS DISTINCT FROM (a.release_version,a.node_id,a.binding_sha256)
     OR NEW.acknowledged_at IS DISTINCT FROM now() THEN RAISE EXCEPTION 'release acknowledgement mismatch'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_presentation_v2_release_ack_validate BEFORE INSERT ON recording_presentation_v2_release_acknowledgements FOR EACH ROW EXECUTE FUNCTION validate_recording_presentation_v2_release_ack();

CREATE FUNCTION validate_recording_presentation_v2_task_terminal() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE auth_count INTEGER; fact_count INTEGER; ack_count INTEGER; target UUID; t recording_presentation_v2_probe_tasks%ROWTYPE;
BEGIN
  target:=COALESCE(NULLIF(to_jsonb(NEW)->>'task_id','')::uuid,NULLIF(to_jsonb(NEW)->>'id','')::uuid);
  SELECT * INTO t FROM recording_presentation_v2_probe_tasks WHERE id=target;
  IF NOT FOUND THEN RETURN NULL; END IF;
  SELECT count(*) INTO auth_count FROM recording_presentation_v2_release_authorizations WHERE task_id=target;
  SELECT count(*) INTO fact_count FROM recording_presentation_v2_authored_facts WHERE task_id=target;
  SELECT count(*) INTO ack_count FROM recording_presentation_v2_release_acknowledgements WHERE task_id=target;
  IF t.initial_disposition='unavailable' AND (auth_count<>0 OR fact_count<>0) THEN RAISE EXCEPTION 'initial unavailable task cannot have release or fact'; END IF;
  IF t.state='completed' AND fact_count<>1 THEN RAISE EXCEPTION 'completed presentation task requires exact fact'; END IF;
  IF t.initial_disposition='retained' AND t.state IN('completed','expired','unavailable') AND auth_count<>1 THEN RAISE EXCEPTION 'retained terminal task requires release authorization'; END IF;
  IF (t.retention_state='released') IS DISTINCT FROM (ack_count=1) THEN
    RAISE EXCEPTION 'presentation release state and acknowledgement must be atomic';
  END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER recording_presentation_v2_task_terminal_validate AFTER INSERT OR UPDATE ON recording_presentation_v2_probe_tasks DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION validate_recording_presentation_v2_task_terminal();
CREATE CONSTRAINT TRIGGER recording_presentation_v2_release_terminal_validate AFTER INSERT ON recording_presentation_v2_release_authorizations DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION validate_recording_presentation_v2_task_terminal();
CREATE CONSTRAINT TRIGGER recording_presentation_v2_ack_terminal_validate AFTER INSERT ON recording_presentation_v2_release_acknowledgements DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION validate_recording_presentation_v2_task_terminal();
