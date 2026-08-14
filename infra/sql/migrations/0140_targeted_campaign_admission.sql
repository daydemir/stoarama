-- Exact, operator-authorized campaign admission. This migration reserves an
-- approved physical scene before probes begin, accepts evidence only from a
-- fresh authenticated managed DO recorder, and seals scheduling + roster
-- protection in one transaction.
CREATE TABLE recording_campaign_admission_approvals (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  request_id UUID NOT NULL,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  actor_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
  actor_email_snapshot TEXT NOT NULL,
  authority_code TEXT NOT NULL CHECK(authority_code IN('deniz_fd_restore_20260814','deniz_scene_approval_20260814')),
  failure_domain_tag TEXT,
  deadline_at TIMESTAMPTZ NOT NULL,
  entries JSONB NOT NULL CHECK(jsonb_typeof(entries)='array' AND jsonb_array_length(entries) BETWEEN 1 AND 32),
  schedule_spec JSONB NOT NULL CHECK(jsonb_typeof(schedule_spec)='object'),
  schedule_sha256 TEXT NOT NULL CHECK(schedule_sha256~'^[0-9a-f]{64}$'),
  approval_sha256 TEXT NOT NULL CHECK(approval_sha256~'^[0-9a-f]{64}$'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
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
  reserved_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  PRIMARY KEY(approval_id,stream_id),
  FOREIGN KEY(scene_frame_evidence_id,account_id,stream_id,scene_identity_sha256)
    REFERENCES recording_scene_frame_evidence(id,account_id,stream_id,scene_identity_sha256) ON DELETE RESTRICT
);
CREATE INDEX recording_campaign_admission_pending_stream ON recording_campaign_admission_reservations(account_id,stream_id);
CREATE INDEX recording_campaign_admission_pending_scene ON recording_campaign_admission_reservations(account_id,scene_identity_sha256);

-- An approval never follows a mutable source row. Any authoritative source or
-- scene-identity field change after reservation permanently invalidates that
-- approval, including an A -> B -> A edit that restores the old bytes and
-- timestamp. A later approval can reserve the new head after this event.
CREATE TABLE recording_campaign_admission_source_fence_events (
  id BIGSERIAL PRIMARY KEY,
  stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE RESTRICT,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  prior_fence_sha256 TEXT NOT NULL CHECK(prior_fence_sha256~'^[0-9a-f]{64}$'),
  next_fence_sha256 TEXT NOT NULL CHECK(next_fence_sha256~'^[0-9a-f]{64}$')
);
CREATE INDEX recording_campaign_admission_source_fence_stream ON recording_campaign_admission_source_fence_events(stream_id,occurred_at);

CREATE TABLE recording_targeted_probe_attempts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  request_id UUID NOT NULL,
  approval_id UUID NOT NULL REFERENCES recording_campaign_admission_approvals(id) ON DELETE RESTRICT,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE RESTRICT,
  attempt_no INTEGER NOT NULL CHECK(attempt_no BETWEEN 1 AND 8),
  node_id BIGINT NOT NULL REFERENCES nodes(id) ON DELETE RESTRICT,
  recorder_droplet_id BIGINT NOT NULL REFERENCES recorder_droplets(id) ON DELETE RESTRICT,
  do_droplet_id BIGINT NOT NULL,
  region TEXT NOT NULL,
  probe_build_sha TEXT NOT NULL CHECK(probe_build_sha~'^[0-9a-f]{40}$'),
  source_revision_id BIGINT REFERENCES stream_source_revisions(id) ON DELETE RESTRICT,
  source_url_sha256 TEXT NOT NULL CHECK(source_url_sha256~'^[0-9a-f]{64}$'),
  source_page_url_sha256 TEXT NOT NULL CHECK(source_page_url_sha256~'^[0-9a-f]{64}$'),
  source_updated_at TIMESTAMPTZ NOT NULL,
  challenge TEXT NOT NULL CHECK(challenge~'^[0-9a-f]{64}$'),
  started_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  expires_at TIMESTAMPTZ NOT NULL,
  UNIQUE(account_id,request_id),
  UNIQUE(approval_id,stream_id,attempt_no),
  CHECK(expires_at=started_at+interval '15 minutes')
);

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
  detail TEXT NOT NULL CHECK(length(detail)<=1024 AND detail~'^(resolve_failed|image_source|ssrf_guard_rejected|temporary_storage_unavailable|parent_context_cancelled|valid_ratio=[0-9]+\.[0-9]{3} segments=[0-9]+ native_signature_stable=(true|false) frame=(true|false)( capture_exit=(network_cut|source_down|other))?)$'),
  observed_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  evidence_sha256 TEXT NOT NULL CHECK(evidence_sha256~'^[0-9a-f]{64}$'),
  CHECK(result<>'ok' OR (valid_ratio>=0.95 AND duration_ms>=120000 AND segment_count>=2 AND
    frame_sha256 IS NOT NULL AND media_sha256 IS NOT NULL AND native_signature_sha256 IS NOT NULL AND challenge_proof_sha256 IS NOT NULL AND
    lower(video_codec)='h264' AND video_width BETWEEN 1 AND 16384 AND video_height BETWEEN 1 AND 16384 AND actual_fps>0 AND actual_fps<=240 AND
    audio_present IS NOT NULL AND ((audio_present AND length(btrim(COALESCE(audio_codec,'')))>0) OR (NOT audio_present AND btrim(COALESCE(audio_codec,''))=''))))
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
  schedule_sha256 TEXT NOT NULL CHECK(schedule_sha256~'^[0-9a-f]{64}$'),
  recording_config_sha256 TEXT NOT NULL CHECK(recording_config_sha256~'^[0-9a-f]{64}$'),
  admitted_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
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
  committed_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp()
);

CREATE TABLE recording_campaign_admission_tx_authorizations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  transaction_id BIGINT NOT NULL,
  action TEXT NOT NULL CHECK(action IN('approve','attempt','evidence','admit')),
  approval_id UUID REFERENCES recording_campaign_admission_approvals(id) ON DELETE RESTRICT,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  actor_user_id BIGINT REFERENCES users(id) ON DELETE RESTRICT,
  account_session_id BIGINT REFERENCES account_sessions(id) ON DELETE RESTRICT,
  node_id BIGINT REFERENCES nodes(id) ON DELETE RESTRICT,
  node_token_id BIGINT REFERENCES node_tokens(id) ON DELETE RESTRICT,
  authorized_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  UNIQUE(transaction_id,action,account_id),
  CHECK((action IN('approve','admit') AND actor_user_id IS NOT NULL AND account_session_id IS NOT NULL AND node_id IS NULL AND node_token_id IS NULL) OR
        (action IN('attempt','evidence') AND actor_user_id IS NULL AND account_session_id IS NULL AND node_id IS NOT NULL AND node_token_id IS NOT NULL)),
  CHECK((action='approve' AND approval_id IS NULL) OR (action<>'approve' AND approval_id IS NOT NULL))
);

CREATE OR REPLACE FUNCTION validate_recording_campaign_tx_authorization()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; valid BOOLEAN;
BEGIN
  IF NEW.transaction_id<>txid_current() OR NEW.authorized_at IS DISTINCT FROM transaction_timestamp() THEN RAISE EXCEPTION 'campaign authorization must be bound to the current database transaction'; END IF;
  IF NEW.action IN('approve','admit') THEN
    EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.account_sessions sess JOIN %I.users u ON u.id=sess.user_id JOIN %I.accounts o ON o.id=$3 JOIN %I.memberships m ON m.user_id=u.id AND m.org_id=$3 AND m.accepted_at IS NOT NULL WHERE sess.id=$1 AND sess.user_id=$2 AND sess.current_org_id=$3 AND sess.revoked_at IS NULL AND sess.expires_at>transaction_timestamp() AND o.status=''active'' AND u.is_operator AND m.role IN(''owner'',''admin''))',s,s,s,s)
      INTO valid USING NEW.account_session_id,NEW.actor_user_id,NEW.account_id;
    IF valid AND NEW.action='admit' THEN
      EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_approvals WHERE id=$1 AND account_id=$2 AND actor_user_id=$3 AND deadline_at>transaction_timestamp())',s)
        INTO valid USING NEW.approval_id,NEW.account_id,NEW.actor_user_id;
    END IF;
  ELSE
    EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.node_tokens tok JOIN %I.nodes n ON n.id=tok.node_id JOIN %I.accounts o ON o.id=n.account_id JOIN %I.recorder_droplets d ON d.node_id=n.id WHERE tok.id=$1 AND tok.node_id=$2 AND tok.revoked_at IS NULL AND n.account_id=$3 AND o.status=''active'' AND n.node_type=''local_recorder'' AND n.status=''active'' AND d.state=''active'' AND d.last_seen_at>=transaction_timestamp()-interval ''120 seconds'')',s,s,s,s)
      INTO valid USING NEW.node_token_id,NEW.node_id,NEW.account_id;
  END IF;
  IF valid IS DISTINCT FROM true THEN RAISE EXCEPTION 'campaign authorization principal is not current and exact'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_tx_authorization_validate BEFORE INSERT ON recording_campaign_admission_tx_authorizations FOR EACH ROW EXECUTE FUNCTION validate_recording_campaign_tx_authorization();

CREATE OR REPLACE FUNCTION validate_recording_campaign_admission_commit()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; valid BOOLEAN; response_valid BOOLEAN; result_count INTEGER; entry_count INTEGER;
BEGIN
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_approvals a JOIN %I.recording_campaign_tracks t ON t.id=$4 WHERE a.id=$1 AND a.account_id=$2 AND a.actor_user_id=$3 AND a.schedule_sha256=$5 AND t.account_id=$2 AND t.state=''active'')',s,s)
    INTO valid USING NEW.approval_id,NEW.account_id,NEW.actor_user_id,NEW.track_id,NEW.schedule_sha256;
  EXECUTE format('SELECT count(*) FROM %I.recording_campaign_admission_results WHERE approval_id=$1',s) INTO result_count USING NEW.approval_id;
  EXECUTE format('SELECT jsonb_array_length(entries) FROM %I.recording_campaign_admission_approvals WHERE id=$1',s) INTO entry_count USING NEW.approval_id;
  IF jsonb_typeof(NEW.response_json)<>'object' OR (SELECT count(*) FROM jsonb_object_keys(NEW.response_json))<>9 OR
     NOT(NEW.response_json ?& ARRAY['items','created','updated','dry_run','relay_streams','online_relay_slots','required_relay_slots','campaign_track_id','campaign_admission_approval_id']) OR
     jsonb_typeof(NEW.response_json->'items')<>'array' OR jsonb_typeof(NEW.response_json->'created')<>'number' OR (NEW.response_json->>'created')!~'^[0-9]+$' OR
     jsonb_typeof(NEW.response_json->'updated')<>'number' OR (NEW.response_json->>'updated')!~'^[0-9]+$' OR jsonb_typeof(NEW.response_json->'dry_run')<>'boolean' OR
     jsonb_typeof(NEW.response_json->'relay_streams')<>'number' OR (NEW.response_json->>'relay_streams')!~'^[0-9]+$' OR
     jsonb_typeof(NEW.response_json->'online_relay_slots')<>'number' OR (NEW.response_json->>'online_relay_slots')!~'^[0-9]+$' OR
     jsonb_typeof(NEW.response_json->'required_relay_slots')<>'number' OR (NEW.response_json->>'required_relay_slots')!~'^[0-9]+$' OR
     jsonb_typeof(NEW.response_json->'campaign_track_id')<>'number' OR (NEW.response_json->>'campaign_track_id')!~'^[1-9][0-9]*$' OR
     jsonb_typeof(NEW.response_json->'campaign_admission_approval_id')<>'string' THEN RAISE EXCEPTION 'campaign admission response has invalid exact shape'; END IF;
  IF (NEW.response_json->>'dry_run')::boolean OR (NEW.response_json->>'campaign_track_id')::bigint<>NEW.track_id OR
     NEW.response_json->>'campaign_admission_approval_id'<>NEW.approval_id::text OR
     (NEW.response_json->>'created')::int+(NEW.response_json->>'updated')::int<>result_count OR
     jsonb_array_length(NEW.response_json->'items')<>result_count THEN RAISE EXCEPTION 'campaign admission response summary mismatch'; END IF;
  IF EXISTS(SELECT 1 FROM jsonb_array_elements(NEW.response_json->'items') item WHERE jsonb_typeof(item)<>'object' OR (SELECT count(*) FROM jsonb_object_keys(item))<>4 OR NOT(item ?& ARRAY['stream_id','recording_id','action','timezone']) OR jsonb_typeof(item->'stream_id')<>'number' OR (item->>'stream_id')!~'^[1-9][0-9]*$' OR jsonb_typeof(item->'recording_id')<>'number' OR (item->>'recording_id')!~'^[1-9][0-9]*$' OR jsonb_typeof(item->'action')<>'string' OR item->>'action' NOT IN('created','updated') OR jsonb_typeof(item->'timezone')<>'string' OR btrim(item->>'timezone')='') OR
     (SELECT count(DISTINCT item->>'stream_id') FROM jsonb_array_elements(NEW.response_json->'items') item)<>result_count THEN RAISE EXCEPTION 'campaign admission response items have invalid exact shape'; END IF;
  EXECUTE format('SELECT bool_and(EXISTS(SELECT 1 FROM %I.recording_campaign_admission_results ar JOIN %I.recordings r ON r.id=ar.recording_id WHERE ar.approval_id=$1 AND ar.track_id=$2 AND ar.stream_id=(item->>''stream_id'')::bigint AND ar.recording_id=(item->>''recording_id'')::bigint AND r.cron_timezone=item->>''timezone'')) FROM jsonb_array_elements($3->''items'') item',s,s)
    INTO response_valid USING NEW.approval_id,NEW.track_id,NEW.response_json;
  IF valid IS DISTINCT FROM true OR result_count<>entry_count OR NEW.committed_at IS DISTINCT FROM transaction_timestamp() OR
     response_valid IS DISTINCT FROM true OR
     NEW.response_sha256<>encode(sha256(convert_to(NEW.response_json::text,'UTF8')),'hex') THEN RAISE EXCEPTION 'campaign admission commit is not the exact sealed response'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_admission_commit_validate BEFORE INSERT ON recording_campaign_admission_commits FOR EACH ROW EXECUTE FUNCTION validate_recording_campaign_admission_commit();

CREATE OR REPLACE FUNCTION enforce_recording_campaign_result_has_commit()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; sealed BOOLEAN;
BEGIN
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_commits WHERE approval_id=$1 AND account_id=$2 AND track_id=$3)',s) INTO sealed USING NEW.approval_id,NEW.account_id,NEW.track_id;
  IF sealed IS DISTINCT FROM true THEN RAISE EXCEPTION 'campaign admission results must commit one immutable replay response'; END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER recording_campaign_admission_result_commit AFTER INSERT ON recording_campaign_admission_results DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION enforce_recording_campaign_result_has_commit();

CREATE OR REPLACE FUNCTION validate_recording_campaign_admission_approval()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; authorized BOOLEAN; entry JSONB; source_hash TEXT; page_hash TEXT; source_updated TIMESTAMPTZ; stream_provider TEXT; stream_external TEXT; stream_label TEXT; stream_timezone TEXT; expected_timezone TEXT; tags TEXT[]; latest_revision BIGINT; scene_bound BOOLEAN; recording_account BIGINT; recording_stream BIGINT; recording_status TEXT; expected_schedule TEXT; expected_approval TEXT; prior_stream BIGINT:=0;
BEGIN
  EXECUTE format('SELECT u.is_operator AND lower(u.email)=lower($2) FROM %I.users u WHERE u.id=$1',s)
    INTO authorized USING NEW.actor_user_id,NEW.actor_email_snapshot;
  IF authorized IS DISTINCT FROM true OR NEW.actor_email_snapshot<>lower(btrim(NEW.actor_email_snapshot)) THEN RAISE EXCEPTION 'campaign approval requires the exact canonical authenticated operator snapshot'; END IF;
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_tx_authorizations WHERE transaction_id=txid_current() AND action=''approve'' AND account_id=$1 AND actor_user_id=$2)',s) INTO authorized USING NEW.account_id,NEW.actor_user_id;
  IF authorized IS DISTINCT FROM true THEN RAISE EXCEPTION 'campaign approval requires typed transaction authorization'; END IF;
  EXECUTE format('SELECT 1 FROM %I.accounts WHERE id=$1 FOR UPDATE',s) USING NEW.account_id;
  IF NEW.created_at IS DISTINCT FROM transaction_timestamp() THEN RAISE EXCEPTION 'campaign approval time is database-authored'; END IF;
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
     (((NEW.schedule_spec->>'storage_destination_id')::bigint>0)=((NEW.schedule_spec->>'delivery_storage_destination_id')::bigint>0)) OR
     jsonb_typeof(NEW.schedule_spec->'delivery')<>'string' OR NEW.schedule_spec->>'delivery' NOT IN('managed','nas_pull','webdav') OR
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
    EXECUTE format('SELECT encode(sha256(convert_to(source_url,''UTF8'')),''hex''),encode(sha256(convert_to(COALESCE(source_page_url,''''),''UTF8'')),''hex''),updated_at,COALESCE(provider,''''),COALESCE(external_id,''''),lower(regexp_replace(name,''[^[:alnum:]]'','''',''g'')),COALESCE(local_timezone,''''),tags,(SELECT max(id) FROM %I.stream_source_revisions WHERE stream_id=st.id) FROM %I.streams st WHERE id=$1 AND deleted_at IS NULL FOR UPDATE',s,s)
      INTO source_hash,page_hash,source_updated,stream_provider,stream_external,stream_label,stream_timezone,tags,latest_revision USING (entry->>'stream_id')::bigint;
    SELECT value->>'timezone' INTO expected_timezone FROM jsonb_array_elements(NEW.schedule_spec->'stream_timezones') value WHERE (value->>'stream_id')::bigint=(entry->>'stream_id')::bigint;
    IF source_hash IS NULL OR source_hash<>(entry->>'source_url_sha256') OR page_hash<>(entry->>'source_page_url_sha256') OR
       floor(extract(epoch from source_updated)*1000000)::bigint<>(entry->>'source_updated_at_unix_micros')::bigint OR
       stream_provider<>(entry->>'provider') OR stream_external<>(entry->>'external_id') OR stream_label<>(entry->>'normalized_label') OR stream_timezone<>expected_timezone OR
       COALESCE(latest_revision,0)<>(entry->>'source_revision_id')::bigint THEN RAISE EXCEPTION 'campaign approval source fence mismatch'; END IF;
    IF NEW.failure_domain_tag IS NOT NULL AND NOT(NEW.failure_domain_tag=ANY(tags)) THEN RAISE EXCEPTION 'campaign approval stream is outside required failure domain'; END IF;
    EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_scene_frame_evidence WHERE id=$1 AND account_id=$2 AND stream_id=$3 AND scene_identity_sha256=$4 AND verified_at BETWEEN transaction_timestamp()-interval ''6 hours'' AND transaction_timestamp())',s)
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
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; entry JSONB; collision BOOLEAN;
BEGIN
  EXECUTE format('SELECT 1 FROM %I.accounts WHERE id=$1 FOR UPDATE',s) USING NEW.account_id;
  FOR entry IN SELECT value FROM jsonb_array_elements(NEW.entries) LOOP
    EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recordings r LEFT JOIN %I.streams active_stream ON active_stream.id=r.stream_id WHERE r.account_id=$1 AND r.status<>''canceled'' AND NOT(r.id=NULLIF($4,0) AND r.status=''completed'') AND (r.stream_id=$2 OR encode(sha256(convert_to(active_stream.source_url,''UTF8'')),''hex'')=$5 OR (NULLIF(active_stream.provider,'''') IS NOT NULL AND active_stream.provider=$6 AND NULLIF(active_stream.external_id,'''') IS NOT NULL AND active_stream.external_id=$7) OR lower(regexp_replace(active_stream.name,''[^[:alnum:]]'','''',''g''))=$8) UNION ALL SELECT 1 FROM %I.recording_campaign_roster_entries e JOIN %I.recording_campaign_tracks t ON t.id=e.track_id JOIN %I.streams protected_stream ON protected_stream.id=e.stream_id WHERE t.account_id=$1 AND t.state IN(''active'',''complete'') AND e.status IN(''protect'',''probation'') AND (e.stream_id=$2 OR e.scene_identity_sha256=$3 OR encode(sha256(convert_to(protected_stream.source_url,''UTF8'')),''hex'')=$5 OR (NULLIF(protected_stream.provider,'''') IS NOT NULL AND protected_stream.provider=$6 AND NULLIF(protected_stream.external_id,'''') IS NOT NULL AND protected_stream.external_id=$7) OR lower(regexp_replace(protected_stream.name,''[^[:alnum:]]'','''',''g''))=$8) UNION ALL SELECT 1 FROM %I.recording_campaign_admission_reservations pending JOIN %I.recording_campaign_admission_approvals approval ON approval.id=pending.approval_id WHERE pending.account_id=$1 AND approval.deadline_at>transaction_timestamp() AND (pending.stream_id=$2 OR pending.scene_identity_sha256=$3 OR pending.recording_id=NULLIF($4,0) OR pending.source_url_sha256=$5 OR (NULLIF(pending.provider,'''') IS NOT NULL AND pending.provider=$6 AND NULLIF(pending.external_id,'''') IS NOT NULL AND pending.external_id=$7) OR pending.normalized_label=$8) LIMIT 1)',s,s,s,s,s,s,s)
      INTO collision USING NEW.account_id,(entry->>'stream_id')::bigint,entry->>'scene_identity_sha256',(entry->>'recording_id')::bigint,entry->>'source_url_sha256',entry->>'provider',entry->>'external_id',entry->>'normalized_label';
    IF collision THEN RAISE EXCEPTION 'campaign approval collides with active/protected occupancy'; END IF;
    EXECUTE format('INSERT INTO %I.recording_campaign_admission_reservations(approval_id,account_id,stream_id,recording_id,source_revision_id,source_url_sha256,source_page_url_sha256,source_updated_at,provider,external_id,normalized_label,scene_frame_evidence_id,scene_identity_sha256) VALUES($1,$2,$3,NULLIF($4,0),NULLIF($5,0),$6,$7,TIMESTAMPTZ ''epoch''+$8*interval ''1 microsecond'',$9,$10,$11,$12,$13)',s)
      USING NEW.id,NEW.account_id,(entry->>'stream_id')::bigint,(entry->>'recording_id')::bigint,(entry->>'source_revision_id')::bigint,entry->>'source_url_sha256',entry->>'source_page_url_sha256',(entry->>'source_updated_at_unix_micros')::bigint,entry->>'provider',entry->>'external_id',entry->>'normalized_label',(entry->>'scene_frame_evidence_id')::bigint,entry->>'scene_identity_sha256';
  END LOOP;
  RETURN NEW;
END $$;
CREATE OR REPLACE FUNCTION validate_recording_campaign_admission_reservation()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; a_account BIGINT; entry JSONB;
BEGIN
  EXECUTE format('SELECT account_id,(SELECT value FROM jsonb_array_elements(entries) value WHERE (value->>''stream_id'')::bigint=$2) FROM %I.recording_campaign_admission_approvals WHERE id=$1',s)
    INTO a_account,entry USING NEW.approval_id,NEW.stream_id;
  IF a_account IS NULL OR a_account<>NEW.account_id OR entry IS NULL OR NEW.reserved_at IS DISTINCT FROM transaction_timestamp() OR
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
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; reserved BOOLEAN; prior_hash TEXT; next_hash TEXT;
BEGIN
  IF (NEW.source_url,NEW.source_page_url,NEW.provider,NEW.external_id,NEW.name,NEW.local_timezone,NEW.tags,NEW.deleted_at,NEW.updated_at)
     IS NOT DISTINCT FROM
     (OLD.source_url,OLD.source_page_url,OLD.provider,OLD.external_id,OLD.name,OLD.local_timezone,OLD.tags,OLD.deleted_at,OLD.updated_at) THEN RETURN NEW; END IF;
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_reservations WHERE stream_id=$1)',s) INTO reserved USING NEW.id;
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

CREATE OR REPLACE FUNCTION validate_recording_targeted_probe_attempt()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; authorized BOOLEAN; a_account BIGINT; a_deadline TIMESTAMPTZ; r_revision BIGINT; r_source TEXT; r_page TEXT; r_updated TIMESTAMPTZ; r_reserved TIMESTAMPTZ; source_clean BOOLEAN; current_revision BIGINT; current_source TEXT; current_page TEXT; current_updated TIMESTAMPTZ; d_node BIGINT; d_do BIGINT; d_region TEXT; d_build TEXT; d_state TEXT; d_seen TIMESTAMPTZ; expected_attempt INTEGER; prior_completed TIMESTAMPTZ;
BEGIN
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_tx_authorizations WHERE transaction_id=txid_current() AND action=''attempt'' AND approval_id=$1 AND account_id=$2 AND node_id=$3)',s) INTO authorized USING NEW.approval_id,NEW.account_id,NEW.node_id;
  IF authorized IS DISTINCT FROM true THEN RAISE EXCEPTION 'targeted attempt requires typed node transaction authorization'; END IF;
  EXECUTE format('SELECT account_id,deadline_at FROM %I.recording_campaign_admission_approvals WHERE id=$1',s) INTO a_account,a_deadline USING NEW.approval_id;
  EXECUTE format('SELECT source_revision_id,source_url_sha256,source_page_url_sha256,source_updated_at,reserved_at FROM %I.recording_campaign_admission_reservations WHERE approval_id=$1 AND stream_id=$2 FOR UPDATE',s) INTO r_revision,r_source,r_page,r_updated,r_reserved USING NEW.approval_id,NEW.stream_id;
  EXECUTE format('SELECT NOT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_source_fence_events WHERE stream_id=$1 AND occurred_at>= $2)',s) INTO source_clean USING NEW.stream_id,r_reserved;
  EXECUTE format('SELECT (SELECT max(id) FROM %I.stream_source_revisions WHERE stream_id=st.id),encode(sha256(convert_to(st.source_url,''UTF8'')),''hex''),encode(sha256(convert_to(COALESCE(st.source_page_url,''''),''UTF8'')),''hex''),st.updated_at FROM %I.streams st WHERE id=$1 AND deleted_at IS NULL FOR SHARE',s,s)
    INTO current_revision,current_source,current_page,current_updated USING NEW.stream_id;
  EXECUTE format('SELECT node_id,do_droplet_id,region,build_sha,state,last_seen_at FROM %I.recorder_droplets WHERE id=$1 FOR SHARE',s) INTO d_node,d_do,d_region,d_build,d_state,d_seen USING NEW.recorder_droplet_id;
  EXECUTE format('SELECT COALESCE(max(attempt_no),0)+1 FROM %I.recording_targeted_probe_attempts WHERE approval_id=$1 AND stream_id=$2',s) INTO expected_attempt USING NEW.approval_id,NEW.stream_id;
  IF expected_attempt>1 THEN
    EXECUTE format('SELECT e.observed_at FROM %I.recording_targeted_probe_attempts a JOIN %I.recording_targeted_probe_evidence e ON e.attempt_id=a.id WHERE a.approval_id=$1 AND a.stream_id=$2 AND a.attempt_no=$3 - 1',s,s) INTO prior_completed USING NEW.approval_id,NEW.stream_id,expected_attempt;
  END IF;
  IF a_account IS NULL OR a_account<>NEW.account_id OR transaction_timestamp()>=a_deadline OR NEW.started_at IS DISTINCT FROM transaction_timestamp() OR NEW.expires_at IS DISTINCT FROM transaction_timestamp()+interval '15 minutes' OR
     r_source IS NULL OR r_revision IS DISTINCT FROM NEW.source_revision_id OR r_source<>NEW.source_url_sha256 OR r_page<>NEW.source_page_url_sha256 OR r_updated<>NEW.source_updated_at OR
     current_revision IS DISTINCT FROM r_revision OR current_source<>r_source OR current_page<>r_page OR current_updated<>r_updated OR
     d_node<>NEW.node_id OR d_do<>NEW.do_droplet_id OR d_region<>NEW.region OR d_build<>NEW.probe_build_sha OR d_state<>'active' OR d_seen<transaction_timestamp()-interval '120 seconds' OR
     source_clean IS DISTINCT FROM true OR NEW.attempt_no<>expected_attempt OR (expected_attempt>1 AND prior_completed IS NULL) THEN RAISE EXCEPTION 'targeted attempt is not a fresh server-issued managed-recorder/source challenge'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_targeted_probe_attempt_validate BEFORE INSERT ON recording_targeted_probe_attempts FOR EACH ROW EXECUTE FUNCTION validate_recording_targeted_probe_attempt();

CREATE OR REPLACE FUNCTION validate_recording_targeted_probe_evidence()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; authorized BOOLEAN; a RECORD; source_clean BOOLEAN; current_revision BIGINT; current_source TEXT; current_page TEXT; current_updated TIMESTAMPTZ; native_text TEXT; native_hash TEXT; proof_hash TEXT; expected_evidence TEXT;
BEGIN
  EXECUTE format('SELECT * FROM %I.recording_targeted_probe_attempts WHERE id=$1 FOR UPDATE',s) INTO a USING NEW.attempt_id;
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_tx_authorizations WHERE transaction_id=txid_current() AND action=''evidence'' AND approval_id=$1 AND account_id=$2 AND node_id=$3)',s) INTO authorized USING NEW.approval_id,NEW.account_id,a.node_id;
  IF authorized IS DISTINCT FROM true THEN RAISE EXCEPTION 'targeted evidence requires typed node transaction authorization'; END IF;
  EXECUTE format('SELECT (SELECT max(id) FROM %I.stream_source_revisions WHERE stream_id=st.id),encode(sha256(convert_to(st.source_url,''UTF8'')),''hex''),encode(sha256(convert_to(COALESCE(st.source_page_url,''''),''UTF8'')),''hex''),st.updated_at FROM %I.streams st WHERE id=$1 AND deleted_at IS NULL FOR SHARE',s,s)
    INTO current_revision,current_source,current_page,current_updated USING NEW.stream_id;
  EXECUTE format('SELECT NOT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_source_fence_events f JOIN %I.recording_campaign_admission_reservations r ON r.stream_id=f.stream_id WHERE r.approval_id=$1 AND r.stream_id=$2 AND f.occurred_at>=r.reserved_at)',s,s) INTO source_clean USING NEW.approval_id,NEW.stream_id;
  native_text:=format(E'v1\nvideo=%s\naudio=%s\naudio_present=%s\nwidth=%s\nheight=%s\nfps=%s\n',lower(btrim(COALESCE(NEW.video_codec,''))),lower(btrim(COALESCE(NEW.audio_codec,''))),COALESCE(NEW.audio_present,false)::text,COALESCE(NEW.video_width,0)::text,COALESCE(NEW.video_height,0)::text,COALESCE(NEW.actual_fps::text,''));
  native_hash:=encode(sha256(convert_to(length(native_text)::text||':'||native_text,'UTF8')),'hex');
  proof_hash:=encode(sha256(convert_to(length(a.challenge)::text||':'||a.challenge||length(COALESCE(NEW.media_sha256,''))::text||':'||COALESCE(NEW.media_sha256,'')||length(COALESCE(NEW.frame_sha256,''))::text||':'||COALESCE(NEW.frame_sha256,'')||length(native_hash)::text||':'||native_hash,'UTF8')),'hex');
  expected_evidence:=encode(sha256(convert_to(jsonb_build_object('attempt_id',NEW.attempt_id,'approval_id',NEW.approval_id,'account_id',NEW.account_id,'stream_id',NEW.stream_id,'result',NEW.result,'valid_ratio',NEW.valid_ratio,'duration_ms',NEW.duration_ms,'segment_count',NEW.segment_count,'frame_sha256',NEW.frame_sha256,'media_sha256',NEW.media_sha256,'native_signature_sha256',native_hash,'challenge_proof_sha256',proof_hash,'video_codec',lower(btrim(COALESCE(NEW.video_codec,''))),'audio_codec',lower(btrim(COALESCE(NEW.audio_codec,''))),'audio_present',NEW.audio_present,'video_width',NEW.video_width,'video_height',NEW.video_height,'actual_fps',NEW.actual_fps,'detail',NEW.detail)::text,'UTF8')),'hex');
  IF a.id IS NULL OR a.approval_id<>NEW.approval_id OR a.account_id<>NEW.account_id OR a.stream_id<>NEW.stream_id OR transaction_timestamp()>a.expires_at OR NEW.observed_at IS DISTINCT FROM transaction_timestamp() OR
     source_clean IS DISTINCT FROM true OR current_revision IS DISTINCT FROM a.source_revision_id OR current_source<>a.source_url_sha256 OR current_page<>a.source_page_url_sha256 OR current_updated<>a.source_updated_at OR
     NEW.native_signature_sha256<>native_hash OR NEW.challenge_proof_sha256<>proof_hash THEN RAISE EXCEPTION 'targeted evidence is not bound to the server challenge and current source fence'; END IF;
  NEW.video_codec:=lower(btrim(COALESCE(NEW.video_codec,''))); NEW.audio_codec:=lower(btrim(COALESCE(NEW.audio_codec,''))); NEW.native_signature_sha256:=native_hash; NEW.challenge_proof_sha256:=proof_hash; NEW.evidence_sha256:=expected_evidence;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_targeted_probe_evidence_validate BEFORE INSERT ON recording_targeted_probe_evidence FOR EACH ROW EXECUTE FUNCTION validate_recording_targeted_probe_evidence();

CREATE OR REPLACE FUNCTION validate_recording_campaign_admission_result()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; authorized BOOLEAN; a_account BIGINT; a_actor BIGINT; a_schedule TEXT; a_deadline TIMESTAMPTZ; a_tag TEXT; r_stream BIGINT; r_recording BIGINT; r_revision BIGINT; r_source TEXT; r_page TEXT; r_updated TIMESTAMPTZ; r_scene BIGINT; r_scene_hash TEXT; source_clean BOOLEAN; current_revision BIGINT; current_source TEXT; current_page TEXT; current_updated TIMESTAMPTZ; current_tags TEXT[]; t_account BIGINT; e_recording BIGINT; e_stream BIGINT; e_actor BIGINT; e_scene TEXT; config_ok BOOLEAN; scene_fresh BOOLEAN; current_config_sha TEXT; p1 RECORD; p2 RECORD;
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
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_scene_frame_evidence WHERE id=$1 AND account_id=$2 AND stream_id=$3 AND scene_identity_sha256=$4 AND verified_at BETWEEN transaction_timestamp()-interval ''6 hours'' AND transaction_timestamp())',s)
    INTO scene_fresh USING r_scene,NEW.account_id,NEW.stream_id,r_scene_hash;
  EXECUTE format('SELECT e.approval_id,e.account_id,e.stream_id,e.result,e.observed_at,e.frame_sha256,e.native_signature_sha256,a.source_revision_id,a.source_url_sha256,a.source_page_url_sha256,a.source_updated_at,a.challenge FROM %I.recording_targeted_probe_evidence e JOIN %I.recording_targeted_probe_attempts a ON a.id=e.attempt_id WHERE e.id=$1',s,s) INTO p1 USING NEW.first_probe_evidence_id;
  EXECUTE format('SELECT e.approval_id,e.account_id,e.stream_id,e.result,e.observed_at,e.frame_sha256,e.native_signature_sha256,a.source_revision_id,a.source_url_sha256,a.source_page_url_sha256,a.source_updated_at,a.challenge FROM %I.recording_targeted_probe_evidence e JOIN %I.recording_targeted_probe_attempts a ON a.id=e.attempt_id WHERE e.id=$1',s,s) INTO p2 USING NEW.second_probe_evidence_id;
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_approvals a JOIN %I.recordings r ON r.id=$2 JOIN %I.streams st ON st.id=r.stream_id WHERE a.id=$1 AND r.account_id=a.account_id AND r.stream_id=$3 AND r.name=st.name||'' [''||st.id::text||'']'' AND r.stream_url=st.source_url AND r.source_kind=CASE WHEN lower(st.source_url) LIKE ''%%.m3u8%%'' OR lower(st.source_url) LIKE ''%%!hls%%'' THEN ''hls_live'' ELSE ''ffmpeg_direct'' END AND r.status=''active'' AND r.paused_at IS NULL AND r.capture_via=''cloud'' AND r.target_fps IS NULL AND r.mode=a.schedule_spec->>''mode'' AND COALESCE(r.cron_expr,'''')=COALESCE(a.schedule_spec->>''cron_expr'','''') AND r.cron_timezone=(SELECT value->>''timezone'' FROM jsonb_array_elements(a.schedule_spec->''stream_timezones'') value WHERE (value->>''stream_id'')::bigint=$3) AND r.clip_duration_sec=(a.schedule_spec->>''clip_duration_sec'')::int AND COALESCE(to_char(r.daily_window_start,''HH24:MI''),'''')=COALESCE(a.schedule_spec->>''daily_window_start'','''') AND COALESCE(to_char(r.daily_window_end,''HH24:MI''),'''')=COALESCE(a.schedule_spec->>''daily_window_end'','''') AND to_jsonb(r.active_weekdays)=a.schedule_spec->''active_weekdays'' AND r.start_at=(a.schedule_spec->>''start_at'')::timestamptz AND r.end_at=(a.schedule_spec->>''end_at'')::timestamptz AND ((a.schedule_spec->>''storage_destination_id'')::bigint>0 AND r.storage_destination_id=(a.schedule_spec->>''storage_destination_id'')::bigint AND r.delivery_storage_destination_id IS NULL OR (a.schedule_spec->>''delivery_storage_destination_id'')::bigint>0 AND r.delivery_storage_destination_id=(a.schedule_spec->>''delivery_storage_destination_id'')::bigint AND r.storage_destination_id IS NULL) AND r.delivery=a.schedule_spec->>''delivery'' AND r.naming_profile=a.schedule_spec->>''naming_profile'' AND r.folder_name=''recordings'' AND r.naming_metadata_jsonb=''{}''::jsonb AND r.storage_retention_tier=''monthly'')',s,s,s)
    INTO config_ok USING NEW.approval_id,NEW.recording_id,NEW.stream_id;
  EXECUTE format('SELECT encode(sha256(convert_to(jsonb_build_object(''account_id'',r.account_id,''stream_id'',r.stream_id,''name'',r.name,''stream_url'',r.stream_url,''source_kind'',r.source_kind,''mode'',r.mode,''cron_expr'',r.cron_expr,''cron_timezone'',r.cron_timezone,''clip_duration_sec'',r.clip_duration_sec,''daily_window_start'',r.daily_window_start,''daily_window_end'',r.daily_window_end,''active_weekdays'',r.active_weekdays,''target_fps'',r.target_fps,''start_at'',r.start_at,''end_at'',r.end_at,''storage_destination_id'',r.storage_destination_id,''delivery_storage_destination_id'',r.delivery_storage_destination_id,''delivery'',r.delivery,''capture_via'',r.capture_via,''naming_profile'',r.naming_profile,''folder_name'',r.folder_name,''naming_metadata_jsonb'',r.naming_metadata_jsonb,''storage_retention_tier'',r.storage_retention_tier)::text,''UTF8'')),''hex'') FROM %I.recordings r WHERE id=$1',s)
    INTO current_config_sha USING NEW.recording_id;
  IF a_account<>NEW.account_id OR a_schedule<>NEW.schedule_sha256 OR transaction_timestamp()>=a_deadline OR t_account<>NEW.account_id OR
     a_actor<>NEW.actor_user_id OR config_ok IS DISTINCT FROM true OR scene_fresh IS DISTINCT FROM true OR source_clean IS DISTINCT FROM true OR
     r_stream<>NEW.stream_id OR (r_recording IS NOT NULL AND r_recording<>NEW.recording_id) OR e_recording<>NEW.recording_id OR e_stream<>NEW.stream_id OR e_actor<>NEW.actor_user_id OR e_scene<>r_scene_hash OR
     current_revision IS DISTINCT FROM r_revision OR current_source<>r_source OR current_page<>r_page OR current_updated<>r_updated OR (a_tag IS NOT NULL AND NOT(a_tag=ANY(current_tags))) OR
     p1.approval_id<>NEW.approval_id OR p2.approval_id<>NEW.approval_id OR p1.account_id<>NEW.account_id OR p2.account_id<>NEW.account_id OR
     p1.stream_id<>NEW.stream_id OR p2.stream_id<>NEW.stream_id OR p1.result<>'ok' OR p2.result<>'ok' OR
     p2.observed_at<=p1.observed_at OR p1.observed_at<transaction_timestamp()-interval '6 hours' OR p2.observed_at<transaction_timestamp()-interval '6 hours' OR
     p1.challenge=p2.challenge OR
     (p1.native_signature_sha256,p1.source_revision_id,p1.source_url_sha256,p1.source_page_url_sha256,p1.source_updated_at) IS DISTINCT FROM
     (p2.native_signature_sha256,p2.source_revision_id,p2.source_url_sha256,p2.source_page_url_sha256,p2.source_updated_at) OR
     (p2.source_revision_id,p2.source_url_sha256,p2.source_page_url_sha256,p2.source_updated_at) IS DISTINCT FROM
     (r_revision,r_source,r_page,r_updated) THEN RAISE EXCEPTION 'campaign admission result evidence/binding mismatch'; END IF;
  NEW.recording_config_sha256:=current_config_sha;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_admission_result_validate BEFORE INSERT ON recording_campaign_admission_results FOR EACH ROW EXECUTE FUNCTION validate_recording_campaign_admission_result();

CREATE OR REPLACE FUNCTION guard_reserved_completed_recording_activation()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; approval UUID; authorized BOOLEAN;
BEGIN
  IF NEW.status='active' AND (TG_OP='INSERT' OR OLD.status<>'active') THEN
    EXECUTE format('SELECT 1 FROM %I.accounts WHERE id=$1 FOR UPDATE',s) USING NEW.account_id;
    EXECUTE format('SELECT ar.approval_id FROM %I.recording_campaign_admission_reservations ar JOIN %I.recording_campaign_admission_approvals a ON a.id=ar.approval_id JOIN %I.streams candidate ON candidate.id=$3 LEFT JOIN %I.recording_campaign_admission_results admitted ON admitted.approval_id=ar.approval_id AND admitted.stream_id=ar.stream_id WHERE ar.account_id=$1 AND a.deadline_at>transaction_timestamp() AND admitted.id IS NULL AND (ar.recording_id=$2 OR ar.stream_id=$3 OR ar.source_url_sha256=encode(sha256(convert_to(candidate.source_url,''UTF8'')),''hex'')) ORDER BY ar.reserved_at DESC LIMIT 1',s,s,s,s,s) INTO approval USING NEW.account_id,NEW.id,NEW.stream_id;
    IF approval IS NOT NULL THEN
      EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_tx_authorizations WHERE transaction_id=txid_current() AND action=''admit'' AND approval_id=$1 AND account_id=$2)',s) INTO authorized USING approval,NEW.account_id;
      IF authorized IS DISTINCT FROM true THEN RAISE EXCEPTION 'reserved recording/stream requires typed campaign admission'; END IF;
    END IF;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_admission_activation_guard BEFORE INSERT OR UPDATE ON recordings FOR EACH ROW EXECUTE FUNCTION guard_reserved_completed_recording_activation();

CREATE OR REPLACE FUNCTION enforce_reserved_activation_has_result()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; approval UUID; sealed BOOLEAN;
BEGIN
  IF NEW.status='active' AND (TG_OP='INSERT' OR OLD.status<>'active') THEN
    EXECUTE format('SELECT ar.approval_id FROM %I.recording_campaign_admission_reservations ar JOIN %I.recording_campaign_admission_approvals a ON a.id=ar.approval_id JOIN %I.streams candidate ON candidate.id=$3 WHERE ar.account_id=$1 AND a.deadline_at>transaction_timestamp() AND (ar.recording_id=$2 OR ar.stream_id=$3 OR ar.source_url_sha256=encode(sha256(convert_to(candidate.source_url,''UTF8'')),''hex'')) ORDER BY ar.reserved_at DESC LIMIT 1',s,s,s) INTO approval USING NEW.account_id,NEW.id,NEW.stream_id;
    IF approval IS NOT NULL THEN
      EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_results WHERE approval_id=$1 AND recording_id=$2)',s) INTO sealed USING approval,NEW.id;
      IF sealed IS DISTINCT FROM true THEN RAISE EXCEPTION 'reserved activation must commit its immutable admission result'; END IF;
    END IF;
  END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER recording_campaign_admission_activation_seal AFTER INSERT OR UPDATE ON recordings DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION enforce_reserved_activation_has_result();

CREATE OR REPLACE FUNCTION enforce_admitted_recording_inverse_seal()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; rid BIGINT:=CASE WHEN TG_OP='DELETE' THEN OLD.id ELSE NEW.id END; expected TEXT; actual TEXT; bound BOOLEAN; lifecycle_ok BOOLEAN;
BEGIN
  EXECUTE format('SELECT recording_config_sha256 FROM %I.recording_campaign_admission_results WHERE recording_id=$1',s) INTO expected USING rid;
  IF expected IS NULL THEN RETURN NULL; END IF;
  IF TG_OP='DELETE' THEN RAISE EXCEPTION 'admitted recording is immutable without a typed release'; END IF;
  EXECUTE format('SELECT encode(sha256(convert_to(jsonb_build_object(''account_id'',r.account_id,''stream_id'',r.stream_id,''name'',r.name,''stream_url'',r.stream_url,''source_kind'',r.source_kind,''mode'',r.mode,''cron_expr'',r.cron_expr,''cron_timezone'',r.cron_timezone,''clip_duration_sec'',r.clip_duration_sec,''daily_window_start'',r.daily_window_start,''daily_window_end'',r.daily_window_end,''active_weekdays'',r.active_weekdays,''target_fps'',r.target_fps,''start_at'',r.start_at,''end_at'',r.end_at,''storage_destination_id'',r.storage_destination_id,''delivery_storage_destination_id'',r.delivery_storage_destination_id,''delivery'',r.delivery,''capture_via'',r.capture_via,''naming_profile'',r.naming_profile,''folder_name'',r.folder_name,''naming_metadata_jsonb'',r.naming_metadata_jsonb,''storage_retention_tier'',r.storage_retention_tier)::text,''UTF8'')),''hex''),r.paused_at IS NULL AND (r.status=''active'' OR (r.status=''completed'' AND r.end_at<=transaction_timestamp())) FROM %I.recordings r WHERE id=$1',s)
    INTO actual,lifecycle_ok USING rid;
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_results ar JOIN %I.recording_campaign_tracks t ON t.id=ar.track_id JOIN %I.recording_campaign_roster_entries e ON e.id=ar.roster_entry_id AND e.track_id=t.id WHERE ar.recording_id=$1 AND t.state=''active'' AND e.recording_id=$1 AND e.stream_id=ar.stream_id AND e.scene_identity_sha256=(SELECT scene_identity_sha256 FROM %I.recording_campaign_admission_reservations WHERE approval_id=ar.approval_id AND stream_id=ar.stream_id) AND e.status IN(''protect'',''probation''))',s,s,s,s)
    INTO bound USING rid;
  IF actual<>expected OR lifecycle_ok IS DISTINCT FROM true OR bound IS DISTINCT FROM true THEN RAISE EXCEPTION 'admitted recording/config/lifecycle/roster inverse seal mismatch'; END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER recording_campaign_admission_recording_inverse AFTER INSERT OR UPDATE OR DELETE ON recordings DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION enforce_admitted_recording_inverse_seal();

CREATE OR REPLACE FUNCTION guard_reserved_campaign_roster_occupancy()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; account BIGINT; approval UUID; authorized BOOLEAN;
BEGIN
  IF NEW.status NOT IN('protect','probation') THEN RETURN NEW; END IF;
  EXECUTE format('SELECT account_id FROM %I.recording_campaign_tracks WHERE id=$1',s) INTO account USING NEW.track_id;
  EXECUTE format('SELECT 1 FROM %I.accounts WHERE id=$1 FOR UPDATE',s) USING account;
  EXECUTE format('SELECT ar.approval_id FROM %I.recording_campaign_admission_reservations ar JOIN %I.recording_campaign_admission_approvals a ON a.id=ar.approval_id JOIN %I.streams candidate ON candidate.id=$2 LEFT JOIN %I.recording_campaign_admission_results admitted ON admitted.approval_id=ar.approval_id AND admitted.stream_id=ar.stream_id WHERE ar.account_id=$1 AND a.deadline_at>transaction_timestamp() AND admitted.id IS NULL AND (ar.stream_id=$2 OR ar.scene_identity_sha256=$3 OR ar.source_url_sha256=encode(sha256(convert_to(candidate.source_url,''UTF8'')),''hex'')) ORDER BY ar.reserved_at DESC LIMIT 1',s,s,s,s) INTO approval USING account,NEW.stream_id,NEW.scene_identity_sha256;
  IF approval IS NOT NULL THEN
    EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_tx_authorizations WHERE transaction_id=txid_current() AND action=''admit'' AND approval_id=$1 AND account_id=$2)',s) INTO authorized USING approval,account;
    IF authorized IS DISTINCT FROM true THEN RAISE EXCEPTION 'reserved stream/scene requires typed campaign admission roster'; END IF;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_campaign_admission_roster_guard BEFORE INSERT OR UPDATE ON recording_campaign_roster_entries FOR EACH ROW EXECUTE FUNCTION guard_reserved_campaign_roster_occupancy();

CREATE OR REPLACE FUNCTION enforce_admitted_roster_inverse_seal()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; eid BIGINT:=CASE WHEN TG_OP='DELETE' THEN OLD.id ELSE NEW.id END; admitted BOOLEAN; bound BOOLEAN;
BEGIN
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_results WHERE roster_entry_id=$1)',s) INTO admitted USING eid;
  IF admitted IS DISTINCT FROM true THEN RETURN NULL; END IF;
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_results ar JOIN %I.recording_campaign_tracks t ON t.id=ar.track_id JOIN %I.recording_campaign_roster_entries e ON e.id=ar.roster_entry_id AND e.track_id=t.id JOIN %I.recording_campaign_admission_reservations r ON r.approval_id=ar.approval_id AND r.stream_id=ar.stream_id WHERE ar.roster_entry_id=$1 AND t.state=''active'' AND e.recording_id=ar.recording_id AND e.stream_id=ar.stream_id AND e.scene_identity_sha256=r.scene_identity_sha256 AND e.status IN(''protect'',''probation''))',s,s,s,s)
    INTO bound USING eid;
  IF bound IS DISTINCT FROM true THEN RAISE EXCEPTION 'admitted roster entry is immutable without a typed release'; END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER recording_campaign_admission_roster_inverse AFTER UPDATE OR DELETE ON recording_campaign_roster_entries DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION enforce_admitted_roster_inverse_seal();

CREATE OR REPLACE FUNCTION enforce_admitted_track_inverse_seal()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; tid BIGINT:=CASE WHEN TG_OP='DELETE' THEN OLD.id ELSE NEW.id END; admitted BOOLEAN; active BOOLEAN;
BEGIN
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_results WHERE track_id=$1)',s) INTO admitted USING tid;
  IF admitted IS DISTINCT FROM true THEN RETURN NULL; END IF;
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_tracks WHERE id=$1 AND (state=''active'' OR (state=''complete'' AND deadline_at<=transaction_timestamp())))',s) INTO active USING tid;
  IF active IS DISTINCT FROM true THEN RAISE EXCEPTION 'admitted campaign track lifecycle is immutable before its deadline without a typed release'; END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER recording_campaign_admission_track_inverse AFTER UPDATE OR DELETE ON recording_campaign_tracks DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION enforce_admitted_track_inverse_seal();

CREATE OR REPLACE FUNCTION enforce_admitted_stream_inverse_seal()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; sid BIGINT:=CASE WHEN TG_OP='DELETE' THEN OLD.id ELSE NEW.id END; admitted BOOLEAN; bound BOOLEAN;
BEGIN
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_results WHERE stream_id=$1)',s) INTO admitted USING sid;
  IF admitted IS DISTINCT FROM true THEN RETURN NULL; END IF;
  IF TG_OP='DELETE' THEN RAISE EXCEPTION 'admitted stream is immutable without a typed release'; END IF;
  EXECUTE format('SELECT bool_and((SELECT max(id) FROM %I.stream_source_revisions WHERE stream_id=st.id) IS NOT DISTINCT FROM r.source_revision_id AND encode(sha256(convert_to(st.source_url,''UTF8'')),''hex'')=r.source_url_sha256 AND encode(sha256(convert_to(COALESCE(st.source_page_url,''''),''UTF8'')),''hex'')=r.source_page_url_sha256 AND st.updated_at=r.source_updated_at AND (a.failure_domain_tag IS NULL OR a.failure_domain_tag=ANY(st.tags))) FROM %I.recording_campaign_admission_results ar JOIN %I.recording_campaign_admission_reservations r ON r.approval_id=ar.approval_id AND r.stream_id=ar.stream_id JOIN %I.recording_campaign_admission_approvals a ON a.id=ar.approval_id JOIN %I.streams st ON st.id=ar.stream_id WHERE ar.stream_id=$1',s,s,s,s,s)
    INTO bound USING sid;
  IF bound IS DISTINCT FROM true THEN RAISE EXCEPTION 'admitted stream source/FD inverse seal mismatch'; END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER recording_campaign_admission_stream_inverse AFTER UPDATE OR DELETE ON streams DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION enforce_admitted_stream_inverse_seal();

CREATE OR REPLACE FUNCTION enforce_admitted_revision_inverse_seal()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE s TEXT:=TG_TABLE_SCHEMA; sid BIGINT:=CASE WHEN TG_OP='DELETE' THEN OLD.stream_id ELSE NEW.stream_id END; admitted BOOLEAN; bound BOOLEAN;
BEGIN
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.recording_campaign_admission_results WHERE stream_id=$1)',s) INTO admitted USING sid;
  IF admitted IS DISTINCT FROM true THEN RETURN NULL; END IF;
  EXECUTE format('SELECT bool_and((SELECT max(id) FROM %I.stream_source_revisions WHERE stream_id=$1) IS NOT DISTINCT FROM r.source_revision_id) FROM %I.recording_campaign_admission_results ar JOIN %I.recording_campaign_admission_reservations r ON r.approval_id=ar.approval_id AND r.stream_id=ar.stream_id WHERE ar.stream_id=$1',s,s,s)
    INTO bound USING sid;
  IF bound IS DISTINCT FROM true THEN RAISE EXCEPTION 'admitted stream source revision inverse seal mismatch'; END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER recording_campaign_admission_revision_inverse AFTER INSERT OR UPDATE OR DELETE ON stream_source_revisions DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION enforce_admitted_revision_inverse_seal();

CREATE OR REPLACE FUNCTION reject_campaign_admission_evidence_mutation() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN RAISE EXCEPTION 'campaign admission evidence is append-only'; END $$;
CREATE TRIGGER recording_campaign_admission_approvals_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_campaign_admission_approvals FOR EACH STATEMENT EXECUTE FUNCTION reject_campaign_admission_evidence_mutation();
CREATE TRIGGER recording_campaign_admission_reservations_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_campaign_admission_reservations FOR EACH STATEMENT EXECUTE FUNCTION reject_campaign_admission_evidence_mutation();
CREATE TRIGGER recording_targeted_probe_attempts_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_targeted_probe_attempts FOR EACH STATEMENT EXECUTE FUNCTION reject_campaign_admission_evidence_mutation();
CREATE TRIGGER recording_targeted_probe_evidence_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_targeted_probe_evidence FOR EACH STATEMENT EXECUTE FUNCTION reject_campaign_admission_evidence_mutation();
CREATE TRIGGER recording_campaign_admission_results_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_campaign_admission_results FOR EACH STATEMENT EXECUTE FUNCTION reject_campaign_admission_evidence_mutation();
CREATE TRIGGER recording_campaign_admission_commits_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_campaign_admission_commits FOR EACH STATEMENT EXECUTE FUNCTION reject_campaign_admission_evidence_mutation();
CREATE TRIGGER recording_campaign_admission_authorizations_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_campaign_admission_tx_authorizations FOR EACH STATEMENT EXECUTE FUNCTION reject_campaign_admission_evidence_mutation();
CREATE TRIGGER recording_campaign_admission_source_fence_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_campaign_admission_source_fence_events FOR EACH STATEMENT EXECUTE FUNCTION reject_campaign_admission_evidence_mutation();

REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON recording_campaign_admission_approvals,recording_campaign_admission_reservations,recording_targeted_probe_attempts,recording_targeted_probe_evidence,recording_campaign_admission_results,recording_campaign_admission_commits,recording_campaign_admission_tx_authorizations,recording_campaign_admission_source_fence_events FROM PUBLIC;
