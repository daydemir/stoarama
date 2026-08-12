-- Durable campaign rosters. The current row is operational truth; every change
-- is also appended to recording_campaign_roster_events for audit/history.
CREATE TABLE recording_campaign_tracks (
  id BIGSERIAL PRIMARY KEY,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  campaign_key TEXT NOT NULL,
  label TEXT NOT NULL,
  deadline_at TIMESTAMPTZ NOT NULL,
  target_count INTEGER NOT NULL CHECK(target_count>0),
  grade_floor TEXT NOT NULL CHECK(grade_floor IN ('GOOD','GREAT')),
  required_consecutive_windows INTEGER NOT NULL DEFAULT 0 CHECK(required_consecutive_windows BETWEEN 0 AND 14),
  state TEXT NOT NULL DEFAULT 'active' CHECK(state IN ('draft','active','complete','retired')),
  created_by_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(account_id,campaign_key)
);
CREATE TABLE recording_campaign_track_events (
  id BIGSERIAL PRIMARY KEY,
  track_id BIGINT NOT NULL REFERENCES recording_campaign_tracks(id) ON DELETE RESTRICT,
  from_state TEXT,
  to_state TEXT NOT NULL CHECK(to_state IN ('draft','active','complete','retired')),
  reason_codes TEXT[] NOT NULL,
  actor_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
  decided_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE recording_campaign_roster_entries (
  id BIGSERIAL PRIMARY KEY,
  track_id BIGINT NOT NULL REFERENCES recording_campaign_tracks(id) ON DELETE RESTRICT,
  recording_id BIGINT NOT NULL REFERENCES recordings(id) ON DELETE RESTRICT,
  stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE RESTRICT,
  scene_identity_sha256 TEXT NOT NULL CHECK(scene_identity_sha256~'^[0-9a-f]{64}$'),
  role TEXT NOT NULL CHECK(role IN ('primary','backup','repair','reserve')),
  rank INTEGER NOT NULL CHECK(rank>0),
  status TEXT NOT NULL CHECK(status IN ('protect','probation','replace','removed')),
  reason_codes TEXT[] NOT NULL DEFAULT '{}',
  effective_at TIMESTAMPTZ NOT NULL,
  decision_at TIMESTAMPTZ NOT NULL,
  evidence_observed_at TIMESTAMPTZ NOT NULL,
  evidence_sha256 TEXT NOT NULL CHECK(evidence_sha256~'^[0-9a-f]{64}$'),
  source_window_end_at TIMESTAMPTZ,
  source_job_id BIGINT REFERENCES recording_jobs(id) ON DELETE RESTRICT,
  source_health_recording_id BIGINT,
  source_health_job_id BIGINT,
  updated_by_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(track_id,recording_id),
  UNIQUE(track_id,rank),
  UNIQUE(track_id,scene_identity_sha256),
  CHECK(cardinality(reason_codes)<=16)
);
CREATE INDEX idx_campaign_roster_protection ON recording_campaign_roster_entries(recording_id,track_id)
  WHERE status IN ('protect','probation');

CREATE TABLE recording_campaign_roster_events (
  id BIGSERIAL PRIMARY KEY,
  track_id BIGINT NOT NULL REFERENCES recording_campaign_tracks(id) ON DELETE RESTRICT,
  roster_entry_id BIGINT NOT NULL REFERENCES recording_campaign_roster_entries(id) ON DELETE RESTRICT,
  recording_id BIGINT NOT NULL REFERENCES recordings(id) ON DELETE RESTRICT,
  stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE RESTRICT,
  event_type TEXT NOT NULL CHECK(event_type IN ('added','decision','ranked','role_changed','status_changed')),
  role TEXT NOT NULL CHECK(role IN ('primary','backup','repair','reserve')),
  rank INTEGER NOT NULL CHECK(rank>0),
  status TEXT NOT NULL CHECK(status IN ('protect','probation','replace','removed')),
  reason_codes TEXT[] NOT NULL DEFAULT '{}',
  decision_at TIMESTAMPTZ NOT NULL,
  evidence_observed_at TIMESTAMPTZ NOT NULL,
  evidence_sha256 TEXT NOT NULL CHECK(evidence_sha256~'^[0-9a-f]{64}$'),
  actor_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
  source_window_end_at TIMESTAMPTZ,
  source_job_id BIGINT REFERENCES recording_jobs(id) ON DELETE RESTRICT,
  source_health_recording_id BIGINT,
  source_health_job_id BIGINT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_campaign_roster_events_track ON recording_campaign_roster_events(track_id,id DESC);

-- Stable cleanup/admission predicate: active campaign material is protected
-- independently of the frozen qualification cohort.
CREATE VIEW protected_campaign_recordings AS
SELECT DISTINCT t.account_id,e.recording_id
FROM recording_campaign_tracks t JOIN recording_campaign_roster_entries e ON e.track_id=t.id
WHERE t.state IN ('active','complete') AND e.status IN ('protect','probation');

CREATE FUNCTION validate_recording_campaign_roster_entry() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expected_stream BIGINT; expected_account BIGINT; track_account BIGINT; authorized BOOLEAN;
BEGIN
  IF TG_OP='UPDATE' AND (NEW.track_id,NEW.recording_id,NEW.stream_id,NEW.scene_identity_sha256)
    IS DISTINCT FROM (OLD.track_id,OLD.recording_id,OLD.stream_id,OLD.scene_identity_sha256) THEN
    RAISE EXCEPTION 'campaign roster recording/scene binding is immutable';
  END IF;
  SELECT account_id,stream_id INTO expected_account,expected_stream FROM recordings WHERE id=NEW.recording_id;
  SELECT account_id INTO track_account FROM recording_campaign_tracks WHERE id=NEW.track_id;
  IF expected_stream IS NULL OR expected_stream<>NEW.stream_id OR expected_account<>track_account THEN
    RAISE EXCEPTION 'campaign roster recording/stream/account binding mismatch';
  END IF;
  SELECT (u.is_operator OR EXISTS(SELECT 1 FROM memberships m WHERE m.user_id=u.id AND m.org_id=track_account AND m.accepted_at IS NOT NULL AND m.role IN ('owner','admin'))) INTO authorized FROM users u WHERE u.id=NEW.updated_by_user_id;
  IF authorized IS DISTINCT FROM true THEN RAISE EXCEPTION 'campaign decision actor is not an account owner/operator'; END IF;
  NEW.updated_at=now();
  RETURN NEW;
END $$;
CREATE TRIGGER trg_recording_campaign_roster_validate BEFORE INSERT OR UPDATE ON recording_campaign_roster_entries
FOR EACH ROW EXECUTE FUNCTION validate_recording_campaign_roster_entry();
CREATE FUNCTION reject_recording_campaign_roster_delete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'campaign roster entries use removed status and cannot be deleted'; END $$;
CREATE TRIGGER trg_recording_campaign_roster_no_delete BEFORE DELETE ON recording_campaign_roster_entries
FOR EACH ROW EXECUTE FUNCTION reject_recording_campaign_roster_delete();

CREATE FUNCTION audit_recording_campaign_roster_entry() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE kind TEXT;
BEGIN
  IF TG_OP='INSERT' THEN kind := 'added';
  ELSIF NEW.status IS DISTINCT FROM OLD.status THEN kind := 'status_changed';
  ELSIF NEW.role IS DISTINCT FROM OLD.role THEN kind := 'role_changed';
  ELSIF NEW.rank IS DISTINCT FROM OLD.rank THEN kind := 'ranked';
  ELSE kind := 'decision'; END IF;
  INSERT INTO recording_campaign_roster_events(track_id,roster_entry_id,recording_id,stream_id,event_type,role,rank,status,reason_codes,decision_at,evidence_observed_at,evidence_sha256,actor_user_id,source_window_end_at,source_job_id,source_health_recording_id,source_health_job_id)
  VALUES(NEW.track_id,NEW.id,NEW.recording_id,NEW.stream_id,kind,NEW.role,NEW.rank,NEW.status,NEW.reason_codes,NEW.decision_at,NEW.evidence_observed_at,NEW.evidence_sha256,NEW.updated_by_user_id,NEW.source_window_end_at,NEW.source_job_id,NEW.source_health_recording_id,NEW.source_health_job_id);
  RETURN NEW;
END $$;
CREATE TRIGGER trg_recording_campaign_roster_audit AFTER INSERT OR UPDATE ON recording_campaign_roster_entries
FOR EACH ROW EXECUTE FUNCTION audit_recording_campaign_roster_entry();

CREATE FUNCTION reject_recording_campaign_roster_event_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'campaign roster events are append-only'; END $$;
CREATE TRIGGER trg_campaign_roster_events_append_only BEFORE UPDATE OR DELETE ON recording_campaign_roster_events
FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_campaign_roster_event_mutation();
CREATE TRIGGER trg_campaign_roster_events_no_truncate BEFORE TRUNCATE ON recording_campaign_roster_events
FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_campaign_roster_event_mutation();
CREATE TRIGGER trg_campaign_track_events_append_only BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_campaign_track_events
FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_campaign_roster_event_mutation();

CREATE FUNCTION transition_recording_campaign_track(p_track BIGINT,p_to TEXT,p_reasons TEXT[],p_actor BIGINT,p_decided TIMESTAMPTZ) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE old_state TEXT; expected_count INTEGER; actual_count INTEGER; track_account BIGINT; authorized BOOLEAN;
BEGIN
 SELECT state,target_count,account_id INTO old_state,expected_count,track_account FROM recording_campaign_tracks WHERE id=p_track FOR UPDATE;
 IF old_state IS NULL OR p_decided IS NULL OR cardinality(p_reasons)=0 THEN RAISE EXCEPTION 'track transition requires track, reasons, and decision time'; END IF;
 IF NOT ((old_state='draft' AND p_to='active') OR (old_state='active' AND p_to='complete') OR (old_state IN ('draft','active','complete') AND p_to='retired')) THEN RAISE EXCEPTION 'invalid campaign track transition'; END IF;
 SELECT (u.is_operator OR EXISTS(SELECT 1 FROM memberships m WHERE m.user_id=u.id AND m.org_id=track_account AND m.accepted_at IS NOT NULL AND m.role IN ('owner','admin'))) INTO authorized FROM users u WHERE u.id=p_actor;
 IF authorized IS DISTINCT FROM true THEN RAISE EXCEPTION 'campaign transition actor is not an account owner/operator'; END IF;
 IF p_to='active' THEN
   SELECT count(*) INTO actual_count FROM recording_campaign_roster_entries WHERE track_id=p_track AND role='primary' AND status IN ('protect','probation');
   IF actual_count<>expected_count THEN RAISE EXCEPTION 'active track requires exact target primary roster'; END IF;
 END IF;
 PERFORM set_config('stoarama.campaign_transition','1',true);
 UPDATE recording_campaign_tracks SET state=p_to,updated_at=now() WHERE id=p_track;
 PERFORM set_config('stoarama.campaign_transition','0',true);
 INSERT INTO recording_campaign_track_events(track_id,from_state,to_state,reason_codes,actor_user_id,decided_at) VALUES(p_track,old_state,p_to,p_reasons,p_actor,p_decided);
END $$;

CREATE FUNCTION guard_recording_campaign_track_state() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.state IS DISTINCT FROM OLD.state AND current_setting('stoarama.campaign_transition',true) IS DISTINCT FROM '1' THEN RAISE EXCEPTION 'use transition_recording_campaign_track'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER trg_campaign_track_state_guard BEFORE UPDATE ON recording_campaign_tracks FOR EACH ROW EXECUTE FUNCTION guard_recording_campaign_track_state();

CREATE INDEX idx_recording_clips_job_created ON recording_clips(recording_job_id,created_at DESC);
