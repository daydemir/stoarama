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
  updated_by_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(track_id,recording_id),
  UNIQUE(track_id,rank),
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
WHERE t.state='active' AND e.status IN ('protect','probation');

CREATE FUNCTION validate_recording_campaign_roster_entry() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expected_stream BIGINT; expected_account BIGINT; track_account BIGINT;
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
  INSERT INTO recording_campaign_roster_events(track_id,roster_entry_id,recording_id,stream_id,event_type,role,rank,status,reason_codes,decision_at,actor_user_id)
  VALUES(NEW.track_id,NEW.id,NEW.recording_id,NEW.stream_id,kind,NEW.role,NEW.rank,NEW.status,NEW.reason_codes,NEW.decision_at,NEW.updated_by_user_id);
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
