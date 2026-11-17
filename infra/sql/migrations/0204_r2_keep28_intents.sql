-- B2 independent keep28 RAW history. No activation, custody refresh or deletion.
CREATE TABLE r2_keep28_intents (
 bucket text NOT NULL, object_key text NOT NULL,
 endpoint_sha256 text NOT NULL CHECK(endpoint_sha256 ~ '^[a-f0-9]{64}$'),
 stream_id bigint NOT NULL CHECK(stream_id>0),
 source_id bigint NOT NULL CHECK(source_id>0),
 size_bytes bigint NOT NULL CHECK(size_bytes>0),
 etag text NOT NULL CHECK(etag<>''), last_modified timestamptz NOT NULL,
 manifest_sha256 text NOT NULL CHECK(manifest_sha256 ~ '^[a-f0-9]{64}$'),
 policy_sha256 text NOT NULL CHECK(policy_sha256='060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811'),
 picks_sha256 text NOT NULL CHECK(picks_sha256 ~ '^[a-f0-9]{64}$'),
 drain_evidence_sha256 text NOT NULL CHECK(drain_evidence_sha256 ~ '^[a-f0-9]{64}$'),
	choice text NOT NULL CHECK(choice IN ('none','st','keep')),
 custody jsonb NOT NULL CHECK(jsonb_typeof(custody)='object'),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bucket,object_key), UNIQUE(source_id)
);
CREATE INDEX r2_keep28_intents_created ON r2_keep28_intents(created_at);
CREATE TABLE r2_keep28_events (
 bucket text NOT NULL,object_key text NOT NULL,event text NOT NULL CHECK(event='absent'),
 observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bucket,object_key,event),
 FOREIGN KEY(bucket,object_key) REFERENCES r2_keep28_intents(bucket,object_key)
);
CREATE TRIGGER r2_keep28_intent_immutable BEFORE UPDATE OR DELETE ON r2_keep28_intents
 FOR EACH ROW EXECUTE FUNCTION guard_r2_object_retirement();
CREATE TRIGGER r2_keep28_event_immutable BEFORE UPDATE OR DELETE ON r2_keep28_events
 FOR EACH ROW EXECUTE FUNCTION guard_r2_object_retirement();
-- A verifier may issue wire authority for an intent only once, across restarts.
CREATE TABLE r2_keep28_attempts (
 bucket text NOT NULL,object_key text NOT NULL,issued_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bucket,object_key),FOREIGN KEY(bucket,object_key) REFERENCES r2_keep28_intents(bucket,object_key)
);
CREATE TRIGGER r2_keep28_attempt_immutable BEFORE UPDATE OR DELETE ON r2_keep28_attempts
 FOR EACH ROW EXECUTE FUNCTION guard_r2_object_retirement();
CREATE TRIGGER r2_keep28_attempt_no_truncate BEFORE TRUNCATE ON r2_keep28_attempts
 FOR EACH STATEMENT EXECUTE FUNCTION guard_r2_object_retirement();


CREATE FUNCTION guard_r2_keep28_source_identity() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF EXISTS(SELECT 1 FROM r2_keep28_intents i WHERE i.source_id=OLD.id AND NOT EXISTS(SELECT 1 FROM r2_keep28_events e WHERE e.bucket=i.bucket AND e.object_key=i.object_key AND e.event='absent'))
 OR (TG_OP='UPDATE' AND EXISTS(SELECT 1 FROM r2_keep28_intents i WHERE i.bucket=NEW.bucket AND i.object_key=NEW.object_key AND NOT EXISTS(SELECT 1 FROM r2_keep28_events e WHERE e.bucket=i.bucket AND e.object_key=i.object_key AND e.event='absent'))) THEN
  RAISE EXCEPTION 'R2 inverse intent freezes source identity'; END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW;
END $$;
CREATE TRIGGER r2_keep28_source_identity BEFORE UPDATE OF id,recording_id,recording_job_id,bucket,object_key,etag,size_bytes,sha256,display_path,clip_start_at,clip_end_at OR DELETE ON recording_clips
 FOR EACH ROW EXECUTE FUNCTION guard_r2_keep28_source_identity();


CREATE TRIGGER r2_keep28_intents_no_truncate BEFORE TRUNCATE ON r2_keep28_intents FOR EACH STATEMENT EXECUTE FUNCTION guard_r2_object_retirement();
CREATE TRIGGER r2_keep28_events_no_truncate BEFORE TRUNCATE ON r2_keep28_events FOR EACH STATEMENT EXECUTE FUNCTION guard_r2_object_retirement();
