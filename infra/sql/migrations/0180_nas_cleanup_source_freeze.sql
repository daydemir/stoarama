-- 0179 remains reserved for the independent V85r stitch change. These
-- unreleased migrations replace the version-row fence entirely.
CREATE TABLE nas_cleanup_frozen_streams (
 plan_id bigint NOT NULL,
 stream_id bigint NOT NULL,
 generation bigserial NOT NULL,
 validated boolean NOT NULL DEFAULT false,
 PRIMARY KEY(plan_id,stream_id)
);
CREATE INDEX nas_cleanup_frozen_streams_stream ON nas_cleanup_frozen_streams(stream_id);
-- Deliberately no plan FK: freeze publication commits on a separate connection
-- while enable holds the plan row. An FK would wait on that very row lock.
CREATE TABLE nas_cleanup_rankings_cache (
 plan_id bigint NOT NULL REFERENCES nas_cleanup_plans(id),
 stream_id bigint NOT NULL,
 freeze_generation bigint NOT NULL,
 best_start text NOT NULL,
 fingerprint text NOT NULL,
 checked_at timestamptz NOT NULL,
 PRIMARY KEY(plan_id,stream_id)
);

CREATE FUNCTION guard_nas_cleanup_frozen_source() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE old_stream bigint; new_stream bigint;
BEGIN
 -- A transaction snapshot predating freeze publication must never bypass it.
 -- Source mutations use READ COMMITTED, whose volatile trigger queries see
 -- the committed freeze even when the outer DML statement started earlier.
 IF current_setting('transaction_isolation') <> 'read committed' THEN
  RAISE EXCEPTION 'NAS cleanup source writes require READ COMMITTED' USING ERRCODE='55000';
 END IF;
 IF TG_OP='TRUNCATE' THEN
  IF EXISTS(SELECT 1 FROM nas_cleanup_frozen_streams) THEN
   RAISE EXCEPTION 'NAS cleanup source streams are frozen' USING ERRCODE='55000';
  END IF;
  RETURN NULL;
 END IF;
 -- The sole UPDATE exemption is purged_at on clips. Full best-14 replay
 -- deliberately includes purged clips and never reads purged_at. Existing
 -- source-hold/retention guards still enforce recovery safety for this write.
 IF TG_TABLE_NAME='recording_clips' AND TG_OP='UPDATE' THEN
  IF (to_jsonb(OLD)-'purged_at') = (to_jsonb(NEW)-'purged_at') THEN RETURN NEW; END IF;
 END IF;
 IF TG_TABLE_NAME='streams' THEN
  IF TG_OP<>'INSERT' THEN old_stream=OLD.id; END IF;
  IF TG_OP<>'DELETE' THEN new_stream=NEW.id; END IF;
 ELSIF TG_TABLE_NAME='recordings' THEN
  IF TG_OP<>'INSERT' THEN old_stream=OLD.stream_id; END IF;
  IF TG_OP<>'DELETE' THEN new_stream=NEW.stream_id; END IF;
 ELSE
  IF TG_OP<>'INSERT' THEN SELECT stream_id INTO old_stream FROM recordings WHERE id=OLD.recording_id; END IF;
  IF TG_OP<>'DELETE' THEN SELECT stream_id INTO new_stream FROM recordings WHERE id=NEW.recording_id; END IF;
 END IF;
 IF EXISTS(SELECT 1 FROM nas_cleanup_frozen_streams WHERE stream_id IN (old_stream,new_stream)) THEN
  RAISE EXCEPTION 'NAS cleanup source stream is frozen (old %, new %)',old_stream,new_stream USING ERRCODE='55000';
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END $$;
-- Run before existing reference guards (which can take catalog row locks).
CREATE TRIGGER aaa_nas_cleanup_freeze BEFORE INSERT OR UPDATE OR DELETE ON recording_clips FOR EACH ROW EXECUTE FUNCTION guard_nas_cleanup_frozen_source();
CREATE TRIGGER aaa_nas_cleanup_freeze BEFORE INSERT OR UPDATE OR DELETE ON recording_collation_hours FOR EACH ROW EXECUTE FUNCTION guard_nas_cleanup_frozen_source();
CREATE TRIGGER aaa_nas_cleanup_freeze BEFORE INSERT OR UPDATE OR DELETE ON recordings FOR EACH ROW EXECUTE FUNCTION guard_nas_cleanup_frozen_source();
CREATE TRIGGER aaa_nas_cleanup_freeze BEFORE INSERT OR UPDATE OR DELETE ON streams FOR EACH ROW EXECUTE FUNCTION guard_nas_cleanup_frozen_source();
CREATE TRIGGER aaa_nas_cleanup_freeze_truncate BEFORE TRUNCATE ON recording_clips FOR EACH STATEMENT EXECUTE FUNCTION guard_nas_cleanup_frozen_source();
CREATE TRIGGER aaa_nas_cleanup_freeze_truncate BEFORE TRUNCATE ON recording_collation_hours FOR EACH STATEMENT EXECUTE FUNCTION guard_nas_cleanup_frozen_source();
CREATE TRIGGER aaa_nas_cleanup_freeze_truncate BEFORE TRUNCATE ON recordings FOR EACH STATEMENT EXECUTE FUNCTION guard_nas_cleanup_frozen_source();
CREATE TRIGGER aaa_nas_cleanup_freeze_truncate BEFORE TRUNCATE ON streams FOR EACH STATEMENT EXECUTE FUNCTION guard_nas_cleanup_frozen_source();

CREATE FUNCTION nas_cleanup_plan_freeze_lifecycle() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.mode='enabled' THEN
  IF EXISTS(SELECT 1 FROM jsonb_array_elements(NEW.manifest) e WHERE NOT EXISTS(
   SELECT 1 FROM nas_cleanup_frozen_streams f WHERE f.plan_id=NEW.id AND f.stream_id=(e->>'stream_id')::bigint AND f.validated)) THEN
   RAISE EXCEPTION 'NAS cleanup enable requires a committed, drained, replay-validated freeze' USING ERRCODE='55000';
  END IF;
 ELSE
  DELETE FROM nas_cleanup_frozen_streams WHERE plan_id=NEW.id;
  DELETE FROM nas_cleanup_rankings_cache WHERE plan_id=NEW.id;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER nas_cleanup_plan_freeze_lifecycle BEFORE INSERT OR UPDATE OF mode ON nas_cleanup_plans FOR EACH ROW EXECUTE FUNCTION nas_cleanup_plan_freeze_lifecycle();
-- A deployed predecessor cannot authorize anything until the new handshake.
UPDATE nas_cleanup_plans SET mode='paused',approved_until=NULL,
 approval_reason='source freeze migration: fresh enable required' WHERE mode='enabled';
UPDATE nas_cleanup_batches SET mode='paused',approved_until=NULL,
 approval_reason='source freeze migration: fresh enable required' WHERE mode='enabled';
