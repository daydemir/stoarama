-- 0179 is reserved by the independent V85r stitch work. No source backfill:
-- absent versions read as zero; every subsequent source mutation bumps one.
CREATE TABLE nas_cleanup_rankings_versions (
 stream_id bigint PRIMARY KEY,
 version bigint NOT NULL
);
CREATE TABLE nas_cleanup_rankings_cache (
 plan_id bigint NOT NULL REFERENCES nas_cleanup_plans(id),
 stream_id bigint NOT NULL,
 source_version bigint NOT NULL,
 best_start text NOT NULL,
 fingerprint text NOT NULL,
 checked_at timestamptz NOT NULL,
 PRIMARY KEY(plan_id,stream_id)
);

-- Cover every replay dependency, including quarantine and both timezone
-- sources. Even metadata-only clip updates invalidate (conservatively).
CREATE FUNCTION invalidate_nas_cleanup_rankings() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE old_stream bigint; new_stream bigint; affected bigint;
BEGIN
 IF TG_OP='TRUNCATE' THEN
  INSERT INTO nas_cleanup_rankings_versions(stream_id,version)
   SELECT id,1 FROM streams ORDER BY id
   ON CONFLICT(stream_id) DO UPDATE SET version=nas_cleanup_rankings_versions.version+1;
  RETURN NULL;
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
 FOR affected IN SELECT DISTINCT x FROM unnest(ARRAY[old_stream,new_stream]) x WHERE x IS NOT NULL ORDER BY x LOOP
  INSERT INTO nas_cleanup_rankings_versions VALUES(affected,1)
   ON CONFLICT(stream_id) DO UPDATE SET version=nas_cleanup_rankings_versions.version+1;
 END LOOP;
 RETURN NULL;
END $$;
CREATE TRIGGER nas_cleanup_rankings_clips AFTER INSERT OR UPDATE OR DELETE ON recording_clips FOR EACH ROW EXECUTE FUNCTION invalidate_nas_cleanup_rankings();
CREATE TRIGGER nas_cleanup_rankings_hours AFTER INSERT OR UPDATE OR DELETE ON recording_collation_hours FOR EACH ROW EXECUTE FUNCTION invalidate_nas_cleanup_rankings();
CREATE TRIGGER nas_cleanup_rankings_recordings AFTER INSERT OR UPDATE OR DELETE ON recordings FOR EACH ROW EXECUTE FUNCTION invalidate_nas_cleanup_rankings();
CREATE TRIGGER nas_cleanup_rankings_streams AFTER INSERT OR UPDATE OR DELETE ON streams FOR EACH ROW EXECUTE FUNCTION invalidate_nas_cleanup_rankings();
CREATE TRIGGER nas_cleanup_rankings_clips_truncate AFTER TRUNCATE ON recording_clips FOR EACH STATEMENT EXECUTE FUNCTION invalidate_nas_cleanup_rankings();
CREATE TRIGGER nas_cleanup_rankings_hours_truncate AFTER TRUNCATE ON recording_collation_hours FOR EACH STATEMENT EXECUTE FUNCTION invalidate_nas_cleanup_rankings();
CREATE TRIGGER nas_cleanup_rankings_recordings_truncate BEFORE TRUNCATE ON recordings FOR EACH STATEMENT EXECUTE FUNCTION invalidate_nas_cleanup_rankings();
CREATE TRIGGER nas_cleanup_rankings_streams_truncate BEFORE TRUNCATE ON streams FOR EACH STATEMENT EXECUTE FUNCTION invalidate_nas_cleanup_rankings();
