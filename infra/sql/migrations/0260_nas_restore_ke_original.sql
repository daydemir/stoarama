-- Deniz, verbatim: "nas shoul dnot have any of the keep everything (except the
-- keep 20)". R2 holds 100% of Keep Everything, so Keep Everything footage that
-- exists only on the NAS goes back to its original R2 key before the NAS copy
-- can leave.
--
-- This is the one NAS -> R2 write the active writer fence (0187) admits: an
-- 'original' restore of a clip whose stream is pinned keep_everything in
-- nas_media_keep20_windows, the 0236 manifest the server also embeds for
-- cleanup. Every other restore row, test restores included, and every other
-- writer stay fenced exactly as before. The restore's capability stays visible
-- to r2_retirement_writes_blocked through put_expires_at, stamped when signed.

-- Only the clip's own catalogued object key, and only while the clip is purged:
-- R2 must not already hold those bytes.
CREATE FUNCTION nas_restore_ke_original(target text, clip bigint, key text) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT target='original' AND EXISTS(SELECT 1 FROM recording_clips c JOIN recordings r ON r.id=c.recording_id
  JOIN nas_media_keep20_windows w ON w.stream_id=r.stream_id
  WHERE c.id=clip AND c.object_key=key AND c.purged_at IS NOT NULL AND w.keep_everything)
$$;

CREATE FUNCTION r2_retirement_guard_restore() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NEW.state IN ('pending','leased') THEN
  IF nas_restore_ke_original(NEW.target,NEW.clip_id,NEW.object_key) THEN
   -- The same shared wire lock as r2_retirement_admit_writer: no fence
   -- transition can interleave with this admission.
   PERFORM pg_advisory_xact_lock_shared(7343226011187);
  ELSE
   PERFORM r2_retirement_admit_writer();
  END IF;
 END IF;
 RETURN NEW;
END $$;

DROP TRIGGER r2_writer_restore ON nas_restore_requests;
CREATE TRIGGER r2_writer_restore BEFORE INSERT OR UPDATE ON nas_restore_requests
 FOR EACH ROW EXECUTE FUNCTION r2_retirement_guard_restore();
