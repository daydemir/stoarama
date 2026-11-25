-- GSS delivery freeze. Owner (Deniz, Nov 25), verbatim: "yea i konw i said to
-- take it down, so its ok if its goen but i wnat to save it if its not" and
-- "namely the footage obv is the main concern, if its on the NAS or R2 or
-- where". This supersedes 0259's "just drop the recordings" for delivery
-- footage only: a non-Keep-Everything day unit is again held by any delivery
-- source or delivery path hold. Incident holds stay; every other 0259 release
-- (R2 reservations, stitch/joined state, per-file source holds) stands.

CREATE OR REPLACE FUNCTION nas_cleanup_day_clip_held(clip bigint, keep_everything boolean) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT CASE WHEN keep_everything THEN nas_cleanup_day_ke_clip_held(clip) ELSE
  EXISTS(SELECT 1 FROM nas_cleanup_incident_sources h WHERE h.clip_id=clip)
  OR EXISTS(SELECT 1 FROM nas_cleanup_incident_files h WHERE h.kind='raw' AND h.source_id=clip)
  OR EXISTS(SELECT 1 FROM delivery_nas_source_holds h WHERE h.clip_id=clip)
  OR EXISTS(SELECT 1 FROM recording_clips c WHERE c.id=clip
   AND delivery_nas_held('raw',c.id,c.display_path,lower(c.sha256),c.size_bytes)) END
$$;

-- A delivery path hold in the directory keeps every directory-day again.
CREATE OR REPLACE FUNCTION nas_cleanup_day_dir_held(dir text, keep_everything boolean) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM delivery_keep_holds h WHERE h.relative_path<>'' AND left(h.relative_path,length(dir)+1)=dir||'/')
$$;
