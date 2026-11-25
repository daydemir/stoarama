-- Non-Keep-Everything NAS day units: the only protections are a live NAS
-- cleanup incident hold, Keep Everything and the pinned window with its guard
-- days. Owner (Deniz), verbatim: "if its outside of keep everything, then a
-- nas copy only needs to be checked if its in the keep 20? otherwise it can be
-- deleted right?"; "we need to remove whatever is making us think thats a
-- mistake"; on delivery: "just drop the recordings its fine, we'll fix the
-- links whenever" (supersedes 0189's "preserve delivery on both stores" for
-- day units).
-- R2 retirement reservations, delivery holds, stitch/joined state and per-file
-- plan source holds no longer keep a non-KE directory-day. A KE unit keeps
-- every 0241 hold: its NAS copy leaves only while its R2 counterpart is live.
-- The per-file cleanup and R2 lane hold functions are unchanged.

-- The 0241 predicate, now only for KE units.
ALTER FUNCTION nas_cleanup_day_clip_held(bigint) RENAME TO nas_cleanup_day_ke_clip_held;

CREATE FUNCTION nas_cleanup_day_clip_held(clip bigint, keep_everything boolean) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT CASE WHEN keep_everything THEN nas_cleanup_day_ke_clip_held(clip) ELSE
  EXISTS(SELECT 1 FROM nas_cleanup_incident_sources h WHERE h.clip_id=clip)
  OR EXISTS(SELECT 1 FROM nas_cleanup_incident_files h WHERE h.kind='raw' AND h.source_id=clip) END
$$;

-- A delivery path hold in the directory keeps a KE directory only.
DROP FUNCTION nas_cleanup_day_dir_held(text);
CREATE FUNCTION nas_cleanup_day_dir_held(dir text, keep_everything boolean) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT keep_everything AND EXISTS(SELECT 1 FROM delivery_keep_holds h WHERE h.relative_path<>'' AND left(h.relative_path,length(dir)+1)=dir||'/')
$$;
