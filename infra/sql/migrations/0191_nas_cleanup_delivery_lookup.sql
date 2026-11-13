-- Every cleanup candidate and permit checks these holds. Keep the exact veto
-- semantics, but let path and content checks use separate bounded index probes.
-- No hold, source dependency or retirement authority changes.
CREATE INDEX delivery_keep_path ON delivery_keep_holds(relative_path) WHERE relative_path<>'';

CREATE OR REPLACE FUNCTION delivery_nas_held(k text,i bigint,p text,sha text,n bigint) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM delivery_keep_holds h WHERE h.relative_path<>'' AND h.relative_path IN (p,regexp_replace(p,'^joined/','')))
 OR EXISTS(SELECT 1 FROM delivery_keep_holds h WHERE h.sha256<>'' AND h.sha256=sha AND h.size_bytes=n)
 OR (k='raw' AND (EXISTS(SELECT 1 FROM delivery_nas_source_holds r WHERE r.clip_id=i) OR EXISTS(SELECT 1 FROM recording_clips c JOIN delivery_keep_holds h ON h.bucket=c.bucket AND h.object_key=c.object_key WHERE c.id=i)))
 OR (k='collated' AND EXISTS(SELECT 1 FROM recording_collation_outputs o WHERE o.id=i AND
 (EXISTS(SELECT 1 FROM delivery_keep_holds h WHERE h.object_key=o.object_key)
 OR EXISTS(SELECT 1 FROM delivery_keep_output_dependencies d JOIN recording_collation_hours h ON h.recording_id=d.recording_id WHERE d.output_id=o.id AND h.id=o.collation_hour_id)
 OR EXISTS(SELECT 1 FROM delivery_nas_source_holds r WHERE r.clip_id=ANY(o.source_clip_ids)))))
$$;
