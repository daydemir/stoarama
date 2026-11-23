-- Keep Everything applies to all R2 footage for the pinned streams, including
-- managed RAW keys whose names do not contain a stream ID. Keep the explicit
-- owner key-prefix holds and use the immutable 20-day manifest for stream scope.
-- Serialize the expanded guard with retirement admission and wire issuance.
SET LOCAL lock_timeout = '5s';
SELECT pg_advisory_xact_lock(7343226011103);
SELECT pg_advisory_xact_lock(7343226011101);
SELECT pg_advisory_xact_lock(7343226011220);

CREATE OR REPLACE FUNCTION r2_owner_keep_held(p_bucket text, p_key text) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM r2_owner_keep_prefixes k WHERE k.bucket=p_bucket AND starts_with(p_key,k.prefix))
 OR (p_bucket='stoarama' AND (
  EXISTS(SELECT 1 FROM recording_clips c
   JOIN recordings r ON r.id=c.recording_id
   JOIN nas_media_keep20_windows w ON w.stream_id=r.stream_id AND w.keep_everything
   WHERE c.bucket=p_bucket AND c.object_key=p_key AND r.account_id=47)
  OR EXISTS(SELECT 1 FROM recording_collation_outputs o
   JOIN recording_collation_hours h ON h.id=o.collation_hour_id
   JOIN recordings r ON r.id=h.recording_id
   JOIN nas_media_keep20_windows w ON w.stream_id=r.stream_id AND w.keep_everything
   WHERE o.object_key=p_key AND r.account_id=47)
  OR EXISTS(SELECT 1 FROM recording_collation_outputs o
   JOIN recording_clips c ON c.id=ANY(o.source_clip_ids)
   JOIN recordings r ON r.id=c.recording_id
   JOIN nas_media_keep20_windows w ON w.stream_id=r.stream_id AND w.keep_everything
   WHERE o.object_key=p_key AND r.account_id=47)
  OR EXISTS(SELECT 1 FROM delivery_keep_holds h
   JOIN recordings r ON r.id=h.recording_id
   JOIN nas_media_keep20_windows w ON w.stream_id=r.stream_id AND w.keep_everything
   WHERE h.bucket=p_bucket AND h.object_key=p_key AND r.account_id=47)
 ))
$$;
