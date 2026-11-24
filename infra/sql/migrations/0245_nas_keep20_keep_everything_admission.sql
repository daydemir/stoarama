-- Deniz (2026-11-24), verbatim: "no copy to nas.. the nas needs to stitch all
-- of the keep 20 so we should leverage stuff thats already stitched, but to do
-- this lets speed up transfer to nas where we can".
--
-- The NAS keeps every pinned 20-day window, Keep Everything streams included.
-- 0236 left Keep Everything windows out of NAS admission, so their in-window
-- collated outputs and raw copy items were refused. This widens admission only:
-- collated delivery and R2->NAS copy may now add those files to the NAS. Raw
-- and joined delivery, stitch scopes, cleanup and the historical fence are
-- unchanged (they keep nas_media_keep20_span / nas_media_keep20_scope).

-- nas_media_keep20_span over every pinned window, Keep Everything included.
CREATE FUNCTION nas_media_keep20_pinned_span(stream bigint,tz text,a timestamptz,b timestamptz) RETURNS integer LANGUAGE sql STABLE AS $$
 SELECT coalesce((SELECT CASE WHEN a IS NULL OR b IS NULL OR b<=a OR a<'2020-01-01' OR b>='2028-01-01'
  OR timezone<>tz OR tz IS NULL THEN 0
 WHEN a<end_at AND b>start_at THEN 1 ELSE -1 END
 FROM nas_media_keep20_windows WHERE stream_id=stream),0)
$$;

CREATE OR REPLACE FUNCTION nas_media_keep20_copy_item(i jsonb) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM nas_media_keep20_windows w
  WHERE w.stream_id=(i->>'stream_id')::bigint AND (i->>'end_at')::timestamptz>(i->>'start_at')::timestamptz
  AND (i->>'start_at')::timestamptz<w.end_at AND (i->>'end_at')::timestamptz>w.start_at)
$$;

-- 0236's definition with only the collated branch on the pinned span.
CREATE OR REPLACE FUNCTION nas_delivery_allowed(connection bigint,kind text,source bigint)
RETURNS boolean LANGUAGE plpgsql STABLE AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM nas_delivery_retirement_fences WHERE connection_id=connection) THEN
  RETURN true;
 END IF;
 IF NOT EXISTS(SELECT 1 FROM nas_media_keep20_epoch WHERE connection_id=connection) THEN
  RETURN nas_delivery_object_span(kind,source)=1;
 END IF;
 IF kind='raw' THEN
  RETURN coalesce((SELECT nas_media_keep20_span(r.stream_id,coalesce(nullif(s.local_timezone,''),r.cron_timezone),
   c.clip_start_at,c.clip_end_at)=1
   FROM recording_clips c JOIN recordings r ON r.id=c.recording_id LEFT JOIN streams s ON s.id=r.stream_id
   WHERE c.id=source AND r.account_id=47),false);
 ELSIF kind='collated' THEN
  RETURN coalesce((SELECT nas_media_keep20_pinned_span(r.stream_id,coalesce(nullif(s.local_timezone,''),r.cron_timezone),
    o.start_at,o.end_at)=1 AND cardinality(o.source_clip_ids)>0 AND NOT EXISTS (
     SELECT 1 FROM unnest(o.source_clip_ids) ids(id)
     LEFT JOIN recording_clips c ON c.id=ids.id LEFT JOIN recordings cr ON cr.id=c.recording_id
     LEFT JOIN streams cs ON cs.id=cr.stream_id
     WHERE cr.account_id IS DISTINCT FROM 47 OR nas_media_keep20_pinned_span(cr.stream_id,
      coalesce(nullif(cs.local_timezone,''),cr.cron_timezone),c.clip_start_at,c.clip_end_at)=0)
   FROM recording_collation_outputs o JOIN recording_collation_hours h ON h.id=o.collation_hour_id
   JOIN recordings r ON r.id=h.recording_id LEFT JOIN streams s ON s.id=r.stream_id
   WHERE o.id=source AND r.account_id=47),false);
 ELSIF kind='joined' THEN
  RETURN coalesce((SELECT a.artifact_kind='batch_index' OR nas_media_keep20_span(r.stream_id,
   coalesce(nullif(s.local_timezone,''),r.cron_timezone),coalesce(h.scheduled_start_at,d.scheduled_start_at),
   coalesce(h.scheduled_end_at,d.scheduled_end_at))=1
   FROM recording_joined_artifacts a LEFT JOIN recording_joined_hours h ON h.id=a.hour_record_id
   LEFT JOIN recording_joined_stream_days d ON d.id=a.stream_day_id
   LEFT JOIN recordings r ON r.id=coalesce(h.recording_id,d.recording_id) LEFT JOIN streams s ON s.id=r.stream_id
   WHERE a.id=source AND a.connection_id=connection),false);
 END IF;
 RETURN false;
END $$;
