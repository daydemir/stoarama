-- A recording clip belongs to the UTC hour of its start. Its whole media
-- interval must remain inside the approved scope, even across hours.
CREATE OR REPLACE FUNCTION nas_stitch_r2_has_exact_input_set(hour_id bigint) RETURNS boolean
LANGUAGE sql STABLE AS $$
 SELECT EXISTS (
  SELECT 1 FROM nas_stitch_r2_hours h
  JOIN nas_stitch_scopes s ON s.id=h.scope_id
  JOIN recordings r ON r.id=h.recording_id AND r.stream_id=s.stream_id AND r.account_id=47
  WHERE h.id=$1 AND h.hour_at>=s.start_at AND h.hour_at+interval '1 hour'<=s.end_at
   AND (SELECT count(*) FROM recording_clips c WHERE c.recording_id=h.recording_id
    AND c.clip_start_at>=h.hour_at AND c.clip_start_at<h.hour_at+interval '1 hour')
     = jsonb_array_length(h.plan->'clips')
   AND (SELECT count(DISTINCT (p->>'source_id')::bigint) FROM jsonb_array_elements(h.plan->'clips') p
    WHERE jsonb_typeof(p->'source_id')='number')=jsonb_array_length(h.plan->'clips')
   AND EXISTS(SELECT 1 FROM nas_stitch_r2_input_holds i WHERE i.hour_id=h.id AND i.released_at IS NULL
    HAVING count(*)=jsonb_array_length(h.plan->'clips') AND sum(i.size_bytes)=h.input_bytes)
   AND NOT EXISTS(
    SELECT 1 FROM jsonb_array_elements(h.plan->'clips') p
    WHERE NOT EXISTS(
     SELECT 1 FROM nas_stitch_r2_input_holds i
     JOIN recording_clips c ON c.id=i.catalog_clip_id AND c.bucket=i.bucket AND c.object_key=i.object_key
     JOIN r2_retirement_writer_fences fence ON fence.bucket=c.bucket
     JOIN nas_inventory_files f ON f.connection_id=13 AND f.clip_id=c.id AND f.state='present'
     WHERE i.hour_id=h.id AND i.catalog_clip_id=(p->>'source_id')::bigint AND i.released_at IS NULL
      AND c.recording_id=h.recording_id AND c.clip_start_at>=h.hour_at
      AND c.clip_start_at<h.hour_at+interval '1 hour'
      AND c.clip_end_at>c.clip_start_at AND c.clip_end_at<=s.end_at
      AND c.clip_start_at=(p->>'start_at')::timestamptz AND c.clip_end_at=(p->>'end_at')::timestamptz
      AND c.display_path=p->>'relative_path' AND c.purged_at IS NULL AND c.etag=i.etag
      AND encode(sha256(convert_to(rtrim(c.endpoint,'/'),'UTF8')),'hex')=fence.endpoint_sha256
      AND lower(c.sha256)=i.sha256 AND c.size_bytes=i.size_bytes
      AND f.relative_path=c.display_path AND f.size_bytes=i.size_bytes AND lower(f.sha256)=i.sha256
      AND i.sha256=p->>'sha256' AND i.size_bytes=(p->>'size_bytes')::bigint
    )
   )
 )
$$;
