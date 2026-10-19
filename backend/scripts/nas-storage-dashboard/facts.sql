BEGIN READ ONLY;
SET LOCAL statement_timeout = '180s';
COPY (
 SELECT i.relative_path path,i.recording_id,'clips' kind,i.size_bytes bytes,
   CASE WHEN c.clip_start_at IS NOT NULL AND c.clip_end_at>c.clip_start_at
     THEN jsonb_build_array(jsonb_build_array(extract(epoch from c.clip_start_at),extract(epoch from c.clip_end_at)))
     ELSE '[]'::jsonb END ranges,c.object_key r2_key,i.state='present' reported_present,
   i.server_received_at observed_at,'clip:'||i.clip_id::text source_id
 FROM nas_inventory_files i LEFT JOIN recording_clips c ON c.id=i.clip_id
 WHERE i.connection_id=13 AND i.server_received_at >= :'cutoff'::timestamptz
 UNION ALL
 SELECT 'joined/'||k.nas_relative_path,h.recording_id,'stitched',k.size_bytes,
   coalesce((SELECT jsonb_agg(jsonb_build_array(extract(epoch from c.clip_start_at),extract(epoch from c.clip_end_at)))
       FROM recording_clips c WHERE c.id=ANY(o.source_clip_ids) AND c.clip_end_at>c.clip_start_at),'[]'::jsonb),
   o.object_key,true,k.verified_at,'collation:'||o.id::text
 FROM recording_collation_output_acks k JOIN recording_collation_outputs o ON o.id=k.output_id
 JOIN recording_collation_hours h ON h.id=o.collation_hour_id
 WHERE k.connection_id=13 AND k.verified_at >= :'cutoff'::timestamptz
 UNION ALL
 SELECT 'joined/'||a.batch_id||'/'||k.relative_path,d.recording_id,'stitched',k.size_bytes,
   coalesce((SELECT jsonb_agg(jsonb_build_array(extract(epoch from s.start_at),extract(epoch from s.end_at)))
     FROM recording_joined_media_sources m JOIN recording_joined_sources s ON s.id=m.source_id
     WHERE m.artifact_id=a.id AND s.end_at>s.start_at),'[]'::jsonb),
   a.object_key,true,k.verified_at,'joined:'||a.id::text
 FROM recording_joined_artifact_acks k JOIN recording_joined_artifacts a ON a.id=k.artifact_id
 JOIN recording_joined_stream_days d ON d.id=a.stream_day_id
 WHERE k.connection_id=13 AND a.artifact_kind='media' AND k.verified_at >= :'cutoff'::timestamptz
) TO STDOUT WITH (FORMAT CSV,HEADER TRUE);
ROLLBACK;
