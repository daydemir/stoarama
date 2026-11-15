
 WITH page_request AS MATERIALIZED (SELECT $3::bigint AS page_size),
 delivery_fence AS MATERIALIZED (
   SELECT EXISTS (SELECT 1 FROM nas_delivery_retirement_fences
     WHERE connection_id=CASE WHEN $1::bigint=47 THEN 13 ELSE 0 END) AS active
 ), owned_windows AS MATERIALIZED (
   SELECT jsonb_object_agg(r.id::text,jsonb_build_array(kept_window.start_at,kept_window.end_at)) AS bounds
   FROM recordings r CROSS JOIN delivery_fence fence
   LEFT JOIN streams stream ON stream.id=r.stream_id
   JOIN nas_delivery_retirement_windows kept_window
     ON kept_window.connection_id=13 AND kept_window.stream_id=r.stream_id
   WHERE fence.active AND r.account_id = $1 AND r.account_id=47 AND r.delivery = 'nas_pull'
     AND kept_window.timezone=coalesce(nullif(stream.local_timezone,''),r.cron_timezone)
 ), available_clips AS NOT MATERIALIZED (
   SELECT c.* FROM recording_clips c
   WHERE c.purged_at IS NULL AND c.released_at IS NULL AND c.size_bytes > 0
     AND c.created_at < now() - interval '90 seconds'
     AND c.id > $2
 )
 SELECT c.id, c.recording_id, c.size_bytes, c.sha256, c.clip_start_at, c.clip_end_at, c.display_path,
        c.recording_job_id, c.capture_lease_token, c.capture_sequence,
        c.capture_attempt_id,c.timestamp_contract_version,c.timestamp_contract,c.timestamp_contract_status,c.timestamp_contract_reason
 FROM (
   (SELECT c.* FROM delivery_fence fence CROSS JOIN LATERAL (
      SELECT c.* FROM available_clips c JOIN recordings r ON r.id=c.recording_id
      WHERE NOT fence.active AND r.account_id = $1 AND r.delivery = 'nas_pull'
       AND (SELECT NOT EXISTS (SELECT 1 FROM clip_storage_billing_contracts nas_hold WHERE nas_hold.mode='nas_collated_hold' AND nas_hold.clip_id=c.id) OFFSET 0)
      ORDER BY c.id ASC LIMIT $3
    ) c)
   UNION ALL
   (SELECT c.* FROM delivery_fence fence CROSS JOIN LATERAL (
      SELECT c.* FROM available_clips c CROSS JOIN owned_windows kept_window
      WHERE fence.active AND NOT EXISTS (SELECT 1 FROM clip_storage_billing_contracts nas_hold WHERE nas_hold.mode='nas_collated_hold' AND nas_hold.clip_id=c.id)
       AND c.clip_start_at IS NOT NULL AND c.clip_end_at IS NOT NULL
       AND c.clip_end_at>c.clip_start_at
       AND c.clip_start_at>='2020-01-01' AND c.clip_end_at<'2028-01-01'
       AND kept_window.bounds ? c.recording_id::text
       AND c.clip_start_at<(kept_window.bounds->c.recording_id::text->>1)::timestamptz
       AND c.clip_end_at>(kept_window.bounds->c.recording_id::text->>0)::timestamptz
      ORDER BY c.id ASC LIMIT (SELECT page_size FROM page_request)
    ) c)
 ) c ORDER BY c.id ASC LIMIT $3
