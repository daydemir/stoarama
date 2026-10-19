BEGIN READ ONLY;
SET LOCAL statement_timeout = '60s';
SELECT jsonb_build_object(
 'generated_at',now(),
 'streams',(SELECT jsonb_agg(jsonb_build_object('recording_id',id,'stream_id',stream_id::text,
   'name',name,'timezone',cron_timezone,'folder',folder_name,
   'window_start',daily_window_start,'window_end',daily_window_end) ORDER BY id)
   FROM recordings WHERE account_id=47),
 'connection',(SELECT jsonb_build_object('last_seen_at',last_seen_at,'version',client_version,
   'phase',client_phase,'has_error',client_last_error<>'','free_bytes',nas_storage_free_bytes,
   'storage_reported_at',nas_storage_reported_at,
   'inventory_completed_at',to_jsonb(c)->'inventory_scan_completed_at') FROM connections c WHERE id=13),
 'audit',(SELECT jsonb_build_object('id',audit_id,'state',state,'started_at',started_at,
   'updated_at',updated_at,'completed_at',completed_at,'summary',summary,
   'artifact_size_bytes',artifact_size_bytes,'artifact_sha256',artifact_sha256)
   FROM nas_space_audits WHERE connection_id=13 ORDER BY started_at DESC LIMIT 1)
);
ROLLBACK;
