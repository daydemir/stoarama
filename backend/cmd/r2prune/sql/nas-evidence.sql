-- Live NAS custody for one non-Keep-Everything stream's 20-day window (--nas-evidence). Read-only, once per window:
-- psql -XqAt -v stream=N -v ws=START_UTC -v we=END_UTC_EXCL -f nas-evidence.sql >> nas-evidence.tsv
COPY (SELECT 'raw',c.object_key,
 (c.sha256 ~ '^[0-9a-fA-F]{64}$' AND n.state='present' AND n.relative_path=c.display_path AND n.sha256=lower(c.sha256) AND n.size_bytes=c.size_bytes) IS TRUE,
 coalesce(extract(epoch FROM n.verified_at)::bigint,0), coalesce(n.file_mtime_ns/1000000000,0), coalesce(extract(epoch FROM n.server_received_at)::bigint,0)
 FROM recordings r JOIN recording_clips c ON c.recording_id=r.id LEFT JOIN nas_inventory_files n ON n.connection_id=13 AND n.clip_id=c.id
 WHERE r.stream_id=:stream AND c.bucket='stoarama' AND c.clip_start_at<:'we' AND c.clip_end_at>:'ws') TO STDOUT;
COPY (SELECT 'col',o.object_key,(a.sha256=o.sha256 AND a.size_bytes=o.size_bytes) IS TRUE,
 coalesce(extract(epoch FROM a.verified_at)::bigint,0), 0, coalesce(extract(epoch FROM a.verified_at)::bigint,0)
 FROM recordings r JOIN recording_collation_hours h ON h.recording_id=r.id JOIN recording_collation_outputs o ON o.collation_hour_id=h.id
 LEFT JOIN recording_collation_output_acks a ON a.output_id=o.id AND a.connection_id=13
 WHERE r.stream_id=:stream AND o.start_at<:'we' AND o.end_at>:'ws') TO STDOUT;
