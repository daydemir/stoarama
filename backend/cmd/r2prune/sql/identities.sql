-- Catalog identities for one stream (--identities). Read-only, once per stream:
-- psql -XqAt -v stream=N -f identities.sql >> identities.tsv
COPY (SELECT 'raw',c.object_key,r.stream_id,coalesce(extract(epoch FROM c.clip_start_at)::bigint,0),coalesce(extract(epoch FROM c.clip_end_at)::bigint,0),c.size_bytes,
 (c.sha256 ~ '^[0-9a-fA-F]{64}$' AND n.state='present' AND n.relative_path=c.display_path AND n.sha256=lower(c.sha256) AND n.size_bytes=c.size_bytes) IS TRUE,
 coalesce(extract(epoch FROM n.verified_at)::bigint,0), split_part(c.display_path,'/',1), c.id
 FROM recordings r JOIN recording_clips c ON c.recording_id=r.id LEFT JOIN nas_inventory_files n ON n.connection_id=13 AND n.clip_id=c.id
 WHERE r.stream_id=:stream AND c.bucket='stoarama') TO STDOUT;
COPY (SELECT 'col',o.object_key,r.stream_id,coalesce(extract(epoch FROM o.start_at)::bigint,0),coalesce(extract(epoch FROM o.end_at)::bigint,0),o.size_bytes,
 (a.sha256=o.sha256 AND a.size_bytes=o.size_bytes) IS TRUE, coalesce(extract(epoch FROM a.verified_at)::bigint,0), split_part(regexp_replace(o.nas_relative_path,'^joined/',''),'/',1), o.id
 FROM recordings r JOIN recording_collation_hours h ON h.recording_id=r.id JOIN recording_collation_outputs o ON o.collation_hour_id=h.id
 LEFT JOIN recording_collation_output_acks a ON a.output_id=o.id AND a.connection_id=13 WHERE r.stream_id=:stream) TO STDOUT;
