-- Identity sources for keys with no catalog row (pass 2): Gen1 artifacts with NAS acks, repair part names, page rows, recording to stream map. Read-only:
-- psql -XqAt -v ON_ERROR_STOP=1 -f derived.sql > derived-db.tsv; then derive_identities.py SNAPSHOT IDENTITIES derived-db.tsv best14-sources.tsv
COPY (SELECT 'gen1', a.object_key, a.artifact_kind, coalesce(h.recording_id,0), a.expected_size_bytes,
 coalesce(extract(epoch FROM least(h.scheduled_start_at, min(s.start_at)))::bigint,0),
 coalesce(extract(epoch FROM greatest(h.scheduled_end_at, max(s.end_at)))::bigint,0),
 (bool_or(s.id IS NOT NULL AND (s.start_at IS NULL OR s.end_at IS NULL))) IS TRUE,
 (k.artifact_id IS NOT NULL AND k.sha256=a.expected_sha256 AND k.size_bytes=a.expected_size_bytes),
 coalesce(extract(epoch FROM k.verified_at)::bigint,0), regexp_replace(a.relative_path,'^.*/','')
 FROM recording_joined_artifacts a
 LEFT JOIN recording_joined_hours h ON h.id=a.hour_record_id
 LEFT JOIN recording_joined_media_sources ms ON ms.artifact_id=a.id
 LEFT JOIN recording_joined_sources s ON s.id=ms.source_id
 LEFT JOIN recording_joined_artifact_acks k ON k.artifact_id=a.id AND k.connection_id=13
 WHERE a.object_key LIKE 'joined/goodplus-%'
 GROUP BY a.id,h.recording_id,h.scheduled_start_at,h.scheduled_end_at,k.artifact_id,k.sha256,k.size_bytes,k.verified_at) TO STDOUT;
COPY (SELECT 'colpart', o.object_key, 'collated', h.recording_id, o.size_bytes, coalesce(extract(epoch FROM o.start_at)::bigint,0), coalesce(extract(epoch FROM o.end_at)::bigint,0), false, false, 0, regexp_replace(o.nas_relative_path,'^.*/','')
 FROM recording_collation_outputs o JOIN recording_collation_hours h ON h.id=o.collation_hour_id WHERE h.recording_id IN (SELECT DISTINCT recording_id FROM delivery_keep_holds WHERE bucket='stoarama' AND recording_id IS NOT NULL)) TO STDOUT;
COPY (SELECT 'page', object_key, kind, coalesce(recording_id,0), size_bytes, 0, 0, false, false, 0, coalesce(sha256,'')||'|'||regexp_replace(coalesce(relative_path,''),'^.*/','') FROM delivery_keep_holds WHERE bucket='stoarama') TO STDOUT;
COPY (SELECT 'rec', id::text, coalesce(account_id::text,''), coalesce(stream_id,0), 0, 0, 0, false, false, 0, '' FROM recordings) TO STDOUT;
