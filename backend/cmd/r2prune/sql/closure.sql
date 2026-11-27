-- 20-day stitch input closure for one stream window (--closure). Read-only, once per window:
-- psql -XqAt -v stream=N -v ws=START_UTC -v we=END_UTC_EXCL -f closure.sql >> closure.tsv
COPY (SELECT 'window_output_input',c.object_key FROM recordings r JOIN recording_collation_hours h ON h.recording_id=r.id JOIN recording_collation_outputs o ON o.collation_hour_id=h.id
 CROSS JOIN LATERAL unnest(o.source_clip_ids) sid JOIN recording_clips c ON c.id=sid
 WHERE r.stream_id=:stream AND o.start_at<:'we' AND o.end_at>:'ws' AND c.bucket='stoarama') TO STDOUT;
COPY (SELECT 'output_window_input',o.object_key FROM recordings r JOIN recording_collation_hours h ON h.recording_id=r.id JOIN recording_collation_outputs o ON o.collation_hour_id=h.id
 WHERE r.stream_id=:stream AND EXISTS(SELECT 1 FROM unnest(o.source_clip_ids) sid JOIN recording_clips c ON c.id=sid WHERE c.clip_start_at IS NULL OR c.clip_end_at IS NULL OR (c.clip_start_at<:'we' AND c.clip_end_at>:'ws'))) TO STDOUT;
