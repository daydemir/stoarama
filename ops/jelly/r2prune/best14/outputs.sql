-- Read-only. Every registered output of the serving hour records (:'hours', a JSON array of hour_id),
-- with its hour status and the manifest's verification of it. One row per output; a second manifest
-- entry for one key shows as a duplicate row, which best14_coverage.py refuses.
COPY (
 SELECT h.hour_id, h.status, h.recording_id, h.local_date, h.delivery_hour, o.id, o.object_key, o.size_bytes, o.part,
  extract(epoch FROM o.start_at)::numeric, extract(epoch FROM o.end_at)::numeric, array_to_string(o.source_clip_ids, ','),
  coalesce(v.e->'verification'->>'payload_chain_matches', ''), coalesce(v.e->'verification'->>'video_timing_matches', ''),
  coalesce(v.e->'verification'->>'edge_decode_ok', '')
 FROM (SELECT DISTINCT hour_id FROM jsonb_array_elements_text(:'hours'::jsonb) hour_id) k
 JOIN recording_collation_hours h ON h.hour_id = k.hour_id
 JOIN recording_collation_outputs o ON o.collation_hour_id = h.id
 LEFT JOIN LATERAL (SELECT e FROM jsonb_array_elements(h.manifest->'outputs') e WHERE e->>'object_key' = o.object_key) v ON true
) TO STDOUT WITH (FORMAT csv, DELIMITER E'\t');
