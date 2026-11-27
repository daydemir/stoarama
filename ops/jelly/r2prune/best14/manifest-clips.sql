-- Read-only. Every clip entry of the serving hour records' manifests (:'hours', a JSON array of hour_id):
-- the exact source bytes and time span each clip was stitched from, and its disposition.
COPY (
 SELECT h.hour_id, e->'clip'->>'clip_id', coalesce(e->>'part', ''), e->>'disposition', e->'clip'->>'object_key',
  e->'clip'->>'size_bytes', lower(e->'clip'->>'sha256'), btrim(e->'clip'->>'etag', '"'),
  extract(epoch FROM (e->'clip'->>'start_utc')::timestamptz)::numeric, extract(epoch FROM (e->'clip'->>'end_utc')::timestamptz)::numeric,
  coalesce(e->'media'->>'content_seconds', '')
 FROM (SELECT DISTINCT hour_id FROM jsonb_array_elements_text(:'hours'::jsonb) hour_id) k
 JOIN recording_collation_hours h ON h.hour_id = k.hour_id
 CROSS JOIN LATERAL jsonb_array_elements(h.manifest->'clips') e
) TO STDOUT WITH (FORMAT csv, DELIMITER E'\t');
