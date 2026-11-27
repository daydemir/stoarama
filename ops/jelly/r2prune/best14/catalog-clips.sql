-- Read-only, by primary key (:'ids', a JSON array of at most 5,000 clip ids). The catalog row of each raw.
COPY (
 SELECT c.id, r.stream_id, c.object_key, c.size_bytes, btrim(c.etag, '"'), lower(c.sha256), (c.purged_at IS NULL)::text,
  extract(epoch FROM c.clip_start_at)::numeric, extract(epoch FROM c.clip_end_at)::numeric
 FROM (SELECT DISTINCT id::bigint FROM jsonb_array_elements_text(:'ids'::jsonb) id) k
 JOIN recording_clips c ON c.id = k.id AND c.bucket = 'stoarama'
 JOIN recordings r ON r.id = c.recording_id
) TO STDOUT WITH (FORMAT csv, DELIMITER E'\t');
