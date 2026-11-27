-- Read-only, by key (:'keys', a JSON array of at most 5,000 output keys). Whether each output is keep-held
-- (an exact delivery keep hold not exempt, as size:etag, or an owner keep), whether any retirement path
-- names it, and the state of its NAS output restores: none, verified (one verified) or unverified.
COPY (
 SELECT k.key,
  coalesce((SELECT d.size_bytes::text || ':' || btrim(d.etag, '"') FROM delivery_keep_holds d
            WHERE d.bucket = 'stoarama' AND d.object_key = k.key AND NOT r2_delivery_exempt('stoarama', k.key)), ''),
  r2_owner_keep_held('stoarama', k.key)::text,
  EXISTS(SELECT FROM r2_object_retirements z WHERE z.bucket = 'stoarama' AND z.object_key = k.key)::text,
  EXISTS(SELECT FROM r2_retirement_reservations z WHERE z.bucket = 'stoarama' AND z.object_key = k.key)::text,
  EXISTS(SELECT FROM r2_nonraw_intents z WHERE z.bucket = 'stoarama' AND z.object_key = k.key)::text,
  (SELECT CASE WHEN count(*) = 0 THEN 'none' WHEN bool_or(q.state = 'verified') THEN 'verified' ELSE 'unverified' END
   FROM recording_collation_outputs o JOIN nas_restore_requests q ON q.output_id = o.id AND q.target = 'output'
   WHERE o.object_key = k.key)
 FROM (SELECT DISTINCT key FROM jsonb_array_elements_text(:'keys'::jsonb) key) k
) TO STDOUT WITH (FORMAT csv, DELIMITER E'\t');
