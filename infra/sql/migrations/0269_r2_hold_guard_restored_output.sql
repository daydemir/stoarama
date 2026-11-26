-- A retired collated output stops blocking delivery holds once a verified NAS
-- output restore (0263) re-created that exact object: the 'verified' restore row
-- (set by handleAccountConnectionRestoreResult only after a full R2 HEAD and
-- re-hash, which also stamps completed_at and verified_etag) carries the
-- output's key, sha256 and size, the hold and the retirement row name those same
-- bytes in that bucket, and the restore completed after the retirement: a
-- restore verified while the object was still in R2 (outcome 'exists') re-created
-- nothing a later retirement deleted. Every other clause of
-- guard_r2_delivery_keys_admitted() (0267) is unchanged. q.output_id=o.id is
-- served by nas_restore_requests_output_idx.
CREATE OR REPLACE FUNCTION guard_r2_delivery_keys_admitted() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r record; n text; BEGIN
 FOR r IN SELECT * FROM new_rows LOOP
  n := regexp_replace(r.relative_path,'^joined/','');
  IF EXISTS(SELECT 1 FROM r2_retirement_reservations v WHERE v.bucket=r.bucket AND v.object_key=r.object_key)
  OR EXISTS(SELECT 1 FROM r2_inverse_intents i WHERE i.bucket=r.bucket AND i.object_key=r.object_key)
  OR EXISTS(SELECT 1 FROM r2_nonraw_intents i WHERE i.bucket=r.bucket AND i.object_key=r.object_key) THEN
   RAISE EXCEPTION 'R2 retirement reservation forbids delivery adoption'; END IF;
  IF r.sha256<>'' AND (EXISTS(SELECT 1 FROM recording_clips c JOIN r2_retirement_reservations v ON v.kind='raw' AND v.source_id=c.id
      WHERE lower(c.sha256)=r.sha256 AND c.size_bytes=r.size_bytes)
    OR EXISTS(SELECT 1 FROM r2_nonraw_intents i WHERE i.sha256=r.sha256 AND i.size_bytes=r.size_bytes)) THEN
   RAISE EXCEPTION 'R2 retirement reservation forbids delivery content alias'; END IF;
  IF r.relative_path<>'' AND (EXISTS(SELECT 1 FROM recording_clips c WHERE c.display_path IN (n,'joined/'||n) AND regexp_replace(c.display_path,'^joined/','')=n
      AND EXISTS(SELECT 1 FROM r2_retirement_reservations v WHERE v.kind='raw' AND v.source_id=c.id))
    OR EXISTS(SELECT 1 FROM r2_nonraw_intents i WHERE i.nas_relative_path IN (n,'joined/'||n) AND regexp_replace(i.nas_relative_path,'^joined/','')=n)) THEN
   RAISE EXCEPTION 'R2 retirement reservation forbids delivery path alias'; END IF;
  -- Transitive closure: a hold admitted on a collated OUTPUT is also an overlap
  -- with that output's OWN reserved/pending RAW sources and with its own pending
  -- collated retirement. Matching is exact on key, normalized path or content, so
  -- unrelated rows are still admitted.
  IF EXISTS(SELECT 1 FROM recording_collation_outputs o
     WHERE (o.object_key=r.object_key
       OR (r.relative_path<>'' AND o.nas_relative_path IN (n,'joined/'||n) AND regexp_replace(o.nas_relative_path,'^joined/','')=n)
       OR (r.sha256<>'' AND o.sha256=r.sha256 AND o.size_bytes=r.size_bytes))
     AND (EXISTS(SELECT 1 FROM r2_retirement_reservations v WHERE v.kind='collated' AND v.source_id=o.id)
       OR EXISTS(SELECT 1 FROM r2_nonraw_intents i WHERE i.kind='collated' AND i.source_id=o.id)
       OR EXISTS(SELECT 1 FROM r2_object_retirements x WHERE x.kind='collated' AND x.source_id=o.id
            AND NOT EXISTS(SELECT 1 FROM nas_restore_requests q WHERE q.output_id=o.id AND q.target='output'
              AND q.state='verified' AND q.completed_at>x.retired_at AND q.verified_etag<>''
              AND q.object_key=o.object_key AND q.sha256=o.sha256 AND q.size_bytes=o.size_bytes
              AND q.object_key=r.object_key AND q.sha256=r.sha256 AND q.size_bytes=r.size_bytes
              AND x.bucket=r.bucket AND x.object_key=q.object_key AND x.sha256=q.sha256 AND x.size_bytes=q.size_bytes))
       OR EXISTS(SELECT 1 FROM recording_clips c JOIN r2_retirement_reservations v ON v.kind='raw' AND v.source_id=c.id
            WHERE c.id=ANY(o.source_clip_ids))
       OR EXISTS(SELECT 1 FROM recording_clips c JOIN r2_inverse_intents i ON i.source_id=c.id
            WHERE c.id=ANY(o.source_clip_ids)))) THEN
   RAISE EXCEPTION 'R2 retirement reservation forbids delivery collated-source closure'; END IF;
 END LOOP;
 RETURN NULL; END $$;
