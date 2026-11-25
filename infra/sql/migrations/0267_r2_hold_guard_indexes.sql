-- guard_r2_delivery_keys_admitted() (0211) ran unindexable per-row predicates:
-- regexp_replace(path) equality and lower(sha256) over recording_clips and
-- recording_collation_outputs, about 2 s per held key. Same accept/reject set:
-- each normalized-path match now also requires path IN (n,'joined/'||n), which
-- regexp_replace(path,'^joined/','')=n already implies (it strips at most one
-- leading 'joined/'), so existing path btrees serve it; content matches get the
-- btrees below.
--
-- Follow 0149/0185: never build a write-blocking index on a large live table.
-- A table over 128 MiB needs the exact index precreated outside this
-- transaction, e.g.:
-- CREATE INDEX CONCURRENTLY r2_hold_guard_clip_content ON recording_clips(lower(sha256));
DO $$
DECLARE ix record;
BEGIN
 FOR ix IN SELECT * FROM (VALUES
  ('recording_clips','r2_hold_guard_clip_content','(lower(sha256))'),
  ('recording_collation_outputs','r2_hold_guard_output_content','(sha256)'),
  ('r2_nonraw_intents','r2_hold_guard_nonraw_content','(sha256)'),
  ('r2_nonraw_intents','r2_hold_guard_nonraw_path','(nas_relative_path)')) v(tbl,name,cols) LOOP
  IF to_regclass(ix.name) IS NULL THEN
   IF pg_relation_size(ix.tbl::regclass)>134217728 THEN
    RAISE EXCEPTION 'precreate % with CREATE INDEX CONCURRENTLY % ON % % before migration', ix.name, ix.name, ix.tbl, ix.cols;
   END IF;
   EXECUTE format('CREATE INDEX %I ON %I %s', ix.name, ix.tbl, ix.cols);
  END IF;
  IF NOT EXISTS(SELECT 1 FROM pg_index i WHERE i.indexrelid=to_regclass(ix.name)
    AND i.indrelid=ix.tbl::regclass AND i.indisvalid AND i.indisready AND NOT i.indisunique
    AND i.indpred IS NULL AND split_part(pg_get_indexdef(i.indexrelid),' USING ',2)='btree '||ix.cols) THEN
   RAISE EXCEPTION '% exists but is not the valid exact hold-guard index: DROP INDEX CONCURRENTLY %; then CREATE INDEX CONCURRENTLY % ON % % before migration', ix.name, ix.name, ix.name, ix.tbl, ix.cols;
  END IF;
 END LOOP;
END $$;

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
       OR EXISTS(SELECT 1 FROM r2_object_retirements x WHERE x.kind='collated' AND x.source_id=o.id)
       OR EXISTS(SELECT 1 FROM recording_clips c JOIN r2_retirement_reservations v ON v.kind='raw' AND v.source_id=c.id
            WHERE c.id=ANY(o.source_clip_ids))
       OR EXISTS(SELECT 1 FROM recording_clips c JOIN r2_inverse_intents i ON i.source_id=c.id
            WHERE c.id=ANY(o.source_clip_ids)))) THEN
   RAISE EXCEPTION 'R2 retirement reservation forbids delivery collated-source closure'; END IF;
 END LOOP;
 RETURN NULL; END $$;
