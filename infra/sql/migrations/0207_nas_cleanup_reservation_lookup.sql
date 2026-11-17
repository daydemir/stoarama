-- On populated databases, first apply 0206, then outside any transaction:
-- CREATE INDEX CONCURRENTLY nas_cleanup_manifest_keys_idx ON nas_cleanup_plans
-- USING gin(nas_cleanup_manifest_keys(manifest)) WITH (fastupdate=off);
-- Keep the read path out of GIN's pending list: on a 1,438-plan fixture a
-- large pending list made the generic planner choose a manifest-wide scan.
-- No plan, item, hold or reservation authority changes.
DO $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_proc WHERE oid='nas_cleanup_manifest_keys(jsonb)'::regprocedure
   AND provolatile='i' AND proisstrict AND proparallel='s'
   AND md5(prosrc)='5ac9662bb8c2a47880021e1831c57e49') THEN
  RAISE EXCEPTION 'nas_cleanup_manifest_keys differs from the indexed immutable projection';
 END IF;
 IF to_regclass('nas_cleanup_manifest_keys_idx') IS NULL THEN
  LOCK TABLE nas_cleanup_plans IN SHARE MODE;
  IF EXISTS(SELECT 1 FROM nas_cleanup_plans LIMIT 1) THEN
   RAISE EXCEPTION 'apply 0206 then precreate nas_cleanup_manifest_keys_idx concurrently before 0207';
  END IF;
  CREATE INDEX nas_cleanup_manifest_keys_idx ON nas_cleanup_plans USING gin(nas_cleanup_manifest_keys(manifest)) WITH (fastupdate=off);
 END IF;
 IF NOT EXISTS(SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am a ON a.oid=c.relam
   JOIN pg_opclass op ON op.oid=i.indclass[0]
   WHERE i.indexrelid='nas_cleanup_manifest_keys_idx'::regclass AND i.indrelid='nas_cleanup_plans'::regclass
   AND i.indisvalid AND i.indisready AND i.indislive AND NOT i.indisunique
   AND a.amname='gin' AND op.opcname='array_ops' AND op.opcdefault
   AND c.reloptions=ARRAY['fastupdate=off']
   AND i.indnkeyatts=1 AND i.indnatts=1 AND i.indpred IS NULL
   AND pg_get_expr(i.indexprs,i.indrelid)='nas_cleanup_manifest_keys(manifest)') THEN
  RAISE EXCEPTION 'nas_cleanup_manifest_keys_idx is not the exact valid reservation index';
 END IF;
END $$;

-- Same rule as nas_cleanup_reserved_files, including ordinal-specific issuance
-- and missing item rows. The index narrows plans BEFORE decoding their manifests.
CREATE FUNCTION nas_cleanup_file_reserved(reserved_kind text,reserved_source bigint)
RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM nas_cleanup_plans p
 CROSS JOIN LATERAL jsonb_array_elements(p.manifest) WITH ORDINALITY e(value,n)
 LEFT JOIN nas_cleanup_items i ON i.plan_id=p.id AND i.ordinal=e.n
 WHERE nas_cleanup_manifest_keys(p.manifest) @> ARRAY[reserved_kind||':'||reserved_source::text]
   AND e.value->>'kind'=reserved_kind AND (e.value->>'source_id')::bigint=reserved_source
   AND (p.mode<>'retired' OR i.delete_ever_issued OR i.id IS NULL))
$$;
