-- Executable delivery-only manifests still use generic recovery guards.
-- No hold releases, source-purge exceptions, or delivery holds are changed.
ALTER TABLE r2_retirement_reservations
 DROP CONSTRAINT r2_retirement_reservations_retirement_class_check;
ALTER TABLE r2_retirement_reservations
 ADD CONSTRAINT r2_retirement_reservations_retirement_class_check
 CHECK (retirement_class IN ('', 'owner_accepted_outside_window', 'owner_r2_delivery_only'));

-- Index creation is an explicit CONCURRENTLY operator step, never a
-- write-blocking migration. CLI executable planning/apply fails closed until
-- both exact indexes are ready; ordinary API upgrades remain deployable.
CREATE FUNCTION r2_retirement_delivery_indexes_ready() RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT count(*)=2 FROM (VALUES
  ('nas_inventory_r2_owner_path', 'relative_path',1),
  ('nas_inventory_r2_owner_content', 'sha256',2)
 ) AS spec(name,first_column,columns)
 JOIN pg_index i ON i.indexrelid=to_regclass(spec.name)
 JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am a ON a.oid=c.relam
 WHERE i.indrelid='nas_inventory_files'::regclass
 AND i.indisvalid AND i.indisready AND NOT i.indisunique AND a.amname='btree'
 AND i.indnatts=i.indnkeyatts AND i.indnkeyatts=spec.columns
 AND i.indcollation[0]=(SELECT attcollation FROM pg_attribute WHERE attrelid=i.indrelid AND attname=spec.first_column)
 AND (spec.columns=1 OR i.indcollation[1]=0)
 AND i.indoption[0]=0 AND (spec.columns=1 OR i.indoption[1]=0)
 AND NOT EXISTS(SELECT 1 FROM unnest(i.indclass) op JOIN pg_opclass cls ON cls.oid=op WHERE NOT cls.opcdefault)
 AND pg_get_indexdef(i.indexrelid,1,false)=spec.first_column
 AND (spec.columns=1 OR pg_get_indexdef(i.indexrelid,2,false)='size_bytes')
 AND pg_get_expr(i.indpred,i.indrelid)='(state <> ''missing''::text)'
$$;

-- Make the existing indexed writer-path veto explicit for generic plans too.
-- Unknown/empty paths remain blocked; no absence authority is widened.
CREATE OR REPLACE FUNCTION nas_delivery_fence_object_covered(k text,i bigint) RETURNS boolean LANGUAGE plpgsql STABLE AS $$ BEGIN
 IF nas_delivery_object_span(k,i)<>-1 THEN RETURN false; END IF;
 RETURN NOT EXISTS(SELECT 1 FROM nas_delivery_object_paths(k,i) p WHERE p='' OR
 EXISTS(SELECT 1 FROM recording_clips c WHERE c.display_path<>'' AND c.display_path=p AND nas_delivery_object_span('raw',c.id)<>-1)
 OR EXISTS(SELECT 1 FROM nas_inventory_files n WHERE n.relative_path=p AND n.connection_id=13 AND nas_delivery_object_span('raw',n.clip_id)<>-1)
 OR EXISTS(SELECT 1 FROM recording_collation_outputs o WHERE (o.nas_relative_path=p OR 'joined/'||o.nas_relative_path=p) AND nas_delivery_object_span('collated',o.id)<>-1));
 END $$;
