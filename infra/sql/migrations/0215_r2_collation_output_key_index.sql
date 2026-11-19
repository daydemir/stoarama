-- The R2 inverse executor's raw target lookup excludes keys that are collation
-- outputs (NOT EXISTS ... recording_collation_outputs WHERE object_key=...).
-- Without an index that probe seq-scans the whole table twice per object, inside
-- the exclusive reserve lock, and caps deletion near one object per second.
--
-- A populated database must precreate the index CONCURRENTLY in its separately
-- reviewed operations window, outside any transaction:
--  CREATE INDEX CONCURRENTLY IF NOT EXISTS r2_collation_outputs_object_key ON recording_collation_outputs(object_key);
-- Empty databases create it inline below.
DO $$
BEGIN
 IF to_regclass('r2_collation_outputs_object_key') IS NULL THEN
  LOCK TABLE recording_collation_outputs IN SHARE MODE;
  IF EXISTS(SELECT 1 FROM recording_collation_outputs LIMIT 1) THEN
   RAISE EXCEPTION 'precreate r2_collation_outputs_object_key concurrently before this migration';
  END IF;
  CREATE INDEX r2_collation_outputs_object_key ON recording_collation_outputs(object_key);
 END IF;
 IF NOT EXISTS(
  SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
  JOIN pg_am a ON a.oid=c.relam JOIN pg_opclass op ON op.oid=i.indclass[0]
  JOIN pg_attribute col ON col.attrelid=i.indrelid AND col.attname='object_key'
  WHERE i.indexrelid=to_regclass('r2_collation_outputs_object_key')
   AND i.indrelid='recording_collation_outputs'::regclass
   AND i.indisvalid AND i.indisready AND i.indislive AND NOT i.indisunique
   AND a.amname='btree' AND i.indnkeyatts=1 AND i.indnatts=1
   AND i.indexprs IS NULL AND i.indpred IS NULL AND i.indoption::text='0'
   AND pg_get_indexdef(i.indexrelid,1,false)='object_key'
   AND i.indkey[0]=col.attnum AND i.indcollation[0]=col.attcollation
   AND op.opcname='text_ops' AND op.opcdefault
 ) THEN
  RAISE EXCEPTION 'r2_collation_outputs_object_key is absent, invalid or has another definition';
 END IF;
END $$;
