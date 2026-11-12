-- Serial permits check expiry and connection lock scope on every file. The
-- pending backlog is irrelevant to both reads; index only outstanding leases.
-- Large tables require exact concurrent precreation outside auto-migration.
DO $$
DECLARE
  table_oid REGCLASS := to_regclass('nas_cleanup_items');
  index_oid REGCLASS := to_regclass('nas_cleanup_items_leased_plan_expiry');
  index_ok BOOLEAN;
BEGIN
  IF table_oid IS NOT NULL THEN
    IF index_oid IS NULL THEN
      IF pg_relation_size(table_oid) > 134217728 THEN
        RAISE EXCEPTION 'precreate nas_cleanup_items_leased_plan_expiry with CREATE INDEX CONCURRENTLY ON nas_cleanup_items(plan_id,lease_expires_at) WHERE state=''leased'' before migration';
      END IF;
      CREATE INDEX nas_cleanup_items_leased_plan_expiry
        ON nas_cleanup_items(plan_id,lease_expires_at) WHERE state='leased';
    ELSE
      SELECT i.indisvalid AND i.indisready AND NOT i.indisunique
             AND i.indrelid=table_oid AND i.indnkeyatts=2 AND i.indnatts=2
             AND am.amname='btree'
             AND pg_get_indexdef(i.indexrelid,1,false)='plan_id'
             AND pg_get_indexdef(i.indexrelid,2,false)='lease_expires_at'
             AND pg_get_expr(i.indpred,i.indrelid)='(state = ''leased''::text)'
        INTO index_ok
        FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
        JOIN pg_am am ON am.oid=c.relam
       WHERE i.indexrelid=index_oid;
      IF index_ok IS DISTINCT FROM true THEN
        RAISE EXCEPTION 'nas_cleanup_items_leased_plan_expiry exists but is not the valid exact leased-state index';
      END IF;
    END IF;
  END IF;
END $$;
