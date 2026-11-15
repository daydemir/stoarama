-- The migration runner is transactional. A large production inventory must
-- first receive these exact indexes in two separate, nontransactional operator
-- statements (one concurrent build at a time on the same table):
-- CREATE INDEX CONCURRENTLY nas_inventory_r2_owner_path ON nas_inventory_files(relative_path) WHERE state<>'missing';
-- CREATE INDEX CONCURRENTLY nas_inventory_r2_owner_content ON nas_inventory_files(sha256,size_bytes) WHERE state<>'missing';
-- Never drop/rebuild an incompatible or invalid index automatically.
DO $$
BEGIN
 IF NOT r2_retirement_delivery_indexes_ready() THEN
  IF pg_relation_size('nas_inventory_files')>134217728 THEN
   RAISE EXCEPTION 'precreate exact nas_inventory_r2_owner_path and nas_inventory_r2_owner_content with CREATE INDEX CONCURRENTLY before migration';
  END IF;
  IF to_regclass('nas_inventory_r2_owner_path') IS NULL THEN
   CREATE INDEX nas_inventory_r2_owner_path ON nas_inventory_files(relative_path) WHERE state<>'missing';
  END IF;
  IF to_regclass('nas_inventory_r2_owner_content') IS NULL THEN
   CREATE INDEX nas_inventory_r2_owner_content ON nas_inventory_files(sha256,size_bytes) WHERE state<>'missing';
  END IF;
 END IF;
 IF NOT r2_retirement_delivery_indexes_ready() THEN
  RAISE EXCEPTION 'delivery presence indexes are not the valid exact indexes required by r2_retirement_delivery_indexes_ready()';
 END IF;
END $$;
