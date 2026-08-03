-- These ledgers protect short-lived upload and API retry workflows; they are
-- not historical media records. Production indexes must be built with CREATE
-- INDEX CONCURRENTLY before this transactional migration runs. Fresh databases
-- are small enough to bootstrap normally. Refuse a large unindexed table rather
-- than acquiring a write-blocking SHARE lock during an API deployment.
DO $$
DECLARE
  index_oid REGCLASS := to_regclass('idx_upload_intents_created_at');
  index_ok BOOLEAN;
BEGIN
  IF index_oid IS NULL THEN
    IF pg_relation_size('upload_intents'::regclass) > 134217728 THEN
      RAISE EXCEPTION 'precreate idx_upload_intents_created_at with CREATE INDEX CONCURRENTLY before migration';
    END IF;
    CREATE INDEX idx_upload_intents_created_at ON upload_intents (created_at);
  ELSE
    SELECT i.indisvalid AND i.indisready AND i.indpred IS NULL
           AND i.indrelid = 'upload_intents'::regclass
           AND i.indnkeyatts = 1 AND a.attname = 'created_at'
      INTO index_ok
      FROM pg_index i
      JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=i.indkey[0]
     WHERE i.indexrelid=index_oid;
    IF index_ok IS DISTINCT FROM true THEN
      RAISE EXCEPTION 'idx_upload_intents_created_at exists but is not a valid created_at index on upload_intents';
    END IF;
  END IF;
END $$;

DO $$
DECLARE
  index_oid REGCLASS := to_regclass('idx_api_idempotency_created_at');
  index_ok BOOLEAN;
BEGIN
  IF index_oid IS NULL THEN
    IF pg_relation_size('api_idempotency'::regclass) > 134217728 THEN
      RAISE EXCEPTION 'precreate idx_api_idempotency_created_at with CREATE INDEX CONCURRENTLY before migration';
    END IF;
    CREATE INDEX idx_api_idempotency_created_at ON api_idempotency (created_at);
  ELSE
    SELECT i.indisvalid AND i.indisready AND i.indpred IS NULL
           AND i.indrelid = 'api_idempotency'::regclass
           AND i.indnkeyatts = 1 AND a.attname = 'created_at'
      INTO index_ok
      FROM pg_index i
      JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=i.indkey[0]
     WHERE i.indexrelid=index_oid;
    IF index_ok IS DISTINCT FROM true THEN
      RAISE EXCEPTION 'idx_api_idempotency_created_at exists but is not a valid created_at index on api_idempotency';
    END IF;
  END IF;
END $$;
