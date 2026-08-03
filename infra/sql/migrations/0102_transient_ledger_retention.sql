-- These ledgers protect short-lived upload and API retry workflows; they are
-- not historical media records. Production indexes must be built with CREATE
-- INDEX CONCURRENTLY before this transactional migration runs. Fresh databases
-- are small enough to bootstrap normally. Refuse a large unindexed table rather
-- than acquiring a write-blocking SHARE lock during an API deployment.
DO $$
DECLARE estimated_rows REAL;
BEGIN
  IF to_regclass('idx_upload_intents_created_at') IS NULL THEN
    SELECT reltuples INTO estimated_rows FROM pg_class WHERE oid='upload_intents'::regclass;
    IF estimated_rows > 100000 THEN
      RAISE EXCEPTION 'precreate idx_upload_intents_created_at with CREATE INDEX CONCURRENTLY before migration';
    END IF;
    CREATE INDEX idx_upload_intents_created_at ON upload_intents (created_at);
  END IF;
END $$;

DO $$
DECLARE estimated_rows REAL;
BEGIN
  IF to_regclass('idx_api_idempotency_created_at') IS NULL THEN
    SELECT reltuples INTO estimated_rows FROM pg_class WHERE oid='api_idempotency'::regclass;
    IF estimated_rows > 100000 THEN
      RAISE EXCEPTION 'precreate idx_api_idempotency_created_at with CREATE INDEX CONCURRENTLY before migration';
    END IF;
    CREATE INDEX idx_api_idempotency_created_at ON api_idempotency (created_at);
  END IF;
END $$;
