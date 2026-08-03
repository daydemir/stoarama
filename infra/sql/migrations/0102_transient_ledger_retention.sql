-- These ledgers protect short-lived upload and API retry workflows; they are
-- not historical media records. Index their timestamps so bounded retention
-- can remove expired entries without repeatedly scanning multi-million-row
-- tables. Production creates these indexes before the cleanup rollout; the
-- IF NOT EXISTS form keeps this migration safe there and bootstraps fresh DBs.
CREATE INDEX IF NOT EXISTS idx_upload_intents_created_at
ON upload_intents (created_at);

CREATE INDEX IF NOT EXISTS idx_api_idempotency_created_at
ON api_idempotency (created_at);
