-- Metadata-only: a constant default adds no table rewrite or backlog update.
-- The migration runner wraps this in a transaction with a 5s lock_timeout.
-- Preserve the monotonic attempt used by client replay fences while granting
-- a fresh three-expired-lease budget to an explicitly rechecked dry-run item.
ALTER TABLE nas_cleanup_items ADD COLUMN recheck_attempt_base integer NOT NULL DEFAULT 0;
