-- Observability only; no new deletion authority or default gate changes.
ALTER TABLE nas_cleanup_items ADD COLUMN stage_timings jsonb NOT NULL DEFAULT '{}'::jsonb;
