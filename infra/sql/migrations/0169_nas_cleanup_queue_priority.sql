-- Scheduling only: the immutable exact-file manifest and all deletion gates
-- are unchanged. Higher priority lets a small read-only pilot run before a
-- bulk audit. This never grants deletion authority.
ALTER TABLE nas_cleanup_plans ADD COLUMN priority integer NOT NULL DEFAULT 0
 CHECK (priority BETWEEN -100 AND 100);
CREATE INDEX nas_cleanup_plan_queue ON nas_cleanup_plans(connection_id,priority DESC,id);
