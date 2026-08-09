-- Lean NAS tree index. Leaf rows stay in the authoritative live ledgers; only
-- directory aggregates are materialized at completed-scan boundaries.
ALTER TABLE connections
  ADD COLUMN IF NOT EXISTS inventory_tree_generation TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS inventory_live_revision BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS inventory_tree_revision BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS inventory_in_progress_generation TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS inventory_in_progress_started_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS inventory_in_progress_reported_at TIMESTAMPTZ;

ALTER TABLE nas_inventory_files
  ADD COLUMN IF NOT EXISTS tree_parent_path TEXT,
  ADD COLUMN IF NOT EXISTS tree_name TEXT;
ALTER TABLE nas_inventory_unmatched_files
  ADD COLUMN IF NOT EXISTS tree_parent_path TEXT,
  ADD COLUMN IF NOT EXISTS tree_name TEXT;

-- These indexes are initially tiny because the additive derived columns are
-- nullable. Full-scan pages populate one connection through ordinary bounded
-- upserts; completion never rewrites the whole live ledger.
CREATE INDEX IF NOT EXISTS idx_nas_inventory_files_tree
  ON nas_inventory_files(connection_id,tree_parent_path,tree_name)
  WHERE state IN('present','mismatch') AND tree_parent_path IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_nas_inventory_unmatched_tree
  ON nas_inventory_unmatched_files(connection_id,tree_parent_path,tree_name)
  WHERE state='present' AND tree_parent_path IS NOT NULL;

CREATE TABLE IF NOT EXISTS nas_inventory_tree_directories (
  connection_id BIGINT NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
  generation TEXT NOT NULL,
  parent_path TEXT NOT NULL,
  name TEXT NOT NULL,
  size_bytes BIGINT NOT NULL,
  descendant_files BIGINT NOT NULL,
  mismatch_files BIGINT NOT NULL,
  nas_only_files BIGINT NOT NULL,
  stale_files BIGINT NOT NULL,
  ambiguous_files BIGINT NOT NULL,
  PRIMARY KEY(connection_id,generation,parent_path,name),
  CONSTRAINT chk_nas_tree_directory_counts CHECK(size_bytes>=0 AND descendant_files>=0 AND mismatch_files>=0 AND nas_only_files>=0 AND stale_files>=0 AND ambiguous_files>=0)
);
