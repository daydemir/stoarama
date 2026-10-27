ALTER TABLE connections
  ADD COLUMN IF NOT EXISTS inventory_dataset_schema TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS inventory_root_exclusions JSONB NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS inventory_in_progress_dataset_schema TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS inventory_in_progress_root_exclusions JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE connections DROP CONSTRAINT IF EXISTS connections_inventory_scope_shape;
ALTER TABLE connections ADD CONSTRAINT connections_inventory_scope_shape CHECK (
  (inventory_dataset_schema = '' AND inventory_root_exclusions = '[]'::jsonb)
  OR (inventory_dataset_schema = 'raw-dataset-v2' AND inventory_root_exclusions = '["#recycle"]'::jsonb)
) NOT VALID;

ALTER TABLE connections DROP CONSTRAINT IF EXISTS connections_inventory_in_progress_scope_shape;
ALTER TABLE connections ADD CONSTRAINT connections_inventory_in_progress_scope_shape CHECK (
  (inventory_in_progress_dataset_schema = '' AND inventory_in_progress_root_exclusions = '[]'::jsonb)
  OR (inventory_in_progress_dataset_schema = 'raw-dataset-v2' AND inventory_in_progress_root_exclusions = '["#recycle"]'::jsonb)
) NOT VALID;

-- Pre-v2 directory rollups have global scope and may contain excluded roots.
-- Keep authoritative file ledgers intact, but make legacy snapshots unavailable
-- until a completed raw-dataset-v2 scan rebuilds the derived tree.
DELETE FROM nas_inventory_tree_directories;
UPDATE connections SET inventory_tree_generation = '' WHERE inventory_dataset_schema = '';
