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
