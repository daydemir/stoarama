ALTER TABLE connections
  ADD COLUMN IF NOT EXISTS inventory_scan_skip_reasons JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE connections
  DROP CONSTRAINT IF EXISTS connections_inventory_scan_skip_reasons_object;
ALTER TABLE connections
  ADD CONSTRAINT connections_inventory_scan_skip_reasons_object
  CHECK (jsonb_typeof(inventory_scan_skip_reasons) = 'object');
