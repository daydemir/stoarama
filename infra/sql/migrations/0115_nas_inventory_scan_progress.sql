ALTER TABLE connections
  ADD COLUMN IF NOT EXISTS inventory_scan_pass_started_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS inventory_scan_rows_visited BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS inventory_scan_rows_skipped BIGINT NOT NULL DEFAULT 0;

ALTER TABLE connections DROP CONSTRAINT IF EXISTS connections_inventory_scan_progress_nonnegative;
ALTER TABLE connections ADD CONSTRAINT connections_inventory_scan_progress_nonnegative CHECK (
  inventory_scan_rows_visited >= 0 AND inventory_scan_rows_skipped >= 0
);
