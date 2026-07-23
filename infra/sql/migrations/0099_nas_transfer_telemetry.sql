ALTER TABLE connections
  ADD COLUMN IF NOT EXISTS nas_batch_completed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS nas_batch_clips INT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS nas_batch_bytes BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS nas_batch_duration_ms BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS nas_download_workers INT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS nas_batch_retries INT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS nas_batch_failures INT NOT NULL DEFAULT 0;

ALTER TABLE connections
  DROP CONSTRAINT IF EXISTS chk_connections_nas_transfer_telemetry,
  ADD CONSTRAINT chk_connections_nas_transfer_telemetry CHECK (
    nas_batch_clips >= 0
    AND nas_batch_bytes >= 0
    AND nas_batch_duration_ms >= 0
    AND nas_download_workers BETWEEN 0 AND 32
    AND nas_batch_retries >= 0
    AND nas_batch_failures >= 0
  );
