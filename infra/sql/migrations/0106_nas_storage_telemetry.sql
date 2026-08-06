-- Lightweight capacity telemetry belongs on the heartbeat. Detailed file state
-- remains in the paginated NAS inventory tables.
ALTER TABLE connections
  ADD COLUMN nas_storage_total_bytes BIGINT,
  ADD COLUMN nas_storage_free_bytes BIGINT,
  ADD COLUMN nas_storage_reported_at TIMESTAMPTZ;

ALTER TABLE connections
  ADD CONSTRAINT chk_connections_nas_storage
    CHECK (
      (nas_storage_total_bytes IS NULL AND nas_storage_free_bytes IS NULL AND nas_storage_reported_at IS NULL)
      OR
      (nas_storage_total_bytes IS NOT NULL
       AND nas_storage_free_bytes IS NOT NULL
       AND nas_storage_reported_at IS NOT NULL
       AND nas_storage_total_bytes > 0
       AND nas_storage_free_bytes >= 0
       AND nas_storage_free_bytes <= nas_storage_total_bytes)
    ) NOT VALID;
