BEGIN;

-- Additive: the upload probe also measures the NAS downlink by downloading the
-- objects it just uploaded, before the server deletes them. Old clients leave
-- these columns NULL.
ALTER TABLE nas_upload_probes
  ADD COLUMN IF NOT EXISTS bytes_downloaded BIGINT CHECK (bytes_downloaded >= 0),
  ADD COLUMN IF NOT EXISTS download_duration_ms BIGINT CHECK (download_duration_ms >= 1),
  ADD COLUMN IF NOT EXISTS download_error TEXT NOT NULL DEFAULT '';

COMMIT;
