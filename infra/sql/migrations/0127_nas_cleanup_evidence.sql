-- Additive, read-only cleanup qualification evidence. These nullable fields
-- cannot authorize cleanup by themselves. A fresh complete scan must populate
-- the entire group; partial/legacy rows remain explicitly UNKNOWN.
ALTER TABLE nas_inventory_files
  ADD COLUMN file_ctime_ns BIGINT,
  ADD COLUMN file_inode BIGINT,
  ADD COLUMN file_device BIGINT,
  ADD COLUMN sidecar_relative_path TEXT,
  ADD COLUMN sidecar_size_bytes BIGINT,
  ADD COLUMN sidecar_sha256 TEXT;

ALTER TABLE nas_inventory_files
  ADD CONSTRAINT chk_nas_inventory_cleanup_evidence CHECK (
    (file_ctime_ns IS NULL AND file_inode IS NULL AND file_device IS NULL
      AND sidecar_relative_path IS NULL AND sidecar_size_bytes IS NULL AND sidecar_sha256 IS NULL)
    OR
    (file_ctime_ns > 0 AND file_inode > 0 AND file_device > 0
      AND sidecar_relative_path <> '' AND left(sidecar_relative_path,1) <> '/'
      AND sidecar_size_bytes > 0 AND sidecar_sha256 ~ '^[0-9a-f]{64}$')
  ) NOT VALID;
