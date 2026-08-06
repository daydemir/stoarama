-- Durable NAS filesystem reconciliation. Inventory enforcement starts in
-- observe mode so existing pullers can complete a full scan before release is
-- ever gated. The recording_clips row and managed object remain retained after
-- release; this inventory is an additional proof, not a replacement backup.
ALTER TABLE connections
  ADD COLUMN inventory_mode TEXT NOT NULL DEFAULT 'observe',
  ADD COLUMN inventory_generation TEXT NOT NULL DEFAULT '',
  ADD COLUMN inventory_scan_started_at TIMESTAMPTZ,
  ADD COLUMN inventory_scan_completed_at TIMESTAMPTZ,
  ADD COLUMN inventory_reported_at TIMESTAMPTZ,
  ADD COLUMN inventory_clips BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN inventory_bytes BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN inventory_mismatches BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN inventory_unmatched BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN inventory_digest TEXT NOT NULL DEFAULT '';

ALTER TABLE connections
  ADD CONSTRAINT chk_connections_inventory_mode
    CHECK (inventory_mode IN ('observe', 'enforce')),
  ADD CONSTRAINT chk_connections_inventory_counts
    CHECK (inventory_clips >= 0 AND inventory_bytes >= 0 AND inventory_mismatches >= 0 AND inventory_unmatched >= 0),
  ADD CONSTRAINT chk_connections_inventory_digest
    CHECK (inventory_digest = '' OR inventory_digest ~ '^[0-9a-f]{64}$');

CREATE TABLE nas_inventory_files (
  connection_id BIGINT NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
  clip_id BIGINT NOT NULL,
  recording_id BIGINT NOT NULL,
  relative_path TEXT NOT NULL,
  size_bytes BIGINT NOT NULL,
  sha256 TEXT NOT NULL,
  state TEXT NOT NULL,
  verified_at TIMESTAMPTZ,
  file_mtime_ns BIGINT NOT NULL DEFAULT 0,
  seen_generation TEXT NOT NULL DEFAULT '',
  client_updated_at TIMESTAMPTZ NOT NULL,
  server_received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (connection_id, clip_id),
  CONSTRAINT chk_nas_inventory_clip_ids CHECK (clip_id > 0 AND recording_id > 0),
  CONSTRAINT chk_nas_inventory_path CHECK (relative_path <> '' AND left(relative_path, 1) <> '/'),
  CONSTRAINT chk_nas_inventory_size CHECK (size_bytes >= 0),
  CONSTRAINT chk_nas_inventory_sha CHECK (sha256 ~ '^[0-9a-f]{64}$'),
  CONSTRAINT chk_nas_inventory_state CHECK (state IN ('present', 'missing', 'mismatch'))
);

CREATE INDEX idx_nas_inventory_files_connection_path
  ON nas_inventory_files(connection_id, relative_path);
CREATE INDEX idx_nas_inventory_files_connection_state
  ON nas_inventory_files(connection_id, state, clip_id);
CREATE INDEX idx_nas_inventory_files_clip
  ON nas_inventory_files(clip_id);

-- Files without a valid Stoarama sidecar are still part of the filesystem and
-- must be visible. They can never satisfy a clip release proof.
CREATE TABLE nas_inventory_unmatched_files (
  connection_id BIGINT NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
  relative_path TEXT NOT NULL,
  size_bytes BIGINT NOT NULL,
  sha256 TEXT NOT NULL,
  state TEXT NOT NULL,
  file_mtime_ns BIGINT NOT NULL DEFAULT 0,
  seen_generation TEXT NOT NULL DEFAULT '',
  client_updated_at TIMESTAMPTZ NOT NULL,
  server_received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(connection_id,relative_path),
  CONSTRAINT chk_nas_inventory_unmatched_path CHECK (relative_path<>'' AND left(relative_path,1)<>'/'),
  CONSTRAINT chk_nas_inventory_unmatched_size CHECK (size_bytes>=0),
  CONSTRAINT chk_nas_inventory_unmatched_sha CHECK (sha256~'^[0-9a-f]{64}$'),
  CONSTRAINT chk_nas_inventory_unmatched_state CHECK (state IN ('present','missing'))
);

CREATE INDEX idx_nas_inventory_unmatched_state
  ON nas_inventory_unmatched_files(connection_id,state,relative_path);
