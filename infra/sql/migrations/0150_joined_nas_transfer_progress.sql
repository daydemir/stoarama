BEGIN;

ALTER TABLE connections
  ADD COLUMN joined_transfer_generation INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN joined_transfer_artifact_id BIGINT,
  ADD COLUMN joined_transfer_offset_bytes BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN joined_transfer_operation TEXT NOT NULL DEFAULT '',
  ADD COLUMN joined_transfer_errno_class TEXT NOT NULL DEFAULT '',
  ADD COLUMN joined_transfer_observed_at TIMESTAMPTZ,
  ADD COLUMN joined_transfer_reset_count BIGINT NOT NULL DEFAULT 0,
  ADD CONSTRAINT connections_joined_transfer_chk CHECK (
    (joined_transfer_generation = 0 AND joined_transfer_artifact_id IS NULL AND joined_transfer_offset_bytes = 0
      AND joined_transfer_operation = '' AND joined_transfer_errno_class = '' AND joined_transfer_observed_at IS NULL
      AND joined_transfer_reset_count = 0)
    OR (joined_transfer_generation > 0 AND joined_transfer_artifact_id > 0 AND joined_transfer_offset_bytes >= 0
      AND joined_transfer_operation IN ('range','verify','validate','publish')
      AND joined_transfer_errno_class IN ('','no_space','permission','missing','io','os_error')
      AND joined_transfer_observed_at IS NOT NULL AND joined_transfer_reset_count >= 0)
  );

ALTER TABLE connections ADD CONSTRAINT connections_joined_transfer_artifact_fk
  FOREIGN KEY (joined_transfer_artifact_id) REFERENCES recording_joined_artifacts(id) ON DELETE RESTRICT;

COMMIT;
