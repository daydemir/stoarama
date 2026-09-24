-- Generation-2 collated hour delivery to the NAS (docs/NAS_JOINED_DELIVERY.md).
-- Requires 0158_recording_collation_v2 (recording_collation_outputs).
--
-- Per-connection policy is server-controlled and OFF by default. The NAS pulls
-- each output's exact object to /clips/joined/<nas_relative_path> and records an
-- exact, append-only acknowledgement. Nothing here deletes or releases data.
ALTER TABLE connections
  ADD COLUMN collated_delivery_enabled BOOLEAN NOT NULL DEFAULT false,
  ADD COLUMN collated_download_bytes_per_sec BIGINT NOT NULL DEFAULT 33554432
    CONSTRAINT connections_collated_rate_check CHECK (collated_download_bytes_per_sec BETWEEN 1048576 AND 1073741824),
  ADD COLUMN collated_download_parallel INTEGER NOT NULL DEFAULT 4
    CONSTRAINT connections_collated_parallel_check CHECK (collated_download_parallel BETWEEN 1 AND 32),
  ADD COLUMN collated_files_pulled BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN collated_bytes_pulled BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN collated_last_ack_at TIMESTAMPTZ,
  ADD COLUMN collated_last_error TEXT NOT NULL DEFAULT ''
    CONSTRAINT connections_collated_last_error_check CHECK (octet_length(collated_last_error) <= 1000),
  ADD COLUMN collated_last_error_output_id BIGINT,
  ADD COLUMN collated_last_error_at TIMESTAMPTZ;

CREATE TABLE recording_collation_output_acks (
  output_id BIGINT NOT NULL REFERENCES recording_collation_outputs(id) ON DELETE RESTRICT,
  connection_id BIGINT NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
  nas_relative_path TEXT NOT NULL,
  size_bytes BIGINT NOT NULL CHECK (size_bytes > 0),
  sha256 TEXT NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
  verified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (output_id, connection_id)
);
CREATE INDEX recording_collation_output_acks_connection_idx ON recording_collation_output_acks (connection_id, output_id);

-- Append-only, except that removing a NAS connection removes its acks (the
-- cascade runs as a nested trigger).
CREATE FUNCTION recording_collation_output_acks_append_only() RETURNS trigger AS $$
BEGIN
  IF TG_OP = 'DELETE' AND pg_trigger_depth() > 1 THEN
    RETURN OLD;
  END IF;
  RAISE EXCEPTION 'recording_collation_output_acks is append-only';
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trg_recording_collation_output_acks_append_only
BEFORE UPDATE OR DELETE ON recording_collation_output_acks
FOR EACH ROW EXECUTE FUNCTION recording_collation_output_acks_append_only();

-- Per-output failure backoff: a failing output (e.g. a conflicting file already
-- at its NAS path) is withheld from the feed until next_attempt_at, so it is
-- neither re-hashed every poll nor able to fill every feed page.
CREATE TABLE recording_collation_output_errors (
  output_id BIGINT NOT NULL REFERENCES recording_collation_outputs(id) ON DELETE RESTRICT,
  connection_id BIGINT NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
  failure_count INTEGER NOT NULL CHECK (failure_count > 0),
  last_error TEXT NOT NULL CHECK (octet_length(last_error) <= 1000),
  last_error_at TIMESTAMPTZ NOT NULL,
  next_attempt_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (output_id, connection_id)
);
