ALTER TABLE recording_canary_reservations
  ADD COLUMN IF NOT EXISTS window_start_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS preopen_stage TEXT,
  ADD CONSTRAINT recording_canary_reservations_preopen_stage_check
    CHECK (preopen_stage IS NULL OR preopen_stage IN ('early','confirm'));

CREATE TABLE recording_canary_preopen_evidence (
  reservation_id UUID PRIMARY KEY,
  recording_id BIGINT NOT NULL,
  node_id BIGINT NOT NULL,
  account_id BIGINT NOT NULL,
  window_start_at TIMESTAMPTZ NOT NULL,
  stage TEXT NOT NULL CHECK (stage IN ('early','confirm')),
  duration_ms BIGINT NOT NULL CHECK (duration_ms BETWEEN 10000 AND 30000),
  size_bytes BIGINT NOT NULL CHECK (size_bytes BETWEEN 1 AND 536870912),
  media_sha256 TEXT NOT NULL CHECK (media_sha256 ~ '^[0-9a-f]{64}$'),
  video_codec TEXT NOT NULL CHECK (video_codec ~ '^[a-z0-9._-]{1,32}$'),
  relay_version TEXT NOT NULL CHECK (relay_version ~ '^[A-Za-z0-9._-]{1,64}$'),
  source_revision TEXT NOT NULL CHECK (source_revision ~ '^[A-Za-z0-9._-]{1,64}$'),
  probe_ok BOOLEAN NOT NULL CHECK (probe_ok),
  decode_ok BOOLEAN NOT NULL CHECK (decode_ok),
  native_copy BOOLEAN NOT NULL CHECK (native_copy),
  uploaded BOOLEAN NOT NULL CHECK (NOT uploaded),
  reservation_created_at TIMESTAMPTZ NOT NULL,
  completed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION freeze_recording_canary_preopen_evidence()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'recording canary preopen evidence is append-only';
END;
$$;

CREATE TRIGGER recording_canary_preopen_evidence_no_update_delete
BEFORE UPDATE OR DELETE ON recording_canary_preopen_evidence
FOR EACH ROW EXECUTE FUNCTION freeze_recording_canary_preopen_evidence();

CREATE TRIGGER recording_canary_preopen_evidence_no_truncate
BEFORE TRUNCATE ON recording_canary_preopen_evidence
FOR EACH STATEMENT EXECUTE FUNCTION freeze_recording_canary_preopen_evidence();
