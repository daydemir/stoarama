-- Exact per-FFmpeg-attempt source-copy timing evidence. NULL is deliberately
-- accepted for rolling compatibility with older workers; supplied evidence is
-- all-or-none and later consumers must treat NULL as UNKNOWN.
ALTER TABLE recording_clips ADD COLUMN capture_attempt_id UUID;
ALTER TABLE recording_clips ADD COLUMN timestamp_contract_version TEXT;
ALTER TABLE recording_clips ADD COLUMN timestamp_contract JSONB;
ALTER TABLE recording_clips ADD COLUMN timestamp_contract_status TEXT;
ALTER TABLE recording_clips ADD COLUMN timestamp_contract_reason TEXT;
CREATE TABLE recording_timestamp_contract_admissions (
  recording_job_id BIGINT NOT NULL REFERENCES recording_jobs(id) ON DELETE RESTRICT,
  lease_token UUID NOT NULL,
  node_id BIGINT NOT NULL,
  account_id BIGINT NOT NULL,
  recording_id BIGINT NOT NULL REFERENCES recordings(id) ON DELETE RESTRICT,
  policy_version TEXT NOT NULL CHECK(policy_version='continuous-source-pts-v1'),
  admitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(recording_job_id,lease_token),
  UNIQUE(recording_job_id,lease_token,node_id,recording_id)
);

CREATE FUNCTION validate_recording_timestamp_contract_admission() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.admitted_at IS DISTINCT FROM now() THEN
    RAISE EXCEPTION 'timestamp contract admission time must be transaction time';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM recording_jobs j
    JOIN recordings r ON r.id=j.recording_id
    JOIN nodes n ON n.id=NEW.node_id
    WHERE j.id=NEW.recording_job_id AND j.recording_id=NEW.recording_id
      AND j.lease_token=NEW.lease_token AND j.status='leased'
      AND j.lease_owner='node:'||NEW.node_id::text AND j.lease_expires_at>NEW.admitted_at
      AND r.id=NEW.recording_id AND r.account_id=NEW.account_id
      AND n.account_id=NEW.account_id AND n.node_type='relay'
  ) THEN RAISE EXCEPTION 'timestamp contract admission identity mismatch'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER trg_recording_timestamp_contract_admission_validate
BEFORE INSERT ON recording_timestamp_contract_admissions FOR EACH ROW
EXECUTE FUNCTION validate_recording_timestamp_contract_admission();

CREATE FUNCTION recording_timestamp_contract_admission_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'timestamp contract admissions are append-only'; END $$;
CREATE TRIGGER trg_recording_timestamp_contract_admission_append_only
BEFORE UPDATE OR DELETE ON recording_timestamp_contract_admissions FOR EACH ROW
EXECUTE FUNCTION recording_timestamp_contract_admission_append_only();
CREATE TRIGGER trg_recording_timestamp_contract_admission_no_truncate
BEFORE TRUNCATE ON recording_timestamp_contract_admissions
EXECUTE FUNCTION recording_timestamp_contract_admission_append_only();

CREATE FUNCTION valid_recording_clip_timestamp_contract(value JSONB) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
  track JSONB;
  video_count integer := 0;
  audio_count integer := 0;
  media_type text;
  stream_indices integer[] := '{}';
  stream_index integer;
  first_ts bigint;
  last_ts bigint;
BEGIN
  IF jsonb_typeof(value) IS DISTINCT FROM 'object' THEN
    RETURN false;
  END IF;
  IF value->>'version' IS DISTINCT FROM '1'
     OR value->>'mode' IS DISTINCT FROM 'muxed_source_copy'
     OR value->>'audio_selection' IS DISTINCT FROM 'first_optional'
     OR jsonb_typeof(value->'tracks') IS DISTINCT FROM 'array' THEN
    RETURN false;
  END IF;
  IF jsonb_array_length(value->'tracks') NOT BETWEEN 1 AND 2 THEN
    RETURN false;
  END IF;
  FOR track IN SELECT * FROM jsonb_array_elements(value->'tracks') LOOP
    IF jsonb_typeof(track) IS DISTINCT FROM 'object' THEN
      RETURN false;
    END IF;
    IF jsonb_typeof(track->'stream_index') IS DISTINCT FROM 'number'
       OR jsonb_typeof(track->'media_type') IS DISTINCT FROM 'string'
       OR jsonb_typeof(track->'time_base_num') IS DISTINCT FROM 'number'
       OR jsonb_typeof(track->'time_base_den') IS DISTINCT FROM 'number'
       OR jsonb_typeof(track->'first_timestamp') IS DISTINCT FROM 'number'
       OR jsonb_typeof(track->'last_timestamp') IS DISTINCT FROM 'number'
       OR jsonb_typeof(track->'last_duration') IS DISTINCT FROM 'number'
       OR jsonb_typeof(track->'unit_count') IS DISTINCT FROM 'number'
       OR jsonb_typeof(track->'codec_signature_sha256') IS DISTINCT FROM 'string' THEN
      RETURN false;
    END IF;
    media_type := track->>'media_type';
    stream_index := (track->>'stream_index')::integer;
    IF stream_index = ANY(stream_indices) THEN RETURN false; END IF;
    stream_indices := array_append(stream_indices,stream_index);
    IF media_type = 'video' THEN video_count := video_count + 1;
    ELSIF media_type = 'audio' THEN audio_count := audio_count + 1;
    ELSE RETURN false;
    END IF;
    first_ts := (track->>'first_timestamp')::bigint;
    last_ts := (track->>'last_timestamp')::bigint;
    IF stream_index < 0
       OR (track->>'time_base_num')::bigint NOT BETWEEN 1 AND 1000000000
       OR (track->>'time_base_den')::bigint NOT BETWEEN 1 AND 1000000000
       OR last_ts < first_ts
       OR (track->>'last_duration')::bigint <= 0
       OR (track->>'unit_count')::bigint NOT BETWEEN 1 AND 100000000
       OR track->>'codec_signature_sha256' !~ '^[0-9a-f]{64}$' THEN
      RETURN false;
    END IF;
    IF media_type = 'audio' AND
       (jsonb_typeof(track->'sample_rate') IS DISTINCT FROM 'number'
        OR jsonb_typeof(track->'last_sample_count') IS DISTINCT FROM 'number'
        OR (track->>'sample_rate')::bigint NOT BETWEEN 8000 AND 768000
        OR (track->>'last_sample_count')::bigint NOT BETWEEN 1 AND 1000000) THEN
      RETURN false;
    END IF;
  END LOOP;
  RETURN video_count = 1 AND audio_count <= 1;
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
  RETURN false;
END $$;

ALTER TABLE recording_clips ADD CONSTRAINT recording_clips_timestamp_contract_coherent CHECK (
  (capture_attempt_id IS NULL AND timestamp_contract_version IS NULL AND timestamp_contract IS NULL AND timestamp_contract_status IS NULL AND timestamp_contract_reason IS NULL)
  OR
  (capture_lease_token IS NOT NULL AND capture_sequence IS NOT NULL
   AND capture_attempt_id IS NOT NULL
   AND timestamp_contract_status IN('per_clip_probe_complete','per_clip_probe_unknown')
   AND (timestamp_contract_reason IS NULL OR timestamp_contract_reason IN('missing_terminal_duration','missing_audio_sample_count','invalid_time_base','probe_output_limit','probe_unavailable'))
   AND ((timestamp_contract_status='per_clip_probe_complete' AND timestamp_contract_reason IS NULL AND timestamp_contract_version='continuous-source-pts-v1'
         AND valid_recording_clip_timestamp_contract(timestamp_contract))
     OR (timestamp_contract_status='per_clip_probe_unknown' AND timestamp_contract_reason IS NOT NULL AND timestamp_contract_reason<>'' AND timestamp_contract_version IS NULL AND timestamp_contract IS NULL)))
) NOT VALID;
-- The table is large and every historical row has NULL capture_attempt_id. Defer
-- the empty partial index to a later concurrent migration after canary evidence
-- exists; building it here would take an avoidable live-ingest lock/scan.

CREATE FUNCTION recording_clip_capture_provenance_immutable() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.capture_lease_token IS DISTINCT FROM OLD.capture_lease_token
     OR NEW.capture_sequence IS DISTINCT FROM OLD.capture_sequence
     OR NEW.capture_attempt_id IS DISTINCT FROM OLD.capture_attempt_id
     OR NEW.timestamp_contract_version IS DISTINCT FROM OLD.timestamp_contract_version
     OR NEW.timestamp_contract IS DISTINCT FROM OLD.timestamp_contract
     OR NEW.timestamp_contract_status IS DISTINCT FROM OLD.timestamp_contract_status
     OR NEW.timestamp_contract_reason IS DISTINCT FROM OLD.timestamp_contract_reason THEN
    RAISE EXCEPTION 'recording clip capture provenance is immutable';
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER trg_recording_clip_capture_provenance_immutable
BEFORE UPDATE ON recording_clips FOR EACH ROW
EXECUTE FUNCTION recording_clip_capture_provenance_immutable();
