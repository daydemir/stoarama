-- Checkpointed, admin-only source HEAD evidence for joined stream-day sealing.
--
-- A preflight run is deliberately separate from recording_joined_stream_days:
-- workers and publication never see it, and the stream day remains pending until
-- the final short transaction verifies every immutable observation.
CREATE TABLE recording_joined_stream_day_head_preflights (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  batch_record_id BIGINT NOT NULL REFERENCES recording_joined_batches(id) ON DELETE RESTRICT,
  stream_day_id BIGINT NOT NULL REFERENCES recording_joined_stream_days(id) ON DELETE RESTRICT,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  connection_id BIGINT NOT NULL REFERENCES connections(id) ON DELETE RESTRICT,
  batch_id TEXT NOT NULL,
  recording_id BIGINT NOT NULL CHECK (recording_id > 0),
  local_date DATE NOT NULL,
  source_snapshot_sha256 TEXT NOT NULL CHECK (source_snapshot_sha256 ~ '^[0-9a-f]{64}$'),
  expected_source_count INTEGER NOT NULL CHECK (expected_source_count >= 0),
  expected_source_bytes BIGINT NOT NULL CHECK (expected_source_bytes >= 0),
  state TEXT NOT NULL DEFAULT 'building' CHECK (state IN ('building','ready','sealed')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ready_at TIMESTAMPTZ,
  sealed_at TIMESTAMPTZ,
  FOREIGN KEY (batch_record_id, account_id, connection_id, batch_id)
    REFERENCES recording_joined_batches(id, account_id, connection_id, batch_id) ON DELETE RESTRICT,
  FOREIGN KEY (batch_record_id, stream_day_id, recording_id)
    REFERENCES recording_joined_stream_days(batch_record_id, id, recording_id) ON DELETE RESTRICT,
  CHECK ((state='building' AND ready_at IS NULL AND sealed_at IS NULL)
      OR (state='ready' AND ready_at IS NOT NULL AND sealed_at IS NULL)
      OR (state='sealed' AND ready_at IS NOT NULL AND sealed_at IS NOT NULL)),
  UNIQUE (stream_day_id),
  UNIQUE (batch_id, recording_id, local_date),
  UNIQUE (id, stream_day_id)
);

CREATE TABLE recording_joined_stream_day_head_observations (
  id BIGSERIAL PRIMARY KEY,
  preflight_id UUID NOT NULL REFERENCES recording_joined_stream_day_head_preflights(id) ON DELETE RESTRICT,
  stream_day_id BIGINT NOT NULL REFERENCES recording_joined_stream_days(id) ON DELETE RESTRICT,
  source_snapshot_id BIGINT NOT NULL REFERENCES recording_joined_source_snapshots(id) ON DELETE RESTRICT,
  day_ordinal INTEGER NOT NULL CHECK (day_ordinal > 0),
  clip_id BIGINT NOT NULL CHECK (clip_id > 0),
  storage_destination_id BIGINT NOT NULL REFERENCES storage_destinations(id) ON DELETE RESTRICT,
  provider TEXT NOT NULL CHECK (provider = btrim(provider) AND provider <> ''),
  endpoint TEXT NOT NULL,
  region TEXT NOT NULL,
  bucket TEXT NOT NULL CHECK (bucket = btrim(bucket) AND bucket <> ''),
  object_key TEXT NOT NULL CHECK (object_key = btrim(object_key) AND object_key <> ''),
  version_id TEXT NOT NULL DEFAULT '',
  etag TEXT NOT NULL CHECK (etag = btrim(etag) AND etag <> ''),
  size_bytes BIGINT NOT NULL CHECK (size_bytes > 0),
  sha256 TEXT NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
  start_at TIMESTAMPTZ NOT NULL,
  end_at TIMESTAMPTZ NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (preflight_id, stream_day_id)
    REFERENCES recording_joined_stream_day_head_preflights(id, stream_day_id) ON DELETE RESTRICT,
  UNIQUE (preflight_id, source_snapshot_id),
  UNIQUE (preflight_id, day_ordinal),
  CHECK (end_at > start_at)
);

CREATE INDEX recording_joined_stream_day_head_observations_next_idx
  ON recording_joined_stream_day_head_observations(preflight_id, day_ordinal);

-- Rows are audit evidence. They are never updated or deleted, and each insert
-- must match the immutable frozen source exactly. The final seal can therefore
-- trust the persisted observation only after rechecking the parent run and day.
CREATE FUNCTION reject_recording_joined_head_observation_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'joined stream-day HEAD observation is immutable';
END $$;
CREATE TRIGGER recording_joined_head_observation_immutable
  BEFORE UPDATE OR DELETE ON recording_joined_stream_day_head_observations
  FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_head_observation_mutation();
CREATE TRIGGER recording_joined_head_observation_no_truncate
  BEFORE TRUNCATE ON recording_joined_stream_day_head_observations
  FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_head_observation_mutation();

CREATE FUNCTION guard_recording_joined_head_observation_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE run RECORD; snapshot RECORD;
BEGIN
  SELECT * INTO STRICT run FROM recording_joined_stream_day_head_preflights
    WHERE id=NEW.preflight_id AND stream_day_id=NEW.stream_day_id FOR SHARE;
  IF run.state <> 'building' THEN RAISE EXCEPTION 'joined HEAD preflight is not building'; END IF;
  SELECT * INTO STRICT snapshot FROM recording_joined_source_snapshots WHERE id=NEW.source_snapshot_id FOR KEY SHARE;
  IF snapshot.stream_day_id <> NEW.stream_day_id
    OR snapshot.clip_id <> NEW.clip_id
    OR snapshot.storage_destination_id <> NEW.storage_destination_id
    OR snapshot.provider <> NEW.provider OR snapshot.endpoint <> NEW.endpoint
    OR snapshot.region <> NEW.region OR snapshot.bucket <> NEW.bucket
    OR snapshot.object_key <> NEW.object_key OR snapshot.ingest_etag <> NEW.etag
    OR snapshot.size_bytes <> NEW.size_bytes OR snapshot.sha256 <> NEW.sha256
    OR snapshot.start_at <> NEW.start_at OR snapshot.end_at <> NEW.end_at
    OR snapshot.day_ordinal <> NEW.day_ordinal
  THEN RAISE EXCEPTION 'joined HEAD observation source identity differs'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_head_observation_insert_guard
  BEFORE INSERT ON recording_joined_stream_day_head_observations
  FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_head_observation_insert();

CREATE FUNCTION guard_recording_joined_head_preflight_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE day RECORD;
BEGIN
  SELECT d.state,d.source_snapshot_sha256,d.source_clip_count,d.source_bytes,d.batch_id,d.recording_id,d.local_date,
         d.account_id,d.connection_id
    INTO STRICT day FROM recording_joined_stream_days d
   WHERE d.id=NEW.stream_day_id AND d.batch_record_id=NEW.batch_record_id FOR SHARE;
  IF day.state <> 'pending' OR NEW.batch_id <> day.batch_id OR NEW.recording_id <> day.recording_id
    OR NEW.local_date <> day.local_date OR NEW.account_id <> day.account_id OR NEW.connection_id <> day.connection_id
    OR NEW.source_snapshot_sha256 <> day.source_snapshot_sha256
    OR NEW.expected_source_count <> day.source_clip_count OR NEW.expected_source_bytes <> day.source_bytes
  THEN RAISE EXCEPTION 'joined HEAD preflight authority differs'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_head_preflight_insert_guard
  BEFORE INSERT ON recording_joined_stream_day_head_preflights
  FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_head_preflight_insert();

CREATE FUNCTION guard_recording_joined_head_preflight_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF ROW(NEW.id,NEW.batch_record_id,NEW.stream_day_id,NEW.account_id,NEW.connection_id,NEW.batch_id,
      NEW.recording_id,NEW.local_date,NEW.source_snapshot_sha256,NEW.expected_source_count,
      NEW.expected_source_bytes,NEW.created_at)
    IS DISTINCT FROM ROW(OLD.id,OLD.batch_record_id,OLD.stream_day_id,OLD.account_id,OLD.connection_id,OLD.batch_id,
      OLD.recording_id,OLD.local_date,OLD.source_snapshot_sha256,OLD.expected_source_count,
      OLD.expected_source_bytes,OLD.created_at)
    OR OLD.state='sealed'
    OR (OLD.state='building' AND NEW.state='ready' AND (
      NEW.ready_at IS NULL OR NEW.sealed_at IS NOT NULL
      OR (SELECT count(*) FROM recording_joined_stream_day_head_observations o WHERE o.preflight_id=OLD.id) <> OLD.expected_source_count
      OR EXISTS (SELECT 1 FROM recording_joined_source_snapshots s
        WHERE s.stream_day_id=OLD.stream_day_id
          AND NOT EXISTS (SELECT 1 FROM recording_joined_stream_day_head_observations o
            WHERE o.preflight_id=OLD.id AND o.source_snapshot_id=s.id))))
    OR (OLD.state='building' AND NEW.state NOT IN ('building','ready'))
    OR (OLD.state='building' AND NEW.state='building' AND (NEW.ready_at IS NOT NULL OR NEW.sealed_at IS NOT NULL))
    OR (OLD.state='ready' AND NEW.state NOT IN ('ready','sealed'))
    OR (OLD.state='ready' AND NEW.state='ready' AND
      (NEW.ready_at IS DISTINCT FROM OLD.ready_at OR NEW.sealed_at IS NOT NULL))
    OR (NEW.state='sealed' AND (NEW.sealed_at IS NULL OR OLD.state<>'ready'
      OR NEW.ready_at IS DISTINCT FROM OLD.ready_at))
  THEN RAISE EXCEPTION 'joined HEAD preflight authority is immutable'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_joined_head_preflight_update_guard
  BEFORE UPDATE ON recording_joined_stream_day_head_preflights
  FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_head_preflight_update();
CREATE FUNCTION reject_recording_joined_head_preflight_delete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'joined HEAD preflight is retention protected'; END $$;
CREATE TRIGGER recording_joined_head_preflight_no_delete
  BEFORE DELETE ON recording_joined_stream_day_head_preflights
  FOR EACH ROW EXECUTE FUNCTION reject_recording_joined_head_preflight_delete();
CREATE TRIGGER recording_joined_head_preflight_no_truncate
  BEFORE TRUNCATE ON recording_joined_stream_day_head_preflights
  FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_head_preflight_delete();
