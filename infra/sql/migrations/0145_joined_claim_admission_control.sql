ALTER TABLE recording_joined_batches
  ADD CONSTRAINT recording_joined_batches_id_batch_id_unique UNIQUE(id,batch_id);

CREATE TABLE recording_joined_admission_controls (
  batch_record_id BIGINT PRIMARY KEY,
  batch_id TEXT NOT NULL UNIQUE,
  claims_paused BOOLEAN NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT recording_joined_admission_controls_batch_fk
    FOREIGN KEY (batch_record_id,batch_id)
    REFERENCES recording_joined_batches(id,batch_id) ON DELETE RESTRICT
);

INSERT INTO recording_joined_admission_controls(batch_record_id,batch_id,claims_paused)
SELECT id,batch_id,TRUE FROM recording_joined_batches;

CREATE FUNCTION create_recording_joined_admission_control() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO recording_joined_admission_controls(batch_record_id,batch_id,claims_paused)
  VALUES(NEW.id,NEW.batch_id,TRUE);
  RETURN NEW;
END $$;

CREATE TRIGGER recording_joined_admission_control_create
AFTER INSERT ON recording_joined_batches
FOR EACH ROW EXECUTE FUNCTION create_recording_joined_admission_control();

CREATE FUNCTION guard_recording_joined_admission_control() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='DELETE' THEN RAISE EXCEPTION 'joined admission control cannot be deleted'; END IF;
  IF NEW.batch_record_id IS DISTINCT FROM OLD.batch_record_id OR NEW.batch_id IS DISTINCT FROM OLD.batch_id
    OR NEW.updated_at<=OLD.updated_at
  THEN RAISE EXCEPTION 'joined admission control identity or time differs'; END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER recording_joined_admission_control_guard
BEFORE UPDATE OR DELETE ON recording_joined_admission_controls
FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_admission_control();

CREATE FUNCTION reject_recording_joined_admission_control_truncate() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'joined admission controls cannot be truncated'; END $$;

CREATE TRIGGER recording_joined_admission_control_no_truncate
BEFORE TRUNCATE ON recording_joined_admission_controls
FOR EACH STATEMENT EXECUTE FUNCTION reject_recording_joined_admission_control_truncate();
