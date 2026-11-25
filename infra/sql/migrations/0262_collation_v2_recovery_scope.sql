-- Deniz, verbatim: "for the 49 streams (or 50? or as many as we have) i want
-- the best 14 days consecutive days stitched and delivered ... serve the best
-- 14 days of as many streams as possible, fully stitched v85 (or whatever the
-- latest version was we were using), via r2, through the nice web ui".
--
-- This is the one collation write the active writer fence (0187) admits:
-- `stoaramactl collation-v2 run --recovery-scope` PUTs and `collation-v2
-- register` INSERTs of a generation-2 hour whose recording and local date the
-- controller listed in collation_v2_recovery_scope. It only adds: every R2 PUT
-- stays create-only (If-None-Match), the catalog admits only keys it does not
-- already hold or reserve for retirement, and a scoped hour or output can never
-- be updated, deleted or truncated. Every other row and writer stays fenced
-- exactly as before. Each recovery PUT is a durable r2_retirement_writer_operations
-- row, so r2_retirement_writes_blocked and r2_retirement_writers_drained keep
-- every R2 delete batch blocked until it completes.
--
-- Guards left in force on purpose: 0180's source freeze (no new hour for a
-- stream an enabled NAS cleanup plan has frozen), 0167's collation-reference
-- hold (no new reference to a clip under an active cleanup hold), 0183's
-- listing-proof lock (it only waits), and the R2 retirement vetoes on output
-- keys. Each protects a delete; none blocks a scoped day outside one.

-- The controller's explicit list. Removing a row ends that day's admission and
-- its add-only hold; it never removes anything already written.
CREATE TABLE collation_v2_recovery_scope (
 recording_id bigint NOT NULL REFERENCES recordings(id) ON DELETE RESTRICT,
 local_date date NOT NULL,
 approved_at timestamptz NOT NULL DEFAULT now(),
 reason text NOT NULL CHECK (btrim(reason)<>''),
 PRIMARY KEY(recording_id, local_date)
);

-- The recording and local date are read from the hour identity itself
-- (collation.HourIDFor, generation 2), which the planner derives in the
-- recording's cron_timezone.
CREATE FUNCTION collation_v2_recovery_scoped(hour text) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT hour ~ '^[a-z0-9][a-z0-9-]{0,62}__recording-[1-9][0-9]*__date-[0-9]{4}-[0-9]{2}-[0-9]{2}__hour-(0[1-9]|1[0-2])__generation-2$'
  AND EXISTS(SELECT 1 FROM collation_v2_recovery_scope s
   WHERE s.recording_id=substring(hour FROM '__recording-([0-9]+)__')::bigint
    AND s.local_date=substring(hour FROM '__date-([0-9-]{10})__')::date)
$$;

-- An hour row whose columns are exactly its own identity, inside the scope.
CREATE FUNCTION collation_v2_recovery_hour(h recording_collation_hours) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT h.hour_id=format('%s__recording-%s__date-%s__hour-%s__generation-%s', h.batch_id, h.recording_id,
   to_char(h.local_date,'YYYY-MM-DD'), lpad(h.delivery_hour::text,2,'0'), h.generation)
  AND h.manifest_object_key='joined/'||h.batch_id||'/coverage/hours/'||h.hour_id||'.json'
  AND collation_v2_recovery_scoped(h.hour_id)
$$;

-- A key no catalog row holds and no R2 retirement reserves.
CREATE FUNCTION collation_v2_recovery_new_key(k text) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT NOT EXISTS(SELECT 1 FROM recording_collation_outputs WHERE object_key=k)
  AND NOT EXISTS(SELECT 1 FROM recording_collation_hours WHERE manifest_object_key=k)
  AND NOT EXISTS(SELECT 1 FROM r2_retirement_reservations WHERE object_key=k)
  AND NOT EXISTS(SELECT 1 FROM r2_inverse_intents WHERE object_key=k)
  AND NOT EXISTS(SELECT 1 FROM r2_nonraw_intents WHERE object_key=k)
$$;

-- The two keys `collation-v2 run` PUTs for one hour: its manifest and its
-- content-addressed parts in the same batch.
CREATE FUNCTION collation_v2_recovery_write(hour text, k text) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT collation_v2_recovery_scoped(hour)
  AND (k='joined/'||split_part(hour,'__',1)||'/coverage/hours/'||hour||'.json'
   OR k ~ ('^joined/'||split_part(hour,'__',1)||'/objects/[0-9a-f]{64}\.mp4$'))
  AND NOT EXISTS(SELECT 1 FROM recording_collation_hours WHERE hour_id=hour)
  AND collation_v2_recovery_new_key(k)
$$;

-- In every admitted branch: the same shared wire lock as
-- r2_retirement_admit_writer, so no fence transition interleaves.
CREATE FUNCTION r2_retirement_guard_collation_hour() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF collation_v2_recovery_hour(NEW) AND collation_v2_recovery_new_key(NEW.manifest_object_key) THEN
  PERFORM pg_advisory_xact_lock_shared(7343226011187);
 ELSE
  PERFORM r2_retirement_admit_writer();
 END IF;
 RETURN NEW;
END $$;

-- A part is admitted only with its hour, registered by this very transaction:
-- a scoped hour gains parts once and never later. Its key is its own sha256,
-- and it starts on the hour's local date in the recording's cron_timezone.
CREATE FUNCTION r2_retirement_guard_collation_output() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF EXISTS(SELECT 1 FROM recording_collation_hours h JOIN recordings r ON r.id=h.recording_id
   WHERE h.id=NEW.collation_hour_id AND h.xmin=pg_current_xact_id()::xid AND collation_v2_recovery_hour(h)
    AND NEW.object_key='joined/'||h.batch_id||'/objects/'||NEW.sha256||'.mp4'
    AND (NEW.start_at AT TIME ZONE r.cron_timezone)::date=h.local_date)
  AND collation_v2_recovery_new_key(NEW.object_key) THEN
  PERFORM pg_advisory_xact_lock_shared(7343226011187);
 ELSE
  PERFORM r2_retirement_admit_writer();
 END IF;
 RETURN NEW;
END $$;

DROP TRIGGER r2_writer_insert ON recording_collation_hours;
CREATE TRIGGER r2_writer_insert BEFORE INSERT ON recording_collation_hours
 FOR EACH ROW EXECUTE FUNCTION r2_retirement_guard_collation_hour();
DROP TRIGGER r2_writer_insert ON recording_collation_outputs;
CREATE TRIGGER r2_writer_insert BEFORE INSERT ON recording_collation_outputs
 FOR EACH ROW EXECUTE FUNCTION r2_retirement_guard_collation_output();

-- What the scope admits stays as admitted.
CREATE FUNCTION collation_v2_recovery_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF TG_OP='TRUNCATE' THEN
  IF EXISTS(SELECT 1 FROM collation_v2_recovery_scope) THEN
   RAISE EXCEPTION 'collation v2 recovery scope forbids truncating %', TG_TABLE_NAME;
  END IF;
  RETURN NULL;
 END IF;
 -- OLD has a different shape per table, so each branch reads only its own.
 IF TG_TABLE_NAME='recording_collation_hours' THEN
  IF collation_v2_recovery_scoped(OLD.hour_id) THEN
   RAISE EXCEPTION 'collation v2 recovery rows are add-only (% on %)', TG_OP, TG_TABLE_NAME;
  END IF;
 ELSIF EXISTS(SELECT 1 FROM recording_collation_hours h WHERE h.id=OLD.collation_hour_id AND collation_v2_recovery_scoped(h.hour_id)) THEN
  RAISE EXCEPTION 'collation v2 recovery rows are add-only (% on %)', TG_OP, TG_TABLE_NAME;
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER collation_v2_recovery_immutable BEFORE UPDATE OR DELETE ON recording_collation_hours
 FOR EACH ROW EXECUTE FUNCTION collation_v2_recovery_immutable();
CREATE TRIGGER collation_v2_recovery_immutable BEFORE UPDATE OR DELETE ON recording_collation_outputs
 FOR EACH ROW EXECUTE FUNCTION collation_v2_recovery_immutable();
CREATE TRIGGER collation_v2_recovery_immutable_truncate BEFORE TRUNCATE ON recording_collation_hours
 FOR EACH STATEMENT EXECUTE FUNCTION collation_v2_recovery_immutable();
CREATE TRIGGER collation_v2_recovery_immutable_truncate BEFORE TRUNCATE ON recording_collation_outputs
 FOR EACH STATEMENT EXECUTE FUNCTION collation_v2_recovery_immutable();

-- The R2 side: a recovery PUT's durable intent names its hour and exact key.
-- It is an ordinary uncompleted operation, so every drain and delete gate that
-- reads this table still sees it.
ALTER TABLE r2_retirement_writer_operations
 ADD COLUMN collation_recovery_hour_id text,
 ADD COLUMN object_key text,
 ADD CONSTRAINT r2_writer_operations_collation_recovery CHECK ((collation_recovery_hour_id IS NULL)=(object_key IS NULL));

CREATE FUNCTION r2_retirement_guard_writer_operation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NEW.completed_at IS NULL AND NEW.collation_recovery_hour_id IS NOT NULL
  AND collation_v2_recovery_write(NEW.collation_recovery_hour_id, NEW.object_key) THEN
  PERFORM pg_advisory_xact_lock_shared(7343226011187);
 ELSE
  PERFORM r2_retirement_admit_writer();
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER r2_writer_insert ON r2_retirement_writer_operations;
CREATE TRIGGER r2_writer_insert BEFORE INSERT ON r2_retirement_writer_operations
 FOR EACH ROW EXECUTE FUNCTION r2_retirement_guard_writer_operation();

-- The wire admission r2.Client holds across a recovery PUT, in place of
-- r2_retirement_admit_writer: the scope and key are checked again.
CREATE FUNCTION collation_v2_recovery_admit_wire(op bigint) RETURNS void LANGUAGE plpgsql AS $$ BEGIN
 IF EXISTS(SELECT 1 FROM r2_retirement_writer_operations o WHERE o.id=op AND o.completed_at IS NULL
   AND collation_v2_recovery_write(o.collation_recovery_hour_id, o.object_key)) THEN
  PERFORM pg_advisory_xact_lock_shared(7343226011187);
 ELSE
  PERFORM r2_retirement_admit_writer();
 END IF;
END $$;
