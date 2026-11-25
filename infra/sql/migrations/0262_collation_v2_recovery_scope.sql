-- Deniz, verbatim: "for the 49 streams (or 50? or as many as we have) i want
-- the best 14 days consecutive days stitched and delivered ... serve the best
-- 14 days of as many streams as possible, fully stitched v85 (or whatever the
-- latest version was we were using), via r2, through the nice web ui".
--
-- This is the one collation write the active writer fence (0187) admits, for a
-- generation-2 hour whose batch, recording and local date the controller
-- listed in collation_v2_recovery_scope. It only adds, in exactly two ways:
--  * Restore: `collation-v2 run --recovery-scope` puts a registered hour's
--    catalogued output or manifest back at its catalogued key, with the
--    catalogued sha256 and size, create-only, after a HEAD under the wire lock
--    finds it absent, and never while a retirement of that key is pending. It
--    writes no catalog row.
--  * New hour: in a fresh `recovery-` batch that no catalog row or writer
--    intent named before its first scope row, `run` creates the hour's manifest
--    and parts (each part keyed by its own sha256) and `collation-v2 register`
--    catalogues them. Nothing older can be adopted under that prefix.
-- A scoped hour or output can never be updated, deleted or truncated, and every
-- key a scope row covers is an owner keep hold (r2_owner_keep_held), so no R2
-- retirement lane can reserve, intend or record its deletion. Scope rows are
-- revoked, never deleted, so a hold outlives its admission. Every other row and
-- writer stays fenced exactly as before. Each recovery PUT is a durable
-- r2_retirement_writer_operations row, so r2_retirement_writes_blocked and
-- r2_retirement_writers_drained keep every R2 delete batch blocked until it
-- completes.
--
-- Guards left in force on purpose: 0180's source freeze (no new hour for a
-- stream an enabled NAS cleanup plan has frozen), 0167's collation-reference
-- hold (no new reference to a clip under an active cleanup hold), 0183's
-- listing-proof lock (it only waits), and the R2 retirement vetoes on output
-- keys. Each protects a delete; none blocks a scoped day outside one.

CREATE TABLE collation_v2_recovery_scope (
 batch_id text NOT NULL CHECK (batch_id ~ '^[a-z0-9][a-z0-9-]{0,62}$'),
 recording_id bigint NOT NULL REFERENCES recordings(id) ON DELETE RESTRICT,
 local_date date NOT NULL,
 approved_at timestamptz NOT NULL DEFAULT now(),
 reason text NOT NULL CHECK (btrim(reason)<>''),
 revoked_at timestamptz,
 PRIMARY KEY(batch_id, recording_id, local_date)
);

-- A retirement of key k, in any bucket, that is admitted but not yet resolved:
-- the predicate 0220/0225 owner keep admission refuses to land a hold over.
CREATE FUNCTION collation_v2_recovery_retirement_pending(k text) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM r2_retirement_reservations x WHERE x.object_key=k
   AND NOT EXISTS(SELECT 1 FROM r2_object_retirements r WHERE r.bucket=x.bucket AND r.object_key=x.object_key))
  OR EXISTS(SELECT 1 FROM r2_inverse_intents i WHERE i.object_key=k
   AND NOT EXISTS(SELECT 1 FROM r2_inverse_events v WHERE v.bucket=i.bucket AND v.object_key=i.object_key AND v.event='absent'))
  OR EXISTS(SELECT 1 FROM r2_keep28_intents i WHERE i.object_key=k
   AND NOT EXISTS(SELECT 1 FROM r2_keep28_events v WHERE v.bucket=i.bucket AND v.object_key=i.object_key AND v.event='absent'))
  OR EXISTS(SELECT 1 FROM r2_nonraw_intents i WHERE i.object_key=k
   AND NOT EXISTS(SELECT 1 FROM r2_nonraw_events v WHERE v.bucket=i.bucket AND v.object_key=i.object_key AND v.event='absent'))
  OR EXISTS(SELECT 1 FROM nas_stitch_r2_stage_retirements r JOIN nas_stitch_r2_stage_objects o ON o.id=r.object_id
   WHERE r.absence_observed_at IS NULL AND o.object_key=k)
$$;

-- Scope rows are inserted, or revoked once; never changed or deleted. A new
-- row takes the owner keep admission locks and refuses to land a hold over a
-- pending retirement of any key it covers. A recovery- batch must be fresh at
-- its first row.
CREATE FUNCTION guard_collation_v2_recovery_scope() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF TG_OP IN ('DELETE','TRUNCATE') THEN
  RAISE EXCEPTION 'collation v2 recovery scope rows are revoked, never deleted';
 END IF;
 IF TG_OP='UPDATE' THEN
  IF OLD.revoked_at IS NOT NULL OR NEW.revoked_at IS NULL OR (to_jsonb(NEW)-'revoked_at')<>(to_jsonb(OLD)-'revoked_at') THEN
   RAISE EXCEPTION 'collation v2 recovery scope rows change only by one revocation';
  END IF;
  RETURN NEW;
 END IF;
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION 'collation v2 recovery scope admission requires READ COMMITTED';
 END IF;
 PERFORM pg_advisory_xact_lock(7343226011103);
 PERFORM pg_advisory_xact_lock(7343226011101);
 PERFORM pg_advisory_xact_lock(7343226011220);
 IF NEW.revoked_at IS NOT NULL THEN
  RAISE EXCEPTION 'collation v2 recovery scope rows start unrevoked';
 END IF;
 IF starts_with(NEW.batch_id,'recovery-')
  AND NOT EXISTS(SELECT 1 FROM collation_v2_recovery_scope WHERE batch_id=NEW.batch_id)
  AND (EXISTS(SELECT 1 FROM recording_collation_hours WHERE batch_id=NEW.batch_id)
   OR EXISTS(SELECT 1 FROM recording_joined_batches WHERE batch_id=NEW.batch_id)
   OR EXISTS(SELECT 1 FROM r2_retirement_writer_operations WHERE starts_with(object_key,'joined/'||NEW.batch_id||'/'))) THEN
  RAISE EXCEPTION 'collation v2 recovery batch % is not fresh', NEW.batch_id;
 END IF;
 IF EXISTS(SELECT 1 FROM recording_collation_hours h
   LEFT JOIN recording_collation_outputs o ON o.collation_hour_id=h.id
   WHERE h.batch_id=NEW.batch_id AND h.recording_id=NEW.recording_id AND h.local_date=NEW.local_date
    AND (collation_v2_recovery_retirement_pending(h.manifest_object_key)
     OR (o.object_key IS NOT NULL AND collation_v2_recovery_retirement_pending(o.object_key)))) THEN
  RAISE EXCEPTION 'collation v2 recovery scope % % % overlaps a pending R2 retirement', NEW.batch_id, NEW.recording_id, NEW.local_date;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER collation_v2_recovery_scope_guard BEFORE INSERT OR UPDATE OR DELETE ON collation_v2_recovery_scope
 FOR EACH ROW EXECUTE FUNCTION guard_collation_v2_recovery_scope();
CREATE TRIGGER collation_v2_recovery_scope_no_truncate BEFORE TRUNCATE ON collation_v2_recovery_scope
 FOR EACH STATEMENT EXECUTE FUNCTION guard_collation_v2_recovery_scope();

-- The batch, recording and local date are read from the hour identity itself
-- (collation.HourIDFor, generation 2), which the planner derives in the
-- recording's cron_timezone. Admission needs an unrevoked row; add-only and the
-- hold need any row.
CREATE FUNCTION collation_v2_recovery_scope_of(hour text, active boolean) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT hour ~ '^[a-z0-9][a-z0-9-]{0,62}__recording-[1-9][0-9]*__date-[0-9]{4}-[0-9]{2}-[0-9]{2}__hour-(0[1-9]|1[0-2])__generation-2$'
  AND EXISTS(SELECT 1 FROM collation_v2_recovery_scope s
   WHERE s.batch_id=split_part(hour,'__',1)
    AND s.recording_id=substring(hour FROM '__recording-([0-9]+)__')::bigint
    AND s.local_date=substring(hour FROM '__date-([0-9-]{10})__')::date
    AND (s.revoked_at IS NULL OR NOT active))
$$;

-- Every key a scope row covers: everything under a recovery- batch, and each
-- catalogued manifest and output of a scoped recording-day.
CREATE FUNCTION collation_v2_recovery_held(k text) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM collation_v2_recovery_scope s
   WHERE starts_with(s.batch_id,'recovery-') AND starts_with(k,'joined/'||s.batch_id||'/'))
  OR EXISTS(SELECT 1 FROM recording_collation_outputs o JOIN recording_collation_hours h ON h.id=o.collation_hour_id
   JOIN collation_v2_recovery_scope s ON s.batch_id=h.batch_id AND s.recording_id=h.recording_id AND s.local_date=h.local_date
   WHERE o.object_key=k)
  OR EXISTS(SELECT 1 FROM recording_collation_hours h
   JOIN collation_v2_recovery_scope s ON s.batch_id=h.batch_id AND s.recording_id=h.recording_id AND s.local_date=h.local_date
   WHERE h.manifest_object_key=k)
$$;

-- Every retirement ledger guard (0220, 0225) and the inverse and nonraw lanes'
-- own selection read r2_owner_keep_held: scoped keys are held there.
CREATE OR REPLACE FUNCTION r2_owner_keep_held(p_bucket text, p_key text) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM r2_owner_keep_prefixes k WHERE k.bucket=p_bucket AND starts_with(p_key,k.prefix))
  OR collation_v2_recovery_held(p_key)
$$;

-- A new hour row whose columns are exactly its own identity, in a scoped
-- recovery- batch.
CREATE FUNCTION collation_v2_recovery_hour(h recording_collation_hours) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT starts_with(h.batch_id,'recovery-')
  AND h.hour_id=format('%s__recording-%s__date-%s__hour-%s__generation-%s', h.batch_id, h.recording_id,
   to_char(h.local_date,'YYYY-MM-DD'), lpad(h.delivery_hour::text,2,'0'), h.generation)
  AND h.manifest_object_key='joined/'||h.batch_id||'/coverage/hours/'||h.hour_id||'.json'
  AND collation_v2_recovery_scope_of(h.hour_id, true)
$$;

-- A key no catalog row holds and no R2 retirement names.
CREATE FUNCTION collation_v2_recovery_new_key(k text) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT NOT EXISTS(SELECT 1 FROM recording_collation_outputs WHERE object_key=k)
  AND NOT EXISTS(SELECT 1 FROM recording_collation_hours WHERE manifest_object_key=k)
  AND NOT EXISTS(SELECT 1 FROM r2_retirement_reservations WHERE object_key=k)
  AND NOT EXISTS(SELECT 1 FROM r2_inverse_intents WHERE object_key=k)
  AND NOT EXISTS(SELECT 1 FROM r2_nonraw_intents WHERE object_key=k)
$$;

-- The keys `collation-v2 run` may PUT for one scoped hour, each named by the
-- exact bytes it carries:
--  * restore: a registered hour's catalogued manifest or output, with its
--    catalogued sha256 (and size), while no retirement of that key is pending;
--  * new hour: in a recovery- batch, the unregistered hour's manifest, or a
--    part whose key is its own sha256, neither held by any catalog row.
CREATE FUNCTION collation_v2_recovery_write(b text, hour text, k text, n bigint, sha text) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT collation_v2_recovery_scope_of(hour, true) AND (
  (EXISTS(SELECT 1 FROM recording_collation_hours h WHERE h.hour_id=hour AND (
     (h.manifest_object_key=k AND h.manifest_sha256=sha)
     OR EXISTS(SELECT 1 FROM recording_collation_outputs o WHERE o.collation_hour_id=h.id AND o.object_key=k AND o.sha256=sha AND o.size_bytes=n)))
   AND NOT collation_v2_recovery_retirement_pending(k))
  OR (starts_with(hour,'recovery-')
   AND (k='joined/'||split_part(hour,'__',1)||'/coverage/hours/'||hour||'.json'
    OR k='joined/'||split_part(hour,'__',1)||'/objects/'||sha||'.mp4')
   AND NOT EXISTS(SELECT 1 FROM recording_collation_hours WHERE hour_id=hour)
   AND collation_v2_recovery_new_key(k)))
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

-- A part is admitted only with its new hour, registered by this very
-- transaction: a scoped hour gains parts once and never later. Its key is its
-- own sha256, and it starts on the hour's local date in the recording's
-- cron_timezone.
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

-- What any scope row ever covered stays as it is.
CREATE FUNCTION collation_v2_recovery_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF TG_OP='TRUNCATE' THEN
  IF EXISTS(SELECT 1 FROM collation_v2_recovery_scope) THEN
   RAISE EXCEPTION 'collation v2 recovery scope forbids truncating %', TG_TABLE_NAME;
  END IF;
  RETURN NULL;
 END IF;
 -- OLD has a different shape per table, so each branch reads only its own.
 IF TG_TABLE_NAME='recording_collation_hours' THEN
  IF collation_v2_recovery_scope_of(OLD.hour_id, false) THEN
   RAISE EXCEPTION 'collation v2 recovery rows are add-only (% on %)', TG_OP, TG_TABLE_NAME;
  END IF;
 ELSIF EXISTS(SELECT 1 FROM recording_collation_hours h WHERE h.id=OLD.collation_hour_id AND collation_v2_recovery_scope_of(h.hour_id, false)) THEN
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

-- The R2 side: a recovery PUT's durable intent names its hour and the exact key,
-- size and sha256 it creates. It is an ordinary uncompleted operation, so every
-- drain and delete gate that reads this table still sees it.
ALTER TABLE r2_retirement_writer_operations
 ADD COLUMN collation_recovery_hour_id text,
 ADD COLUMN object_key text,
 ADD COLUMN size_bytes bigint CHECK (size_bytes>0),
 ADD COLUMN sha256 text CHECK (sha256 ~ '^[0-9a-f]{64}$'),
 ADD CONSTRAINT r2_writer_operations_collation_recovery CHECK (
  (collation_recovery_hour_id IS NULL AND object_key IS NULL AND size_bytes IS NULL AND sha256 IS NULL)
  OR (collation_recovery_hour_id IS NOT NULL AND object_key IS NOT NULL AND size_bytes IS NOT NULL AND sha256 IS NOT NULL));

CREATE FUNCTION r2_retirement_guard_writer_operation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NEW.collation_recovery_hour_id IS NULL THEN
  PERFORM r2_retirement_admit_writer();
 ELSIF NEW.completed_at IS NULL
  AND collation_v2_recovery_write(NEW.bucket, NEW.collation_recovery_hour_id, NEW.object_key, NEW.size_bytes, NEW.sha256) THEN
  PERFORM pg_advisory_xact_lock_shared(7343226011187);
 ELSE
  RAISE EXCEPTION 'collation v2 recovery write % is outside the approved scope', NEW.object_key;
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER r2_writer_insert ON r2_retirement_writer_operations;
CREATE TRIGGER r2_writer_insert BEFORE INSERT ON r2_retirement_writer_operations
 FOR EACH ROW EXECUTE FUNCTION r2_retirement_guard_writer_operation();

-- The wire admission r2.Client holds across a recovery HEAD and PUT, in place
-- of r2_retirement_admit_writer: the scope and key are checked again, and its
-- own intent row stays share-locked, so no settlement completes a PUT in flight.
CREATE FUNCTION collation_v2_recovery_admit_wire(op bigint) RETURNS void LANGUAGE plpgsql AS $$ BEGIN
 PERFORM 1 FROM r2_retirement_writer_operations o WHERE o.id=op AND o.completed_at IS NULL
  AND o.collation_recovery_hour_id IS NOT NULL
  AND collation_v2_recovery_write(o.bucket, o.collation_recovery_hour_id, o.object_key, o.size_bytes, o.sha256) FOR SHARE OF o;
 IF NOT FOUND THEN
  RAISE EXCEPTION 'collation v2 recovery write % is no longer admitted', op;
 END IF;
 PERFORM pg_advisory_xact_lock_shared(7343226011187);
END $$;

-- Completes every unfinished recovery intent for one key, once its caller has
-- read that key's exact bytes back from R2: a late copy of any of those PUTs can
-- now only fail If-None-Match. A row whose PUT is still in flight is skipped.
CREATE FUNCTION collation_v2_recovery_settle(b text, k text, n bigint, sha text) RETURNS integer LANGUAGE sql AS $$
 WITH settled AS (
  UPDATE r2_retirement_writer_operations SET completed_at=clock_timestamp()
  WHERE id IN (SELECT id FROM r2_retirement_writer_operations
   WHERE completed_at IS NULL AND collation_recovery_hour_id IS NOT NULL
    AND bucket=b AND object_key=k AND size_bytes=n AND sha256=sha FOR UPDATE SKIP LOCKED)
  RETURNING 1)
 SELECT count(*)::integer FROM settled
$$;
