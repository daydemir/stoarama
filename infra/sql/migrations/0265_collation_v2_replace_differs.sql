-- Deniz, verbatim: "serve the best 14 days of as many streams as possible,
-- fully stitched v85 (or whatever the latest version was we were using), via
-- r2, through the nice web ui". Approved fallback, on the owner's behalf:
-- "first priority is best 14 stitched"; researchers do not care which FFmpeg
-- did the stream copy.
--
-- A registered hour whose byte-identical restore (0262) reported `differs`
-- (its original FFmpeg nightly cannot be rebuilt) may be stitched again with
-- the current FFmpeg and registered as a generation-3 hour in a fresh recovery-
-- batch that supersedes it. Supersede only: the old generation-2 row, its
-- outputs and its R2 keys are never updated, deleted or overwritten, and they
-- stay held from every retirement lane. The collated NAS feed already serves
-- only the newest generation of each recording hour, so it prefers the
-- replacement with no reader change.
--
-- A replacement is admitted only when all hold:
--  * the controller approved that exact old hour for that recovery batch in
--    collation_v2_recovery_replacements (`collation-v2 approve-replacements`
--    inserts only hours a run reported `differs`);
--  * the new hour's batch, recording and date are in an unrevoked scope row
--    of that recovery batch;
--  * the old hour is generation 2 and no other hour supersedes it;
--  * keys are new and under the recovery batch, as for any new hour (0262).
-- Every other generation-2 shadow stays refused exactly as 0262 refuses it.

-- Every existing hour is generation 2; a generation-3 hour exists only as a
-- replacement, which points at the generation-2 hour it supersedes.
ALTER TABLE recording_collation_hours
 ADD COLUMN supersedes_collation_hour_id bigint UNIQUE REFERENCES recording_collation_hours(id) ON DELETE RESTRICT,
 ADD CONSTRAINT recording_collation_hours_replacement CHECK (
  (generation=2 AND supersedes_collation_hour_id IS NULL)
  OR (generation=3 AND supersedes_collation_hour_id IS NOT NULL AND supersedes_joined_hour_id IS NULL)) NOT VALID;

CREATE TABLE collation_v2_recovery_replacements (
 collation_hour_id bigint PRIMARY KEY REFERENCES recording_collation_hours(id) ON DELETE RESTRICT,
 batch_id text NOT NULL CHECK (batch_id ~ '^recovery-[a-z0-9][a-z0-9-]{0,52}$'),
 approved_at timestamptz NOT NULL DEFAULT now(),
 reason text NOT NULL CHECK (btrim(reason)<>'')
);

-- Approvals are insert-only. Each one holds the old hour's keys for good, so
-- like a scope row it takes the owner keep admission locks and never lands
-- over a pending retirement; it names only a generation-2 hour.
CREATE FUNCTION guard_collation_v2_recovery_replacement() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF TG_OP<>'INSERT' THEN
  RAISE EXCEPTION 'collation v2 recovery replacements are insert-only';
 END IF;
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION 'collation v2 recovery replacement admission requires READ COMMITTED';
 END IF;
 PERFORM pg_advisory_xact_lock(7343226011103);
 PERFORM pg_advisory_xact_lock(7343226011101);
 PERFORM pg_advisory_xact_lock(7343226011220);
 IF NOT EXISTS(SELECT 1 FROM recording_collation_hours h WHERE h.id=NEW.collation_hour_id AND h.generation=2) THEN
  RAISE EXCEPTION 'collation v2 recovery replacement % names no generation-2 hour', NEW.collation_hour_id;
 END IF;
 IF EXISTS(SELECT 1 FROM recording_collation_hours h LEFT JOIN recording_collation_outputs o ON o.collation_hour_id=h.id
   WHERE h.id=NEW.collation_hour_id AND (collation_v2_recovery_retirement_pending(h.manifest_object_key)
    OR (o.object_key IS NOT NULL AND collation_v2_recovery_retirement_pending(o.object_key)))) THEN
  RAISE EXCEPTION 'collation v2 recovery replacement % overlaps a pending R2 retirement', NEW.collation_hour_id;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER collation_v2_recovery_replacement_guard BEFORE INSERT OR UPDATE OR DELETE ON collation_v2_recovery_replacements
 FOR EACH ROW EXECUTE FUNCTION guard_collation_v2_recovery_replacement();
CREATE TRIGGER collation_v2_recovery_replacement_no_truncate BEFORE TRUNCATE ON collation_v2_recovery_replacements
 FOR EACH STATEMENT EXECUTE FUNCTION guard_collation_v2_recovery_replacement();
ALTER TABLE collation_v2_recovery_replacements ENABLE ALWAYS TRIGGER collation_v2_recovery_replacement_guard;
ALTER TABLE collation_v2_recovery_replacements ENABLE ALWAYS TRIGGER collation_v2_recovery_replacement_no_truncate;

-- The scope of a generation-2 or generation-3 hour identity.
CREATE OR REPLACE FUNCTION collation_v2_recovery_scope_of(hour text, active boolean) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT hour ~ '^[a-z0-9][a-z0-9-]{0,62}__recording-[1-9][0-9]*__date-[0-9]{4}-[0-9]{2}-[0-9]{2}__hour-(0[1-9]|1[0-2])__generation-[23]$'
  AND EXISTS(SELECT 1 FROM collation_v2_recovery_scope s
   WHERE s.batch_id=split_part(hour,'__',1)
    AND s.recording_id=substring(hour FROM '__recording-([0-9]+)__')::bigint
    AND s.local_date=substring(hour FROM '__date-([0-9-]{10})__')::date
    AND (s.revoked_at IS NULL OR NOT active))
$$;

-- The generation-2 hour a generation-3 identity in recovery batch b may
-- replace: same recording, date and hour, approved for b, and superseded by no
-- other hour (the replacement's own row, inserted first in its transaction,
-- does not count against its parts).
CREATE FUNCTION collation_v2_recovery_replaceable(hour text) RETURNS bigint LANGUAGE sql STABLE AS $$
 SELECT old.id FROM recording_collation_hours old
  JOIN collation_v2_recovery_replacements a ON a.collation_hour_id=old.id AND a.batch_id=split_part(hour,'__',1)
 WHERE hour ~ '__generation-3$' AND old.generation=2
  AND old.recording_id=substring(hour FROM '__recording-([0-9]+)__')::bigint
  AND old.local_date=substring(hour FROM '__date-([0-9-]{10})__')::date
  AND old.delivery_hour=substring(hour FROM '__hour-([0-9]{2})__')::smallint
  AND NOT EXISTS(SELECT 1 FROM recording_collation_hours newer WHERE newer.supersedes_collation_hour_id=old.id AND newer.hour_id<>hour)
$$;

-- Old keys stay held too: every catalogued output and manifest of an hour an
-- approval names, whether or not its day is scoped.
CREATE OR REPLACE FUNCTION collation_v2_recovery_held(k text) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM collation_v2_recovery_scope s
   WHERE starts_with(s.batch_id,'recovery-') AND starts_with(k,'joined/'||s.batch_id||'/'))
  OR EXISTS(SELECT 1 FROM recording_collation_outputs o JOIN recording_collation_hours h ON h.id=o.collation_hour_id
   JOIN collation_v2_recovery_scope s ON s.batch_id=h.batch_id AND s.recording_id=h.recording_id AND s.local_date=h.local_date
   WHERE o.object_key=k)
  OR EXISTS(SELECT 1 FROM recording_collation_hours h
   JOIN collation_v2_recovery_scope s ON s.batch_id=h.batch_id AND s.recording_id=h.recording_id AND s.local_date=h.local_date
   WHERE h.manifest_object_key=k)
  OR EXISTS(SELECT 1 FROM recording_collation_outputs o JOIN collation_v2_recovery_replacements a ON a.collation_hour_id=o.collation_hour_id
   WHERE o.object_key=k)
  OR EXISTS(SELECT 1 FROM recording_collation_hours h JOIN collation_v2_recovery_replacements a ON a.collation_hour_id=h.id
   WHERE h.manifest_object_key=k)
$$;

-- A new hour row in a scoped recovery- batch: generation 2 for a new hour, or
-- generation 3 superseding exactly the approved generation-2 hour it replaces.
CREATE OR REPLACE FUNCTION collation_v2_recovery_hour(h recording_collation_hours) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT starts_with(h.batch_id,'recovery-') AND collation_v2_recovery_identity(h)
  AND collation_v2_recovery_scope_of(h.hour_id, true)
  AND ((h.generation=2 AND h.supersedes_collation_hour_id IS NULL)
   OR (h.generation=3 AND h.supersedes_joined_hour_id IS NULL
    AND h.supersedes_collation_hour_id=collation_v2_recovery_replaceable(h.hour_id)))
$$;

-- 0262's write admission, with exactly one change in the new-hour branch: a
-- generation-3 identity may stand beside the generation-2 hour it replaces
-- (and nothing else); a generation-2 identity is refused over any registered
-- generation-2 hour as before.
CREATE OR REPLACE FUNCTION collation_v2_recovery_write(b text, hour text, k text, n bigint, sha text) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT collation_v2_recovery_scope_of(hour, true)
  AND NOT EXISTS(SELECT 1 FROM r2_retirement_writer_fences f WHERE f.bucket<>b) AND (
  (EXISTS(SELECT 1 FROM recording_collation_hours h JOIN recording_collation_outputs o ON o.collation_hour_id=h.id
     WHERE h.hour_id=hour AND collation_v2_recovery_identity(h) AND o.object_key=k AND o.sha256=sha AND o.size_bytes=n)
   AND NOT collation_v2_recovery_retirement_pending(k))
  OR (starts_with(hour,'recovery-')
   AND (k='joined/'||split_part(hour,'__',1)||'/coverage/hours/'||hour||'.json'
    OR k='joined/'||split_part(hour,'__',1)||'/objects/'||sha||'.mp4')
   AND NOT EXISTS(SELECT 1 FROM recording_collation_hours h WHERE h.hour_id=hour)
   AND CASE WHEN hour ~ '__generation-3$' THEN collation_v2_recovery_replaceable(hour) IS NOT NULL
    ELSE NOT EXISTS(SELECT 1 FROM recording_collation_hours h
     WHERE h.generation=2 AND h.recording_id=substring(hour FROM '__recording-([0-9]+)__')::bigint
      AND h.local_date=substring(hour FROM '__date-([0-9-]{10})__')::date
      AND h.delivery_hour=substring(hour FROM '__hour-([0-9]{2})__')::smallint) END
   AND collation_v2_recovery_new_key(k)))
$$;

-- 0262's add-only guard, extended to every hour an approval names (and its
-- outputs), scoped or not: a superseded hour stays exactly as it was.
CREATE OR REPLACE FUNCTION collation_v2_recovery_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF TG_OP='TRUNCATE' THEN
  IF EXISTS(SELECT 1 FROM collation_v2_recovery_scope) OR EXISTS(SELECT 1 FROM collation_v2_recovery_replacements) THEN
   RAISE EXCEPTION 'collation v2 recovery scope forbids truncating %', TG_TABLE_NAME;
  END IF;
  RETURN NULL;
 END IF;
 -- OLD has a different shape per table, so each branch reads only its own.
 IF TG_TABLE_NAME='recording_collation_hours' THEN
  IF collation_v2_recovery_scope_of(OLD.hour_id, false)
   OR EXISTS(SELECT 1 FROM collation_v2_recovery_replacements a WHERE a.collation_hour_id=OLD.id) THEN
   RAISE EXCEPTION 'collation v2 recovery rows are add-only (% on %)', TG_OP, TG_TABLE_NAME;
  END IF;
 ELSIF EXISTS(SELECT 1 FROM recording_collation_hours h WHERE h.id=OLD.collation_hour_id AND (collation_v2_recovery_scope_of(h.hour_id, false)
   OR EXISTS(SELECT 1 FROM collation_v2_recovery_replacements a WHERE a.collation_hour_id=h.id))) THEN
  RAISE EXCEPTION 'collation v2 recovery rows are add-only (% on %)', TG_OP, TG_TABLE_NAME;
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END $$;

-- Deniz, verbatim: "keep the old file names agnostic to the stitching tool but
-- yes it's fine just stitched so we know what was done but the files should
-- just be stitched and delivered". A replacement part keeps the delivery path
-- format of the hour it supersedes, which for the same clips is the very same
-- path; the tool and version live only in its manifest and catalog row. So a
-- delivery path is unique per generation, and a generation-3 part may reuse a
-- path only from the hour its own hour supersedes.
ALTER TABLE recording_collation_outputs
 ADD COLUMN generation smallint NOT NULL DEFAULT 2 CHECK (generation IN (2,3));
ALTER TABLE recording_collation_outputs DROP CONSTRAINT recording_collation_outputs_nas_relative_path_key;
CREATE UNIQUE INDEX recording_collation_outputs_path_generation ON recording_collation_outputs(nas_relative_path, generation);

CREATE FUNCTION guard_collation_output_generation() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE h recording_collation_hours%ROWTYPE;
BEGIN
 SELECT * INTO STRICT h FROM recording_collation_hours WHERE id=NEW.collation_hour_id;
 IF NEW.generation<>h.generation THEN
  RAISE EXCEPTION 'collated output generation % differs from its hour''s %', NEW.generation, h.generation;
 END IF;
 IF EXISTS(SELECT 1 FROM recording_collation_outputs o WHERE o.nas_relative_path=NEW.nas_relative_path
   AND o.collation_hour_id IS DISTINCT FROM h.supersedes_collation_hour_id) THEN
  RAISE EXCEPTION 'delivery path % belongs to another hour', NEW.nas_relative_path;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER collation_output_generation BEFORE INSERT ON recording_collation_outputs
 FOR EACH ROW EXECUTE FUNCTION guard_collation_output_generation();
ALTER TABLE recording_collation_outputs ENABLE ALWAYS TRIGGER collation_output_generation;
