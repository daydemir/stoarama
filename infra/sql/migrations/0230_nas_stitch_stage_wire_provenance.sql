-- A Durham stage absence is publishable only by the invocation that saw its own
-- DELETE acknowledged, then an empty exact-key multipart inventory, then a
-- strict HEAD 404 for that exact key. Two older receipts cannot prove any part
-- of that:
--
--  * `absence_observed_at` alone was a bare 404 (0217), which a delete marker
--    or a conflicting provider error could satisfy.
--  * `strict_absence_observed_at` (0219) tightened the provider read but was
--    still recorded by a *later* reconciliation pass, so it never proved that
--    the same invocation's own DELETE produced the absence.
--
-- Both are therefore legacy evidence. They are preserved exactly as recorded,
-- never edited, never reissued, and never treated as resolved: an intent that
-- carries only legacy evidence stays held, and the fence, dependency, NAS
-- cleanup and owner-keep guards below keep holding it. Migration 0222 already
-- established the same rule for pre-upgrade staged results.
--
-- `wire_absence_observed_at` is the distinct durable provenance the current
-- command writes: one stamp, in the same UPDATE that records the absence, only
-- after its own answered DELETE and its own empty-inventory strict readback.
-- The guard makes that atomic and one-shot: it may only appear together with
-- the other two stamps, all equal, on a row that had none of them.
--
-- Why a new migration rather than an edit: 0217/0219/0220 are deployed, so
-- their files are immutable here. This one replaces only the functions that
-- decide "resolved", and does not touch any deployed constraint that would
-- have to be re-derived from live data.
--
-- Operator sequencing: apply with the new controller. A stage confirmation sent
-- by an older binary now fails its UPDATE and leaves the intent held. That is
-- the intended refusal, not data loss: the row, its issued_at and any legacy
-- absence stamp are untouched, and no DELETE is reissued for it.

ALTER TABLE nas_stitch_r2_stage_retirements ADD COLUMN wire_absence_observed_at timestamptz;
ALTER TABLE nas_stitch_r2_stage_retirements ADD CONSTRAINT nas_stitch_r2_stage_wire_absence_stamped
 CHECK(wire_absence_observed_at IS NULL OR
  (absence_observed_at IS NOT NULL AND strict_absence_observed_at IS NOT NULL
   AND wire_absence_observed_at=strict_absence_observed_at));

-- The one predicate every reader shares. A NULL input (including a LEFT JOIN
-- with no retirement row) is unresolved, so absence of a row never reads as
-- success. No index is added here: the deployed `nas_stitch_r2_stage_unresolved`
-- partial index already covers the held rows, which are the ones any query
-- filters on.
CREATE FUNCTION nas_stitch_r2_stage_retirement_resolved(p_absence timestamptz, p_strict timestamptz, p_wire timestamptz)
RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
 SELECT p_absence IS NOT NULL AND p_strict IS NOT NULL AND p_wire IS NOT NULL
$$;

-- One-shot, and only for the new provenance. An INSERT may create the bare
-- intent and nothing else. An UPDATE may stamp all three observations together
-- on a row that carried none of them; nothing may add the new stamp to a row
-- that already has a legacy observation, which is exactly what keeps those held.
CREATE OR REPLACE FUNCTION guard_nas_stitch_r2_stage_retirement() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='INSERT' AND NEW.absence_observed_at IS NULL AND NEW.strict_absence_observed_at IS NULL
  AND NEW.wire_absence_observed_at IS NULL THEN
  RETURN NEW;
 END IF;
 IF TG_OP='UPDATE' AND NEW.object_id=OLD.object_id AND NEW.issued_at=OLD.issued_at
  AND OLD.absence_observed_at IS NULL AND OLD.strict_absence_observed_at IS NULL
  AND OLD.wire_absence_observed_at IS NULL
  AND NEW.absence_observed_at IS NOT NULL
  AND NEW.strict_absence_observed_at=NEW.absence_observed_at
  AND NEW.wire_absence_observed_at=NEW.absence_observed_at THEN
  RETURN NEW;
 END IF;
 RAISE EXCEPTION 'stitch staging retirement intent and absence observations are one-shot' USING ERRCODE='55000';
END $$;

-- Dependent delivery, collation and raw admissions stay blocked until the new
-- provenance resolves the intent.
CREATE OR REPLACE FUNCTION guard_nas_stitch_r2_stage_dependency() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE entry jsonb:=to_jsonb(NEW);
BEGIN
 IF EXISTS(SELECT 1 FROM nas_stitch_r2_stage_retirements d JOIN nas_stitch_r2_stage_objects o ON o.id=d.object_id
  WHERE NOT nas_stitch_r2_stage_retirement_resolved(d.absence_observed_at,d.strict_absence_observed_at,d.wire_absence_observed_at)
  AND (
   (o.object_key=entry->>'object_key' AND (entry->>'bucket' IS NULL OR o.bucket=entry->>'bucket'))
   OR o.relative_path=coalesce(entry->>'relative_path',entry->>'nas_relative_path',entry->>'display_path')
   OR (TG_TABLE_NAME<>'recording_clips' AND o.sha256=entry->>'sha256' AND o.size_bytes::text=entry->>'size_bytes'))) THEN
  RAISE EXCEPTION 'unresolved stitch staging retirement protects dependency admission' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END $$;

-- Fence removal keeps waiting on every held intent, legacy evidence included.
CREATE OR REPLACE FUNCTION guard_r2_stitch_fence_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION 'R2 stitch fence transition requires READ COMMITTED' USING ERRCODE='55000';
 END IF;
 IF NOT pg_try_advisory_xact_lock(7343226011187) THEN
  RAISE EXCEPTION 'R2 stitch staging admission is busy' USING ERRCODE='55P03';
 END IF;
 -- Take a fresh READ COMMITTED snapshot after acquiring the transition lock.
 IF EXISTS(SELECT 1 FROM nas_stitch_r2_stage_objects
   WHERE state IN ('starting','uploading','completing') OR grants_drain_at>=clock_timestamp())
  OR EXISTS(SELECT 1 FROM nas_stitch_r2_stage_retirements
   WHERE NOT nas_stitch_r2_stage_retirement_resolved(absence_observed_at,strict_absence_observed_at,wire_absence_observed_at)) THEN
  RAISE EXCEPTION 'R2 stitch staging writers or unresolved retirements have not drained';
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END $$;

-- Certified NAS cleanup still waits for a resolved stage retirement, and a
-- legacy stamp no longer resolves one.
CREATE OR REPLACE FUNCTION nas_cleanup_certified_stitch_staging_absent(certificate jsonb) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM nas_stitch_r2_certificates c
  WHERE c.hour_id=(certificate->>'hour_id')::bigint AND c.attempt=(certificate->>'attempt')::int
   AND c.plan_sha256=certificate->>'plan_sha256' AND c.manifest_sha256=certificate->>'manifest_sha256'
   AND EXISTS(SELECT 1 FROM nas_stitch_r2_stage_objects o WHERE o.hour_id=c.hour_id AND o.attempt=c.attempt AND o.state='staged')
   AND NOT EXISTS(SELECT 1 FROM nas_stitch_r2_stage_objects o
    LEFT JOIN nas_stitch_r2_stage_retirements retired ON retired.object_id=o.id AND o.state='staged'
    LEFT JOIN nas_stitch_r2_aborted_absences aborted ON aborted.object_id=o.id AND o.state='aborted'
    WHERE o.hour_id=c.hour_id
     AND NOT nas_stitch_r2_stage_retirement_resolved(retired.absence_observed_at,retired.strict_absence_observed_at,retired.wire_absence_observed_at)
     AND aborted.object_id IS NULL))
$$;

-- Owner keep admission keeps the exact lock order it was installed with
-- (1103, 1101, 1220 exclusive) and keeps refusing to land over a held intent.
CREATE OR REPLACE FUNCTION guard_r2_owner_keep_admission() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 -- Each check below must see what committed before the locks were granted.
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION 'R2 owner keep admission requires READ COMMITTED';
 END IF;
 PERFORM pg_advisory_xact_lock(7343226011103);
 PERFORM pg_advisory_xact_lock(7343226011101);
 PERFORM pg_advisory_xact_lock(7343226011220);
 IF EXISTS(SELECT 1 FROM r2_retirement_reservations x WHERE x.bucket=NEW.bucket AND starts_with(x.object_key,NEW.prefix)
    AND NOT EXISTS(SELECT 1 FROM r2_object_retirements r WHERE r.bucket=x.bucket AND r.object_key=x.object_key))
  OR EXISTS(SELECT 1 FROM r2_inverse_intents i WHERE i.bucket=NEW.bucket AND starts_with(i.object_key,NEW.prefix)
    AND NOT EXISTS(SELECT 1 FROM r2_inverse_events v WHERE v.bucket=i.bucket AND v.object_key=i.object_key AND v.event='absent'))
  OR EXISTS(SELECT 1 FROM r2_keep28_intents i WHERE i.bucket=NEW.bucket AND starts_with(i.object_key,NEW.prefix)
    AND NOT EXISTS(SELECT 1 FROM r2_keep28_events v WHERE v.bucket=i.bucket AND v.object_key=i.object_key AND v.event='absent'))
  OR EXISTS(SELECT 1 FROM nas_stitch_r2_stage_retirements r JOIN nas_stitch_r2_stage_objects o ON o.id=r.object_id
    WHERE NOT nas_stitch_r2_stage_retirement_resolved(r.absence_observed_at,r.strict_absence_observed_at,r.wire_absence_observed_at)
     AND o.bucket=NEW.bucket AND starts_with(o.object_key,NEW.prefix)) THEN
  RAISE EXCEPTION 'R2 owner keep hold % overlaps a pending R2 retirement', NEW.prefix;
 END IF;
 RETURN NEW;
END $$;
