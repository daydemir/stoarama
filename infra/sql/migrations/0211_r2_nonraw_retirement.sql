-- Reviewed non-raw (collated) R2 retirement, owner scope 2026-11-18.
--
-- Deniz, 2026-11-17, verbatim: "i'm ok with breaking public links"
-- Deniz, 2026-11-18 approximately 16:05Z, verbatim:
-- 'lets just go faster now, as long as we're only deleting footage that is all of / 1. not in the keep everythign set / 2. not in the 28 days set AND not on the NAS (ie waiting to be transferred) / then just delete it all now no need to limit 5tb / day'
-- Root's later clarification (2026-11-18) puts joined/best14-20261021/ in
-- scope as eligible footage. This migration therefore drops ONLY that one
-- negative prefix clause from0174 and adds ONLY that one set to the separate
-- positive collated allowlist. staging/ and repaired/ stay refused. No other
-- prefix, hold, incident or authority is widened, and the executor still
-- requires a real catalog identity; a key prefix is never mapping authority.
--
-- This file creates collated-only immutable intent/attempt/event history plus
-- one reviewed retirement class. It adds no writer capability, changes no
-- lifecycle or user goal state, and deletes nothing.

-- Deliveries are an ownership veto derived from catalog/dependency rows, never
-- a source kind or a table. RAW keeps its real clip namespace; collated gets
-- its own namespace keyed by recording_collation_outputs.id, so no collated id
-- can ever enter the RAW ledger (0201 UNIQUE(source_id) + raw-only guards).
CREATE TABLE r2_nonraw_intents (
 bucket text NOT NULL, object_key text NOT NULL,
 endpoint_sha256 text NOT NULL CHECK(endpoint_sha256 ~ '^[a-f0-9]{64}$'),
 kind text NOT NULL CHECK(kind='collated'),
 stream_id bigint NOT NULL CHECK(stream_id>0),
 source_id bigint NOT NULL CHECK(source_id>0),
 source_ids bigint[] NOT NULL CHECK(cardinality(source_ids)>0),
 size_bytes bigint NOT NULL CHECK(size_bytes>0),
 sha256 text NOT NULL CHECK(sha256 ~ '^[a-f0-9]{64}$'),
 nas_relative_path text NOT NULL CHECK(nas_relative_path<>''),
 etag text NOT NULL CHECK(etag<>''), last_modified timestamptz NOT NULL,
 verification_at timestamptz NOT NULL,
 manifest_sha256 text NOT NULL CHECK(manifest_sha256 ~ '^[a-f0-9]{64}$'),
 policy_sha256 text NOT NULL CHECK(policy_sha256='060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811'),
 picks_sha256 text NOT NULL CHECK(picks_sha256 ~ '^[a-f0-9]{64}$'),
 drain_evidence_sha256 text NOT NULL CHECK(drain_evidence_sha256 ~ '^[a-f0-9]{64}$'),
 retirement_class text NOT NULL CHECK(retirement_class='owner_delivery_scope_20261118_v1'),
 choice text NOT NULL CHECK(choice IN ('none','st','keep')),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bucket,object_key), UNIQUE(kind,source_id)
);
CREATE INDEX r2_nonraw_intents_created ON r2_nonraw_intents(created_at);
CREATE TABLE r2_nonraw_events (
 bucket text NOT NULL,object_key text NOT NULL,event text NOT NULL CHECK(event='absent'),
 observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bucket,object_key,event),
 FOREIGN KEY(bucket,object_key) REFERENCES r2_nonraw_intents(bucket,object_key)
);
-- One permanent wire issuance per key, across restarts and processes.
CREATE TABLE r2_nonraw_attempts (
 bucket text NOT NULL,object_key text NOT NULL,issued_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bucket,object_key),FOREIGN KEY(bucket,object_key) REFERENCES r2_nonraw_intents(bucket,object_key)
);
CREATE TRIGGER r2_nonraw_intent_immutable BEFORE UPDATE OR DELETE ON r2_nonraw_intents
 FOR EACH ROW EXECUTE FUNCTION guard_r2_object_retirement();
CREATE TRIGGER r2_nonraw_event_immutable BEFORE UPDATE OR DELETE ON r2_nonraw_events
 FOR EACH ROW EXECUTE FUNCTION guard_r2_object_retirement();
CREATE TRIGGER r2_nonraw_attempt_immutable BEFORE UPDATE OR DELETE ON r2_nonraw_attempts
 FOR EACH ROW EXECUTE FUNCTION guard_r2_object_retirement();
CREATE TRIGGER r2_nonraw_intents_no_truncate BEFORE TRUNCATE ON r2_nonraw_intents
 FOR EACH STATEMENT EXECUTE FUNCTION guard_r2_object_retirement();
CREATE TRIGGER r2_nonraw_events_no_truncate BEFORE TRUNCATE ON r2_nonraw_events
 FOR EACH STATEMENT EXECUTE FUNCTION guard_r2_object_retirement();
CREATE TRIGGER r2_nonraw_attempts_no_truncate BEFORE TRUNCATE ON r2_nonraw_attempts
 FOR EACH STATEMENT EXECUTE FUNCTION guard_r2_object_retirement();

-- A pending collated intent freezes the exact output identity it authorized.
-- Confirmed absence keeps history without permanently locking the catalog.
CREATE FUNCTION guard_r2_nonraw_output_identity() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF EXISTS(SELECT 1 FROM r2_nonraw_intents i WHERE i.source_id=OLD.id AND NOT EXISTS(SELECT 1 FROM r2_nonraw_events e WHERE e.bucket=i.bucket AND e.object_key=i.object_key AND e.event='absent'))
 OR (TG_OP='UPDATE' AND EXISTS(SELECT 1 FROM r2_nonraw_intents i WHERE i.bucket='stoarama' AND i.object_key=NEW.object_key AND NOT EXISTS(SELECT 1 FROM r2_nonraw_events e WHERE e.bucket=i.bucket AND e.object_key=i.object_key AND e.event='absent'))) THEN
  RAISE EXCEPTION 'R2 nonraw intent freezes collated output identity'; END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW;
END $$;
-- The output's OWN live interval and hour determine its pinned-window class, so a
-- pending intent must freeze them exactly as it freezes key/content identity. A
-- post-reservation interval rewrite into keep28 can no longer race the DELETE.
CREATE TRIGGER r2_nonraw_output_identity BEFORE UPDATE OF id,object_key,nas_relative_path,sha256,size_bytes,collation_hour_id,source_clip_ids,start_at,end_at OR DELETE ON recording_collation_outputs
 FOR EACH ROW EXECUTE FUNCTION guard_r2_nonraw_output_identity();

-- /0174 narrow amendment. Verbatim constraint names were confirmed against a
-- disposable PostgreSQL: the negative-prefix clause is a single-column CHECK so
-- PostgreSQL named it r2_object_retirements_object_key_check1, and the positive
-- kind/collated allowlist is the table-level r2_object_retirements_check. Only
-- those two are dropped. Both replacements are re-added with explicit stable
-- names so this stays reviewable and idempotent-by-name, never by guessing.
ALTER TABLE r2_object_retirements DROP CONSTRAINT r2_object_retirements_object_key_check1;
ALTER TABLE r2_object_retirements DROP CONSTRAINT r2_object_retirements_check;
ALTER TABLE r2_object_retirements ADD CONSTRAINT r2_object_retirements_prefix_check
 CHECK (object_key NOT LIKE 'staging/%' AND object_key NOT LIKE 'repaired/%');
ALTER TABLE r2_object_retirements ADD CONSTRAINT r2_object_retirements_collated_prefix_check
 CHECK ((kind = 'raw' AND object_key LIKE 'managed/%')
     OR (kind = 'collated' AND (object_key LIKE 'joined/goodplus-%'
         OR object_key LIKE 'joined/nightly-collation-v2/%'
         OR object_key LIKE 'joined/best14-20261021/%')));

-- RAW delivery eligibility needs the same typed provenance as collated: an
-- exact owner-scoped intent that carries the reviewed class. Legacy B1 intents
-- keep the empty class, so they can never inherit this exemption.
ALTER TABLE r2_inverse_intents
 ADD COLUMN retirement_class text NOT NULL DEFAULT ''::text
 CHECK (retirement_class IN ('', 'owner_delivery_scope_20261118_v1'));

-- 0190 called this guard on the reservation AND on the final retirement receipt.
-- The receipt table has no retirement_class column, so the receipt looks up the
-- exact reservation. The exemption is one reviewed class with a matching typed
-- intent in the matching namespace and identical identity; every other class,
-- every legacy intent and every untyped retirement stays strictly vetoed.
CREATE OR REPLACE FUNCTION guard_delivery_r2_retirement() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF TG_TABLE_NAME='r2_retirement_reservations' THEN
  IF NEW.retirement_class='owner_delivery_scope_20261118_v1' AND NEW.kind='collated'
   AND EXISTS(SELECT 1 FROM r2_nonraw_intents i WHERE i.bucket=NEW.bucket AND i.object_key=NEW.object_key
     AND i.source_id=NEW.source_id AND i.size_bytes=NEW.size_bytes AND i.sha256=NEW.sha256
     AND i.manifest_sha256=NEW.manifest_sha256) THEN RETURN NEW; END IF;
  -- RAW has no sha256 column in its ledger, so its exact identity is source,
  -- key, size and the trimmed ETag the intent and the source row both carry.
  IF NEW.retirement_class='owner_delivery_scope_20261118_v1' AND NEW.kind='raw'
   AND EXISTS(SELECT 1 FROM r2_inverse_intents i JOIN recording_clips c ON c.id=i.source_id
     WHERE i.bucket=NEW.bucket AND i.object_key=NEW.object_key AND i.source_id=NEW.source_id
     AND i.size_bytes=NEW.size_bytes AND i.manifest_sha256=NEW.manifest_sha256
     AND lower(c.sha256)=NEW.sha256 AND c.bucket=i.bucket AND c.object_key=i.object_key
     AND btrim(c.etag,'"')=btrim(i.etag,'"')
     AND i.retirement_class='owner_delivery_scope_20261118_v1') THEN RETURN NEW; END IF;
 ELSE
  IF EXISTS(SELECT 1 FROM r2_retirement_reservations r WHERE r.kind=NEW.kind AND r.source_id=NEW.source_id
     AND r.bucket=NEW.bucket AND r.object_key=NEW.object_key AND r.size_bytes=NEW.size_bytes
     AND r.sha256=NEW.sha256 AND r.manifest_sha256=NEW.manifest_sha256
     AND r.retirement_class='owner_delivery_scope_20261118_v1'
     AND (EXISTS(SELECT 1 FROM r2_nonraw_intents i WHERE i.bucket=NEW.bucket AND i.object_key=NEW.object_key
       AND i.source_id=NEW.source_id AND i.size_bytes=NEW.size_bytes AND i.sha256=NEW.sha256
       AND i.manifest_sha256=NEW.manifest_sha256)
      OR EXISTS(SELECT 1 FROM r2_inverse_intents i JOIN recording_clips c ON c.id=i.source_id
       WHERE i.bucket=NEW.bucket AND i.object_key=NEW.object_key AND i.source_id=NEW.source_id
       AND i.size_bytes=NEW.size_bytes AND i.manifest_sha256=NEW.manifest_sha256
       AND lower(c.sha256)=NEW.sha256 AND btrim(c.etag,'"')=btrim(i.etag,'"')
       AND i.retirement_class='owner_delivery_scope_20261118_v1'))) THEN RETURN NEW; END IF;
 END IF;
 IF EXISTS(SELECT 1 FROM delivery_keep_holds h WHERE h.bucket=NEW.bucket AND h.object_key=NEW.object_key) THEN
  RAISE EXCEPTION 'DELIVERY KEEP HOLD: R2 retirement forbidden'; END IF;
 RETURN NEW;
END $$;

-- /0201's admission guard was one AFTER statement function over every row. Once
-- any reviewed delivery-scope key is legitimately reserved, that global form
-- would veto every later unrelated hold write forever. These replacements judge
-- ONLY the rows admitted/enriched by the current statement (transition tables).
-- A genuinely NEW overlap with a reservation/intent still fails; an overlap that
-- already existed before this statement cannot veto unrelated future writes.
-- BEFORE-time protection fences and their lock order are unchanged.
CREATE FUNCTION guard_r2_delivery_keys_admitted() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r record; BEGIN
 FOR r IN SELECT * FROM new_rows LOOP
  IF EXISTS(SELECT 1 FROM r2_retirement_reservations v WHERE v.bucket=r.bucket AND v.object_key=r.object_key)
  OR EXISTS(SELECT 1 FROM r2_inverse_intents i WHERE i.bucket=r.bucket AND i.object_key=r.object_key)
  OR EXISTS(SELECT 1 FROM r2_nonraw_intents i WHERE i.bucket=r.bucket AND i.object_key=r.object_key) THEN
   RAISE EXCEPTION 'R2 retirement reservation forbids delivery adoption'; END IF;
  IF r.sha256<>'' AND (EXISTS(SELECT 1 FROM recording_clips c JOIN r2_retirement_reservations v ON v.kind='raw' AND v.source_id=c.id
      WHERE lower(c.sha256)=r.sha256 AND c.size_bytes=r.size_bytes)
    OR EXISTS(SELECT 1 FROM r2_nonraw_intents i WHERE i.sha256=r.sha256 AND i.size_bytes=r.size_bytes)) THEN
   RAISE EXCEPTION 'R2 retirement reservation forbids delivery content alias'; END IF;
  IF r.relative_path<>'' AND (EXISTS(SELECT 1 FROM recording_clips c WHERE regexp_replace(c.display_path,'^joined/','')=regexp_replace(r.relative_path,'^joined/','')
      AND EXISTS(SELECT 1 FROM r2_retirement_reservations v WHERE v.kind='raw' AND v.source_id=c.id))
    OR EXISTS(SELECT 1 FROM r2_nonraw_intents i WHERE regexp_replace(i.nas_relative_path,'^joined/','')=regexp_replace(r.relative_path,'^joined/',''))) THEN
   RAISE EXCEPTION 'R2 retirement reservation forbids delivery path alias'; END IF;
  -- Transitive closure: a hold admitted on a collated OUTPUT is also an overlap
  -- with that output's OWN reserved/pending RAW sources and with its own pending
  -- collated retirement. Matching is exact on key, normalized path or content, so
  -- unrelated rows are still admitted.
  IF EXISTS(SELECT 1 FROM recording_collation_outputs o
     WHERE (o.object_key=r.object_key
       OR (r.relative_path<>'' AND regexp_replace(o.nas_relative_path,'^joined/','')=regexp_replace(r.relative_path,'^joined/',''))
       OR (r.sha256<>'' AND o.sha256=r.sha256 AND o.size_bytes=r.size_bytes))
     AND (EXISTS(SELECT 1 FROM r2_retirement_reservations v WHERE v.kind='collated' AND v.source_id=o.id)
       OR EXISTS(SELECT 1 FROM r2_nonraw_intents i WHERE i.kind='collated' AND i.source_id=o.id)
       OR EXISTS(SELECT 1 FROM r2_object_retirements x WHERE x.kind='collated' AND x.source_id=o.id)
       OR EXISTS(SELECT 1 FROM recording_clips c JOIN r2_retirement_reservations v ON v.kind='raw' AND v.source_id=c.id
            WHERE c.id=ANY(o.source_clip_ids))
       OR EXISTS(SELECT 1 FROM recording_clips c JOIN r2_inverse_intents i ON i.source_id=c.id
            WHERE c.id=ANY(o.source_clip_ids)))) THEN
   RAISE EXCEPTION 'R2 retirement reservation forbids delivery collated-source closure'; END IF;
 END LOOP;
 RETURN NULL; END $$;
-- A source hold must also close over collated outputs: a clip that is a source
-- of an already-typed output is exactly the overlap the reviewed scope retired.
CREATE FUNCTION guard_r2_delivery_sources_admitted() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r record; BEGIN
 FOR r IN SELECT * FROM new_rows LOOP
  IF EXISTS(SELECT 1 FROM r2_retirement_reservations v WHERE v.kind='raw' AND v.source_id=r.clip_id)
  OR EXISTS(SELECT 1 FROM r2_retirement_reservations v JOIN recording_collation_outputs o
     ON v.kind='collated' AND v.source_id=o.id WHERE r.clip_id=ANY(o.source_clip_ids))
  OR EXISTS(SELECT 1 FROM r2_nonraw_intents i JOIN recording_collation_outputs o
     ON i.kind='collated' AND i.source_id=o.id WHERE r.clip_id=ANY(o.source_clip_ids))
  -- Co-sourcing an output with an already-reserved clip creates a new delivery
  -- closure overlap for that reservation, even though the reserved clip itself
  -- is unchanged. This is the exact new-overlap veto, not a global one.
  OR EXISTS(SELECT 1 FROM recording_collation_outputs o
     JOIN recording_clips mine ON mine.id=r.clip_id AND mine.id=ANY(o.source_clip_ids)
     CROSS JOIN LATERAL unnest(o.source_clip_ids) sid
     JOIN r2_retirement_reservations v ON v.kind='raw' AND v.source_id=sid) THEN
   RAISE EXCEPTION 'R2 retirement reservation forbids delivery source hold'; END IF;
 END LOOP;
 RETURN NULL; END $$;
CREATE FUNCTION guard_r2_delivery_dependencies_admitted() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r record; BEGIN
 FOR r IN SELECT * FROM new_rows LOOP
  -- Exact output identity, plus a reservation on one of THAT output's own
  -- source clips. A reservation on an unrelated clip of the same recording is
  -- not this dependency's identity and must not veto it.
  IF EXISTS(SELECT 1 FROM r2_retirement_reservations v WHERE v.kind='collated' AND v.source_id=r.output_id)
  OR EXISTS(SELECT 1 FROM r2_nonraw_intents i WHERE i.kind='collated' AND i.source_id=r.output_id)
  OR EXISTS(SELECT 1 FROM recording_collation_outputs o JOIN recording_clips c ON c.id=ANY(o.source_clip_ids)
     JOIN r2_retirement_reservations v ON v.kind='raw' AND v.source_id=c.id
     WHERE o.id=r.output_id) THEN
   RAISE EXCEPTION 'R2 retirement reservation forbids delivery dependency'; END IF;
 END LOOP;
 RETURN NULL; END $$;

-- Transition tables cannot be combined with an UPDATE column list, so output and
-- hour changes are filtered by comparing OLD/NEW rows for exactly the columns
-- the original trigger watched. Unrelated writes stay admitted. INSERT and
-- UPDATE need separate functions because only the UPDATE trigger declares
-- old_rows.
CREATE FUNCTION guard_r2_delivery_outputs_admitted() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r record; BEGIN
 FOR r IN SELECT * FROM new_rows LOOP
  PERFORM r2_delivery_admission_veto(r.object_key,r.id,r.source_clip_ids,r.sha256,r.size_bytes,r.nas_relative_path);
 END LOOP;
 RETURN NULL; END $$;
CREATE FUNCTION guard_r2_delivery_outputs_updated() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r record; BEGIN
 FOR r IN SELECT n.* FROM new_rows n LEFT JOIN old_rows o ON o.id=n.id
  WHERE o.id IS NULL OR o.object_key IS DISTINCT FROM n.object_key
   OR o.nas_relative_path IS DISTINCT FROM n.nas_relative_path
   OR o.collation_hour_id IS DISTINCT FROM n.collation_hour_id
   OR o.source_clip_ids IS DISTINCT FROM n.source_clip_ids
   OR o.sha256 IS DISTINCT FROM n.sha256 OR o.size_bytes IS DISTINCT FROM n.size_bytes
 LOOP
  PERFORM r2_delivery_admission_veto(r.object_key,r.id,r.source_clip_ids,r.sha256,r.size_bytes,r.nas_relative_path);
 END LOOP;
 RETURN NULL; END $$;
-- recording_collation_outputs has no bucket column, so this closed helper
-- compares object_key/content/path across every bucket. Output keys are
-- globally unique content-addressed names.
CREATE FUNCTION r2_delivery_admission_veto(k text,output_id bigint,src bigint[],sha text,n bigint,path text) RETURNS void LANGUAGE plpgsql AS $$ BEGIN
 IF EXISTS(SELECT 1 FROM r2_retirement_reservations v WHERE v.object_key=k)
 OR EXISTS(SELECT 1 FROM r2_inverse_intents i WHERE i.object_key=k)
 OR EXISTS(SELECT 1 FROM r2_nonraw_intents i WHERE i.object_key=k)
 OR EXISTS(SELECT 1 FROM r2_retirement_reservations v WHERE v.kind='collated' AND v.source_id=output_id)
 OR EXISTS(SELECT 1 FROM r2_nonraw_intents i WHERE i.kind='collated' AND i.source_id=output_id)
 OR (sha<>'' AND EXISTS(SELECT 1 FROM r2_nonraw_intents i WHERE i.sha256=sha AND i.size_bytes=n))
 OR (path<>'' AND EXISTS(SELECT 1 FROM r2_nonraw_intents i WHERE regexp_replace(i.nas_relative_path,'^joined/','')=regexp_replace(path,'^joined/','')))
 OR EXISTS(SELECT 1 FROM recording_clips c JOIN r2_retirement_reservations v ON v.kind='raw' AND v.source_id=c.id
    WHERE c.id=ANY(src)) THEN
  RAISE EXCEPTION 'R2 retirement reservation forbids delivery adoption'; END IF;
END $$;
CREATE FUNCTION guard_r2_delivery_hours_admitted() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r record; BEGIN
 FOR r IN SELECT n.* FROM new_rows n JOIN old_rows o ON o.id=n.id
  WHERE o.recording_id IS DISTINCT FROM n.recording_id LOOP
  -- Judge ONLY the changed hour's own outputs, their source clips and their own
  -- dependencies. A reservation on some unrelated clip of the destination
  -- recording is not this hour's identity and must not veto the move.
  IF EXISTS(SELECT 1 FROM recording_collation_outputs x
     JOIN r2_retirement_reservations v ON v.kind='collated' AND v.source_id=x.id
     WHERE x.collation_hour_id=r.id)
  OR EXISTS(SELECT 1 FROM recording_collation_outputs x
     JOIN r2_nonraw_intents i ON i.kind='collated' AND i.source_id=x.id
     WHERE x.collation_hour_id=r.id)
  OR EXISTS(SELECT 1 FROM recording_collation_outputs x CROSS JOIN LATERAL unnest(x.source_clip_ids) sid
     JOIN r2_retirement_reservations v ON v.kind='raw' AND v.source_id=sid
     WHERE x.collation_hour_id=r.id)
  OR EXISTS(SELECT 1 FROM recording_collation_outputs x
     JOIN delivery_keep_output_dependencies d ON d.output_id=x.id
     JOIN r2_retirement_reservations v ON v.kind='collated' AND v.source_id=x.id
     WHERE x.collation_hour_id=r.id) THEN
   RAISE EXCEPTION 'R2 retirement reservation forbids delivery adoption'; END IF;
 END LOOP;
 RETURN NULL; END $$;

DROP TRIGGER r2_inverse_delivery_key_admission ON delivery_keep_holds;
DROP TRIGGER r2_inverse_delivery_source_admission ON delivery_nas_source_holds;
DROP TRIGGER r2_inverse_delivery_output_admission ON delivery_keep_output_dependencies;
DROP TRIGGER r2_inverse_output_delivery_admission ON recording_collation_outputs;
DROP TRIGGER r2_inverse_hour_delivery_admission ON recording_collation_hours;
CREATE TRIGGER r2_inverse_delivery_key_admission AFTER INSERT ON delivery_keep_holds
 REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION guard_r2_delivery_keys_admitted();
CREATE TRIGGER r2_inverse_delivery_key_update_admission AFTER UPDATE ON delivery_keep_holds
 REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION guard_r2_delivery_keys_admitted();
CREATE TRIGGER r2_inverse_delivery_source_admission AFTER INSERT ON delivery_nas_source_holds
 REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION guard_r2_delivery_sources_admitted();
CREATE TRIGGER r2_inverse_delivery_source_update_admission AFTER UPDATE ON delivery_nas_source_holds
 REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION guard_r2_delivery_sources_admitted();
CREATE TRIGGER r2_inverse_delivery_output_admission AFTER INSERT ON delivery_keep_output_dependencies
 REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION guard_r2_delivery_dependencies_admitted();
CREATE TRIGGER r2_inverse_delivery_output_update_admission AFTER UPDATE ON delivery_keep_output_dependencies
 REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION guard_r2_delivery_dependencies_admitted();
CREATE TRIGGER r2_inverse_output_delivery_insert_admission AFTER INSERT ON recording_collation_outputs
 REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION guard_r2_delivery_outputs_admitted();
CREATE TRIGGER r2_inverse_output_delivery_update_admission AFTER UPDATE ON recording_collation_outputs
 REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION guard_r2_delivery_outputs_updated();
CREATE TRIGGER r2_inverse_hour_delivery_admission AFTER UPDATE ON recording_collation_hours
 REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION guard_r2_delivery_hours_admitted();

-- The old global statement function has no callers left after the trigger
-- replacements above; drop it so nothing can silently reintroduce the veto.
DROP FUNCTION guard_r2_inverse_delivery_admission();

-- The receipt table has no retirement_class column, so this class is also
-- recorded on the reservation that the reviewer can inspect directly.
ALTER TABLE r2_retirement_reservations
 DROP CONSTRAINT r2_retirement_reservations_retirement_class_check;
ALTER TABLE r2_retirement_reservations
 ADD CONSTRAINT r2_retirement_reservations_retirement_class_check
 CHECK (retirement_class IN ('', 'owner_accepted_outside_window', 'owner_r2_delivery_only', 'owner_delivery_scope_20261118_v1'));

