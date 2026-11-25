-- Deniz, verbatim: "serve the best 14 days of as many streams as possible, fully
-- stitched v85 ... via r2, through the nice web ui". Some of those stitched
-- outputs now exist only on the NAS: their R2 objects were deleted after the NAS
-- acknowledged its copy. This lets the NAS restore exactly such an output back
-- to its own catalogued key, create-only, the way 0260 restores Keep Everything
-- originals.
--
-- An 'output' restore row names one recording_collation_outputs row. It is
-- leased, signed and admitted past the writer fence (0187) only while the
-- controller has put that output in nas_restore_output_scope and the row still
-- carries the output's exact key, size, sha256 and video/mp4 type, and the NAS path of the
-- connection's acknowledged copy of those same bytes. Nothing here changes the
-- output row, its acks or any R2 retirement history.

-- Controller-approved outputs. Insert to approve; delete to withdraw.
CREATE TABLE nas_restore_output_scope (
 output_id bigint PRIMARY KEY REFERENCES recording_collation_outputs(id) ON DELETE RESTRICT,
 approved_at timestamptz NOT NULL DEFAULT now()
);
CREATE FUNCTION nas_restore_output_scope_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 RAISE EXCEPTION 'nas_restore_output_scope rows are never rewritten; delete and insert instead';
END $$;
CREATE TRIGGER nas_restore_output_scope_immutable BEFORE UPDATE ON nas_restore_output_scope
 FOR EACH ROW EXECUTE FUNCTION nas_restore_output_scope_immutable();

ALTER TABLE nas_restore_requests ADD COLUMN output_id bigint;
ALTER TABLE nas_restore_requests ALTER COLUMN clip_id DROP NOT NULL;
ALTER TABLE nas_restore_requests DROP CONSTRAINT nas_restore_requests_target_check;
-- 0157's unnamed "only originals write a non-test key" check.
DO $$ DECLARE names text[]; BEGIN
 SELECT array_agg(conname) INTO names FROM pg_constraint
  WHERE conrelid='nas_restore_requests'::regclass AND contype='c'
   AND pg_get_constraintdef(oid)=$c$CHECK (((target = 'original'::text) OR (object_key ~~ 'nas-restore-test/%'::text)))$c$;
 IF cardinality(names) IS DISTINCT FROM 1 THEN RAISE EXCEPTION 'expected one 0157 test-key check, found %', names; END IF;
 EXECUTE format('ALTER TABLE nas_restore_requests DROP CONSTRAINT %I', names[1]);
END $$;
ALTER TABLE nas_restore_requests
 ADD CONSTRAINT nas_restore_requests_target_check CHECK (target IN ('original','test','output')),
 ADD CONSTRAINT nas_restore_requests_test_key_check CHECK ((target = 'test') = (object_key LIKE 'nas-restore-test/%')),
 ADD CONSTRAINT nas_restore_requests_subject_check CHECK ((target = 'output') = (output_id IS NOT NULL) AND (target = 'output') = (clip_id IS NULL));
CREATE INDEX nas_restore_requests_output_idx ON nas_restore_requests (output_id) WHERE output_id IS NOT NULL;

-- The output's own key, size, sha256 and media type; the NAS path is the
-- connection's acknowledged copy of those bytes under the NAS joined/ root.
CREATE FUNCTION nas_restore_scoped_output(target text, output bigint, conn bigint, key text, size bigint, sha text, path text, ctype text) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT target='output' AND ctype='video/mp4' AND EXISTS(SELECT 1 FROM nas_restore_output_scope s
  JOIN recording_collation_outputs o ON o.id=s.output_id
  JOIN recording_collation_output_acks a ON a.output_id=o.id AND a.connection_id=conn
  WHERE s.output_id=output AND o.object_key=key AND o.size_bytes=size AND o.sha256=sha
   AND a.size_bytes=size AND a.sha256=sha AND a.nas_relative_path=o.nas_relative_path AND path='joined/'||o.nas_relative_path)
$$;

-- Every restore the lease, signer, report and fence serve.
CREATE FUNCTION nas_restore_admitted(q nas_restore_requests) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT nas_restore_ke_original(q.target,q.clip_id,q.object_key,q.size_bytes,q.sha256,q.relative_path)
  OR nas_restore_scoped_output(q.target,q.output_id,q.connection_id,q.object_key,q.size_bytes,q.sha256,q.relative_path,q.content_type)
$$;

CREATE OR REPLACE FUNCTION r2_retirement_guard_restore() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NEW.state IN ('pending','leased') THEN
  IF nas_restore_admitted(NEW) THEN
   -- The same shared wire lock as r2_retirement_admit_writer: no fence
   -- transition can interleave with this admission.
   PERFORM pg_advisory_xact_lock_shared(7343226011187);
  ELSE
   PERFORM r2_retirement_admit_writer();
  END IF;
 END IF;
 RETURN NEW;
END $$;
