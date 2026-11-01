-- NAS cleanup is an exact-file, operator-owned workflow. Merely importing or
-- observing a plan never enables deletion. Pull keys cannot create/arm plans.
CREATE TABLE nas_cleanup_plans (
 id bigserial PRIMARY KEY,
 account_id bigint NOT NULL REFERENCES accounts(id),
 connection_id bigint NOT NULL REFERENCES connections(id),
 label text NOT NULL UNIQUE CHECK(label ~ '^[a-z0-9][a-z0-9._-]{0,79}$'),
 report_sha256 text NOT NULL CHECK(report_sha256 ~ '^[a-f0-9]{64}$'),
 manifest jsonb NOT NULL CHECK(jsonb_typeof(manifest)='array'),
 plan_sha256 text NOT NULL CHECK(plan_sha256 ~ '^[a-f0-9]{64}$'),
 mode text NOT NULL DEFAULT 'dry_run' CHECK(mode IN ('dry_run','enabled','paused')),
 allow_nas_only boolean NOT NULL DEFAULT false,
 approval_reason text NOT NULL DEFAULT '',
 approved_until timestamptz,
 created_at timestamptz NOT NULL DEFAULT now(),
 CHECK(mode<>'enabled' OR (length(approval_reason)>0 AND approved_until IS NOT NULL))
);
CREATE TABLE nas_cleanup_items (
 id bigserial PRIMARY KEY,
 plan_id bigint NOT NULL REFERENCES nas_cleanup_plans(id),
 ordinal integer NOT NULL CHECK(ordinal>0),
 delete_ever_issued boolean NOT NULL DEFAULT false,
 state text NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','leased','would_delete','keep','missing','conflict','delete_issued','deleted','unverified')),
 attempt integer NOT NULL DEFAULT 0,
 lease_expires_at timestamptz,
 proof jsonb,
 proof_sha256 text,
 recovery text NOT NULL DEFAULT 'unchecked',
 policy_reason text NOT NULL DEFAULT '',
 outcome jsonb,
 updated_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(plan_id,ordinal)
);
CREATE INDEX nas_cleanup_pending ON nas_cleanup_items(plan_id,id) WHERE state IN ('pending','leased');
CREATE FUNCTION guard_nas_cleanup_plan() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF TG_OP='DELETE' OR ROW(NEW.account_id,NEW.connection_id,NEW.label,NEW.report_sha256,NEW.manifest,NEW.plan_sha256,NEW.created_at) IS DISTINCT FROM ROW(OLD.account_id,OLD.connection_id,OLD.label,OLD.report_sha256,OLD.manifest,OLD.plan_sha256,OLD.created_at) THEN
 RAISE EXCEPTION 'cleanup plan identity is immutable'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER nas_cleanup_plan_identity BEFORE UPDATE OR DELETE ON nas_cleanup_plans FOR EACH ROW EXECUTE FUNCTION guard_nas_cleanup_plan();
CREATE FUNCTION guard_nas_cleanup_item() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF TG_OP='DELETE' OR (OLD.delete_ever_issued AND NOT NEW.delete_ever_issued) OR ROW(NEW.plan_id,NEW.ordinal) IS DISTINCT FROM ROW(OLD.plan_id,OLD.ordinal) THEN RAISE EXCEPTION 'cleanup item identity is immutable'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER nas_cleanup_item_identity BEFORE UPDATE OR DELETE ON nas_cleanup_items FOR EACH ROW EXECUTE FUNCTION guard_nas_cleanup_item();

-- Recovery/source holds are DB-enforced so old purge binaries cannot race a
-- verified NAS deletion. Successful NAS removals retain their R2 recovery copy.
CREATE TABLE nas_cleanup_source_holds (
 plan_id bigint NOT NULL REFERENCES nas_cleanup_plans(id),
 clip_id bigint NOT NULL REFERENCES recording_clips(id),
 PRIMARY KEY(plan_id,clip_id)
);
CREATE INDEX nas_cleanup_source_holds_clip_idx ON nas_cleanup_source_holds(clip_id,plan_id);
CREATE INDEX nas_cleanup_issued_recent_idx ON nas_cleanup_items(updated_at) WHERE delete_ever_issued;
CREATE FUNCTION nas_cleanup_clip_held(clip bigint) RETURNS boolean LANGUAGE sql VOLATILE AS $$
 SELECT EXISTS(SELECT 1 FROM nas_cleanup_source_holds h JOIN nas_cleanup_plans p ON p.id=h.plan_id
 WHERE h.clip_id=clip AND (p.mode<>'paused' OR EXISTS(SELECT 1 FROM nas_cleanup_items i WHERE i.plan_id=p.id AND i.delete_ever_issued)))
$$;
CREATE FUNCTION guard_nas_cleanup_source() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF nas_cleanup_clip_held(OLD.id) AND (TG_OP='DELETE' OR ROW(NEW.purged_at,NEW.bucket,NEW.object_key,NEW.sha256,NEW.size_bytes,NEW.display_path,NEW.clip_start_at,NEW.clip_end_at,NEW.recording_id)
 IS DISTINCT FROM ROW(OLD.purged_at,OLD.bucket,OLD.object_key,OLD.sha256,OLD.size_bytes,OLD.display_path,OLD.clip_start_at,OLD.clip_end_at,OLD.recording_id)) THEN
 RAISE EXCEPTION 'retention protected: NAS cleanup source/recovery hold'; END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;RETURN NEW;
END $$;
CREATE TRIGGER nas_cleanup_source_hold BEFORE UPDATE OR DELETE ON recording_clips FOR EACH ROW EXECUTE FUNCTION guard_nas_cleanup_source();
CREATE FUNCTION guard_nas_cleanup_stream() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NEW.local_timezone IS DISTINCT FROM OLD.local_timezone AND EXISTS(SELECT 1 FROM nas_cleanup_plans p CROSS JOIN LATERAL jsonb_array_elements(p.manifest) i WHERE (i->>'stream_id')::bigint=OLD.id AND p.mode<>'paused') THEN RAISE EXCEPTION 'retention protected: NAS cleanup canonical timezone hold'; END IF;RETURN NEW;
END $$;
CREATE TRIGGER nas_cleanup_stream_hold BEFORE UPDATE ON streams FOR EACH ROW EXECUTE FUNCTION guard_nas_cleanup_stream();
CREATE FUNCTION guard_nas_cleanup_recording() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF ROW(NEW.account_id,NEW.stream_id,NEW.cron_timezone) IS DISTINCT FROM ROW(OLD.account_id,OLD.stream_id,OLD.cron_timezone)
 AND EXISTS(SELECT 1 FROM nas_cleanup_source_holds h JOIN recording_clips c ON c.id=h.clip_id WHERE c.recording_id=OLD.id AND nas_cleanup_clip_held(c.id)) THEN RAISE EXCEPTION 'retention protected: NAS cleanup recording identity hold'; END IF;RETURN NEW;
END $$;
CREATE TRIGGER nas_cleanup_recording_hold BEFORE UPDATE ON recordings FOR EACH ROW EXECUTE FUNCTION guard_nas_cleanup_recording();
CREATE FUNCTION guard_nas_cleanup_output() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF EXISTS(SELECT 1 FROM nas_cleanup_plans p CROSS JOIN LATERAL jsonb_array_elements(p.manifest) i WHERE i->>'kind'='collated' AND (i->>'source_id')::bigint=OLD.id AND (p.mode<>'paused' OR EXISTS(SELECT 1 FROM nas_cleanup_items n WHERE n.plan_id=p.id AND n.delete_ever_issued))) THEN
 RAISE EXCEPTION 'retention protected: NAS cleanup collated identity hold'; END IF;
 IF TG_OP='DELETE' THEN RETURN OLD;END IF;RETURN NEW;
END $$;
CREATE TRIGGER nas_cleanup_output_hold BEFORE UPDATE OR DELETE ON recording_collation_outputs FOR EACH ROW EXECUTE FUNCTION guard_nas_cleanup_output();

-- Admission of a new source reference takes the same clip-row fence as cleanup
-- admission. References cannot appear between the final policy check and unlink.
CREATE FUNCTION guard_nas_cleanup_new_reference() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE source bigint;
BEGIN
 IF TG_TABLE_NAME='recording_joined_source_snapshots' THEN
   PERFORM id FROM recording_clips WHERE id=NEW.clip_id FOR KEY SHARE;
   IF nas_cleanup_clip_held(NEW.clip_id) THEN RAISE EXCEPTION 'retention protected: NAS cleanup source-reference fence'; END IF;
 ELSE
   FOR source IN SELECT DISTINCT (p->'clip'->>'clip_id')::bigint FROM jsonb_array_elements(NEW.manifest->'clips') p ORDER BY 1 LOOP
     PERFORM id FROM recording_clips WHERE id=source FOR KEY SHARE;
     IF nas_cleanup_clip_held(source) THEN RAISE EXCEPTION 'retention protected: NAS cleanup collation-reference fence'; END IF;
   END LOOP;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER nas_cleanup_joined_reference BEFORE INSERT ON recording_joined_source_snapshots FOR EACH ROW EXECUTE FUNCTION guard_nas_cleanup_new_reference();
CREATE TRIGGER nas_cleanup_collation_reference BEFORE INSERT OR UPDATE OF manifest ON recording_collation_hours FOR EACH ROW EXECUTE FUNCTION guard_nas_cleanup_new_reference();
