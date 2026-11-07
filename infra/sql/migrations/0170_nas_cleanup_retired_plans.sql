-- keep28 policy: plans admitted under a superseded cleanup policy are retired
-- (a terminal state transition; no plan, item or hold row is ever deleted).
-- Retired plans release read-use/source holds exactly like paused plans, so
-- they stop blocking R2 purges, unless any item ever issued a deletion (that
-- permanent R2 recovery hold is unchanged). Grants no deletion authority.
ALTER TABLE nas_cleanup_plans DROP CONSTRAINT nas_cleanup_plans_mode_check;
ALTER TABLE nas_cleanup_plans ADD CONSTRAINT nas_cleanup_plans_mode_check
 CHECK (mode IN ('dry_run','enabled','paused','retired'));

CREATE OR REPLACE FUNCTION guard_nas_cleanup_plan() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF TG_OP='DELETE' OR ROW(NEW.account_id,NEW.connection_id,NEW.label,NEW.report_sha256,NEW.manifest,NEW.plan_sha256,NEW.created_at) IS DISTINCT FROM ROW(OLD.account_id,OLD.connection_id,OLD.label,OLD.report_sha256,OLD.manifest,OLD.plan_sha256,OLD.created_at) THEN
 RAISE EXCEPTION 'cleanup plan identity is immutable'; END IF;
 IF OLD.mode='retired' AND NEW.mode<>'retired' THEN RAISE EXCEPTION 'retired cleanup plan is terminal'; END IF;
 IF NEW.mode='retired' AND OLD.mode NOT IN ('dry_run','paused','retired') THEN RAISE EXCEPTION 'only nonexecuting cleanup plans may be retired'; END IF;
 RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION nas_cleanup_clip_held(clip bigint) RETURNS boolean LANGUAGE sql VOLATILE AS $$
 SELECT EXISTS(SELECT 1 FROM nas_cleanup_source_holds h JOIN nas_cleanup_plans p ON p.id=h.plan_id
 WHERE h.clip_id=clip AND (p.mode NOT IN ('paused','retired') OR EXISTS(SELECT 1 FROM nas_cleanup_items i WHERE i.plan_id=p.id AND i.delete_ever_issued)))
$$;

CREATE OR REPLACE FUNCTION guard_nas_cleanup_stream() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NEW.local_timezone IS DISTINCT FROM OLD.local_timezone AND EXISTS(SELECT 1 FROM nas_cleanup_plans p CROSS JOIN LATERAL jsonb_array_elements(p.manifest) i WHERE (i->>'stream_id')::bigint=OLD.id AND p.mode NOT IN ('paused','retired')) THEN RAISE EXCEPTION 'retention protected: NAS cleanup canonical timezone hold'; END IF;RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION guard_nas_cleanup_output() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF (TG_OP='DELETE' OR ROW(NEW.nas_relative_path,NEW.object_key,NEW.sha256,NEW.size_bytes,NEW.start_at,NEW.end_at,NEW.source_clip_ids,NEW.collation_hour_id) IS DISTINCT FROM ROW(OLD.nas_relative_path,OLD.object_key,OLD.sha256,OLD.size_bytes,OLD.start_at,OLD.end_at,OLD.source_clip_ids,OLD.collation_hour_id)) AND EXISTS(SELECT 1 FROM nas_cleanup_plans p CROSS JOIN LATERAL jsonb_array_elements(p.manifest) i WHERE i->>'kind'='collated' AND (i->>'source_id')::bigint=OLD.id AND (p.mode NOT IN ('paused','retired') OR EXISTS(SELECT 1 FROM nas_cleanup_items n WHERE n.plan_id=p.id AND n.delete_ever_issued))) THEN
 RAISE EXCEPTION 'retention protected: NAS cleanup collated identity hold'; END IF;
 IF TG_OP='DELETE' THEN RETURN OLD;END IF;RETURN NEW;
END $$;
