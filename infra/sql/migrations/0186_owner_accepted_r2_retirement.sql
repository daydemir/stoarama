-- Explicit owner authority is consumed only by this retirement class.
-- Other classes and every ordinary cleanup/source mutation keep their holds.
ALTER TABLE r2_retirement_reservations ADD COLUMN retirement_class text NOT NULL DEFAULT ''
 CHECK(retirement_class IN ('','owner_accepted_outside_window'));
ALTER VIEW nas_cleanup_r2_effective_releases RENAME TO nas_cleanup_all_effective_releases;
CREATE VIEW nas_cleanup_r2_effective_releases AS
 SELECT * FROM nas_cleanup_all_effective_releases
 WHERE coalesce(authority->'candidate'->>'reason','')<>'owner_accepted_outside_window';
CREATE VIEW nas_cleanup_owner_effective_releases AS
 SELECT x.* FROM nas_cleanup_all_effective_releases x
 JOIN nas_cleanup_items i ON i.id=x.item_id JOIN nas_cleanup_plans p ON p.id=i.plan_id
 WHERE coalesce(x.authority->'candidate'->>'reason','')<>'owner_accepted_outside_window'
 OR p.mode='retired';
CREATE OR REPLACE VIEW nas_cleanup_r2_reserved_files AS
 SELECT p.account_id,p.connection_id,p.label,e.value->>'kind' kind,(e.value->>'source_id')::bigint source_id
 FROM nas_cleanup_plans p CROSS JOIN LATERAL jsonb_array_elements(p.manifest) WITH ORDINALITY e(value,n)
 LEFT JOIN nas_cleanup_items i ON i.plan_id=p.id AND i.ordinal=e.n
 WHERE (p.mode<>'retired' OR i.delete_ever_issued OR i.id IS NULL)
 AND NOT EXISTS(SELECT 1 FROM nas_cleanup_r2_effective_releases r WHERE r.item_id=i.id);

CREATE VIEW nas_cleanup_owner_reserved_files AS
 SELECT p.account_id,p.connection_id,p.label,e.value->>'kind' kind,(e.value->>'source_id')::bigint source_id
 FROM nas_cleanup_plans p CROSS JOIN LATERAL jsonb_array_elements(p.manifest) WITH ORDINALITY e(value,n)
 LEFT JOIN nas_cleanup_items i ON i.plan_id=p.id AND i.ordinal=e.n
 WHERE (p.mode<>'retired' OR i.delete_ever_issued OR i.id IS NULL)
 AND NOT EXISTS(SELECT 1 FROM nas_cleanup_owner_effective_releases r WHERE r.item_id=i.id);

CREATE OR REPLACE FUNCTION nas_cleanup_owner_clip_held(clip bigint) RETURNS boolean LANGUAGE sql VOLATILE AS $$
 SELECT EXISTS(SELECT 1 FROM nas_cleanup_incident_sources WHERE clip_id=clip)
 OR EXISTS(SELECT 1 FROM nas_cleanup_source_holds h JOIN nas_cleanup_plans p ON p.id=h.plan_id
 WHERE h.clip_id=clip AND (p.mode<>'retired' OR EXISTS(SELECT 1 FROM nas_cleanup_items i WHERE i.plan_id=p.id AND i.delete_ever_issued))
 AND (NOT EXISTS(SELECT 1 FROM jsonb_array_elements(p.manifest) e WHERE (e->'source_ids') @> to_jsonb(ARRAY[clip]) OR (e->>'kind'='raw' AND (e->>'source_id')::bigint=clip))
 OR EXISTS(SELECT 1 FROM jsonb_array_elements(p.manifest) WITH ORDINALITY e(value,n)
 LEFT JOIN nas_cleanup_items i ON i.plan_id=p.id AND i.ordinal=e.n
 WHERE ((e.value->'source_ids') @> to_jsonb(ARRAY[clip]) OR (e.value->>'kind'='raw' AND (e.value->>'source_id')::bigint=clip))
 AND (p.mode<>'retired' OR i.delete_ever_issued OR i.id IS NULL)
 AND NOT EXISTS(SELECT 1 FROM nas_cleanup_owner_effective_releases r WHERE r.item_id=i.id))))
$$;

CREATE FUNCTION guard_owner_accepted_hold_release() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NEW.authority->'candidate'->>'reason'='owner_accepted_outside_window'
 AND NOT EXISTS(SELECT 1 FROM nas_cleanup_items i JOIN nas_cleanup_plans p ON p.id=i.plan_id
 WHERE i.id=NEW.item_id AND p.mode='retired') THEN
 RAISE EXCEPTION 'owner release requires retired cleanup plan'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER owner_accepted_hold_release_guard BEFORE INSERT ON nas_cleanup_r2_releases
 FOR EACH ROW EXECUTE FUNCTION guard_owner_accepted_hold_release();

CREATE OR REPLACE FUNCTION guard_r2_retirement_reservation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.retirement_class='owner_accepted_outside_window' THEN
  IF EXISTS(SELECT 1 FROM nas_cleanup_owner_reserved_files f WHERE f.kind=NEW.kind AND f.source_id=NEW.source_id)
  OR (NEW.kind='raw' AND nas_cleanup_owner_clip_held(NEW.source_id))
  OR (NEW.kind='collated' AND EXISTS(SELECT 1 FROM recording_collation_outputs o
    WHERE o.id=NEW.source_id AND o.source_clip_ids && ARRAY[
    296368,296406,296453,296637,296684,296727,296769,296844,296891,296940,296985,297022,
    297109,297152,297204,297248,297292,297335,297379,297419,297469,297516,297559,297602,
    297638,297684,297734,297859]::bigint[])) THEN
   RAISE EXCEPTION 'retention protected: owner class recovery hold';
  END IF;
 ELSE
  IF EXISTS(SELECT 1 FROM nas_cleanup_r2_reserved_files f WHERE f.kind=NEW.kind AND f.source_id=NEW.source_id)
  OR (NEW.kind='raw' AND nas_cleanup_clip_held(NEW.source_id)) THEN
   RAISE EXCEPTION 'retention protected: NAS cleanup already reserves R2 recovery';
  END IF;
 END IF;
 RETURN NEW;
END $$;

-- Only purged_at can change under class-specific authority, and only after
-- the independent durable reservation commits AND this transaction records
-- owner-class retirement. Reservation alone cannot authorize a generic purge.
CREATE OR REPLACE FUNCTION guard_nas_cleanup_source() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF TG_OP='UPDATE' AND OLD.purged_at IS NULL AND NEW.purged_at IS NOT NULL
 AND (to_jsonb(OLD)-'purged_at')=(to_jsonb(NEW)-'purged_at')
 AND NOT nas_cleanup_owner_clip_held(OLD.id)
 AND EXISTS(SELECT 1 FROM r2_retirement_reservations x WHERE x.kind='raw' AND x.source_id=OLD.id
 AND x.retirement_class='owner_accepted_outside_window' AND x.bucket=OLD.bucket AND x.object_key=OLD.object_key
 AND x.size_bytes=OLD.size_bytes AND x.sha256=lower(OLD.sha256)
 AND EXISTS(SELECT 1 FROM r2_object_retirements r WHERE r.kind=x.kind AND r.source_id=x.source_id
 AND r.reason='owner_accepted_outside_window' AND r.manifest_sha256=x.manifest_sha256
 AND r.bucket=x.bucket AND r.object_key=x.object_key AND r.size_bytes=x.size_bytes AND r.sha256=x.sha256)) THEN RETURN NEW; END IF;
 IF nas_cleanup_clip_held(OLD.id) AND (TG_OP='DELETE' OR ROW(NEW.purged_at,NEW.bucket,NEW.object_key,NEW.sha256,NEW.size_bytes,NEW.display_path,NEW.clip_start_at,NEW.clip_end_at,NEW.recording_id)
 IS DISTINCT FROM ROW(OLD.purged_at,OLD.bucket,OLD.object_key,OLD.sha256,OLD.size_bytes,OLD.display_path,OLD.clip_start_at,OLD.clip_end_at,OLD.recording_id)) THEN
 RAISE EXCEPTION 'retention protected: NAS cleanup source/recovery hold'; END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;RETURN NEW;
END $$;
