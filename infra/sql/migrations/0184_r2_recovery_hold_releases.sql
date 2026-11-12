-- Append-only operator authority superseding the old R2-copy standing rule
-- for exact, verified NAS deletion receipts only. Never discard source data.
CREATE TABLE nas_cleanup_r2_releases (
 item_id bigint PRIMARY KEY REFERENCES nas_cleanup_items(id),
 manifest_sha256 text NOT NULL CHECK(manifest_sha256 ~ '^[a-f0-9]{64}$'),
 picks_sha256 text NOT NULL CHECK(picks_sha256 ~ '^[a-f0-9]{64}$'),
 policy_sha256 text NOT NULL CHECK(policy_sha256 ~ '^[a-f0-9]{64}$'),
 authority jsonb NOT NULL, reason text NOT NULL CHECK(length(reason)>0),
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TRIGGER nas_cleanup_r2_release_append_only BEFORE UPDATE OR DELETE ON nas_cleanup_r2_releases
 FOR EACH ROW EXECUTE FUNCTION guard_r2_object_retirement();
-- Freeze incident identities independently of receipt state or later plans.
CREATE VIEW nas_cleanup_incident_files AS
 SELECT 'raw'::text kind,source_id FROM unnest(ARRAY[
 296368,296406,296453,296637,296684,296727,296769,296844,296891,296940,296985,297022,
 297109,297152,297204,297248,297292,297335,297379,297419,297469,297516,297559,297602,
 297638,297684,297734,297859]::bigint[]) source_id;
CREATE VIEW nas_cleanup_incident_sources AS SELECT source_id clip_id FROM nas_cleanup_incident_files;
CREATE FUNCTION guard_nas_cleanup_r2_release() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE e jsonb; i nas_cleanup_items; p nas_cleanup_plans;
BEGIN
 SELECT * INTO STRICT i FROM nas_cleanup_items WHERE id=NEW.item_id FOR UPDATE;
 SELECT * INTO STRICT p FROM nas_cleanup_plans WHERE id=i.plan_id FOR SHARE;
 e:=coalesce(i.item,p.manifest->(i.ordinal-1));
 IF i.state<>'deleted' OR NOT i.delete_ever_issued OR i.recovery NOT IN ('r2_sha_verified','r2_etag_bound')
 OR coalesce(i.outcome->>'outcome','')<>'deleted'
 OR NOT coalesce(NEW.authority->'candidate'->>'choice' IN ('none','st','keep')
 AND NEW.authority->'candidate'->>'source_id'=e->>'source_id'
 AND NEW.authority->'candidate'->>'kind'=e->>'kind'
 AND NEW.authority->>'recovery'=i.recovery
 AND NEW.authority->>'receipt_sha256'=encode(sha256(convert_to(i.outcome::text,'UTF8')),'hex')
 AND NEW.authority->>'item_sha256'=encode(sha256(convert_to(e::text,'UTF8')),'hex')
 AND NEW.authority->>'recovery_sha256'=encode(sha256(convert_to(coalesce(i.recovery_evidence::text,''),'UTF8')),'hex'),false)
 OR (i.recovery='r2_etag_bound' AND NOT coalesce(i.recovery_evidence->>'object_key'=e->>'object_key'
 AND i.recovery_evidence->>'sha256'=e->>'sha256' AND (i.recovery_evidence->>'size_bytes')::bigint=(e->>'size_bytes')::bigint
 AND length(i.recovery_evidence->>'etag')>0 AND length(i.recovery_evidence->>'verified_at')>0,false))
 OR EXISTS(SELECT 1 FROM nas_cleanup_incident_files f WHERE f.kind=e->>'kind' AND f.source_id=(e->>'source_id')::bigint)
 OR (e->>'kind'='raw' AND EXISTS(SELECT 1 FROM nas_cleanup_incident_sources f WHERE f.clip_id=(e->>'source_id')::bigint))
 OR (e->>'kind'='collated' AND EXISTS(SELECT 1 FROM recording_collation_outputs o CROSS JOIN LATERAL unnest(o.source_clip_ids) x JOIN nas_cleanup_incident_sources f ON f.clip_id=x WHERE o.id=(e->>'source_id')::bigint)) THEN
  RAISE EXCEPTION 'retention protected: receipt is uncertain or incident protected';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER nas_cleanup_r2_release_guard BEFORE INSERT ON nas_cleanup_r2_releases
 FOR EACH ROW EXECUTE FUNCTION guard_nas_cleanup_r2_release();

CREATE VIEW nas_cleanup_r2_effective_releases AS
 SELECT r.* FROM nas_cleanup_r2_releases r JOIN nas_cleanup_items i ON i.id=r.item_id
 WHERE i.state='deleted' AND i.delete_ever_issued AND i.recovery IN ('r2_sha_verified','r2_etag_bound') AND i.outcome->>'outcome'='deleted'
 AND r.authority->>'recovery'=i.recovery
 AND r.authority->>'receipt_sha256'=encode(sha256(convert_to(i.outcome::text,'UTF8')),'hex')
 AND r.authority->>'recovery_sha256'=encode(sha256(convert_to(coalesce(i.recovery_evidence::text,''),'UTF8')),'hex');
CREATE VIEW nas_cleanup_r2_reserved_files AS
 SELECT p.account_id,p.connection_id,p.label,e.value->>'kind' kind,(e.value->>'source_id')::bigint source_id
 FROM nas_cleanup_plans p CROSS JOIN LATERAL jsonb_array_elements(p.manifest) WITH ORDINALITY e(value,n)
 LEFT JOIN nas_cleanup_items i ON i.plan_id=p.id AND i.ordinal=e.n
 WHERE (p.mode<>'retired' OR i.delete_ever_issued OR i.id IS NULL)
 AND NOT EXISTS(SELECT 1 FROM nas_cleanup_r2_effective_releases r WHERE r.item_id=i.id);

-- Source holds can span every raw source of collated output. Release a raw
-- source only if ALL items in that plan which use it have explicit releases;
-- a missing/unmapped item or incident identity retains the hold.
CREATE OR REPLACE FUNCTION nas_cleanup_clip_held(clip bigint) RETURNS boolean LANGUAGE sql VOLATILE AS $$
 SELECT EXISTS(SELECT 1 FROM nas_cleanup_incident_sources WHERE clip_id=clip)
 OR EXISTS(SELECT 1 FROM nas_cleanup_source_holds h JOIN nas_cleanup_plans p ON p.id=h.plan_id
 WHERE h.clip_id=clip AND (p.mode<>'retired' OR EXISTS(SELECT 1 FROM nas_cleanup_items i WHERE i.plan_id=p.id AND i.delete_ever_issued))
 AND (NOT EXISTS(SELECT 1 FROM jsonb_array_elements(p.manifest) e WHERE (e->'source_ids') @> to_jsonb(ARRAY[clip]) OR (e->>'kind'='raw' AND (e->>'source_id')::bigint=clip))
 OR EXISTS(SELECT 1 FROM jsonb_array_elements(p.manifest) WITH ORDINALITY e(value,n)
 LEFT JOIN nas_cleanup_items i ON i.plan_id=p.id AND i.ordinal=e.n
 WHERE ((e.value->'source_ids') @> to_jsonb(ARRAY[clip]) OR (e.value->>'kind'='raw' AND (e.value->>'source_id')::bigint=clip))
 AND (p.mode<>'retired' OR i.delete_ever_issued OR i.id IS NULL)
 AND NOT EXISTS(SELECT 1 FROM nas_cleanup_r2_effective_releases r WHERE r.item_id=i.id))))
$$;
CREATE OR REPLACE FUNCTION guard_nas_cleanup_output() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF (TG_OP='DELETE' OR ROW(NEW.nas_relative_path,NEW.object_key,NEW.sha256,NEW.size_bytes,NEW.start_at,NEW.end_at,NEW.source_clip_ids,NEW.collation_hour_id) IS DISTINCT FROM ROW(OLD.nas_relative_path,OLD.object_key,OLD.sha256,OLD.size_bytes,OLD.start_at,OLD.end_at,OLD.source_clip_ids,OLD.collation_hour_id))
 AND (EXISTS(SELECT 1 FROM nas_cleanup_r2_reserved_files f WHERE f.kind='collated' AND f.source_id=OLD.id)
 OR EXISTS(SELECT 1 FROM nas_cleanup_incident_files f WHERE f.kind='collated' AND f.source_id=OLD.id)) THEN
 RAISE EXCEPTION 'retention protected: NAS cleanup collated identity hold'; END IF;
 IF TG_OP='DELETE' THEN RETURN OLD;END IF;RETURN NEW;
END $$;
CREATE OR REPLACE FUNCTION guard_r2_retirement_reservation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM nas_cleanup_r2_reserved_files f WHERE f.kind=NEW.kind AND f.source_id=NEW.source_id)
 OR (NEW.kind='raw' AND nas_cleanup_clip_held(NEW.source_id)) THEN
 RAISE EXCEPTION 'retention protected: NAS cleanup already reserves R2 recovery'; END IF;
 RETURN NEW;
END $$;
