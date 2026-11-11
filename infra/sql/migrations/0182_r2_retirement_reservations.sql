-- Durable R2 deletion intent. It is committed BEFORE issuing DeleteObject and
-- survives a lost response/DB commit, so cleanup can never use that key again
-- as a recovery copy. Reconciliation is operator-owned; no automatic expiry.
CREATE TABLE r2_retirement_reservations (
 kind text NOT NULL CHECK(kind IN ('raw','collated')),
 source_id bigint NOT NULL CHECK(source_id>0),
 bucket text NOT NULL,
 object_key text NOT NULL,
 size_bytes bigint NOT NULL CHECK(size_bytes>0),
 sha256 text NOT NULL CHECK(sha256 ~ '^[a-f0-9]{64}$'),
 manifest_sha256 text NOT NULL CHECK(manifest_sha256 ~ '^[a-f0-9]{64}$'),
 created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(kind,source_id), UNIQUE(bucket,object_key)
);
CREATE TRIGGER r2_retirement_reservations_append_only BEFORE UPDATE OR DELETE ON r2_retirement_reservations
 FOR EACH ROW EXECUTE FUNCTION guard_r2_object_retirement();

-- Existing admission takes 7343226011103 before catalog locks. r2-retire takes
-- that same fence, then the public fence, before committing a reservation.
-- DB guards also cover older API binaries and direct plan/source-hold inserts.
CREATE FUNCTION guard_nas_cleanup_r2_retirement() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_TABLE_NAME='nas_cleanup_plans' THEN
  IF EXISTS(SELECT 1 FROM jsonb_array_elements(NEW.manifest) i
    JOIN r2_retirement_reservations r ON r.kind=i->>'kind' AND r.source_id=(i->>'source_id')::bigint) THEN
   RAISE EXCEPTION 'retention protected: R2 retirement pending or completed';
  END IF;
 ELSE
  IF EXISTS(SELECT 1 FROM r2_retirement_reservations r WHERE r.kind='raw' AND r.source_id=NEW.clip_id) THEN
   RAISE EXCEPTION 'retention protected: R2 recovery copy retirement pending or completed';
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER nas_cleanup_r2_retirement_plan BEFORE INSERT ON nas_cleanup_plans
 FOR EACH ROW EXECUTE FUNCTION guard_nas_cleanup_r2_retirement();
CREATE TRIGGER nas_cleanup_r2_retirement_source BEFORE INSERT ON nas_cleanup_source_holds
 FOR EACH ROW EXECUTE FUNCTION guard_nas_cleanup_r2_retirement();

CREATE FUNCTION guard_r2_retirement_reservation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM nas_cleanup_reserved_files f WHERE f.kind=NEW.kind AND f.source_id=NEW.source_id)
 OR (NEW.kind='raw' AND EXISTS(SELECT 1 FROM nas_cleanup_source_holds h JOIN nas_cleanup_plans p ON p.id=h.plan_id
   WHERE h.clip_id=NEW.source_id AND (p.mode<>'retired' OR EXISTS(SELECT 1 FROM nas_cleanup_items i WHERE i.plan_id=p.id AND i.delete_ever_issued)))) THEN
  RAISE EXCEPTION 'retention protected: NAS cleanup already reserves R2 recovery';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER r2_retirement_reservation_cleanup_guard BEFORE INSERT ON r2_retirement_reservations
 FOR EACH ROW EXECUTE FUNCTION guard_r2_retirement_reservation();
