-- Additive COPY diagnostics; no activation or deletion authority.
ALTER TABLE nas_r2_copy_items
 ADD COLUMN blocked_reason text NOT NULL DEFAULT '' CHECK(blocked_reason IN ('','source_changed','r2_identity','final_conflict','sidecar_conflict','partial_conflict','path_conflict','checksum_mismatch')),
 ADD COLUMN completion text NOT NULL DEFAULT '' CHECK(completion IN ('','verified_in_place','downloaded')),
 ADD COLUMN range_length bigint NOT NULL DEFAULT 0,
 ADD COLUMN downloaded_bytes bigint NOT NULL DEFAULT 0 CHECK(downloaded_bytes>=0),
 ADD COLUMN committed_bytes bigint NOT NULL DEFAULT 0 CHECK(committed_bytes>=0),
 ADD COLUMN updated_at timestamptz NOT NULL DEFAULT clock_timestamp();
UPDATE nas_r2_copy_items SET blocked_reason='source_changed' WHERE state='blocked';
CREATE INDEX nas_r2_copy_protected_key ON nas_r2_copy_items((item->>'bucket'),(item->>'object_key')) WHERE state<>'verified';

-- This is also the durable per-item mismatch latch. Reimport only updates the
-- identical source_id and cannot clear a blocked state or change its identity.
CREATE FUNCTION guard_nas_r2_copy_block() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF OLD.state='blocked' AND (TG_OP='DELETE' OR NEW.state<>'blocked' OR NEW.item<>OLD.item OR NEW.blocked_reason<>OLD.blocked_reason) THEN
  RAISE EXCEPTION 'COPY blocked item requires separately reviewed reconciliation';
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER nas_r2_copy_block_latch BEFORE UPDATE OR DELETE ON nas_r2_copy_items FOR EACH ROW EXECUTE FUNCTION guard_nas_r2_copy_block();

-- Retirement publishes intent on another connection while its outer transaction
-- owns the global admission fence. Never reacquire that global fence here.
-- Both independent intent publication and COPY import serialize on this exact
-- key instead; hash collisions only conservatively serialize unrelated keys.
CREATE FUNCTION lock_nas_r2_copy_key(b text,k text) RETURNS void LANGUAGE sql VOLATILE AS $$
 SELECT pg_advisory_xact_lock(hashtextextended(jsonb_build_array(b,k)::text,7343226011200));
$$;
CREATE FUNCTION guard_r2_copy_reservation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 PERFORM lock_nas_r2_copy_key(NEW.bucket,NEW.object_key);
 IF EXISTS(SELECT 1 FROM nas_r2_copy_items WHERE state<>'verified'
  AND item->>'bucket'=NEW.bucket AND item->>'object_key'=NEW.object_key) THEN
  RAISE EXCEPTION 'COPY custody pending or blocked: R2 key protected';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER r2_copy_reservation_guard BEFORE INSERT ON r2_retirement_reservations FOR EACH ROW EXECUTE FUNCTION guard_r2_copy_reservation();

-- Serialize initial custody admission with retirement intent; neither side can
-- win from a stale read. Normal exact-manifest ON CONFLICT imports stay valid.
CREATE FUNCTION guard_nas_r2_copy_admission() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 PERFORM lock_nas_r2_copy_key(NEW.item->>'bucket',NEW.item->>'object_key');
 IF EXISTS(SELECT 1 FROM r2_retirement_reservations WHERE bucket=NEW.item->>'bucket' AND object_key=NEW.item->>'object_key') THEN
  RAISE EXCEPTION 'COPY source has retirement intent; preserved';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER nas_r2_copy_admission BEFORE INSERT ON nas_r2_copy_items FOR EACH ROW EXECUTE FUNCTION guard_nas_r2_copy_admission();
