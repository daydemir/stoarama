-- Every retirement class, including older binaries, observes the new holds.
SELECT pg_advisory_xact_lock(7343226011103);
SELECT pg_advisory_xact_lock(7343226011101);
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM r2_retirement_reservations x JOIN delivery_keep_holds h USING(bucket,object_key)) THEN
 RAISE EXCEPTION 'delivery keep import overlaps R2 intent; reconciliation required'; END IF;
END $$;
CREATE FUNCTION guard_delivery_r2_retirement() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF EXISTS(SELECT 1 FROM delivery_keep_holds h WHERE h.bucket=NEW.bucket AND h.object_key=NEW.object_key) THEN
 RAISE EXCEPTION 'DELIVERY KEEP HOLD: R2 retirement forbidden'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER delivery_r2_reservation BEFORE INSERT OR UPDATE ON r2_retirement_reservations FOR EACH ROW EXECUTE FUNCTION guard_delivery_r2_retirement();
CREATE TRIGGER delivery_r2_retirement BEFORE INSERT OR UPDATE ON r2_object_retirements FOR EACH ROW EXECUTE FUNCTION guard_delivery_r2_retirement();

-- owner_r2_delivery_only remains planning-only. This migration grants no
-- new retirement/release authority and changes no existing hold-release rules.
