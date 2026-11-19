-- Owner keep holds on R2 key prefixes. No R2 retirement path may reserve or
-- record the retirement of any key under one, whatever its catalog status.
--
-- Deniz, 2026-11-19, on streams 854 and 537, verbatim: "These two are actually
-- for my thesis. That's what that extra data probably is from: March to June.
-- That's for my thesis data so let's actually keep that for now."

CREATE TABLE r2_owner_keep_prefixes (
 bucket text NOT NULL CHECK(bucket<>''),
 prefix text NOT NULL CHECK(prefix ~ '^[a-z0-9][a-z0-9/_.-]*/$'),
 decision text NOT NULL CHECK(decision<>''),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bucket,prefix)
);

CREATE FUNCTION guard_r2_owner_keep_prefix_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 RAISE EXCEPTION 'R2 owner keep holds are append-only';
END $$;
CREATE TRIGGER r2_owner_keep_prefix_immutable BEFORE UPDATE OR DELETE ON r2_owner_keep_prefixes
 FOR EACH ROW EXECUTE FUNCTION guard_r2_owner_keep_prefix_immutable();
CREATE TRIGGER r2_owner_keep_prefix_no_truncate BEFORE TRUNCATE ON r2_owner_keep_prefixes
 FOR EACH STATEMENT EXECUTE FUNCTION guard_r2_owner_keep_prefix_immutable();


CREATE FUNCTION r2_owner_keep_held(p_bucket text, p_key text) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM r2_owner_keep_prefixes k WHERE k.bucket=p_bucket AND starts_with(p_key,k.prefix))
$$;

-- 7343226011220 serializes owner keep admission with every guarded ledger
-- write: writers take it shared, a new hold takes it exclusively.
CREATE FUNCTION guard_r2_owner_keep_retirement() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 PERFORM pg_advisory_xact_lock_shared(7343226011220);
 IF r2_owner_keep_held(NEW.bucket,NEW.object_key) THEN
  RAISE EXCEPTION 'R2 OWNER KEEP HOLD: retirement forbidden for %', NEW.object_key;
 END IF;
 RETURN NEW;
END $$;

-- Every R2 retirement ledger refuses a held key: reservations and receipts,
-- and the RAW and keep-28 intents and attempts. ENABLE ALWAYS keeps the guards
-- live in replica-mode sessions too.
DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['r2_retirement_reservations','r2_object_retirements',
  'r2_inverse_intents','r2_inverse_attempts','r2_keep28_intents','r2_keep28_attempts'] LOOP
  EXECUTE format('CREATE TRIGGER r2_owner_keep_hold BEFORE INSERT OR UPDATE ON %I FOR EACH ROW EXECUTE FUNCTION guard_r2_owner_keep_retirement()',t);
  EXECUTE format('ALTER TABLE %I ENABLE ALWAYS TRIGGER r2_owner_keep_hold',t);
 END LOOP;
END $$;

-- A held key's catalog row may not be marked purged.
CREATE FUNCTION guard_r2_owner_keep_purge() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.purged_at IS NULL AND NEW.purged_at IS NOT NULL THEN
  PERFORM pg_advisory_xact_lock_shared(7343226011220);
  IF r2_owner_keep_held(OLD.bucket,OLD.object_key) OR r2_owner_keep_held(NEW.bucket,NEW.object_key) THEN
   RAISE EXCEPTION 'R2 OWNER KEEP HOLD: purge forbidden for %', OLD.object_key;
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER r2_owner_keep_purge BEFORE UPDATE OF purged_at ON recording_clips
 FOR EACH ROW EXECUTE FUNCTION guard_r2_owner_keep_purge();
ALTER TABLE recording_clips ENABLE ALWAYS TRIGGER r2_owner_keep_purge;

-- A new hold serializes with R2 retirement and catalog purges: it takes the
-- protection locks that reservations take exclusively and the wire holds
-- shared, plus 7343226011220, which every guarded ledger write and purge takes
-- shared. Then it refuses to land over any pending reservation or unresolved
-- intent, so a hold never appears under a DELETE already authorized or a purge
-- already admitted.
CREATE FUNCTION guard_r2_owner_keep_admission() RETURNS trigger LANGUAGE plpgsql AS $$
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
    AND NOT EXISTS(SELECT 1 FROM r2_keep28_events v WHERE v.bucket=i.bucket AND v.object_key=i.object_key AND v.event='absent')) THEN
  RAISE EXCEPTION 'R2 owner keep hold % overlaps a pending R2 retirement', NEW.prefix;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER r2_owner_keep_admission BEFORE INSERT ON r2_owner_keep_prefixes
 FOR EACH ROW EXECUTE FUNCTION guard_r2_owner_keep_admission();
ALTER TABLE r2_owner_keep_prefixes ENABLE ALWAYS TRIGGER r2_owner_keep_admission;

-- The stitch-staging retirement ledger names its object by id; its key must
-- not be under an owner keep prefix either.
CREATE FUNCTION guard_r2_owner_keep_stage_retirement() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 PERFORM pg_advisory_xact_lock_shared(7343226011220);
 IF EXISTS(SELECT 1 FROM nas_stitch_r2_stage_objects o WHERE o.id=NEW.object_id AND r2_owner_keep_held(o.bucket,o.object_key)) THEN
  RAISE EXCEPTION 'R2 OWNER KEEP HOLD: stage retirement forbidden for object %', NEW.object_id;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER r2_owner_keep_stage_retirement BEFORE INSERT ON nas_stitch_r2_stage_retirements
 FOR EACH ROW EXECUTE FUNCTION guard_r2_owner_keep_stage_retirement();
ALTER TABLE nas_stitch_r2_stage_retirements ENABLE ALWAYS TRIGGER r2_owner_keep_stage_retirement;

-- Seed after admission is installed, so a conflict with a pending retirement
-- fails this migration instead of landing silently.
INSERT INTO r2_owner_keep_prefixes(bucket,prefix,decision) VALUES
 ('stoarama','raw/stream/854/','Deniz 2026-11-19: "These two are actually for my thesis. That''s what that extra data probably is from: March to June. That''s for my thesis data so let''s actually keep that for now."'),
 ('stoarama','raw/stream/537/','Deniz 2026-11-19: "These two are actually for my thesis. That''s what that extra data probably is from: March to June. That''s for my thesis data so let''s actually keep that for now."');

ALTER TABLE r2_owner_keep_prefixes ENABLE ALWAYS TRIGGER r2_owner_keep_prefix_immutable;
ALTER TABLE r2_owner_keep_prefixes ENABLE ALWAYS TRIGGER r2_owner_keep_prefix_no_truncate;
