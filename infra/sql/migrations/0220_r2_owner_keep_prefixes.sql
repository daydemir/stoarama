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

INSERT INTO r2_owner_keep_prefixes(bucket,prefix,decision) VALUES
 ('stoarama','raw/stream/854/','Deniz 2026-11-19: "These two are actually for my thesis. That''s what that extra data probably is from: March to June. That''s for my thesis data so let''s actually keep that for now."'),
 ('stoarama','raw/stream/537/','Deniz 2026-11-19: "These two are actually for my thesis. That''s what that extra data probably is from: March to June. That''s for my thesis data so let''s actually keep that for now."');

CREATE FUNCTION r2_owner_keep_held(p_bucket text, p_key text) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM r2_owner_keep_prefixes k WHERE k.bucket=p_bucket AND starts_with(p_key,k.prefix))
$$;

CREATE FUNCTION guard_r2_owner_keep_retirement() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF r2_owner_keep_held(NEW.bucket,NEW.object_key) THEN
  RAISE EXCEPTION 'R2 OWNER KEEP HOLD: retirement forbidden for %', NEW.object_key;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER r2_owner_keep_reservation BEFORE INSERT OR UPDATE ON r2_retirement_reservations
 FOR EACH ROW EXECUTE FUNCTION guard_r2_owner_keep_retirement();
CREATE TRIGGER r2_owner_keep_retirement BEFORE INSERT OR UPDATE ON r2_object_retirements
 FOR EACH ROW EXECUTE FUNCTION guard_r2_owner_keep_retirement();
