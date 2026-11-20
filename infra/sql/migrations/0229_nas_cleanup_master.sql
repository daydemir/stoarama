-- The NAS cleanup deletion master switch is a durable singleton, replacing the
-- NAS_CLEANUP_DELETION_ENABLED env var, so deploys no longer change it. The API
-- reads it live inside each decision it guards (FOR SHARE, so a change waits for
-- in-flight decisions) and treats a missing or unreadable row as OFF. Only the
-- controller credential changes it through the audited
-- `stoaramactl nas-cleanup master on|off` path, and every change appends one
-- immutable history row.
CREATE TABLE nas_cleanup_master (
 singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
 enabled boolean NOT NULL,
 changed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 -- actor is the authenticated server-side principal; claimed_by is the
 -- operator-supplied name. Evidence only: never authority.
 actor text NOT NULL CHECK (length(actor) BETWEEN 1 AND 200),
 claimed_by text NOT NULL DEFAULT '' CHECK (length(claimed_by) <= 200),
 reason text NOT NULL CHECK (reason = btrim(reason) AND length(reason) BETWEEN 1 AND 500)
);

CREATE TABLE nas_cleanup_master_history (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 changed_at timestamptz NOT NULL,
 actor text NOT NULL,
 claimed_by text NOT NULL DEFAULT '',
 old_enabled boolean,
 new_enabled boolean NOT NULL,
 reason text NOT NULL,
 CHECK (old_enabled IS DISTINCT FROM new_enabled)
);

CREATE FUNCTION guard_nas_cleanup_master() RETURNS trigger LANGUAGE plpgsql SET search_path = '' AS $$
BEGIN
 IF TG_OP IN ('DELETE','TRUNCATE') THEN
  RAISE EXCEPTION 'NAS cleanup master control cannot be removed';
 END IF;
 IF TG_OP='UPDATE' AND NEW.enabled=OLD.enabled THEN
  RAISE EXCEPTION 'NAS cleanup master change must flip enabled';
 END IF;
 NEW.changed_at := clock_timestamp();
 RETURN NEW;
END $$;
CREATE TRIGGER nas_cleanup_master_guard BEFORE INSERT OR UPDATE OR DELETE ON nas_cleanup_master
 FOR EACH ROW EXECUTE FUNCTION guard_nas_cleanup_master();
CREATE TRIGGER nas_cleanup_master_no_truncate BEFORE TRUNCATE ON nas_cleanup_master
 FOR EACH STATEMENT EXECUTE FUNCTION guard_nas_cleanup_master();

-- History is appended only by record_nas_cleanup_master(). It is SECURITY
-- DEFINER, owned by a dedicated insert-only role, and bound at creation time to
-- this schema's history table with an explicit empty search_path, so neither a
-- caller's search_path nor a later CREATE in another schema can redirect the
-- write. The guard rejects every insert that does not come from the recorder.
--
-- Provenance is load-bearing: if the dedicated role cannot be created or
-- assumed, this migration FAILS. Falling back to the migrating role as the
-- recorder owner would make the actor on every history row the runtime role,
-- which is exactly the evidence a compromised runtime could forge.
--
-- Migrator privileges (see docs/NAS_CLEANUP.md): ownership of the schema (to
-- CREATE objects and transfer the recorder), CREATE on the schema for the new
-- owner at transfer time (granted and revoked below), and either CREATEROLE or
-- a pre-created `nas_cleanup_master_writer` role granted to the migrator WITH
-- SET TRUE. Nothing here needs a superuser, and the API runtime role needs no
-- part of it: it only executes the recorder through its triggers.
DO $$
DECLARE
  history_table text := quote_ident(current_schema()) || '.nas_cleanup_master_history';
  history_schema text := quote_ident(current_schema());
  writer name := 'nas_cleanup_master_writer';
BEGIN
  BEGIN
    CREATE ROLE nas_cleanup_master_writer NOLOGIN;
  EXCEPTION
    WHEN duplicate_object THEN NULL;
    -- Without CREATEROLE the role must already exist by design.
    WHEN insufficient_privilege THEN NULL;
  END;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = writer) THEN
    RAISE EXCEPTION 'migration 0229 needs a dedicated recorder role: create role nas_cleanup_master_writer NOLOGIN and grant it to the migrator WITH SET TRUE, or run the migration as a role with CREATEROLE'
      USING HINT = 'refusing to fall back to the migrating role as the history recorder owner';
  END IF;
  IF NOT pg_has_role(current_user, writer, 'MEMBER') THEN
    -- A CREATEROLE migrator becomes the new role's admin but, since
    -- PostgreSQL 16, not automatically able to SET ROLE to it. Grant the
    -- membership it needs; if it cannot, fail rather than fall back.
    BEGIN
      EXECUTE format('GRANT %I TO %I WITH SET TRUE', writer, current_user);
    EXCEPTION WHEN insufficient_privilege THEN
      RAISE EXCEPTION 'migrator % is not a member of %', current_user, writer
        USING HINT = 'grant the recorder role to the migrator WITH SET TRUE, or run the migration as a role with CREATEROLE';
    END;
    IF NOT pg_has_role(current_user, writer, 'MEMBER') THEN
      RAISE EXCEPTION 'migrator % is not a member of % after granting it', current_user, writer
        USING HINT = 'refusing to fall back to the migrating role as the history recorder owner';
    END IF;
  END IF;
  -- ALTER ... OWNER TO requires the migrator to be able to SET ROLE to the new
  -- owner. A membership granted WITH SET FALSE passes the member test but
  -- cannot be assumed, so upgrade it when possible and fail loudly if not.
  IF current_user <> writer AND NOT (SELECT rolsuper FROM pg_roles WHERE rolname = current_user)
     AND EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = 'pg_catalog.pg_auth_members'::regclass AND attname = 'set_option')
     AND NOT EXISTS (SELECT 1 FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.roleid JOIN pg_roles g ON g.oid = m.member
                      WHERE r.rolname = writer AND g.rolname = current_user AND m.set_option) THEN
    BEGIN
      EXECUTE format('GRANT %I TO %I WITH SET TRUE', writer, current_user);
    EXCEPTION WHEN insufficient_privilege THEN
      RAISE EXCEPTION 'migrator % is a member of % but cannot SET ROLE to it', current_user, writer
        USING HINT = 'grant the recorder role WITH SET TRUE so the migrator can transfer its ownership';
    END;
    IF NOT EXISTS (SELECT 1 FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.roleid JOIN pg_roles g ON g.oid = m.member
                    WHERE r.rolname = writer AND g.rolname = current_user AND m.set_option) THEN
      RAISE EXCEPTION 'migrator % cannot assume the recorder role %', current_user, writer
        USING HINT = 'refusing to fall back to the migrating role as the history recorder owner';
    END IF;
  END IF;
  -- ALTER FUNCTION ... OWNER TO requires the incoming owner to hold CREATE on
  -- the schema at transfer time. Grant it for the transfer and take it back in
  -- this same transaction, so the insert-only role never keeps it.
  EXECUTE format('GRANT USAGE ON SCHEMA %s TO %I', history_schema, writer);
  EXECUTE format('GRANT CREATE ON SCHEMA %s TO %I', history_schema, writer);
  EXECUTE format('GRANT INSERT ON %s TO %I', history_table, writer);
  EXECUTE format('REVOKE ALL ON %s FROM PUBLIC', history_table);
  EXECUTE format($fn$
CREATE FUNCTION %s.record_nas_cleanup_master() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = '' AS $rec$
BEGIN
  -- Only the master table's own trigger, and only for an insert or update,
  -- may append evidence. A trigger on any other table, or on another event,
  -- that calls this recorder is refused.
  IF TG_OP NOT IN ('INSERT','UPDATE') THEN
    RAISE EXCEPTION 'NAS cleanup master history records only master insert/update';
  END IF;
  IF TG_RELID IS DISTINCT FROM '%s.nas_cleanup_master'::regclass THEN
    RAISE EXCEPTION 'NAS cleanup master recorder is bound to the master table';
  END IF;
  INSERT INTO %s(changed_at,actor,claimed_by,old_enabled,new_enabled,reason)
  VALUES(NEW.changed_at,NEW.actor,NEW.claimed_by,
    CASE WHEN TG_OP='UPDATE' THEN OLD.enabled END,NEW.enabled,NEW.reason);
  RETURN NULL;
END $rec$;
$fn$, history_schema, history_schema, history_table);
  EXECUTE format('ALTER FUNCTION %s.record_nas_cleanup_master() OWNER TO %I', history_schema, writer);
  EXECUTE format('REVOKE CREATE ON SCHEMA %s FROM %I', history_schema, writer);
  EXECUTE format($fn$
CREATE FUNCTION %s.guard_nas_cleanup_master_history() RETURNS trigger
LANGUAGE plpgsql SET search_path = '' AS $grd$
BEGIN
  IF TG_OP<>'INSERT' OR pg_trigger_depth()<2 THEN
    RAISE EXCEPTION 'NAS cleanup master history is append-only through the master control';
  END IF;
  -- Only the recorder (the dedicated insert-only owner) may append.
  IF current_user IS DISTINCT FROM %L THEN
    RAISE EXCEPTION 'NAS cleanup master history is written only by its recorder';
  END IF;
  RETURN NEW;
END $grd$;
$fn$, history_schema, writer);
END $$;
CREATE TRIGGER nas_cleanup_master_record AFTER INSERT OR UPDATE ON nas_cleanup_master
 FOR EACH ROW EXECUTE FUNCTION record_nas_cleanup_master();
CREATE TRIGGER nas_cleanup_master_history_guard BEFORE INSERT OR UPDATE OR DELETE ON nas_cleanup_master_history
 FOR EACH ROW EXECUTE FUNCTION guard_nas_cleanup_master_history();
CREATE TRIGGER nas_cleanup_master_history_no_truncate BEFORE TRUNCATE ON nas_cleanup_master_history
 FOR EACH STATEMENT EXECUTE FUNCTION guard_nas_cleanup_master_history();

ALTER TABLE nas_cleanup_master ENABLE ALWAYS TRIGGER nas_cleanup_master_guard;
ALTER TABLE nas_cleanup_master ENABLE ALWAYS TRIGGER nas_cleanup_master_no_truncate;
ALTER TABLE nas_cleanup_master ENABLE ALWAYS TRIGGER nas_cleanup_master_record;
ALTER TABLE nas_cleanup_master_history ENABLE ALWAYS TRIGGER nas_cleanup_master_history_guard;
ALTER TABLE nas_cleanup_master_history ENABLE ALWAYS TRIGGER nas_cleanup_master_history_no_truncate;

INSERT INTO nas_cleanup_master(enabled,actor,reason)
VALUES(false,'migration 0229','initial state: OFF');
