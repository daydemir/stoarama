-- The NAS cleanup deletion master switch is a durable singleton, replacing the
-- NAS_CLEANUP_DELETION_ENABLED env var, so deploys no longer change it. The API
-- reads it live inside each decision it guards (FOR SHARE, so a change waits for
-- in-flight decisions) and treats a missing or unreadable row as OFF. Only the
-- audited `stoaramactl nas-cleanup master on|off` path changes it, and every
-- change appends one immutable history row.
CREATE TABLE nas_cleanup_master (
 singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
 enabled boolean NOT NULL,
 changed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 actor text NOT NULL CHECK (length(actor) BETWEEN 1 AND 200),
 reason text NOT NULL CHECK (length(reason) BETWEEN 1 AND 500)
);

CREATE TABLE nas_cleanup_master_history (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 changed_at timestamptz NOT NULL,
 actor text NOT NULL,
 old_enabled boolean,
 new_enabled boolean NOT NULL,
 reason text NOT NULL,
 CHECK (old_enabled IS DISTINCT FROM new_enabled)
);

CREATE FUNCTION guard_nas_cleanup_master() RETURNS trigger LANGUAGE plpgsql AS $$
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

CREATE FUNCTION record_nas_cleanup_master() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 INSERT INTO nas_cleanup_master_history(changed_at,actor,old_enabled,new_enabled,reason)
 VALUES(NEW.changed_at,NEW.actor,CASE WHEN TG_OP='UPDATE' THEN OLD.enabled END,NEW.enabled,NEW.reason);
 RETURN NULL;
END $$;
CREATE TRIGGER nas_cleanup_master_record AFTER INSERT OR UPDATE ON nas_cleanup_master
 FOR EACH ROW EXECUTE FUNCTION record_nas_cleanup_master();

-- History is written only by the trigger above and never changes.
CREATE FUNCTION guard_nas_cleanup_master_history() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP<>'INSERT' OR pg_trigger_depth()<2 THEN
  RAISE EXCEPTION 'NAS cleanup master history is append-only through the master control';
 END IF;
 RETURN NEW;
END $$;
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
