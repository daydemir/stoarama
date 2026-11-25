-- Keep Everything NAS day units. Deniz, verbatim: "nas shoul dnot have any of
-- the keep everything (except the keep 20)"; "if its the same size and same
-- file name or whatever checks we have already?".
-- A KE unit names its exact media files (name and size, ascending); the plan
-- digest binds them and they are immutable like the rest of the unit. The
-- server permits a named file only while its R2 counterpart is live. Other
-- units keep names NULL.
ALTER TABLE nas_cleanup_day_units ADD COLUMN names jsonb
 CHECK(names IS NULL OR (jsonb_typeof(names)='array' AND jsonb_array_length(names) BETWEEN 1 AND 100000));
-- Named files the latest permit left out because their R2 counterpart is no longer live.
ALTER TABLE nas_cleanup_day_units ADD COLUMN dropped_names integer NOT NULL DEFAULT 0 CHECK(dropped_names>=0);

CREATE OR REPLACE FUNCTION guard_nas_cleanup_day_unit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' OR ROW(NEW.plan_id,NEW.ordinal,NEW.stream_id,NEW.relative_dir,NEW.local_date,NEW.files,NEW.bytes,NEW.names)
  IS DISTINCT FROM ROW(OLD.plan_id,OLD.ordinal,OLD.stream_id,OLD.relative_dir,OLD.local_date,OLD.files,OLD.bytes,OLD.names)
  OR NEW.attempt<OLD.attempt
  OR (jsonb_array_length(OLD.receipts)>0 AND (SELECT jsonb_agg(e ORDER BY n) FROM jsonb_array_elements(NEW.receipts)
   WITH ORDINALITY x(e,n) WHERE n<=jsonb_array_length(OLD.receipts)) IS DISTINCT FROM OLD.receipts)
  OR (OLD.state IN ('deleted','refused','client_refused') AND NEW.state<>OLD.state) THEN
  RAISE EXCEPTION 'NAS day cleanup unit identity and receipts are append-only';
 END IF;
 RETURN NEW;
END $$;
