-- Read-only stream-day NAS inventory. For seven GSS streams the catalog says the
-- best-14 raw is in R2, it is not, and the NAS copies are not inventoried, so no
-- restore can run. Deniz, verbatim: "serve the best 14 days of as many streams
-- as possible, fully stitched v85 ... via r2, through the nice web ui".
--
-- The operator queues one unit per (recording, local date). The r2-copy-v8-inventory
-- client leases a unit, lists that stream's raw and joined day directories
-- read-only, and reports each media file with the sha256 of the bytes it read.
-- The API matches each raw file to the recording's one clip of that name and
-- size starting on that day. The match is recorded here only: nas_inventory_files
-- stays the client's own sidecar-backed ledger, which cleanup and retirement
-- lanes read as exact-path evidence.

-- Controller-approved stream-days (the best-14 days). Insert to approve; delete
-- to withdraw. No unit is queued or leased, and no clip restored, outside it:
-- a verified restore also clears the clip's purged_at.
CREATE TABLE nas_restore_day_scope (
 recording_id bigint NOT NULL REFERENCES recordings(id) ON DELETE RESTRICT,
 local_date date NOT NULL,
 reason text NOT NULL CHECK (btrim(reason) <> ''),
 approved_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY (recording_id, local_date)
);
CREATE FUNCTION nas_restore_day_scope_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 RAISE EXCEPTION 'nas_restore_day_scope rows are never rewritten; delete and insert instead';
END $$;
CREATE TRIGGER nas_restore_day_scope_immutable BEFORE UPDATE ON nas_restore_day_scope
 FOR EACH ROW EXECUTE FUNCTION nas_restore_day_scope_immutable();

CREATE TABLE nas_day_inventory_units (
 id bigserial PRIMARY KEY,
 connection_id bigint NOT NULL REFERENCES connections(id),
 label text NOT NULL CHECK (label ~ '^[a-z0-9][a-z0-9._-]{0,79}$'),
 recording_id bigint NOT NULL REFERENCES recordings(id),
 local_date date NOT NULL,
 state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','leased','completed','failed','canceled')),
 attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
 max_attempts integer NOT NULL DEFAULT 3 CHECK (max_attempts BETWEEN 1 AND 10),
 leased_at timestamptz,
 lease_expires_at timestamptz,
 scan_started_at timestamptz,
 completed_at timestamptz,
 client_version text NOT NULL DEFAULT '',
 last_error text NOT NULL DEFAULT '' CHECK (octet_length(last_error) <= 500),
 created_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE (label, recording_id, local_date),
 CHECK ((state = 'leased') = (lease_expires_at IS NOT NULL))
);
CREATE UNIQUE INDEX nas_day_inventory_units_active ON nas_day_inventory_units (recording_id, local_date)
 WHERE state IN ('pending','leased');
CREATE INDEX nas_day_inventory_units_queue ON nas_day_inventory_units (connection_id, state, id);
CREATE FUNCTION nas_day_inventory_unit_scoped() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM nas_restore_day_scope s WHERE s.recording_id=NEW.recording_id AND s.local_date=NEW.local_date) THEN
  RAISE EXCEPTION 'recording % day % is not in nas_restore_day_scope', NEW.recording_id, NEW.local_date;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER nas_day_inventory_unit_scoped BEFORE INSERT ON nas_day_inventory_units
 FOR EACH ROW EXECUTE FUNCTION nas_day_inventory_unit_scoped();

CREATE TABLE nas_day_inventory_files (
 unit_id bigint NOT NULL REFERENCES nas_day_inventory_units(id),
 relative_path text NOT NULL CHECK (relative_path <> '' AND left(relative_path,1) <> '/' AND octet_length(relative_path) <= 1024),
 kind text NOT NULL CHECK (kind IN ('raw','joined')),
 size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
 mtime_ns bigint NOT NULL CHECK (mtime_ns >= 0),
 sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
 -- The recording's only clip of the unit's local day with this file's name and
 -- size, when no other file of the unit has them.
 clip_id bigint REFERENCES recording_clips(id),
 PRIMARY KEY (unit_id, relative_path),
 CHECK ((kind = 'joined') = (relative_path LIKE 'joined/%')),
 CHECK (kind = 'raw' OR clip_id IS NULL)
);
CREATE INDEX nas_day_inventory_files_clip ON nas_day_inventory_files (clip_id) WHERE clip_id IS NOT NULL;

-- An 'original' restore of a clip a completed unit of this connection matched,
-- from the exact path where the unit read the clip's catalogued size and
-- sha256, only to the clip's own key, only for a clip starting on the unit's
-- local day while that day is in nas_restore_day_scope, and never while any
-- R2 deletion of the key is pending (0262's predicate). Keep Everything and
-- purged_at are not required: the PUT is create-only and signed with the
-- sha256, an existing object is re-hashed and never overwritten, and
-- retirement history stays as it is. Canceling the unit or deleting the scope
-- row withdraws the restore.
CREATE FUNCTION nas_restore_inventoried_original(target text, conn bigint, clip bigint, key text, size bigint, sha text, path text) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT target='original' AND EXISTS(SELECT 1 FROM recording_clips c
  JOIN nas_day_inventory_files f ON f.clip_id=c.id
  JOIN nas_day_inventory_units u ON u.id=f.unit_id AND u.state='completed' AND u.connection_id=conn AND u.recording_id=c.recording_id
  JOIN recordings r ON r.id=c.recording_id
  JOIN nas_media_keep20_windows w ON w.stream_id=r.stream_id
  JOIN nas_restore_day_scope s ON s.recording_id=u.recording_id AND s.local_date=u.local_date
  WHERE c.id=clip AND c.object_key=key AND c.size_bytes=size AND lower(c.sha256)=sha
   AND f.relative_path=path AND f.size_bytes=size AND f.sha256=sha
   AND (c.clip_start_at AT TIME ZONE w.timezone)::date=u.local_date
   AND NOT collation_v2_recovery_retirement_pending(key))
$$;

CREATE OR REPLACE FUNCTION nas_restore_admitted(q nas_restore_requests) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT nas_restore_ke_original(q.target,q.clip_id,q.object_key,q.size_bytes,q.sha256,q.relative_path)
  OR nas_restore_scoped_output(q.target,q.output_id,q.connection_id,q.object_key,q.size_bytes,q.sha256,q.relative_path,q.content_type)
  OR nas_restore_inventoried_original(q.target,q.connection_id,q.clip_id,q.object_key,q.size_bytes,q.sha256,q.relative_path)
$$;
