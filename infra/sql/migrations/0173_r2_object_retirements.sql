-- r2-retire: an append-only record of every R2 object that
-- `stoaramactl r2-retire apply` deleted, with the reason and the approved
-- manifest digest. Grants no deletion authority by itself.
--
-- Raw clips additionally get recording_clips.purged_at (the existing
-- convention used by nas-verified-purge and joined-source-purge, so every
-- reader that already skips purged clips keeps working, and the NAS cleanup
-- and joined retention triggers still veto held clips). Stitched outputs have
-- no purged_at column; this table is their only "gone from R2" marker, and the
-- collated NAS feed skips outputs listed here.
CREATE TABLE r2_object_retirements (
 id bigserial PRIMARY KEY,
 kind text NOT NULL CHECK (kind IN ('raw','collated')),
 source_id bigint NOT NULL,
 stream_id bigint NOT NULL,
 bucket text NOT NULL CHECK (length(bucket) > 0),
 object_key text NOT NULL CHECK (length(object_key) > 0 AND object_key = btrim(object_key)),
 size_bytes bigint NOT NULL CHECK (size_bytes > 0),
 sha256 text NOT NULL CHECK (sha256 ~ '^[a-f0-9]{64}$'),
 reason text NOT NULL CHECK (reason ~ '^[a-z0-9_]{1,80}$'),
 -- The owner's R2 pick for the stream at apply time. 'all' (keep everything)
 -- and undecided streams can never be retired.
 choice text NOT NULL CHECK (choice IN ('none','st','keep')),
 manifest_sha256 text NOT NULL CHECK (manifest_sha256 ~ '^[a-f0-9]{64}$'),
 policy_sha256 text NOT NULL CHECK (policy_sha256 ~ '^[a-f0-9]{64}$'),
 already_absent boolean NOT NULL DEFAULT false,
 retired_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE (kind, source_id),
 UNIQUE (bucket, object_key),
 -- Protected prefixes are refused at the database too.
 CHECK (object_key NOT LIKE 'joined/best14-20261021/%' AND object_key NOT LIKE 'staging/%'),
 CHECK ((kind = 'raw' AND object_key LIKE 'managed/%')
     OR (kind = 'collated' AND (object_key LIKE 'joined/goodplus-%' OR object_key LIKE 'joined/nightly-collation-v2/%')))
);
CREATE INDEX r2_object_retirements_retired_at_idx ON r2_object_retirements (retired_at);

CREATE FUNCTION guard_r2_object_retirement() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 RAISE EXCEPTION 'r2 object retirements are append-only';
END $$;
CREATE TRIGGER r2_object_retirements_append_only BEFORE UPDATE OR DELETE ON r2_object_retirements
 FOR EACH ROW EXECUTE FUNCTION guard_r2_object_retirement();
