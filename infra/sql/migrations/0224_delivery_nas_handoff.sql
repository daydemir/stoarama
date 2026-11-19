-- The reviewed public-video roster augments ordinary collated delivery for
-- account 47, NAS connection 13. This migration queues no copies and alters
-- no retention or publication authority.
ALTER TABLE connections ADD COLUMN delivery_nas_handoff_enabled boolean NOT NULL DEFAULT false,
 ADD COLUMN delivery_nas_handoff_roster_sha256 text,
 ADD CONSTRAINT delivery_nas_handoff_connection_scope CHECK (
  NOT delivery_nas_handoff_enabled OR (id=13 AND account_id=47 AND kind='nas_pull'
   AND coalesce(delivery_nas_handoff_roster_sha256,'') ~ '^[a-f0-9]{64}$')
 );

CREATE TABLE delivery_nas_handoff_outputs (
 output_id bigint PRIMARY KEY REFERENCES recording_collation_outputs(id) ON DELETE RESTRICT,
 recording_id bigint NOT NULL REFERENCES recordings(id) ON DELETE RESTRICT,
 roster_sha256 text NOT NULL CHECK (roster_sha256 ~ '^[a-f0-9]{64}$'),
 picks_sha256 text NOT NULL CHECK (picks_sha256 ~ '^[a-f0-9]{64}$'),
 bucket text NOT NULL CHECK (bucket='stoarama'),
 object_key text NOT NULL UNIQUE CHECK (object_key ~ '^joined/(goodplus-[^/]+|nightly-collation-v2)/'),
 etag text NOT NULL CHECK (etag<>''),
 size_bytes bigint NOT NULL CHECK (size_bytes>0),
 sha256 text NOT NULL CHECK (sha256 ~ '^[a-f0-9]{64}$'),
 nas_relative_path text NOT NULL CHECK (nas_relative_path<>''),
 approved_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE FUNCTION reject_delivery_nas_handoff_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 RAISE EXCEPTION 'delivery NAS handoff approval is immutable';
END
$$;
CREATE TRIGGER delivery_nas_handoff_immutable
 BEFORE UPDATE OR DELETE ON delivery_nas_handoff_outputs
 FOR EACH ROW EXECUTE FUNCTION reject_delivery_nas_handoff_mutation();
CREATE TRIGGER delivery_nas_handoff_no_truncate
 BEFORE TRUNCATE ON delivery_nas_handoff_outputs
 FOR EACH STATEMENT EXECUTE FUNCTION reject_delivery_nas_handoff_mutation();

-- Round zero copies missing files. The controller starts a new round for
-- an independent final readback of the complete approved roster.
CREATE TABLE delivery_nas_handoff_rounds (
 roster_sha256 text NOT NULL CHECK(roster_sha256 ~ '^[a-f0-9]{64}$'),
 round bigint NOT NULL CHECK(round>0),
 started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(roster_sha256,round)
);
CREATE TRIGGER delivery_nas_handoff_rounds_immutable
 BEFORE UPDATE OR DELETE ON delivery_nas_handoff_rounds
 FOR EACH ROW EXECUTE FUNCTION reject_delivery_nas_handoff_mutation();
CREATE TRIGGER delivery_nas_handoff_rounds_no_truncate
 BEFORE TRUNCATE ON delivery_nas_handoff_rounds
 FOR EACH STATEMENT EXECUTE FUNCTION reject_delivery_nas_handoff_mutation();

-- Rejoin the live catalog and the exact public hold on every offer. Alias,
-- path, content or hold drift removes the row from the copy feed.
CREATE VIEW delivery_nas_handoff_copyable AS
SELECT d.output_id,d.etag,d.roster_sha256,
 coalesce((SELECT max(q.round) FROM delivery_nas_handoff_rounds q WHERE q.roster_sha256=d.roster_sha256),0) AS round
FROM delivery_nas_handoff_outputs d
JOIN recording_collation_outputs o ON o.id=d.output_id AND o.object_key=d.object_key
  AND o.nas_relative_path=d.nas_relative_path AND o.sha256=d.sha256 AND o.size_bytes=d.size_bytes
JOIN recording_collation_hours h ON h.id=o.collation_hour_id AND h.recording_id=d.recording_id
  AND h.generation=2 AND h.status='collated'
JOIN recordings r ON r.id=h.recording_id AND r.account_id=47
JOIN delivery_keep_holds k ON k.bucket=d.bucket AND k.object_key=d.object_key
  AND k.kind='video_collated' AND k.etag=d.etag AND k.size_bytes=d.size_bytes
  AND k.sha256 IN ('',d.sha256);

-- A connection-13 readback must hash the final file at its canonical path
-- after promotion. Old ordinary delivery ACKs are not handoff receipts.
CREATE TABLE delivery_nas_handoff_readbacks (
 output_id bigint REFERENCES delivery_nas_handoff_outputs(output_id) ON DELETE RESTRICT,
 round bigint NOT NULL DEFAULT 0 CHECK(round>=0),
 connection_id bigint NOT NULL CHECK (connection_id=13) REFERENCES connections(id) ON DELETE CASCADE,
 size_bytes bigint NOT NULL CHECK(size_bytes>0),
 mtime_ns bigint NOT NULL CHECK(mtime_ns>0),
 ctime_ns bigint NOT NULL CHECK(ctime_ns>0),
 inode bigint NOT NULL CHECK(inode>0),
 device bigint NOT NULL CHECK(device>=0),
 root_inode bigint NOT NULL CHECK(root_inode>0),
 observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(output_id,round)
);
-- Match ordinary delivery ACK lifecycle when the owning NAS is removed.
CREATE FUNCTION reject_delivery_nas_handoff_readback_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' AND pg_trigger_depth()>1 THEN
  RETURN OLD;
 END IF;
 RAISE EXCEPTION 'delivery NAS handoff readback is immutable';
END
$$;
CREATE TRIGGER delivery_nas_handoff_readbacks_immutable
 BEFORE UPDATE OR DELETE ON delivery_nas_handoff_readbacks
 FOR EACH ROW EXECUTE FUNCTION reject_delivery_nas_handoff_readback_mutation();
CREATE TRIGGER delivery_nas_handoff_readbacks_no_truncate
 BEFORE TRUNCATE ON delivery_nas_handoff_readbacks
 FOR EACH STATEMENT EXECUTE FUNCTION reject_delivery_nas_handoff_mutation();
