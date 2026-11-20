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

CREATE TABLE delivery_nas_handoff_mounts (
 roster_sha256 text PRIMARY KEY CHECK(roster_sha256 ~ '^[a-f0-9]{64}$'),
 device bigint NOT NULL CHECK(device>=0),
 inode bigint NOT NULL CHECK(inode>0)
);
CREATE TRIGGER delivery_nas_handoff_mounts_immutable
 BEFORE UPDATE OR DELETE ON delivery_nas_handoff_mounts
 FOR EACH ROW EXECUTE FUNCTION reject_delivery_nas_handoff_mutation();
CREATE TRIGGER delivery_nas_handoff_mounts_no_truncate
 BEFORE TRUNCATE ON delivery_nas_handoff_mounts
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
SELECT d.output_id,d.etag,d.roster_sha256,m.device,m.inode,
 coalesce((SELECT max(q.round) FROM delivery_nas_handoff_rounds q WHERE q.roster_sha256=d.roster_sha256),0) AS round
FROM delivery_nas_handoff_outputs d
JOIN delivery_nas_handoff_mounts m ON m.roster_sha256=d.roster_sha256
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

CREATE TABLE delivery_nas_handoff_errors (
 output_id bigint NOT NULL REFERENCES delivery_nas_handoff_outputs(output_id) ON DELETE RESTRICT,
 round bigint NOT NULL CHECK(round>=0),
 failure_count integer NOT NULL CHECK(failure_count>0),
 last_error text NOT NULL,
 next_attempt_at timestamptz NOT NULL,
 PRIMARY KEY(output_id,round)
);

-- A metadata sweep of exactly the approved paths, separate from the
-- full-volume listing and its deletion evidence.
CREATE TABLE delivery_nas_handoff_checks (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 roster_sha256 text NOT NULL,
 round bigint NOT NULL,
 manifest jsonb NOT NULL CHECK(jsonb_typeof(manifest)='array' AND jsonb_array_length(manifest) BETWEEN 1 AND 20000),
 manifest_sha256 text NOT NULL CHECK(manifest_sha256=encode(sha256(convert_to(manifest::text,'UTF8')),'hex')),
 requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 checked_at timestamptz,
 cancelled_at timestamptz,
 failed_output_id bigint REFERENCES delivery_nas_handoff_outputs(output_id) ON DELETE RESTRICT,
 FOREIGN KEY(roster_sha256,round) REFERENCES delivery_nas_handoff_rounds(roster_sha256,round) ON DELETE RESTRICT,
 CHECK (failed_output_id IS NULL OR checked_at IS NOT NULL),
 CHECK (checked_at IS NULL OR checked_at>=requested_at),
 CHECK (cancelled_at IS NULL OR (cancelled_at>=requested_at AND checked_at IS NULL))
);
CREATE UNIQUE INDEX delivery_nas_handoff_checks_pending
 ON delivery_nas_handoff_checks(roster_sha256,round) WHERE checked_at IS NULL AND cancelled_at IS NULL;
CREATE INDEX delivery_nas_handoff_checks_latest ON delivery_nas_handoff_checks(roster_sha256,round,id DESC);
CREATE FUNCTION guard_delivery_nas_handoff_check() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' OR OLD.checked_at IS NOT NULL OR OLD.cancelled_at IS NOT NULL
    OR (NEW.checked_at IS NULL AND NEW.cancelled_at IS NULL)
    OR ROW(OLD.id,OLD.roster_sha256,OLD.round,OLD.manifest,OLD.manifest_sha256,OLD.requested_at)
       IS DISTINCT FROM ROW(NEW.id,NEW.roster_sha256,NEW.round,NEW.manifest,NEW.manifest_sha256,NEW.requested_at) THEN
  RAISE EXCEPTION 'delivery NAS handoff metadata check is immutable';
 END IF;
 RETURN NEW;
END
$$;
CREATE TRIGGER delivery_nas_handoff_checks_immutable BEFORE UPDATE OR DELETE ON delivery_nas_handoff_checks
 FOR EACH ROW EXECUTE FUNCTION guard_delivery_nas_handoff_check();
CREATE TRIGGER delivery_nas_handoff_checks_no_truncate BEFORE TRUNCATE ON delivery_nas_handoff_checks
 FOR EACH STATEMENT EXECUTE FUNCTION reject_delivery_nas_handoff_mutation();

-- Consume physical readbacks from the latest controller-started verification
-- round. Custody does not depend on keeping a delivery-page hold in place.
CREATE VIEW delivery_nas_handoff_certified AS
SELECT p.output_id,p.recording_id,p.roster_sha256,p.picks_sha256,p.bucket,p.object_key,p.etag,
       p.size_bytes,p.sha256,p.nas_relative_path,d.connection_id,d.round,d.mtime_ns,d.ctime_ns,
       d.inode,d.device,d.root_inode,d.observed_at,check_run.id AS check_id,check_run.checked_at
FROM delivery_nas_handoff_outputs p
JOIN delivery_nas_handoff_mounts m ON m.roster_sha256=p.roster_sha256
JOIN recording_collation_outputs o ON o.id=p.output_id AND o.object_key=p.object_key
 AND o.nas_relative_path=p.nas_relative_path AND o.sha256=p.sha256 AND o.size_bytes=p.size_bytes
JOIN recording_collation_hours h ON h.id=o.collation_hour_id AND h.recording_id=p.recording_id
 AND h.generation=2 AND h.status='collated'
JOIN recordings r ON r.id=h.recording_id AND r.account_id=47
JOIN delivery_nas_handoff_rounds verification ON verification.roster_sha256=p.roster_sha256
 AND NOT EXISTS(SELECT 1 FROM delivery_nas_handoff_rounds newer
  WHERE newer.roster_sha256=p.roster_sha256 AND newer.round>verification.round)
JOIN delivery_nas_handoff_readbacks d ON d.output_id=p.output_id AND d.connection_id=13
 AND d.round=verification.round AND d.observed_at>=verification.started_at AND d.size_bytes=p.size_bytes
 AND d.device=m.device AND d.root_inode=m.inode
JOIN recording_collation_output_acks a ON a.output_id=p.output_id AND a.connection_id=d.connection_id
 AND a.nas_relative_path=p.nas_relative_path AND a.sha256=p.sha256 AND a.size_bytes=p.size_bytes
JOIN delivery_nas_handoff_checks check_run ON check_run.roster_sha256=p.roster_sha256
 AND check_run.round=d.round AND check_run.requested_at>=d.observed_at
 AND check_run.checked_at IS NOT NULL AND check_run.failed_output_id IS NULL
 AND NOT EXISTS(SELECT 1 FROM delivery_nas_handoff_checks newer
  WHERE newer.roster_sha256=check_run.roster_sha256 AND newer.round=check_run.round AND newer.id>check_run.id);
