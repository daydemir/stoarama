-- Owner-approved exemptions from the public delivery page keep.
--
-- delivery_keep_holds is every link on the public delivery page (Deniz,
-- 2026-11-12: keep up only those links). This migration lets exactly the keys
-- the owner later approved for deletion lose that protection, one reviewed
-- class at a time, and only once the class's own condition holds. It seeds
-- nothing and deletes nothing: the controller seeds each class later from a
-- pinned roster (stoaramactl r2-inverse exempt), and deletion still runs through
-- the reviewed collated lane with its own identity checks.
--
-- Deniz, 2026-11-19, verbatim decisions (via the root thread):
--  outside_window: "1. delete the "outside every window" set from r2 …"
--    (4,348 collated delivery videos, 0.582 TB).
--  confident_repair: "Delete them (Recommended)", answering "Delete the 3.38 TB
--    from R2 once the page comes down. Saves ~$50/month and loses no unique
--    footage." (9,232 video_assembly objects under
--    experimental/stitching-confident-repairs/, 3.377 TB).
--  in_window: "File list only": the 2.35 TB of in-window videos are copied to
--    the NAS and certified, a static manifest maps each old link to its NAS
--    path, the page comes down, then the in-window R2 copies go.
-- The keep-everything delivery videos (1.0 TB) stay in R2; the seed command
-- refuses any key whose stream is keep-everything.

CREATE TYPE r2_delivery_exemption_class AS ENUM ('outside_window','confident_repair','in_window');

-- The ONE committed retirement state of the old public delivery page, set once
-- when the page is taken down (handoff step c). It lives here, in PostgreSQL, and
-- the public handlers read it live on every request: the page, its metadata, its
-- download tickets and its file responses all refuse while this row exists, for
-- the current and for every pinned generation. Retirement is therefore a serving
-- fact the server enforces, never a claim recorded from CLI arguments. The row
-- names the generation that was serving when it was written, for the record.
--
-- This row is the DESTRUCTIVE PREDICATE. It cannot exist until the proof is
-- committed, and the proof is enforced in SQL by guard_r2_delivery_page_retirement
-- below, not by the CLI: a quiesce row, zero unexpired public reader leases, and
-- zero download capabilities that could still be transferring. r2_delivery_page_retired()
-- reads only this row, so no exemption class opens a moment early.
CREATE TABLE r2_delivery_page_retirement (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 generation text NOT NULL CHECK(generation ~ '^[a-f0-9]{32}$'),
 decision text NOT NULL CHECK(decision<>''),
 retired_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- The staging state that arms retirement. Committing this row is the point of
-- no return: from then on every public delivery route refuses a NEW request
-- (503, reload later), because r2_delivery_public_serving() is false, while the
-- readers admitted before it finish their response. It is deliberately a
-- separate, non-destructive fact: the page is not retired yet, no exemption
-- class is open, and the rollback window is documented as closed in
-- docs/r2-inverse.md. The retirement trigger refuses to run without it.
CREATE TABLE r2_delivery_page_quiesce (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 reason text NOT NULL CHECK(reason<>''),
 armed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
-- One admitted public response, from before its first byte to after its last
-- socket write. A response records itself here before it reads a registry, a
-- metadata asset, a ticket or a file, and deletes the row after its body is
-- written. The row is a LEASE with a hard expiry, so a crashed or abandoned
-- response delays retirement by at most that lease and never wedges it. The
-- retirement trigger refuses while any lease is unexpired, so retirement cannot
-- commit in the middle of a response; the API never holds a database connection
-- or a lock while streaming, only this row.
CREATE TABLE r2_delivery_public_readers (
 reader_id text PRIMARY KEY CHECK(reader_id ~ '^[a-f0-9]{32}$'),
 admitted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 expires_at timestamptz NOT NULL,
 CHECK(expires_at > admitted_at)
);
CREATE INDEX r2_delivery_public_readers_live ON r2_delivery_public_readers(expires_at);

-- Every download capability the public page hands out, so retirement can drain
-- them instead of leaving presigned URLs live. A row is written in the same
-- transaction, under the same lock, as the capability it describes, so a
-- capability can never outlive its record. expires_at is the provider's own
-- capability expiry; last_byte_by adds the maximum in-flight transfer bound,
-- because a provider does not cut a transfer that already started when the
-- capability expires. Retirement refuses until last_byte_by has passed, measured
-- on the database clock, so it can never retire while a transfer may still be
-- reading bytes.
CREATE TABLE r2_delivery_download_tickets (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 bucket text NOT NULL,
 object_key text NOT NULL,
 etag text NOT NULL CHECK(etag<>''),
 size_bytes bigint NOT NULL CHECK(size_bytes>0),
 object_path_sha256 text NOT NULL CHECK(object_path_sha256 ~ '^[a-f0-9]{64}$'),
 issued_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 expires_at timestamptz NOT NULL,
 last_byte_by timestamptz NOT NULL,
 CHECK(last_byte_by >= expires_at)
);
CREATE INDEX r2_delivery_download_tickets_live ON r2_delivery_download_tickets(expires_at);
CREATE INDEX r2_delivery_download_tickets_transferring ON r2_delivery_download_tickets(last_byte_by);

-- The owner approval each roster was approved for: one append-only row per
-- ROSTER DIGEST, recorded by its own command step before any seeding. The digest
-- is unique across classes, so an approved roster can never be replayed under a
-- second class. Every exemption row must name a matching approval.
CREATE TABLE r2_delivery_exemption_approvals (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 class r2_delivery_exemption_class NOT NULL,
 roster_sha256 text NOT NULL CHECK(roster_sha256 ~ '^[a-f0-9]{64}$'),
 decision text NOT NULL CHECK(decision<>''),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(roster_sha256)
);

-- One exemption per exact page key. etag and size_bytes must equal the live
-- delivery_keep_holds row, both at admission and every time the exemption is
-- consulted; any drift keeps the key held. approval_id binds the row to the one
-- owner approval for its class and roster digest.
CREATE TABLE r2_delivery_exemptions (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 bucket text NOT NULL,
 object_key text NOT NULL,
 etag text NOT NULL CHECK(etag<>''),
 size_bytes bigint NOT NULL CHECK(size_bytes>0),
 class r2_delivery_exemption_class NOT NULL,
 approval_id bigint NOT NULL REFERENCES r2_delivery_exemption_approvals(id),
 decision text NOT NULL CHECK(decision<>''),
 roster_sha256 text NOT NULL CHECK(roster_sha256 ~ '^[a-f0-9]{64}$'),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(bucket,object_key)
);

CREATE FUNCTION guard_r2_delivery_exemption_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 RAISE EXCEPTION 'R2 delivery exemptions, approvals, the quiesce state and the page retirement state are append-only';
END $$;
DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['r2_delivery_exemptions','r2_delivery_exemption_approvals','r2_delivery_page_quiesce','r2_delivery_page_retirement'] LOOP
  EXECUTE format('CREATE TRIGGER %s_immutable BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION guard_r2_delivery_exemption_immutable()',t,t);
  EXECUTE format('CREATE TRIGGER %s_no_truncate BEFORE TRUNCATE ON %I FOR EACH STATEMENT EXECUTE FUNCTION guard_r2_delivery_exemption_immutable()',t,t);
  EXECUTE format('ALTER TABLE %I ENABLE ALWAYS TRIGGER %s_immutable',t,t);
  EXECUTE format('ALTER TABLE %I ENABLE ALWAYS TRIGGER %s_no_truncate',t,t);
 END LOOP;
END $$;

-- Admission reads the live page row, so it must see what committed before it.
-- Each class names the exact page kind and key family the owner decided on, and
-- every row must reference the owner approval for THAT class and roster digest:
-- the referenced approval's class, digest and decision must equal the row's, in
-- SQL, so no writer can bind a row to an approval for another class.
CREATE FUNCTION guard_r2_delivery_exemption_admission() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE h delivery_keep_holds;
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION 'R2 delivery exemption admission requires READ COMMITTED' USING ERRCODE='55000';
 END IF;
 SELECT * INTO h FROM delivery_keep_holds WHERE bucket=NEW.bucket AND object_key=NEW.object_key FOR SHARE;
 IF NOT FOUND OR h.etag<>NEW.etag OR h.size_bytes<>NEW.size_bytes THEN
  RAISE EXCEPTION 'R2 delivery exemption needs the exact live page identity of %', NEW.object_key;
 END IF;
 IF NOT EXISTS(SELECT 1 FROM r2_delivery_exemption_approvals a WHERE a.id=NEW.approval_id
    AND a.class=NEW.class AND a.roster_sha256=NEW.roster_sha256 AND a.decision=NEW.decision) THEN
  RAISE EXCEPTION 'R2 delivery exemption % must name its own class/roster approval', NEW.object_key;
 END IF;
 IF NOT ((NEW.class='confident_repair' AND h.kind='video_assembly'
    AND starts_with(NEW.object_key,'experimental/stitching-confident-repairs/'))
  OR (NEW.class IN ('outside_window','in_window') AND h.kind='video_collated'
    AND (starts_with(NEW.object_key,'joined/goodplus-') OR starts_with(NEW.object_key,'joined/nightly-collation-v2/')))) THEN
  RAISE EXCEPTION 'R2 delivery exemption class % does not cover % (%)', NEW.class, NEW.object_key, h.kind;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER r2_delivery_exemption_admission BEFORE INSERT ON r2_delivery_exemptions
 FOR EACH ROW EXECUTE FUNCTION guard_r2_delivery_exemption_admission();
ALTER TABLE r2_delivery_exemptions ENABLE ALWAYS TRIGGER r2_delivery_exemption_admission;

-- One roster digest, one class, enforced in SQL and not only by a unique index:
-- a second approval for the same digest under another class is refused outright,
-- and the same digest under the same class may not be re-recorded either.
CREATE FUNCTION guard_r2_delivery_approval_class() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE existing r2_delivery_exemption_approvals;
BEGIN
 SELECT * INTO existing FROM r2_delivery_exemption_approvals WHERE roster_sha256=NEW.roster_sha256;
 IF FOUND AND (existing.class<>NEW.class OR existing.decision<>NEW.decision) THEN
  RAISE EXCEPTION 'roster digest % is already approved for class % only; it can never be replayed under %',
   NEW.roster_sha256, existing.class, NEW.class;
 END IF;
 IF FOUND THEN
  RAISE EXCEPTION 'roster digest % is already approved', NEW.roster_sha256;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER r2_delivery_approval_class BEFORE INSERT ON r2_delivery_exemption_approvals
 FOR EACH ROW EXECUTE FUNCTION guard_r2_delivery_approval_class();
ALTER TABLE r2_delivery_exemption_approvals ENABLE ALWAYS TRIGGER r2_delivery_approval_class;

-- The in_window gate's NAS custody hook. The stitching thread's NAS handoff
-- (#574, 0224) certifies each in-window video on the NAS in
-- delivery_nas_handoff_certified. Its follow-up replaces only this body with an
-- exact match there on output_id, bucket, object_key, etag, size_bytes and
-- sha256 with connection_id=13. Until then nothing is certified and the
-- in_window class stays held.
CREATE FUNCTION r2_delivery_nas_certified(p_output_id bigint, p_bucket text, p_key text, p_etag text, p_size bigint, p_sha256 text)
RETURNS boolean LANGUAGE sql STABLE AS $$ SELECT false $$;

-- True only when the public delivery page is retired, checked live on every
-- call. It reads EXACTLY the one committed serving state above, the same row the
-- public handlers refuse on, so the gate and the page can never disagree: no
-- second fact, no CLI-asserted state.
CREATE FUNCTION r2_delivery_page_retired() RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM r2_delivery_page_retirement)
$$;

-- True while the page may serve a NEW request. It is false from the moment the
-- staging quiesce row commits and stays false once the page is retired. The API
-- reads it live before admitting a reader, so neither the quiesce state nor the
-- retired state can be bypassed by a warm cache, a pinned generation or a
-- direct storage read. It is also the SQL-side arm gate: the retirement trigger
-- requires this to be false, so retirement is impossible without a committed
-- quiesce.
CREATE FUNCTION r2_delivery_public_serving() RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT NOT EXISTS(SELECT 1 FROM r2_delivery_page_quiesce)
    AND NOT EXISTS(SELECT 1 FROM r2_delivery_page_retirement)
$$;

-- The exclusive side of the shared/exclusive public admission this migration
-- implements. Every admitted public response holds ONE short shared transaction
-- (pg_advisory_xact_lock_shared) for the registry/metadata/ticket decision, and
-- releases it before streaming; its r2_delivery_public_readers lease row is what
-- spans the byte transfer. Retirement takes this same lock exclusively, so it
-- serializes against registry publication, ticket minting and admission without
-- ever waiting on a socket. A plain SELECT keeps it in one place, so the server
-- and the CLI cannot disagree about the key.
CREATE FUNCTION r2_delivery_public_lock() RETURNS bigint LANGUAGE sql IMMUTABLE AS $$ SELECT 7343226011104::bigint $$;

-- A public reader lease is admitted only while the page may serve, in the same
-- statement that writes it. READ COMMITTED is required so an earlier snapshot
-- cannot admit a reader after a quiesce committed.
CREATE FUNCTION guard_r2_delivery_public_reader() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION 'a public delivery reader lease requires READ COMMITTED' USING ERRCODE='55000';
 END IF;
 PERFORM pg_advisory_xact_lock_shared(r2_delivery_public_lock());
 IF NOT r2_delivery_public_serving() THEN
  RAISE EXCEPTION 'the public delivery page is not serving'
   USING ERRCODE='55000', HINT='retire/quiesce is in progress; no new public reader is admitted';
 END IF;
 IF NEW.expires_at<=clock_timestamp() OR NEW.expires_at>clock_timestamp()+interval '10 minutes' THEN
  RAISE EXCEPTION 'public reader lease must expire within ten minutes';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER r2_delivery_public_reader_admission BEFORE INSERT ON r2_delivery_public_readers
 FOR EACH ROW EXECUTE FUNCTION guard_r2_delivery_public_reader();
ALTER TABLE r2_delivery_public_readers ENABLE ALWAYS TRIGGER r2_delivery_public_reader_admission;

-- A ticket row is written in the same transaction as its capability, while the
-- page serves, under the shared public lock: retirement's exclusive lock then
-- cannot commit between the page check and the row, and no ticket can be minted
-- after retirement armed. last_byte_by is at least the capability expiry.
CREATE FUNCTION guard_r2_delivery_download_ticket() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION 'a public delivery ticket requires READ COMMITTED' USING ERRCODE='55000';
 END IF;
 PERFORM pg_advisory_xact_lock_shared(r2_delivery_public_lock());
 IF NOT r2_delivery_public_serving() THEN
  RAISE EXCEPTION 'the public delivery page is not serving'
   USING ERRCODE='55000', HINT='no download capability may be minted after quiesce or retirement';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER r2_delivery_download_ticket_admission BEFORE INSERT ON r2_delivery_download_tickets
 FOR EACH ROW EXECUTE FUNCTION guard_r2_delivery_download_ticket();
ALTER TABLE r2_delivery_download_tickets ENABLE ALWAYS TRIGGER r2_delivery_download_ticket_admission;

-- Retirement is the destructive predicate and it is proved HERE, in SQL, under
-- the exclusive public lock, never by a caller's claim:
--   1. the page must be quiesced (the staging fact committed first);
--   2. no admitted public reader lease may be unexpired (every response that was
--      streaming has finished, judged on the database clock);
--   3. no download capability may still be transferring (its last_byte_by has
--      passed), so a presigned URL cannot outlive the page.
-- The INSERT itself is refused otherwise, so a stale, older or hand-run binary
-- cannot retire the page early. Retiring twice is refused by the singleton key.
CREATE FUNCTION guard_r2_delivery_page_retirement() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE live_readers bigint; live_capabilities bigint; armed boolean;
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION 'page retirement requires READ COMMITTED' USING ERRCODE='55000';
 END IF;
 PERFORM pg_advisory_xact_lock(r2_delivery_public_lock());
 SELECT EXISTS(SELECT 1 FROM r2_delivery_page_quiesce) INTO armed;
 IF NOT armed THEN
  RAISE EXCEPTION 'page retirement requires the committed quiesce state'
   USING ERRCODE='55000', HINT='run r2-inverse page-quiesce first and let admitted readers drain';
 END IF;
 SELECT count(*) INTO live_readers FROM r2_delivery_public_readers WHERE expires_at>clock_timestamp();
 IF live_readers<>0 THEN
  RAISE EXCEPTION 'refusing page retirement: % admitted public readers are still unexpired', live_readers;
 END IF;
 SELECT count(*) INTO live_capabilities FROM r2_delivery_download_tickets WHERE last_byte_by>clock_timestamp();
 IF live_capabilities<>0 THEN
  RAISE EXCEPTION 'refusing page retirement: % download capabilities may still be transferring', live_capabilities;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER r2_delivery_page_retirement_proof BEFORE INSERT ON r2_delivery_page_retirement
 FOR EACH ROW EXECUTE FUNCTION guard_r2_delivery_page_retirement();
ALTER TABLE r2_delivery_page_retirement ENABLE ALWAYS TRIGGER r2_delivery_page_retirement_proof;

-- True only for an exempt exact page key whose class gate is open now. A class
-- without a gate branch yields NULL and stays held.
CREATE FUNCTION r2_delivery_exempt(p_bucket text, p_key text) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM r2_delivery_exemptions x
  JOIN delivery_keep_holds h ON h.bucket=x.bucket AND h.object_key=x.object_key
  WHERE x.bucket=p_bucket AND x.object_key=p_key AND h.etag=x.etag AND h.size_bytes=x.size_bytes
  AND CASE x.class
   WHEN 'outside_window' THEN true
   WHEN 'confident_repair' THEN r2_delivery_page_retired()
   WHEN 'in_window' THEN r2_delivery_page_retired()
    AND EXISTS(SELECT 1 FROM recording_collation_outputs o WHERE o.object_key=x.object_key
     AND o.size_bytes=x.size_bytes AND lower(o.sha256)=h.sha256
     AND r2_delivery_nas_certified(o.id,x.bucket,x.object_key,x.etag,x.size_bytes,h.sha256))
  END)
$$;

-- 0190's guard on every reservation and receipt, for every lane and binary: an
-- exact page key is refused unless it is exempt now and the row names the
-- page's exact size and content. Exemptions cover only these exact delivery
-- objects; the derived RAW delivery guard (r2_inverse_delivery_held) is
-- unchanged and holds every RAW source in every mode.
CREATE OR REPLACE FUNCTION guard_delivery_r2_retirement() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF EXISTS(SELECT 1 FROM delivery_keep_holds h WHERE h.bucket=NEW.bucket AND h.object_key=NEW.object_key
   AND NOT (h.size_bytes=NEW.size_bytes AND h.sha256=NEW.sha256 AND r2_delivery_exempt(h.bucket,h.object_key))) THEN
  RAISE EXCEPTION 'DELIVERY KEEP HOLD: R2 retirement forbidden';
 END IF;
 RETURN NEW;
END $$;

-- Confident repairs have no catalog row. They retire under their own kind,
-- keyed by the exemption id, so no repair can be mistaken for a collated
-- output or a RAW clip.
ALTER TABLE r2_nonraw_intents DROP CONSTRAINT r2_nonraw_intents_kind_check;
ALTER TABLE r2_nonraw_intents DROP CONSTRAINT r2_nonraw_intents_source_ids_check;
ALTER TABLE r2_nonraw_intents ADD CONSTRAINT r2_nonraw_intents_kind_check
 CHECK ((kind='collated' AND cardinality(source_ids)>0) OR (kind='delivery_repair' AND cardinality(source_ids)=0));
ALTER TABLE r2_retirement_reservations DROP CONSTRAINT r2_retirement_reservations_kind_check;
ALTER TABLE r2_retirement_reservations ADD CONSTRAINT r2_retirement_reservations_kind_check
 CHECK (kind IN ('raw','collated','delivery_repair'));
ALTER TABLE r2_object_retirements DROP CONSTRAINT r2_object_retirements_kind_check;
ALTER TABLE r2_object_retirements DROP CONSTRAINT r2_object_retirements_collated_prefix_check;
ALTER TABLE r2_object_retirements ADD CONSTRAINT r2_object_retirements_kind_prefix_check
 CHECK ((kind = 'raw' AND object_key LIKE 'managed/%')
     OR (kind = 'collated' AND (object_key LIKE 'joined/goodplus-%'
         OR object_key LIKE 'joined/nightly-collation-v2/%'
         OR object_key LIKE 'joined/best14-20261021/%'))
     OR (kind = 'delivery_repair' AND object_key LIKE 'experimental/stitching-confident-repairs/%'));

-- A pending collated intent freezes its own output only; a repair intent's
-- source_id is an exemption id, not an output id.
CREATE OR REPLACE FUNCTION guard_r2_nonraw_output_identity() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF EXISTS(SELECT 1 FROM r2_nonraw_intents i WHERE i.kind='collated' AND i.source_id=OLD.id AND NOT EXISTS(SELECT 1 FROM r2_nonraw_events e WHERE e.bucket=i.bucket AND e.object_key=i.object_key AND e.event='absent'))
 OR (TG_OP='UPDATE' AND EXISTS(SELECT 1 FROM r2_nonraw_intents i WHERE i.bucket='stoarama' AND i.object_key=NEW.object_key AND NOT EXISTS(SELECT 1 FROM r2_nonraw_events e WHERE e.bucket=i.bucket AND e.object_key=i.object_key AND e.event='absent'))) THEN
  RAISE EXCEPTION 'R2 nonraw intent freezes collated output identity'; END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW;
END $$;

-- The key-level live veto every nonraw kind shares: an incident, a page key
-- that is not exempt now, a receipt, an unverified or blocked COPY, or another
-- retirement of the same key.
CREATE FUNCTION r2_nonraw_key_held(p_bucket text, p_key text, p_kind text, p_id bigint) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM nas_cleanup_incident_roster i WHERE i.bucket=p_bucket AND i.object_key=p_key)
  OR EXISTS(SELECT 1 FROM delivery_keep_holds k WHERE k.bucket=p_bucket AND k.object_key=p_key AND NOT r2_delivery_exempt(p_bucket,p_key))
  OR EXISTS(SELECT 1 FROM r2_object_retirements x WHERE x.bucket=p_bucket AND x.object_key=p_key)
  OR EXISTS(SELECT 1 FROM nas_r2_copy_items ci WHERE (ci.state<>'verified' OR ci.blocked_reason<>'')
    AND ci.item->>'bucket'=p_bucket AND ci.item->>'object_key'=p_key)
  OR EXISTS(SELECT 1 FROM r2_nonraw_intents ni WHERE ni.bucket=p_bucket AND ni.object_key=p_key AND NOT (ni.kind=p_kind AND ni.source_id=p_id))
  OR EXISTS(SELECT 1 FROM r2_inverse_intents ri WHERE ri.bucket=p_bucket AND ri.object_key=p_key)
  OR EXISTS(SELECT 1 FROM r2_keep28_intents ki WHERE ki.bucket=p_bucket AND ki.object_key=p_key)
  OR EXISTS(SELECT 1 FROM r2_retirement_reservations rv WHERE rv.bucket=p_bucket AND rv.object_key=p_key AND NOT (rv.kind=p_kind AND rv.source_id=p_id))
  OR EXISTS(SELECT 1 FROM nas_stitch_r2_input_holds sh WHERE sh.bucket=p_bucket AND sh.object_key=p_key AND sh.released_at IS NULL)
  OR r2_owner_keep_held(p_bucket,p_key)
$$;

-- The same veto without its page-key term, for the seed's own decision about a
-- key it is about to exempt. Every OTHER hold is unchanged, so the seed can never
-- admit a key the execution-time classifier would refuse for a non-page reason.
CREATE FUNCTION r2_nonraw_key_held_except_page(p_bucket text, p_key text, p_kind text, p_id bigint) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM nas_cleanup_incident_roster i WHERE i.bucket=p_bucket AND i.object_key=p_key)
  OR EXISTS(SELECT 1 FROM r2_object_retirements x WHERE x.bucket=p_bucket AND x.object_key=p_key)
  OR EXISTS(SELECT 1 FROM nas_r2_copy_items ci WHERE (ci.state<>'verified' OR ci.blocked_reason<>'')
    AND ci.item->>'bucket'=p_bucket AND ci.item->>'object_key'=p_key)
  OR EXISTS(SELECT 1 FROM r2_nonraw_intents ni WHERE ni.bucket=p_bucket AND ni.object_key=p_key AND NOT (ni.kind=p_kind AND ni.source_id=p_id))
  OR EXISTS(SELECT 1 FROM r2_inverse_intents ri WHERE ri.bucket=p_bucket AND ri.object_key=p_key)
  OR EXISTS(SELECT 1 FROM r2_keep28_intents ki WHERE ki.bucket=p_bucket AND ki.object_key=p_key)
  OR EXISTS(SELECT 1 FROM r2_retirement_reservations rv WHERE rv.bucket=p_bucket AND rv.object_key=p_key AND NOT (rv.kind=p_kind AND rv.source_id=p_id))
  OR EXISTS(SELECT 1 FROM nas_stitch_r2_input_holds sh WHERE sh.bucket=p_bucket AND sh.object_key=p_key AND sh.released_at IS NULL)
  OR r2_owner_keep_held(p_bucket,p_key)
$$;

-- 0211's nonraw ledger predates the owner keep (0220) and stitch input (0217)
-- guards on every other retirement ledger; give it the same ones.
DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['r2_nonraw_intents','r2_nonraw_attempts'] LOOP
  EXECUTE format('CREATE TRIGGER r2_owner_keep_hold BEFORE INSERT OR UPDATE ON %I FOR EACH ROW EXECUTE FUNCTION guard_r2_owner_keep_retirement()',t);
  EXECUTE format('ALTER TABLE %I ENABLE ALWAYS TRIGGER r2_owner_keep_hold',t);
  EXECUTE format('CREATE TRIGGER r2_stitch_input_hold_guard BEFORE INSERT ON %I FOR EACH ROW EXECUTE FUNCTION guard_r2_retirement_stitch_input()',t);
 END LOOP;
END $$;

-- And a new owner keep hold refuses to land over a pending nonraw intent, as it
-- already does over the RAW, keep-28 and stitch-staging ones.
CREATE OR REPLACE FUNCTION guard_r2_owner_keep_admission() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
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
    AND NOT EXISTS(SELECT 1 FROM r2_keep28_events v WHERE v.bucket=i.bucket AND v.object_key=i.object_key AND v.event='absent'))
  OR EXISTS(SELECT 1 FROM r2_nonraw_intents i WHERE i.bucket=NEW.bucket AND starts_with(i.object_key,NEW.prefix)
    AND NOT EXISTS(SELECT 1 FROM r2_nonraw_events v WHERE v.bucket=i.bucket AND v.object_key=i.object_key AND v.event='absent'))
  OR EXISTS(SELECT 1 FROM nas_stitch_r2_stage_retirements r JOIN nas_stitch_r2_stage_objects o ON o.id=r.object_id
    WHERE r.absence_observed_at IS NULL AND o.bucket=NEW.bucket AND starts_with(o.object_key,NEW.prefix)) THEN
  RAISE EXCEPTION 'R2 owner keep hold % overlaps a pending R2 retirement', NEW.prefix;
 END IF;
 RETURN NEW;
END $$;
