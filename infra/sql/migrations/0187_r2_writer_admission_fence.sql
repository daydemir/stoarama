-- Additive and opt-in. Deploy every DB-driven writer with the wire admission
-- gate before activating. No production fence is installed by migration.
CREATE TABLE r2_retirement_writer_fences (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 bucket text NOT NULL CHECK(btrim(bucket)<>''),
 endpoint_sha256 text NOT NULL CHECK(endpoint_sha256 ~ '^[a-f0-9]{64}$'),
 activated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 -- Old capability issuers may not have logged expiry. Wait the provider maximum
 -- plus bounded request time, regardless of job/intent consumption status.
 drain_not_before timestamptz NOT NULL DEFAULT clock_timestamp()+interval '7 days 2 minutes'
);
CREATE TABLE r2_retirement_writer_capabilities (
 id bigserial PRIMARY KEY, bucket text NOT NULL, expires_at timestamptz NOT NULL
);
CREATE INDEX r2_retirement_writer_capabilities_expiry ON r2_retirement_writer_capabilities(bucket,expires_at);

CREATE FUNCTION r2_retirement_admit_writer() RETURNS void LANGUAGE plpgsql AS $$ BEGIN
 PERFORM pg_advisory_xact_lock_shared(7343226011187);
 IF EXISTS(SELECT 1 FROM r2_retirement_writer_fences) THEN
  RAISE EXCEPTION 'R2 retirement writer admission is fenced';
 END IF;
END $$;
CREATE FUNCTION r2_retirement_guard_fence() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 -- Never queue an exclusive lock behind a writer that might itself need a
 -- second shared admission connection. Refuse activation; finish drain first.
 IF NOT pg_try_advisory_xact_lock(7343226011187) THEN
  RAISE EXCEPTION 'R2 wire writes or admissions still in flight';
 END IF;
 IF NOT pg_try_advisory_xact_lock(7343226011173) THEN
  RAISE EXCEPTION 'R2 retirement apply is active';
 END IF;
 IF TG_OP='UPDATE' THEN RAISE EXCEPTION 'R2 writer fence is immutable'; END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 NEW.activated_at:=clock_timestamp();
 NEW.drain_not_before:=NEW.activated_at+interval '7 days 2 minutes';
 RETURN NEW;
END $$;
CREATE TRIGGER r2_writer_fence_transition BEFORE INSERT OR UPDATE OR DELETE ON r2_retirement_writer_fences
 FOR EACH ROW EXECUTE FUNCTION r2_retirement_guard_fence();

-- SQL admissions protect even old callers. Job completion/cancellation may drain
-- after activation, but INSERT or any transition/heartbeat into a runnable state
-- is rejected. Catalog registrations are insert-fenced separately from receipts.
CREATE FUNCTION r2_retirement_guard_admission() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF TG_NARGS=0 OR (to_jsonb(NEW)->>TG_ARGV[0])=ANY(string_to_array(TG_ARGV[1],',')) THEN
  PERFORM r2_retirement_admit_writer();
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER r2_writer_recordings BEFORE INSERT OR UPDATE OF status ON recordings FOR EACH ROW EXECUTE FUNCTION r2_retirement_guard_admission('status','active');
CREATE TRIGGER r2_writer_streams BEFORE INSERT OR UPDATE OF recording_state ON streams FOR EACH ROW EXECUTE FUNCTION r2_retirement_guard_admission('recording_state','on');
CREATE TRIGGER r2_writer_capture_runtime BEFORE INSERT OR UPDATE ON stream_capture_runtime FOR EACH ROW EXECUTE FUNCTION r2_retirement_guard_admission('status','resolving,running');
CREATE TRIGGER r2_writer_recording_jobs BEFORE INSERT OR UPDATE ON recording_jobs FOR EACH ROW EXECUTE FUNCTION r2_retirement_guard_admission('status','pending,leased');
CREATE TRIGGER r2_writer_transfer BEFORE INSERT OR UPDATE ON clip_transfer_jobs FOR EACH ROW EXECUTE FUNCTION r2_retirement_guard_admission('status','pending,leased');
CREATE TRIGGER r2_writer_inference BEFORE INSERT OR UPDATE ON inference_box_jobs FOR EACH ROW EXECUTE FUNCTION r2_retirement_guard_admission('status','pending,leased');
CREATE TRIGGER r2_writer_restore BEFORE INSERT OR UPDATE ON nas_restore_requests FOR EACH ROW EXECUTE FUNCTION r2_retirement_guard_admission('state','pending,leased');
CREATE TRIGGER r2_writer_stitch BEFORE INSERT OR UPDATE ON nas_stitch_tasks FOR EACH ROW EXECUTE FUNCTION r2_retirement_guard_admission('state','pending,leased');
CREATE TRIGGER r2_writer_native_stitch BEFORE INSERT OR UPDATE ON recording_native_stitch_tasks FOR EACH ROW EXECUTE FUNCTION r2_retirement_guard_admission('state','pending,leased');
CREATE TRIGGER r2_writer_probe BEFORE INSERT OR UPDATE ON recording_presentation_v2_probe_tasks FOR EACH ROW EXECUTE FUNCTION r2_retirement_guard_admission('state','awaiting_retention,pending,leased');
CREATE TRIGGER r2_writer_joined_batch BEFORE INSERT OR UPDATE ON recording_joined_batches FOR EACH ROW EXECUTE FUNCTION r2_retirement_guard_admission('state','snapshotting,building,frozen,index_sealed');
CREATE TRIGGER r2_writer_joined_hour BEFORE INSERT OR UPDATE ON recording_joined_hours FOR EACH ROW EXECUTE FUNCTION r2_retirement_guard_admission('state','pending,leased');
CREATE TRIGGER r2_writer_joined_day BEFORE INSERT OR UPDATE ON recording_joined_stream_days FOR EACH ROW EXECUTE FUNCTION r2_retirement_guard_admission('state','pending');
CREATE TRIGGER r2_writer_joined_publish BEFORE INSERT OR UPDATE ON recording_joined_artifacts FOR EACH ROW EXECUTE FUNCTION r2_retirement_guard_admission('publication_state','sealed,publishing');
DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['recording_clips','recording_collation_hours','recording_collation_outputs',
 'nas_stitch_outputs','recording_native_stitch_certifications','media_objects','frames',
 'r2_retirement_writer_capabilities'] LOOP
  EXECUTE format('CREATE TRIGGER r2_writer_insert BEFORE INSERT ON %I FOR EACH ROW EXECUTE FUNCTION r2_retirement_guard_admission()',t);
 END LOOP;
 FOREACH t IN ARRAY ARRAY['capture_session_leases','recording_upload_intents','upload_intents'] LOOP
  EXECUTE format('CREATE TRIGGER r2_writer_capability BEFORE INSERT OR UPDATE ON %I FOR EACH ROW EXECUTE FUNCTION r2_retirement_guard_admission()',t);
 END LOOP;
END $$;

CREATE FUNCTION r2_retirement_writers_drained(b text) RETURNS boolean LANGUAGE sql AS $$
 SELECT NOT (
 EXISTS(SELECT 1 FROM recordings WHERE status='active')
 OR EXISTS(SELECT 1 FROM recording_jobs WHERE status IN ('pending','leased'))
 OR EXISTS(SELECT 1 FROM streams WHERE recording_state='on')
 OR EXISTS(SELECT 1 FROM stream_capture_runtime WHERE status IN ('resolving','running'))
 OR EXISTS(SELECT 1 FROM capture_session_leases WHERE lease_expires_at>clock_timestamp())
 OR EXISTS(SELECT 1 FROM recording_upload_intents WHERE bucket=b AND expires_at>clock_timestamp())
 OR EXISTS(SELECT 1 FROM upload_intents WHERE bucket=b AND expires_at>clock_timestamp())
 OR EXISTS(SELECT 1 FROM r2_retirement_writer_capabilities WHERE bucket=b AND expires_at>clock_timestamp())
 OR EXISTS(SELECT 1 FROM clip_transfer_jobs WHERE status IN ('pending','leased'))
 OR EXISTS(SELECT 1 FROM inference_box_jobs WHERE status IN ('pending','leased'))
 OR EXISTS(SELECT 1 FROM nas_restore_requests WHERE state IN ('pending','leased') OR put_expires_at>clock_timestamp())
 OR EXISTS(SELECT 1 FROM nas_stitch_tasks WHERE state IN ('pending','leased'))
 OR EXISTS(SELECT 1 FROM recording_native_stitch_tasks WHERE state IN ('pending','leased'))
 OR EXISTS(SELECT 1 FROM recording_presentation_v2_probe_tasks WHERE state IN ('awaiting_retention','pending','leased'))
 OR EXISTS(SELECT 1 FROM recording_joined_batches WHERE state IN ('snapshotting','building','frozen','index_sealed'))
 OR EXISTS(SELECT 1 FROM recording_joined_hours WHERE state IN ('pending','leased'))
 OR EXISTS(SELECT 1 FROM recording_joined_stream_days WHERE state='pending')
 OR EXISTS(SELECT 1 FROM recording_joined_artifacts WHERE publication_state IN ('sealed','publishing'))
 );
$$;
