-- Durable, append-only proof that exact NAS bytes for one completed continuous
-- window decode and can be losslessly concatenated within each contiguous
-- native-media run. Timeline quality and current NAS presence remain separate
-- claims; neither is implied by a media certification row.
CREATE TABLE recording_native_stitch_tasks (
  id BIGSERIAL PRIMARY KEY,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  -- Logical ids deliberately have no FK: immutable audit facts must neither be
  -- erased by nor block ordinary recording/job pruning.
  recording_id BIGINT NOT NULL CHECK(recording_id>0),
  recording_job_id BIGINT NOT NULL CHECK(recording_job_id>0),
  window_start_at TIMESTAMPTZ NOT NULL,
  window_end_at TIMESTAMPTZ NOT NULL,
  health_calculated_at TIMESTAMPTZ NOT NULL,
  health_metric_version INTEGER NOT NULL CHECK(health_metric_version>=2),
  health_facts JSONB NOT NULL CHECK(jsonb_typeof(health_facts)='object'),
  job_schedule_facts JSONB NOT NULL CHECK(jsonb_typeof(job_schedule_facts)='object'),
  qualification_scope TEXT NOT NULL DEFAULT 'byte_run_audit'
    CHECK(qualification_scope IN('byte_run_audit','authoritative_occurrence')),
  clip_manifest JSONB NOT NULL CHECK(jsonb_typeof(clip_manifest)='array'),
  clip_manifest_sha256 TEXT NOT NULL CHECK(clip_manifest_sha256~'^[0-9a-f]{64}$'),
  clip_count INTEGER NOT NULL CHECK(clip_count BETWEEN 1 AND 1024),
  source_bytes BIGINT NOT NULL CHECK(source_bytes BETWEEN 1 AND 68719476736),
  policy_version TEXT NOT NULL CHECK(btrim(policy_version)<>''),
  priority INTEGER NOT NULL DEFAULT 100 CHECK(priority BETWEEN 1 AND 10000),
  state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN('pending','leased','passed','partial','failed','stale')),
  attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count BETWEEN 0 AND 100),
  claim_token UUID,
  claimed_connection_id BIGINT,
  lease_expires_at TIMESTAMPTZ,
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_reason_code TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK(window_end_at>window_start_at),
  CHECK((state='leased' AND claim_token IS NOT NULL AND claimed_connection_id IS NOT NULL AND lease_expires_at IS NOT NULL)
     OR (state<>'leased' AND claim_token IS NULL AND claimed_connection_id IS NULL AND lease_expires_at IS NULL)),
  UNIQUE(account_id,recording_job_id,policy_version)
);
CREATE INDEX recording_native_stitch_tasks_claim_idx
  ON recording_native_stitch_tasks(account_id,priority,next_attempt_at,id)
  WHERE state IN('pending','leased');

-- Relational task clips are the bounded scheduling/index surface. The JSON
-- manifest remains the immutable canonical audit blob, never a claim-time
-- jsonb_to_recordset scan across every pending task.
CREATE TABLE recording_native_stitch_task_clips (
  task_id BIGINT NOT NULL REFERENCES recording_native_stitch_tasks(id) ON DELETE RESTRICT,
  ordinal INTEGER NOT NULL CHECK(ordinal>0),
  clip_id BIGINT NOT NULL CHECK(clip_id>0),
  relative_path TEXT NOT NULL CHECK(relative_path<>'' AND left(relative_path,1)<>'/'),
  size_bytes BIGINT NOT NULL CHECK(size_bytes>0),
  sha256 TEXT NOT NULL CHECK(sha256~'^[0-9a-f]{64}$'),
  PRIMARY KEY(task_id,ordinal),
  UNIQUE(task_id,clip_id),
  UNIQUE(task_id,relative_path)
);
CREATE INDEX recording_native_stitch_task_clips_clip_idx
  ON recording_native_stitch_task_clips(clip_id,task_id);

CREATE TABLE recording_native_stitch_certifications (
  id BIGSERIAL PRIMARY KEY,
  task_id BIGINT NOT NULL REFERENCES recording_native_stitch_tasks(id) ON DELETE RESTRICT,
  claim_token UUID NOT NULL,
  connection_id BIGINT NOT NULL CHECK(connection_id>0),
  status TEXT NOT NULL CHECK(status IN('passed','partial','failed','unknown')),
  nas_byte_decode_status TEXT NOT NULL CHECK(nas_byte_decode_status IN('passed','failed','unknown')),
  native_run_concat_status TEXT NOT NULL CHECK(native_run_concat_status IN('passed','failed','unknown')),
  within_run_frame_adjacency_status TEXT NOT NULL CHECK(within_run_frame_adjacency_status IN('passed','failed','unknown')),
  within_run_audio_sample_continuity_status TEXT NOT NULL CHECK(within_run_audio_sample_continuity_status IN('passed','failed','unknown','not_present')),
  window_continuity_status TEXT NOT NULL CHECK(window_continuity_status IN('passed','partitioned','failed','unknown')),
  run_count INTEGER NOT NULL CHECK(run_count BETWEEN 0 AND 1024),
  seam_count INTEGER NOT NULL CHECK(seam_count BETWEEN 0 AND 1023),
  audio_seam_count INTEGER NOT NULL CHECK(audio_seam_count BETWEEN 0 AND 1023),
  inventory_generation TEXT NOT NULL,
  inventory_digest TEXT NOT NULL CHECK(inventory_digest~'^[0-9a-f]{64}$'),
  inventory_completed_at TIMESTAMPTZ NOT NULL,
  report JSONB NOT NULL CHECK(jsonb_typeof(report)='object'),
  report_sha256 TEXT NOT NULL CHECK(report_sha256~'^[0-9a-f]{64}$'),
  policy_version TEXT NOT NULL,
  client_version TEXT NOT NULL,
  ffmpeg_version TEXT NOT NULL,
  ffprobe_version TEXT NOT NULL,
  reason_codes TEXT[] NOT NULL CHECK(cardinality(reason_codes) BETWEEN 1 AND 16),
  started_at TIMESTAMPTZ NOT NULL,
  completed_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK(completed_at>=started_at),
  UNIQUE(task_id,claim_token)
);

CREATE TABLE recording_native_stitch_certification_clips (
  certification_id BIGINT NOT NULL REFERENCES recording_native_stitch_certifications(id) ON DELETE RESTRICT,
  ordinal INTEGER NOT NULL CHECK(ordinal>0),
  clip_id BIGINT NOT NULL CHECK(clip_id>0),
  recording_job_id BIGINT NOT NULL,
  relative_path TEXT NOT NULL CHECK(relative_path<>'' AND left(relative_path,1)<>'/'),
  size_bytes BIGINT NOT NULL CHECK(size_bytes>0),
  sha256 TEXT NOT NULL CHECK(sha256~'^[0-9a-f]{64}$'),
  sidecar_sha256 TEXT NOT NULL CHECK(sidecar_sha256~'^[0-9a-f]{64}$'),
  clip_start_at TIMESTAMPTZ NOT NULL,
  clip_end_at TIMESTAMPTZ NOT NULL,
  capture_generation TEXT NOT NULL CHECK(btrim(capture_generation)<>''),
  capture_sequence BIGINT NOT NULL CHECK(capture_sequence>0),
  capture_attempt_id UUID,
  timestamp_contract_version TEXT,
  timestamp_contract_status TEXT,
  timestamp_contract_reason TEXT,
  timestamp_contract_sha256 TEXT CHECK(timestamp_contract_sha256 IS NULL OR timestamp_contract_sha256~'^[0-9a-f]{64}$'),
  recomputed_timestamp_contract JSONB,
  file_identity JSONB NOT NULL CHECK(jsonb_typeof(file_identity)='object'),
  native_signature JSONB NOT NULL CHECK(jsonb_typeof(native_signature)='object'),
  native_signature_sha256 TEXT NOT NULL CHECK(native_signature_sha256~'^[0-9a-f]{64}$'),
  strict_decode_status TEXT NOT NULL CHECK(strict_decode_status IN('passed','failed','unknown')),
  video_timeline JSONB,
  audio_present BOOLEAN NOT NULL,
  audio_timeline JSONB,
  PRIMARY KEY(certification_id,ordinal),
  UNIQUE(certification_id,clip_id),
  CHECK(clip_end_at>clip_start_at),
  CHECK(
    (capture_attempt_id IS NULL AND timestamp_contract_version IS NULL AND
     timestamp_contract_status IS NULL AND timestamp_contract_reason IS NULL AND
     timestamp_contract_sha256 IS NULL AND recomputed_timestamp_contract IS NULL)
    OR
    (capture_attempt_id IS NOT NULL AND timestamp_contract_version IS NULL AND
     timestamp_contract_status='per_clip_probe_unknown' AND
     timestamp_contract_reason IN('missing_terminal_duration','missing_audio_sample_count','invalid_time_base','probe_output_limit','probe_unavailable') AND
     timestamp_contract_sha256 IS NULL AND recomputed_timestamp_contract IS NULL)
    OR
    (capture_attempt_id IS NOT NULL AND timestamp_contract_version='continuous-source-pts-v1' AND
     timestamp_contract_status='per_clip_probe_complete' AND timestamp_contract_reason IS NULL AND
     timestamp_contract_sha256 IS NOT NULL AND jsonb_typeof(recomputed_timestamp_contract)='object')
  )
);

CREATE TABLE recording_native_stitch_certification_runs (
  certification_id BIGINT NOT NULL REFERENCES recording_native_stitch_certifications(id) ON DELETE RESTRICT,
  ordinal INTEGER NOT NULL CHECK(ordinal>0),
  first_clip_ordinal INTEGER NOT NULL CHECK(first_clip_ordinal>0),
  last_clip_ordinal INTEGER NOT NULL CHECK(last_clip_ordinal>=first_clip_ordinal),
  clip_count INTEGER NOT NULL CHECK(clip_count>0),
  source_bytes BIGINT NOT NULL CHECK(source_bytes>0 AND source_bytes<=34359738368),
  native_signature_sha256 TEXT NOT NULL CHECK(native_signature_sha256~'^[0-9a-f]{64}$'),
  capture_generation TEXT NOT NULL CHECK(btrim(capture_generation)<>''),
  capture_attempt_id UUID,
  timestamp_contract_version TEXT,
  boundary_reason TEXT NOT NULL CHECK(boundary_reason IN('window_start','capture_generation_change','capture_attempt_change','native_signature_change','temporal_gap')),
  validation_status TEXT NOT NULL CHECK(validation_status IN('single_clip_decode_only','lossless_concat_decode_passed','failed','unknown')),
  PRIMARY KEY(certification_id,ordinal),
  CHECK(clip_count=last_clip_ordinal-first_clip_ordinal+1)
);

CREATE TABLE recording_native_stitch_certification_seams (
  certification_id BIGINT NOT NULL REFERENCES recording_native_stitch_certifications(id) ON DELETE RESTRICT,
  ordinal INTEGER NOT NULL CHECK(ordinal>0),
  previous_clip_id BIGINT NOT NULL CHECK(previous_clip_id>0),
  next_clip_id BIGINT NOT NULL CHECK(next_clip_id>0),
  evidence JSONB NOT NULL CHECK(jsonb_typeof(evidence)='object'),
  verdict TEXT NOT NULL CHECK(verdict IN('exact','duplicate','missing','ambiguous','not_applicable')),
  reason TEXT NOT NULL CHECK(btrim(reason)<>''),
  PRIMARY KEY(certification_id,ordinal),
  CHECK(previous_clip_id<>next_clip_id)
);

CREATE TABLE recording_native_stitch_certification_audio_seams (
  certification_id BIGINT NOT NULL REFERENCES recording_native_stitch_certifications(id) ON DELETE RESTRICT,
  ordinal INTEGER NOT NULL CHECK(ordinal>0),
  previous_clip_id BIGINT NOT NULL CHECK(previous_clip_id>0),
  next_clip_id BIGINT NOT NULL CHECK(next_clip_id>0),
  evidence JSONB NOT NULL CHECK(jsonb_typeof(evidence)='object'),
  verdict TEXT NOT NULL CHECK(verdict IN('exact','missing','ambiguous','not_applicable','not_present')),
  reason TEXT NOT NULL CHECK(btrim(reason)<>''),
  PRIMARY KEY(certification_id,ordinal),
  CHECK(previous_clip_id<>next_clip_id)
);

CREATE FUNCTION guard_recording_native_stitch_task() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF jsonb_array_length(NEW.clip_manifest)<>NEW.clip_count THEN
    RAISE EXCEPTION 'native stitch task manifest count differs';
  END IF;
  IF TG_OP='INSERT' AND NEW.state<>'pending' THEN RAISE EXCEPTION 'native stitch tasks start pending'; END IF;
  IF TG_OP='UPDATE' AND
    (NEW.account_id,NEW.recording_id,NEW.recording_job_id,NEW.window_start_at,NEW.window_end_at,
     NEW.health_calculated_at,NEW.health_metric_version,NEW.health_facts,NEW.job_schedule_facts,NEW.qualification_scope,NEW.clip_manifest,
     NEW.clip_manifest_sha256,NEW.clip_count,NEW.source_bytes,NEW.policy_version,NEW.priority)
    IS DISTINCT FROM
    (OLD.account_id,OLD.recording_id,OLD.recording_job_id,OLD.window_start_at,OLD.window_end_at,
     OLD.health_calculated_at,OLD.health_metric_version,OLD.health_facts,OLD.job_schedule_facts,OLD.qualification_scope,OLD.clip_manifest,
     OLD.clip_manifest_sha256,OLD.clip_count,OLD.source_bytes,OLD.policy_version,OLD.priority) THEN
    RAISE EXCEPTION 'native stitch task evidence is immutable';
  END IF;
  IF TG_OP='UPDATE' THEN
    IF OLD.state IN('passed','partial','failed','stale') THEN RAISE EXCEPTION 'terminal native stitch task is immutable'; END IF;
    IF NEW.attempt_count<OLD.attempt_count OR NEW.attempt_count>OLD.attempt_count+1 THEN RAISE EXCEPTION 'invalid native stitch attempt counter'; END IF;
    IF OLD.state='pending' AND NEW.state='leased' THEN
      IF NEW.attempt_count<>OLD.attempt_count+1 OR NEW.claim_token IS NULL OR NEW.claimed_connection_id IS NULL OR NEW.lease_expires_at IS NULL THEN RAISE EXCEPTION 'invalid native stitch claim'; END IF;
    ELSIF OLD.state='pending' AND NEW.state IN('pending','stale') THEN
      IF NEW.attempt_count<>OLD.attempt_count OR NEW.claim_token IS NOT NULL THEN RAISE EXCEPTION 'invalid pending native stitch mutation'; END IF;
    ELSIF OLD.state='leased' AND NEW.state='leased' THEN
      IF (NEW.claim_token,NEW.claimed_connection_id,NEW.lease_expires_at,NEW.attempt_count) IS DISTINCT FROM (OLD.claim_token,OLD.claimed_connection_id,OLD.lease_expires_at,OLD.attempt_count) THEN RAISE EXCEPTION 'leased native stitch claim is fenced'; END IF;
    ELSIF OLD.state='leased' AND NEW.state IN('pending','passed','partial','failed','stale') THEN
      IF NEW.attempt_count<>OLD.attempt_count OR NEW.claim_token IS NOT NULL OR NEW.claimed_connection_id IS NOT NULL OR NEW.lease_expires_at IS NOT NULL THEN RAISE EXCEPTION 'invalid native stitch completion'; END IF;
    ELSE RAISE EXCEPTION 'invalid native stitch task transition'; END IF;
  END IF;
  NEW.updated_at=now();
  RETURN NEW;
END $$;
CREATE TRIGGER recording_native_stitch_task_guard BEFORE INSERT OR UPDATE ON recording_native_stitch_tasks
FOR EACH ROW EXECUTE FUNCTION guard_recording_native_stitch_task();

CREATE FUNCTION guard_recording_native_stitch_task_clip() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE frozen JSONB;
DECLARE task_state TEXT;
BEGIN
  SELECT state,clip_manifest->(NEW.ordinal-1) INTO task_state,frozen
  FROM recording_native_stitch_tasks WHERE id=NEW.task_id FOR SHARE;
  IF task_state IS NULL OR task_state<>'pending' OR frozen IS NULL OR
     (frozen->>'ordinal')::integer<>NEW.ordinal OR
     (frozen->>'clip_id')::bigint<>NEW.clip_id OR
     frozen->>'relative_path'<>NEW.relative_path OR
     (frozen->>'size_bytes')::bigint<>NEW.size_bytes OR
     frozen->>'sha256'<>NEW.sha256 THEN
    RAISE EXCEPTION 'native stitch task clip differs from frozen manifest';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER recording_native_stitch_task_clip_guard BEFORE INSERT ON recording_native_stitch_task_clips
FOR EACH ROW EXECUTE FUNCTION guard_recording_native_stitch_task_clip();

CREATE FUNCTION reject_native_stitch_fact_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'native stitch certification facts are append-only'; END $$;
CREATE TRIGGER native_stitch_task_no_delete BEFORE DELETE OR TRUNCATE ON recording_native_stitch_tasks
FOR EACH STATEMENT EXECUTE FUNCTION reject_native_stitch_fact_mutation();
CREATE TRIGGER native_stitch_task_clip_no_mutation BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_native_stitch_task_clips
FOR EACH STATEMENT EXECUTE FUNCTION reject_native_stitch_fact_mutation();
CREATE TRIGGER native_stitch_cert_no_mutation BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_native_stitch_certifications
FOR EACH STATEMENT EXECUTE FUNCTION reject_native_stitch_fact_mutation();
CREATE TRIGGER native_stitch_clip_no_mutation BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_native_stitch_certification_clips
FOR EACH STATEMENT EXECUTE FUNCTION reject_native_stitch_fact_mutation();
CREATE TRIGGER native_stitch_run_no_mutation BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_native_stitch_certification_runs
FOR EACH STATEMENT EXECUTE FUNCTION reject_native_stitch_fact_mutation();
CREATE TRIGGER native_stitch_seam_no_mutation BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_native_stitch_certification_seams
FOR EACH STATEMENT EXECUTE FUNCTION reject_native_stitch_fact_mutation();
CREATE TRIGGER native_stitch_audio_seam_no_mutation BEFORE UPDATE OR DELETE OR TRUNCATE ON recording_native_stitch_certification_audio_seams
FOR EACH STATEMENT EXECUTE FUNCTION reject_native_stitch_fact_mutation();

CREATE VIEW recording_native_stitch_facts AS
SELECT t.id task_id,t.policy_version,t.created_at task_created_at,
  t.account_id,t.recording_id,t.recording_job_id,t.window_start_at,t.window_end_at,t.qualification_scope,
  t.clip_manifest_sha256,t.clip_count,t.source_bytes,t.state,
  c.id certification_id,c.status,c.nas_byte_decode_status,c.native_run_concat_status,
  c.within_run_frame_adjacency_status,c.within_run_audio_sample_continuity_status,c.window_continuity_status,
  c.run_count,c.seam_count,c.audio_seam_count,c.inventory_generation,c.inventory_digest,c.inventory_completed_at,
  c.report_sha256,c.reason_codes,c.completed_at
FROM recording_native_stitch_tasks t
LEFT JOIN recording_native_stitch_certifications c ON c.task_id=t.id
;
