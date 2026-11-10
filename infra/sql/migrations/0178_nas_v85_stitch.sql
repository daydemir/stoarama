-- Opt-in NAS-local V85. No scope, job or deletion authority is seeded.
CREATE TABLE nas_stitch_scopes (
 id bigserial PRIMARY KEY,
 stream_id bigint NOT NULL REFERENCES streams(id),
 start_at timestamptz NOT NULL, end_at timestamptz NOT NULL,
 reason text NOT NULL CHECK(length(reason) BETWEEN 1 AND 500),
 approved_until timestamptz NOT NULL,
 deletion_approved boolean NOT NULL DEFAULT false,
 paused boolean NOT NULL DEFAULT false,
 created_at timestamptz NOT NULL DEFAULT now(),
 CHECK(end_at>start_at AND end_at<=start_at+interval '29 days')
);
CREATE TABLE nas_stitch_corpus (
 sha256 text PRIMARY KEY CHECK(sha256='5b2765ecb4f6483c8e48b5a6c958c7c0d8082764d4da57901626a22569c6346b'),
 body bytea NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE nas_stitch_edges (
 left_id bigint NOT NULL, left_sha text NOT NULL, left_size bigint NOT NULL,
 right_id bigint NOT NULL, right_sha text NOT NULL, right_size bigint NOT NULL,
 recording_id bigint NOT NULL, diagnostic_sha text NOT NULL,
 nomination boolean NOT NULL, witness jsonb NOT NULL,
 PRIMARY KEY(left_id,left_sha,left_size,right_id,right_sha,right_size)
);
CREATE TABLE nas_stitch_tasks (
 id bigserial PRIMARY KEY, scope_id bigint NOT NULL REFERENCES nas_stitch_scopes(id),
 recording_id bigint NOT NULL REFERENCES recordings(id), hour_at timestamptz NOT NULL,
 plan jsonb NOT NULL, plan_sha256 text NOT NULL CHECK(plan_sha256 ~ '^[a-f0-9]{64}$'),
 state text NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','leased','verified','held')),
 attempt integer NOT NULL DEFAULT 0, lease_token text, lease_expires_at timestamptz,
 result jsonb, cleanup_plan_id bigint REFERENCES nas_cleanup_plans(id),
 updated_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(recording_id,hour_at)
);
CREATE TABLE nas_stitch_outputs (
 task_id bigint NOT NULL REFERENCES nas_stitch_tasks(id),
 relative_path text NOT NULL UNIQUE, sha256 text NOT NULL CHECK(sha256 ~ '^[a-f0-9]{64}$'),
 size_bytes bigint NOT NULL CHECK(size_bytes>0), proof jsonb NOT NULL,
 PRIMARY KEY(task_id,sha256)
);
ALTER TABLE connections ADD COLUMN nas_stitch_status jsonb,
 ADD COLUMN nas_stitch_status_at timestamptz;
-- Output proofs and the imported corpus are write-once. Never erase evidence.
CREATE FUNCTION guard_nas_stitch_evidence() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 RAISE EXCEPTION 'NAS stitch evidence is append-only'; END $$;
CREATE TRIGGER nas_stitch_output_immutable BEFORE UPDATE OR DELETE ON nas_stitch_outputs FOR EACH ROW EXECUTE FUNCTION guard_nas_stitch_evidence();
CREATE TRIGGER nas_stitch_corpus_immutable BEFORE UPDATE OR DELETE ON nas_stitch_corpus FOR EACH ROW EXECUTE FUNCTION guard_nas_stitch_evidence();
CREATE TRIGGER nas_stitch_edge_immutable BEFORE UPDATE OR DELETE ON nas_stitch_edges FOR EACH ROW EXECUTE FUNCTION guard_nas_stitch_evidence();
CREATE FUNCTION guard_nas_stitch_task() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF TG_OP='DELETE' OR ROW(NEW.scope_id,NEW.recording_id,NEW.hour_at,NEW.plan,NEW.plan_sha256) IS DISTINCT FROM ROW(OLD.scope_id,OLD.recording_id,OLD.hour_at,OLD.plan,OLD.plan_sha256)
 OR (OLD.state IN ('verified','held') AND ROW(NEW.state,NEW.result) IS DISTINCT FROM ROW(OLD.state,OLD.result))
 OR (OLD.cleanup_plan_id IS NOT NULL AND NEW.cleanup_plan_id IS DISTINCT FROM OLD.cleanup_plan_id) THEN
 RAISE EXCEPTION 'NAS stitch task identity/result is immutable'; END IF; RETURN NEW; END $$;
CREATE TRIGGER nas_stitch_task_immutable BEFORE UPDATE OR DELETE ON nas_stitch_tasks FOR EACH ROW EXECUTE FUNCTION guard_nas_stitch_task();
CREATE FUNCTION guard_nas_stitch_scope() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF TG_OP='DELETE' OR ROW(NEW.stream_id,NEW.start_at,NEW.end_at,NEW.reason,NEW.approved_until,NEW.deletion_approved,NEW.created_at) IS DISTINCT FROM ROW(OLD.stream_id,OLD.start_at,OLD.end_at,OLD.reason,OLD.approved_until,OLD.deletion_approved,OLD.created_at) THEN
 RAISE EXCEPTION 'NAS stitch scope identity is immutable'; END IF; RETURN NEW; END $$;
CREATE TRIGGER nas_stitch_scope_immutable BEFORE UPDATE OR DELETE ON nas_stitch_scopes FOR EACH ROW EXECUTE FUNCTION guard_nas_stitch_scope();
-- Partial repair evidence is deliberately separate from retirement authority.
CREATE TABLE nas_stitch_repairs (
 task_id bigint NOT NULL REFERENCES nas_stitch_tasks(id), clip_id bigint NOT NULL REFERENCES recording_clips(id),
 proof jsonb NOT NULL, PRIMARY KEY(task_id,clip_id)
);
CREATE TRIGGER nas_stitch_repair_immutable BEFORE UPDATE OR DELETE ON nas_stitch_repairs FOR EACH ROW EXECUTE FUNCTION guard_nas_stitch_evidence();
CREATE INDEX nas_stitch_edges_right_nominee ON nas_stitch_edges(right_id,right_sha,right_size) WHERE nomination;
CREATE INDEX nas_stitch_edges_left_nominee ON nas_stitch_edges(left_id,left_sha,left_size) WHERE nomination;
CREATE INDEX nas_stitch_tasks_ready ON nas_stitch_tasks(id) WHERE state IN ('pending','leased');
