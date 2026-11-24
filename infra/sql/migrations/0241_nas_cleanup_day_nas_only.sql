-- Apply after 0239 and 0240 (fast-copy client version and lease cap).
-- Owner (Deniz), verbatim: "yes, delete"; "if its outside of keep everything,
-- then a nas copy only needs to be checked if its in the keep 20? otherwise it
-- can be deleted right?"; "yea we don tneed exreme intense checkingm"; "lift
-- nas cleanup dialy limit please".
-- NAS raw footage of a non-Keep-Everything stream outside its pinned 20-day
-- window leaves the NAS one stream-day directory at a time, with no R2 copy
-- check. Installing this creates no plan and no approval; the generic
-- allow_nas_only prohibition on nas_cleanup_plans is unchanged. Only the
-- r2-copy-v8-tunable-copy client (admitted by its own binding migration)
-- receives day permits.

-- The holds that still matter without an R2 copy check: delivery, incident,
-- stitch input, joined release, retirement reservation, and any live per-file
-- cleanup plan that reserves the clip.
CREATE FUNCTION nas_cleanup_day_clip_held(clip bigint) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM recording_clips c JOIN recordings r ON r.id=c.recording_id WHERE c.id=clip AND (
  delivery_nas_held('raw',c.id,c.display_path,lower(c.sha256),c.size_bytes)
  OR EXISTS(SELECT 1 FROM nas_cleanup_incident_sources h WHERE h.clip_id=c.id)
  OR EXISTS(SELECT 1 FROM nas_cleanup_incident_files h WHERE h.kind='raw' AND h.source_id=c.id)
  OR EXISTS(SELECT 1 FROM r2_retirement_reservations h WHERE h.kind='raw' AND h.source_id=c.id)
  OR EXISTS(SELECT 1 FROM nas_stitch_r2_input_holds h WHERE h.catalog_clip_id=c.id AND h.released_at IS NULL)
  OR EXISTS(SELECT 1 FROM nas_stitch_tasks t WHERE t.recording_id=r.id
   AND t.hour_at>=date_trunc('hour',c.clip_start_at,'UTC') AND t.hour_at<c.clip_end_at
   AND (t.state IN ('pending','leased','held') OR (t.state='verified' AND t.cleanup_plan_id IS NULL AND t.registration_abandoned_at IS NULL)))
  OR EXISTS(SELECT 1 FROM nas_stitch_r2_hours t WHERE t.recording_id=r.id
   AND t.hour_at>=date_trunc('hour',c.clip_start_at,'UTC') AND t.hour_at<c.clip_end_at AND t.state IN ('pending','leased'))
  OR EXISTS(SELECT 1 FROM recording_joined_snapshot_scopes scope
   JOIN recording_joined_batches batch ON batch.id=scope.batch_record_id AND batch.state='snapshotting'
   WHERE scope.recording_id=r.id AND scope.recording_job_id=c.recording_job_id AND scope.high_water_clip_id>=c.id)
  OR EXISTS(SELECT 1 FROM recording_joined_dry_run_scopes scope
   JOIN recording_joined_dry_runs run ON run.id=scope.dry_run_id
   WHERE scope.recording_id=r.id AND scope.recording_job_id=c.recording_job_id AND scope.high_water_clip_id>=c.id
    AND NOT EXISTS(SELECT 1 FROM recording_joined_batches batch WHERE batch.batch_id=run.batch_id
     AND batch.connection_id=run.connection_id AND batch.generation=run.generation AND batch.state IN ('frozen','index_sealed','published')))
  OR (EXISTS(SELECT 1 FROM recording_joined_source_snapshots snapshot WHERE snapshot.clip_id=c.id)
   AND NOT r2_joined_source_released(c.id))
  OR EXISTS(SELECT 1 FROM nas_cleanup_source_holds h JOIN nas_cleanup_plans p ON p.id=h.plan_id
   WHERE h.clip_id=c.id AND p.mode<>'retired')))
$$;

-- A delivery path hold anywhere in the directory keeps the whole directory.
CREATE FUNCTION nas_cleanup_day_dir_held(dir text) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM delivery_keep_holds h WHERE h.relative_path<>'' AND left(h.relative_path,length(dir)+1)=dir||'/')
$$;

CREATE TABLE nas_cleanup_day_plans (
 id bigserial PRIMARY KEY,
 account_id bigint NOT NULL CHECK(account_id=47),
 connection_id bigint NOT NULL REFERENCES connections(id) CHECK(connection_id=13),
 label text NOT NULL UNIQUE CHECK(label ~ '^[a-z0-9][a-z0-9._-]{0,79}$'),
 policy_sha256 text NOT NULL CHECK(policy_sha256='bcc670a34024c2f8e2ba3df2566397f370d03e93ff33a7bb828c4b83eb123393'),
 plan_sha256 text NOT NULL UNIQUE CHECK(plan_sha256 ~ '^[a-f0-9]{64}$'),
 units integer NOT NULL CHECK(units BETWEEN 1 AND 1000),
 files bigint NOT NULL CHECK(files>0),
 bytes bigint NOT NULL CHECK(bytes>0),
 mode text NOT NULL DEFAULT 'dry_run' CHECK(mode IN ('dry_run','enabled','paused','retired')),
 approval_reason text NOT NULL DEFAULT '',
 approved_until timestamptz,
 created_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now(),
 CHECK(mode<>'enabled' OR (approved_until IS NOT NULL AND approval_reason<>''))
);
-- One plan at a time.
CREATE UNIQUE INDEX nas_cleanup_day_plans_one_enabled ON nas_cleanup_day_plans((true)) WHERE mode='enabled';

CREATE TABLE nas_cleanup_day_units (
 id bigserial PRIMARY KEY,
 plan_id bigint NOT NULL REFERENCES nas_cleanup_day_plans(id),
 ordinal integer NOT NULL CHECK(ordinal>0),
 stream_id bigint NOT NULL CHECK(stream_id>0),
 relative_dir text NOT NULL CHECK(relative_dir<>'' AND left(relative_dir,1)<>'/' AND octet_length(relative_dir)<=1024),
 local_date date NOT NULL,
 files bigint NOT NULL CHECK(files>0),
 bytes bigint NOT NULL CHECK(bytes>0),
 state text NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','permitted','deleted','refused','client_refused','retired')),
 attempt integer NOT NULL DEFAULT 0 CHECK(attempt>=0),
 permit_token_sha256 text CHECK(permit_token_sha256 ~ '^[a-f0-9]{64}$'),
 permit_expires_at timestamptz,
 refusal text NOT NULL DEFAULT '',
 -- One receipt per permit attempt, appended; deletion totals sum them.
 receipts jsonb NOT NULL DEFAULT '[]' CHECK(jsonb_typeof(receipts)='array'),
 updated_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(plan_id,ordinal),
 CHECK(state<>'permitted' OR (permit_token_sha256 IS NOT NULL AND permit_expires_at IS NOT NULL AND attempt>0)),
 CHECK(state NOT IN ('deleted','client_refused') OR jsonb_array_length(receipts)>0)
);
-- A stream-day directory belongs to at most one live plan.
CREATE UNIQUE INDEX nas_cleanup_day_units_live ON nas_cleanup_day_units(relative_dir,local_date) WHERE state IN ('pending','permitted');
CREATE INDEX nas_cleanup_day_units_work ON nas_cleanup_day_units(plan_id,ordinal) WHERE state IN ('pending','permitted');

CREATE FUNCTION guard_nas_cleanup_day_plan() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' OR ROW(NEW.account_id,NEW.connection_id,NEW.label,NEW.policy_sha256,NEW.plan_sha256,NEW.units,NEW.files,NEW.bytes,NEW.created_at)
  IS DISTINCT FROM ROW(OLD.account_id,OLD.connection_id,OLD.label,OLD.policy_sha256,OLD.plan_sha256,OLD.units,OLD.files,OLD.bytes,OLD.created_at)
  OR (OLD.mode='retired' AND NEW.mode<>'retired') THEN
  RAISE EXCEPTION 'NAS day cleanup plan identity is immutable';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER nas_cleanup_day_plan_identity BEFORE UPDATE OR DELETE ON nas_cleanup_day_plans
 FOR EACH ROW EXECUTE FUNCTION guard_nas_cleanup_day_plan();

CREATE FUNCTION guard_nas_cleanup_day_unit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' OR ROW(NEW.plan_id,NEW.ordinal,NEW.stream_id,NEW.relative_dir,NEW.local_date,NEW.files,NEW.bytes)
  IS DISTINCT FROM ROW(OLD.plan_id,OLD.ordinal,OLD.stream_id,OLD.relative_dir,OLD.local_date,OLD.files,OLD.bytes)
  OR NEW.attempt<OLD.attempt
  OR (jsonb_array_length(OLD.receipts)>0 AND (SELECT jsonb_agg(e ORDER BY n) FROM jsonb_array_elements(NEW.receipts)
   WITH ORDINALITY x(e,n) WHERE n<=jsonb_array_length(OLD.receipts)) IS DISTINCT FROM OLD.receipts)
  OR (OLD.state IN ('deleted','refused','client_refused') AND NEW.state<>OLD.state) THEN
  RAISE EXCEPTION 'NAS day cleanup unit identity and receipts are append-only';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER nas_cleanup_day_unit_identity BEFORE UPDATE OR DELETE ON nas_cleanup_day_units
 FOR EACH ROW EXECUTE FUNCTION guard_nas_cleanup_day_unit();
