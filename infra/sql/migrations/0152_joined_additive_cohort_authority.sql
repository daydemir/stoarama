-- One approved additive cohort, not a general cohort registry. Legacy branches
-- retain the original IDs, count, hash, cutoff, wire seals and source-retention guards.
-- This migration neither activates a run nor changes worker admission/recordings.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '15s';

CREATE FUNCTION recording_joined_cohort_ids(batch TEXT) RETURNS BIGINT[]
LANGUAGE SQL IMMUTABLE AS $$
  SELECT CASE WHEN batch='goodplus-20260911-generation-1' THEN
    ARRAY[339,407,417,424,427,430,441,444,445]::BIGINT[]
  ELSE ARRAY[377,335,337,355,385,350,382,384,348,403,380,379,383,404,401,408,406,
    409,422,418,419,413,420,428,423,425,416,421,437,440,429,431,439]::BIGINT[] END;
$$;
CREATE FUNCTION recording_joined_cohort_ids_sha256(batch TEXT) RETURNS TEXT
LANGUAGE SQL IMMUTABLE AS $$
  SELECT CASE WHEN batch='goodplus-20260911-generation-1'
    THEN '85b4fc9db34b50aa53666f1d364bef8ce267b581d56d17411d5342bd20648888'
    ELSE '6038d4a23be9b0b5c2bb29ea933743a5ceb7f06b8875e417a3f16b44051ebd71' END;
$$;
CREATE FUNCTION recording_joined_cohort_cutoff_json(batch TEXT) RETURNS JSONB
LANGUAGE SQL IMMUTABLE AS $$
  SELECT CASE WHEN batch='goodplus-20260911-generation-1'
    THEN '"2026-09-11T05:15:24.544Z"'::JSONB
    ELSE '"2026-08-21T06:59:07.534131Z"'::JSONB END;
$$;

ALTER TABLE recording_qualification_runs
  DROP CONSTRAINT recording_qualification_runs_target_recording_count_check,
  ADD CONSTRAINT recording_qualification_runs_target_recording_count_check CHECK (
    target_recording_count >= 50 OR
    (definition_version='recording-qualification-tier1-historical-import-v1'
      AND target_recording_count=cardinality(recording_joined_cohort_ids(definition_jsonb->>'batch_id')))
  );
-- Keep all ordinary/legacy runs in the original single-active bucket. Only the
-- exact approved historical definition gets a second bucket; no cancellation.
DROP INDEX recording_qualification_runs_one_active_idx;
CREATE UNIQUE INDEX recording_qualification_runs_one_active_idx
  ON recording_qualification_runs (account_id,
    (definition_version='recording-qualification-tier1-historical-import-v1'
      AND COALESCE(definition_jsonb->>'batch_id','')='goodplus-20260911-generation-1'))
  WHERE status='active';

-- Preserve the exact old CHECK expression for every non-additive batch. Locate
-- only the three anonymous original pin constraints, independent of PG naming.
DO $$
DECLARE item RECORD; replaced INTEGER:=0; replacement TEXT;
BEGIN
  FOR item IN SELECT conname,pg_get_expr(conbin,conrelid) expression
    FROM pg_constraint WHERE conrelid='recording_joined_batches'::regclass AND contype='c'
  LOOP
    replacement:=NULL;
    IF item.expression LIKE '%expected_recordings = 33%' THEN
      replacement:='expected_recordings=9 AND expected_stream_days=126 AND expected_scheduled_hours=1512';
    ELSIF item.expression LIKE '%6038d4a23be9b0b5c2bb29ea933743a5ceb7f06b8875e417a3f16b44051ebd71%' THEN
      replacement:='ordered_recording_ids_sha256=''85b4fc9db34b50aa53666f1d364bef8ce267b581d56d17411d5342bd20648888''';
    ELSIF item.expression LIKE '%eligibility_cutoff%2026-08-21%' THEN
      replacement:='eligibility_cutoff=''2026-09-11T05:15:24.544Z''::TIMESTAMPTZ';
    END IF;
    IF replacement IS NOT NULL THEN
      EXECUTE format('ALTER TABLE recording_joined_batches DROP CONSTRAINT %I, ADD CONSTRAINT %I CHECK (CASE WHEN batch_id=%L THEN (%s) ELSE (%s) END)',
        item.conname,item.conname,'goodplus-20260911-generation-1',replacement,item.expression);
      replaced:=replaced+1;
    END IF;
  END LOOP;
  IF replaced<>3 THEN RAISE EXCEPTION 'expected exactly three original joined cohort pins'; END IF;
END $$;

-- Preserve the dry-run state machine; only its approved cohort size differs.
DO $$
DECLARE item RECORD; replaced INTEGER:=0;
BEGIN
  FOR item IN SELECT conname,pg_get_expr(conbin,conrelid) expression
    FROM pg_constraint WHERE conrelid='recording_joined_dry_runs'::regclass AND contype='c'
  LOOP
    IF item.expression LIKE '%completed_recordings = 33%final_plan_bytes%' THEN
      EXECUTE format('ALTER TABLE recording_joined_dry_runs DROP CONSTRAINT %I, ADD CONSTRAINT %I CHECK (%s)',
        item.conname,item.conname,replace(item.expression,'completed_recordings = 33',
          'completed_recordings = cardinality(recording_joined_cohort_ids(batch_id))'));
      replaced:=replaced+1;
    END IF;
  END LOOP;
  IF replaced<>1 THEN RAISE EXCEPTION 'expected original joined dry-run ready count guard'; END IF;
END $$;

CREATE OR REPLACE FUNCTION enforce_recording_qualification_run_lifecycle()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  member_count INTEGER;
  window_count INTEGER;
  invalid_count INTEGER;
  member_json JSONB;
  window_json JSONB;
  definition_sha TEXT;
  window_sha TEXT;
  authoritative_count INTEGER:=0;
  authoritative RECORD;
  historical BOOLEAN;
BEGIN
  IF TG_OP='INSERT' THEN
    IF NEW.status<>'building' THEN RAISE EXCEPTION 'qualification run must start building'; END IF;
    RETURN NEW;
  END IF;
  IF TG_OP='DELETE' THEN
    IF OLD.status<>'building' AND NOT (OLD.status='canceled' AND OLD.frozen_at IS NULL) THEN
      RAISE EXCEPTION 'activated qualification run cannot be deleted';
    END IF;
    RETURN OLD;
  END IF;
  IF OLD.status='building' AND NEW.status='active' THEN
    IF NEW.account_id IS DISTINCT FROM OLD.account_id OR NEW.definition_version IS DISTINCT FROM OLD.definition_version OR
       NEW.definition_jsonb IS DISTINCT FROM OLD.definition_jsonb OR NEW.target_recording_count IS DISTINCT FROM OLD.target_recording_count OR
       NEW.target_window_count IS DISTINCT FROM OLD.target_window_count OR NEW.required_good_or_great IS DISTINCT FROM OLD.required_good_or_great OR
       NEW.max_acceptable IS DISTINCT FROM OLD.max_acceptable OR NEW.window_sequence_start_at IS DISTINCT FROM OLD.window_sequence_start_at OR
       NEW.created_at IS DISTINCT FROM OLD.created_at THEN
      RAISE EXCEPTION 'activation cannot rewrite qualification definition';
    END IF;
    historical:=OLD.definition_version='recording-qualification-tier1-historical-import-v1';
    SELECT count(*)::int INTO member_count FROM recording_qualification_members WHERE run_id=OLD.id;
    SELECT count(*)::int INTO window_count FROM recording_qualification_windows WHERE run_id=OLD.id;
    IF member_count<>OLD.target_recording_count OR
       (historical AND member_count<>cardinality(recording_joined_cohort_ids(OLD.definition_jsonb->>'batch_id'))) OR (NOT historical AND member_count<50) OR
       window_count<>member_count*14 THEN
      RAISE EXCEPTION 'qualification cohort is incomplete';
    END IF;
    FOR authoritative IN
      SELECT m.recording_id,m.account_id,m.stream_id,m.recording_name,m.cron_timezone,
             m.daily_window_start,m.daily_window_end,m.active_weekdays,m.schedule_start_at,m.schedule_end_at,
             r.id AS authoritative_id,r.account_id AS authoritative_account_id,
             r.stream_id AS authoritative_stream_id,r.name AS authoritative_name,
             r.mode AS authoritative_mode,r.status AS authoritative_status,
             r.cron_timezone AS authoritative_timezone,r.daily_window_start AS authoritative_window_start,
             r.daily_window_end AS authoritative_window_end,r.active_weekdays AS authoritative_weekdays,
             r.start_at AS authoritative_start_at,r.end_at AS authoritative_end_at
      FROM recording_qualification_members m
      JOIN recordings r ON r.id=m.recording_id
      WHERE m.run_id=OLD.id
      ORDER BY r.id
      FOR SHARE OF r
    LOOP
      authoritative_count:=authoritative_count+1;
      IF authoritative.authoritative_account_id<>authoritative.account_id OR
         authoritative.authoritative_stream_id IS DISTINCT FROM authoritative.stream_id OR
         authoritative.authoritative_name<>authoritative.recording_name OR
         authoritative.authoritative_mode<>'continuous' OR
         (historical AND authoritative.authoritative_status NOT IN('active','completed')) OR
         (NOT historical AND authoritative.authoritative_status<>'active') OR
         authoritative.authoritative_timezone<>authoritative.cron_timezone OR
         authoritative.authoritative_window_start IS DISTINCT FROM authoritative.daily_window_start OR
         authoritative.authoritative_window_end IS DISTINCT FROM authoritative.daily_window_end OR
         authoritative.authoritative_weekdays<>authoritative.active_weekdays OR
         authoritative.authoritative_start_at<>authoritative.schedule_start_at OR
         authoritative.authoritative_end_at IS DISTINCT FROM authoritative.schedule_end_at THEN
        RAISE EXCEPTION 'qualification member does not match authoritative recording %', authoritative.recording_id;
      END IF;
    END LOOP;
    IF authoritative_count<>member_count THEN
      RAISE EXCEPTION 'qualification cohort references missing authoritative recordings';
    END IF;

    IF historical THEN
      IF OLD.definition_jsonb->>'version' IS DISTINCT FROM OLD.definition_version OR
         OLD.definition_jsonb->>'authority_kind' IS DISTINCT FROM 'historical_operator_import_v1' OR
         OLD.definition_jsonb->>'batch_id' IS DISTINCT FROM (CASE WHEN OLD.definition_jsonb->>'batch_id'='goodplus-20260911-generation-1' THEN 'goodplus-20260911-generation-1' ELSE 'goodplus-20260821-generation-1' END) OR
         (OLD.definition_jsonb->>'generation')::INTEGER IS DISTINCT FROM 1 OR
         (OLD.definition_jsonb->'generation')::TEXT IS DISTINCT FROM '1' OR
         OLD.definition_jsonb->'cutoff' IS DISTINCT FROM recording_joined_cohort_cutoff_json(OLD.definition_jsonb->>'batch_id') OR
         OLD.definition_jsonb->>'ordered_recording_ids_sha256' IS DISTINCT FROM recording_joined_cohort_ids_sha256(OLD.definition_jsonb->>'batch_id') OR
         COALESCE(OLD.definition_jsonb->>'request_sha256','') !~ '^[0-9a-f]{64}$' OR
         COALESCE(OLD.definition_jsonb->>'qualification_jobs_sha256','') !~ '^[0-9a-f]{64}$' OR
	     COALESCE(OLD.definition_jsonb->>'qualification_jobs_canonical','')='' OR
	     COALESCE(OLD.definition_jsonb->>'request_canonical','')='' OR
	     OLD.definition_jsonb->>'qualification_jobs_canonical' IS DISTINCT FROM
	       recording_historical_qualification_jobs_canonical(OLD.definition_jsonb->'recording_jobs') OR
	     encode(sha256(convert_to(recording_historical_qualification_jobs_canonical(
	       OLD.definition_jsonb->'recording_jobs'),'UTF8')),'hex')
	       IS DISTINCT FROM OLD.definition_jsonb->>'qualification_jobs_sha256' OR
	     (OLD.definition_jsonb->>'qualification_jobs_canonical')::JSONB
	       IS DISTINCT FROM OLD.definition_jsonb->'recording_jobs' OR
	     OLD.definition_jsonb->>'request_canonical' IS DISTINCT FROM
	       recording_historical_request_canonical(OLD.definition_jsonb->'canonical_plan') OR
	     encode(sha256(convert_to(recording_historical_request_canonical(
	       OLD.definition_jsonb->'canonical_plan'),'UTF8')),'hex')
	       IS DISTINCT FROM OLD.definition_jsonb->>'request_sha256' OR
	     (OLD.definition_jsonb->>'request_canonical')::JSONB
	       IS DISTINCT FROM (OLD.definition_jsonb->'canonical_plan')-'request_sha256'::TEXT OR
	     OLD.definition_jsonb->'canonical_plan'->>'request_sha256'
	       IS DISTINCT FROM OLD.definition_jsonb->>'request_sha256' OR
	     OLD.definition_jsonb->'canonical_plan'->>'qualification_jobs_sha256'
	       IS DISTINCT FROM OLD.definition_jsonb->>'qualification_jobs_sha256' OR
         OLD.definition_jsonb->'historical_scene_claim' IS DISTINCT FROM 'false'::JSONB OR
         OLD.definition_jsonb->'historical_per_day_grade_claim' IS DISTINCT FROM 'false'::JSONB OR
         jsonb_typeof(OLD.definition_jsonb->'canonical_plan') IS DISTINCT FROM 'object' OR
         ARRAY(SELECT key FROM jsonb_object_keys(OLD.definition_jsonb->'canonical_plan') AS keys(key)
           ORDER BY key COLLATE "C") IS DISTINCT FROM ARRAY['account_id','authority_kind','batch_id','connection_id',
             'cutoff','generation','members','ordered_recording_ids_sha256','qualification_jobs_sha256',
             'qualification_rule_version','request_sha256','schema_version']::TEXT[] OR
         (OLD.definition_jsonb->'canonical_plan'->>'schema_version')::INTEGER IS DISTINCT FROM 1 OR
         (OLD.definition_jsonb->'canonical_plan'->'schema_version')::TEXT IS DISTINCT FROM '1' OR
         OLD.definition_jsonb->'canonical_plan'->>'authority_kind' IS DISTINCT FROM 'historical_operator_import_v1' OR
         OLD.definition_jsonb->'canonical_plan'->>'batch_id' IS DISTINCT FROM (CASE WHEN OLD.definition_jsonb->>'batch_id'='goodplus-20260911-generation-1' THEN 'goodplus-20260911-generation-1' ELSE 'goodplus-20260821-generation-1' END) OR
         (OLD.definition_jsonb->'canonical_plan'->>'generation')::INTEGER IS DISTINCT FROM 1 OR
         (OLD.definition_jsonb->'canonical_plan'->'generation')::TEXT IS DISTINCT FROM '1' OR
         (OLD.definition_jsonb->'canonical_plan'->>'account_id')::BIGINT IS DISTINCT FROM OLD.account_id OR
         (OLD.definition_jsonb->'canonical_plan'->'account_id')::TEXT IS DISTINCT FROM OLD.account_id::TEXT OR
         NOT EXISTS(SELECT 1 FROM connections c WHERE c.id=(OLD.definition_jsonb->'canonical_plan'->>'connection_id')::BIGINT
           AND c.account_id=OLD.account_id) OR
         (OLD.definition_jsonb->'canonical_plan'->'connection_id')::TEXT IS DISTINCT FROM
           (OLD.definition_jsonb->'canonical_plan'->>'connection_id')::BIGINT::TEXT OR
         OLD.definition_jsonb->'canonical_plan'->'cutoff' IS DISTINCT FROM recording_joined_cohort_cutoff_json(OLD.definition_jsonb->>'batch_id') OR
         OLD.definition_jsonb->'canonical_plan'->>'ordered_recording_ids_sha256' IS DISTINCT FROM
           recording_joined_cohort_ids_sha256(OLD.definition_jsonb->>'batch_id') OR
         OLD.definition_jsonb->'canonical_plan'->>'qualification_rule_version' IS DISTINCT FROM OLD.definition_version OR
         jsonb_typeof(OLD.definition_jsonb->'canonical_plan'->'members') IS DISTINCT FROM 'array' OR
         jsonb_array_length(OLD.definition_jsonb->'canonical_plan'->'members')<>cardinality(recording_joined_cohort_ids(OLD.definition_jsonb->>'batch_id')) OR
         jsonb_typeof(OLD.definition_jsonb->'recording_jobs') IS DISTINCT FROM 'array' OR
         jsonb_array_length(OLD.definition_jsonb->'recording_jobs')<>cardinality(recording_joined_cohort_ids(OLD.definition_jsonb->>'batch_id')) OR
         ARRAY(SELECT m.recording_id FROM recording_qualification_members m WHERE m.run_id=OLD.id ORDER BY m.ordinal)
           IS DISTINCT FROM recording_joined_cohort_ids(OLD.definition_jsonb->>'batch_id') OR
         EXISTS(SELECT 1 FROM recording_qualification_members m WHERE m.run_id=OLD.id AND
           (m.scene_identity_sha256 IS NOT NULL OR m.scene_frame_evidence_id IS NOT NULL OR
            m.window_generator_version<>'historical-explicit-jobs-v1')) OR
         EXISTS(SELECT 1 FROM jsonb_array_elements(OLD.definition_jsonb->'recording_jobs') WITH ORDINALITY e(item,ord)
           WHERE jsonb_typeof(item) IS DISTINCT FROM 'object' OR
             (item->>'recording_id')::BIGINT IS DISTINCT FROM
               (recording_joined_cohort_ids(OLD.definition_jsonb->>'batch_id'))[ord] OR
             (item->'recording_id')::TEXT IS DISTINCT FROM (item->>'recording_id')::BIGINT::TEXT OR
             jsonb_typeof(item->'job_ids') IS DISTINCT FROM 'array' OR jsonb_array_length(item->'job_ids')<>14) OR
         EXISTS(SELECT 1 FROM jsonb_array_elements(OLD.definition_jsonb->'recording_jobs') entries(item)
           CROSS JOIN LATERAL jsonb_array_elements(entries.item->'job_ids') jobs(job_id)
           WHERE job_id::TEXT IS DISTINCT FROM (job_id#>>'{}')::BIGINT::TEXT) OR
         (SELECT count(DISTINCT jobs.job_id::BIGINT)
            FROM jsonb_array_elements(OLD.definition_jsonb->'recording_jobs') AS entries(item)
            CROSS JOIN LATERAL jsonb_array_elements_text(entries.item->'job_ids') AS jobs(job_id))<>14*cardinality(recording_joined_cohort_ids(OLD.definition_jsonb->>'batch_id')) THEN
        RAISE EXCEPTION 'historical qualification authority differs';
      END IF;
      IF OLD.definition_jsonb->>'batch_id'='goodplus-20260911-generation-1' AND EXISTS(
        SELECT 1 FROM recording_qualification_members m
        JOIN recording_qualification_windows w ON w.run_id=m.run_id AND w.recording_id=m.recording_id
        WHERE m.run_id=OLD.id AND w.local_open_at::DATE IS DISTINCT FROM
          (ARRAY['2026-08-26','2026-08-27','2026-08-15','2026-08-07','2026-08-13',
            '2026-08-08','2026-08-07','2026-08-08','2026-08-12']::DATE[])[m.ordinal]+w.ordinal-1
      ) THEN RAISE EXCEPTION 'additive qualification best-window authority differs'; END IF;
      -- One-time activation locks the exact imported raw facts while copying them into the
      -- immutable plan. Later recording work remains completely trigger-free.
      FOR authoritative IN
        SELECT j.id
        FROM recording_qualification_members m
        JOIN recording_qualification_windows w ON w.run_id=m.run_id AND w.recording_id=m.recording_id
        JOIN LATERAL (
          SELECT (e.item->'job_ids'->>(w.ordinal-1))::BIGINT job_id
          FROM jsonb_array_elements(OLD.definition_jsonb->'recording_jobs') WITH ORDINALITY e(item,ord)
          WHERE e.ord=m.ordinal AND (e.item->>'recording_id')::BIGINT=m.recording_id
        ) selected ON TRUE
        JOIN recording_jobs j ON j.id=selected.job_id
        WHERE m.run_id=OLD.id
        ORDER BY j.id
        FOR SHARE OF j
      LOOP
        NULL;
      END LOOP;
      SELECT count(*)::int INTO invalid_count
      FROM recording_qualification_members m
      JOIN recording_qualification_windows w ON w.run_id=m.run_id AND w.recording_id=m.recording_id
      LEFT JOIN LATERAL (
        SELECT (e.item->'job_ids'->>(w.ordinal-1))::BIGINT job_id
        FROM jsonb_array_elements(OLD.definition_jsonb->'recording_jobs') WITH ORDINALITY e(item,ord)
        WHERE e.ord=m.ordinal AND (e.item->>'recording_id')::BIGINT=m.recording_id
      ) selected ON TRUE
      LEFT JOIN LATERAL (
        SELECT OLD.definition_jsonb->'canonical_plan'->'members'->(m.ordinal-1) AS member,
          OLD.definition_jsonb->'canonical_plan'->'members'->(m.ordinal-1)->'qualification'->'days'->(w.ordinal-1) AS day
      ) imported ON TRUE
      LEFT JOIN recording_jobs j ON j.id=selected.job_id
      WHERE m.run_id=OLD.id AND (
        selected.job_id IS NULL OR imported.member IS NULL OR imported.day IS NULL OR
        (ARRAY(SELECT key FROM jsonb_object_keys(imported.member) AS keys(key) ORDER BY key COLLATE "C")
          IS DISTINCT FROM ARRAY['active_weekdays','qualification','recording_id','recording_name',
            'schedule_start_at','stream_id','stream_name','timezone']::TEXT[] AND
         ARRAY(SELECT key FROM jsonb_object_keys(imported.member) AS keys(key) ORDER BY key COLLATE "C")
          IS DISTINCT FROM ARRAY['active_weekdays','qualification','recording_id','recording_name','schedule_end_at',
            'schedule_start_at','stream_id','stream_name','timezone']::TEXT[]) OR
        ARRAY(SELECT key FROM jsonb_object_keys(imported.member->'qualification') AS keys(key) ORDER BY key COLLATE "C")
          IS DISTINCT FROM ARRAY['authority_kind','days','evidence_sha256','frozen_at','recording_id','timezone']::TEXT[] OR
        jsonb_typeof(imported.member->'qualification'->'days') IS DISTINCT FROM 'array' OR
        jsonb_array_length(imported.member->'qualification'->'days')<>14 OR
        ARRAY(SELECT key FROM jsonb_object_keys(imported.day) AS keys(key) ORDER BY key COLLATE "C")
          IS DISTINCT FROM ARRAY['completed_at','job_id','job_status','local_date','qualification_window_ordinal',
            'reason_codes','scheduled_for','window_end','window_start']::TEXT[] OR
        (imported.member->>'recording_id')::BIGINT IS DISTINCT FROM m.recording_id OR
        (imported.member->'recording_id')::TEXT IS DISTINCT FROM m.recording_id::TEXT OR
        (imported.member->>'stream_id')::BIGINT IS DISTINCT FROM m.stream_id OR
        (imported.member->'stream_id')::TEXT IS DISTINCT FROM m.stream_id::TEXT OR
        imported.member->>'recording_name' IS DISTINCT FROM m.recording_name OR
        imported.member->>'stream_name' IS DISTINCT FROM m.stream_name OR
        imported.member->>'timezone' IS DISTINCT FROM m.cron_timezone OR
        (imported.member->>'schedule_start_at')::TIMESTAMPTZ IS DISTINCT FROM m.schedule_start_at OR
        (imported.member->'schedule_start_at')::TEXT IS DISTINCT FROM
          recording_historical_go_utc_time_json(imported.member->'schedule_start_at') OR
        (imported.member->>'schedule_end_at')::TIMESTAMPTZ IS DISTINCT FROM m.schedule_end_at OR
        (imported.member ? 'schedule_end_at' AND (imported.member->'schedule_end_at')::TEXT IS DISTINCT FROM
          recording_historical_go_utc_time_json(imported.member->'schedule_end_at')) OR
        (imported.member->>'active_weekdays')::SMALLINT IS DISTINCT FROM m.active_weekdays OR
        (imported.member->'active_weekdays')::TEXT IS DISTINCT FROM m.active_weekdays::TEXT OR
        (imported.member->'qualification'->>'recording_id')::BIGINT IS DISTINCT FROM m.recording_id OR
        (imported.member->'qualification'->'recording_id')::TEXT IS DISTINCT FROM m.recording_id::TEXT OR
        imported.member->'qualification'->>'timezone' IS DISTINCT FROM m.cron_timezone OR
        imported.member->'qualification'->>'authority_kind' IS DISTINCT FROM 'historical_operator_import_v1' OR
        imported.member->'qualification'->'frozen_at' IS DISTINCT FROM recording_joined_cohort_cutoff_json(OLD.definition_jsonb->>'batch_id') OR
        COALESCE(imported.member->'qualification'->>'evidence_sha256','') !~ '^[0-9a-f]{64}$' OR
        recording_historical_qualification_evidence_sha256(imported.member->'qualification')
          IS DISTINCT FROM imported.member->'qualification'->>'evidence_sha256' OR
        (imported.day->>'qualification_window_ordinal')::INTEGER IS DISTINCT FROM w.ordinal OR
        (imported.day->'qualification_window_ordinal')::TEXT IS DISTINCT FROM w.ordinal::TEXT OR
        imported.day->>'local_date' IS DISTINCT FROM to_char(w.local_open_at,'YYYY-MM-DD') OR
        (imported.day->>'job_id')::BIGINT IS DISTINCT FROM selected.job_id OR
        (imported.day->'job_id')::TEXT IS DISTINCT FROM selected.job_id::TEXT OR
        (imported.day->'scheduled_for')::TEXT IS DISTINCT FROM
          recording_historical_go_utc_time_json(imported.day->'scheduled_for') OR
        (imported.day->>'scheduled_for')::TIMESTAMPTZ IS DISTINCT FROM j.scheduled_for OR
        imported.day->>'job_status' IS DISTINCT FROM j.status OR
        (imported.day->>'window_start')::TIMESTAMPTZ IS DISTINCT FROM w.window_start_at OR
        (imported.day->'window_start')::TEXT IS DISTINCT FROM
          recording_historical_go_utc_time_json(imported.day->'window_start') OR
        (imported.day->>'window_end')::TIMESTAMPTZ IS DISTINCT FROM w.window_end_at OR
        (imported.day->'window_end')::TEXT IS DISTINCT FROM
          recording_historical_go_utc_time_json(imported.day->'window_end') OR
        (imported.day->>'completed_at')::TIMESTAMPTZ IS DISTINCT FROM j.completed_at OR
        (imported.day->'completed_at')::TEXT IS DISTINCT FROM
          recording_historical_go_utc_time_json(imported.day->'completed_at') OR
        imported.day->'reason_codes' IS DISTINCT FROM to_jsonb(array_remove(ARRAY[
          CASE WHEN j.scheduled_for<>j.fire_at THEN 'scheduled_for_drift' END,
          CASE WHEN j.status='error' THEN 'terminal_job_error' END]::TEXT[],NULL)) OR
        j.id IS NULL OR j.recording_id<>m.recording_id OR j.kind<>'continuous_window' OR
        j.status NOT IN('done','error') OR j.completed_at IS NULL OR j.fire_at<>w.window_start_at OR
        j.window_end_at<>w.window_end_at OR j.completed_at>(recording_joined_cohort_cutoff_json(OLD.definition_jsonb->>'batch_id')#>>'{}')::TIMESTAMPTZ OR
        j.completed_at<j.fire_at OR
        (j.status='done' AND j.completed_at<j.window_end_at) OR
        (j.status='error' AND NOT ((m.recording_id=348 AND w.local_open_at::DATE='2026-07-29'::DATE) OR
          (m.recording_id IN(408,406,409) AND w.local_open_at::DATE='2026-08-11'::DATE))) OR
        w.local_open_at<>w.window_start_at AT TIME ZONE m.cron_timezone OR
        w.local_end_at<>w.window_end_at AT TIME ZONE m.cron_timezone OR
        w.local_open_at::TIME<>'08:00'::TIME OR w.local_end_at::TIME<>'20:00'::TIME OR
        w.expected_seconds<>43200 OR
        (w.ordinal>1 AND w.local_open_at::DATE<>(SELECT prior.local_open_at::DATE+1 FROM recording_qualification_windows prior
          WHERE prior.run_id=w.run_id AND prior.recording_id=w.recording_id AND prior.ordinal=w.ordinal-1))
      );
    ELSE
      SELECT count(*)::int INTO invalid_count
      FROM recording_qualification_members m
      LEFT JOIN recording_scene_frame_evidence e ON e.id=m.scene_frame_evidence_id
      LEFT JOIN LATERAL (
        SELECT count(*) n,min(ordinal) lo,max(ordinal) hi,
               bool_and(window_start_at>=OLD.window_sequence_start_at) starts_after_cutoff,
               bool_and(local_open_at=window_start_at AT TIME ZONE m.cron_timezone) opens_match,
               bool_and(local_end_at=window_end_at AT TIME ZONE m.cron_timezone) ends_match,
               bool_and(open_utc_offset_seconds=EXTRACT(EPOCH FROM (local_open_at-(window_start_at AT TIME ZONE 'UTC')))::int) open_offsets_match,
               bool_and(end_utc_offset_seconds=EXTRACT(EPOCH FROM (local_end_at-(window_end_at AT TIME ZONE 'UTC')))::int) end_offsets_match,
               bool_and(window_start_at>=m.schedule_start_at AND (m.schedule_end_at IS NULL OR window_end_at<=m.schedule_end_at)) envelope_match
        FROM recording_qualification_windows w WHERE w.run_id=m.run_id AND w.recording_id=m.recording_id
      ) c ON true
      WHERE m.run_id=OLD.id AND (
        m.scene_identity_sha256 IS NULL OR m.scene_frame_evidence_id IS NULL OR
        e.id IS NULL OR e.account_id<>m.account_id OR e.stream_id<>m.stream_id OR e.scene_identity_sha256<>m.scene_identity_sha256 OR
        m.window_generator_version<>'recsched-next-full-v1' OR
        e.verified_at<now()-interval '24 hours' OR e.verified_at>now()+interval '5 minutes' OR
        c.n<>14 OR c.lo<>1 OR c.hi<>14 OR NOT c.starts_after_cutoff OR NOT c.opens_match OR NOT c.ends_match OR
        NOT c.open_offsets_match OR NOT c.end_offsets_match OR NOT c.envelope_match
      );
    END IF;
    IF invalid_count<>0 THEN RAISE EXCEPTION 'qualification evidence or window set is invalid'; END IF;

    UPDATE recording_qualification_members m SET
      schedule_config_sha256=encode(sha256(convert_to(jsonb_build_object(
        'cron_timezone',m.cron_timezone,'daily_window_start',m.daily_window_start,
        'daily_window_end',m.daily_window_end,'active_weekdays',m.active_weekdays,
        'schedule_start_epoch',EXTRACT(EPOCH FROM m.schedule_start_at),
        'schedule_end_epoch',EXTRACT(EPOCH FROM m.schedule_end_at)
      )::text,'UTF8')),'hex'),
      window_sequence_sha256=encode(sha256(convert_to((
        SELECT jsonb_agg(jsonb_build_object(
          'ordinal',w.ordinal,'local_open_at',w.local_open_at,'local_end_at',w.local_end_at,
          'open_offset',w.open_utc_offset_seconds,'end_offset',w.end_utc_offset_seconds,
          'window_start_epoch',EXTRACT(EPOCH FROM w.window_start_at),
          'window_end_epoch',EXTRACT(EPOCH FROM w.window_end_at),'expected_seconds',w.expected_seconds
        ) ORDER BY w.ordinal)::text
        FROM recording_qualification_windows w
        WHERE w.run_id=m.run_id AND w.recording_id=m.recording_id
      ),'UTF8')),'hex')
    WHERE m.run_id=OLD.id;

    SELECT jsonb_agg(jsonb_build_object(
      'ordinal',m.ordinal,'recording_id',m.recording_id,'stream_id',m.stream_id,
      'recording_name',m.recording_name,'stream_name',m.stream_name,
      'scene_identity_sha256',m.scene_identity_sha256,'scene_evidence_id',m.scene_frame_evidence_id,
      'cron_timezone',m.cron_timezone,'daily_window_start',m.daily_window_start,
      'daily_window_end',m.daily_window_end,'active_weekdays',m.active_weekdays,
      'schedule_start_epoch',EXTRACT(EPOCH FROM m.schedule_start_at),
      'schedule_end_epoch',EXTRACT(EPOCH FROM m.schedule_end_at),
      'window_generator_version',m.window_generator_version,
      'schedule_config_sha256',m.schedule_config_sha256,'window_sequence_sha256',m.window_sequence_sha256
    ) ORDER BY m.ordinal) INTO member_json
    FROM recording_qualification_members m WHERE m.run_id=OLD.id;
    SELECT jsonb_agg(jsonb_build_object(
      'recording_id',w.recording_id,'ordinal',w.ordinal,'local_open_at',w.local_open_at,
      'local_end_at',w.local_end_at,'open_offset',w.open_utc_offset_seconds,'end_offset',w.end_utc_offset_seconds,
      'window_start_epoch',EXTRACT(EPOCH FROM w.window_start_at),'window_end_epoch',EXTRACT(EPOCH FROM w.window_end_at),
      'expected_seconds',w.expected_seconds
    ) ORDER BY m.ordinal,w.ordinal) INTO window_json
    FROM recording_qualification_windows w
    JOIN recording_qualification_members m ON m.run_id=w.run_id AND m.recording_id=w.recording_id
    WHERE w.run_id=OLD.id;
    definition_sha:=encode(sha256(convert_to(OLD.definition_jsonb::text,'UTF8')),'hex');
    window_sha:=encode(sha256(convert_to(window_json::text,'UTF8')),'hex');
    NEW.definition_sha256:=definition_sha;
    NEW.windows_sha256:=window_sha;
    NEW.cohort_sha256:=encode(sha256(convert_to(jsonb_build_object(
      'account_id',OLD.account_id,'definition_version',OLD.definition_version,
      'definition_sha256',definition_sha,'members',member_json,'windows_sha256',window_sha
    )::text,'UTF8')),'hex');
    SELECT max(window_end_at) INTO NEW.qualification_due_at FROM recording_qualification_windows WHERE run_id=OLD.id;
    NEW.frozen_at:=now();
    NEW.canceled_at:=NULL;
    RETURN NEW;
  END IF;
  IF OLD.status IN ('building','active') AND NEW.status='canceled' THEN
    IF NEW.account_id IS DISTINCT FROM OLD.account_id OR NEW.definition_version IS DISTINCT FROM OLD.definition_version OR
       NEW.definition_jsonb IS DISTINCT FROM OLD.definition_jsonb OR NEW.definition_sha256 IS DISTINCT FROM OLD.definition_sha256 OR
       NEW.cohort_sha256 IS DISTINCT FROM OLD.cohort_sha256 OR NEW.windows_sha256 IS DISTINCT FROM OLD.windows_sha256 OR
       NEW.target_recording_count IS DISTINCT FROM OLD.target_recording_count OR NEW.target_window_count IS DISTINCT FROM OLD.target_window_count OR
       NEW.required_good_or_great IS DISTINCT FROM OLD.required_good_or_great OR NEW.max_acceptable IS DISTINCT FROM OLD.max_acceptable OR
       NEW.window_sequence_start_at IS DISTINCT FROM OLD.window_sequence_start_at OR NEW.qualification_due_at IS DISTINCT FROM OLD.qualification_due_at OR
       NEW.created_at IS DISTINCT FROM OLD.created_at OR NEW.frozen_at IS DISTINCT FROM OLD.frozen_at THEN
      RAISE EXCEPTION 'cancel cannot rewrite qualification definition';
    END IF;
    NEW.canceled_at:=now();
    RETURN NEW;
  END IF;
  IF NEW IS DISTINCT FROM OLD THEN RAISE EXCEPTION 'invalid qualification lifecycle transition'; END IF;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION validate_recording_joined_batch_snapshot() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE batch recording_joined_batches%ROWTYPE; request JSONB;
BEGIN
  SELECT * INTO STRICT batch FROM recording_joined_batches WHERE id=NEW.id FOR UPDATE;
  request := convert_from(batch.freeze_request_bytes,'UTF8')::JSONB;
  IF batch.state<>'building' OR batch.freeze_started_at IS NOT NULL
    OR NOT EXISTS(SELECT 1 FROM connections c WHERE c.id=batch.connection_id AND c.account_id=batch.account_id
      AND c.joined_protocol_version=1 FOR UPDATE)
    OR NOT EXISTS(SELECT 1 FROM recording_qualification_runs q
      WHERE q.id=batch.qualification_run_id AND q.account_id=batch.account_id AND q.status='active'
        AND q.cohort_sha256=batch.qualification_cohort_sha256 AND q.windows_sha256=batch.qualification_windows_sha256
        AND q.frozen_at=batch.qualification_frozen_at FOR SHARE)
    OR (SELECT count(*) FROM recording_joined_batch_recordings br WHERE br.batch_record_id=batch.id)<>batch.expected_recordings
    OR ARRAY(SELECT br.recording_id FROM recording_joined_batch_recordings br WHERE br.batch_record_id=batch.id
      ORDER BY br.priority_ordinal) IS DISTINCT FROM recording_joined_cohort_ids(batch.batch_id)
    OR (SELECT count(*) FROM recording_joined_stream_days d WHERE d.batch_record_id=batch.id)<>batch.expected_stream_days
    OR (SELECT count(*) FROM recording_joined_stream_days d WHERE d.batch_record_id=batch.id AND d.state='pending')<>batch.expected_stream_days
    OR (SELECT count(*) FROM recording_joined_source_snapshots s WHERE s.batch_record_id=batch.id)<>batch.expected_source_clips
    OR (SELECT COALESCE(sum(size_bytes),0) FROM recording_joined_source_snapshots s WHERE s.batch_record_id=batch.id)<>batch.expected_source_bytes
    OR (SELECT count(*) FROM recording_joined_freeze_exclusions e WHERE e.batch_record_id=batch.id)<>batch.expected_freeze_exclusions
    OR (SELECT encode(sha256(convert_to(COALESCE(string_agg(format('%s\n%s\n%s\n%s\n',e.recording_id,
        COALESCE(e.clip_id::TEXT,''),e.reason_code,e.evidence_sha256),'' ORDER BY e.recording_id,e.clip_id,
        e.reason_code,e.evidence_sha256),''),'UTF8')),'hex') FROM recording_joined_freeze_exclusions e
      WHERE e.batch_record_id=batch.id)<>batch.freeze_exclusions_sha256
    OR EXISTS(SELECT 1 FROM recording_joined_batch_recordings br
      JOIN recording_qualification_runs q ON q.id=br.qualification_run_id AND q.account_id=batch.account_id
      LEFT JOIN LATERAL (SELECT count(*) AS days FROM recording_joined_stream_days d WHERE d.batch_recording_id=br.id) actual ON TRUE
      LEFT JOIN LATERAL (SELECT q.definition_jsonb->'canonical_plan'->'members'->(br.priority_ordinal-1) AS member) imported ON TRUE
      WHERE br.batch_record_id=batch.id AND (br.qualification_run_id IS DISTINCT FROM batch.qualification_run_id
        OR br.selection_tier IS DISTINCT FROM 'good+' OR br.qualification_policy_version IS DISTINCT FROM q.definition_version
        OR br.priority_ordinal IS DISTINCT FROM (SELECT count(*) FROM recording_joined_batch_recordings earlier
          WHERE earlier.batch_record_id=br.batch_record_id AND earlier.priority_ordinal<=br.priority_ordinal)
        OR br.first_local_date IS DISTINCT FROM (SELECT w.local_open_at::DATE FROM recording_qualification_windows w
          WHERE w.run_id=br.qualification_run_id AND w.recording_id=br.recording_id AND w.ordinal=1)
        OR br.last_local_date IS DISTINCT FROM (SELECT w.local_open_at::DATE FROM recording_qualification_windows w
          WHERE w.run_id=br.qualification_run_id AND w.recording_id=br.recording_id AND w.ordinal=14)
        OR br.completed_at>batch.eligibility_cutoff OR br.completed_at IS DISTINCT FROM
          (SELECT max(d.completed_at) FROM recording_joined_stream_days d WHERE d.batch_recording_id=br.id)
        OR actual.days IS DISTINCT FROM 14 OR br.authoritative_job_ids IS DISTINCT FROM ARRAY(
          SELECT d.recording_job_id FROM recording_joined_stream_days d WHERE d.batch_recording_id=br.id ORDER BY d.date_ordinal)
        OR (q.definition_version='recording-qualification-tier1-historical-import-v1' AND (
          imported.member IS NULL OR (imported.member->>'recording_id')::BIGINT IS DISTINCT FROM br.recording_id OR
          imported.member->'qualification' IS DISTINCT FROM br.qualification))))
    OR jsonb_typeof(request->'recordings') IS DISTINCT FROM 'array'
    OR jsonb_array_length(request->'recordings') IS DISTINCT FROM batch.expected_recordings
    OR EXISTS(SELECT 1 FROM recording_joined_batch_recordings br
      CROSS JOIN LATERAL (SELECT request->'recordings'->(br.priority_ordinal-1) item) frozen
      WHERE br.batch_record_id=batch.id AND (
        ARRAY(SELECT key FROM jsonb_object_keys(frozen.item) AS object_keys(key) ORDER BY key COLLATE "C") IS DISTINCT FROM
          ARRAY['expected_exclusions','expected_exclusions_sha256','expected_source_bytes','expected_source_clips','frozen_recording','qualification','snapshot_days']::TEXT[]
        OR ARRAY(SELECT key FROM jsonb_object_keys(frozen.item->'frozen_recording') AS object_keys(key) ORDER BY key COLLATE "C") IS DISTINCT FROM
          ARRAY['completed_at','folder_name','naming_metadata','priority_ordinal','qualification_sha256','recording_id','selection_tier','timezone']::TEXT[]
        OR (frozen.item->'frozen_recording'->>'recording_id')::BIGINT IS DISTINCT FROM br.recording_id
        OR (frozen.item->'frozen_recording'->>'priority_ordinal')::INTEGER IS DISTINCT FROM br.priority_ordinal
        OR frozen.item->'frozen_recording'->>'selection_tier' IS DISTINCT FROM br.selection_tier
        OR frozen.item->'frozen_recording'->>'qualification_sha256' IS DISTINCT FROM br.qualification_sha256
        OR (frozen.item->'frozen_recording'->>'completed_at')::TIMESTAMPTZ IS DISTINCT FROM br.completed_at
        OR frozen.item->'frozen_recording'->>'timezone' IS DISTINCT FROM br.timezone
        OR frozen.item->'frozen_recording'->>'folder_name' IS DISTINCT FROM br.folder_name
        OR frozen.item->'frozen_recording'->'naming_metadata' IS DISTINCT FROM br.naming_metadata
        OR frozen.item->'qualification' IS DISTINCT FROM br.qualification))
    OR EXISTS(SELECT 1 FROM recording_joined_stream_days d
      JOIN recording_joined_batch_recordings br ON br.id=d.batch_recording_id
      JOIN recording_qualification_runs q ON q.id=br.qualification_run_id
      JOIN recording_qualification_windows w ON w.run_id=br.qualification_run_id AND w.recording_id=br.recording_id
        AND w.ordinal=d.date_ordinal
      LEFT JOIN LATERAL (SELECT br.qualification->'days'->(d.date_ordinal-1) AS day) imported ON TRUE
      LEFT JOIN LATERAL (SELECT count(*) AS clips,COALESCE(sum(size_bytes),0) AS bytes
        FROM recording_joined_source_snapshots s WHERE s.stream_day_id=d.id) actual ON TRUE
      WHERE d.batch_record_id=batch.id AND (actual.clips<>d.source_clip_count OR actual.bytes<>d.source_bytes
        OR d.local_date<>br.first_local_date+d.date_ordinal-1
        OR ROW(d.scheduled_start_at,d.scheduled_end_at) IS DISTINCT FROM ROW(w.window_start_at,w.window_end_at)
        OR d.completed_at>batch.eligibility_cutoff
        OR (br.qualification_policy_version<>'recording-qualification-tier1-historical-import-v1'
          AND (d.completed_at<d.scheduled_end_at OR batch.qualification_frozen_at>d.scheduled_start_at))
        OR (br.qualification_policy_version='recording-qualification-tier1-historical-import-v1'
          AND (d.completed_at<d.scheduled_start_at OR batch.qualification_frozen_at<=batch.eligibility_cutoff OR
            imported.day IS NULL OR
            (imported.day->>'qualification_window_ordinal')::INTEGER IS DISTINCT FROM d.date_ordinal OR
            imported.day->>'local_date' IS DISTINCT FROM d.local_date::TEXT OR
            (imported.day->>'job_id')::BIGINT IS DISTINCT FROM d.recording_job_id OR
            ROW((imported.day->>'window_start')::TIMESTAMPTZ,(imported.day->>'window_end')::TIMESTAMPTZ,
              (imported.day->>'completed_at')::TIMESTAMPTZ) IS DISTINCT FROM
              ROW(d.scheduled_start_at,d.scheduled_end_at,d.completed_at)))
        OR EXISTS(SELECT 1 FROM (SELECT s.day_ordinal,row_number() OVER (ORDER BY s.start_at,s.clip_id) AS expected_ordinal
          FROM recording_joined_source_snapshots s WHERE s.stream_day_id=d.id) ordered
          WHERE ordered.day_ordinal<>ordered.expected_ordinal)))
    OR EXISTS(SELECT 1 FROM recording_joined_hours h WHERE h.batch_record_id=batch.id)
    OR EXISTS(SELECT 1 FROM recording_joined_sources s WHERE s.batch_record_id=batch.id)
  THEN RAISE EXCEPTION 'joined building batch snapshot is incomplete'; END IF;
  RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION guard_recording_joined_batch_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE request JSONB;
BEGIN
  IF current_setting('transaction_isolation') <> 'read committed'
  THEN RAISE EXCEPTION 'joined snapshot apply requires read committed'; END IF;
  PERFORM pg_advisory_xact_lock(137,1);
  request := convert_from(NEW.freeze_request_bytes,'UTF8')::JSONB;
  IF NEW.state<>'snapshotting' OR NEW.freeze_started_at IS NOT NULL OR NEW.frozen_at IS NOT NULL
    OR ARRAY(SELECT key FROM jsonb_object_keys(request) object_keys(key) ORDER BY key COLLATE "C") IS DISTINCT FROM ARRAY[
      'account_id','batch_id','connection_id','expected_scheduled_hours','expected_stream_days','freeze_exclusions_sha256',
      'frozen_denominator_sha256','generation','media_tool','policy_version','provisional_exclusions',
      'provisional_source_bytes','provisional_source_clips','qualification_jobs_sha256','recording_ids','recordings',
      'schema_version','selection_authority','source_endpoint']::TEXT[]
    OR (request->>'schema_version')::INTEGER IS DISTINCT FROM 2
    OR request->>'batch_id' IS DISTINCT FROM NEW.batch_id
    OR (request->>'generation')::INTEGER IS DISTINCT FROM NEW.generation
    OR (request->>'account_id')::BIGINT IS DISTINCT FROM NEW.account_id
    OR (request->>'connection_id')::BIGINT IS DISTINCT FROM NEW.connection_id
    OR request->>'source_endpoint' IS DISTINCT FROM NEW.source_endpoint
    OR request->>'policy_version' IS DISTINCT FROM NEW.policy_version
    OR request->'media_tool' IS DISTINCT FROM NEW.media_tool
    OR request->'media_tool'->>'identity_sha256' IS DISTINCT FROM NEW.media_tool_sha256
    OR request->>'qualification_jobs_sha256' IS DISTINCT FROM NEW.qualification_jobs_sha256
    OR request->>'frozen_denominator_sha256' IS DISTINCT FROM NEW.frozen_denominator_sha256
    OR request->>'freeze_exclusions_sha256' IS DISTINCT FROM NEW.freeze_exclusions_sha256
    OR (request->>'expected_stream_days')::INTEGER IS DISTINCT FROM NEW.expected_stream_days
    OR (request->>'expected_scheduled_hours')::INTEGER IS DISTINCT FROM NEW.expected_scheduled_hours
    OR (request->>'provisional_source_clips')::BIGINT IS DISTINCT FROM NEW.expected_source_clips
    OR (request->>'provisional_source_bytes')::BIGINT IS DISTINCT FROM NEW.expected_source_bytes
    OR (request->>'provisional_exclusions')::BIGINT IS DISTINCT FROM NEW.expected_freeze_exclusions
    OR jsonb_array_length(request->'recordings') IS DISTINCT FROM cardinality(recording_joined_cohort_ids(NEW.batch_id))
    OR request->'recording_ids' IS DISTINCT FROM to_jsonb(recording_joined_cohort_ids(NEW.batch_id))
    OR request->'selection_authority'->>'selection_basis' IS DISTINCT FROM NEW.selection_basis
    OR ARRAY(SELECT key FROM jsonb_object_keys(request->'selection_authority') keys(key) ORDER BY key COLLATE "C")
      IS DISTINCT FROM ARRAY['cutoff','ordered_recording_ids_sha256','qualification_cohort_sha256',
        'qualification_rule_version','qualification_run_frozen_at','qualification_run_id',
        'qualification_windows_sha256','selected_qualification_windows_sha256','selection_basis']::TEXT[]
    OR request->'selection_authority'->>'ordered_recording_ids_sha256' IS DISTINCT FROM NEW.ordered_recording_ids_sha256
    OR (request->'selection_authority'->>'cutoff')::TIMESTAMPTZ IS DISTINCT FROM NEW.eligibility_cutoff
    OR (request->'selection_authority'->>'qualification_run_id')::BIGINT IS DISTINCT FROM NEW.qualification_run_id
    OR (request->'selection_authority'->>'qualification_run_frozen_at')::TIMESTAMPTZ IS DISTINCT FROM NEW.qualification_frozen_at
    OR request->'selection_authority'->>'qualification_cohort_sha256' IS DISTINCT FROM NEW.qualification_cohort_sha256
    OR request->'selection_authority'->>'qualification_windows_sha256' IS DISTINCT FROM NEW.qualification_windows_sha256
    OR request->'selection_authority'->>'selected_qualification_windows_sha256' IS DISTINCT FROM NEW.selected_qualification_windows_sha256
    OR request->'selection_authority'->>'qualification_rule_version' IS DISTINCT FROM
      (SELECT q.definition_version FROM recording_qualification_runs q WHERE q.id=NEW.qualification_run_id)
    OR EXISTS(SELECT 1 FROM jsonb_array_elements(request->'recordings') WITH ORDINALITY item(value,ordinal)
      WHERE ARRAY(SELECT key FROM jsonb_object_keys(value) keys(key) ORDER BY key COLLATE "C") IS DISTINCT FROM ARRAY[
          'expected_exclusions','expected_exclusions_sha256','expected_source_bytes','expected_source_clips',
          'frozen_recording','qualification','snapshot_days']::TEXT[]
        OR (value->'frozen_recording'->>'priority_ordinal')::INTEGER IS DISTINCT FROM ordinal
        OR ARRAY(SELECT key FROM jsonb_object_keys(value->'frozen_recording') keys(key) ORDER BY key COLLATE "C")
          IS DISTINCT FROM ARRAY['completed_at','folder_name','naming_metadata','priority_ordinal',
            'qualification_sha256','recording_id','selection_tier','timezone']::TEXT[]
        OR (value->'frozen_recording'->>'recording_id')::BIGINT IS DISTINCT FROM
          (request->'recording_ids'->>((ordinal-1)::INTEGER))::BIGINT
        OR jsonb_array_length(value->'snapshot_days') IS DISTINCT FROM 14
        OR EXISTS(SELECT 1 FROM jsonb_array_elements(value->'snapshot_days') WITH ORDINALITY day(fact,day_ordinal)
          WHERE ARRAY(SELECT key FROM jsonb_object_keys(fact) keys(key) ORDER BY key COLLATE "C") IS DISTINCT FROM ARRAY[
              'date_ordinal','expected_source_bytes','expected_source_clips','expected_source_sha256',
              'high_water_clip_id','local_date','recording_job_id']::TEXT[]
            OR (fact->>'date_ordinal')::INTEGER IS DISTINCT FROM day_ordinal))
    OR NOT EXISTS(SELECT 1 FROM connections c WHERE c.id=NEW.connection_id AND c.account_id=NEW.account_id
      AND c.joined_protocol_version=1 FOR UPDATE)
    OR NOT EXISTS(SELECT 1 FROM recording_qualification_runs q WHERE q.id=NEW.qualification_run_id
      AND q.account_id=NEW.account_id AND q.status='active' AND q.cohort_sha256=NEW.qualification_cohort_sha256
      AND q.windows_sha256=NEW.qualification_windows_sha256 AND q.frozen_at=NEW.qualification_frozen_at FOR SHARE)
  THEN RAISE EXCEPTION 'joined batch must enter an owned snapshotting state'; END IF;
  RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION guard_recording_joined_dry_run_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF ROW(NEW.id,NEW.account_id,NEW.connection_id,NEW.batch_id,NEW.generation,NEW.qualification_run_id,
      NEW.input_bytes,NEW.input_sha256,NEW.skeleton_bytes,NEW.skeleton_sha256,NEW.created_at)
    IS DISTINCT FROM ROW(OLD.id,OLD.account_id,OLD.connection_id,OLD.batch_id,OLD.generation,OLD.qualification_run_id,
      OLD.input_bytes,OLD.input_sha256,OLD.skeleton_bytes,OLD.skeleton_sha256,OLD.created_at)
    OR (OLD.state IN ('ready','invalidated') AND ROW(NEW.state,NEW.completed_recordings,NEW.final_plan_bytes,NEW.final_plan_sha256,NEW.ready_at,NEW.invalidated_at)
      IS DISTINCT FROM ROW(OLD.state,OLD.completed_recordings,OLD.final_plan_bytes,OLD.final_plan_sha256,OLD.ready_at,OLD.invalidated_at))
    OR (OLD.state='building' AND (
      (NEW.state='building' AND (NEW.completed_recordings NOT IN (OLD.completed_recordings,OLD.completed_recordings+1)
        OR NEW.invalidated_at IS NOT NULL))
      OR (NEW.state='ready' AND (ROW(OLD.completed_recordings,NEW.completed_recordings) IS DISTINCT FROM ROW((cardinality(recording_joined_cohort_ids(OLD.batch_id))-1)::SMALLINT,cardinality(recording_joined_cohort_ids(OLD.batch_id))::SMALLINT)
        OR NEW.invalidated_at IS NOT NULL))
      OR (NEW.state='ready' AND ((SELECT count(*) FROM recording_joined_dry_run_scopes s WHERE s.dry_run_id=OLD.id)<>14*cardinality(recording_joined_cohort_ids(OLD.batch_id))
        OR (SELECT count(*) FROM recording_joined_dry_run_recordings r WHERE r.dry_run_id=OLD.id)<>cardinality(recording_joined_cohort_ids(OLD.batch_id))
        OR EXISTS(SELECT 1 FROM recording_joined_dry_run_scopes s
          CROSS JOIN LATERAL (SELECT convert_from(NEW.final_plan_bytes,'UTF8')::jsonb->'recordings'->(s.priority_ordinal-1)
            ->'snapshot_days'->(s.date_ordinal-1) expected_day) expected
          WHERE s.dry_run_id=OLD.id AND ROW((expected.expected_day->>'recording_job_id')::BIGINT,
            (expected.expected_day->>'high_water_clip_id')::BIGINT,expected.expected_day->>'local_date',
            (expected.expected_day->>'date_ordinal')::INTEGER)
            IS DISTINCT FROM ROW(s.recording_job_id,s.high_water_clip_id,s.local_date::TEXT,s.date_ordinal))
        OR EXISTS(SELECT 1 FROM recording_joined_dry_run_recordings r
          CROSS JOIN LATERAL (SELECT convert_from(NEW.final_plan_bytes,'UTF8')::jsonb->'recordings'->(r.priority_ordinal-1) item) expected
            WHERE r.dry_run_id=OLD.id AND expected.item IS DISTINCT FROM convert_from(r.evidence_bytes,'UTF8')::jsonb->'recording')
        OR EXISTS(SELECT 1
          FROM jsonb_array_elements(convert_from(NEW.final_plan_bytes,'UTF8')::jsonb->'recordings') WITH ORDINALITY final(item,ord)
          WHERE (final.item->'frozen_recording') IS DISTINCT FROM
              (convert_from(OLD.skeleton_bytes,'UTF8')::jsonb->'recordings'->((final.ord-1)::INTEGER)->'frozen_recording')
            OR (final.item->'qualification') IS DISTINCT FROM
              (convert_from(OLD.skeleton_bytes,'UTF8')::jsonb->'recordings'->((final.ord-1)::INTEGER)->'qualification'))
        OR (convert_from(NEW.final_plan_bytes,'UTF8')::jsonb
          - ARRAY['recordings','provisional_source_clips','provisional_source_bytes','provisional_exclusions',
            'frozen_denominator_sha256','freeze_exclusions_sha256','request_sha256']::TEXT[])
          IS DISTINCT FROM (convert_from(OLD.skeleton_bytes,'UTF8')::jsonb
          - ARRAY['recordings','provisional_source_clips','provisional_source_bytes','provisional_exclusions',
            'frozen_denominator_sha256','freeze_exclusions_sha256','request_sha256']::TEXT[])))
      OR (NEW.state='invalidated' AND (NEW.completed_recordings<>OLD.completed_recordings OR NEW.invalidated_at IS NULL))
      OR NEW.state NOT IN ('building','ready','invalidated')))
  THEN RAISE EXCEPTION 'joined dry-run authority is immutable'; END IF;
  RETURN NEW;
END $$;

