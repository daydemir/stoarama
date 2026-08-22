ALTER TABLE recording_qualification_runs
  DROP CONSTRAINT recording_qualification_runs_target_recording_count_check;
ALTER TABLE recording_qualification_runs
  ADD CONSTRAINT recording_qualification_runs_target_recording_count_check CHECK (
    target_recording_count >= 50 OR
    (definition_version='recording-qualification-tier1-historical-import-v1' AND target_recording_count=33)
  );

ALTER TABLE recording_qualification_members
  ALTER COLUMN scene_identity_sha256 DROP NOT NULL,
  ALTER COLUMN scene_frame_evidence_id DROP NOT NULL;

-- The row trigger below retains the prospective completed-at-end invariant and
-- admits only the four version-gated historical terminal-error rows early.
ALTER TABLE recording_joined_stream_days
  DROP CONSTRAINT recording_joined_stream_days_check,
  ADD CONSTRAINT recording_joined_stream_days_check CHECK (
    scheduled_end_at > scheduled_start_at AND completed_at >= scheduled_start_at
  );

-- Reproduce the exact Go QualificationWindow wire bytes with evidence_sha256
-- cleared. This makes the inner evidence seal independently enforceable even
-- for a direct database activation; the outer operator request hash alone is
-- intentionally insufficient.
CREATE OR REPLACE FUNCTION recording_historical_go_utc_time_json(value JSONB)
RETURNS TEXT LANGUAGE SQL IMMUTABLE STRICT AS $$
  WITH parsed AS (SELECT (value#>>'{}')::TIMESTAMPTZ AT TIME ZONE 'UTC' AS moment)
  SELECT to_jsonb(to_char(moment,'YYYY-MM-DD"T"HH24:MI:SS')||
    CASE WHEN to_char(moment,'US')='000000' THEN '' ELSE '.'||rtrim(to_char(moment,'US'),'0') END||'Z')::TEXT
  FROM parsed;
$$;

CREATE OR REPLACE FUNCTION recording_historical_go_string_json(value TEXT)
RETURNS TEXT LANGUAGE SQL IMMUTABLE STRICT AS $$
  SELECT replace(replace(replace(replace(replace(to_jsonb(value)::TEXT,
    '<',E'\\u003c'),'>',E'\\u003e'),'&',E'\\u0026'),chr(8232),E'\\u2028'),chr(8233),E'\\u2029');
$$;

CREATE OR REPLACE FUNCTION recording_historical_qualification_canonical(qualification JSONB, clear_evidence BOOLEAN)
RETURNS TEXT LANGUAGE SQL IMMUTABLE STRICT AS $$
  SELECT
    '{"recording_id":'||(qualification->>'recording_id')::BIGINT::TEXT||
    ',"timezone":'||recording_historical_go_string_json(qualification->>'timezone')||
    ',"days":['||COALESCE((
      SELECT string_agg(
        '{"local_date":'||recording_historical_go_string_json(day->>'local_date')||
        ',"qualification_window_ordinal":'||(day->>'qualification_window_ordinal')::INTEGER::TEXT||
        ',"job_id":'||(day->>'job_id')::BIGINT::TEXT||
        ',"scheduled_for":'||recording_historical_go_utc_time_json(day->'scheduled_for')||
        ',"job_status":'||recording_historical_go_string_json(day->>'job_status')||
        ',"reason_codes":['||COALESCE((SELECT string_agg(recording_historical_go_string_json(reason),',' ORDER BY reason_ordinal)
          FROM jsonb_array_elements_text(day->'reason_codes') WITH ORDINALITY reasons(reason,reason_ordinal)),'')||']'||
        ',"window_start":'||recording_historical_go_utc_time_json(day->'window_start')||
        ',"window_end":'||recording_historical_go_utc_time_json(day->'window_end')||
        ',"completed_at":'||recording_historical_go_utc_time_json(day->'completed_at')||'}',',' ORDER BY day_ordinal)
      FROM jsonb_array_elements(qualification->'days') WITH ORDINALITY days(day,day_ordinal)
    ),'')||']'||
    ',"frozen_at":'||recording_historical_go_utc_time_json(qualification->'frozen_at')||
    ',"authority_kind":'||recording_historical_go_string_json(qualification->>'authority_kind')||
    ',"evidence_sha256":'||CASE WHEN clear_evidence THEN '""' ELSE
      recording_historical_go_string_json(qualification->>'evidence_sha256') END||'}';
$$;

CREATE OR REPLACE FUNCTION recording_historical_qualification_evidence_sha256(qualification JSONB)
RETURNS TEXT LANGUAGE SQL IMMUTABLE STRICT AS $$
  SELECT encode(sha256(convert_to(recording_historical_qualification_canonical(qualification,TRUE),'UTF8')),'hex');
$$;

CREATE OR REPLACE FUNCTION recording_historical_qualification_jobs_canonical(recording_jobs JSONB)
RETURNS TEXT LANGUAGE SQL IMMUTABLE STRICT AS $$
  SELECT '['||COALESCE(string_agg(
    '{"recording_id":'||(entry->>'recording_id')::BIGINT::TEXT||
    ',"job_ids":['||COALESCE((SELECT string_agg((job#>>'{}')::BIGINT::TEXT,',' ORDER BY job_ordinal)
      FROM jsonb_array_elements(entry->'job_ids') WITH ORDINALITY jobs(job,job_ordinal)),'')||']}',
    ',' ORDER BY entry_ordinal),'')||']'
  FROM jsonb_array_elements(recording_jobs) WITH ORDINALITY entries(entry,entry_ordinal);
$$;

CREATE OR REPLACE FUNCTION recording_historical_request_canonical(plan JSONB)
RETURNS TEXT LANGUAGE SQL IMMUTABLE STRICT AS $$
  SELECT
    '{"schema_version":'||(plan->>'schema_version')::INTEGER::TEXT||
    ',"authority_kind":'||recording_historical_go_string_json(plan->>'authority_kind')||
    ',"batch_id":'||recording_historical_go_string_json(plan->>'batch_id')||
    ',"generation":'||(plan->>'generation')::INTEGER::TEXT||
    ',"account_id":'||(plan->>'account_id')::BIGINT::TEXT||
    ',"connection_id":'||(plan->>'connection_id')::BIGINT::TEXT||
    ',"cutoff":'||recording_historical_go_utc_time_json(plan->'cutoff')||
    ',"ordered_recording_ids_sha256":'||recording_historical_go_string_json(plan->>'ordered_recording_ids_sha256')||
    ',"qualification_rule_version":'||recording_historical_go_string_json(plan->>'qualification_rule_version')||
    ',"qualification_jobs_sha256":'||recording_historical_go_string_json(plan->>'qualification_jobs_sha256')||
    ',"members":['||COALESCE((
      SELECT string_agg(
        '{"recording_id":'||(member->>'recording_id')::BIGINT::TEXT||
        ',"stream_id":'||(member->>'stream_id')::BIGINT::TEXT||
        ',"recording_name":'||recording_historical_go_string_json(member->>'recording_name')||
        ',"stream_name":'||recording_historical_go_string_json(member->>'stream_name')||
        ',"timezone":'||recording_historical_go_string_json(member->>'timezone')||
        ',"schedule_start_at":'||recording_historical_go_utc_time_json(member->'schedule_start_at')||
        CASE WHEN member ? 'schedule_end_at' THEN
          ',"schedule_end_at":'||recording_historical_go_utc_time_json(member->'schedule_end_at') ELSE '' END||
        ',"active_weekdays":'||(member->>'active_weekdays')::SMALLINT::TEXT||
        ',"qualification":'||recording_historical_qualification_canonical(member->'qualification',FALSE)||'}',
        ',' ORDER BY member_ordinal)
      FROM jsonb_array_elements(plan->'members') WITH ORDINALITY members(member,member_ordinal)
    ),'')||']}';
$$;

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
       (historical AND member_count<>33) OR (NOT historical AND member_count<50) OR
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
         authoritative.authoritative_mode<>'continuous' OR authoritative.authoritative_status<>'active' OR
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
         OLD.definition_jsonb->>'batch_id' IS DISTINCT FROM 'goodplus-20260821-generation-1' OR
         (OLD.definition_jsonb->>'generation')::INTEGER IS DISTINCT FROM 1 OR
         (OLD.definition_jsonb->'generation')::TEXT IS DISTINCT FROM '1' OR
         OLD.definition_jsonb->'cutoff' IS DISTINCT FROM '"2026-08-21T06:59:07.534131Z"'::JSONB OR
         OLD.definition_jsonb->>'ordered_recording_ids_sha256' IS DISTINCT FROM '6038d4a23be9b0b5c2bb29ea933743a5ceb7f06b8875e417a3f16b44051ebd71' OR
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
         OLD.definition_jsonb->'canonical_plan'->>'batch_id' IS DISTINCT FROM 'goodplus-20260821-generation-1' OR
         (OLD.definition_jsonb->'canonical_plan'->>'generation')::INTEGER IS DISTINCT FROM 1 OR
         (OLD.definition_jsonb->'canonical_plan'->'generation')::TEXT IS DISTINCT FROM '1' OR
         (OLD.definition_jsonb->'canonical_plan'->>'account_id')::BIGINT IS DISTINCT FROM OLD.account_id OR
         (OLD.definition_jsonb->'canonical_plan'->'account_id')::TEXT IS DISTINCT FROM OLD.account_id::TEXT OR
         NOT EXISTS(SELECT 1 FROM connections c WHERE c.id=(OLD.definition_jsonb->'canonical_plan'->>'connection_id')::BIGINT
           AND c.account_id=OLD.account_id) OR
         (OLD.definition_jsonb->'canonical_plan'->'connection_id')::TEXT IS DISTINCT FROM
           (OLD.definition_jsonb->'canonical_plan'->>'connection_id')::BIGINT::TEXT OR
         OLD.definition_jsonb->'canonical_plan'->'cutoff' IS DISTINCT FROM '"2026-08-21T06:59:07.534131Z"'::JSONB OR
         OLD.definition_jsonb->'canonical_plan'->>'ordered_recording_ids_sha256' IS DISTINCT FROM
           '6038d4a23be9b0b5c2bb29ea933743a5ceb7f06b8875e417a3f16b44051ebd71' OR
         OLD.definition_jsonb->'canonical_plan'->>'qualification_rule_version' IS DISTINCT FROM OLD.definition_version OR
         jsonb_typeof(OLD.definition_jsonb->'canonical_plan'->'members') IS DISTINCT FROM 'array' OR
         jsonb_array_length(OLD.definition_jsonb->'canonical_plan'->'members')<>33 OR
         jsonb_typeof(OLD.definition_jsonb->'recording_jobs') IS DISTINCT FROM 'array' OR
         jsonb_array_length(OLD.definition_jsonb->'recording_jobs')<>33 OR
         ARRAY(SELECT m.recording_id FROM recording_qualification_members m WHERE m.run_id=OLD.id ORDER BY m.ordinal)
           IS DISTINCT FROM ARRAY[377,335,337,355,385,350,382,384,348,403,380,379,383,404,401,408,406,
             409,422,418,419,413,420,428,423,425,416,421,437,440,429,431,439]::BIGINT[] OR
         EXISTS(SELECT 1 FROM recording_qualification_members m WHERE m.run_id=OLD.id AND
           (m.scene_identity_sha256 IS NOT NULL OR m.scene_frame_evidence_id IS NOT NULL OR
            m.window_generator_version<>'historical-explicit-jobs-v1')) OR
         EXISTS(SELECT 1 FROM jsonb_array_elements(OLD.definition_jsonb->'recording_jobs') WITH ORDINALITY e(item,ord)
           WHERE jsonb_typeof(item) IS DISTINCT FROM 'object' OR
             (item->>'recording_id')::BIGINT IS DISTINCT FROM
               (ARRAY[377,335,337,355,385,350,382,384,348,403,380,379,383,404,401,408,406,
                 409,422,418,419,413,420,428,423,425,416,421,437,440,429,431,439]::BIGINT[])[ord] OR
             (item->'recording_id')::TEXT IS DISTINCT FROM (item->>'recording_id')::BIGINT::TEXT OR
             jsonb_typeof(item->'job_ids') IS DISTINCT FROM 'array' OR jsonb_array_length(item->'job_ids')<>14) OR
         EXISTS(SELECT 1 FROM jsonb_array_elements(OLD.definition_jsonb->'recording_jobs') entries(item)
           CROSS JOIN LATERAL jsonb_array_elements(entries.item->'job_ids') jobs(job_id)
           WHERE job_id::TEXT IS DISTINCT FROM (job_id#>>'{}')::BIGINT::TEXT) OR
         (SELECT count(DISTINCT jobs.job_id::BIGINT)
            FROM jsonb_array_elements(OLD.definition_jsonb->'recording_jobs') AS entries(item)
            CROSS JOIN LATERAL jsonb_array_elements_text(entries.item->'job_ids') AS jobs(job_id))<>462 THEN
        RAISE EXCEPTION 'historical qualification authority differs';
      END IF;
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
        imported.member->'qualification'->'frozen_at' IS DISTINCT FROM '"2026-08-21T06:59:07.534131Z"'::JSONB OR
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
        j.window_end_at<>w.window_end_at OR j.completed_at>'2026-08-21T06:59:07.534131Z'::TIMESTAMPTZ OR
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

CREATE OR REPLACE FUNCTION guard_recording_joined_freeze_child_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE batch_key BIGINT; batch_state TEXT; batch_freeze_started_at TIMESTAMPTZ; qualification_version TEXT;
BEGIN
  IF TG_TABLE_NAME='recording_joined_day_boundaries' THEN
    SELECT batch_record_id INTO STRICT batch_key FROM recording_joined_stream_days
      WHERE id=(to_jsonb(NEW)->>'stream_day_id')::BIGINT;
  ELSE
    batch_key := (to_jsonb(NEW)->>'batch_record_id')::BIGINT;
  END IF;
  SELECT state,freeze_started_at INTO STRICT batch_state,batch_freeze_started_at
    FROM recording_joined_batches WHERE id=batch_key FOR SHARE;
  IF (TG_TABLE_NAME IN ('recording_joined_batch_recordings','recording_joined_freeze_exclusions',
      'recording_joined_stream_days','recording_joined_source_snapshots') AND batch_state<>'snapshotting')
    OR (TG_TABLE_NAME IN ('recording_joined_day_boundaries','recording_joined_hours','recording_joined_sources')
      AND (batch_state<>'building' OR batch_freeze_started_at IS NOT NULL))
  THEN RAISE EXCEPTION 'joined frozen source scope is immutable'; END IF;
  IF TG_TABLE_NAME='recording_joined_batch_recordings' AND NOT EXISTS(SELECT 1 FROM recordings r
    WHERE r.id=(to_jsonb(NEW)->>'recording_id')::BIGINT AND r.account_id=(to_jsonb(NEW)->>'account_id')::BIGINT
      AND r.cron_timezone=to_jsonb(NEW)->>'timezone' AND r.mode='continuous' AND r.delivery='nas_pull'
      AND r.daily_window_start='08:00'::TIME AND r.daily_window_end='20:00'::TIME FOR KEY SHARE)
  THEN RAISE EXCEPTION 'joined recording scope differs'; END IF;
  IF TG_TABLE_NAME='recording_joined_batch_recordings' AND NOT EXISTS(
    SELECT 1 FROM recording_joined_batches b
    JOIN recording_qualification_runs q ON q.id=b.qualification_run_id AND q.account_id=b.account_id
    JOIN recording_qualification_members m ON m.run_id=b.qualification_run_id
      AND m.recording_id=(to_jsonb(NEW)->>'recording_id')::BIGINT
      AND m.account_id=(to_jsonb(NEW)->>'account_id')::BIGINT
    WHERE b.id=batch_key AND b.qualification_run_id=(to_jsonb(NEW)->>'qualification_run_id')::BIGINT
      AND q.status='active' AND q.definition_version=to_jsonb(NEW)->>'qualification_policy_version'
      AND m.cron_timezone=to_jsonb(NEW)->>'timezone' FOR KEY SHARE OF b,q,m)
  THEN RAISE EXCEPTION 'joined qualification member differs'; END IF;
  IF TG_TABLE_NAME='recording_joined_stream_days' THEN
    SELECT q.definition_version INTO STRICT qualification_version
    FROM recording_joined_batch_recordings br
    JOIN recording_qualification_runs q ON q.id=br.qualification_run_id
    WHERE br.id=(to_jsonb(NEW)->>'batch_recording_id')::BIGINT AND br.batch_record_id=batch_key;
    IF qualification_version='recording-qualification-tier1-historical-import-v1' THEN
      IF NOT EXISTS(
        SELECT 1 FROM recording_joined_batch_recordings br
        JOIN recording_joined_batches b ON b.id=br.batch_record_id
        JOIN recording_qualification_runs q ON q.id=br.qualification_run_id AND q.account_id=b.account_id
        JOIN recording_qualification_windows w ON w.run_id=br.qualification_run_id
          AND w.recording_id=br.recording_id AND w.ordinal=(to_jsonb(NEW)->>'date_ordinal')::INTEGER
        JOIN LATERAL (SELECT q.definition_jsonb->'canonical_plan'->'members'->(br.priority_ordinal-1)
          ->'qualification'->'days'->((to_jsonb(NEW)->>'date_ordinal')::INTEGER-1) AS day) imported ON TRUE
        WHERE br.id=(to_jsonb(NEW)->>'batch_recording_id')::BIGINT AND br.batch_record_id=batch_key
          AND b.qualification_frozen_at>b.eligibility_cutoff
          AND imported.day IS NOT NULL
          AND imported.day IS NOT DISTINCT FROM br.qualification->'days'->((to_jsonb(NEW)->>'date_ordinal')::INTEGER-1)
          AND (imported.day->>'qualification_window_ordinal')::INTEGER=(to_jsonb(NEW)->>'date_ordinal')::INTEGER
          AND imported.day->>'local_date'=to_jsonb(NEW)->>'local_date'
          AND (imported.day->>'job_id')::BIGINT=(to_jsonb(NEW)->>'recording_job_id')::BIGINT
          AND (imported.day->>'window_start')::TIMESTAMPTZ=(to_jsonb(NEW)->>'scheduled_start_at')::TIMESTAMPTZ
          AND (imported.day->>'window_end')::TIMESTAMPTZ=(to_jsonb(NEW)->>'scheduled_end_at')::TIMESTAMPTZ
          AND (imported.day->>'completed_at')::TIMESTAMPTZ=(to_jsonb(NEW)->>'completed_at')::TIMESTAMPTZ
          AND (imported.day->>'completed_at')::TIMESTAMPTZ<=b.eligibility_cutoff
          AND w.local_open_at::DATE=(imported.day->>'local_date')::DATE
          AND ROW(w.window_start_at,w.window_end_at) IS NOT DISTINCT FROM
            ROW((imported.day->>'window_start')::TIMESTAMPTZ,(imported.day->>'window_end')::TIMESTAMPTZ)
          AND imported.day->>'job_status' IN('done','error')
          AND (imported.day->>'completed_at')::TIMESTAMPTZ>=(imported.day->>'window_start')::TIMESTAMPTZ
          AND (imported.day->>'job_status'='error' OR
            (imported.day->>'completed_at')::TIMESTAMPTZ>=(imported.day->>'window_end')::TIMESTAMPTZ)
          AND (imported.day->>'job_status'<>'error' OR
            ((br.recording_id=348 AND imported.day->>'local_date'='2026-07-29') OR
             (br.recording_id IN(408,406,409) AND imported.day->>'local_date'='2026-08-11')))
        FOR KEY SHARE OF br,b,q,w)
      THEN RAISE EXCEPTION 'joined historical qualification window differs'; END IF;
    ELSIF NOT EXISTS(
      SELECT 1 FROM recording_joined_batch_recordings br
      JOIN recording_joined_batches b ON b.id=br.batch_record_id
      JOIN recording_qualification_runs q ON q.id=br.qualification_run_id AND q.account_id=b.account_id
      JOIN recording_qualification_windows w ON w.run_id=br.qualification_run_id
        AND w.recording_id=br.recording_id AND w.ordinal=(to_jsonb(NEW)->>'date_ordinal')::INTEGER
      JOIN recording_jobs j ON j.id=(to_jsonb(NEW)->>'recording_job_id')::BIGINT
      WHERE br.id=(to_jsonb(NEW)->>'batch_recording_id')::BIGINT AND br.batch_record_id=batch_key
        AND q.definition_version=qualification_version
        AND w.local_open_at::DATE=(to_jsonb(NEW)->>'local_date')::DATE
        AND ROW(w.window_start_at,w.window_end_at) IS NOT DISTINCT FROM
          ROW((to_jsonb(NEW)->>'scheduled_start_at')::TIMESTAMPTZ,(to_jsonb(NEW)->>'scheduled_end_at')::TIMESTAMPTZ)
        AND j.recording_id=br.recording_id AND j.fire_at=w.window_start_at AND j.kind='continuous_window'
        AND j.window_end_at=w.window_end_at AND j.completed_at=(to_jsonb(NEW)->>'completed_at')::TIMESTAMPTZ
        AND j.completed_at<=b.eligibility_cutoff AND j.scheduled_for=w.window_start_at AND j.status='done'
        AND b.qualification_frozen_at<=w.window_start_at AND j.completed_at>=j.window_end_at
      FOR KEY SHARE OF br,b,q,w FOR SHARE OF j)
    THEN RAISE EXCEPTION 'joined prospective qualification window differs'; END IF;
  END IF;
  IF TG_TABLE_NAME='recording_joined_hours' AND
    ((to_jsonb(NEW)->>'state')<>'pending' OR (to_jsonb(NEW)->>'attempt_count')::INTEGER<>0)
  THEN RAISE EXCEPTION 'joined hour must enter pending'; END IF;
  IF TG_TABLE_NAME='recording_joined_stream_days' AND (to_jsonb(NEW)->>'state')<>'pending'
  THEN RAISE EXCEPTION 'joined stream day must enter pending'; END IF;
  IF TG_TABLE_NAME IN ('recording_joined_day_boundaries','recording_joined_hours','recording_joined_sources')
    AND NOT EXISTS(SELECT 1 FROM recording_joined_stream_days d
      WHERE d.id=(to_jsonb(NEW)->>'stream_day_id')::BIGINT AND d.state='pending' FOR SHARE)
  THEN RAISE EXCEPTION 'joined sealed stream day is immutable'; END IF;
  RETURN NEW;
END $$;

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
      ORDER BY br.priority_ordinal) IS DISTINCT FROM ARRAY[377,335,337,355,385,350,382,384,348,403,380,379,
      383,404,401,408,406,409,422,418,419,413,420,428,423,425,416,421,437,440,429,431,439]::BIGINT[]
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
        ARRAY(SELECT key FROM jsonb_object_keys(frozen.item) AS object_keys(key) ORDER BY key COLLATE "C") IS DISTINCT FROM ARRAY['frozen_recording','qualification']::TEXT[]
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

CREATE OR REPLACE FUNCTION validate_recording_joined_stream_day(p_stream_day_id BIGINT) RETURNS BOOLEAN LANGUAGE plpgsql AS $$
DECLARE d recording_joined_stream_days%ROWTYPE; b recording_joined_day_boundaries%ROWTYPE;
  source_count INTEGER; source_bytes BIGINT; last_position INTEGER := 0; expected_position INTEGER;
  expected_at TIMESTAMPTZ; expected_clip BIGINT; expected_prev BIGINT; expected_next BIGINT;
  expected_prev_end TIMESTAMPTZ; expected_next_start TIMESTAMPTZ; expected_gap BIGINT;
  expected_skew BIGINT; recording_timezone TEXT; expected_day_start TIMESTAMPTZ; expected_day_end TIMESTAMPTZ;
  ledger JSONB; batch recording_joined_batches%ROWTYPE; batch_recording recording_joined_batch_recordings%ROWTYPE;
BEGIN
  SELECT * INTO STRICT d FROM recording_joined_stream_days WHERE id = p_stream_day_id;
  SELECT * INTO STRICT batch FROM recording_joined_batches WHERE id=d.batch_record_id;
  SELECT * INTO STRICT batch_recording FROM recording_joined_batch_recordings WHERE id=d.batch_recording_id;
  recording_timezone := batch_recording.timezone;
  ledger := convert_from(d.ledger_bytes,'UTF8')::jsonb;
  expected_day_start := make_timestamptz(extract(year FROM d.local_date)::INTEGER,
    extract(month FROM d.local_date)::INTEGER,extract(day FROM d.local_date)::INTEGER,8,0,0,recording_timezone);
  expected_day_end := make_timestamptz(extract(year FROM d.local_date)::INTEGER,
    extract(month FROM d.local_date)::INTEGER,extract(day FROM d.local_date)::INTEGER,20,0,0,recording_timezone);
  SELECT count(*), COALESCE(sum(size_bytes), 0) INTO source_count, source_bytes
    FROM recording_joined_sources WHERE stream_day_id = d.id;
  IF d.state<>'sealed'
    OR ARRAY(SELECT key FROM jsonb_object_keys(ledger) AS object_keys(key) ORDER BY key COLLATE "C") IS DISTINCT FROM ARRAY['batch_id','consecutive_pairs',
      'cross_day_boundaries','cross_hour_boundaries','first_clip_id','frozen_source_sha256','generation','hour_source_claim_sha256','hours',
      'last_clip_id','ledger_sha256','local_date','qualification_day','qualification_sha256','recording_id',
      'schema_version','source_bytes','source_claim_sha256','source_clip_count','sources','timezone']::TEXT[]
    OR (batch_recording.qualification_policy_version<>'recording-qualification-tier1-historical-import-v1'
      AND ARRAY(SELECT key FROM jsonb_object_keys(ledger->'qualification_day') AS object_keys(key) ORDER BY key COLLATE "C") IS DISTINCT FROM
        ARRAY['completed_at','job_id','local_date','qualification_window_ordinal','window_end','window_start']::TEXT[])
    OR (batch_recording.qualification_policy_version='recording-qualification-tier1-historical-import-v1'
      AND ARRAY(SELECT key FROM jsonb_object_keys(ledger->'qualification_day') AS object_keys(key) ORDER BY key COLLATE "C") IS DISTINCT FROM
        ARRAY['completed_at','job_id','job_status','local_date','qualification_window_ordinal','reason_codes','scheduled_for','window_end','window_start']::TEXT[])
    OR EXISTS(SELECT 1 FROM jsonb_array_elements(ledger->'sources') source WHERE
      ARRAY(SELECT key FROM jsonb_object_keys(source) AS object_keys(key) ORDER BY key COLLATE "C") IS DISTINCT FROM ARRAY['bucket','clip_id','end_utc','endpoint',
        'object','provider','recording_id','recording_job_id','region','released_at','seam_to_previous','start_utc',
        'storage_destination_id']::TEXT[]
      OR ARRAY(SELECT key FROM jsonb_object_keys(source->'object') AS object_keys(key) ORDER BY key COLLATE "C") NOT IN
        (ARRAY['etag','key','sha256','size_bytes']::TEXT[],ARRAY['etag','key','sha256','size_bytes','version_id']::TEXT[])
      OR ARRAY(SELECT key FROM jsonb_object_keys(source->'seam_to_previous') AS object_keys(key) ORDER BY key COLLATE "C") IS DISTINCT FROM
        ARRAY['reason','signed_gap_nanoseconds','verdict']::TEXT[])
    OR EXISTS(SELECT 1 FROM jsonb_array_elements(ledger->'hours') hour WHERE
      ARRAY(SELECT key FROM jsonb_object_keys(hour) AS object_keys(key) ORDER BY key COLLATE "C") IS DISTINCT FROM
        ARRAY['clock_hour','delivery_hour','source_clip_ids']::TEXT[])
    OR EXISTS(SELECT 1 FROM jsonb_array_elements(ledger->'consecutive_pairs') pair WHERE
      ARRAY(SELECT key FROM jsonb_object_keys(pair) AS object_keys(key) ORDER BY key COLLATE "C") IS DISTINCT FROM ARRAY['next_clip_id',
        'next_presentation_start_utc','previous_clip_id','previous_presentation_end_utc','signed_gap_nanoseconds']::TEXT[])
    OR EXISTS(SELECT 1 FROM jsonb_array_elements(ledger->'cross_hour_boundaries') boundary WHERE
      ARRAY(SELECT key FROM jsonb_object_keys(boundary) AS object_keys(key) ORDER BY key COLLATE "C") IS DISTINCT FROM ARRAY['actual_seam_utc',
        'allocation_decision','boundary_skew_nanoseconds','next_clip_id','next_delivery_hour','next_presentation_start_utc',
        'previous_clip_id','previous_delivery_hour','previous_presentation_end_utc','reason','scheduled_utc',
        'signed_gap_nanoseconds','verdict']::TEXT[])
    OR EXISTS(SELECT 1 FROM jsonb_array_elements(ledger->'cross_day_boundaries') boundary WHERE
      ARRAY(SELECT key FROM jsonb_object_keys(boundary) AS object_keys(key) ORDER BY key COLLATE "C") IS DISTINCT FROM ARRAY['allocation_decision',
        'boundary_skew_nanoseconds','next_clip_id','next_presentation_start_utc','previous_clip_id',
        'previous_presentation_end_utc','reason','scheduled_next_start_utc','scheduled_previous_end_utc',
        'signed_gap_nanoseconds','verdict']::TEXT[])
    OR (ledger->>'schema_version')::INTEGER IS DISTINCT FROM 1 OR ledger->>'batch_id' IS DISTINCT FROM d.batch_id
    OR (ledger->>'generation')::INTEGER IS DISTINCT FROM batch.generation
    OR (ledger->>'recording_id')::BIGINT IS DISTINCT FROM d.recording_id
    OR ledger->>'timezone' IS DISTINCT FROM recording_timezone OR (ledger->>'local_date')::DATE IS DISTINCT FROM d.local_date
    OR ledger->'qualification_day' IS DISTINCT FROM batch_recording.qualification->'days'->(d.date_ordinal-1)
    OR ledger->>'qualification_sha256' IS DISTINCT FROM batch_recording.qualification_sha256
    OR ledger->>'frozen_source_sha256' IS DISTINCT FROM d.source_snapshot_sha256
    OR ledger->>'source_claim_sha256' IS DISTINCT FROM d.source_manifest_sha256
    OR (ledger->>'source_clip_count')::INTEGER IS DISTINCT FROM d.source_clip_count
    OR (ledger->>'source_bytes')::BIGINT IS DISTINCT FROM d.source_bytes
    OR ledger->>'ledger_sha256' IS DISTINCT FROM d.ledger_sha256
    OR jsonb_array_length(ledger->'sources') IS DISTINCT FROM d.source_clip_count
    OR jsonb_array_length(ledger->'hours') IS DISTINCT FROM 12
    OR jsonb_array_length(ledger->'hour_source_claim_sha256') IS DISTINCT FROM 12
    OR jsonb_array_length(ledger->'cross_hour_boundaries') IS DISTINCT FROM 11
    OR jsonb_array_length(ledger->'cross_day_boundaries') IS DISTINCT FROM 2
    OR jsonb_array_length(ledger->'consecutive_pairs') IS DISTINCT FROM GREATEST(d.source_clip_count-1,0)
    OR (d.source_clip_count=0 AND (ledger->'first_clip_id' IS DISTINCT FROM 'null'::jsonb OR ledger->'last_clip_id' IS DISTINCT FROM 'null'::jsonb))
    OR (d.source_clip_count>0 AND ((ledger->>'first_clip_id')::BIGINT IS DISTINCT FROM (SELECT clip_id FROM recording_joined_sources WHERE stream_day_id=d.id ORDER BY day_ordinal LIMIT 1)
      OR (ledger->>'last_clip_id')::BIGINT IS DISTINCT FROM (SELECT clip_id FROM recording_joined_sources WHERE stream_day_id=d.id ORDER BY day_ordinal DESC LIMIT 1)))
    OR source_count <> d.source_clip_count OR source_bytes <> d.source_bytes
    OR (SELECT count(*) FROM recording_joined_source_snapshots s WHERE s.stream_day_id=d.id)<>d.source_clip_count
    OR (SELECT COALESCE(sum(size_bytes),0) FROM recording_joined_source_snapshots s
      WHERE s.stream_day_id=d.id)<>d.source_bytes
    OR EXISTS(SELECT 1 FROM recording_joined_source_snapshots snapshot
      LEFT JOIN recording_joined_sources observed ON observed.source_snapshot_id=snapshot.id
      WHERE snapshot.stream_day_id=d.id AND (observed.id IS NULL OR observed.stream_day_id<>d.id
        OR ROW(observed.clip_id,observed.recording_id,observed.recording_job_id,observed.storage_destination_id,
          observed.provider,observed.endpoint,observed.region,observed.bucket,observed.object_key,observed.etag,
          observed.size_bytes,observed.sha256,observed.start_at,observed.end_at,observed.clip_created_at,observed.released_at)
          IS DISTINCT FROM ROW(snapshot.clip_id,snapshot.recording_id,snapshot.recording_job_id,
          snapshot.storage_destination_id,snapshot.provider,snapshot.endpoint,snapshot.region,snapshot.bucket,
          snapshot.object_key,snapshot.ingest_etag,snapshot.size_bytes,snapshot.sha256,snapshot.start_at,
          snapshot.end_at,snapshot.clip_created_at,snapshot.released_at)))
    OR d.scheduled_start_at <> expected_day_start OR d.scheduled_end_at <> expected_day_end
    OR (source_count > 0 AND ((SELECT min(day_ordinal) FROM recording_joined_sources WHERE stream_day_id = d.id) <> 1
      OR (SELECT max(day_ordinal) FROM recording_joined_sources WHERE stream_day_id = d.id) <> source_count))
    OR EXISTS(SELECT 1 FROM (SELECT day_ordinal,row_number() OVER (ORDER BY start_at,clip_id) AS expected_ordinal
      FROM recording_joined_sources WHERE stream_day_id=d.id) ordered WHERE day_ordinal<>expected_ordinal)
    OR (SELECT count(*) FROM recording_joined_hours WHERE stream_day_id = d.id) <> 12
    OR EXISTS(SELECT 1 FROM recording_joined_hours h
      JOIN recording_joined_batch_recordings priority_recording ON priority_recording.id=d.batch_recording_id
      LEFT JOIN LATERAL (SELECT count(*)::INTEGER AS clip_count, COALESCE(sum(size_bytes), 0)::BIGINT AS bytes
        FROM recording_joined_sources s WHERE s.hour_record_id = h.id) actual ON TRUE
      WHERE h.stream_day_id = d.id AND (h.source_clip_count <> actual.clip_count OR h.source_bytes <> actual.bytes
        OR h.priority_ordinal<>(priority_recording.priority_ordinal-1)*168+(d.date_ordinal-1)*12+h.delivery_hour
        OR h.local_date<>d.local_date OR h.clock_hour<>h.delivery_hour+7
        OR h.scheduled_start_at<>make_timestamptz(extract(year FROM d.local_date)::INTEGER,
          extract(month FROM d.local_date)::INTEGER,extract(day FROM d.local_date)::INTEGER,h.clock_hour,0,0,recording_timezone)
        OR h.scheduled_end_at<>make_timestamptz(extract(year FROM d.local_date)::INTEGER,
          extract(month FROM d.local_date)::INTEGER,extract(day FROM d.local_date)::INTEGER,h.clock_hour+1,0,0,recording_timezone)))
    OR EXISTS(SELECT 1 FROM recording_joined_sources s WHERE s.stream_day_id=d.id
      AND ((s.start_at AT TIME ZONE recording_timezone)::DATE<>d.local_date
        OR (s.end_at AT TIME ZONE recording_timezone)::DATE<>d.local_date))
    OR (SELECT count(*) FROM recording_joined_day_boundaries WHERE stream_day_id = d.id AND boundary_kind = 'cross_hour') <> 11
    OR (SELECT count(*) FROM recording_joined_day_boundaries WHERE stream_day_id = d.id AND boundary_kind = 'cross_day') <> 2
  THEN RAISE EXCEPTION 'joined stream-day cardinality differs'; END IF;

  IF EXISTS(
    SELECT 1 FROM recording_joined_sources s
    LEFT JOIN LATERAL (
      SELECT value AS source FROM jsonb_array_elements(ledger->'sources') WITH ORDINALITY item(value, ordinal)
      WHERE ordinal=s.day_ordinal
    ) js ON TRUE
    WHERE s.stream_day_id=d.id AND (js.source IS NULL
      OR (js.source->>'clip_id')::BIGINT IS DISTINCT FROM s.clip_id
      OR (js.source->>'recording_id')::BIGINT IS DISTINCT FROM s.recording_id
      OR (js.source->>'recording_job_id')::BIGINT IS DISTINCT FROM s.recording_job_id
      OR (js.source->>'storage_destination_id')::BIGINT IS DISTINCT FROM s.storage_destination_id
      OR js.source->>'provider' IS DISTINCT FROM s.provider OR js.source->>'endpoint' IS DISTINCT FROM s.endpoint
      OR js.source->>'region' IS DISTINCT FROM s.region OR js.source->>'bucket' IS DISTINCT FROM s.bucket
      OR (js.source->>'start_utc')::TIMESTAMPTZ IS DISTINCT FROM s.start_at
      OR (js.source->>'end_utc')::TIMESTAMPTZ IS DISTINCT FROM s.end_at
      OR (js.source->>'released_at')::TIMESTAMPTZ IS DISTINCT FROM s.released_at
      OR js.source->'object'->>'key' IS DISTINCT FROM s.object_key
      OR js.source->'object'->>'etag' IS DISTINCT FROM s.etag
      OR COALESCE(js.source->'object'->>'version_id','') IS DISTINCT FROM s.version_id
      OR (js.source->'object'->>'size_bytes')::BIGINT IS DISTINCT FROM s.size_bytes
      OR js.source->'object'->>'sha256' IS DISTINCT FROM s.sha256
      OR js.source->'seam_to_previous' IS DISTINCT FROM s.seam_to_previous
      OR s.audio_contract IS NOT NULL)
  ) THEN RAISE EXCEPTION 'joined canonical ledger sources differ'; END IF;
  IF EXISTS(
    SELECT 1 FROM recording_joined_hours h
    LEFT JOIN LATERAL (
      SELECT value AS hour FROM jsonb_array_elements(ledger->'hours') WITH ORDINALITY item(value, ordinal)
      WHERE ordinal=h.delivery_hour
    ) jh ON TRUE
    WHERE h.stream_day_id=d.id AND (jh.hour IS NULL
      OR (jh.hour->>'delivery_hour')::INTEGER IS DISTINCT FROM h.delivery_hour
      OR (jh.hour->>'clock_hour')::INTEGER IS DISTINCT FROM h.clock_hour
      OR jh.hour->'source_clip_ids' IS DISTINCT FROM (SELECT COALESCE(jsonb_agg(s.clip_id ORDER BY s.day_ordinal),'[]'::jsonb)
        FROM recording_joined_sources s WHERE s.hour_record_id=h.id)
      OR ledger->'hour_source_claim_sha256'->>(h.delivery_hour-1) IS DISTINCT FROM h.source_claim_sha256)
  ) THEN RAISE EXCEPTION 'joined canonical ledger hours differ'; END IF;
  IF EXISTS(
    SELECT 1 FROM recording_joined_sources previous
    JOIN recording_joined_sources next ON next.stream_day_id=previous.stream_day_id AND next.day_ordinal=previous.day_ordinal+1
    LEFT JOIN LATERAL (
      SELECT value AS pair FROM jsonb_array_elements(ledger->'consecutive_pairs') WITH ORDINALITY item(value, ordinal)
      WHERE ordinal=previous.day_ordinal
    ) jp ON TRUE
    WHERE previous.stream_day_id=d.id AND (jp.pair IS NULL
      OR (jp.pair->>'previous_clip_id')::BIGINT IS DISTINCT FROM previous.clip_id
      OR (jp.pair->>'next_clip_id')::BIGINT IS DISTINCT FROM next.clip_id
      OR (jp.pair->>'previous_presentation_end_utc')::TIMESTAMPTZ IS DISTINCT FROM previous.end_at
      OR (jp.pair->>'next_presentation_start_utc')::TIMESTAMPTZ IS DISTINCT FROM next.start_at
      OR (jp.pair->>'signed_gap_nanoseconds')::BIGINT IS DISTINCT FROM (extract(epoch FROM (next.start_at-previous.end_at))*1000000000)::BIGINT)
  ) THEN RAISE EXCEPTION 'joined canonical ledger pairs differ'; END IF;

  FOR b IN SELECT * FROM recording_joined_day_boundaries
    WHERE stream_day_id = d.id AND boundary_kind = 'cross_hour' ORDER BY ordinal
  LOOP
    expected_at := make_timestamptz(extract(year FROM d.local_date)::INTEGER,
      extract(month FROM d.local_date)::INTEGER,extract(day FROM d.local_date)::INTEGER,8+b.ordinal,0,0,recording_timezone);
    IF b.scheduled_at <> expected_at THEN RAISE EXCEPTION 'joined hour boundary schedule differs'; END IF;
    IF NOT EXISTS(SELECT 1 FROM jsonb_array_elements(ledger->'cross_hour_boundaries') WITH ORDINALITY item(value,ordinal)
      WHERE ordinal=b.ordinal AND (value->>'previous_delivery_hour')::INTEGER=b.previous_delivery_hour
        AND (value->>'next_delivery_hour')::INTEGER=b.next_delivery_hour
        AND (value->>'previous_clip_id')::BIGINT IS NOT DISTINCT FROM b.previous_clip_id
        AND (value->>'next_clip_id')::BIGINT IS NOT DISTINCT FROM b.next_clip_id
        AND (value->>'previous_presentation_end_utc')::TIMESTAMPTZ IS NOT DISTINCT FROM b.previous_presentation_end_at
        AND (value->>'next_presentation_start_utc')::TIMESTAMPTZ IS NOT DISTINCT FROM b.next_presentation_start_at
        AND (value->>'signed_gap_nanoseconds')::BIGINT IS NOT DISTINCT FROM b.signed_gap_nanoseconds
        AND (value->>'scheduled_utc')::TIMESTAMPTZ=b.scheduled_at
        AND (value->>'actual_seam_utc')::TIMESTAMPTZ IS NOT DISTINCT FROM b.actual_seam_at
        AND (value->>'boundary_skew_nanoseconds')::BIGINT IS NOT DISTINCT FROM b.boundary_skew_nanoseconds
        AND value->>'allocation_decision'=b.allocation_decision AND value->>'verdict'=b.verdict AND value->>'reason'=b.reason)
    THEN RAISE EXCEPTION 'joined canonical hour boundary differs'; END IF;
    SELECT candidate_position, candidate_at, candidate_clip_id
      INTO expected_position, expected_at, expected_clip
      FROM (
        SELECT day_ordinal - 1 AS candidate_position, start_at AS candidate_at, clip_id AS candidate_clip_id
          FROM recording_joined_sources WHERE stream_day_id = d.id AND day_ordinal - 1 >= last_position
        UNION ALL
        SELECT day_ordinal, end_at, clip_id FROM recording_joined_sources
          WHERE stream_day_id = d.id AND day_ordinal >= last_position
      ) candidates
      ORDER BY abs((extract(epoch FROM (candidate_at - b.scheduled_at)) * 1000000)::BIGINT),
        candidate_at, candidate_position, candidate_clip_id LIMIT 1;
    IF expected_position IS NULL THEN expected_position := 0; END IF;
    IF expected_position <> (SELECT count(*) FROM recording_joined_sources s JOIN recording_joined_hours h
      ON h.id=s.hour_record_id WHERE s.stream_day_id=d.id AND h.delivery_hour<=b.ordinal)
    THEN RAISE EXCEPTION 'joined hour allocation position differs'; END IF;
    SELECT clip_id, end_at INTO expected_prev, expected_prev_end FROM recording_joined_sources
      WHERE stream_day_id = d.id AND day_ordinal = expected_position;
    SELECT clip_id, start_at INTO expected_next, expected_next_start FROM recording_joined_sources
      WHERE stream_day_id = d.id AND day_ordinal = expected_position + 1;
    IF expected_prev IS NOT NULL AND expected_next IS NOT NULL THEN
      expected_gap := (extract(epoch FROM (expected_next_start - expected_prev_end)) * 1000000000)::BIGINT;
    ELSE expected_gap := NULL; END IF;
    IF b.previous_clip_id IS DISTINCT FROM expected_prev OR b.next_clip_id IS DISTINCT FROM expected_next
      OR b.previous_presentation_end_at IS DISTINCT FROM expected_prev_end
      OR b.next_presentation_start_at IS DISTINCT FROM expected_next_start
      OR b.signed_gap_nanoseconds IS DISTINCT FROM expected_gap
      OR (expected_prev IS NOT NULL AND expected_next IS NOT NULL AND b.actual_seam_at IS DISTINCT FROM expected_at)
      OR (expected_prev IS NOT NULL AND expected_next IS NOT NULL AND b.boundary_skew_nanoseconds IS DISTINCT FROM
        (extract(epoch FROM (expected_at - b.scheduled_at)) * 1000000000)::BIGINT)
      OR ((expected_prev IS NULL OR expected_next IS NULL) AND
        (b.actual_seam_at IS NOT NULL OR b.boundary_skew_nanoseconds IS NOT NULL))
      OR (expected_prev IS NOT NULL AND expected_next IS NOT NULL AND
        ROW(b.allocation_decision, b.verdict, b.reason) IS DISTINCT FROM ROW('split_before_next_source', 'allocated', 'closest_source_boundary'))
      OR (expected_prev IS NULL AND expected_next IS NOT NULL AND
        ROW(b.allocation_decision, b.verdict, b.reason) IS DISTINCT FROM ROW('no_source_before_boundary', 'absent_source', 'previous_source_absent'))
      OR (expected_prev IS NOT NULL AND expected_next IS NULL AND
        ROW(b.allocation_decision, b.verdict, b.reason) IS DISTINCT FROM ROW('no_source_after_boundary', 'absent_source', 'next_source_absent'))
      OR (expected_prev IS NULL AND expected_next IS NULL AND
        ROW(b.allocation_decision, b.verdict, b.reason) IS DISTINCT FROM ROW('no_sources', 'absent_source', 'both_sources_absent'))
    THEN RAISE EXCEPTION 'joined closest hour boundary differs'; END IF;
    last_position := expected_position;
  END LOOP;

  IF EXISTS(
    SELECT 1 FROM recording_joined_sources s
    JOIN recording_joined_hours h ON h.id=s.hour_record_id
    LEFT JOIN recording_joined_day_boundaries lower_boundary
      ON lower_boundary.stream_day_id=d.id AND lower_boundary.boundary_kind='cross_hour'
        AND lower_boundary.ordinal=h.delivery_hour-1
    LEFT JOIN recording_joined_sources lower_source
      ON lower_source.stream_day_id=d.id AND lower_source.clip_id=lower_boundary.previous_clip_id
    LEFT JOIN recording_joined_day_boundaries upper_boundary
      ON upper_boundary.stream_day_id=d.id AND upper_boundary.boundary_kind='cross_hour'
        AND upper_boundary.ordinal=h.delivery_hour
    LEFT JOIN recording_joined_sources upper_source
      ON upper_source.stream_day_id=d.id AND upper_source.clip_id=upper_boundary.previous_clip_id
    WHERE s.stream_day_id=d.id AND (
      s.day_ordinal<=CASE WHEN h.delivery_hour=1 THEN 0 ELSE COALESCE(lower_source.day_ordinal,0) END
      OR s.day_ordinal>CASE WHEN h.delivery_hour=12 THEN source_count ELSE COALESCE(upper_source.day_ordinal,0) END
      OR s.hour_ordinal<>s.day_ordinal-CASE WHEN h.delivery_hour=1 THEN 0 ELSE COALESCE(lower_source.day_ordinal,0) END)
  ) THEN RAISE EXCEPTION 'joined hour source membership differs'; END IF;

  FOR b IN SELECT * FROM recording_joined_day_boundaries
    WHERE stream_day_id = d.id AND boundary_kind = 'cross_day' ORDER BY ordinal
  LOOP
    IF NOT EXISTS(SELECT 1 FROM jsonb_array_elements(ledger->'cross_day_boundaries') WITH ORDINALITY item(value,ordinal)
      WHERE ordinal=b.ordinal
        AND (value->>'previous_clip_id')::BIGINT IS NOT DISTINCT FROM b.previous_clip_id
        AND (value->>'next_clip_id')::BIGINT IS NOT DISTINCT FROM b.next_clip_id
        AND (value->>'previous_presentation_end_utc')::TIMESTAMPTZ IS NOT DISTINCT FROM b.previous_presentation_end_at
        AND (value->>'next_presentation_start_utc')::TIMESTAMPTZ IS NOT DISTINCT FROM b.next_presentation_start_at
        AND (value->>'signed_gap_nanoseconds')::BIGINT IS NOT DISTINCT FROM b.signed_gap_nanoseconds
        AND (value->>'scheduled_previous_end_utc')::TIMESTAMPTZ=b.scheduled_previous_end_at
        AND (value->>'scheduled_next_start_utc')::TIMESTAMPTZ=b.scheduled_next_start_at
        AND (value->>'boundary_skew_nanoseconds')::BIGINT IS NOT DISTINCT FROM b.boundary_skew_nanoseconds
        AND value->>'allocation_decision'=b.allocation_decision AND value->>'verdict'=b.verdict AND value->>'reason'=b.reason)
    THEN RAISE EXCEPTION 'joined canonical cross-day boundary differs'; END IF;
    expected_prev := NULL; expected_next := NULL; expected_prev_end := NULL; expected_next_start := NULL;
    IF b.ordinal = 1 THEN
      SELECT s.clip_id, s.end_at INTO expected_prev, expected_prev_end
        FROM recording_joined_stream_days previous_day
        JOIN recording_joined_sources s ON s.stream_day_id = previous_day.id
        WHERE previous_day.batch_record_id = d.batch_record_id AND previous_day.recording_id = d.recording_id
          AND previous_day.local_date = d.local_date - 1 ORDER BY s.day_ordinal DESC LIMIT 1;
      SELECT clip_id, start_at INTO expected_next, expected_next_start FROM recording_joined_sources
        WHERE stream_day_id = d.id ORDER BY day_ordinal LIMIT 1;
    ELSE
      SELECT clip_id, end_at INTO expected_prev, expected_prev_end FROM recording_joined_sources
        WHERE stream_day_id = d.id ORDER BY day_ordinal DESC LIMIT 1;
      SELECT s.clip_id, s.start_at INTO expected_next, expected_next_start
        FROM recording_joined_stream_days next_day
        JOIN recording_joined_sources s ON s.stream_day_id = next_day.id
        WHERE next_day.batch_record_id = d.batch_record_id AND next_day.recording_id = d.recording_id
          AND next_day.local_date = d.local_date + 1 ORDER BY s.day_ordinal LIMIT 1;
    END IF;
    IF b.ordinal=1 THEN
      expected_at := make_timestamptz(extract(year FROM d.local_date-1)::INTEGER,
        extract(month FROM d.local_date-1)::INTEGER,extract(day FROM d.local_date-1)::INTEGER,20,0,0,recording_timezone);
      expected_day_start := make_timestamptz(extract(year FROM d.local_date)::INTEGER,
        extract(month FROM d.local_date)::INTEGER,extract(day FROM d.local_date)::INTEGER,8,0,0,recording_timezone);
    ELSE
      expected_at := make_timestamptz(extract(year FROM d.local_date)::INTEGER,
        extract(month FROM d.local_date)::INTEGER,extract(day FROM d.local_date)::INTEGER,20,0,0,recording_timezone);
      expected_day_start := make_timestamptz(extract(year FROM d.local_date+1)::INTEGER,
        extract(month FROM d.local_date+1)::INTEGER,extract(day FROM d.local_date+1)::INTEGER,8,0,0,recording_timezone);
    END IF;
    IF b.scheduled_previous_end_at<>expected_at OR b.scheduled_next_start_at<>expected_day_start
    THEN RAISE EXCEPTION 'joined cross-day schedule differs'; END IF;
    IF expected_prev IS NOT NULL AND expected_next IS NOT NULL THEN
      expected_gap := (extract(epoch FROM (expected_next_start - expected_prev_end)) * 1000000000)::BIGINT;
      expected_skew := expected_gap - (extract(epoch FROM (b.scheduled_next_start_at - b.scheduled_previous_end_at)) * 1000000000)::BIGINT;
    ELSE expected_gap := NULL; expected_skew := NULL; END IF;
    IF b.previous_clip_id IS DISTINCT FROM expected_prev OR b.next_clip_id IS DISTINCT FROM expected_next
      OR b.previous_presentation_end_at IS DISTINCT FROM expected_prev_end
      OR b.next_presentation_start_at IS DISTINCT FROM expected_next_start
      OR b.signed_gap_nanoseconds IS DISTINCT FROM expected_gap
      OR b.boundary_skew_nanoseconds IS DISTINCT FROM expected_skew
      OR b.actual_seam_at IS NOT NULL
      OR (expected_gap IS NOT NULL AND b.verdict <> CASE WHEN expected_gap < 0 THEN 'overlap' ELSE 'scheduled_gap' END)
      OR (expected_gap IS NOT NULL AND ROW(b.allocation_decision, b.reason)
        IS DISTINCT FROM ROW('separate_local_days', 'scheduled_day_boundary'))
      OR (expected_gap IS NULL AND b.verdict <> 'absent_source')
      OR (expected_gap IS NULL AND b.ordinal=1 AND expected_prev IS NULL AND
        ROW(b.allocation_decision,b.reason) IS DISTINCT FROM ROW('no_previous_day_source','previous_source_absent'))
      OR (expected_gap IS NULL AND b.ordinal=1 AND expected_prev IS NOT NULL AND
        ROW(b.allocation_decision,b.reason) IS DISTINCT FROM ROW('empty_day_after_previous_source','next_source_absent'))
      OR (expected_gap IS NULL AND b.ordinal=2 AND expected_next IS NULL AND
        ROW(b.allocation_decision,b.reason) IS DISTINCT FROM ROW('no_next_day_source','next_source_absent'))
      OR (expected_gap IS NULL AND b.ordinal=2 AND expected_next IS NOT NULL AND
        ROW(b.allocation_decision,b.reason) IS DISTINCT FROM ROW('empty_day_before_next_source','previous_source_absent'))
    THEN RAISE EXCEPTION 'joined cross-day boundary differs'; END IF;
  END LOOP;
  RETURN TRUE;
END $$;
