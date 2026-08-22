-- Historical Tier-1 imports remain valid after a recording reaches its normal completed state.
-- Prospective qualification still requires an active recording.
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
