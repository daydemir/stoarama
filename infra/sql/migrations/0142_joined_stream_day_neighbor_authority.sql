-- Align deferred stream-day validation with the immutable planner authority.
--
-- The planner is allowed to seal stream days in any order.  Its cross-day
-- ledger evidence uses the frozen source snapshots of adjacent days, while
-- the old validator looked only at recording_joined_sources (which exists
-- only after an adjacent day was sealed).  That made the first day in an
-- otherwise valid batch fail with "joined cross-day boundary differs".
-- Keep current-day checks on recording_joined_sources, but resolve only the
-- two adjacent-day lookups from immutable snapshots.
DO $$
DECLARE
  function_sql TEXT;
  rewritten_sql TEXT;
  previous_join CONSTANT TEXT := 'JOIN recording_joined_sources s ON s.stream_day_id = previous_day.id';
  next_join CONSTANT TEXT := 'JOIN recording_joined_sources s ON s.stream_day_id = next_day.id';
BEGIN
  SELECT pg_get_functiondef(p.oid)
    INTO function_sql
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
   WHERE n.nspname = current_schema()
     AND p.proname = 'validate_recording_joined_stream_day'
     AND pg_get_function_identity_arguments(p.oid) = 'p_stream_day_id bigint';

  IF function_sql IS NULL THEN
    RAISE EXCEPTION 'joined stream-day validator function is missing';
  END IF;
  IF position(previous_join IN function_sql) = 0 OR position(next_join IN function_sql) = 0 THEN
    RAISE EXCEPTION 'joined stream-day validator neighbor clauses differ';
  END IF;

  rewritten_sql := replace(function_sql, previous_join,
    'JOIN recording_joined_source_snapshots s ON s.stream_day_id = previous_day.id');
  rewritten_sql := replace(rewritten_sql, next_join,
    'JOIN recording_joined_source_snapshots s ON s.stream_day_id = next_day.id');
  EXECUTE rewritten_sql;
END
$$;
