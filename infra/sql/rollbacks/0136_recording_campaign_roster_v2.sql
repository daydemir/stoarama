-- Separately reviewed operator rollback artifact. This is not an automatic
-- migration Down and must not be run without explicit production authority.
-- It restores the exact 0129 protection predicate without dropping the view,
-- preserving its OID, owner, grants, options, and external dependents.
DO $$
DECLARE ns TEXT:=current_schema();
BEGIN
  IF ns IS NULL OR ns='pg_temp' OR ns LIKE 'pg_temp_%' OR cardinality(current_schemas(false))<>1 THEN
    RAISE EXCEPTION 'rollback requires exactly one controlled non-temporary schema';
  END IF;
  PERFORM set_config('search_path',quote_ident(ns)||',pg_catalog',true);
  EXECUTE format($view$
    CREATE OR REPLACE VIEW %1$I.protected_campaign_recordings AS
    SELECT DISTINCT t.account_id,e.recording_id
    FROM %1$I.recording_campaign_tracks t
    JOIN %1$I.recording_campaign_roster_entries e ON e.track_id=t.id
    WHERE t.state IN('active','complete') AND e.status IN('protect','probation')
  $view$,ns);
END $$;
