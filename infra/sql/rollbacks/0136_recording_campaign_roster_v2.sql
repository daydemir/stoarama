-- Separately reviewed operator rollback artifact. This is not an automatic
-- migration Down and must not be run without explicit production authority.
-- It restores the exact 0129 protection predicate without dropping the view,
-- preserving its OID, owner, grants, options, and external dependents.
CREATE OR REPLACE VIEW protected_campaign_recordings AS
SELECT DISTINCT t.account_id,e.recording_id
FROM recording_campaign_tracks t
JOIN recording_campaign_roster_entries e ON e.track_id=t.id
WHERE t.state IN('active','complete') AND e.status IN('protect','probation');
