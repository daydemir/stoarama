-- The trigger from 0107 protects concurrent new inserts. This separate
-- transaction backfills old clips without rewriting or exclusively locking the
-- active recording_clips table. Classification is authoritative only from the
-- eligible date recorded by 0107; the already-open period remains manual.
INSERT INTO clip_storage_billing_contracts(clip_id,mode,authoritative)
SELECT c.id, CASE
    WHEN sd.managed AND r.storage_retention_tier <> 'yearly_prepaid' AND r.delivery='nas_pull' THEN 'nas_pull_monthly'
    WHEN sd.managed AND r.storage_retention_tier <> 'yearly_prepaid' THEN 'managed_monthly'
    ELSE 'excluded'
  END, false
FROM recording_clips c
JOIN recordings r ON r.id=c.recording_id
LEFT JOIN storage_destinations sd ON sd.id=c.storage_destination_id
ON CONFLICT(clip_id) DO NOTHING;
