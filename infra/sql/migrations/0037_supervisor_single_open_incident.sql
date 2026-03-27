WITH ranked AS (
  SELECT
    id,
    ROW_NUMBER() OVER (
      PARTITION BY stream_id
      ORDER BY updated_at DESC, id DESC
    ) AS rn
  FROM stream_recording_incidents
  WHERE status='open'
    AND incident_type IN ('down_10m', 'spotty_2h')
)
UPDATE stream_recording_incidents i
SET
  status='resolved',
  resolved_at=COALESCE(i.resolved_at, now()),
  last_observed_at=now(),
  updated_at=now()
FROM ranked r
WHERE i.id=r.id
  AND r.rn > 1;

DROP INDEX IF EXISTS idx_stream_recording_incidents_open_unique;

CREATE UNIQUE INDEX IF NOT EXISTS idx_stream_recording_incidents_open_unique
ON stream_recording_incidents (stream_id)
WHERE status='open'
  AND incident_type IN ('down_10m', 'spotty_2h');
