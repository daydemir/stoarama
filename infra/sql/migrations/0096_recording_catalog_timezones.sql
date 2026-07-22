SELECT rec.id
FROM recordings rec
JOIN streams st ON st.id = rec.stream_id
WHERE rec.status IN ('active', 'paused')
  AND st.deleted_at IS NULL
  AND BTRIM(st.local_timezone) <> ''
  AND EXISTS (SELECT 1 FROM pg_timezone_names zone WHERE zone.name = BTRIM(st.local_timezone))
  AND rec.cron_timezone <> BTRIM(st.local_timezone)
ORDER BY rec.id
FOR UPDATE OF rec;

WITH affected AS (
  SELECT rec.id
  FROM recordings rec
  JOIN streams st ON st.id = rec.stream_id
  WHERE rec.status IN ('active', 'paused')
    AND st.deleted_at IS NULL
    AND BTRIM(st.local_timezone) <> ''
    AND EXISTS (SELECT 1 FROM pg_timezone_names zone WHERE zone.name = BTRIM(st.local_timezone))
    AND rec.cron_timezone <> BTRIM(st.local_timezone)
)
UPDATE recording_jobs job
SET status = 'canceled',
    lease_owner = NULL,
    lease_expires_at = NULL,
    updated_at = now()
WHERE job.recording_id IN (SELECT id FROM affected)
  AND job.status IN ('pending', 'leased');

UPDATE recordings rec
SET cron_timezone = BTRIM(st.local_timezone),
    last_enqueued_fire_at = now(),
    next_fire_at = NULL,
    updated_at = now()
FROM streams st
WHERE rec.stream_id = st.id
  AND rec.status IN ('active', 'paused')
  AND st.deleted_at IS NULL
  AND BTRIM(st.local_timezone) <> ''
  AND EXISTS (SELECT 1 FROM pg_timezone_names zone WHERE zone.name = BTRIM(st.local_timezone))
  AND rec.cron_timezone <> BTRIM(st.local_timezone);
