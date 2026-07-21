WITH routed AS (
  UPDATE recordings rec
  SET capture_via = 'relay', updated_at = now()
  FROM streams st, accounts a
  WHERE rec.stream_id = st.id
    AND rec.account_id = a.id
    AND a.id = 47
    AND a.name = 'MIT SCL'
    AND rec.status = 'active'
    AND rec.capture_via = 'cloud'
    AND (
      st.provider = 'SDOT'
      OR lower(st.source_url) LIKE 'https://61e0c5d388c2e.streamlock.net/%'
    )
  RETURNING rec.id
)
UPDATE recording_jobs job
SET status = 'canceled',
    lease_owner = NULL,
    lease_expires_at = NULL,
    updated_at = now()
WHERE job.recording_id IN (SELECT id FROM routed)
  AND job.status = 'leased'
  AND job.kind = 'continuous_window';
