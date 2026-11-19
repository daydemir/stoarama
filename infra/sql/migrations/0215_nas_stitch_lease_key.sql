-- Ordinary NAS attempts retain their established connection identity. A
-- Durham attempt has its own key owner, which cannot become NULL by deletion.
ALTER TABLE nas_stitch_tasks
 ADD COLUMN lease_api_key_id bigint REFERENCES account_api_keys(id),
 ADD COLUMN registration_abandoned_at timestamptz,
 ADD COLUMN registration_abandoned_reason text,
 ADD CONSTRAINT nas_stitch_abandoned_registration_valid CHECK (
  (registration_abandoned_at IS NULL AND registration_abandoned_reason IS NULL)
  OR (registration_abandoned_at IS NOT NULL AND registration_abandoned_reason IS NOT NULL
      AND state='verified' AND cleanup_plan_id IS NULL AND lease_api_key_id IS NOT NULL
      AND length(registration_abandoned_reason) BETWEEN 1 AND 500)
 );
