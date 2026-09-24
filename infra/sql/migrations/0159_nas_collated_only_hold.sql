-- Collated-only NAS delivery (operator setting, per recording).
--
-- A nas_pull recording in collated_only mode keeps its raw 1-minute clips in
-- managed staging (R2) instead of handing them to the NAS raw feed; the NAS
-- receives the recording only as collated (joined) hour files. The decision is
-- frozen per clip at insert, beside the storage billing contract, so switching a
-- recording never strands clips that were already offered to the NAS and never
-- changes the billing of an existing clip.
--
-- Billing: a held clip gets storage contract mode nas_collated_hold, which the
-- stream_hour_month facts exclude (see recorder_metering.go). Without that mode
-- a held clip would stay unreleased past the 24h nas_pull staging grace and be
-- metered as managed storage. Recording-hour metering is unaffected.
--
-- Nothing is enabled here: every recording defaults to raw.
ALTER TABLE recordings
  ADD COLUMN nas_delivery_mode TEXT NOT NULL DEFAULT 'raw'
    CONSTRAINT recordings_nas_delivery_mode_check CHECK (nas_delivery_mode IN ('raw','collated_only')),
  ADD COLUMN nas_delivery_mode_updated_at TIMESTAMPTZ,
  ADD COLUMN nas_delivery_mode_reason TEXT NOT NULL DEFAULT ''
    CONSTRAINT recordings_nas_delivery_mode_reason_check CHECK (octet_length(nas_delivery_mode_reason) <= 512);

-- Existing rows already satisfy the widened set; NOT VALID skips a full-table
-- validation scan while inserts keep flowing (new rows are still checked).
ALTER TABLE clip_storage_billing_contracts DROP CONSTRAINT clip_storage_billing_contracts_mode_check;
ALTER TABLE clip_storage_billing_contracts
  ADD CONSTRAINT clip_storage_billing_contracts_mode_check
  CHECK (mode IN ('excluded','managed_monthly','nas_pull_monthly','nas_collated_hold')) NOT VALID;

CREATE OR REPLACE FUNCTION freeze_clip_storage_billing() RETURNS trigger AS $$
BEGIN
  INSERT INTO clip_storage_billing_contracts(clip_id,mode,authoritative)
  SELECT NEW.id, CASE
      WHEN sd.managed AND r.delivery='nas_pull' AND r.nas_delivery_mode='collated_only' THEN 'nas_collated_hold'
      WHEN sd.managed AND r.storage_retention_tier <> 'yearly_prepaid' AND r.delivery='nas_pull' THEN 'nas_pull_monthly'
      WHEN sd.managed AND r.storage_retention_tier <> 'yearly_prepaid' THEN 'managed_monthly'
      ELSE 'excluded'
    END, true
  FROM recordings r
  LEFT JOIN storage_destinations sd ON sd.id=NEW.storage_destination_id
  WHERE r.id=NEW.recording_id;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
