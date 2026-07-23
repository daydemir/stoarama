ALTER TABLE relay_connectivity_alert_events
  ADD COLUMN account_id BIGINT REFERENCES accounts(id);

UPDATE relay_connectivity_alert_events event
SET account_id = node.account_id
FROM nodes node
WHERE node.id = event.node_id;

-- The alert product is intentionally MIT-SCL-only. Close legacy pending events
-- from every other organization so they cannot surface after this hard cut.
UPDATE relay_connectivity_alert_events
SET notified_at = now()
WHERE account_id <> 47 AND notified_at IS NULL;

ALTER TABLE relay_connectivity_alert_events
  ALTER COLUMN account_id SET NOT NULL;

CREATE INDEX relay_connectivity_alert_events_account_pending_idx
  ON relay_connectivity_alert_events (account_id, id)
  WHERE notified_at IS NULL;
