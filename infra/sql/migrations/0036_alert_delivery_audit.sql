BEGIN;

CREATE TABLE IF NOT EXISTS alert_delivery_events (
  id BIGSERIAL PRIMARY KEY,
  incident_id BIGINT REFERENCES stream_recording_incidents(id) ON DELETE CASCADE,
  stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
  recipient TEXT NOT NULL,
  message_type TEXT NOT NULL DEFAULT '',
  provider TEXT NOT NULL DEFAULT '',
  provider_message_id TEXT NOT NULL DEFAULT '',
  provider_status TEXT NOT NULL DEFAULT 'pending' CHECK (
    provider_status IN ('pending', 'accepted', 'delivered', 'opened', 'bounced', 'complained', 'failed', 'rejected')
  ),
  subject TEXT NOT NULL DEFAULT '',
  payload_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  provider_payload_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  error_text TEXT NOT NULL DEFAULT '',
  sent_at TIMESTAMPTZ,
  delivered_at TIMESTAMPTZ,
  opened_at TIMESTAMPTZ,
  bounced_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_alert_delivery_events_updated_at ON alert_delivery_events;
CREATE TRIGGER trg_alert_delivery_events_updated_at
BEFORE UPDATE ON alert_delivery_events
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_alert_delivery_events_incident_created
ON alert_delivery_events (incident_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_alert_delivery_events_stream_created
ON alert_delivery_events (stream_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_alert_delivery_events_status_created
ON alert_delivery_events (provider_status, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_alert_delivery_events_provider_message_id
ON alert_delivery_events (provider, provider_message_id)
WHERE provider_message_id <> '';

CREATE TABLE IF NOT EXISTS email_webhook_events (
  id BIGSERIAL PRIMARY KEY,
  provider TEXT NOT NULL,
  provider_event_id TEXT NOT NULL,
  event_type TEXT NOT NULL DEFAULT '',
  provider_message_id TEXT NOT NULL DEFAULT '',
  payload_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  processed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_email_webhook_events_provider_event
ON email_webhook_events (provider, provider_event_id);

CREATE INDEX IF NOT EXISTS idx_email_webhook_events_message_created
ON email_webhook_events (provider_message_id, created_at DESC);

COMMIT;
