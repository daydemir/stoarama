CREATE TABLE IF NOT EXISTS stream_source_revisions (
  id BIGSERIAL PRIMARY KEY,
  stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
  actor TEXT NOT NULL DEFAULT '',
  reason TEXT NOT NULL DEFAULT '',
  previous_source_url TEXT NOT NULL DEFAULT '',
  new_source_url TEXT NOT NULL DEFAULT '',
  previous_source_page_url TEXT NOT NULL DEFAULT '',
  new_source_page_url TEXT NOT NULL DEFAULT '',
  previous_source_family TEXT NOT NULL DEFAULT '',
  new_source_family TEXT NOT NULL DEFAULT '',
  previous_capture_type TEXT NOT NULL DEFAULT '',
  new_capture_type TEXT NOT NULL DEFAULT '',
  previous_execution_class TEXT NOT NULL DEFAULT '',
  new_execution_class TEXT NOT NULL DEFAULT '',
  metadata_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_stream_source_revisions_stream_created
ON stream_source_revisions (stream_id, created_at DESC);

CREATE TABLE IF NOT EXISTS stream_recording_incidents (
  id BIGSERIAL PRIMARY KEY,
  stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
  incident_type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'resolved')),
  first_observed_at TIMESTAMPTZ NOT NULL,
  last_observed_at TIMESTAMPTZ NOT NULL,
  opened_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  resolved_at TIMESTAMPTZ,
  last_notified_at TIMESTAMPTZ,
  notify_count INTEGER NOT NULL DEFAULT 0,
  details_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_stream_recording_incidents_open_unique
ON stream_recording_incidents (stream_id, incident_type)
WHERE status = 'open';

CREATE INDEX IF NOT EXISTS idx_stream_recording_incidents_status_observed
ON stream_recording_incidents (status, last_observed_at DESC);
