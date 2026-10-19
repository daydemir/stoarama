BEGIN;

-- Metadata-only NAS stat inventories. Completion verifies HEAD length and the
-- signed uploader-declared SHA metadata; it is not a fresh server payload hash.
CREATE TABLE nas_space_audits (
  connection_id BIGINT NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
  audit_id UUID NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('scanning','uploading','complete','failed')),
  client_version TEXT NOT NULL DEFAULT '',
  summary JSONB NOT NULL CHECK (jsonb_typeof(summary)='object'),
  artifact_object_key TEXT NOT NULL DEFAULT '',
  artifact_size_bytes BIGINT NOT NULL DEFAULT 0 CHECK (artifact_size_bytes BETWEEN 0 AND 2147483648),
  artifact_sha256 TEXT NOT NULL DEFAULT '' CHECK (artifact_sha256='' OR artifact_sha256 ~ '^[0-9a-f]{64}$'),
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at TIMESTAMPTZ,
  PRIMARY KEY(connection_id,audit_id),
  CHECK ((artifact_size_bytes=0 AND artifact_sha256='' AND artifact_object_key='') OR
         (artifact_size_bytes>0 AND artifact_sha256<>'' AND artifact_object_key<>'')),
  CHECK (state NOT IN ('uploading','complete') OR artifact_size_bytes>0),
  CHECK ((state='complete')=(completed_at IS NOT NULL))
);
CREATE INDEX nas_space_audits_latest_idx ON nas_space_audits(connection_id,started_at DESC);

COMMIT;
