-- Collation v2 (generation 2): robust joined hours that replace generation-1
-- joined media in place. Generation-1 rows and objects are never mutated or
-- deleted here; a generation-2 hour supersedes its generation-1 hour by pointer.
SET LOCAL lock_timeout = '5s';

CREATE TABLE recording_collation_hours (
  id BIGSERIAL PRIMARY KEY,
  hour_id TEXT NOT NULL UNIQUE CHECK (hour_id ~ '^[a-z0-9][a-z0-9-]{0,62}__recording-[1-9][0-9]*__date-[0-9]{4}-[0-9]{2}-[0-9]{2}__hour-(0[1-9]|1[0-2])__generation-[2-9][0-9]*$'),
  batch_id TEXT NOT NULL CHECK (batch_id ~ '^[a-z0-9][a-z0-9-]{0,62}$'),
  generation INTEGER NOT NULL CHECK (generation >= 2),
  recording_id BIGINT NOT NULL REFERENCES recordings(id) ON DELETE RESTRICT,
  local_date DATE NOT NULL,
  delivery_hour SMALLINT NOT NULL CHECK (delivery_hour BETWEEN 1 AND 12),
  status TEXT NOT NULL CHECK (status IN ('collated', 'gap_only', 'quarantine_only')),
  supersedes_joined_hour_id BIGINT UNIQUE REFERENCES recording_joined_hours(id) ON DELETE RESTRICT,
  manifest_object_key TEXT NOT NULL UNIQUE,
  manifest_sha256 TEXT NOT NULL CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
  manifest JSONB NOT NULL CHECK (jsonb_typeof(manifest) = 'object'),
  source_clip_count INTEGER NOT NULL CHECK (source_clip_count >= 0),
  included_clip_count INTEGER NOT NULL CHECK (included_clip_count >= 0),
  excluded_clip_count INTEGER NOT NULL CHECK (excluded_clip_count >= 0),
  join_count INTEGER NOT NULL CHECK (join_count >= 0),
  split_count INTEGER NOT NULL CHECK (split_count >= 0),
  registered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (source_clip_count = included_clip_count + excluded_clip_count),
  UNIQUE (recording_id, local_date, delivery_hour, generation)
);

CREATE TABLE recording_collation_outputs (
  id BIGSERIAL PRIMARY KEY,
  collation_hour_id BIGINT NOT NULL REFERENCES recording_collation_hours(id) ON DELETE RESTRICT,
  part INTEGER NOT NULL CHECK (part > 0),
  parts INTEGER NOT NULL CHECK (parts >= part),
  object_key TEXT NOT NULL CHECK (object_key ~ '^joined/[a-z0-9][a-z0-9-]{0,62}/objects/[0-9a-f]{64}\.mp4$'),
  nas_relative_path TEXT NOT NULL UNIQUE CHECK (nas_relative_path = btrim(nas_relative_path) AND nas_relative_path <> ''
    AND left(nas_relative_path, 1) <> '/' AND nas_relative_path !~ '(^|/)\.\.(/|$)'),
  size_bytes BIGINT NOT NULL CHECK (size_bytes > 0),
  sha256 TEXT NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
  start_at TIMESTAMPTZ NOT NULL,
  end_at TIMESTAMPTZ NOT NULL CHECK (end_at > start_at),
  source_clip_ids BIGINT[] NOT NULL CHECK (cardinality(source_clip_ids) > 0),
  r2_verified_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (collation_hour_id, part)
);

CREATE INDEX recording_collation_hours_recording_idx ON recording_collation_hours (recording_id, local_date, delivery_hour);
