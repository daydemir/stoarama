BEGIN;

CREATE TABLE IF NOT EXISTS stream_capture_type_tag_cleanup_0068_backup (
  stream_id BIGINT PRIMARY KEY,
  tags_before TEXT[] NOT NULL,
  capture_type_tags TEXT[] NOT NULL,
  backed_up_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO stream_capture_type_tag_cleanup_0068_backup (stream_id, tags_before, capture_type_tags)
SELECT
  s.id,
  COALESCE(s.tags, ARRAY[]::text[]),
  COALESCE((
    SELECT array_agg(t.tag ORDER BY t.ord)
    FROM unnest(COALESCE(s.tags, ARRAY[]::text[])) WITH ORDINALITY AS t(tag, ord)
    WHERE lower(btrim(t.tag)) LIKE 'capture_type:%'
  ), ARRAY[]::text[])
FROM streams s
WHERE EXISTS (
  SELECT 1
  FROM unnest(COALESCE(s.tags, ARRAY[]::text[])) AS t(tag)
  WHERE lower(btrim(t.tag)) LIKE 'capture_type:%'
)
ON CONFLICT (stream_id) DO NOTHING;

UPDATE streams s
SET
  tags = COALESCE((
    SELECT array_agg(t.tag ORDER BY t.ord)
    FROM unnest(COALESCE(s.tags, ARRAY[]::text[])) WITH ORDINALITY AS t(tag, ord)
    WHERE lower(btrim(t.tag)) NOT LIKE 'capture_type:%'
  ), ARRAY[]::text[]),
  updated_at = now()
WHERE EXISTS (
  SELECT 1
  FROM unnest(COALESCE(s.tags, ARRAY[]::text[])) AS t(tag)
  WHERE lower(btrim(t.tag)) LIKE 'capture_type:%'
);

COMMIT;
