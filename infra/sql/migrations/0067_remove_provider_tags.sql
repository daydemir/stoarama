BEGIN;

CREATE TABLE IF NOT EXISTS stream_provider_tag_cleanup_0067_backup (
  stream_id BIGINT PRIMARY KEY,
  tags_before TEXT[] NOT NULL,
  provider_tags TEXT[] NOT NULL,
  backed_up_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO stream_provider_tag_cleanup_0067_backup (stream_id, tags_before, provider_tags)
SELECT
  s.id,
  COALESCE(s.tags, ARRAY[]::text[]),
  COALESCE((
    SELECT array_agg(t.tag ORDER BY t.ord)
    FROM unnest(COALESCE(s.tags, ARRAY[]::text[])) WITH ORDINALITY AS t(tag, ord)
    WHERE lower(btrim(t.tag)) LIKE 'provider:%'
  ), ARRAY[]::text[])
FROM streams s
WHERE EXISTS (
  SELECT 1
  FROM unnest(COALESCE(s.tags, ARRAY[]::text[])) AS t(tag)
  WHERE lower(btrim(t.tag)) LIKE 'provider:%'
)
ON CONFLICT (stream_id) DO NOTHING;

UPDATE streams s
SET
  tags = COALESCE((
    SELECT array_agg(t.tag ORDER BY t.ord)
    FROM unnest(COALESCE(s.tags, ARRAY[]::text[])) WITH ORDINALITY AS t(tag, ord)
    WHERE lower(btrim(t.tag)) NOT LIKE 'provider:%'
  ), ARRAY[]::text[]),
  updated_at = now()
WHERE EXISTS (
  SELECT 1
  FROM unnest(COALESCE(s.tags, ARRAY[]::text[])) AS t(tag)
  WHERE lower(btrim(t.tag)) LIKE 'provider:%'
);

COMMIT;
