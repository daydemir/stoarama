-- Immutable identity projection only. Apply this migration before the concurrent
-- index prerequisite for 0207. Never replace this function under a live index:
-- a changed projection requires a new function/index pair.
CREATE FUNCTION nas_cleanup_manifest_keys(manifest jsonb) RETURNS text[]
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
 SELECT ARRAY(SELECT (e->>'kind')||':'||((e->>'source_id')::bigint)::text
 FROM jsonb_array_elements(manifest) e)
$$;
