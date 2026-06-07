BEGIN;

-- Relabel the stream provider value WOLFF_EU_SQUARES -> EU_SQUARES.
-- The provider string is stored on streams.provider and source_candidates.provider.
-- Idempotent: re-running matches zero rows once the relabel is applied.

UPDATE streams
SET provider = 'EU_SQUARES'
WHERE provider = 'WOLFF_EU_SQUARES';

UPDATE source_candidates
SET provider = 'EU_SQUARES'
WHERE provider = 'WOLFF_EU_SQUARES';

COMMIT;
