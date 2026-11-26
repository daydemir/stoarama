-- While a NAS cleanup plan is enabled (or a deletion was issued in the last two
-- minutes), the public GSS delivery serves only this pinned generation, never
-- whatever public/current.json names (fence from 0167). It was a code constant;
-- the swap step now moves it with current.json, so a republish needs no deploy.
-- Serving still requires every file and asset to match delivery_keep_holds.
-- No row means the fenced public delivery fails closed.
CREATE TABLE gss_public_generation_pin (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 generation text NOT NULL CHECK(generation ~ '^[0-9a-f]{32}$'),
 pinned_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
-- Seed: the live best-14 ledger generation (#628), built from ae14e746 by
-- backend/scripts/gss/republish.sh.
INSERT INTO gss_public_generation_pin(generation) VALUES('549402679c9d5f158cece1c4f77bad16');
