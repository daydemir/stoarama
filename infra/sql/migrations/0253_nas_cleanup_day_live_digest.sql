-- Closing a day plan retires its pending and permitted units, so a retired plan
-- may have deleted none of its directory-days. Its digest must not block
-- admitting those days again: digest uniqueness covers live plans only.
-- Labels stay unique.
ALTER TABLE nas_cleanup_day_plans DROP CONSTRAINT nas_cleanup_day_plans_plan_sha256_key;
CREATE UNIQUE INDEX nas_cleanup_day_plans_live_digest ON nas_cleanup_day_plans(plan_sha256) WHERE mode<>'retired';
