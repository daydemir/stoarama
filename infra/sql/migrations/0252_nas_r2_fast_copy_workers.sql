-- Fast-copy object streams per NAS connection, sent with every fast lease and
-- changed by an admin without a redeploy (replaces NAS_R2_FAST_COPY_WORKERS;
-- 16 was its production value on 2026-11-24).
ALTER TABLE connections ADD COLUMN r2_fast_copy_workers INTEGER NOT NULL DEFAULT 16
 CONSTRAINT connections_r2_fast_copy_workers_check CHECK (r2_fast_copy_workers BETWEEN 1 AND 32);
