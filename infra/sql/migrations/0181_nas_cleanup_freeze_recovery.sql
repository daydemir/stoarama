-- The activation transaction holds public fence 7343226011101 from before
-- publication through commit/rollback. Its xid is a durable attempt identity;
-- acquiring that same fence proves no activation attempt is still in progress.
-- No timeout can safely substitute for that proof.
ALTER TABLE nas_cleanup_frozen_streams
 ADD COLUMN activation_attempt_id bigint NOT NULL DEFAULT txid_current(),
 ADD COLUMN created_at timestamptz NOT NULL DEFAULT clock_timestamp();

-- Keep a permanent singleton so FOR SHARE also fences maintenance starting
-- when it was previously inactive (an absent flag row would race INSERT).
CREATE TABLE nas_cleanup_maintenance (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 active boolean NOT NULL DEFAULT false,
 reason text NOT NULL DEFAULT '',
 started_at timestamptz
);
INSERT INTO nas_cleanup_maintenance(singleton) VALUES(true);
