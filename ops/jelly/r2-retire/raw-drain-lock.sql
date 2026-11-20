-- RAW drain receipt: holders of the raw-drain bigint advisory key
-- 7343226011173, read from pg_locks. Include it with \ir from an ops/receipt
-- script; it prints one "<label>|<holders>" line so each receipt stays
-- distinguishable.
--
-- A bigint advisory key is stored as high 32 bits in classid and low 32 bits in
-- objid, with objsubid=1 -- for this key, classid=1709 objid=3126902309. Bigint
-- advisory keys are database-local, so only the current database can hold one.
-- Never filter on classid=0: it matches nothing, and a count of 0 from it looks
-- exactly like a clean drain while the real key is still held.
--
-- Zero holders is only one part of a RAW drain proof. The quiesce/terminal job
-- must also have finished, and the last write to the object must precede this
-- observation; a zero observed before the writer stopped is not evidence.
--
-- Callers may set :lock_label before \ir to label their own receipt.
\set ON_ERROR_STOP on

\if :{?lock_label}
\else
\set lock_label lock_1173_holders
\endif

SELECT :'lock_label' AS q, count(*) AS holders
 FROM pg_locks
 WHERE locktype = 'advisory'
   AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
   AND classid = ((7343226011173::bigint >> 32) & 4294967295)::oid
   AND objid = (7343226011173::bigint & 4294967295)::oid
   AND objsubid = 1
   AND granted;
