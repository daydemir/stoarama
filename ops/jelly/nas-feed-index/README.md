# NAS feed index precreation

Do not execute this file as a deployment or migration hook. The sole cleanup
controller must first obtain a fresh review of a concrete production operation
bound to the actual endpoint, DB/table OIDs, live backend, natural batch boundary,
recovery, protected incident holds, and provider storage evidence.

The window requires master OFF in configuration and runtime; no enabled cleanup
plan or outstanding execution authority; the runner naturally stopped with its
owned STOP and a clean fully recovered last plan; no proof work, other DDL,
backend deployment, or client publication in progress; and fresh provider disk
metrics with at least 8 GB free. Preserve copy configuration and all policy,
fence, custody, inventory and recovery gates. The additive index grants no media
operation or R2 deletion authority.

Known long, visible application readers are allowed. CREATE INDEX CONCURRENTLY
waits for old transactions/snapshots itself. General transaction age is not an
idle prerequisite for this window. Opaque sessions and conflicting relation locks
must be identified in the concrete review, not silently treated as readers.
The 90s lock timeout deliberately covers the observed 55-60s read transactions;
the separate 30min statement timeout is the hard command bound. This is not a
change to any other index operation's reviewed guard or timeout.

Run precreate.sql in ONE autocommit psql invocation, with existing Jelly
credentials kept in its child environment. Never wrap it in BEGIN. There is no
IF NOT EXISTS, drop, automatic retry, transaction cancellation, or session kill.
Inspect failures and any INVALID remainder; further action needs its own review.
After success verify the exact valid/ready/live btree definition, its six ascending
keys, predicate and target relation, plus unchanged live/ownership/recovery and
storage barriers. A later plan-only EXPLAIN on PostgreSQL 18 needs its own bounded
read approval; local PostgreSQL 17 plans do not establish production performance.

A populated database without the precreated index refuses migration 0197. Empty
new databases initialize the index normally. Wrong or invalid definitions refuse;
the migration never repairs, drops or rebuilds one. Do not merge the feed change
before reviewed precreation is complete: that would make unrelated later backend
deploys depend on an index not yet built. The operator window is separately
reviewed; this README and SQL file grant no execution permission.

The fenced feed reads recording/window ownership and its page in one read-only
repeatable-read snapshot. Each recording contributes an ordered cursor stream,
with every window, maturity and hold predicate before its limit. PostgreSQL can
lazily merge those streams, then fetch only the selected payloads. Rendering only
typed database IDs and UTC window timestamps gives the planner concrete window
selectivity even for generic plans; account, cursor and page size remain bound
parameters. The covering index orders recording ID, cursor ID, start, end, size
and creation time. Its time predicates can reject outside-window index entries
before consulting a non-visible heap tuple.

Local evidence includes both positive and empty tails, generic/custom/automatic
plans on a two-million-row wide heap, and a second two-million-row fixture with
no visibility-map credit and an eligible backlog followed by an empty tail.
Resource budgets cover candidate tuples, heap visits, buffers and temporary I/O.
These are local access-path measurements, not a production latency claim or a
cold-storage benchmark. The handler's concurrent owner/hold/insert regression
fails under READ COMMITTED and passes under its required repeatable-read mode.
The empty-database migration takes its write-excluding lock before testing
emptiness; a concurrent first writer must force the populated-database refusal.
