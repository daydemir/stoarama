# Full-bucket R2 partition

`stoaramactl r2-inverse partition --picks /path/to/r2-picks.json --out /new/report/dir`

This command reads the entire configured bucket without a prefix filter. It
loads the catalog and protections in a PostgreSQL read-only repeatable-read
transaction, then writes `partition.jsonl` (every listed key, class and reason)
and `summary.json` (per-class object counts and bytes, exactly reconciled to the
listing total). It never calls DELETE or changes database rows, and requires no
writer fence or drain evidence. Only a complete successful report is usable;
partial JSONL without summary is an interrupted report. Output directories must
be new, so earlier evidence is never overwritten.

Protected classes: Keep Everything by stream or a known stream-folder path;
keep28 by pinned stream window (including every collated source); permanent
incident roster and dependent outputs; cleanup reservations and unretired or
ever-issued source holds; retirement reservations/receipts; pending restores;
joined source snapshots, snapshotting scopes and dry-run scopes; A1 `blocked` items by exact bucket/key regardless of
inventory; delivery keys and source/output dependencies; stitch tools under
`experimental/v85-retired-WORK-custody/`; unknown or ambiguous identities.
Delivery remains protected pending Deniz's answer. No hold is inferred released
from an exact NAS inventory record or a changed cleanup receipt.

Permanent incidents have one immutable `nas_cleanup_incident_roster` descriptor
view (53 direct identities, 25 exact R2 keys, 292 raw identities including the
1414 collated-source dependencies). The compatibility views
`nas_cleanup_incident_files` and
`nas_cleanup_incident_sources` preserve their OIDs and derive from it, independently
of mutable plan/receipt state. Kind namespaces and literal controller-supplied
R2 keys are preserved; no R2 sidecar identity is inferred. Legacy release/owner
checks and SQL reservation/output guards consume the same canonical roster.
Missing required tables/views or unreadable holds fail the command. All unknown
keys appear individually in JSONL and are ineligible for deletion. `deletable`
is a planning classification only; this report grants no wire authority.

The sole controller runs this read-only on Jelly after merge. Compare total
bucket bytes with the approximately 53 TB bill, inspect unknown counts, and
retain the report as durable evidence. No production partition was run by the
implementation agent. Collated outputs are held in this release, including outside-window outputs;
raw keys with multiple catalog identities remain unknown. The executor is a separate change and has additional
live fence, drain, intent, budget and provider checks.

## Raw-only executor (separate release)

The sole controller can run a bounded batch with this interface. D-min remains
a metadata feeder plus safety/space decision; it has no R2 apply hook. A later
reviewed integration can call this interface after the drain/canary gate:

```
stoaramactl r2-inverse apply \
  --manifest REPORT/partition.jsonl --expected-sha256 PARTITION_SHA \
  --summary REPORT/summary.json --expected-summary-sha256 SUMMARY_SHA \
  --picks PICKS --ledger PRIVATE_LEDGER \
  --drain-evidence DRAIN_JSON --expected-drain-evidence-sha256 DRAIN_SHA \
  --workers 16 --max-items 100 --max-bytes CANARY_BYTES \
  --budget-24h-bytes 0
```

The rolling budget defaults to unlimited (`--budget-24h-bytes 0`) for both inverse
and NAS-backed keep28 apply. A positive value sets an optional limit; negative
values are rejected. Byte accounting, including unresolved intents, remains
conservatively charged by minute over the preceding 24h in either mode. The
default 100-object limit is the canary size; subsequent tick calls may increase
`--max-items` and the required positive per-run byte bound.
Only raw keys with one certain catalog identity and no protection are eligible.
Collated, KE, keep28, tools, delivery, held and unknown keys are never deleted.
The partition and picks must match the pinned policy and be no older than 24h.
The tick can generate a new read-only partition when its current one expires.

Each new key commits an ordinary source reservation and a permanent key intent
before wire, charges its byte budget, and fsyncs a private append-only JSONL
ledger. The network phase takes the metadata barrier first in shared mode, then
ordered joined/admission locks, allowing independent keys to run concurrently. A common live delivery predicate includes content/path aliases
and current collated-output dependencies without relying on a refreshed source
cache; admission of those protections and output membership changes is fenced. It exercises the installed purge guards before DELETE, rolls
that probe back, HEAD-checks listing/catalog ETag, size and the fixed pre-fence LastModified cutoff, and
rechecks live protections, controller drain evidence and migration 0198 at the
wire. The private concrete executor verifier issues authority only after these checks
and independently commits a permanent one-issuance row. No exported approval
callback or generic R2 delete API is added. Repeated verification cannot remint
authority. The entire per-key network phase has a 30-second deadline, including
HEAD, multipart inspection and confirming HEAD; timeout retains the intent for
reconciliation. There is one DELETE attempt, no SDK replay or redirects. Success requires
a confirming 404. Receipt and raw purged_at publication happens separately with
exclusive admission locks, without upgrading a network transaction's locks.

An existing intent is never retransmitted, including a crash before wire. On
restart, a 404 reconciles its receipt and ledger; a present object stays held as
`ambiguous_held` while unrelated fresh keys can proceed. Preserve the ledger and
DB records. There is no automated intent expiry, removal, or ambiguous retry.
A damaged/partial local ledger fails closed; it is never truncated or repaired.

Drain evidence is a controller-reviewed, SHA-pinned JSON artifact, bound to the
exact bucket, endpoint and durable fence activation/deadline. It must include
`bucket`, `endpoint_sha256`, `fence_activated_at`, `drain_not_before`,
`observed_at`, `deployed_writer_coverage_evidence`, and
`proof`, and `accepted_put_completion_evidence`. Use
`proof: accepted_put_completion_v1` for substantiated terminal evidence. The observation is strictly after the real deadline. Completion evidence remains bound
to that immutable fence; it does not need a daily reapproval. References must substantiate
all deployed writer coverage and completion of externally accepted PUTs.
A string claiming completion or capability expiry does not establish that fact.
The implementation agent creates none of this production evidence.

Both executors accept the explicit `owner_fence_policy_20261118_v1` route in
[the owner fence policy contract](r2-owner-fence-policy.md). It replaces the old
B1-only key-policy route, pins the committed owner decision and this partition
and summary, and explicitly accepts the final HEAD/unconditional DELETE race
without claiming external PUT completion. Exact live fence binding, six live
wire/capability vetoes, source identity, holds and B2 NAS custody remain mandatory.

Controller canary gate after migration 0210 and fresh bound owner evidence:

1. Record baseline provider bucket bytes and object count using the controller's
   read-only metrics. Pin exactly 100 eligible raw keys and their total bytes.
2. Run one 100-object batch with an explicit per-run byte cap and `--budget-24h-bytes 0`. On any error,
   preserve evidence; reconcile existing intents by HEAD before continuing.
3. Require 100 distinct absent ledger keys = 100 DB absent events = 100 provider
   removals, and HEAD 404 for all 100. Provider bucket bytes must fall; account for
   metric publication delay explicitly. No double-counting reconciliations.
4. Require sustained >=8 objects/s measured by elapsed batch wall time. The CLI
   prints `deleted`, `reconciled`, `ambiguous_held`, `bytes`, `elapsed_seconds`,
   and `objects_per_second`. Synthetic-provider throughput is not this gate.
5. Only after every gate passes, configure larger controller-owned batches.
   Tick integration is a later change.
   Keep the same durable ledger across runs. Preserve reports outside temp.

Full-cardinality startup cost and memory remain unmeasured: each invocation reads
the manifest, private ledger and historical intent keys. The offline throughput
fixture includes 10,010 collated outputs but does not establish 4.17M-key scale.

Protection metadata changes take the epoch-row barrier with NOWAIT and can return
SQLSTATE 55P03 during an active batch; retry metadata admission in a new
transaction. They require READ COMMITTED so an earlier snapshot cannot miss a
committed retirement. Cleanup receipts/control do not take this barrier or a new
global lock. New cleanup recovery admission instead locks raw clip rows in order
and rejects a committed reservation; retired items cannot reactivate a hold.
Output IDs and source-hold identities cannot be rebound.

COPY reimport keeps its existing key-before-row order. Already-protected COPY
progress and unchanged verified rows take no new key lock. A clean verified row
gaining protection requires READ COMMITTED and tries the same key lock without
waiting; a busy key returns 55P03, and a committed reservation vetoes adoption.

The CLI pool has 20 connections. Workers default to 16 and may use at most the
pool capacity minus two (18 here), retaining the session and independent issuance
connections. Invalid capacity fails before any reservation or budget charge.

## Keep28 RAW with fresh NAS custody (B2)

`r2-inverse apply-keep28` takes the same bounded, SHA-pinned partition, picks,
ledger, drain and budget arguments as `apply`. Use a **separate B2 ledger**;
every B2 record contains `lane: keep28_raw_v1`. Both commands reject the other
lane's ledger before any append. B1 still refuses keep28. B2 admits only a unique
managed RAW identity overlapping pinned keep28, in account 47 / NAS connection
13, with a known non-KE owner choice. All existing KE stream/path, delivery,
incident, live hold, exact A1 blocked-key, purge, unknown, tool-prefix and
collated exclusions remain. A broad partition can label collated/ambiguous
keep28 keys; that classification cannot authorize them here.

Selection intersects the report with fresh exact inventory in `verified_at`,
source-ID order. Reservation and final authorization both use the existing
`r2RetireLockNASIdentity` lock: exact connection/source/path/size/SHA, present,
both verified and server-received timestamps within 24 hours, exact sidecar
path/size/SHA. The complete proof is persisted with the permanent B2 intent and
compared again under lock, including timestamps and sidecar. Old October 30
inventory does not qualify. The final inventory row stays locked through DELETE
and confirming HEAD404. Each PostgreSQL clock sample fixes custody expiry as
one absolute local monotonic instant, 2 seconds early (`keep28CustodyMargin`).
The authority keeps the running minimum across every sample, and every later
handoff (first request byte, socket write deadline, confirming HEAD) ends at it
or the existing 30-second cap, so neither a pause after sampling nor local
wall-clock skew can extend authority. Selection takes only keys whose custody
outlasts one network timeout plus the margin, so a key custody would skip uses
no item or byte allowance. Reservation repeats that check under lock: a key
whose custody shortened or aged out after selection is counted as
`custody_short` and skipped before any reservation, intent, budget charge or
ledger record, and fresh custody retries it. Selection admits only exact,
complete inventory evidence, so a legacy row without sidecar evidence is
ineligible rather than a batch failure. Reservation requires the identity
selection observed (source ID, catalog path, size, media SHA, sidecar digest
and size) before it considers custody age; any difference, or a changed
state, stops the batch. A job is done only when a clean batch selected
nothing. Job totals sum per-batch counts; held keys and `last_modified_held`
are snapshots of the latest batch.

Custody is a 24-hour freshness policy on an existing, verified NAS copy, not a
physical deadline. The requirement is that custody is valid at the last
PostgreSQL sample with at least the 2-second margin to spare. The accepted
residuals, which have no data-loss consequence and get no further machinery:

- the provider executing an already-accepted DELETE after expiry, bounded by
  the 30-second network timeout;
- a PostgreSQL clock step larger than the margin after the last sample;
- a host suspend during which the monotonic clock stops.

Migration 0204 creates independent `r2_keep28_intents`, `r2_keep28_attempts`,
`r2_keep28_events` and a pending-source identity guard. Ordinary reservations
and the conservative byte budget remain shared protections. B2 issues its
single-use authority from its own lane tables and adds the custody proof and
custody deadline to every check, but sends over the same one-attempt first-wire
transport as B1: a fresh HTTP/1.1 connection per DELETE, the batch send gate on
the first request byte, and the same lost-response classification. The first
request byte is refused at or after the DELETE's deadline, which for B2 is never
later than custody expiry, and the socket write deadline is set to that instant.
A response counts as received when its first byte is read from the connection;
a failed DELETE is classified only after its connection is closed and any read
under way has returned. A B2 key whose DELETE lost its response stays held and
is never retransmitted. An
existing B2 intent is never retransmitted, even after a stop before wire.
Restart treats held B2 intents exactly as B1's: an issued key whose object is
gone is reported `held_absent`, an unissued one stops the run, and a present
object must still match and remains `ambiguous_held`. `r2-inverse job-keep28`
runs B2 batches with the same per-batch owner evidence and exact readback, over
the B2 tables, as `r2-inverse job` does for B1. Its batches default to the
100-object canary size, and it refuses to start without explicit `--max-bytes`
and `--budget-24h-bytes` (0 = unlimited). Both jobs refuse more workers than the
database pool allows before writing any partition or evidence.

Deniz's November 17 order to delete group 3 that is on the NAS supersedes the
stitch-proof prerequisite **only for this keep28 RAW lane**. This new command
accepts no stitch-proof and never calls the legacy stitch-gated retirement
entry point. The old `r2-retire` command and its stitch-proof gate are unchanged.
Final HEAD identity, the strict pre-fence cutoff, the immutable 0198 fence with
its owner-adjusted 0210 deadline, and pinned owner evidence still apply.

Only the sole production controller may deploy and run this command. Use the
same 100-object canary gate, but reconcile **B2** DB events, B2 ledger and exact
provider removals separately from B1. Require HEAD404, bucket-byte reduction and
at least 8 objects/second by full batch wall time before larger batches.
Production readiness and throughput are not established by offline tests.

COPY ordinary import still does not re-lease verified rows. The separately
reviewed PR2 adds optional bounded verify-only refresh through D; see
[finish tick](../ops/jelly/finish/README.md#bounded-keep28-custody-refresh-pr2).
Its controller flag and queue status grant no deletion authority. No timestamp
refresh without hashing, manual reset, NAS scanner or reduced resource guard
is supplied. The recorded 1,158,889 RAW / 16.44 TB requires about 5.37 days at 150
verifications/minute or 2.01 days at 400/minute (~95 MB/s of hashing). Memory and
load can hold the worker entirely. Selection and startup memory at full
cardinality remain unmeasured: B2 retains the report's keep28 entries in memory,
and both lanes read their historical intents and ledger. Use bounded controller
batches and measure startup/RSS before scaling.

## Delivery page exemptions (0225)

Every link on the public delivery page is a `delivery_keep_holds` row, and no
lane may retire one. Migration 0225 lets exactly the page keys Deniz later
approved for deletion lose that protection, one class at a time, and only while
the class's gate is open. The gate lives in SQL (`r2_delivery_exempt`), so the
0190 reservation/receipt guard, the collated lane and any stale binary all read
the same answer. Only these exact delivery objects are exempted. The derived
RAW delivery guard (`r2_inverse_delivery_held`) stays mandatory in every lane
and mode, in the partition and at live authorize: a RAW source of a delivery
output is released only by the separate reviewed per-source consumer, never by
this lane.

| Class | Page rows | Gate |
| --- | --- | --- |
| `outside_window` | `video_collated` under `joined/goodplus-*` or `joined/nightly-collation-v2/`, outside every keep window (4,348, 0.582 TB) | open once seeded |
| `confident_repair` | `video_assembly` under `experimental/stitching-confident-repairs/`, no catalog row (9,232, 3.377 TB) | page retirement marker exists |
| `in_window` | `video_collated` inside a keep window (10,893, 2.347 TB) | page retirement marker exists **and** the exact output is NAS-certified (`r2_delivery_nas_certified`) |

An exemption binds the exact `(bucket, object_key, etag, size_bytes)` of the
live page row and stores the class's verbatim owner decision and the roster
SHA-256. It is append-only. If the page row's identity ever drifts, the key is
held again. Keep-everything delivery videos (1.0 TB) are never exempted: the
seed command refuses any key whose stream is `all` in the pinned picks, and the
lane refuses it again. `r2_delivery_nas_certified(output_id, bucket, key, etag,
size, sha256)` returns false until the follow-up to the stitching thread's NAS
handoff (#574, 0224) binds it to `delivery_nas_handoff_certified`: an exact
match on output_id, bucket, object_key, etag, size_bytes and sha256 with
connection_id 13. Until then `in_window` stays closed.

Exempt keys still need everything else the collated lane checks: the owner
fence evidence, KE, a decided stream choice, live holds, the KE epoch, a
per-key HEAD identity against the listed ETag and size (which must equal the
exemption's), one durable intent and attempt per key, a fresh HTTP/1.1
connection per DELETE behind the send gate, and the job's exact batch
readback. A lost reply holds only its own key. Confident repairs retire under
kind `delivery_repair`, keyed by exemption id; outside- and in-window videos
retire as `collated`. Receipts carry `owner_delivery_page_<class>`.

### Controller order

1. Build a class roster read-only (below), pin its SHA-256, then seed it:
   `stoaramactl r2-inverse exempt --class CLASS --roster FILE
   --expected-roster-sha256 HEX --picks docs/records/r2-picks-20261118.json
   --expected-picks-sha256 HEX`. The whole roster is admitted in one
   transaction, or nothing is.
2. `outside_window` can be seeded and retired now:
   `stoaramactl r2-inverse job --lane collated ...`.
3. Handoff (a) and (b): the in-window videos are copied and certified on the
   NAS and the static CSV+JSON manifest of old links to NAS paths is published.
4. Handoff (c): take the old page down, then run
   `stoaramactl r2-inverse page-retired` once. This opens `confident_repair`.
5. Seed `confident_repair` and run the collated job. Seed `in_window` once the
   0224 follow-up certifies the copies; until then it stays held.

### Read-only roster queries

The in-window roster at `records/delivery-inwindow-roster-20261119/` (SHA-256
`da4a4a7f4c98409ad0927878ad9995327f2a749d717e1e47b276e19cec06e8ca`) is already
pinned. The other two use the same picks list. Generate it from the pinned picks
file; this reproduces the in-window query's `VALUES` list byte for byte:

```sh
jq -r '[.choices | to_entries[] | select(.value.choice // "" != "")
 | "(\(.key),'"'"'\(.value.choice)'"'"','"'"'\(.value.keep_start_utc)'"'"'::timestamptz,'"'"'\(.value.keep_end_utc)'"'"'::timestamptz)"] | join(",")' \
 docs/records/r2-picks-20261118.json
```

Put its output in place of `PICKS`, and run each query in `psql` inside
`BEGIN READ ONLY; … ROLLBACK;`. Each writes JSONL sorted by key.

`outside_window`: the same page family as the in-window roster, on a decided
non-KE stream, with no overlap with that stream's keep window.

```sql
\copy (WITH picks(stream_id,choice,ws,we) AS (VALUES PICKS), c AS (SELECT h.object_key,h.size_bytes,h.etag,o.id output_id,o.sha256,o.start_at,o.end_at,hr.recording_id,r.stream_id FROM delivery_keep_holds h JOIN recording_collation_outputs o ON o.object_key=h.object_key JOIN recording_collation_hours hr ON hr.id=o.collation_hour_id JOIN recordings r ON r.id=hr.recording_id JOIN picks p ON p.stream_id=r.stream_id WHERE h.bucket='stoarama' AND h.kind='video_collated' AND (h.object_key LIKE 'joined/goodplus-%' OR h.object_key LIKE 'joined/nightly-collation-v2/%') AND p.choice<>'all' AND p.ws IS NOT NULL AND NOT (o.start_at < p.we AND o.end_at > p.ws)) SELECT jsonb_build_object('bucket','stoarama','object_key',object_key,'size_bytes',size_bytes,'etag',etag,'output_id',output_id,'sha256',sha256,'start_at',start_at,'end_at',end_at,'recording_id',recording_id,'stream_id',stream_id)::text FROM c ORDER BY object_key) TO 'outside-window-roster.jsonl'
```

`confident_repair`: every page repair on a decided non-KE stream.

```sql
\copy (WITH picks(stream_id,choice,ws,we) AS (VALUES PICKS) SELECT jsonb_build_object('bucket',h.bucket,'object_key',h.object_key,'size_bytes',h.size_bytes,'etag',h.etag,'sha256',h.sha256,'recording_id',h.recording_id,'stream_id',r.stream_id)::text FROM delivery_keep_holds h JOIN recordings r ON r.id=h.recording_id JOIN picks p ON p.stream_id=r.stream_id WHERE h.bucket='stoarama' AND h.kind='video_assembly' AND h.object_key LIKE 'experimental/stitching-confident-repairs/%' AND p.choice IN ('none','st','keep') ORDER BY h.object_key) TO 'confident-repair-roster.jsonl'
```

Expect 4,348 rows / 0.582 TB and 9,232 rows / 3.377 TB. A shortfall means some
keys are on a keep-everything or undecided stream; they stay on the page and in
R2. Report the difference to Deniz rather than widening a query.
