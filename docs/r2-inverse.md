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
authority. Each key's network calls and database steps have separate bounds, so
waiting on a sibling's protection locks never spends the network budget. The
network phase (identity HEAD, multipart inspection, the DELETE and the confirming
HEAD) has a 30-second deadline from when the wire transaction holds its locks.
Each database step (reservation, wire connection, wire preparation, the wire commit, each
publication attempt) gets its own 2 minutes. A timeout retains the intent for
reconciliation. There is one DELETE attempt, no SDK replay or redirects. A DELETE
whose request was fully written but which got no answer by its deadline is a lost
response: only its key is held. Success requires
a confirming 404 for the exact key. The confirming HEAD is a read, so it alone
retries, in the executor's own loop rather than the SDK's: at most 4 attempts of
at most 4 seconds each, with full-jitter backoff of at most 2 seconds between
them, under its own 22-second deadline inside the key's network deadline (and,
for keep-28, custody end). Each attempt is one request on its own fresh HTTP/1.1
connection with keep-alives off, so neither the transport nor the SDK can turn
one attempt into two: four attempts send exactly four HEADs. Only timeouts,
connection errors, 429, every 5xx (including a proxy's 520-599) and throttling
retry, and a provider's `X-Amz-Retry-After` never stretches the
backoff. It follows no redirect: a 404 from anywhere but the key cannot confirm
it. A 200 means the object remains: it fails at once. When every attempt fails,
the job stops and the key stays held, as a lost response does.

A publication step whose parent context ended first — the job cancelled or its
deadline passed before the step's own 2-minute bound — stops the job and holds
nothing; it is not an unpublished key. Only the step's own bound expiring, or
lock contention inside it, holds the key as `unpublished`.

Receipt and raw purged_at publication happens separately with exclusive admission
locks, without upgrading a network transaction's locks. It is bookkeeping for a
key a 404 already confirmed gone, so it retries up to 3 times when a step ran out
of its own 2-minute bound (waiting for a pool connection, a lock or a statement)
or lost to lock contention (55P03, 40P01, 40001). It disables the role's
`statement_timeout`, so a 57014 there can only be an explicit cancel, which stops
the job, as does any refusal or inconsistency. A failed publication COMMIT has an
unknown outcome, so it stops the job instead of being retried or held. If every
retry fails, the key is held exactly like a lost response: issued, unresolved,
reported in `unpublished`, never resent, and the batch's readback must name it
among the unresolved keys. Every failing key logs its step (`reserve`, `wire`,
`head`, `commit` or `publish`) and key.

A batch whose only failures held keys continues the job even if it published
nothing, as when a lock convoy catches every in-flight key at once. Three such
batches in a row (`inverseJobStalledBatches`) stop the job, so a persistent outage
cannot spin.

An existing intent is never retransmitted, including a crash before wire. On
restart, an issued key whose object now answers 404 is reported as
`held_absent` and stays unresolved: the executor never reconciles it into a
receipt or a ledger absence, because an attempt row cannot prove which actor
removed the object. A present object stays held as `ambiguous_held` while
unrelated fresh keys can proceed. Preserve the ledger and
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
are snapshots of the latest batch that read its partition; a batch that failed
before reading it keeps the last measurement.

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

## Job restart

A job's `--work` directory is lost with its one-off Render container. So after
writing and sealing a fresh partition, `job` and `job-keep28` store it, gzipped,
as the bucket endpoint's one reusable partition in `r2_inverse_partitions`
(migration 0226), replacing the previous one, and print its `partition_sha256` in
the `partition` step. It lives in PostgreSQL, not R2: under the writer fence every
new R2 write is refused, and an ungated operator PUT would break the drain the
executor relies on.

To restart without relisting the bucket, pass that digest as
`--reuse-partition HEX`. The job then restores the stored partition into its new
`--work` directory and stops unless all of these hold:

- the restored `partition.jsonl` has exactly that digest;
- its summary names the same bucket, endpoint and Keep-28 policy, and picks
  equal to the independently approved `--expected-picks-sha256`;
- it is fresh by the same 24-hour rule every batch applies ("partition is stale
  or future-dated", by database time);
- its external classification inputs are unchanged. The summary's
  `protection_sha256` digests the KE epoch and the exact owner keep prefixes, KE
  folders, external hold set, delivery holds and joined releases, read with the
  partition's own SQL. The job reads them again and requires an equal digest.
  Retirement reservations and receipts, the job's own progress, are left out:
  they would otherwise invalidate the partition after its first key, and the
  live gate refuses every reserved or retired key anyway.

The digest cannot prove the whole partition still classifies the same way. Catalog
fields the KE epoch does not version, such as clip spans and timezones, can change
without it. So the per-key live reclassification is load-bearing for safety:
each candidate's identity is reloaded and reclassified under lock at reserve and
again at the wire, and a key that is no longer eligible is refused. The reverse is
a completeness gap, not a safety one: a reused listing can omit a key that became
eligible after it, and the next fresh partition picks that key up. The reused
partition only proposes candidates: reserve's database checks, live holds,
custody and the wire HEAD identity check still run for every key, so a key held
after the reuse check is still refused.
On any refusal, run the job without `--reuse-partition` to take a fresh partition.

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
| `confident_repair` | `video_assembly` under `experimental/stitching-confident-repairs/`, no catalog row (9,232, 3.377 TB) | the page is retired (`r2_delivery_page_retired()`) |
| `in_window` | `video_collated` inside a keep window (10,893, 2.347 TB) | the page is retired **and** the exact output is NAS-certified (`r2_delivery_nas_certified`) |

An exemption binds the exact `(bucket, object_key, etag, size_bytes)` of the
live page row, the class's verbatim owner decision and the roster SHA-256, and it
references the append-only owner approval for **that class and roster digest**
(`r2_delivery_exemption_approvals`). `roster_sha256` is UNIQUE across the whole
table and a SQL trigger refuses a second approval for the same digest, so a
roster digest approved for one class can never be replayed under another. The
approval is recorded first, by its own command with the owner's decision text;
seeding only consumes it and refuses a roster with no approval for its exact
class, digest and decision. The seed also recomputes each row's class with the
collated executor's own live classifier (inside vs outside the stream's keep
window, or a confident repair) and refuses the whole roster on any disagree. The
read-only `r2-inverse validate` step runs that SAME decision before any approval
exists, so the owner only ever approves a digest the wire would accept; the
documented roster SQL is candidate generation, never validation. Exemptions,
approvals, the quiesce row and the retirement row are append-only. If the page
row's identity ever drifts, the key is held again; the partition compares the
listing's ETag and size against the exempt identity before reporting a
candidate.

Seeding requires a positive, window-bound, non-keep-everything owner decision
for the page recording's stream *and* for the output's own stream; a missing
pick is refused. At deletion time the page recording's stream is rechecked live,
so a page row on a keep-everything or undecided stream holds the key even when
the catalog output names a decided one. Dependency-only collated outputs that
are not page keys follow the 2026-11-18 owner scope, but anything whose NAS copy
is still pending or blocked ("waiting to be transferred") stays held. Footage
already verified on the NAS is not excluded by that rule.
Keep-everything delivery videos (1.0 TB) are never exempted: the seed command
refuses any key whose stream is `all` in the pinned picks, and the lane refuses
it again. `r2_delivery_nas_certified(output_id, bucket, key, etag,
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

0. Take the old public page's **legacy origins** out of traffic first. Nothing in
   this section can make an origin that is still in front of the page stop
   serving: the API enforces the retirement state on its own routes, and any
   handled origin must be removed by whoever owns it. Prove the removal, not a
   promise:

   - every legacy hostname/CDN route/redirect/custom domain that resolves to the
     delivery page or its R2 keys is removed or returns nothing, checked with
     `dig`/`curl -sSI` from off-host and recorded with the timestamp;
   - the R2 bucket has no public bucket-level domain, no worker route and no
     lifecycle rule that republishes the delivery prefix;
   - `public/current.json` still names the generation this run retires, so the
     retired generation and the removed origin are the same object.

   **No rollback to a legacy origin after arming.** `page-quiesce` is the point
   of no return: the numbers below describe the only safe path forward.

1. Build a class roster read-only (below) and pin its SHA-256. The roster SQL
   only GENERATES candidates. Before the owner sees a digest, run the read-only
   validator, which applies the collated executor's own live classifier and
   writes nothing:

   ```
   stoaramactl r2-inverse validate --class CLASS --roster FILE \
     --expected-roster-sha256 HEX --picks docs/records/r2-picks-20261118.json \
     --expected-picks-sha256 HEX
   ```

   It refuses the whole roster on the first candidate the classifier holds, so a
   mixed-source window (any source in the pinned window makes the output
   `in_window`), a keep-everything page or output stream, a KE folder component
   in the stored path, an identity drift or any other hold cannot be approved as
   the wrong class. A candidate roster the validator refuses is not approved
   again with a widened query: report the difference to Deniz.
2. Record the owner's approval for the validated digest as its own step, under
   exactly one class, giving it the roster and picks it should validate:

   ```
   stoaramactl r2-inverse approve --class CLASS --roster FILE \
     --expected-roster-sha256 HEX --picks docs/records/r2-picks-20261118.json \
     --expected-picks-sha256 HEX --decision TEXT
   ```

   Approval is not a procedural trust: it re-runs the validator's decision on
   the exact roster bytes in the approval's own transaction and refuses to record
   a digest the classifier would reject or a digest without its bytes. Seeding
   never creates an approval.
3. Seed the class from the pinned roster:
   `stoaramactl r2-inverse exempt --class CLASS --roster FILE
   --expected-roster-sha256 HEX --picks docs/records/r2-picks-20261118.json
   --expected-picks-sha256 HEX`. The whole roster is admitted in one
   transaction, or nothing is, and every row references the approval recorded
   above for that class and digest. It re-runs the same decision the validator
   ran, so a roster that changed after validation is refused.
4. `outside_window` can be seeded and retired now:
   `stoaramactl r2-inverse job-collated ...`.
5. Handoff (a) and (b): the in-window videos are copied and certified on the
   NAS and the static CSV+JSON manifest of old links to NAS paths is published.
6. Handoff (c): arm, record cutover authority, let the derived deadline pass,
   then retire. The gate is SQL, not this prose.

   a. **Arm.** `stoaramactl r2-inverse page-quiesce --reason TEXT` (once). From
      this commit every public delivery route refuses a NEW request with 503 and
      admits no reader, ticket or publication; readers already admitted finish
      under their lease. No exemption class opens and the page is not retired.
      This is the point of no return.

   b. **Record the reviewed cutover authority.**
      `stoaramactl r2-inverse page-cutover --evidence FILE
      --expected-evidence-sha256 HEX`. The evidence is the facts the API cannot
      see, and its canonical bytes are pinned by `--expected-evidence-sha256`:
      the serving `generation`, the **last legacy five-minute grant** time and
      its grant-list digest, the **legacy origin removal** time and its off-host
      evidence digest, the **cache-expired** time and its evidence digest, the
      **non-serving rollback routing** digest, and the **verified end of every
      active transfer** (`transfer_bound_at`), direct-from-R2 included. SQL
      derives `deadline_at` as the greatest of those plus the five-minute legacy
      grant window; the command cannot supply it.

      If you cannot prove a bound — no authoritative byte clock for a direct R2
      transfer, an origin whose cache cannot be shown expired — do not write a
      plausible number. Stop: without this row retirement is refused, and with
      it `confident_repair`, `in_window` and every derivative release stay
      closed. That is the correct outcome when the drain is unproven.

   c. **Wait for the derived deadline.** Query the row, not a wall clock from
      this document:

      ```sql
      SELECT clock_timestamp() AS now, deadline_at,
             count(*) FILTER (WHERE deadline_at > clock_timestamp()) AS not_yet
      FROM r2_delivery_cutover_authority WHERE singleton;
      ```

      `not_yet` must be 0 before the next step. The same query shows the tracked
      capability drain:

      ```sql
      SELECT clock_timestamp() AS now, count(*) AS live,
             coalesce(max(expires_at),'-infinity') AS latest_expiry
      FROM r2_delivery_download_tickets WHERE expires_at > clock_timestamp();
      ```

      `live` must be 0. Note the explicit limit: a URL a recipient saved before
      arming cannot be revoked, only drained, and the tracked ledger may be
      empty while an untracked legacy URL is still live. That is exactly why the
      authority's `legacy_grant_boundary_at` and `transfer_bound_at` are required
      independently of the ledger.

   d. **Retire.** `stoaramactl r2-inverse page-retired --generation HEX32
      --decision TEXT` (once). 0225's trigger refuses it — under the exclusive
      public lock — unless the quiesce row exists, a cutover authority for THIS
      generation exists, its `deadline_at` is already past by the database
      clock, no admitted reader lease is unexpired, no tracked capability is
      unexpired and no page key has a pending derivative retirement. A
      hand-written INSERT, an older binary or a second process is refused for the
      same reasons, so there is no split-brain CLI-only check.
      `r2_delivery_page_retired()` reads exactly that row, and every public
      delivery route refuses 410 from then on for the current and every pinned
      generation. This opens `confident_repair`.

   e. **Confirm.** `SELECT r2_delivery_page_retired(),
      r2_delivery_public_serving();` must read `t, f`.

7. Seed `confident_repair` and run the collated job. Seed `in_window` once the
   0224 follow-up certifies the copies; until then it stays held.

#### Lifetime and bounds of the admission

- **Readers.** Each public response holds one `r2_delivery_public_readers` row
  from just before its first registry read to after its last socket write. The
  lease expires in 2 minutes — longer than the API's 60s write timeout, so a live
  slow response never loses it and a crashed one delays retirement by at most 2
  minutes. The API holds no database connection and no lock while streaming.
- **Tickets.** Each capability is one `r2_delivery_download_tickets` row, written
  in the same statement as the capability under the shared public lock. It
  records only the provider's `expires_at`. It records **no** transfer bound: a
  bound the server invented would be a guess, and the authoritative end of a
  started transfer is the cutover authority's `transfer_bound_at`.
- **The lock and the pool.** `r2_delivery_public_lock()` is one advisory key:
  shared for reader admission, ticket minting and registry publication, exclusive
  for arming and the retirement proof. A cold registry load validates its keep
  snapshot on the SAME transaction that holds the lock — never a second pool
  acquire — and the registry mutex is held only for pointer reads and the final
  publish, never across the network read or the fence. So with a two-connection
  pool, two concurrent cold loads still each need only one connection and a
  quiesce behind them always finds one free. `TestDeliveryRegistryColdLoadAndQuiesceDoNotDeadlock`
  is that regression.
- **Failure bounds.** A reader that cannot prove the page is serving refuses
  (503). A retirement that cannot prove the cutover and the drains refuses.
  Nothing is cached: the quiesce, cutover and retirement rows are read live.

#### Remaining provider drain assumptions (stated, not hidden)

- A presigned URL already returned to a recipient is not revocable. The drain
  relies on the provider refusing it after `expires_at` and on the operator's
  `transfer_bound_at` covering a transfer that started just before expiry. It
  assumes the provider honours presign expiry within its documented skew and
  applies no longer cache to the signed URL. There is no provider byte clock in
  this system; if one is unavailable, `transfer_bound_at` cannot be proved and
  retirement stays closed.
- An origin removed in step 0 keeps its own caches for as long as that owner
  configures. Removing the route is necessary; it is not proof that an edge cache
  has purged. `cache_expired_at` and its evidence digest are the record that the
  caches are known expired.
- A legacy origin that keeps serving the OLD static page from its own cache never
  reaches the API and is outside this mechanism; step 0 plus
  `legacy_origin_removed_at` and its evidence are the only control for it.
- `r2_delivery_nas_certified` is still a hardcoded false stub in this branch
  (#574's follow-up binds it): `in_window` cannot open here, regardless of the
  cutover authority.

### Read-only roster queries### Read-only roster queries

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

**These queries only generate candidates.** The `outside_window` query cannot
see a source closure that straddles the stream's window: it tests the output's
own interval, while the classifier marks the output `in_window` when ANY source
overlaps. It also cannot see a KE folder component in the stored path, a page
stream decided `all`, or any live hold. Every candidate must therefore pass
`r2-inverse validate` before its digest is approved; a row the validator refuses
is not a shortfall to widen around, it is a key that stays on the page.
