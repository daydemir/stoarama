# Stoarama operations

Operator credentials, production environment changes, deployment and cloud
spend controls. Product model and development setup: `../README.md`.

## Development Rule

Every product workflow ships in this order:

1. API behavior exists
2. `stoaramactl` can exercise it end-to-end
3. tests cover it
4. the web UI follows the same model

The public `stoarama` CLI is narrower and is for account/node workflows, not operator administration.

## V1 Product Shape

- admin-curated streams
- magic-link accounts
- account API keys
- enrolled source/inference nodes
- hosted capture
- BYOC inference metadata and provenance

## Local Credentials

Runtime secrets live in `local/*.env` (gitignored; committed `.example` files show the shape). Check there before asking for credentials or reaching for Render:

- `render.env` — production env mirror: `SERVICE_TOKEN` plus R2 (`R2_ACCOUNT_ID`, `R2_BUCKET`, `R2_REGION`, and `R2_ACCESS_KEY_ID`/`R2_SECRET_ACCESS_KEY`, which map to `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` for AWS-style tooling)
- `recording-supervisor.env` — operator API key AND the production Postgres `DATABASE_URL` (the only local source of prod DB access)
- `youtube-relay-source.env` — operator API key + relay topology config
- `mitscl.env` — account-47 API token (`BACKEND_API_URL` + `MITSCL_API_TOKEN`)
- `do-capture.env` — `DIGITALOCEAN_TOKEN` + `TF_VAR_*` for the DigitalOcean capture boxes
- `relay-signing-private.key` — relay payload signing key

Load with: `set -a; . local/render.env; set +a`. Never print values; never copy between machines without Deniz approval.

## Render Operator Access

The Render CLI is installed at `~/.local/bin/render` and is already authenticated (workspace `ay` / `tea-d1g1uevfte5s7384s1q0`, deniz@denizay.org). Its config is `~/.render/cli.yaml`; the bearer token is at YAML path `.api.key` (NOT `.profiles.default.apiKey` — that path does not exist, and `yq` is not installed on this machine).

The CLI has **no** `env-var` subcommand. Read/write service env vars through the REST API:

```sh
rkey() { awk '/^ *key:/{print $2}' ~/.render/cli.yaml; }   # re-read per call
curl -fsS -H "Authorization: Bearer $(rkey)" \
  "https://api.render.com/v1/services/$SERVICE_ID/env-vars?limit=100" \
  | python3 -c 'import json,sys; [print(e.get("envVar",e)["key"]) for e in json.load(sys.stdin)]'
```

Print KEYS ONLY, as above. The raw response contains every production secret
value, so piping it to stdout leaks the lot into a transcript. If you need one
value, select that single key and use it without echoing it.

Service IDs: `stoarama-api` = `srv-d6usqn94tr6s73d94cd0`, `stoarama-recorder-control` = `srv-d8vcdspo3t8c73far4e0`.

**Change env vars ONE KEY AT A TIME (`PUT .../env-vars/KEY`). Never replace the whole env set.**
A bulk env replacement on `stoarama-api` on 2026-07-29 dropped `STORAGE_CRED_KEY` and stopped
all MIT clip capture for 33 minutes (01:33-02:06 UTC). It surfaced as misleading 409s, not as an
auth error: upload-intent reserved an idempotency key, then returned 503 because the key was
gone, and retries looked like duplicates. Recovery needed a Render deploy rollback (a rollback
restores that deploy's env snapshot) plus a relay fleet rollback. The structural cause is that
Blueprint sync preserves the service but does NOT materialize `sync: false` variables, so
anything set only in the dashboard silently disappears from a replacement.

**Never invent a replacement `STORAGE_CRED_KEY`** — every encrypted storage destination becomes
unreadable. Rotating it means re-encrypting the destination ciphertexts in the same transaction.

After any env edit, confirm the sensitive keys are still present before walking away:
`STORAGE_CRED_KEY`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET`, `R2_ACCOUNT_ID`, `DATABASE_URL`.

Writing an env var (`PUT .../env-vars/KEY`) does **not** trigger a deploy, and `POST .../restart` does **not** reload env — the process keeps the values it booted with. To make an env change take effect, trigger a deploy of the same commit:

```sh
rkey() { awk '/^ *key:/{print $2}' ~/.render/cli.yaml; }   # short-lived, re-read per call

LIVE=$(curl -fsS -H "Authorization: Bearer $(rkey)" \
  "https://api.render.com/v1/services/$SERVICE_ID/deploys?limit=100" \
  | python3 -c 'import json,sys
live=[d for d in (e.get("deploy",e) for e in json.load(sys.stdin)) if d.get("status")=="live"]
assert live, "no live deploy in this page; page further with ?cursor="
print(live[0]["commit"]["id"])')

curl -fsS --show-error -X POST -H "Authorization: Bearer $(rkey)" \
  -H 'Content-Type: application/json' \
  -d "{\"clearCache\":\"do_not_clear\",\"commitId\":\"$LIVE\"}" \
  "https://api.render.com/v1/services/$SERVICE_ID/deploys" \
  | LIVE="$LIVE" python3 -c 'import json,os,sys
d=json.load(sys.stdin)
assert d.get("id") and d.get("status"), d
got=d.get("commit",{}).get("id")
assert got==os.environ["LIVE"], "deployed %s, asked for %s" % (got, os.environ["LIVE"])
print(d["id"], d["status"], got[:8])'
```

Three traps here. `curl -s` alone exits 0 on an HTTP 400+, so a bad service id or
an expired token looks like a successful deploy trigger — hence `-f` and the
assertion on the response rather than eyeballing it. A deploy POSTed without
`commitId` takes whatever is newest on the connected branch, so anything pushed
between your check and the POST ships too; pin it to the commit you verified.
And the newest deploy is not necessarily the running one — it may be failed or
still building — so filter for `status == "live"` rather than taking the first
element.

## DigitalOcean Spend Guard

The DO token lives on Render as `DO_API_TOKEN`: on `stoarama-recorder-control` (pool autoscaler + scale-up spend guard), `stoarama-recording-health` (hourly spend alerts), and `stoarama-recording-health-summary` (digest section). `stoarama-api` has no DO token. Locally it is `DIGITALOCEAN_TOKEN` in `local/do-capture.env`.

- `stoaramactl do-spend report [--json]` shows month-to-date usage, current burn at list price, the breakdown by tag/name prefix, and every droplet/volume nobody owns. It needs `DATABASE_URL` (prod: `local/recording-supervisor.env`) and `DO_API_TOKEN`.
- Settings: code defaults are warn $5/day, critical $10/day, budget $150, allowlist `stoarama-survey-*,copresence-*,stoarama-collate-*=15`. Deployed on all three services (collation week, Sep-Oct 2026): warn $20/day, critical $30/day, budget $500, same allowlist. Read the live Render env before trusting either.
- The hourly `recording-health run` emails operators (deduped in `ops_alert_episodes`, daily reminders) when burn exceeds `DO_SPEND_WARN_USD_PER_DAY` / `DO_SPEND_CRITICAL_USD_PER_DAY`, month to date exceeds `DO_SPEND_MONTHLY_BUDGET_USD`, a droplet/volume outside the recorder pool and `DO_SPEND_ALLOWLIST` has run over an hour, an allowlist entry with a cap (`glob=USD`, e.g. `stoarama-collate-*=15`) costs more than that per day across its droplets and their volumes, or the pool controller blocked a scale-up. Allowlisted resources always count toward burn.
- Create hand-made DO resources knowing the tripwire pages at the first hourly sweep after the resource is an hour old (up to about two hours); add long-lived ones to `DO_SPEND_ALLOWLIST` instead of ignoring the mail.
- The pool controller skips any scale-up whose projected account burn exceeds the critical ceiling. At the default $10/day with the current ~$4.6/day baseline that caps the pool near 10 s-2vcpu-4gb droplets; raise `DO_SPEND_CRITICAL_USD_PER_DAY` before planned growth.
- `DO_SPEND_*` keys are read independently by `stoarama-recorder-control`, `stoarama-recording-health`, and `stoarama-recording-health-summary`: change them on all three (one key at a time), then redeploy recorder-control (the crons pick them up on their next run).

`AUTO_MIGRATE=true` on `stoarama-api`, so a deploy runs migrations on boot. Before deploying, confirm `origin/main` matches the live commit and that no migration is unapplied, or you will ship more than the env change.

`.api.key` is short-lived; `expires_at` sits beside it in the same file, so check there rather than trusting a date written down here. Re-read it from the file on each use rather than caching it; on a 401 refresh it with the CLI instead of asking Deniz for a dashboard value.

## Delivery keep holds and R2 retirement previews

The 2026-11-12 owner decision keeps the public delivery set and the 21 `all`
streams in R2 for now. NAS keeps keep28 **plus** delivery files and their required
source dependencies, including delivery dates outside keep28. R2 delivery is a
holdover until NAS copying, stitching, and the serving cutover are validated.

Migrations 0188/0189 add the exact 32,825-key hold seed (key, ETag, size, SHA and
presentation path), the 59,801 public assembly input identities from the
SHA-verified growth representation, and durable raw-source dependency closure.
Holds veto NAS preparation, admission, enable, first/replayed permits, local
client guards and V85r retirement; database triggers also fence older callers.
Deploy the migrations before the new API/client. An outstanding protected NAS
permit or R2 intent requires drain/reconciliation before migration/import.
The migrations are additive; they neither delete media nor release old holds.

`stoaramactl delivery-keep --keep-file FILE --expected-sha256 SHA --out NEW_DIR`
checks the exact file against the live current/pinned public registry and emits
a future copy/verify queue. ACKs are not fresh verified copies. `--import` only
adds or enriches holds under admission/public locks; it cannot release them.

`r2-retire plan --class owner_r2_delivery_only --head-guard-compatible ...`
enumerates the whole bucket and reports now/later/copy/blocked. With migration
0192, the class can execute only a fresh, non-preview `now`
manifest carrying `head_guard_compatible=true`, and apply requires
`--provider-unconditional-delete-with-head-guard` plus the owner credential-risk
acknowledgment. Other phases and old previews remain unexecutable.
Outside-window objects without NAS presence evidence qualify
now; any inventory, listing (including partial generations/aliases), ACK or raw
delivery evidence waits for NAS cleanup under the owner rules. Verified cleanup
receipts supersede delivery evidence under the existing recovery-hold rules;
keep28 media requires fresh exact NAS identity or enters copy-first. Existing
incidents, references, shared keys, reservations and protected/operator prefixes
still veto. Unmapped keys stay blocked; unreferenced staging is separately
reported for owner visibility. Collated objects lacking catalog ETags cannot
enter a HEAD-guard-compatible retirement manifest.

Every executable retirement manifest requires version 2 and a live delivery
hold digest. Apply preflights every selected key, rechecks holds and the public
registry under the common fences before and after HEAD, and refuses v7/unchecked
manifests. `--delivery-keep-preview FILE --expected-delivery-keep-sha256 SHA`
allows a read-only pre-migration preview; these manifests are unexecutable.
Large classification files are written directly as gzip with exact file digests.

Migration `0192_delivery_only_r2_apply.sql` admits only the delivery-only
reservation class and preserves generic recovery holds; it does not inherit the
outside-only owner's hold-release/source-purge exception. It also requires
indexed broad NAS path/content vetoes. Before executable planning, precreate
the exact indexes outside the migration transaction:

```sql
CREATE INDEX CONCURRENTLY nas_inventory_r2_owner_path
 ON nas_inventory_files(relative_path) WHERE state<>'missing';
CREATE INDEX CONCURRENTLY nas_inventory_r2_owner_content
 ON nas_inventory_files(sha256,size_bytes) WHERE state<>'missing';
```

The migration never creates these indexes. Executable planning/apply refuses
missing or incompatible indexes before catalog enumeration. Planning
reads inventory/catalog evidence in primary-key pages; delivery-only apply
rechecks evidence per object under the fences without enumerating inventory.
The writer drain, multipart-zero inventory, provider proof, minimum age, fresh
HEAD identity, 5 TB/24h budget and durable ledger remain required. A separately
authorized writer-fence deadline override is an operator action; this tooling
does not change or deactivate that fence.

No delivery-hold release operation ships here. Eventual R2 removal requires a
separately reviewed command/migration proving verified NAS copies, completed
stitching and the page serving correctly from NAS (including Range, HEAD and
existing downloader links). Creating a PR or a dry-run manifest installs none
of these changes in production.

### NAS client cleanup-batch-v5 delivery guard

`cleanup-batch-v5` selects the eight-file serial cleanup group with the exact
delivery-key/dependency guard and requires `delivery_keep_checked=true` on every
unlink permit. The server accepts v5 for execution and grouped observation while
retaining every predecessor. This carries forward v4's grouped cap transaction,
batched diagnostics, durability ordering, and steady eight-file budget of 21
SQLite commits / 45 fsyncs. Release with the existing NAS client release procedure;
this change does not publish or deploy a client.

### NAS client cleanup-batch-v6 throughput

`cleanup-batch-v6` keeps v5's serial per-file permits, proof checks, delivery
holds and durability ordering. A covering SQLite index and transactional rolling
totals replace repeated full cap-ledger scans. Reads visit only rows crossing
the strict `at_epoch > now - 86400` boundary; insert/update/delete triggers keep
reservations and releases in the same transaction. History is retained so a
backward wall-clock adjustment restores the exact original count. The first
upgrade creates the index and totals once; later restarts reuse them.

Receipt/quarantine schema initialization runs once at startup. Journal writes
retain their per-item compare-and-swap checks without reading all receipts.
Namespace enumeration remains before and after hashing and after the durable
attempt marker immediately before unlink; the redundant pre-marker enumeration
is removed. An eight-file steady group still uses 21 SQLite commits / 45 fsyncs.

Heartbeat `cleanup` includes `aborted_groups`, `last_abort_stage`,
`last_abort_reason`, `last_abort_at` and `last_abort_files`. Handled per-file
refusals separately report `item_failures` and `last_item_failure_*`; they do
not change receipt or continuation behavior. Reasons/stages are fixed labels,
never raw exception contents. Counters are per client process and the last
failure remains visible after later successes.

Deploy v6 support, then stage and promote `NAS_VERSION=cleanup-batch-v6` through
the existing release scripts only after natural drain, no enabled plan/batch,
and confirmed master OFF. Verify the executing heartbeat version before restart.
Do not pause an enabled issued plan to deploy or promote. V5 remains accepted.

Grouped directory fsync and bulk permits are deferred to a separately reviewed
v7. Grouping eight directory fsyncs into one would save 7 of 45 steady syncs
(a theoretical 16% sync-bound time reduction, about 18% more files/minute).
Bulk permits would save at most 7 observation round trips per eight files but
would age later permits during earlier durability writes; no production speedup
estimate is justified without measuring that latency and proving crash safety.

The delivery migrations are `0189_delivery_keep_holds.sql` and
`0190_delivery_r2_retirement_guards.sql`, following main's
`0188_nas_cleanup_leased_index.sql`. Migration identities use full filenames.
The upgrade regression applies main first, then both delivery migrations, checks
the exact seed and R2 veto, and replays migration discovery without duplication.

### NAS cleanup inventory repair (v6.1)

`cleanup-batch-v6.1` retains v6's execution protocol and durability budget.
Inventory observations stay in a bounded memory batch until one locked SQLite
checkpoint commits them. No scan transaction remains open during NAS I/O or
between journal lock acquisitions, so cleanup cannot roll back scan observations
or fail its own `BEGIN IMMEDIATE` against them. Completion still requires the
actual complete scan and acknowledged final publication.

Fresh joined marker publication removes only its own current-call, exclusively
created temporary hardlink after exact content and namespace checks. Restart
recovery retains the existing preservation path. Historical aliases are never
removed by this release. Heartbeat `cleanup.joined_quarantine` keeps the total
alias count and adds `proven_stage_count`, `unknown_alias_count`,
`ledger_problem_count` and bounded `stage_evidence` (artifact, inode, size, hash).
Only a stable single-link marker matching a completed alias move and canonical
copy, exact artifact identity and copied hash can count as a proven stage.
Unknown aliases, ledger problems and pending recovery remain release blockers.

The keep28 preparer now reads the unchanged SHA-pinned reviewed report and frozen
index before proposing files. It excludes unreviewed/wrong-owner parents,
ambiguous names, missing/changed/linked frozen entries and missing raw sidecars.
Admission remains authoritative and unchanged. The Jelly ranking cache uses a new
namespace so catalog-only density estimates, including stream 17234's unreviewed
September 25 folder, are rebuilt through the same selector.

Stage/promote this version only from reviewed merged main through the existing
release scripts, in a naturally idle master-OFF window. Before restart, verify a
fresh executing heartbeat, stable zero cycle errors, actual inventory completion,
no inventory mismatch and either zero aliases or complete benign stage proof.
Do not clear the runner's preparer error or remove STOP until these gates pass.

### NAS client cleanup-batch-v7 grouped durability

The exact `cleanup-batch-v7` release requests up to eight permits together for
one complete, initially leased roster. The server retains each file's recovery,
delivery hold, session, proof and budget checks. All unlinks remain serial under
the media fence. Every member uses the same request-start clock and existing
15-second permit age limit, including the sidecar syscall.

V7 durably appends all ledger intents, then commits conservative attempt markers
before requesting permits. This keeps filesystem sync latency outside the permit
clock. Each marker retains the original observation bytes for reconciliation.
Replay clears an unissued marker only after the server positively acknowledges
the exact attempt; issued authority remains uncertain. Conflicts and lost replies
preserve the marker. Replay requests no new permits and performs no unlinks.

After unlinks, every distinct pinned directory inode is synced before any durable
success result. Ledger results and SQLite results are grouped separately. Only
the live owned worker can certify a refusal before any unlink was attempted;
crashes, possible attempts and failed directory syncs remain uncertain.
Predecessor releases keep their existing serial permits and durability ordering.

Real disposable-file/FULL-WAL measurements reduce a steady eight-file group in
one folder from 45 fsyncs / 21 SQLite commits to 10 fsyncs / 7 commits. With
explicitly simulated 300 ms sync and 250 ms permit latency, medians were 15.824 s
(v6.1) and 3.347 s (v7). These are controlled benchmark results, not NAS rates.
Stage and promote only from reviewed merged source after natural drain, no
enabled plan or batch, and confirmed master OFF. Keep R2 copying and stitching
disabled unless separately authorized conditions are met.

### Full R2 recovery reads

Full-object recovery downloads share one process-wide weighted limit across
HTTP requests. Each 64 MiB (rounded up) consumes one of 16 units; objects over
1 GiB run alone. Conditional HEAD verification and MD5/ETag matches bypass this
full-read limit, so already verified execution does not queue behind audits.
The weights bound read pressure; they are not a measured bandwidth optimum.

Each full read has a 35-second child deadline inside the existing 40-second
prefetch budget and 60-second HTTP write limit. An execution request still has
its original 15-second deadline, including any capacity wait. Cancellation and
incomplete reads yield no verification verdict. Exact ETag/version, byte count
and SHA checks, retained completed results, and all deletion gates are unchanged.
This addresses repeated large-object retries without storing media or introducing
an asynchronous verifier. Validate production progress after a natural batch gap;
controlled contention tests do not establish production throughput.


## R2 retirement during wind-down

Every supported R2 wire deletion now requires the real NAS stitching prerequisite
and an independent postrun artifact digest. See
[R2 stitching prerequisite](R2_STITCH_PREREQUISITE.md) for the owner wording,
read-only verification command, immutable replay and recovery contract.

RAW drain = [this helper](../ops/jelly/r2-retire/raw-drain-lock.sql) plus a terminal job, never classid=0.

Legacy `survey delete-stream-captures`, `joined-source-purge --apply` and
`nas-verified-purge --apply` refuse before catalog or object mutations. Keep the
NAS purge cron suspended and its enable flag false. Probe requests issue no new
capabilities. Restore requests issue none either, with one exception: an
'original' restore of a Keep Everything clip (`nas_restore_ke_original`, the
pinned `keep_everything` streams of `nas_media_keep20_windows`). Deniz, verbatim:
"nas shoul dnot have any of the keep everything (except the keep 20)"; R2 holds
100% of Keep Everything. Migration 0260 admits only those rows past the writer
fence; their live `put_expires_at` holds `r2_retirement_writes_blocked` false,
so R2 delete batches wait while a restore capability is live. Test restores and
every other row stay untouched and refused; existing rows and objects remain
preserved. The fence admits one more writer, collation v2 recovery. Deniz,
verbatim: "serve the best 14 days of as many streams as possible, fully
stitched v85 (or whatever the latest version was we were using), via r2,
through the nice web ui". Migration 0262 admits a generation-2 hour only when
the controller has listed its batch, recording and local date (the day in the
recording's `cron_timezone`) in `collation_v2_recovery_scope`, in one of two
ways:

- **Restore.** `collation-v2 plan --scope days` plans a registered hour as a
  restore in its own batch, and `collation-v2 run --recovery-scope` puts its
  catalogued outputs back at their catalogued keys with their catalogued
  sha256 and size, in the fenced bucket. Restores cover outputs only: delivery
  reads outputs, and a registered hour's manifest is never restored. It HEADs the key under the wire lock
  and sends no PUT when the object is present. It is refused while any
  retirement of that key is pending, and it writes no catalog row.
- **New hour.** Only in a fresh `recovery-` batch, and only for a recording,
  date and hour that no generation-2 hour holds yet (`plan --scope days
  --recovery-batch` names only unregistered hours, and refuses an unscoped
  batch whose `joined/<batch>/` prefix already holds an R2 object). No catalog row or writer
  intent may name the batch before its first scope row. `run` creates the
  hour's manifest and its parts, each part keyed by its own sha256.
  `collation-v2 register` then catalogues the hour and its parts in one
  transaction.

Every key a scope row covers is an owner keep hold (`r2_owner_keep_held`), so
no R2 retirement lane can reserve, intend or record its deletion. A classic
`r2-retire` plan that names one fails closed. Scope rows are revoked, never
deleted, and no scope row lands over a pending retirement. A scoped hour or part
can never be updated, deleted or truncated.

Each recovery PUT is a durable `r2_retirement_writer_operations` intent naming
the hour and the exact key, size and sha256, and carries that sha256 as its
`ChecksumSHA256`, so R2 refuses any other body. `r2_retirement_writes_blocked`
and `r2_retirement_writers_drained` therefore hold every R2 delete batch back
while it is open. An intent completes at once on every path that leaves no
possible write: a refused wire admission, a present key, a failed HEAD, a 412 or
any other definitive 4xx. Only an ambiguous PUT, or a failed read-back or
completion after one, stays open. A PUT still in flight share-locks its intent.
`collation-v2 settle-recovery --apply` completes every other open intent,
whatever its key now holds, and reports each key as verified, absent (its hour
needs another run) or foreign. That is safe because every recovery key is held
for good, and a late copy of its PUT is create-only and checksum-bound. A
recovery client makes no generic PUT and signs no capability.
`PresignNASRestorePut`, stitch staging and `DeleteObjectOnce` are unaffected by
the binding and stay gated by their own rows. The NAS cleanup source freeze (0180), the collation-reference hold (0167)
and the R2 retirement key vetoes still apply: a stream frozen by an enabled
cleanup plan waits for that plan.

New account and admin S3 destination verification/provisioning is retired
before writing probes or credentials. Existing managed destinations resolve by
read-only lookup; missing managed storage is not implicitly created, billed or marked verified.
Existing destination selection and separately transported WebDAV verification
remain available. These backend changes require the production controller's
reviewed between-batch deployment; merging does not deploy or authorize deletion.

## R2 writer readiness reconciliation

Migration0198 separates the retirement consumer's `r2_retirement_writes_blocked`
check from the historical twenty-category diagnostic. A runnable job may remain
unchanged when durable admission prevents it from writing. The consumer requires
the exact bucket/endpoint fence, its immutable minimum drain deadline, and no
outstanding wire operations, PUT capabilities, upload intents, capture leases or
restore PUT capabilities. Unknown scope blocks globally. Every item locks the
exact fence row and repeats the check immediately before its single wire DELETE;
fence removal stays excluded through the existing apply session lock.

This database predicate alone is insufficient production evidence. The controller
must verify every deployed overlapping automated writer uses this fence or is
excluded, and prove externally accepted PUT requests have finished or cannot
reach selected keys. URL expiry, signer revocation and the two-minute signing
allowance do not prove an accepted upload completed. Unknown completion remains
blocking after the seven-day deadline. No deadline override, credential change,
historical-row reconciliation or broader deletion authority is introduced.
Keep all custody, retention, delivery, incident, provider-proof and stitching
checks; the approved verified-NAS exception requires its own typed consumer path.

`stoaramactl r2-retire reconcile-writers --out NEW_FILE.jsonl --bucket stoarama`
creates a read-only, repeatable-read observation of every category in
`r2_retirement_writers_drained`. It writes a private, fsynced JSONL record for
**each blocking item**, including its lifecycle, owner/heartbeat/lease facts
where recorded, and the SHA256 of those allowlisted observations. Zero-count
categories appear in the header. Existing evidence files cannot be overwritten.
Migration0196 adds the twentieth category, `capture_process_runs`: every
`starting` or `running` process blocks drain independently of parent/runtime
state and `stopped_at`. The active writer fence rejects runnable process
inserts, transitions and heartbeats; ordinary terminal history remains legal.
These checks do not prove an old owner stopped or authorize cancellation.
The command has a 15-second SQL statement limit and does not read media tables,
provider credentials, signed URLs or claim tokens into the report.

This is the diagnostic stage of reconciliation. It deliberately has no `--apply`
option: every item remains unproven and `apply_command` is null. To reconcile a
writer, first establish its exact owner and obtain fresh primary evidence that
its process is stopped and cannot resume, that no recent writes/heartbeats or
live leases remain, and that the owning stream/machine is stopped. A suspended
provider, old timestamp, expired lease or active fence alone does not prove all
of this. Preserve provider/machine evidence alongside the report. Re-run the
observation before proposing any mutation; its digest is an observation digest,
not a full-row compare-and-swap token or apply authority.

The existing joined lifecycle guards do not support abandoning pending hours,
expired ordinary leases, or sealed artifacts. A parent batch state change does
not drain these children. Keep such work blocking until a separately reviewed,
atomic audited cancellation protocol exists; never create temporary claims,
renew leases, disable triggers, or modify the drain function to evade it. NAS
stitch tasks also require owner process evidence even when their leases expired.
The writer admission fence and its immutable seven-day drain deadline remain
independent requirements.

Migration `0194_r2_delivery_presence_indexes.sql` completes the delivery index
prerequisite. Since migrations are transactional, inventories larger than
128 MiB require **two separate** concurrent builds before deploying it:

```sql
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY nas_inventory_r2_owner_path
  ON nas_inventory_files(relative_path) WHERE state<>'missing';
CREATE INDEX CONCURRENTLY nas_inventory_r2_owner_content
  ON nas_inventory_files(sha256,size_bytes) WHERE state<>'missing';
SELECT r2_retirement_delivery_indexes_ready();
```

Run these outside a transaction, one build at a time; do not wrap the script in
one extended-protocol execution or `psql --single-transaction`. Concurrent
builds still scan the inventory and consume I/O and storage. Check production
relation sizes, active transactions and existing index definitions first. Verify
NAS cleanup is between plans, master OFF, and no deployment is in flight;
coordinate the whole window with the cleanup operator. A failed concurrent
build can leave an invalid index: inspect and review it explicitly rather than
automatically dropping or accepting it. The migration validates the exact
existing indexes through the unchanged readiness function and fails closed on
incompatible definitions. On small fresh databases it creates them in the
migration transaction. Build readiness does not authorize any writer rewrite,
fence override or R2 deletion.


### Audited retirement of stopped joined writers

`stoaramactl r2-retire cancel-joined-writers --batch-record-id 5 --out NEW_FILE`
observes the complete joined batch snapshot and emits row digests, owner IDs and
lifecycle states without exporting claim tokens, source plans or media. It is a
dry run by default. Migration 0195 is required; this does not change the existing
read-only `reconcile-writers` command.

Apply requires the exact reviewed snapshot digest, a checksummed owner map,
a fresh independent production-review reference, and the controller-pinned live
backend commit. A new private evidence file is fsynced before any transition.
Every leased owner must map exactly to a primary provider resource.
The entire batch must retain complete ownership history: unattempted rows or
first attempts with their owner still recorded. A reclaim, released claim or
completed attempted row loses prior-owner identity and refuses cancellation;
current claims cannot substitute for a complete old-owner inventory. Render proof
requires the exact joined service suspended with work scope disabled. A deleted
DO owner requires its preserved primary resource identity and original account
UUID association, a fresh matching account read, and a fresh exact-resource 404.
A 404 under an unbound account, a hostname guess, an expired lease or a missing
provider name is not a stop proof. Worker lanes whose IDs differ from the parent
machine name remain unsupported until their exact association is proved; do not
invent a resource receipt or relax the matcher.

The owner map is a JSON array of typed `render_suspended` or `do_destroyed`
entries with owner/resource IDs. DO entries additionally name an absolute local
historical resource file and its reviewed SHA256. That JSON contains
`account_uuid` and the original provider `droplet` identity; the account
association must come from preserved primary evidence in the same authorization
context. All source evidence and credentials remain on Jelly. Unknown fields,
missing mappings, digest drift, unsupported proof kinds and provider errors stop
the command. Provider reads never stop, restart or destroy machines.

Only after exact-head council/green CI and a separate fresh production review,
run in the cleanup controller's explicit global-idle/master-OFF window:

```sh
stoaramactl r2-retire cancel-joined-writers --batch-record-id 5 \
  --owner-map /absolute/reviewed-owners.json --expected-owner-map-sha256 OWNER_MAP_SHA \
  --expected-snapshot-sha256 REVIEWED_SNAPSHOT_SHA \
  --expected-live-commit CONTROLLER_PINNED_BACKEND_SHA \
  --reviewer-reference 'exact independent production review and window record' \
  --out /absolute/new-apply-evidence.jsonl --apply
```

The command independently confirms configured/runtime master OFF, the exact
live backend with no concurrent deployment, and no enabled cleanup work or
outstanding cleanup authority. Provider HTTP reads finish before the database
transaction; the controller must hold that whole window. SQL takes the existing
writer/freeze locks, rechecks the exact complete snapshot, requires 15 minutes
without writes/heartbeats and no live leases, and requires a fresh exact owner
proof roster. Unsupported pending stream-days remain blockers.

Audit INSERT admission independently locks and validates the fence, full snapshot,
owner roster and activity. SQL derives every exact permitted before/after row;
callers cannot supply a different authorization. A deferred constraint checks
the entire expected audit roster, actual committed rows, admission and parent.
Direct audit inserts cannot commit a partial or forged cancellation. Retired
admission retains its exact audited after-image, including when a caller runs
the deferred constraint early or attempts to reopen it in a later transaction.

The transaction appends immutable run and per-item before/after digests, pauses
admission, terminalizes every pending/expired hour and sealed/nonlive-publishing
artifact, checks all children, then terminalizes the parent through its existing
guard. No claim is manufactured, no attempt count changes, no lease is renewed,
no trigger is disabled, and the drain function is unchanged. New child terminal
transitions require exact same-transaction audit authorization with unchanged
identity/result fields. Every stored after-digest is checked against its actual
row before return. Any error rolls everything back. A lost commit response must
be investigated using the durable run UUID; never delete audit rows or retry by
manufacturing new authority.

This narrow command does not reconcile capture, Durham, NAS stitch task 1, or
any other writer category. It does not delete R2/NAS media, change the immutable
fence, release recovery/delivery/reference holds, or prove global writer drain.
The ordinary diagnostic still reports every remaining blocker honestly. No
production cancellation has been performed by this implementation.

### COPY source lookup and collated paging rollout (0206–0208)

The COPY lease validates a source before HEAD and again under admission. The old
reservation view expands every cleanup manifest for each lookup; 1,438 plans
(86 MB of JSON) cost 923 ms per source check on basic_1gb. The new lookup first
uses a GIN index of immutable normalized `(kind, source_id)` keys, then applies
the existing ordinal-specific reservation rule. Source/recovery holds, missing
item vetoes, permanent issued-item protection and the old reservation view remain
in force. No plan or item is rewritten.

The original collated feed exceeded a 30-second read-only EXPLAIN ANALYZE budget.
The paged query took 291 ms on the first 1,000 candidates and 115 ms near the tail
of the same production catalog (both empty). A read-only probe of 50 real outputs
intersecting the active keep window evaluated the unchanged delivery fence and
returned all 50 in 39 ms. Production source fanout has p99=60 and maximum=61. A 50-output high-fanout
probe took 3.9s; reducing the fence stage to 10 returned ten high-fanout outputs
in 291ms. The exact feed query scanning the high-fanout region took 228ms.
The expensive stage is capped at 10 held outputs, with its own advancing cursor,
so a rejected held prefix cannot trap the scan or cause 1,000 recursive checks. These are DB execution times, not HTTP timings.
The new reservation index is deliberately **not** provisioned during read-only
production diagnosis; its production timing must be verified during the assigned
release slot. Local tests compare the indexed predicate with the old view.

Release sequence, to be executed only by the assigned controller:

1. Keep the existing runner STOP and maintenance fence under their owner's
   control. Confirm the reviewed 0205 schema (including
   `r2_keep28_refresh_control` and `nas_r2_copy_items.work_mode`) is installed
   before deployment: ordinary COPY leases also require it. Refresh activation
   remains a separate controller decision. Prepare and verify the immutable `r2-copy-v6` client artifact but do not start it.
   The controller must inventory and quiesce **every caller** of the collated
   route for cutover, including callers whose collated policy is disabled: old
   clients reject the added cursor field. Scope is not assumed to be connection13
   alone. Resume each caller only with a compatible immutable artifact.
   Same-version publication is unsupported. COPY's two-second transport budget is unchanged.
2. Apply only migration `0206_nas_cleanup_manifest_keys.sql` through the migration
   CLI using a staged migration directory containing that file. It creates the
   immutable projection independently and records it in `schema_migrations`.
3. Outside a transaction, precreate the index using an operator DB session:

   ```sql
   CREATE INDEX CONCURRENTLY nas_cleanup_manifest_keys_idx
     ON nas_cleanup_plans USING gin(nas_cleanup_manifest_keys(manifest)) WITH (fastupdate=off);
   ```

   Build the index with `fastupdate=off` from the start. A large GIN pending list
   caused the generic planner to revert to a full manifest scan on a local
   1,438-plan/89MB fixture; the reviewed option retained the index plan and a
   0.5ms complete lookup. Do not silently alter an older index into compliance;
   a differently built index needs separately reviewed replacement/validation.
   Do not replace the projection function beneath an existing index. Migration
   0207 rejects mismatched function contents and missing, invalid or differently
   defined indexes; inspect a failed concurrent build before any remedial action.
4. Apply `0207_nas_cleanup_reservation_lookup.sql` and
   `0208_nas_storage_v6_binding.sql`, then deploy the reviewed API and compatible
   finish-controller CLI before rolling the prepared v6 client in the controller's assigned slot. The API
   budgets every COPY control at 1.5 seconds, starting before authentication and
   delivery fencing; exhausted budgets return retryable 503. Admission still
   rechecks source identity and serializes grants with active renewals. V5 and
   V6 share the nonce/freshness storage-proof contract; migration 0208 adds the
   exact v6 identity without promoting or rewriting existing samples. The shared
   version predicate covers cleanup-space, late-tier and custody-refresh gates.
   Root must audit private collector/version pins for v6 before activation;
   compatibility of those external pins is not established by this code change.
5. The controller performs its own authorized release/fence sequence. Capture
   this baseline, retaining its exact `sampled_at` as T0 and both byte totals:

   ```sql
   SELECT clock_timestamp() AS sampled_at,
          coalesce(sum(downloaded_bytes),0) AS downloaded_bytes,
          coalesce(sum(committed_bytes),0) AS committed_bytes
   FROM nas_r2_copy_items WHERE connection_id=13;
   ```

   After about 60 seconds, compare connection-wide totals and substitute the
   saved T0 timestamp below. Do not group counters by current state: a custody
   refresh can move historical bytes into the verified group without moving data.

   ```sql
   SELECT clock_timestamp() AS sampled_at,
          coalesce(sum(downloaded_bytes),0) AS downloaded_bytes,
          coalesce(sum(committed_bytes),0) AS committed_bytes,
          count(*) FILTER (WHERE state='verified' AND completion='downloaded'
            AND committed_bytes>0 AND verified_at >
            TIMESTAMPTZ '<saved T0 UTC>') AS new_original_downloads
   FROM nas_r2_copy_items WHERE connection_id=13;
   ```

   Require a connection-wide byte total to increase AND a new original downloaded completion
   after T0. Refresh preserves `verified_at` and both original byte counters, so
   its state transitions cannot satisfy this proof. Attempts or changing per-state
   totals alone are insufficient. Keep inventory mismatch and unverified recovery
   problem counts at zero. Any mismatch stops the release; uncertain/held cleanup
   items never become retry candidates.
