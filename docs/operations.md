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
enumerates the whole bucket and reports now/later/copy/blocked. The class is
**planning-only**. Outside-window objects without NAS presence evidence qualify
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
