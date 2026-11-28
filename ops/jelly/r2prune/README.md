# Legacy r2prune deletion hold and historical receipts

Owner, 2026-11-25 (via the project lead), verbatim: "make every manual r2prune
apply write receipts at apply time, so a DB record can never be skipped again."

**Deletion is disabled.** The legacy direct binary, manual wrapper and routine
cycle refuse new object deletion because their preflight and post-response
receipts cannot establish a shared, durable admission guaranteeing a verified
exact surviving copy and permanent protection against deletion of that copy.
Neither reviewed manifests, NAS evidence, stitched coverage, nor an open
historical intent can authorize an execution.

`r2prune list` and `r2prune plan` remain read-only. The routine cycle can list
and plan, but a plan with deletion candidates fails before any execution intent,
receipt-source claims, or R2 mutation. Its plan is retained for inspection.

Two historical bare-binary runs (2026-11-21 outside-window, 2026-11-22 delivery
videos) deleted objects without writing their `r2_object_retirements` receipts.
Historical `listing` and `verify` intents and ledgers remain readable. Use
`receipts` to reconcile acknowledged deletes and `backfill` to reconcile older
bare-binary runs; do not rerun a delete to settle an uncertain outcome.
Every ledger is refused if its header names another manifest or listing than
the intent pins, or if it logs a key twice. Rows outside a historical verify
chunk's listing are deferred, not final. Failed or unlogged keys in manual intents stay unresolved. The routine cycle
retains its historical receipt replay behavior.

This source change does not disable previously installed binaries or
scripts. Keep the operational STOP in place. No deployment or resumption is
authorized by this source change.

### Read-only planning: covered best-14 raws the public GSS page serves stitched

Owner, Nov 26: "ok lets just make sure we're deleting the raw clips that we know
are already stitched. we dont need both". `best14/` builds the `--best14-coverage`
rows. `extract.py` reads the serving hour records and manifests from the analysis
snapshot (prod by `hour_id` for any the snapshot lacks), lists R2 under the
returned prefixes with `r2prune list`, then reads raw catalog rows and output
holds from prod by key, 5,000 per statement. `best14_coverage.py` is offline. A
raw is covered only when its clip is `included` in a part of an hour record the
best-14 ledger serves in the named generation, and that part:

- is the one registered output of that part, in a `collated` hour, verified by
  the manifest (`payload_chain_matches`, `video_timing_matches`, `edge_decode_ok`);
- holds the clip's exact catalog bytes (key, size, sha256, ETag) in its
  `source_clip_ids`, with the clip's catalog time span inside its own;
- is live in R2 at its catalog size with an owner keep (as above), named by no
  receipt, reservation or nonraw intent, with no unverified NAS restore, and not a
  Keep Everything sole copy.

It also stays (an exclusion wins over coverage from any other row of the same raw)
when the hour is served by a clock assembly or follows one, sits at
a seam the audit flagged (both hours, any verdict) or holds a reviewer-exception
clip, or is named by `--exclude-hour`. `--within KEYS` limits coverage to raws the
owner approved by count. `r2prune` decides `delete_best14_raw_stitched` for a
non-KE raw with a matching row even inside its 20-day window (the window keeps the
NAS copy, not the R2 one), but never for Keep Everything, a key with any key or
prefix protection (a raw still in `gss-protect.tsv` stays `protect_live_site`), a
derived identity, an unmapped stream or interval, or a recent write. The window
stitch closure (every source of an output overlapping a window) does not hold a
covered raw: being such a source is what the coverage row proves. Tests:

```sh
/usr/bin/python3 -I ops/jelly/r2prune/best14/test_best14_coverage.py -q
go -C backend test ./cmd/r2prune
```

### Served-generation gate

The read-only gate reads the
generation `gss_public_generation_pin` names now, GETs its registry
`public/generations/<id>.json` and fails closed unless no key that generation
serves (files and assets) is in the keys about to be deleted, every one has a
`page_link` row in the plan's `--db-protect` file, and every one the plan's
`classified.tsv` (beside `--summary`) lists is `protect_live_site`. `manual_apply.py gate PLAN_DIR
--db-protect FILE` runs the same check on a plan.

`manual_apply.py served-keys OUT` writes the same served keys, one per line, for
`r2prune --served`: each is `protect_live_site` everywhere, where a `page_link` row
holds only inside a window or on Keep Everything. A read-only plan
passes it, so the gate's classification check holds for every served key the
plan lists.

`manual_apply.py protect-served` unions the pinned generation's served keys into
`~/stoarama-gss-freeze/gss-protect.tsv` under `cycle.lock`. It never removes a row,
backs up `gss-protect.tsv` and the lane's `cycle.py`, and raises `cycle.py`'s
short-list threshold to the new count. Run it after every publish.

## Other commands

```sh
# replay the receipts of an open intent
/usr/bin/python3 -I ops/jelly/r2prune/manual_apply.py receipts WORKDIR
# receipts for a run made without the wrapper (never starts r2prune)
/usr/bin/python3 -I ops/jelly/r2prune/manual_apply.py backfill WORKDIR --manifest M --ledger L [--defer-blocked]
# list open intents; exits non-zero when any is open
/usr/bin/python3 -I ops/jelly/r2prune/manual_apply.py status ROOT [ROOT ...]
```

`backfill` checks the ledger header against the manifest digest, resolves receipt
sources, dry-runs the receipt rows at the ledger's own times (any `blocked` row
stops before the intent), writes the intent (pinning the ledger's digest), then
applies the receipts. A second run on a closed backfill re-runs the dry run and
reports `already_applied`. Keys the ledger leaves without a final outcome keep the
intent open and exit non-zero: they need their own reconciliation.

Every `backfill` run dry-runs every acknowledged delete and applies only the rows
still to insert. With `--defer-blocked`, a key the receipt path refuses (for example
a NAS cleanup source hold added after the delete) gets no receipt and its hold is
never touched: it is written to `WORKDIR/deferred-blocked.tsv` (key, size, reasons),
every admitted key is receipted, and `receipt-result.json` reads `state: deferred`,
so the intent stays open and `status` reports it. Rerun the same command once holds
are released; it applies what they now admit and closes the intent when nothing is
left deferred. Without the flag, any blocked key still refuses the whole run.

Historical receipt reconciliation checks the manifest and
`receipt-sources.json` against the intent's digests and holds
`~/stoarama-r2prune-lane/cycle.lock` (`status` does not); a held lock refuses
at once. Credentials come only from the environment files; no new delete
process is started.

Tests (offline stubs for `psql` and `r2prune`, a local HTTP server for registry GETs):

```sh
/usr/bin/python3 -I ops/jelly/r2prune/test_manual_apply.py -q
```

## The lane

`cycle.py`, `preflight.sql`, `protect.sql` and `receipt-apply.sql` here are the
source for the lane in `~/stoarama-r2prune-lane/` (launchd
`com.stoarama.r2prune-lane`, every 15 minutes). Historical installed copies may
not include this hold.

While `~/stoarama-r2prune-lane/STOP` exists, each cycle logs
`{"cycle": ..., "skipped": "STOP"}` to `runs.jsonl` and exits 0 before any replay, DB
or R2 work. Keep STOP in place while deletion is held.

The catalog preflight sends at most 5,000 candidates per statement
(`ROWS_PER_STATEMENT`), as two arrays, all in one `REPEATABLE READ READ ONLY`
transaction, so its vetoes equal one statement over every candidate: a single
137k-row JSON literal preceded the 2026-11-26 production OOM. Any failed chunk
fails the cycle before planning.

The receipts read (`RECEIPTS`) finds each window's live raw clips through
`r2prune_receipts_live_clip_window` (migration 0273), a partial index of live
`stoarama` clips keyed by recording and both clip bounds, so the heap is read only
for clips it keeps.

```sh
STOARAMA_TEST_DATABASE_URL=... /usr/bin/python3 -I ops/jelly/r2prune/test_cycle.py -q
```

With a test database it also checks the chunked vetoes equal the old JSON-literal
statement on a stub catalog, and that chunks read one snapshot.
