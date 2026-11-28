# Manual r2prune deletes

Owner, 2026-11-25 (via the project lead), verbatim: "make every manual r2prune
apply write receipts at apply time, so a DB record can never be skipped again."

Two manual `r2prune delete` runs (2026-11-21 outside-window, 2026-11-22 delivery
videos) deleted R2 objects whose `r2_object_retirements` receipts were never
written, because they were run as a bare binary from a shell. Any `r2prune delete`
outside the R2 prune lane now goes through `manual_apply.py`, on Jelly. It has two
delete modes, chosen by the manifest's classes. Keep Everything keys stay in R2
regardless of stitched coverage. The retired class `delete_ke_raw_stitched` and
legacy `--ke-coverage` flag are refused for deletion; historical manifests remain
readable for receipt reconciliation only.

```sh
# listing: ordinary manifests without covered best-14 rows
/usr/bin/python3 -I ops/jelly/r2prune/manual_apply.py delete WORKDIR -- \
  --manifest plan/delete-manifest.jsonl --expected-sha256 HEX --summary plan/summary.json \
  --fresh-listing snapshot.tsv <the plan's rule flags> [--limit N] [--workers 8]

# verify: only delete_best14_raw_stitched rows, --best14-coverage; the wrapper owns --fresh-listing
/usr/bin/python3 -I ops/jelly/r2prune/manual_apply.py delete [--chunk 5000] [--chunks K] WORKDIR -- \
  --manifest plan/delete-manifest.jsonl --expected-sha256 HEX --summary plan/summary.json \
  <the plan's rule flags, including the class's coverage flag> [--workers 8]
```

Everything after `--` is passed to `r2prune delete` unchanged, except `--ledger`
(and in verify mode `--fresh-listing`), which the wrapper owns. Use one new
`WORKDIR` per reviewed manifest; a canary and the full run share it.

`r2prune`'s source is `backend/cmd/r2prune`; the binary is
`~/stoarama-r2prune-lane/r2prune` (`go version -m` names the commit it was built
from). The wrapper does not change it.

## What the wrapper does

1. Checks the manifest digest, then resolves every manifest key to its catalog
   receipt identity, read-only: a raw key to its one `recording_clips` row, a
   collated key to its one `recording_collation_outputs` row, with the identity
   the lane's `receipt_ready` checks (size, sha256, raw ETag). Saved as
   `receipt-sources.json`. **Any key without one stops the run before any
   DELETE**, including `experimental/stitching-confident-repairs/` keys, which
   have no catalog row: those retire only as `delivery_repair` through
   `stoaramactl r2-inverse`.
2. Before each `r2prune` call, preflight: runs `dry-run.sql` (plain read-only
   SELECTs) on the receipt rows that call would write if every key it may delete
   were deleted now. Any `blocked` row (source drift, an existing receipt, an owner
   keep, a reservation, a NAS restore, cleanup or incident hold, or a trigger the
   apply fires, such as `nas_cleanup_source_hold` on `purged_at`) stops the run
   before any DELETE. Output is kept in `WORKDIR/preflight/`.
   The source lookup and the dry run each send at most 5,000 rows per statement
   (`ROWS_PER_STATEMENT`): a single 293k-row literal dropped the production
   connection. Every check is per row, so `dry-run.out` is the merged one-shot
   output: summary groups summed, then the non-insert rows in seq order.
3. Writes `delete-intent.json` (mode, manifest and sources digests, and in listing
   mode the `--fresh-listing` digest), then runs
   `r2prune delete --ledger WORKDIR/ledger-<stamp>.jsonl ...`.
4. Whether or not `r2prune` succeeded, renders `receipt-backfill.tsv` from the
   deletes that ledger acknowledged first (earliest time across every ledger;
   reason `r2prune_ledger_<class>`, choice `none`, the ledger's manifest digest and
   policy digest, exactly as the lane's `cycle.py receipt_tsv`), and applies it
   with `receipt-apply.sql`: the lane's receipt path, committed here byte for byte
   (sha256 `2e06ede3…a2d4`). It inserts the receipts and sets raw `purged_at` in
   500-row transactions, retries `55P03` lock contention, and resumes from the
   last committed batch.
5. Writes `receipt-result.json` only after the apply verified its readback. It is
   `complete` only when every manifest key has a final ledger outcome (`deleted`,
   `skipped_protected`, `skipped_changed`); a key that was never logged (lost
   reply, crash, a half-written last line) or logged `failed` may still be gone
   from R2, so it keeps the intent open.

Every ledger is refused if its header names another manifest or another fresh
listing than the intent pins, or if it logs one key twice.

### Listing mode

The intent pins the first run's `--fresh-listing` digest. While the intent is
open, a rerun with another listing is refused: a key absent from a new listing
would be logged `skipped_changed absent_from_fresh_listing`, which is final, so a
key an earlier attempt really deleted would close without a receipt. A full run
(no `--limit`) that leaves keys without a final outcome exits non-zero; rerun the
same command so r2prune settles them. A canary leaves the intent open without
failing.

### Verify mode (`delete_best14_raw_stitched`)

The owner's condition for the covered deletes: every chunk is re-verified live
right before its own `r2prune` call. A chunk is the previous chunks' unsettled raws
first, then up to `--chunk` (default 5000) raws not yet sent. For each, read-only:

- **Raw:** R2 HEAD shows the planned size, ETag and mtime; its `recording_clips`
  row has the planned size and `purged_at IS NULL`; the raw key has no owner keep.
- **Every covering output** (each coverage row at the raw's size): R2 HEAD shows
  the coverage size; exactly one `recording_collation_outputs` row at that size, in
  a `collated` hour; the raw's clip is still in its `source_clip_ids` and its
  catalog `clip_start_at`..`clip_end_at` lies inside the output's
  `start_at`..`end_at`; the output has an owner keep (such as
  `collation_v2_recovery_held`), the protection a sole copy must have (a
  `delivery_keep_holds` row alone blocks no retirement since 0244); no
  `r2_object_retirements`, `r2_retirement_reservations` or `r2_nonraw_intents` row
  for the output key; no NAS output restore left unverified; and the output is in
  no other open intent's manifest (manual workdirs beside `WORKDIR` and lane
  cycles).

**The first mismatch stops the run before that chunk's `r2prune` call.** `--chunk`
is at most 5,000, and a run without `--chunks` is refused until a canary chunk
(`--chunks 1`) has settled in the workdir. The
wrapper then writes `listing-<stamp>.tsv` holding only the chunk's verified raws
(at their planned identity) and their covering outputs (as HEAD reported them),
and passes it as `--fresh-listing`. r2prune logs every key outside the chunk as
`skipped_changed absent_from_fresh_listing`; the wrapper reads those as
**deferred** (no state, never final) and drops them from the ledger once r2prune
exits, so 60 chunks do not write 60 manifest-sized ledgers. A raw an earlier chunk
sent to DELETE (`failed` or unlogged) may already be gone from R2; it is listed at
its planned identity so r2prune's idempotent DELETE acknowledges it and it gets
its receipt. A chunk that leaves any of its keys unsettled stops the run; rerun the
same command. `--chunks K` stops after K chunks (a canary) with the intent open.

### Covered best-14 raws the public GSS page serves stitched

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

Before every `r2prune` call (both modes, every chunk) the wrapper reads the
generation `gss_public_generation_pin` names now, GETs its registry
`public/generations/<id>.json` and fails closed unless no key that generation
serves (files and assets) is in the keys about to be deleted, every one has a
`page_link` row in the plan's `--db-protect` file, and every one the plan's
`classified.tsv` (beside `--summary`) lists is `protect_live_site`. Re-deriving it
per chunk catches a generation swap between chunks; the pin is read again after
each chunk, and a move during one stops the run once its receipts are written. `manual_apply.py gate PLAN_DIR
--db-protect FILE` runs the same check on a plan.

`manual_apply.py served-keys OUT` writes the same served keys, one per line, for
`r2prune --served`: each is `protect_live_site` everywhere, where a `page_link` row
holds only inside a window or on Keep Everything. A plan meant for a manual delete
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

A later `delete` in the same `WORKDIR` replays an open intent first and re-checks
the manifest and `receipt-sources.json` against the intent's digests. The wrapper
holds `~/stoarama-r2prune-lane/cycle.lock` (`status` does not), so it never runs
beside a lane cycle; a held lock refuses at once. Credentials come only from
`~/Build/stoarama/local/recording-supervisor.env` (catalog, as libpq `PG*`
variables, never argv) and `render.env` (R2, passed to `r2prune` and used in
memory to sign HEADs); the DB password is redacted from any error.

Tests (offline stubs for `psql` and `r2prune`, a local HTTP server for R2 HEAD):

```sh
/usr/bin/python3 -I ops/jelly/r2prune/test_manual_apply.py -q
```

## The lane

`cycle.py`, `preflight.sql`, `protect.sql` and `receipt-apply.sql` here are the
installed copies in `~/stoarama-r2prune-lane/` (launchd `com.stoarama.r2prune-lane`,
every 15 minutes). Install them by copying between cycles, while
`~/stoarama-r2prune-lane/cycle.lock` is free.

While `~/stoarama-r2prune-lane/STOP` exists, each cycle logs
`{"cycle": ..., "skipped": "STOP"}` to `runs.jsonl` and exits 0 before any replay, DB
or R2 work. Remove the file to resume.

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
