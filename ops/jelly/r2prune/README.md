# Manual r2prune deletes

Owner, 2026-11-25 (via the project lead), verbatim: "make every manual r2prune
apply write receipts at apply time, so a DB record can never be skipped again."

Two manual `r2prune delete` runs (2026-11-21 outside-window, 2026-11-22 delivery
videos) deleted R2 objects whose `r2_object_retirements` receipts were never
written, because they were run as a bare binary from a shell. Any `r2prune delete`
outside the R2 prune lane now goes through `manual_apply.py`, on Jelly. It has two
delete modes, chosen by the manifest's classes:

```sh
# listing: any manifest without delete_ke_raw_stitched rows
/usr/bin/python3 -I ops/jelly/r2prune/manual_apply.py delete WORKDIR -- \
  --manifest plan/delete-manifest.jsonl --expected-sha256 HEX --summary plan/summary.json \
  --fresh-listing snapshot.tsv <the plan's rule flags> [--limit N] [--workers 8]

# verify: a manifest of delete_ke_raw_stitched rows only (class (a)); the wrapper owns --fresh-listing
/usr/bin/python3 -I ops/jelly/r2prune/manual_apply.py delete [--chunk 5000] [--chunks K] WORKDIR -- \
  --manifest plan/delete-manifest.jsonl --expected-sha256 HEX --summary plan/summary.json \
  <the plan's rule flags, including --ke-coverage> [--workers 8]
```

Everything after `--` is passed to `r2prune delete` unchanged, except `--ledger`
(and in verify mode `--fresh-listing`), which the wrapper owns. Use one new
`WORKDIR` per reviewed manifest; a canary and the full run share it.

`r2prune` itself lives only on unmerged branches (`backend/cmd/r2prune`); the
binary is `~/stoarama-r2prune-lane/r2prune`. The wrapper does not change it.

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

### Verify mode (class (a), `delete_ke_raw_stitched`)

The owner's condition for the class (a) delete: every chunk is re-verified live
right before its own `r2prune` call. A chunk is the previous chunks' unsettled raws
first, then up to `--chunk` (default 5000) raws not yet sent. For each, read-only:

- **Raw:** R2 HEAD shows the planned size, ETag and mtime; its `recording_clips`
  row has the planned size and `purged_at IS NULL`.
- **Every covering output** (each `--ke-coverage` row at the raw's size): R2 HEAD
  shows the coverage size; exactly one `recording_collation_outputs` row at that
  size; the raw's clip is still in its `source_clip_ids`; no
  `r2_object_retirements` row, `r2_retirement_reservations` row, owner keep
  (`r2_owner_keep_held`) or `r2_nonraw_intents` row for the output key; the output
  is in no other open intent's manifest (manual workdirs beside `WORKDIR` and lane
  cycles).

**The first mismatch stops the run before that chunk's `r2prune` call.** The
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

## Other commands

```sh
# replay the receipts of an open intent
/usr/bin/python3 -I ops/jelly/r2prune/manual_apply.py receipts WORKDIR
# receipts for a run made without the wrapper (never starts r2prune)
/usr/bin/python3 -I ops/jelly/r2prune/manual_apply.py backfill WORKDIR --manifest M --ledger L
# list open intents; exits non-zero when any is open
/usr/bin/python3 -I ops/jelly/r2prune/manual_apply.py status ROOT [ROOT ...]
```

`backfill` checks the ledger header against the manifest digest, resolves receipt
sources, dry-runs the receipt rows at the ledger's own times (any `blocked` row
stops before the intent), writes the intent (pinning the ledger's digest), then
applies the receipts. A second run on a closed backfill re-runs the dry run and
reports `already_applied`. Keys the ledger leaves without a final outcome keep the
intent open and exit non-zero: they need their own reconciliation.

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
