# Manual r2prune deletes

Owner, 2026-11-25 (via the project lead), verbatim: "make every manual r2prune
apply write receipts at apply time, so a DB record can never be skipped again."

Two manual `r2prune delete` runs (2026-11-21 outside-window, 2026-11-22 delivery
videos) deleted R2 objects whose `r2_object_retirements` receipts were never
written, because they were run as a bare binary from a shell. Any `r2prune delete`
outside the R2 prune lane now goes through `manual_apply.py`, on Jelly:

```sh
/usr/bin/python3 -I ops/jelly/r2prune/manual_apply.py delete WORKDIR -- \
  --manifest plan/delete-manifest.jsonl --expected-sha256 HEX --summary plan/summary.json \
  --fresh-listing snapshot.tsv <the plan's rule flags> [--limit N] [--workers 8]
```

Everything after `--` is passed to `r2prune delete` unchanged, except `--ledger`,
which the wrapper owns. Use one new `WORKDIR` per reviewed manifest; a canary and
the full run share it.

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
2. Preflight: runs `dry-run.sql` (plain read-only SELECTs) on the receipt rows the
   run would write if every not-yet-deleted key were deleted now. Any `blocked`
   row (source drift, an existing receipt, an owner keep, a reservation, a NAS
   restore, cleanup or incident hold, or a trigger the apply fires, such as
   `nas_cleanup_source_hold` on `purged_at`) stops the run before any DELETE.
   Output is kept in `WORKDIR/preflight/`.
3. Writes `delete-intent.json` (manifest and sources digests), then runs
   `r2prune delete --ledger WORKDIR/ledger-<stamp>.jsonl ...`.
4. Whether or not `r2prune` succeeded, renders `receipt-backfill.tsv` from every
   ledger in `WORKDIR` (one row per acknowledged `deleted` key, earliest time;
   reason `r2prune_ledger_<class>`, choice `none`, the ledger's manifest digest and
   policy digest, exactly as the lane's `cycle.py receipt_tsv`), and applies it
   with `receipt-apply.sql`: the lane's receipt path, committed here byte for byte
   (sha256 `2e06ede3…a2d4`). It inserts the receipts and sets raw `purged_at` in
   500-row transactions, retries `55P03` lock contention, and resumes from the
   last committed batch.
5. Writes `receipt-result.json` only after the apply verified its readback.

It fails closed: a failed receipt write exits non-zero and leaves the intent open
with no `receipt-result.json`. Fix the cause and replay just the receipts:

```sh
/usr/bin/python3 -I ops/jelly/r2prune/manual_apply.py receipts WORKDIR
```

A later `delete` in the same `WORKDIR` replays an open intent first. The wrapper
holds `~/stoarama-r2prune-lane/cycle.lock`, so it never runs beside a lane cycle.
Credentials come only from `~/Build/stoarama/local/recording-supervisor.env`
(catalog, as libpq `PG*` variables, never argv) and `render.env` (R2, passed to
`r2prune`); the DB password is redacted from any error.

Tests (offline stubs for `psql` and `r2prune`):

```sh
/usr/bin/python3 -I ops/jelly/r2prune/test_manual_apply.py -q
```
