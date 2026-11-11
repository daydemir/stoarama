# Jelly-hosted Stoarama services

Jelly (`jellys-mac-mini`) runs small Stoarama services as plain user launchd jobs.
They do not depend on Muavin.

| Job | What it serves | Code | Data/state |
|---|---|---|---|
| `com.stoarama.seam-review` | Seam/repair review UI on 127.0.0.1:8476 (tailnet: `https://jellys-mac-mini.taildfdcc0.ts.net:8476`) | `ops/jelly/seam-review/*.py` (snapshot of `~/.stoarama/seam-review/app/stoarama-proof/review/`) | review data stays in `~/.stoarama/seam-review/` (~19 GB, not in git) |
| `com.stoarama.r2-select` | R2 keep picks page + deletion preview on 127.0.0.1:8787 (tailnet: `:8443`) | `ops/jelly/r2-select/server.py` (runs from `~/stoarama-r2-select/`) | `~/stoarama-r2-select/r2-picks.json`, `history/`, `public/` |
| `com.stoarama.repair` | Batch repair of truncated relay MP4s (no moov): R2 original -> Annex B remux -> full decode check -> upload to `repaired/<original key>`; originals never modified | `ops/jelly/repair/` (installed at `~/stoarama-repair/bin/`) | `~/stoarama-repair/ledger.sqlite` (+ `ledger.jsonl`, `run.log`, `candidates.tsv`, `selection.json`, `work/`) |

The plists in `ops/jelly/launchd/` are the installed definitions.

Install or restart a job:

```sh
cp ops/jelly/launchd/<label>.plist ~/Library/LaunchAgents/
launchctl bootout gui/$(id -u)/<label> 2>/dev/null || true
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/<label>.plist
```

Retired on 2026-11-10: Muavin's `stoarama-audit` (port 8765) and `stoarama-audit-refresh`. They had been failing since 2026-09-15 because their report sources were deleted. Their old data is archived on Mooney at `~/stoarama-archive/jelly-review-20261110/`.

## Repair job

Migrated from Mooney to Jelly on 2026-11-10. Mooney's run was drained with a STOP file, its WAL was checkpointed, and the ledger was copied byte-identically to Jelly (sha256 6885c152...). It signs R2 URLs locally with `presign_remote.py`, which reads the existing scoped credentials in `~/Build/stoarama/local/render.env` and never prints them.

- Progress: `~/stoarama-repair/bin/status.sh`
- Graceful stop: `touch ~/stoarama-repair/STOP` (remove it before restarting)
- Resume: `launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.stoarama.repair.plist`

The job is resumable and idempotent: in-flight rows reset to pending on start, and existing `repaired/` objects are HEAD-checked before each PUT.

## Cleanup runner (not installed by this change)

`cleanup-runner/README.md` documents the supervised standing-approval NAS cleanup
job, offline tests, owner delivery, dry-run effects, and stop/drain procedure. Its
launchd definition is `launchd/com.stoarama.cleanup-runner.plist`. Install separately
only between batches with no enabled plan and master OFF.

## R2 keep calendar

`r2-select/public/` now contains the formerly live-only picker, deletion preview,
109-stream catalog and thumbnails. The first implementation commit snapshots
Jelly's live files (server.py matched; public/ and select.py were missing).
`select.py` is a historical, offline window generator: **do not run it in the
live app** because it writes `selection.json`. Use Python's `-I` flag for the
other scripts; the legacy filename shadows the standard-library `select` module.

The picker reads static `public/coverage.json`. The export includes every keep28
policy stream plus any account-47 streams with R2 raw clips or collation outputs.
Timezones and pinned windows come from
`backend/internal/api/nas_cleanup_keep28_policy.json` at the implementation's
`origin/main`; non-policy streams use `streams.local_timezone` and have no pinned
window. Unknown timezones fail the export.

Coverage unions valid `recording_clips` timestamp ranges before splitting them
at each local midnight. It excludes purged clips, R2 retirements, non-R2 endpoints
and empty object pointers. This is DB-catalog presence, **not** a new R2 HEAD
inventory. Stitched outputs do not fill raw coverage gaps. Historical catalog
bounds (including purged clips) preserve the full recording span; absent raw R2
days show zero. Hours and days recorded count the surviving raw coverage. All
days divide by 86,400, including 23/25-hour DST days (which can exceed 100%).
Green is strictly >90%; yellow includes exactly 70% and 90%; zero uses an empty
dashed outline. Average coverage includes every gap day. `Days ≥90%` includes
exactly 90%, even though that day's colour is yellow. Current raw and registered
collation storage sizes are refreshed in the same snapshot; uncatalogued V85
outputs remain outside the cost estimate.

Generate on Jelly, with this script, the policy and the catalog staged together
outside the live app; load `DATABASE_URL` only there (never copy the env file):

```sh
set -a
. ~/Build/stoarama/local/recording-supervisor.env
set +a
python3 -I coverage.py --policy nas_cleanup_keep28_policy.json \
  --catalog streams.json --output coverage.json
```

The script sets `PGOPTIONS='-c default_transaction_read_only=on'`, uses Homebrew
PostgreSQL 17 psql, captures DB errors without exposing connection details and
makes one SELECT query. Alternatively `--sql-only` emits the query for an SSH
stdin pipe to the same read-only psql; `--query-result <file>` builds JSON from
its result. No request-time DB access is added.

Validation:

```sh
python3 -I ops/jelly/r2-select/test_app.py -q
NODE_PATH=<installed-playwright-node_modules> node ops/jelly/r2-select/browser_test.cjs http://127.0.0.1:8787
```

`coverage_fixture.sql` can be prepended as CTEs to `coverage.query()` (replace its
initial `WITH `) to shadow DB tables with SELECT-only fixtures. It proves range
union, midnight splits, a 25-hour DST day, account isolation, purge/retirement
exclusions and a non-policy stream. Offline HTTP tests exercise the real handler
with an isolated temporary root: owner saves preserve every pick, metadata and
pinned-window bytes; non-owner requests are denied. Browser tests intercept
selection requests and prove unchanged and unknown-stream picks survive saves.

Deployment manifest is **only** `server.py`, `public/index.html`, `public/app.css`,
`public/app.js`, `public/coverage.json`. Before replacing these exact files,
copy existing versions to a new timestamped `~/stoarama-r2-select/history/code-*`
directory, recording absent files too. Hash `selection.json` and `r2-picks.json`
before and after, preserving absence as a state. Do not copy a whole directory or
use a delete/sync flag. Restart the existing job with
`launchctl kickstart -k gui/$(id -u)/com.stoarama.r2-select`, then curl the tailnet
URL from Jelly. Keep the old thumbnails, plan, catalog and both selection files.

The owner-only save header rule is unchanged. The browser refuses to save after
a failed picks load, preserves untouched picks verbatim, and detects a document
changed since load. This is an advisory conflict check, not an atomic multiwriter
lock. Verify successful saves in a temporary copy seeded from existing picks;
do not write test picks into the owner's live file. On 2026-11-11 that file was
absent, so preserve absence and use an isolated Jelly handler for the round-trip.
