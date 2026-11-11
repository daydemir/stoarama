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
