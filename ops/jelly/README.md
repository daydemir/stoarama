# Jelly-hosted Stoarama services

Jelly (`jellys-mac-mini`) runs two small Stoarama services as plain user launchd jobs.
They do not depend on Muavin.

| Job | What it serves | Code | Data/state |
|---|---|---|---|
| `com.stoarama.seam-review` | Seam/repair review UI on 127.0.0.1:8476 (tailnet: `https://jellys-mac-mini.taildfdcc0.ts.net:8476`) | `ops/jelly/seam-review/*.py` (snapshot of `~/.stoarama/seam-review/app/stoarama-proof/review/`) | review data stays in `~/.stoarama/seam-review/` (~19 GB, not in git) |
| `com.stoarama.r2-select` | R2 keep picks page + deletion preview on 127.0.0.1:8787 (tailnet: `:8443`) | `ops/jelly/r2-select/server.py` (runs from `~/stoarama-r2-select/`) | `~/stoarama-r2-select/r2-picks.json`, `history/`, `public/` |

The plists in `ops/jelly/launchd/` are the installed definitions.

Install or restart a job:

```sh
cp ops/jelly/launchd/<label>.plist ~/Library/LaunchAgents/
launchctl bootout gui/$(id -u)/<label> 2>/dev/null || true
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/<label>.plist
```

Retired on 2026-11-10: Muavin's `stoarama-audit` (port 8765) and `stoarama-audit-refresh`. They had been failing since 2026-09-15 because their report sources were deleted. Their old data is archived on Mooney at `~/stoarama-archive/jelly-review-20261110/`.
