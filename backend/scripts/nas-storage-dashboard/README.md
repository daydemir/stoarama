# Private NAS storage dashboard

Read-only operator projection for account 47 / NAS connection 13. The existing
NAS pull client supplies audit progress and a completed, checksum-verified stat
report. This collector uses read-only PostgreSQL transactions and bounded R2
ListObjectsV2 metadata requests. It never connects to the NAS, opens media,
changes remote storage, enables workers, or authorizes cleanup.

`refresh.py --state DIR --publish DIR [--r2-budget SECONDS]` imports an incremental
inventory/ACK index, imports a completed report if available, and emits static
JSON. Python 3.9+, psql, the pinned audit-report CLI, and the existing scoped local
credential environment are required. `run_refresh.sh` is the installed Jelly
entry point; keep scripts, credentials, and the SQLite cache outside the served
folder. Both output directories are app-owned; nonempty unmarked directories and
unknown output JSON are preserved. Refreshes are serialized with flock. The entire stream/day map is one atomic JSON publication. Failed
refreshes retain the previous published snapshot and expose a failure status.

The initial day map is explicitly provisional inventory/ACK evidence. After the
verified report imports, only stat-observed paths matching known file sizes map
to days. Recycle-bin paths are separate. Errors, undated files, unrecognized
paths, and bytes that cannot map stay unknown; they are never inferred empty.
The walk is a time-spanning observation of the clips mount, not a transactional
snapshot of every NAS share. Changes after a file's stat time require a new NAS
audit; a dashboard refresh does not re-stat the NAS.

Coverage is the union of registered source intervals per stream and local
calendar day, separately for clips and stitched files. Duplicate files retain
their full logical size but add no overlapping coverage. Midnight/DST boundaries
split time; file counts and bytes belong to the first covered day. Timelines use
elapsed hours since local midnight (23/25 hours on DST transition days). Stitched
coverage describes included source intervals, not a new payload-quality test.
Best-14 and Best-20 windows are the previously selected, frozen 2026-10-19
retention evidence; they are not deletion rules.

R2 evidence is key+size metadata with check timestamps, never a fresh content
hash. A bounded listing resumes its cursor each refresh. Absence stays unchecked
until its prefix has completed and both the NAS evidence and local discovery
of the expected key predate that crawl’s start. Only expected NAS object keys
are cached; unrelated objects are listed but not indexed into the day map.
Completed prefix indexes are rebuilt after 24 hours. R2 indexing pauses below
6.25 GiB of local free space before import (5.125 GiB after import); NAS progress still
publishes. The local report index stores media candidates only; other entries
remain in the verified full artifact and scan summary. Large indexes can take
multiple refreshes to finish. Old matches are explicitly dated; neither a
listing absence nor a size match proves a safe cleanup operation.

Install the three `site/` assets plus generated JSON under the existing private
review server's `vid/nas-storage/` directory. The existing localhost:8476 server
and Tailscale HTTPS proxy own hosting. No new daemon or public funnel is needed.
The server must allow JSON MIME types; no secrets/scripts belong in this path.

Use the existing Hermes scheduler's script-only `--no-agent` mode, schedule
`*/30 * * * *`, and local delivery. Install the entry point under
`~/.hermes/scripts/`; no model calls or extra compute workers are needed. Verify
an actual execution record and the next scheduled run. The browser polls the
static snapshot every minute and flags age over 45 minutes. Host/network failure
can delay updates; previous data remains dated rather than appearing current.

The one-time owner command `--retire-obsolete-cache-sha256 SHA256` retires only
`state/metadata.sqlite3`, the v1 preview's generated index, while holding the
collector lock. Obtain its digest from the positively identified preview artifact.
Unknown/changed files, symlinks, hardlinks and SQLite companions are retained.
This flag is never part of the recurring job and never touches NAS files.

Tests: `python3 -m unittest discover -s backend/scripts/nas-storage-dashboard -p 'test_*.py' -v`.
