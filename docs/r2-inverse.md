# Full-bucket R2 partition

`stoaramactl r2-inverse partition --picks /path/to/r2-picks.json --out /new/report/dir`

This command reads the entire configured bucket without a prefix filter. It
loads the catalog and protections in a PostgreSQL read-only repeatable-read
transaction, then writes `partition.jsonl` (every listed key, class and reason)
and `summary.json` (per-class object counts and bytes, exactly reconciled to the
listing total). It never calls DELETE or changes database rows, and requires no
writer fence or drain evidence. Only a complete successful report is usable;
partial JSONL without summary is an interrupted report. Output directories must
be new, so earlier evidence is never overwritten.

Protected classes: Keep Everything by stream or a known stream-folder path;
keep28 by pinned stream window (including every collated source); permanent
incident roster and dependent outputs; cleanup reservations and unretired or
ever-issued source holds; retirement reservations/receipts; pending restores;
joined source snapshots, snapshotting scopes and dry-run scopes; A1 `blocked` items by exact bucket/key regardless of
inventory; delivery keys and source/output dependencies; stitch tools under
`experimental/v85-retired-WORK-custody/`; unknown or ambiguous identities.
Delivery remains protected pending Deniz's answer. No hold is inferred released
from an exact NAS inventory record or a changed cleanup receipt.

Incident identities come from `nas_cleanup_incident_files` and
`nas_cleanup_incident_sources`, independently of mutable plan/receipt state.
Missing required tables/views or unreadable holds fail the command. All unknown
keys appear individually in JSONL and are ineligible for deletion. `deletable`
is a planning classification only; this report grants no wire authority.

The sole controller runs this read-only on Jelly after merge. Compare total
bucket bytes with the approximately 53 TB bill, inspect unknown counts, and
retain the report as durable evidence. No production partition was run by the
implementation agent. Collated outputs are held in this release, including outside-window outputs;
raw keys with multiple catalog identities remain unknown. The executor is a separate change and has additional
live fence, drain, intent, budget and provider checks.
