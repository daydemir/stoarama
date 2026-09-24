# NAS joined (collated) delivery

## Path contract

The NAS delivery side owns where collated hours land on the NAS. Collation
writes the path; the NAS client validates it and never invents one.

```
/clips/joined/<nas_relative_path>

nas_relative_path =
  <folder>/<Month>/<DD-Weekday>/<id>_<Plaza>_<YYYY>_<Month>_W<n>_<Weekday>_hour_<HH>[_part_NN]_<HHMMSS>-<HHMMSS>.mp4
hour manifest =
  <folder>/<Month>/<DD-Weekday>/<id>_<Plaza>_<YYYY>_<Month>_W<n>_<Weekday>_hour_<HH>.manifest.json
```

Build it with `recordingnaming.BuildCollatedPath` /
`BuildCollatedManifestPath`; do not format it by hand.

- `<folder>/<Month>/<DD-Weekday>` is exactly the raw clip tree (raw clips sit at
  `/clips/<folder>/<Month>/<DD-Weekday>/…`), so a collated hour mirrors the
  folder and day of the raw clips it replaces.
- `HH` is the local clock hour, `00`–`23`, in the recording's timezone. This
  differs from generation 1, whose `hour_01`–`hour_12` counted window hours
  from 08:00. When DST repeats a local hour, the second one is `hour_HH_dst2`.
- `_part_NN` (two digits, from 01) appears only when an hour has more than one
  part. `HHMMSS-HHMMSS` is the local wall-clock range of the part's first and
  last presented frame. The end may run up to 15 minutes past the hour and wraps
  past midnight for hour 23; the day folder is always the hour's own day.
- The R2 key prefix (`managed/acct-47/…`) is never part of a NAS path. R2 object
  keys for joined media stay content-addressed.

### Why a separate `joined/` tree

Collated files do not go into the raw tree itself. The raw inventory walks
`/clips` (skipping only `joined/`) and matches every file to a clip row; a file
it cannot match is recorded as unmatched, and unmatched files block inventory
enforcement and exact NAS cleanup plans (docs/NAS_EXACT_CLEANUP.md). Putting
joined hours beside raw clips would make every one of them an unmatched file.
`joined/` is already reserved: raw clip paths may not start with it and the
inventory never scans it. The subtree below `joined/` is identical to the raw
tree, so browsing either shows the same folders and days.

Generation 1 files keep their original location,
`/clips/joined/<batch_id>/<folder>/<Month>/<Weekday>/…` (15 files). Batch ids
are lowercase with hyphens and cannot collide with plaza folder names.

## Generation 1 stall (artifact 3830)

Only 15 generation-1 files reached the NAS; each 15–22 MB part took hours and
delivery stopped on 2026-09-09 at artifact 3830. The cause is starvation, not a
broken file:

- Production runs joined protocol generation 8. The client's fair-share mode
  starts at generation 9, so joined delivery runs in yield mode: before each
  8 MiB range, and every 32 MiB of hashing, it lists the raw feed and gives up if
  any raw clip is pending. With about 20 live recordings the raw feed is almost
  never empty, so joined work rarely gets past the first boundary.
- The `io_error` blocker is only the most recent transient network failure
  (an `OSError`) at that yield boundary. It happens before the client records
  transfer progress, so the server's transfer telemetry still shows 2026-09-11
  even though the client restarted on 2026-09-24.
- Even fair-share mode grants one 8 MiB range per raw page, about 11 GB/day,
  and the download rate is hard-coded at 8 MiB/s.

Generation 2 replaces these hours, so generation 1 is not restarted. The
generation-2 lane below runs beside raw delivery with its own server-set budget
instead of yielding to it.

## Generation 2 delivery lane

Collation v2 records each part in `recording_collation_outputs`
(`nas_relative_path`, size, SHA-256, content-addressed R2 object). The NAS
pulls those rows directly:

1. `GET /api/v1/account/collated` returns the connection's delivery policy and a
   page of undelivered outputs (R2-verified, nas_pull recordings of the account).
2. `GET /api/v1/account/collated/{id}/download` presigns the exact object.
3. The client downloads to a hidden partial beside the final path, verifies
   size and SHA-256, fsyncs, links it into place without overwriting, and
   `POST /api/v1/account/collated/ack` records the exact identity.

The policy (per connection, server-controlled): enabled, bytes/s budget,
parallel downloads. It runs concurrently with raw delivery, so neither lane can
starve the other.
