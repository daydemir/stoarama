# R2 retirement from the saved owner picks

Operator execution only. Planning is read-only; this change neither runs an R2
DELETE nor queues a production copy, deploys code, applies migrations, changes the
owner's picks, nor changes the running cleanup job.

## Scope and accounting

Extend `stoaramactl r2-retire plan` with `--nas-backed --connection-id 13`.
The saved pick must match the pinned keep28 window and timezone, policy SHA
`060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811`.
`all` and unknown picks stay. The existing prefix protections stay.
A current R2 listing of the account catalog's managed folder and joined-version
prefixes supplies exact keys, ETags and sizes; missing catalog objects do not count as savings. Uncatalogued/purged keys
in those prefixes are blocked, with stream 0. The checksummed `r2-listing.jsonl`
records the exact prefixes; eight concurrent listing workers avoid serializing
millions of keys. Other bucket prefixes are outside
this estimate. $/month = decimal bytes / 1e12 * $15; requests are not deducted.

Every phase is an exact JSONL manifest with bucket, key, size, SHA/ETag and
reason, plus a SHA-256 in `summary.json`. `streams.tsv` includes all observed
streams and their phase totals. Only `manifest.jsonl` and `phase1.jsonl` are
executable. Later/copy/blocked manifests cannot be passed to `apply`.

- `phase1.jsonl`: currently empty. Outside-window objects lacking positive
  NAS absence evidence are blocked as `nas_absence_unproven`. A missing inventory
  record, an old `present` verification, or a `missing` row alone never authorizes
  retirement. Old manifests are revalidated by the same rule during apply.
- `manifest.jsonl` / `now`: kept-window `none` footage whose **same
  file bytes** have exact, fresh present NAS inventory. Timeline overlap with a
  different stitched file never substitutes for identity.
- `later.jsonl`: outside-window footage still present on NAS, otherwise eligible.
  This is conditional potential, **not promised eligibility after cleanup**:
  current cleanup retains whole-plan R2 recovery holds permanently once any
  deletion is issued. Such a hold moves the object to blocked on re-plan, even
  after clean receipts. Releasing/reconciling these holds needs separate reviewed
  tooling; this command never releases them or assumes completion releases them.
- `copy.jsonl`: kept raw `none` footage absent from NAS. `copy-batches/` contains
  bounded subsets, at most 5,000 objects / 100 GB each, with their own digests.
  Stitched outputs without exact `nas_inventory_files` evidence remain blocked;
  an ACK alone never authorizes their kept-window R2 deletion.
- `blocked.jsonl`: `all`, protected prefixes, held/uncertain/shared/referenced
  objects, stale/mismatched NAS evidence, or unprovable identity/timing.

`nas_raw_retained_bytes` counts raw bytes pinned by the proposed NAS-backed and
copy-then-retire operations. Deleting these kept raw R2 copies prevents V85r NAS
raw retirement: NAS will retain **raw plus stitched**, as V85r still requires R2
recovery. It is an upper bound on foregone raw reclamation, not a promise all
those files would otherwise pass V85r inclusion/decoder gates. Check reported
free space against the copy bytes plus the existing 1.25 TB NAS pull reserve.

## Coordination and freshness

Migration **0182** is mandatory before NAS-backed apply. Each item takes the
existing cleanup admission fence then the public fence, locks the catalog row,
reloads changed cleanup manifests/holds, and rechecks live identity, owner picks,
policy, reservations and eligibility. Unrelated enabled cleanup plans may keep
running; referenced objects are excluded. Legacy manifests retain the existing
global enabled-plan veto.

Before each R2 deletion the tool independently commits an append-only
`r2_retirement_reservations` row. That intent survives crashes or failed DB/R2
responses. Database triggers reject new cleanup plans/source holds that would
rely on it; the reverse guard rejects an R2 reservation for an already-reserved
cleanup object. Together with the shared fences this prevents overlapping
admission. Existing issued/uncertain plans stay protected. Reservations do not
expire automatically, including on failure; retain the manifest/ledger and resume
the same manifest after investigation. Never delete a reservation to retry.

For kept footage, NAS `verified_at` must be within 24 hours (maximum 5 minutes
future skew), the server must have received the evidence within 24 hours, and
raw files must include SHA/size evidence for the canonical
`<media-path>.stoarama.json` sidecar. Apply rechecks these limits and locks the
matching inventory row through deletion/commit. A recent heartbeat that merely
re-sends an old payload hash does not refresh `verified_at`. Rehash/copy and
report again when stale. Each R2 delete uses `If-Match` on the manifest ETag,
rechecks HEAD size/SHA metadata, and requires a missing-object HEAD afterward.
The existing fsynced intent/done ledger and DB receipts remain. The rolling
budget charges retirement receipts from the last 24 hours plus **all unresolved
reservations, regardless of age or manifest**. A successful R2 DELETE followed
by a failed DB commit therefore cannot restore deletion budget. A reservation
with a matching completed retirement is charged once, by the receipt timestamp;
unresolved reservations never expire automatically. Retries may conservatively
charge their reserved bytes again against the proposed run; investigate rather
than deleting reservations to obtain budget.

Cloudflare's [compatibility table](https://developers.cloudflare.com/r2/api/s3/api/)
and [conditional-write documentation](https://developers.cloudflare.com/r2/api/s3/conditional-operations/)
do not document conditional `DeleteObject`. NAS-backed apply therefore also
requires a checksummed, bucket-and-endpoint-bound proof from the **operator-only**
`ops/jelly/r2-retire/verify_conditional_delete.py` probe, no older than 24 hours.
It creates one random 64-byte scratch key, requires a wrong ETag delete to return
`PreconditionFailed` with identical bytes still readable, then confirms a matching
ETag delete. If unsupported or ignored, it writes no proof and apply stays
blocked. Never remove the precondition or substitute an unconditional delete.
The implementation agent does not run this probe or any DELETE.

## Phase-1 evidence audit (2026-11-12 UTC)

Read-only SQL on Jelly found connection 13 still advertising incomplete
`scan-20261030T171934-b238fad7`, started October 30 at 17:19:34 UTC, with
`inventory_scan_completed_at IS NULL`. Its latest pass began November 12 at
00:06:09 UTC and reported 28,800 visited rows, zero skips, at 01:00 UTC;
`client_previous_exit=self_update`. These are progress reports, not a completed
full scan. Historical interruptions cannot be attributed conclusively from this
single telemetry snapshot.

The completion path exists: `Inventory._full_scan_locked` resumes incomplete
metadata, starts a new pass, traverses the dataset, flushes dirty rows, refuses
completion on skips/interruption, posts `complete=true`, then saves completion
locally only after acceptance. Server inventory completion persists the timestamp
and clears the in-progress generation; heartbeat also accepts a non-null completion
from the client. A heartbeat or partial page cannot manufacture it. Client
self-updates restart a pass, and the cleanup execution lane pauses inventory at
maintenance checkpoints. The evidence establishes that no completion was published,
not which historical interruption prevented each pass. No production scan,
restart, deployment or metadata repair was performed for this audit.

Furthermore, `raw-dataset-v2` explicitly excludes `joined/` and `#recycle/`.
Even a completed raw scan would not prove collated-path absence. A scan begun
before a later possible delivery also cannot prove that delivery absent. No
fresh exact-path non-existence attestation is currently supplied to this tool.
`last_cursor_id` is delivery progress, not a complete delivery ledger; released
clips were acknowledged after delivery, and an unreleased clip may have been
written before acknowledgement failed. Neither state proves a file was never
on disk. These classes remain blocked, including rows with no inventory history.

The strongest available positive evidence is an accepted exact cleanup deletion
receipt: 8,714 `deleted` items with issued permits and verified R2 recovery
(`r2_sha_verified` or `r2_etag_bound`). However, **every one remains reserved**
by `nas_cleanup_reserved_files`; issued plans also preserve their whole-plan R2
source/recovery holds. This patch deliberately preserves those protections.
Receipt presence alone grants no R2 deletion authority. A future workflow must
resolve recovery obligations explicitly before admitting this class; this tool
neither releases holds nor infers that completion releases them. Consequently no
currently evidenced class qualifies for phase 1.

Any non-missing linked or unmatched inventory at a candidate path vetoes phase 1
at any age; collated presence now checks `nas_inventory_files` as well as output
ACKs and unmatched files. Exact-present freshness remains relevant only to
kept-window `none` retirement. The new `*-v2` plan is the reviewable result;
previous `20261111-final/phase1.jsonl` must not be executed.

## Operator copy step (after reviewed API/client deployment)

`nas-restore` is the opposite direction (NAS → R2). The new manual client command
reuses `process_clip(release=False)`, `download_verified`, the atomic media rename,
fsynced deterministic sidecar writer and durable SQLite inventory/sync path.
It never advances the pull cursor or calls release/purge. Existing conflicting
media or sidecars are preserved and fail the copy rather than being quarantined
or overwritten.

`POST /account/connections/redownload` requires the authenticated **connection 13**
NAS pull key, even if another account key has the NAS scope. The server derives
the bucket/key from clip ID and account ownership. It checks the expected
SHA/size, canonical timezone and intersection with the same pinned keep28 window,
refuses purged/retired/reserved/cleanup-held files, HEADs the exact R2 identity,
and signs an exact If-Match GET for 15 minutes. It changes no DB or lifecycle
state, including for released clips. Persistent credentials stay on their current
machines; the NAS uses its existing connection key and the short-lived GET.

Perform the copy between cleanup batches, with no enabled plan/batch and the
cleanup master OFF. Gracefully drain the Jelly runner using its existing STOP
mechanism and wait for `phase=stopped`; never pause an enabled plan or force-kill
the runner to copy/deploy. Phase-1 R2 apply does not require that drain.

Stop the ordinary NAS pull process first, then run the reviewed single-file
client in the **existing connection-13 container configuration**, with its usual
`/clips` and `/state` mounts and existing environment. The exclusive client lock
prevents concurrent local cleanup/stitch/inventory work. Mount checks, the normal
free-space reserve and an explicit transfer cap apply. Before reporting inventory,
the client rechecks the server policy/holds, freshly rehashes the committed file,
binds the durable sidecar, commits SQLite (FULL/WAL), and uploads exact evidence.
A failure leaves media intact and prevents eligibility based on that attempt.

Example in the existing Compose project (stage only reviewed code and the exact
batch manifest under the existing state mount first):

```sh
docker compose stop stoarama-pull
docker compose run --rm --no-deps stoarama-pull \
  python3 /state/r2-retire/stoarama_pull.py copy-manifest \
  --manifest /state/r2-retire/0000.jsonl --expected-sha256 COPY_BATCH_SHA256 \
  --max-bytes 100000000000
docker compose start stoarama-pull
```

Re-run a bounded batch safely after interruption: already-correct files are
rehashed/reported and consume no transfer budget. Continue subsequent batches
only while free-space reserve allows. Generate a **new** R2 retirement plan
after the server shows exact fresh present copies; the copy manifest grants no
R2 deletion authority. Publishing client/API changes is a separate operator step.
