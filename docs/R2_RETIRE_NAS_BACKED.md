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

- `phase1.jsonl`: outside both the file and full source-hull windows, no
  non-missing NAS inventory/unmatched evidence or output ACK, non-`all` pick,
  no cleanup reservation/hold, pipeline reference, restore, identity uncertainty
  or grace-period conflict. This is the first operator execution set.
- `manifest.jsonl` / `now`: phase 1 plus kept-window `none` footage whose **same
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
The existing fsynced intent/done ledger, DB receipts and byte budgets remain.

Cloudflare's [compatibility table](https://developers.cloudflare.com/r2/api/s3/api/)
and [conditional-write documentation](https://developers.cloudflare.com/r2/api/s3/conditional-operations/)
do not document conditional `DeleteObject`. NAS-backed apply therefore also
requires a checksummed proof from the **operator-only**
`ops/jelly/r2-retire/verify_conditional_delete.py` probe, no older than 24 hours.
It creates one random 64-byte scratch key, requires a wrong ETag delete to return
`PreconditionFailed` with identical bytes still readable, then confirms a matching
ETag delete. If unsupported or ignored, it writes no proof and apply stays
blocked. Never remove the precondition or substitute an unconditional delete.
The implementation agent does not run this probe or any DELETE.

## Operator phase 1

On Jelly, use the reviewed binary and plan paths in the delivered `OPERATOR.md`.
Load the existing credentials locally; do not copy environment files:

```sh
set -a
. ~/Build/stoarama/local/recording-supervisor.env
. ~/Build/stoarama/local/render.env
set +a
unset PGOPTIONS
# Apply the reviewed migrations through the normal migration runner first.
stoaramactl migrate up --dir /path/to/reviewed/infra/sql/migrations
stoaramactl r2-retire apply \
  --manifest ~/r2-retire-plans/RUN/phase1.jsonl \
  --expected-sha256 PHASE1_SHA256 --max-bytes PHASE1_BYTES \
  --conditional-delete-proof ~/r2-retire-plans/RUN/delete-proof.json \
  --expected-delete-proof-sha256 DELETE_PROOF_SHA256 \
  --budget-24h-bytes 5000000000000 --rate 10 \
  --ledger ~/r2-retire-plans/RUN/phase1-receipts.jsonl --json
```

The rolling 5 TB cap can require subsequent runs of the same approved manifest.
A new NAS presence or hold stops the run by default. Review and re-plan; do not
relax the rules. Manifest maximum age remains seven days (default three).
A versioning/lifecycle read denied by R2 must be reported as **unverified**;
logical deleted bytes are not a verified billing reduction until bucket retention
settings are known. No lifecycle rule or versioned object deletion is performed.

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
