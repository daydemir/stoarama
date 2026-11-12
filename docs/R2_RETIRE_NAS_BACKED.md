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

## Listing proof and owner-authorized recovery hold releases

The saved owner picks supersede the old "R2 copies stay" rule for explicitly
selected non-`all` streams outside keep28. This does not authorize deleting the
last copy of kept footage. Incident raw sources from the 28 uncertain receipts
in plans 1390/1391 are pinned by identity in migration 0184, independently of
subsequent item state or plan admission. Other uncertain receipts, including
`media_deleted_sidecar_failed`, never confer release authority.

Migration 0183 adds a separate hash-free namespace protocol. The existing
client's normal server-directed self-update mechanism should publish these
bytes with **`NAS_VERSION=listing-fence-v1`**, after API rollout; the source keeps
its `development` placeholder for the release script. The cleanup execution
allowlist explicitly includes the release and retains `cleanup-refusals-v1`,
`listing-proof-v1`, and all reviewed predecessors. The new release includes
certified cleanup refusals and receipt replay together with listing proof and
the delivery fence. No Compose/container environment
change or manual NAS access is needed. The migration enables listing only for
existing connection 13/account 47; the heartbeat's `listing_enabled` switch and
`connections.nas_listing_enabled` remain server-controlled.

The server disarms the listing job after a successful generation, preserving
the proof and avoiding recurring namespace/database work. Re-enable
`connections.nas_listing_enabled` server-side when another fresh proof is
needed; completed-marker retries remain safe. No NAS container change is needed.

The independent worker scans every top-level directory and root file, including
`joined/`, `_V85r_keep28` trees and hidden directories. It reads directory
entries and file stat metadata only, never media bytes. A separate SQLite
checkpoint survives process restarts; uploads are keyset-paged in batches of
200 with a small background yield. Every directory's device/inode/mtime/ctime
must remain stable around its enumeration. The completed local snapshot is
immutable while uploading; later managed writes are excluded by the server's
generation-start watermark. The clips mount's device/inode must remain stable
through upload and resume. Missing mounts,
I/O failures, symlinks and concurrent membership changes prevent completion.
An interrupted upload replays identical pages; a changed namespace restarts
under a new generation. Partial generations only supply presence vetoes.

The API assigns the start timestamp before walking begins. It validates page
sequence/replay digests, unique canonical paths, and per-folder file/byte
counts. It marks a generation complete only when **all** declared top-level
folders (and the root-files folder) completed with consistent stored counts.
Listing-derived absence requires a settled delivery-retirement fence covering
that object and a complete generation **started within 24 hours and strictly
after settlement**. Release times, cursor watermarks and delivery ACKs alone
never establish listing absence. Effective cleanup deletion receipts remain
an independent positive evidence class.
Any matching catalog path or known inventory alias (including keep28 paths)
in that generation or a later partial generation vetoes
retirement regardless of file size. A complete newer namespace proof can
supersede older hash inventory/ACK observations; observations at/after the proof
or from another connection continue to veto. A stale exact hash observation
cannot authorize kept footage after a newer listing reports its path absent or
a different size/mtime/ctime/inode/device identity.
Evidence publication and cursor advances share the retirement transaction's
public fence; apply rereads freshness, writer time, presence and protections.

Hold release is a separate, operator-only CLI. Dry-run needs no new schema and
uses a repeatable-read, read-only transaction:

```sh
stoaramactl r2-retire release-holds --picks "$HOME/stoarama-r2-select/r2-picks.json" \
  --out /new/path/hold-release.json
```

The manifest binds the exact picks digest, pinned keep28 policy/boundaries,
plan/item identity, recovery evidence and accepted deletion receipt. Only
`deleted` + `delete_ever_issued` + `r2_sha_verified` or exact-identity
`r2_etag_bound` recovery can qualify. Missing/unknown picks, `all`, changed
windows/timezones, kept or unknown source timing and incident sources remain
held. Existing source/reference protections still independently govern R2
retirement. Review the exact file and digest, then, **only as an explicitly
authorized operator action after migration 0184**, release with:

```sh
stoaramactl r2-retire release-holds --apply --picks "$HOME/stoarama-r2-select/r2-picks.json" \
  --manifest /new/path/hold-release.json --expected-sha256 SHA256 \
  --reason 'owner saved non-all picks; outside keep28'
```

Apply takes the cleanup admission/public fences and item locks, revalidates live
identity/receipt/evidence and rereads the picks before committing append-only
authority records. Every action logs its digest and reason. It never deletes
NAS or R2 objects. Original cleanup reservations stay intact; a separate R2
reservation view and source-hold predicate recognize valid explicit releases.
Unreleased siblings, missing item rows and incident sources stay held. Changed
receipt/recovery state invalidates release effectiveness. Receipt evidence uses
the immutable permit start (or the earlier immutable plan creation time for
legacy rows), so replaying a receipt cannot hide later NAS presence. Generate a **new**
normal retirement manifest after release; its existing conditional-delete
capability proof, reservation ledger and byte budgets are still mandatory.

Planning may include a release preview:

```sh
stoaramactl r2-retire plan --nas-backed --connection-id 13 --sample 0 \
  --picks "$HOME/stoarama-r2-select/r2-picks.json" --out /new/retirement/dir \
  --hold-release-preview /new/path/hold-release.json \
  --expected-hold-release-sha256 SHA256
```

`after-hold-release.jsonl` estimates additional live-key eligibility after that
exact release. The fence-and-listing projection below is the additional
conditional upper bound if a future fresh complete listing started after
fence settlement confirms absence. These forecasts carry
`phase=blocked` and a projection-only header that `apply` rejects. Actual
`phase1.jsonl` remains governed by production evidence. The agent ran only
read-only planning; no production migrations, releases, copies, deletes, client
updates or deployments were performed.

## Connection 13 delivery-retirement fence

Before applying migration 0185 to a populated database, a separately authorized
operator must run this standalone statement **outside a transaction**:

```sql
CREATE INDEX CONCURRENTLY nas_delivery_raw_writer_path
  ON recording_clips(display_path) WHERE display_path<>'';
```

Following migration 0149, migration 0185 refuses to build the missing index when
`pg_relation_size(recording_clips)` exceeds 128 MiB. Small/fresh databases build
it transactionally. An existing index is accepted only if it is valid, ready,
nonunique, and exactly matches the btree key, default opclass/collation/order,
no included columns, and predicate above. A failed concurrent build can leave
an invalid index: inspect and repair it before retrying; migration 0185 rejects
invalid or mismatched indexes rather than silently accepting their names.
This implementation task does not execute the operator statement in production.

Migration **0185 is additive and does not activate the fence**. Activation is
irreversible and separately authorized; this implementation task performs no
production transitions. Deploy the new admission middleware to **every API
instance**, retire all old instances and requests, then use:

```sh
stoaramactl r2-retire delivery-fence --activate --all-api-instances-gated
```

This records the effective timestamp, exact policy bytes and SHA
`060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811`, and immutable
per-stream UTC windows. Raw feeds/downloads, collated feeds/downloads, joined
feeds/downloads, and NAS restore lease/result paths refuse outside-window or
unknown media identities. Kept-window delivery and the reviewed kept-window
redownload remain available. The connection key is confined to the existing
pull route allowlist even if its key scopes change; browser/global/producer
capability routes cannot bypass the fence. NAS stitch tool downloads contain
pinned ffmpeg/ffprobe executables, not recording media. Other stitch/verify/
benchmark endpoints do not issue recording download capabilities.

Admission and activation serialize through one advisory lock. Before activation,
capability responses require a committed download lease covering seven days
(the maximum S3 URL lifetime), plus the two-minute bounded API request. Existing
untracked legacy URLs require at least seven days after activation. Settlement
refuses while either barrier or any recorded lease remains live. Expiry does
**not** stop an already-running GET: after all capabilities expire, stop/restart
the NAS download workers, wait for every transfer to finish or terminate, and
record the concrete drain evidence with the operator CLI:

```sh
stoaramactl r2-retire delivery-fence \
  --settle-drain-acknowledgment 'exact NAS worker shutdown/wait evidence and time'
```

Do not make that acknowledgment while a transfer can still write a media file.
It is a durable operator attestation, never an automatic inference from expiry.
Fence, windows, leases and settlement records are immutable. A fresh complete
listing must **start strictly after settlement**; a listing that finishes after
settlement but started earlier provides no fence authority. The listing must
include each candidate's top-level folder, `joined/` where applicable, and known
inventory alias folders. Later partial listings still veto presence. A kept or
unknown writer sharing a path prevents fence-based absence. All existing R2
retirement vetoes, recovery holds, incident identities, joined references, owner
`all` picks, shared keys and kept-window rules remain independent checks.

Planning with `--hold-release-preview` now also writes
`after-fence-and-listing-upper-bound.jsonl`. This is the conditional upper bound
after fence settlement and an adequately scoped fresh listing confirming
absence, regardless of release/cursor/ACK history. The obsolete listing-only
watermark forecast is no longer emitted.
It is additional to `after-hold-release.jsonl` and actual eligibility. Its
`phase=blocked` projection header cannot authorize deletion. Neither activation
nor any fabricated listing/settlement is needed to produce these read-only
forecasts on the current production schema.

## Owner-accepted outside-window retirement

`--class owner_accepted_outside_window` is explicit owner disposal authority,
separate from both absence-proof retirement and kept-window NAS copying. It
implies the NAS-backed transport safeguards, but never selects kept-window
footage. Valid saved non-`all` picks must still match the exact pinned policy.
No-history and missing inventory may qualify without positive NAS-absence proof.
Present/unknown inventory (including other connections, aliases, matching
content, and unmatched paths) remains a veto; historical ACK alone is not current
inventory. This class never runs the evidence logic that supersedes old presence.

All existing prefix, identity, grace, restore, joined snapshot, artifact and
shared-key protections remain. Additionally, every collated source must be
outside its own stream's pinned window under a valid non-`all` pick. Raw sources
referenced by a kept/`all`/unknown collated output are protected. Uncatalogued
objects and protected best14 outputs remain blocked. `nas_stitch_outputs` are
NAS-only evidence with no R2 object key; they are not R2 deletion candidates.

Migration **0186** is required before either owner-class hold release or apply;
planning works read-only without it. `release-holds --class
owner_accepted_outside_window` previews only exact deleted, recovery-verified
receipts from **retired** plans. Active/dry-run/paused plans, uncertain receipts,
unmapped source holds, reservations and all 28 incident identities (including
collated descendants) remain protected. The existing receipt guard validates
identity and receipt/recovery hashes. Owner release records are excluded from
the legacy effective-release view. Only an owner-class durable reservation can
consume them, and only a `purged_at` transition may use that exception; other
source mutations and classes retain their holds.

Review and commit the exact hold-release manifest separately, before retirement.
This makes authority visible to the independent reservation connection. A
`--hold-release-preview` with the same class produces a **blocked, non-executable**
`after-hold-release.jsonl` projection. It preserves presence, references and all
other holds. Regenerate an ordinary executable manifest after the release; never
apply the projection. The current ordinary manifest can retire independent,
unheld objects without releasing any receipts.

```sh
stoaramactl r2-retire release-holds --class owner_accepted_outside_window \
  --picks "$HOME/stoarama-r2-select/r2-picks.json" --out hold-release.json
stoaramactl r2-retire plan --class owner_accepted_outside_window \
  --picks "$HOME/stoarama-r2-select/r2-picks.json" --connection-id 13 --out plan
```

Operator execution, after reviewing the manifest digest and supplying a fresh
approved conditional-delete proof:

```sh
stoaramactl r2-retire apply --manifest plan/manifest.jsonl \
  --expected-sha256 REVIEWED_MANIFEST_SHA256 --max-bytes 5000000000000 \
  --budget-24h-bytes 5000000000000 \
  --conditional-delete-proof REVIEWED_PROOF_PATH \
  --expected-delete-proof-sha256 REVIEWED_PROOF_SHA256
```

The owner class refuses a rolling budget above 5 decimal TB. Each deletion still
reselects live inventory, cleanup holds, incident identities, own/source windows,
references, aliases and unchanged picks under the admission/public fences. It
retains conditional If-Match, post-delete HEAD, durable reservations, fsynced
ledger, and unresolved-intent budget charging. No production action is needed
to build or inspect the dry-run artifacts.
