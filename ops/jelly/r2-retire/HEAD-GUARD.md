# Retirement on providers that ignore conditional DELETE

This is an explicit, weaker alternative to provider compare-and-delete. No
production deletion or probe execution is authorized by this implementation.
The 2026-11-12 wrong-ETag production probe observation is **not** a conditional
DELETE proof. Existing manifests, including reviewed v7, remain unchanged.

## Authority and limits

Default NAS-backed apply still requires successful conditional DELETE evidence.
`--provider-unconditional-delete-with-head-guard` instead requires a fresh,
checksummed probe proving a wrong-ETag DELETE succeeded **and** HEAD returned
404. Supported, ambiguous, stale, future, wrong-bucket or wrong-endpoint results
cannot authorize the alternative. The operator-only probe now emits either
kind of evidence, uses one wire attempt, and touches only its newly generated
64-byte `r2-retire-probes/<uuid>.bin`. Do not run it as part of normal tests.
The historical probe run did not emit this new proof format: collect/review a
new result separately; do not convert a narrative into purported probe output.

Every object requires nonempty catalog, manifest and fresh HEAD ETags to agree,
plus exact catalog/manifest size and SHA, and matching HEAD SHA metadata when
present. HEAD LastModified must be strictly older than `--min-object-age`
(default/minimum 168h); absent/future dates veto. LastModified is provider data,
not the catalog's creation or clip time. Objects retain their planning ETag.

**Collated catalog rows currently have no ETag column.** This mode rejects mixed
or collated manifests before any ledger or reservation write, rather than
inventing authority from the listing. Missing/drifted raw catalog ETags also
fail preflight. Use `plan --head-guard-compatible` with the same owner class and
picks to generate a new manifest containing only eligible raw objects whose
catalog/listing ETags agree; review its new totals and digest. This is not an
approval to replace v7 or to backfill catalog ETags from unverified HEADs.

## Writer inventory (source behavior; enforced gates below)

These are all repository R2 write families, including helpers capable of using
arbitrary destination prefixes. Every DB-driven family is covered by the durable
SQL/wire admission gate below.
Ungated credentials are restricted by conservative source prefixes, including
universal authority where confinement is not established. Paths below are relative
to the repository root.

| Writer family | Entry points / exact key rules | Work/capabilities to drain |
| --- | --- | --- |
| `recorder` | `backend/internal/api/server_recording_clips.go` upload intent/presign; `internal/recordingworker/worker.go` PUT. `storageObjectKey(destination.key_prefix, display_path)`; legacy `recordings/<id>/…`, current managed account/folder/time keys. Deterministic replay can overwrite old keys. | All recordings paused/canceled, no pending/leased jobs, uploaders stopped, **consumed as well as pending** signed URLs expired. |
| `capture` | `backend/internal/api/server.go` generic upload-intent presigns for segment/boxed/thumbnail keys; `internal/captureapi/client.go` PUT. | No enabled capture, live capture leases or unexpired upload intents; stop old workers. `capture_jobs` was removed by migration 0058 and is not a guard. |
| `collation` | `backend/internal/collation/runner.go` verified create-only MP4/manifest writes; `cmd/stoaramactl/collation_v2.go` worker/register. `joined/<batch>/objects/<sha>.mp4` and batch/hour manifests. | Workers and registration stopped, in-flight PUT/readback/registration drained. Create-only can recreate a retired key. |
| `joined-publish` | `backend/internal/api/server_recording_joined.go` create-only presigns; joined workers publish `joined/<batch>/…` and staging artifacts. | Claims/publication stopped, outstanding capability TTL elapsed, all uploads complete/stopped. |
| `v85r-stitch` | `clients/nas-pull/v85r/producer/internal/collation/` shares joined content-addressed key rules; `backend/internal/api/server_nas_stitch.go`, `server_recording_native_stitch.go`, and NAS client registration modify catalog/retention evidence. Registration alone is not a PUT. | Producer, stitch and registration jobs stopped; include live-only historical producers. |
| `repair` | `ops/jelly/repair/presign_remote.py`, `repair.py`: PUT only `repaired/<source-key>` (including legacy mutable mode). | Repair job stopped, issued PUT URLs expired. `repaired/` stays outside retirement allowlist. |
| `nas-restore-redownload` | `backend/internal/api/server_nas_restore.go` creates exact-key create-only PUT; `clients/nas-pull/stoarama_pull.py` restore uploader. `server_nas_redownload.go` issues GET/re-download work, not PUT, but can change live evidence. | Stop restore/redownload workers and admissions; drain restore leases and **all** issued capabilities, including completed tasks whose URL still lives. |
| `r2-select` | `ops/jelly/r2-select/server.py`, `coverage.py`, `select.py`: local picks/history/public files and DB reads, **no repository R2 PUT**. | Stop picker during reviewed apply so picks stay pinned. Audit deployed/live-only thumbnail generators separately. |
| `thumbnails` | Capture thumbnail presigns in `backend/internal/api/server.go`, capture API uploader; recorder thumbnail pointers in `server_recording_clips.go`. Picker public thumbnails are local files. | Stop every deployed thumbnail generator; record actual destination prefixes, including any direct R2 writer absent from the repository. |
| `transfer` | `backend/cmd/stoaramactl/recorder_transfer.go`, `recorder_objstore.go`: streamed multipart PUT to destination key; export/automatic delivery in `server_recording_export.go` and `server_recording_clips.go` can enqueue it. Destination prefix is configurable and can overlap `managed/`. | Stop transfer workers and enqueuers; finish or abort multipart uploads, not just upload parts. |
| `frames-inference-survey-import` | `backend/internal/api/server.go` direct frame PUT; `server_imports.go`; `internal/survey/survey.go`; `internal/inferencebox/manager.go`; `cmd/stoaramactl/main.go` backfill/snapshot. Usually `raw/stream/…`, `boxed/…`, survey keys. | Stop schedulers, workers, CLI import/backfill and API write paths; verify deployed prefixes. |
| `exports` | `backend/internal/api/server.go`, `server_recording_export.go`, `server_day_zip.go`: single/multipart ZIP PUT under `exports/…`. | Stop API export admissions/workers, drain multipart completion. |
| `releases-probes` | `backend/scripts/{release,promote}-{relay,nas-pull}.sh`, `relay-release-immutable.sh`, `nas-cleanup/build_frozen_index.py`; NAS upload-probe/space-audit and storage verification API paths; this probe. `relay-releases/`, configurable NAS release prefix, `staging/`, `nas-probes/`, `nas-space-audits/`, `.stoarama-verify/<probe>`, scratch keys. | Stop publishing/probes and outstanding presigns. Mutable latest pointers are writers too. |
| `operator-cli` | `backend/internal/r2/client.go` general PutBytes/PutReader/PutMultipart/presign helpers; `cmd/stoaramactl/collation_v2.go` explicit `put-manifest --key`; AWS CLI/curl/rclone and untracked jobs with bucket credentials. **Universal prefix until proven otherwise.** | Inventory every credential holder/process/machine; stop them, drain presigned URLs and multipart completion. A code search cannot establish that a live credential holder is idle. |

`backend/` is implicit before `internal/` or `cmd/` in the first row. No prefix
in this table grants deletion authority. The existing per-kind key allowlists,
protected `repaired/`, `staging/` and best14 prefixes, owner windows, NAS presence,
restore/reference/shared-key/incident holds, reservations and budgets still veto.

## State-backed writer fence and drain

The former writer-quiescence JSON and its flags have been removed. Revision
strings, evidence text and `multipart_uploads_drained=true` cannot authorize
retirement. The inventory above describes source behavior, not deletion authority.

Migration **0187** adds a durable, global maintenance fence and capability ledger.
Roll out the migration and every API/DB-worker binary first, retire old instances,
then explicitly activate with `r2-retire writer-fence --activate --all-writers-gated`.
This deployment prerequisite is analogous to the delivery fence; it does **not**
assert that work has drained. The migration and ordinary startup install no active
fence. No deployment or activation is performed by this PR.

SQL admissions check the durable row under shared advisory lock 7343226011187.
The R2 client uses the same gate for every single/multipart PUT and every PUT
presign in DB-driven production constructors, including configurable transfer
and storage-verification clients. The shared lock spans the entire upload,
including multipart completion. A wire intent is also committed before network
I/O and completed only after success. Lost DB connections, process crashes and
ambiguous wire errors leave that intent active indefinitely; lock loss, elapsed
time or an operator attestation cannot establish drain. Activation tries the exclusive lock and refuses
while any upload/admission is in flight, without queuing behind it. After
activation, queued/leased jobs must finish or be canceled; they cannot acquire
new PUT capabilities or start uploads. Terminal job transitions remain available.

The SQL drain check requires no active recordings, enabled stream recording,
running/resolving capture runtime, live capture leases, pending/leased recording,
transfer, inference, restore, native/V85r stitch or presentation-probe work, or
unfinished joined batches/days/hours/publication. Consumed upload intents and
completed restores still veto until their PUT URLs expire. Direct collation,
frames/imports/survey, thumbnails and in-memory exports use the common wire gate;
their SQL catalog/registration inserts are also admission-fenced. A queued
in-memory export cannot PUT after activation. R2-select writes local picks only;
apply continues to reload and compare the reviewed picks.

Every new PUT capability is persisted **before** signing, with expiry plus a
two-minute bounded-signing allowance. Activation additionally imposes a mandatory
**7 days + 2 minutes** legacy-capability drain. Plan/review the new raw-only
manifest *after* that wait; the fence is bound to bucket/endpoint rather than a
manifest that would become stale during the wait. Missing migrations, mismatched
fence scope, active queues or live capabilities fail closed. A failed/crashed
apply leaves the fence installed. Explicit `writer-fence --deactivate` is refused
while the apply session lock is held; there is no timed automatic reopening.

`ListMultipartUploads` is queried against the real bucket, requiring **zero
uploads overall**, not just zero listed parts. Any listing error, denied or
unsupported API, returned upload or truncated response vetoes. Querying with
`MaxUploads=1` is sufficient for zero overall: a nonempty/truncated first page
already fails. This runs before apply opens its ledger, after the apply lock,
and twice per item around the final HEAD/live checks. No upload is aborted by
retirement; it is an inventory operation only.

## Ungated credentials and Jelly wrapper

**The unconditional mode currently refuses every nonempty manifest.** Operator
credentials and configurable release/probe tools have bucket-wide writer
authority. Their inventory prefix is the empty prefix, which overlaps every key.
The Go preflight refuses it before any ledger or reservation write. There is no
operator-supplied prefix list, stopped-process claim or evidence digest that can
waive this. Enabling this mode requires a separately reviewed provider-enforced
credential confinement change and a corresponding narrowed code inventory.
Neither stopping tools nor waiting seven days narrows credential authority.
Unbound R2 clients default to read-only. Explicit standalone/ungated synthetic
clients (used by transport fixtures) establish no DB-fence safety; no production
writer constructor selects that mode.

The Jelly repair job is separately confined by source behavior to `repaired/`.
All such keys remain refused by the CLI even if repair is stopped. Use
`python3 ops/jelly/r2-retire/apply.py --stoaramactl /absolute/path/stoaramactl
--manifest FILE --expected-sha256 SHA -- [apply flags]` on Jelly. For overlapping
repair keys the wrapper verifies a valid launchd GUI domain and that
`com.stoarama.repair` is booted out. Loaded-but-idle jobs, permission errors and
unknown launchd state fail closed. The wrapper never stops jobs and never prints
launchd output or credentials. Its stopped-state check never overrides the CLI's
prefix veto. Non-overlapping manifests do not require repair shutdown.

## Residual TOCTOU

**HEAD followed by DELETE is not atomic.** Even with exact identity/age checks,
durable DB admission and real multipart inventory, a direct credential holder,
an overlooked writer, a still-running multipart completion or an operator
using its bucket credentials can replace the object after HEAD or recreate it
between DELETE and confirmation. Post-delete 404 confirms absence at that instant;
it cannot prove which generation was deleted. Repeating HEAD does not eliminate
this race. The explicit flag accepts that residual risk only for a provider
proved unsupported; prefer conditional DELETE whenever available. Run summary,
run-start ledger and per-object guarded observations record residual TOCTOU;
the ledger retains the provider proof digest and observed ETag/LastModified.
Current universal-prefix refusal prevents accepting that residual race at all.
