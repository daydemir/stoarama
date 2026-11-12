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

## Writer inventory at source revision d2d21661

These are all repository R2 write families, including helpers capable of using
arbitrary destination prefixes. The named inventory is required in evidence;
unknown or absent families fail closed. Verify deployed revisions and every
out-of-repository script/credential holder separately. Paths below are relative
to the repository root.

| Evidence family | Entry points / exact key rules | Required drain evidence |
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

## Reviewed writer evidence

`--writer-quiescence-proof FILE --expected-writer-proof-sha256 SHA` is mandatory.
It is an **operator attestation**, not an automatically verified provider fence.
The file binds the exact manifest digest, bucket, endpoint SHA, creation/expiry
(maximum 24 hours), and the commitment to keep every writer stopped through
expiry. Each inventory name must have actual deployed revision, shutdown/drain
command or log evidence, stop time, capability expiry, drain time and affirmative
multipart drain evidence. Do not include credentials, signed URLs or tokens.

Schema (replace every placeholder; no generated default attestation):

```json
{
  "bucket": "<reviewed bucket>",
  "endpoint_sha256": "<configured endpoint digest>",
  "manifest_sha256": "<new reviewed manifest digest>",
  "created_at": "<UTC RFC3339>",
  "expires_at": "<UTC RFC3339 within 24 hours>",
  "keep_stopped_through_expiry": true,
  "active_writer_prefixes": [],
  "writers": {
    "recorder": {
      "stopped_at": "<UTC RFC3339>",
      "capabilities_expired_at": "<UTC RFC3339>",
      "drained_at": "<UTC RFC3339 at/after stop and expiry>",
      "multipart_uploads_drained": true,
      "deployed_revision": "<exact deployed revision>",
      "evidence": "<reviewable shutdown, URL-expiry and multipart evidence>"
    }
  }
}
```

Include **all 14** table families, not just the example recorder entry.
`active_writer_prefixes` must be explicitly present. Any literal prefix overlap
with an item vetoes, including partial path components. Broad credential holders
are a universal writer and must be stopped; a non-overlapping prefix claim is
not a substitute for their shutdown. Rehashing stale evidence does not renew its
age. File modification revokes the approved digest. Evidence and provider proof
are re-read for every object and after the final HEAD/live checks.

Apply independently rejects **any** active recording or pending/leased recorder
job globally, enabled capture, live capture lease, or unexpired target-bucket
upload URL regardless of consumed status. Per-object SHARE locks on recordings,
recording_jobs, streams, capture_session_leases, recording_upload_intents and
upload_intents prevent new database admissions through DELETE and commit. The
existing NAS cleanup/public fences protect restore, reference and publication
changes. This is a maintenance window; those locks can briefly block writers.

The order is preflight -> fences/row locks -> original HEAD/live vetoes ->
durable budget reservation -> transactional receipt/purged marker -> fsynced
intent -> final HEAD identity/LastModified -> every live veto/owner picks ->
guarded observation ledger -> proof freshness/revocation -> one unconditional
DELETE -> HEAD **must** be 404 -> DB commit -> fsynced done ledger. Single-attempt
DELETE never retries a 5xx or lost response. Stop on ambiguity, retain reservation
and intent, and re-run full guards on resume. Existing absent-object recovery,
per-run cap, 5 TB owner daily cap, unresolved-intent charging and receipts remain.

## Residual TOCTOU

**HEAD followed by DELETE is not atomic.** Even with exact identity/age checks,
DB admission locks and reviewed shutdown evidence, a direct credential holder,
an overlooked writer, a still-running multipart completion or an operator
breaking the stop commitment can replace the object after HEAD or recreate it
between DELETE and confirmation. Post-delete 404 confirms absence at that instant;
it cannot prove which generation was deleted. Repeating HEAD does not eliminate
this race. The explicit flag accepts that residual risk only for a provider
proved unsupported; prefer conditional DELETE whenever available. Run summary,
run-start ledger and per-object guarded observations record residual TOCTOU;
the ledger retains provider/writer proof digests and observed ETag/LastModified.
