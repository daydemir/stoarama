# NAS-local V85r pilot (ships disabled)

This worker runs **inside `stoarama_pull.py`**, in the existing NAS container.
The single-file R2 client release already contains it; no second service or
scheduler is installed. This change does not publish/promote a client release,
deploy the API, change production data, or enable a scope.

## Scope and policy

Owner decision (2026-11-10): ship the **rebuilt V85 pipeline as V85r**. Every
new task, result, output and repair proof uses policy `v85r`, revision
`v85r-rebuilt-1`; historical frozen witnesses retain their original statuses.
The versioned output folder is `_V85r_keep28`. No production scope is seeded.

`clients/nas-pull/v85r/manifest.json` pins every recovered classifier file,
rebuilt `cached_actual_span_features.py`, identity-only veto projection, recovered SDK source and
rebuilt Linux amd64 evaluator by SHA-256. `build.py` uses Go 1.26.5, CGO off,
GOAMD64=v1, trimpath and a stripped deterministic binary, then embeds the
complete compressed package in the self-updating single-file client. The
client pins the compressed bundle and manifest, checks exact member membership,
and hashes every installed member before use. It never executes recovered code
from an arbitrary `/tmp` path; historical imports are mapped inside an isolated
child. Rebuild with `python3 clients/nas-pull/v85r/build.py` and review source,
manifest, client and server-pin changes together. No artifact release is needed.

Evidence: 49,855/49,855 replayed decisions exact; 453/453 cached contexts exact;
853/853 fresh nominations agree. Two fresh statuses and decoder-error fields
differed. This is **not complete reproduction** of all 73,843 October rows:
23,988 original feature rows remain missing, and the fresh evaluator is rebuilt
from a later recovered SDK. The owner explicitly chose this distinct policy.

The server imports only the exact October JSONL (`5b2765ecb4f6483c8e48b5a6c958c7c0d8082764d4da57901626a22569c6346b`,
73,843 rows / 52,462 nominations; gzip pin `dbac9a28d62d35138163eb9fec7da6b5481b04a167c2522dd1dcf665f25b0f46`).
The body and source71/human12/visual6 vetoes are now recovered. The exact
export includes 5,283 non-nominated rows without diagnostic hashes; those rows
remain importable as frozen denials/unknowns. A nominated row always needs a
valid diagnostic hash. Full server import and independent client indexing of
all 73,843 rows were exercised against the recovered bytes. The client still
independently hashes/indexes the full export. Frozen lookup requires ordered
clip ID + SHA-256 + size. Current metadata and actual bytes feed the recovered
20-second feature producer; short-span replacement requires freshly decoded,
authenticated source contexts. No CLOCK, trimming, spatial-positive promotion,
threshold port or assumed continuity is added.

A join requires a fresh V85r nomination with the recognized unambiguous final
status. An exact frozen witness takes precedence only when the current decision
and status agree. Any status or nomination difference, missing input, unknown
status, measurement failure or decoder error is **HOLD**. Zero extra decoder
errors is deliberately stricter than October tolerance. Negative decisions split
runs. A HOLD also marks both raw endpoints ineligible for retirement, even if
separate verified singleton outputs include them. Server and client independently
enforce that gate. New exact inputs can receive a fresh V85r nomination; they
are never represented as October V85. Known-source and human vetoes apply by
whole hash, visual vetoes by ordered hashes/sizes (including ID aliases).

### FFmpeg pin and search

October recorded `N-126826-gc0e8b139fd-20260924`, ffmpeg SHA
`2388de8e05ad71c913ee1a9c4504a1aa5de09ac100a1de92858dd9ec08e9f98b` and
ffprobe SHA `9b2bac862606a48f5bc36091d38e5334cf241538a6f4cc47c60f68e7973a365c`.
Both exact linux x86_64 executables were recovered from R2's historical custody
prefix `experimental/v85-retired-WORK-custody/20261013/<sha256>.bin`:
ffmpeg **174,165,384 bytes**, ffprobe **173,923,656 bytes**. Whole readback hashes
match October's pins. The search covered recovered evidence, Mooney archives,
session logs, releases and read-only R2 listings through Jelly's existing
credentials. Historical PUT-UNKNOWN receipts supplied the final object keys.
No credentials moved and no R2 objects were changed.

V85r downloads these exact binary artifacts through a scoped, default-off
connection-13 API capability, with exact HEAD identity/If-Match and whole SHA/size
verification, fsync and atomic installation. It never substitutes PATH tools or
the older NAS benchmark build. A missing artifact or wrong hash is HOLD. The
existing benchmark/other media lanes retain their original tool pins.

Every output/repair proof records the policy, package manifest hash, both tool
versions and binary hashes. Tool hash mismatch prevents measurement/build.
The server checks that provenance before registration or retirement.

The embedded policy selects all recordings for the approved stream. Ready hour
units are bounded to 200 clips and 2.5 GB. They include only whole clips inside
the exact scope/keep UTC bounds; boundary-crossing clips retain raw. UTC hour
assembly names avoid DST fold collisions. The worker does not fill capture gaps
or promise one 28-day continuous movie. Missing NAS identities defer their hour
without admitting incomplete source coverage. Other ready units may proceed.

## Verification and retirement

For each supported source/output, software verification checks:

1. Whole input SHA/size and a fresh exact physical raw/sidecar proof.
2. H.264 codec/extradata/timebase identity and positively **no source audio**.
   Audio-bearing, unknown-layout and unsupported packet/frame cases retain raw.
3. Strict source/output decode through EOF with `-xerror -err_detect explode`;
   cryptographic decoded frame hashes/counts/raw presentation PTS conservation.
4. Every video packet payload/size/duration/flags in source order, constant
   source PTS/DTS translations, increasing DTS and exact rational source spans.
   Hidden edit-list packet inventories must also agree. Packet/frame mapping
   must be one-to-one; no count-only inference or Adler32 identity is accepted.
5. A second, seeked strict decode around each joined seam.
6. File fsync, atomic create-only publication, directory fsync, then reopen and
   whole SHA readback. A restart adopts only byte-identical existing outputs.

Media lives in `<raw-root-folder>_V85r_keep28/<recording>/<UTC-hour>/` next to the
raw tree. Raw inventory skips this reserved versioned tree; disk audits still
count it. Packet/frame scratch is bounded to one assembly and removed after the
child exits. Outputs, provenance and source media are preserved. Directory file
descriptors, `O_NOFOLLOW`, same-device checks and Linux parent `/proc/.../fd`
paths confine child reads/writes. Symlinks and conflicting outputs stop work.

Server output rows and proofs are append-only. The worker writes durable local
registration receipts after the server accepts its result. A second readiness
ACK records `completed_no_retirement` and releases the lane. A lost ACK is
reconciled from the SQLite FULL/WAL journal before new work.

NAS-local stitches retain their raw inputs. The API and client refuse
legacy stitch deletion permits, including legacy registered results.
Migration 0214 requires old stitch cleanup plans to be disabled and their
issued permits to have expired before opening the local stitch lane.
Preserve existing SQLite journals, outputs, ledgers and plans. Ordinary NAS
cleanup and R2 retirement retain their separate authority.

## Decoded frame verification

The ordinary audit still strictly decodes each source and each assembled
output independently through EOF. The [original V85r audit](https://github.com/daydemir/stoarama/pull/446)
uses FFmpeg's rawvideo encoder and the framehash muxer to compute SHA-256
for each decoded packet. For the qualified H.264 pixel formats, the same
decode, stream mapping, frame timing and rawvideo encoding feed a
synchronous FFmpeg tee. One branch emits frame metadata before the other
writes the exact packet bytes. There is no pixel-format override. Python
hashes the complete payload of each packet using SHA-256 and writes the
original stream, DTS, PTS, duration and byte count beside the digest.

The verifier requires clean EOF on both pipes, successful FFmpeg exit and
an active build deadline. An incomplete ledger aborts the build before
publication. After a source decode exits unsuccessfully with intact pipe
framing, the original verifier must independently reject the source before
the worker can retain it or try repair. The source and output ledger comparison,
packet inventory, independently seeked seam decodes and durable whole-file
readback still run. Proof builds, their readback checks, repairs, and pixel
formats outside the parity matrix use the original framehash command.

## Dedicated Durham credential

After the dedicated-key deployment and explicit owner approval, an operator
session can issue a key with `POST /api/v1/admin/nas-stitch/key`. The response
shows the token once. Save it through the approved secret workflow. It grants
only the ordinary stitch lease, checkpoint, result and readiness POSTs.
Provision the pinned tools and corpus offline. The existing admin key-ID
revocation route revokes this key independently of connection 13. Older API
binaries cannot authenticate the dedicated key's stored digest.

The operator CLI can issue the same fixed account-47 credential with
`stoaramactl nas-stitch mint-durham-key` using the operator database connection.
Supply the owner authorization reference and an inherited writable pipe from
the approved 1Password sink. The CLI emits the bearer only through that pipe.
The browser and CLI share the one-active-key guard and audit transaction.
Pause both browser and CLI issuance whenever any serving controller lacks
the shared guard, including after a rollback. Resume once every serving
controller runs the guard.
If issuance or vault acknowledgement is uncertain, reconcile the recorded
key ID and vault item before any further issuance attempt.

A result awaiting its durable receipt keeps the shared stitch lane occupied.
If a dedicated key is revoked or expires in that window, the operator status
lists outstanding registrations first and shows each exact plan digest, owner
key ID and abandonment marker. An authorized operator
can record abandonment with `POST /api/v1/admin/nas-stitch/{id}/abandon-registration`
and the exact `plan_sha256` and `reason`. This preserves the output and frozen
result, records who released the lane, and leaves the abandoned hour without a
readiness receipt. Older binaries ignore the marker and can block the lane
again after a rollback.

## Resource controls and restart

One lane runs at **nice 19 / ionice idle**, including Python hashing; missing
Linux priority tools disables the lane. Every FFmpeg decoder/filter uses one
thread. Before a unit and between jobs, require >=384 MiB MemAvailable and cgroup
headroom, load <=1.5 times effective CPUs, a mounted NAS root, and the larger of
the normal pull reserve or 1 GiB, plus 2.2 times source bytes before admission.
The child watchdog checks resources every five seconds and kills its process
group on critical pressure, cancellation, timeout or self-update. Raw remains.
The two-hour server lease is fenced by a random token; an expired attempt can
only resume the same immutable plan. SQLite retains local verified results and
reconciles registration instead of deleting based on guessed completion.

The self-updater sets the existing inventory stop event, which also cancels this
lane. It waits for the lane's active call/children before replacement. Existing
pull, inventory and cleanup behavior does not enable V85r when its switches are
off. Stitch progress/errors ride the heartbeat (`connections.nas_stitch_status`)
and the existing redacted client-log upload (V85/stitch topics included).

The authenticated heartbeat sends `stitch_enabled` to connection 13 from
`NAS_STITCH_ENABLED`. It sends `stitch_deletion_enabled=false` for every client.
The client starts OFF and re-evaluates stitching on every heartbeat, normally
every 30 seconds. `STOARAMA_V85_STITCH_ENABLED=false` vetoes stitching locally.
Any response other than an authenticated explicit enable revokes authority.

Server stitching OFF blocks new leases and cancels the current native child
through the same watchdog as checkpoint `continue:false`. A later ON permits a
new job with a fresh lease.

## Resource admission overrides

The resource gate is checked before every lease and again by the 5s mid-job
refresh. Two container variables adjust it without editing the pinned client:

| Variable | Default | Meaning |
|---|---|---|
| `STOARAMA_STITCH_MIN_MEMORY_BYTES` | `402653184` (384 MiB) | Minimum free memory, integer bytes |
| `STOARAMA_STITCH_MAX_LOAD_PER_CPU` | `1.5` | Maximum load1 per usable CPU, float |

Defaults are exactly the previously hard-coded values, so an unset environment
behaves as before. A non-numeric, non-finite or non-positive value fails the
process at startup with a clear error; there is no silent fallback to the
default. Both variables govern the pre-lease check and the refresh, so a running
job is re-evaluated under the configured limits. A resource hold returns the
exact lease through the checkpoint endpoint before retrying. A failed return
leaves the original lease expiry in force.

## One-stream pilot procedure (operator action after review)

No commands below were run against production for this PR.

1. Use the recovered exact gzip export, verify both compressed/uncompressed
   pins and the manifest census, and verify the bundled source/hash alias vetoes. Do not bypass hash validation.
   Replay/admit a representative production unit with the NAS's pinned FFmpeg
   before scaling the pilot. The synthetic corpus adapter is
   test-only and cannot admit production media.
2. Deploy migration/API and a reviewed client candidate through the existing
   release process. Verify `nice` and `ionice` exist inside the NAS container.
   Use `NAS_VERSION=v85r-server-switch-v1`. Set server
   `NAS_STITCH_ENABLED=true`; leave `NAS_STITCH_DELETION_ENABLED=false`.
   Unset container switches follow the server automatically; no container env
   edit is required. A previously explicit local `false` remains a kill switch
   and cannot be remotely overridden. Verify the next heartbeat enables the lane.
   Leave the existing cleanup master as otherwise required by normal cleanup.
3. With the operator's existing service authentication, import the exact file:
   `stoaramactl nas-stitch import-corpus --input approved85-decisions.jsonl`.
   Choose one stream and one ready hour entirely inside its embedded window.
   Approve a bounded scope (all dates RFC3339):

   ```sh
   stoaramactl nas-stitch approve --stream-id STREAM \
     --start START --end END --approved-until EXPIRY \
     --reason 'Deniz approval reference'
   ```

   Approvals expire within 180 days. Scope bounds are immutable.
4. Check the output hash, source dispositions, recovered-prefix provenance and
   `completed_no_retirement` in `stoaramactl nas-stitch status` and the client logs.
5. Stop stitching with server `NAS_STITCH_ENABLED=false` or
   `stoaramactl nas-stitch pause --scope-id ID`. The heartbeat and checkpoint
   revoke running work when stitching is disabled.

## Limits and estimate

The NAS benchmark (production `nas_benchmarks` id 2) measured 165 seconds for
one recording hour / 60 clips / 237 MB at nice 15: **21.8 recording h/wall h**.
That extrapolates 17,792 remaining recording hours to **34 days** continuously.
This worker adds strict source decode/pixel/PTS and hidden-inventory audits,
uses one decoder thread, and nice19/idle yields to NAS activity. Budget roughly
**5–15 recording h/wall h (49–148 days)** until a representative pilot measures
strict verified throughput. This is an estimate, not a new NAS benchmark.

Deferred work: full-corpus reproduction of missing original features; review
new current-negative bundle revisions; audio-bearing
native runs; unsupported B-frame/edit-list layouts; and automatic import/reuse
of the ~1,103 complete historical V85 hours. Existing R2 V85 presence/ETag and
old publication receipts are not fresh NAS-native verification or deletion
permission. This worker reuses its own exact verified local outputs after a
restart; it does not silently relabel collation-v2, best14 or historic V85.
Local repairs support the existing bounded mdat→Annex B/VUI method, including a
clean prefix before a gap; unsupported FPS/corruption patterns retain raw.
Repaired prefixes are labeled with original/repaired hashes, VUI FPS, frame
count and synthesized timing. They remain outside V85r join admission and **never**
authorize original deletion. Imported R2 repair-ledger copies and subsequent
repair-qualified joins require separate exact provenance/admission work.

## Cleanup integration and review conflicts

Main's #449 source freezes and freeze-generation verdict cache, #451 serial
execution lane / 15-second observation budget, #452 writer-drain retry and
unused execution-lease return, and #450 operations documentation move are
integrated. #453's Jelly cleanup runner is operationally independent; this
worker remains NAS-local. Never build it on Jelly.

No writer fence spans a stitch build. All scratch/output writes stay inside the
new versioned sibling tree; raw inputs are read-only, directory/file descriptors
are pinned, and source/output proofs are freshly read back. Cleanup can acquire
its execution lane even during a slow native tool or source hash. The local
execution-owner signal cancels the native child on its watchdog checkpoint.
Every five seconds the child also checks its exact task/token/plan against the
server; a source freeze on that stream, expired lease, inactive scope/switch,
transport error or missing response stops it. A source retired during the
bounded checkpoint gap cannot be used for a successful fresh source proof;
building never unlinks or writes through a source descriptor. New leases pause
conservatively while any cleanup source freeze exists. Registration replay is
metadata-only and retains already verified outputs.

The explicit future client release is **`NAS_VERSION=v85r-corpus-ua-v1`** (predecessor `v85r-server-switch-v1`),
allowlisted alongside reviewed predecessors, including `v85r-fence-progress-v1`
and `joined-marker-recovery-v1`.
Build that release from merged `main` with this explicit label. Local stitched
media, repairs, proofs and registration receipts use the same alias quarantine,
verified private copy, atomic canonical replacement and crash-safe ledger as
joined delivery. Startup reporting replays their shared quarantine ledger.
Keep both server stitch/deletion switches
off until the separately approved pilot. This work does not stage/promote a
release, mutate production, merge, or deploy.

Regression coverage includes real PostgreSQL freeze/cached-generation changes,
source-write refusal, checkpoint identity/stream/switch behavior, shared reviewed
session gates, retirement refusal on replay, and cancellation of a live native child after
a lane acquisition.
The stitch migration remains **0179_nas_v85r_stitch.sql**, with main's cleanup
freeze migrations 0180/0181. Joined lifecycle fixtures apply them in order.

## Durham R2 handoff

Durham works only on reviewed whole-hour plans backed by R2 and exact raw
custody on NAS connection 13. The read-only planner emits a private manifest
and prints its SHA-256. Review that manifest before import. Import with the
queue paused; imports serialize with each other, not with R2 retirement. Each
input holds its own key lock until commit, so one import takes at most 1,000
inputs; choose `--max-hours` to fit. Older importers and direct SQL bypass
this cap, so roll out the capped importer before importing again. It recomputes the frozen
plan, verifies every catalog identity, and atomically adds
`public.nas_stitch_r2_input_holds` and the corresponding hour rows.

```sh
stoaramactl nas-stitch plan-durham --scope-ids IDS --max-hours N --out HOLD.json
stoaramactl nas-stitch import-durham --input HOLD.json --sha256 SHA
stoaramactl nas-stitch control-durham --enable --max-staging-bytes BYTES
stoaramactl nas-stitch control-durham --pause
```

Four Durham workers claim distinct hours, preferring distinct R2 bytes that
the hour can release. The worker connects directly to the Stoarama API and
the configured R2 origin over TLS. It receives exact GET and multipart-part
capabilities for its own lease. Stoarama creates and completes uploads only
for the unique `stitch-staging/<generation>/<sha256>.mp4` key. The private
attempt and part journals survive a worker restart within the lease.
Ambiguous multipart start or completion keeps the ledger and stage budget
reserved for controller reconciliation. Keep unknown multipart uploads gated.

Durham uses the native Linux ARM64 profile in
`clients/nas-pull/v85r/durham-arm64.json`. The previous production installer
selected the October amd64 profile and ran its evaluator and media tools
through QEMU. The offline ARM64 benchmark used the Debian 7.1.5 media tools and
the evaluator built from the same pinned Go sources. The ARM64 bundle carries
the same corpus and classifier, with both evaluator calls aimed at the native
binary. The worker verifies the bundle, executable machine types and tool
SHA-256 values before requesting a lease. The bundle also pins the resolved
ARM64 loader and shared library hashes. The worker verifies the loaded
dependency set before startup and before building an hour. A package or
library update that changes the set stops the worker until the new build
has been reviewed and replayed. The API binds the profile to the exact
lease attempt. The worker checks that identity on lease, at resume and
before processing. PostgreSQL requires matching output provenance when
issuing a certificate, including when the API is rolled back.

Build the native bundle with Go 1.26.5 and the exact signed-index Debian
`ffmpeg_7.1.5-0+deb13u1_arm64.deb`:

```sh
python3 -B clients/nas-pull/v85r/build_durham.py \
  --ffmpeg-deb "$DEB" --out "$RELEASE/durham-arm64.tar.gz"
```

Merge using `[skip render]`. The R2 controller owns API deployment and
migration 0222. Pause the Durham queue and turn off API stitching before
upgrading or rolling back. Record the serving API revision and migration set
and reconcile expired attempts, stage grants and unresolved multipart
operations before opening a window. Drain leased NAS output copies before
rollback. An issued copy lease can still renew and certify after the API stitch
switch is off. Preserve legacy staged results and their
input holds.

On the existing Debian ARM64 guest, install the signed
`ffmpeg=7:7.1.5-0+deb13u1` package with the reviewed dependencies. Install the
reviewed `stoarama_pull.py`, `durham_stitch.py`, and systemd unit. Put the
profile JSON and generated bundle in the root-owned
`/opt/stoarama-stitch/v85r/`. Keep the private systemd credential, service user,
direct API and R2 origins, and macOS boot cron. The unit clears QEMU and
dynamic-loader overrides and reserves 512 MiB of available guest memory.
Check startup and pin verification with the queue off. Open work only after
the matching API and schema are serving. The NAS-local client and proof
replay use the October profile.

The API grants staged output leases to connection 13 on
`r2-copy-v7-certified-stitch` while both `NAS_STITCH_ENABLED` and
`NAS_R2_COPY_ENABLED` are true. The client leaves the legacy opt-in out of its
request, so a rolled-back API can continue raw copies without leasing staged
outputs. `stoaramactl nas-stitch status` reports the heartbeat version and time,
server switches and maintenance hold in `durham_copy`. Enable the Durham queue
before enabling API stitching. The queue blocks new NAS-local leases, and
Durham waits for existing NAS-local work and outstanding registration to drain.
Keep the queue enabled until the pilot receipt commits and turn off API stitching
before pausing the Durham queue.
It downloads to a private partial, verifies the size and full SHA-256,
publishes create-only, syncs the file and directory, then reopens and hashes
the published file before certifying. The last exact output receipt commits
the hour certificate and releases the input holds in the same transaction.
A failed or held seam, omitted clip, repair, or source identity change keeps
the input holds live. The keep-28 executor still requires fresh exact raw
NAS custody before it may delete a released raw.

Keep the existing writer fence and cutoff in place. Standard R2 retirement
excludes staging keys. A paginated multipart check at startup and before wire
authorization exempts only registered bucket, key and provider upload IDs.
Keep the custody lane gated during rollback and until both executors enforce
holds. Retire staged outputs through the separate controller command after
certification, read and write grant drain, a live drained writer fence,
exact evidence, and dependency checks. An ambiguous delete retains its
one-shot intent permanently: it is never reissued, edited or re-derived, it
blocks fence removal and conflicting dependencies, and a later read-only
observation of absence never publishes it. Only an invocation whose own DELETE
was answered may publish a strict absence. For NAS raw cleanup, reconcile legacy
staged absence receipts once with the current command to record a strict HEAD
confirmation alongside the original observation.

### Stage retirement premise

The provider does not implement conditional `DeleteObject`: the reviewed
receipt `58f60cbb660865e6912c0580129ed4191a7a7dcc7bb8f10893fe5dccea82c96c`
(2026-11-19T19:05:06Z) recorded `conditional_delete_unsupported=true` with
`wrong_etag_delete_succeeded=true`. That is a **design premise, not an
affirmative safety result**. A stage retirement therefore does not rely on
`If-Match`, and no proof of it is regenerated: `If-Match` on a DELETE is a
header the provider ignores.

`retire-durham-stage --apply` instead reuses the RAW lanes' audited
single-attempt transport unchanged:

- one direct, no-redirect, non-retrying HEAD of the exact generation, then
  exactly one DELETE whose first request byte is refused at or after the wire
  deadline, over its own HTTP/1.1 connection with no keep-alive, so neither Go's
  HTTP/2 client nor a pooled connection can turn one attempt into two;
- a lost or unknown answer is `ambiguous`: the one-shot intent is kept, nothing
  is retried, edited or reissued, and only this invocation's own answered DELETE
  followed by a strict absence proof can publish absence;
- only a strict `404` with no delete marker and no conflicting provider error
  header confirms absence, and a `200` fails at once;
- publication also requires a complete, exact-key multipart inventory that is
  empty. A `404` alone is never enough: an unfinished upload for the same key
  could still recreate the object after the observation.

One earliest bound governs every step that can issue. The 30-second wire
deadline is fixed before the first pre-issuance read, and the exact-key
multipart inventory, the fence read and the independently committed attempt
all run under it, so a slow check or a late commit can never consume an
authority that has already expired. The gate compares the wall clock against
the bound itself rather than trusting a delayed context timer, before the
attempt commit and again after an acknowledged one. A late or unknown commit
leaves the intent held and sends zero DELETEs; an unknown commit is held the
same way. The confirmation budget after an answered DELETE stays independent
of that wire deadline, and no DELETE is ever retried.

The live fence is bound by its exact stamps. `activated_at` and
`drain_not_before` are compared without truncation: both come from the same
`timestamptz` row, whose microsecond precision survives the round trip, and
the reviewed evidence must name that row's own values, so a subsecond drift is
a different fence rather than a rounded match.

One provider endpoint, taken from the committed stage row. That row's
`endpoint_sha256` is the authority, not whatever the operator's process config
currently names: the retirement loads it from the row, refuses a client built
for any other endpoint, refuses a wire whose own credential names any other
endpoint, and re-verifies the same bucket and endpoint at the exact HEAD and at
the DELETE. A stage on the same bucket name at a different endpoint is refused
rather than split across two providers, and no check compares a value with
itself.

The DB side is unchanged from the migration's guards and takes its locks in the
same order as admission: the per-object session lock, then shared writer
admission `7343226011187`, then the protection epoch, then the stage row and
its certified hour, certificate and connection-13 copy receipt `FOR SHARE`
(never `FOR UPDATE`: the separately committed intent needs `KEY SHARE` on that
same row). Every lock wait is bounded and fails closed rather than queueing.

The command deletes only `stitch-staging/<uuid>/<sha256>.mp4` for the exact
object ID and certified attempt. It refuses any other key shape, so RAW,
keep-28, delivery and every other prefix are unreachable rather than merely
filtered. It requires the exact stage identity, the certified hour and
manifest, a verified connection-13 copy receipt with its identity, no live
stage or COPY grant or lease or range, an empty exact-key multipart inventory,
the protected-dependency checks, and `NOT r2_owner_keep_held` on the stage key
itself. `stitch-staging/` is never its own owner exemption.

Source provenance is authoritative and fails closed. The hour's stream must be
decided in the reviewed owner picks and must not be a keep-everything stream.
The command derives the decided and keep-everything sets itself from the one
pinned picks file (`--picks` with `--expected-picks-sha256`) using the existing
selection parser and effective-choice rule, and requires the reviewed evidence
to equal that derivation exactly. The evidence never supplies the sets that are
used: its `decided_stream_ids` and `ke_stream_ids` are only compared, so a
hand-written array cannot name a stream the owner never decided. A pick made
against a different policy window stays undecided, and any missing, incomplete
or unknown provenance refuses the stage. Every plan clip must have its exact
committed hold and matching catalog identity; and no source may be under a 0220
owner keep prefix (the thesis streams and folders), a pinned keep-everything
folder, or permanent incident evidence.

```sh
stoaramactl nas-stitch retire-durham-stage --object-id ID --sha256 SHA --apply \
  --evidence STAGE.json --expected-evidence-sha256 EVIDENCE_SHA \
  --picks r2-picks.json --expected-picks-sha256 PICKS_SHA
stoaramactl nas-stitch retire-durham-stage --object-id ID --sha256 SHA --reconcile
```

A settled intent never becomes absence through a later read. `--reconcile`
only *replays* the confirmation of an absence that a prior invocation's own
answered DELETE already published; it never manufactures one from a stale or
newer observation, and it never edits what an earlier invocation recorded. An
issued target whose DELETE was unconfirmed, including one that now reads `404`,
stays held: a later `404` proves neither that this DELETE caused it nor that an
ungoverned writer cannot recreate the key. Such a row is never confirmed,
reissued or updated here and blocks fence removal and conflicting dependencies.

The same reconcile command can record absence for an aborted stage, including
one from an earlier attempt. That is a separate, safe proof: the object reached
the terminal `aborted` state without a possible completion, its writer drain
and multipart outcome are both settled, and the exact-key multipart listing is
empty and the key reads `404`. An abort after a possible multipart completion
remains held because that request can still have an unresolved outcome, and no
other hold is released by this path.

### Stable stage evidence (reviewed, not generated)

One reviewed file per stage object. It binds one key and its hour, so it can
never authorize a second target. There is no expiry and no fresh probe: the
premise above is historical.

```json
{
  "version": 1,
  "bucket": "stoarama",
  "endpoint_sha256": "<sha256 of the provider endpoint>",
  "object_id": 123,
  "object_key": "stitch-staging/<generation>/<sha256>.mp4",
  "sha256": "<stage object sha256>",
  "size_bytes": 123456,
  "hour_id": 45,
  "attempt": 2,
  "plan_sha256": "<certified plan>",
  "manifest_sha256": "<certified staged manifest>",
  "fence_activated_at": "2026-11-12T14:28:47Z",
  "drain_not_before": "<live fence drain_not_before>",
  "deployed_writer_coverage_evidence": "<controller reference: which deployed writers this drain covers>",
  "external_writer_race": "external_writer_race_not_proven_v1",
  "picks_sha256": "<sha256 of the reviewed owner picks>",
  "decided_stream_ids": [3],
  "ke_stream_ids": []
}
```

`external_writer_race` is a fixed identifier, not free text and not an
acceptance. It records that an ungoverned holder of the account R2 credential
can still write this key between the final HEAD and the DELETE acknowledgement.
The RAW lane's owner acceptance of that residue is RAW-only and does not extend
here; an operator-supplied freeform flag is not authorization.

`picks_sha256` must equal the digest of the file passed as `--picks`, and that
digest must equal the independently approved `--expected-picks-sha256`. A
swapped picks file cannot ride along on an approved digest for other bytes.
`decided_stream_ids` and `ke_stream_ids` are recorded so a reviewer can read the
decision, but the command re-derives both sets from the pinned file and refuses
a document that differs, so they are evidence of the derivation rather than its
source.

### Rollout prerequisites for stage retirement

Nothing below is authorized by merging this change; all of it is controller
evidence reviewed separately, and `retire-durham-stage` stays unusable until it
exists:

1. `deployed_writer_coverage_evidence` naming every deployed component that
   holds the account R2 credential and the drain that covers each one. A
   rolled-back API, a Render job or an operator script with its own client makes
   the fence a claim rather than a proof.
2. A written decision on the residual external-writer race for stage keys,
   identifying who accepted it and why it is narrower than for RAW footage.
3. A live, drained writer fence whose activation matches the evidence, and the
   exact per-stage evidence above reviewed and pinned by digest.
4. The Durham queue paused with zero live leases and no unresolved stage intent
   or retirement before the first invocation.

The stage generation itself is only ever retired after certification and a
drained connection-13 copy; no step here releases an input hold or a delivery
hold, and no unresolved intent is ever cleared by this command.

### Release certified raw on the NAS

The Durham producer always sends `retirement_held: true`. The connection-13
certificate releases the exact inputs in the same transaction. The
`nas_stitch_r2_certified_raw` view exposes that released set. Each clip maps
to the certified output that contains it, including when an hour has several
outputs.

Keep the original files on the NAS through the controller's keep-28 R2 DELETE
and confirming HEAD. After the exact reservation, intent, attempt, retirement
receipt and absence event are durable, the clip appears in
`nas_cleanup_certified_stitch_raw`. An unresolved R2 DELETE keeps the original
on the NAS.

Use `stoaramactl nas-cleanup prepare --input MANIFEST.json` with the usual
label and items in a separate plan. Request
`{"kind":"certified_stitch_raw","source_id":901}` for an exact eligible clip.
The API derives the replacement proof and original sidecar digest and uses the
existing dry run, plan approval, source freeze, delivery veto and deletion
budget. NAS deletion also requires recorded absence for every staging object
from every attempt for that hour. Before issuing or replaying a permit, the API
checks each aborted stage's writer grant drain, lists multipart uploads for its
exact key prefix, and requires an unambiguous HEAD 404 for that key. Hours with
no aborted stages require no provider reads for this check. The initial dry run
records the raw and sidecar identities from the NAS and hashes them against the
exact recorded evidence, including raws copied after the frozen inventory audit. Before asking for a
permit the client rechecks those files and hashes the retained output. It
rechecks the open files and pathnames immediately before unlink. A missing
or changed output keeps the raw.

Apply 0219 with the NAS deletion master off. Before preparing a certified
dry run, verify the updated API on every serving instance, the exact
`r2-copy-v7-certified-stitch` NAS client, and the updated Jelly gates. Verify
the deployed keep-28 controller closes admission at the first DELETE write,
including when admission closes while the keep-28 transport is dialing.
#565 establishes that behavior for the outside-keep-28 lane. The keep-28
transport needs its own implementation and regression test before rollout.
Keep the ordinary R2 recovery verifier and its Jelly runner on ordinary
plans. Certified plans use connection 13's replacement readback for their
dry-run and deletion evidence. Pause certified dry runs and enabled plans
and drain live permits before rolling either side back.

Before opening the queue beyond the pilot, verify that one full hour from a
non-keep-everything stream has a connection-13 certificate and exact R2
retirement evidence. Leave NAS deletion off during this rollout.
