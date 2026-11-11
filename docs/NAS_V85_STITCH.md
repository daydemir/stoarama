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

Server output rows/proofs are append-only. Local durable registration receipts
are written only after the server registration ACK. A second readiness ACK then
admits an existing cleanup dry-run plan containing **only** fully included
originals; cleanup cannot race ahead of the local receipts. A lost ACK is
reconciled from the SQLite FULL/WAL journal before new work. The stitch lane
never unlinks raw itself.

`stitched_included` is a new permit reason, which may apply inside keep28 only
when the registered source proof and current owner scope both still agree. It
uses the existing exact-file cleanup rehash, R2 recovery-copy verification,
source holds, 30-second permit, receipt replay, append-only deletion ledger,
shared 5 TB/24h byte budget and local file/byte caps. It never grants NAS-only
retirement. A purged/missing R2 raw copy keeps the NAS raw even if stitched.
R2 objects are neither overwritten nor deleted by this worker.

Before each raw unlink, the client independently requires its local registration
receipt, exact complete inclusion proof, enabled local switches, and a fresh
whole output rehash. An output/source mismatch or missing receipt blocks unlink.
The common cleanup machinery auto-pauses execution on anomalies. Failed/paused
retirement blocks further growing work in this single lane until reconciled;
do not erase SQLite journals, outputs, ledgers or plans to restart it.

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

## One-stream pilot procedure (operator action after review)

No commands below were run against production for this PR.

1. Use the recovered exact gzip export, verify both compressed/uncompressed
   pins and the manifest census, and verify the bundled source/hash alias vetoes. Do not bypass hash validation.
   Replay/admit a representative production unit with the NAS's pinned FFmpeg
   before any deletion. This PR's synthetic corpus adapter is
   test-only and cannot admit production media.
2. Deploy migration/API and a reviewed client candidate through the existing
   release process. Verify `nice` and `ionice` exist inside the NAS container.
   Set server `NAS_STITCH_ENABLED=true` and client
   `STOARAMA_V85_STITCH_ENABLED=true`. Leave **both new deletion switches OFF**.
   Leave the existing cleanup master as otherwise required by normal cleanup.
3. With the operator's existing service authentication, import the exact file:
   `stoaramactl nas-stitch import-corpus --input approved85-decisions.jsonl`.
   Choose one stream and one ready hour entirely inside its embedded window.
   Approve a bounded scope (all dates RFC3339):

   ```sh
   stoaramactl nas-stitch approve --stream-id STREAM \
     --start START --end END --approved-until EXPIRY \
     --reason 'Deniz approval reference' --approve-deletion
   ```

   `--approve-deletion` records the owner's intent for this exact scope; it does
   not turn on either deletion switch. Approvals expire within 180 days. Missing
   this flag leaves raw retained; scopes are immutable, so choose the intended
   authority explicitly before starting. Do not submit overlapping scopes.
4. Use `stoaramactl nas-stitch status`, NAS heartbeat and client logs. Check the
   new output SHA, raw exception list, recovered-prefix provenance and the common
   cleanup plan's complete `would_delete`/verified R2 evidence. Missing corpus,
   unsupported native layouts, audio and missing recovery copies are HOLD.
5. After reviewing that exact pilot, enable client
   `STOARAMA_V85_STITCH_DELETION_ENABLED=true`, then server
   `NAS_STITCH_DELETION_ENABLED=true` last. The existing
   `NAS_CLEANUP_DELETION_ENABLED` must also be on. No extra per-hour approval is
   needed within the owner's scope: each verified unit completes the common
   dry run, passes the common one-plan/budget gates, then is armed for at most
   24 hours and the remaining scope lifetime. Existing cleanup work has the
   same shared caps. Completion closes only this lane's cleanly deleted plan;
   R2 recovery holds persist. Verify ledger/receipts and measured NAS headroom.
6. Stop with `stoaramactl nas-stitch pause --scope-id ID` or either new deletion
   switch. Pause prevents new work and new grants; an already issued exact-file
   permit retains its existing <=30-second lifetime. Reconcile an anomaly using
   the common cleanup report/recheck tools; never widen an inclusion claim.

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

Stitch retirement activation now calls the same committed-freeze publication,
source-writer drain and replay/cache activation as ordinary cleanup, under the
public cleanup fence. Both fresh stitched permits and the common cleanup lease,
observe, receipt and reconciliation paths retain session binding, serial caps,
15-second permit-observation deadlines, R2 recovery evidence and lease returns.
The stitched inclusion check replaces only the outside-keep decision: a missing
or changed activation/cache generation cannot grant a fresh permit.

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

The explicit future client release is **`NAS_VERSION=v85r-fence-progress-v1`**,
allowlisted alongside reviewed predecessors, including `joined-marker-recovery-v1`.
Build that release from merged `main` with this explicit label. Local stitched
media, repairs, proofs and registration receipts use the same alias quarantine,
verified private copy, atomic canonical replacement and crash-safe ledger as
joined delivery. Startup reporting replays their shared quarantine ledger.
Keep every stitch/deletion switch
off until the separately approved pilot. This work does not stage/promote a
release, mutate production, merge, or deploy.

Regression coverage includes real PostgreSQL freeze/cached-generation changes,
source-write refusal, checkpoint identity/stream/switch behavior, shared reviewed
session gates, a deliberately blocked build concurrent with actual included-raw
retirement, and cancellation of a live native child after lane acquisition.
The stitch migration remains **0179_nas_v85r_stitch.sql**, with main's cleanup
freeze migrations 0180/0181. Joined lifecycle fixtures apply them in order.
