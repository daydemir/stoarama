# NAS-local V85 pilot (ships disabled)

This worker runs **inside `stoarama_pull.py`**, in the existing NAS container.
The single-file R2 client release already contains it; no second service or
scheduler is installed. This change does not publish/promote a client release,
deploy the API, change production data, or enable a scope.

## Scope and policy

The only seam-positive authority is the approved October frozen decision export:

- policy `v85-qualified-negative-unknown-capture-successor-v2`
- review revision `v85-qualified15-8c0ab4d0369d8d4b`
- classifier entry SHA `a24b6014953d1d962fa55c569a834adbc5b638bf7c61f00f977164a18205cad3`
- frozen decision gzip SHA `dbac9a28d62d35138163eb9fec7da6b5481b04a167c2522dd1dcf665f25b0f46` (9,028,408 bytes)
- uncompressed JSONL SHA `5b2765ecb4f6483c8e48b5a6c958c7c0d8082764d4da57901626a22569c6346b` (30,292,366 bytes)
- export manifest SHA `e877140ad5f4cf93380ca58b3331391a34f445f1882628dba015bbcacb0103b1`
- keep28 bytes SHA `060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811`

The API imports only those exact uncompressed export bytes and its 73,843-edge /
52,462-nomination census. The client independently downloads, hashes and indexes
the entire export before using any row. Lookup binds the ordered **raw clip ID,
whole SHA, byte size**, recording ID and frozen diagnostic identity. Existing
final nominations retain their frozen negative vetoes. There is no fresh
classifier, threshold port, spatial-positive route, CLOCK route or trimming.
Missing membership splits a run; a source without any positive frozen endpoint
membership remains raw. Capture-time gaps/overlaps and incompatible native
layouts also split. No decision is inferred from file names or elapsed coverage.

**Activation prerequisite:** the handoff recovers the classifier sources, but
not the export body, original feature-producer inputs or full veto bundles.
The recovered manifest distinguishes gzip and JSONL hashes above. Import accepts
only the exact JSONL body; verify the original gzip against its separate pin
before decompression. The body has not been recovered. An exact export must be
recovered and its identity/census confirmed before this worker can lease
production work. Failure to recover it is HOLD, not permission
to recreate the export from today's features. Any changed current negative
evidence needs a reviewed admission/corpus revision; this implementation does
not ingest new negative diagnostic bundles or extend October's source corpus.
Do not activate against known newer denials without that admission work. The
recovered source71 alias evidence intersects five frozen nominees, four inside
the current kept windows. Those denials must be reconciled before activation.
Frozen nominees overlap at most 41,986 of 1,158,902 currently kept clips across
34 of 109 streams (identity/size upper bounds, pending current SHA verification).
The remaining footage needs the approved feature producers and a versioned
corpus extension with unchanged nomination and negative-only semantics. This
frozen-replay draft does not complete that broader V85 goal.

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

Media lives in `<raw-root-folder>_V85_keep28/<recording>/<UTC-hour>/` next to the
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
pull, inventory and cleanup behavior does not enable V85 when its switches are
off. Stitch progress/errors ride the heartbeat (`connections.nas_stitch_status`)
and the existing redacted client-log upload (V85/stitch topics included).

## One-stream pilot procedure (operator action after review)

No commands below were run against production for this PR.

1. Recover the exact gzip export, verify both compressed and uncompressed
   pins and the manifest census, and reconcile the known alias/negative
   conflicts through reviewed corpus admission. Do not bypass hash validation.
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

Deferred admission work: recover/replay the full frozen artifacts; review corpus
extensions/current negative bundles for sources outside October; audio-bearing
native runs; unsupported B-frame/edit-list layouts; and automatic import/reuse
of the ~1,103 complete historical V85 hours. Existing R2 V85 presence/ETag and
old publication receipts are not fresh NAS-native verification or deletion
permission. This worker reuses its own exact verified local outputs after a
restart; it does not silently relabel collation-v2, best14 or historic V85.
Local repairs support the existing bounded mdat→Annex B/VUI method, including a
clean prefix before a gap; unsupported FPS/corruption patterns retain raw.
Repaired prefixes are labeled with original/repaired hashes, VUI FPS, frame
count and synthesized timing. They remain outside V85 membership and **never**
authorize original deletion. Imported R2 repair-ledger copies and subsequent
repair-qualified joins require separate exact provenance/admission work.
