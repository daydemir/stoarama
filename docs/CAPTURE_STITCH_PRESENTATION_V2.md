# Capture and stitch presentation evidence v2

Status: frozen design contract. This document is normative for future
implementation. It does not enable capture evidence, native stitching, or any
worker rollout.

This contract supersedes informal capture-v2 notes. It extends, but does not
reinterpret, the bounded verifier merged in [PR #185](https://github.com/daydemir/stoarama/pull/185).
The v1 facts and stop-on-first outcome rules remain immutable. The media-lag
handoff work tracked in [issue #184](https://github.com/daydemir/stoarama/issues/184)
is a separate concern and must not be folded into this rollout.

## Claim boundary

The strongest v2 claim is:

> Exact stored clip bytes retain a continuous presentation timeline emitted by
> one admitted, native-copy recorder FFmpeg attempt.

It does not prove that the upstream source emitted every frame or sample, that
nothing was lost before the muxer, or that multiple recorder attempts form one
continuous source timeline. A seamless scheduled-window claim additionally
requires the authoritative expected occurrence, complete envelope coverage,
one native run, zero gaps or overlaps, and every present evidence axis verified.

The following are explicit non-goals:

- no re-encoding, upload, release, deletion, source change, segment-duration
  change, lease surrender, or recorder restart by the verifier;
- no inference from average frame rate, logical clip wall times, reset per-file
  timestamps, pixel equality, or provider/source identity;
- no rejection or loss of captured footage because evidence is unavailable;
- no enablement on protected recordings during the first rollout;
- no mutation or reinterpretation of v1 certifications.

## Versioned policies

- Capture policy: `continuous-source-presentation-edge-v2`.
- Stitch policy: `native-stitch-presentation-v2`.
- Parser schema and semantic tool compatibility are separately versioned.
- Every persisted authored or corroborated fact names all three versions.

## Admission and attempt identity

The server issues an admission and attempt UUID bound to the exact:

- account, recording, current stream and source snapshot;
- recording job, lease token/generation, and relay node;
- capture policy, parser schema, and semantic capture-tool identity.

The lease and admission are rechecked atomically at clip ingest. One attempt is
one admitted native-copy FFmpeg process lifetime. A reconnect, process restart,
capture-generation handoff, HLS discontinuity or timestamp reset, audio
fallback or track-set change, target-FPS/re-encode path, or behavior the wrapper
cannot observe starts a new attempt or makes the affected axis unknown. Numeric
timestamp alignment across attempts never removes that boundary.

Admission is default-off and scoped to exactly one node and one recording. Old
clients and unadmitted recordings preserve the existing behavior.

## Base ingest and retention activation state machine

Capture and upload are the priority path. Presentation probing never delays the
next segment, upload, ingest acknowledgment, heartbeat, or lease renewal.

### Before ingest

After the muxer has closed the trailer and relinquished every writable handle,
the serialized segment owner may create a durable fail-safe retained reference
under an app-owned, mode-restricted staging root. The recorder never modifies,
truncates, or reuses a finalized inode.

The retained reference must be on the same filesystem:

- use a hard link where supported;
- an allowlisted copy-on-write clone is permitted only after its snapshot
  semantics have been proven for that platform;
- a full capture-hot-path copy is not permitted.

The source is opened `O_RDONLY|O_NOFOLLOW`. The staging reference is verified
as a regular file with the exact expected device/inode for a hard link, or the
approved clone identity, plus exact size and SHA-256. Its registry record and
parent directory are fsynced. If durable retention cannot be established,
ordinary ingest proceeds with probe disposition `unavailable`; no claimable
task is created.

### Idempotent base ingest

Base ingest is idempotent on the immutable tuple:

- account, recording, job, lease generation and admission;
- upload intent, destination, bucket and object key;
- capture sequence and attempt;
- clip SHA-256 and size.

The first successful transaction commits the clip, one stable probe task ID,
its absolute deadline, and an idempotency response. The initial task state is
`awaiting_retention`, which is not claimable. An identical retry or exact status
lookup returns the same `clip_id`, `probe_task_id`, disposition, and deadline,
even when the upload intent is already consumed. Any changed immutable field is
a conflict. Concurrent identical calls converge; different calls cannot create
two clips or tasks.

Local cleanup retains the ordinary source or fail-safe reference until this
exact result is recovered. A generic conflict is never treated as success.

### Adopt and activate retention

After recovering the stable server task ID, the owner adopts the same retained
inode into a confined task directory keyed by the server UUID. The adoption is
same-filesystem, fsynced, and revalidated `O_NOFOLLOW` against device/inode,
size, SHA-256, deadline, and the local registry.

`retention_identity_sha256` is a canonical typed digest of the server task ID,
node ID, retained device/inode or approved clone identity, size, SHA-256, and
absolute deadline. It is never a digest of a path string.

An authenticated, idempotent activation call locks and rechecks the task, clip,
admission, node, deadline, file facts, expected task revision, and retention
method. Only `awaiting_retention -> pending` makes the task claimable. Exact
concurrent or replayed activation returns the same revision; changed retention
facts conflict. A task that misses the deadline becomes operationally expired
or unavailable and produces no relay-authored fact.

No task is claimable without durable retained bytes.

## Probe leases, retries, and local byte ownership

Operational probe tasks are mutable only through narrow, database-enforced
compare-and-swap transitions:

```text
awaiting_retention -> pending -> leased -> completed
                              \-> pending (bounded retry/backoff)
awaiting_retention|pending|leased -> expired|unavailable
```

- A claim is restricted to the frozen owner node and exact retention identity.
- `claimed_at` and the absolute retention deadline never change.
- Lease expiry may be reclaimed only by the same node while the exact retained
  bytes still exist.
- Retention stays held across bounded retries and backoff.
- No retry extends the absolute deadline.
- Once bytes are released or the task is terminal, it can never return to
  pending or leased.
- Every transition appends an audit event.

The task and authored evidence are different records. Completion inserts one
immutable terminal authored-fact row under the valid claim and atomically marks
the task completed. The fact is unique by task, preserves the exact request
bytes and hash, claim token, tool/parser identity, and axis outcomes. An
authenticated byte-identical completion replay returns the stored response;
a differing replay conflicts. Expiry alone does not fabricate an
`authored_unknown` observation.

## Ordered, recoverable byte release

Filesystem deletion and a database transaction are never described as atomic.
Release follows an ordered protocol:

1. The server first commits terminal completion, expiry, or unavailability and
   creates an immutable, monotonically versioned release authorization bound to
   task, node, retention identity, and terminal state. The task is permanently
   nonclaimable.
2. The node recovers and validates that authorization, fsyncs a confined local
   release tombstone, reopens and validates the retained file `O_NOFOLLOW`,
   unlinks only that exact task-owned file, and fsyncs the parent directory.
3. The node marks the tombstone released, fsyncs it, and idempotently
   acknowledges the exact release version to the server.

Crashes before server terminalization leave the task and bytes retryable.
Crashes after it leave at worst a bounded retained orphan. Startup replays the
authorization, tombstone, unlink, and acknowledgment. A lost authorization or
acknowledgment is recovered by exact status lookup. If bytes disappear before
server terminalization, the node first reports unavailability and waits for the
release authorization; it never deletes and hopes the database update succeeds.

Startup scans only registered, confined task directories and tombstones.
Symlinks, nonregular files, identity mismatches, path escapes, and arbitrary
siblings are rejected and left untouched.

## Retention and load bounds

Initial hard defaults per node are:

- at most two retained clips;
- at most the smaller of 2 GiB and configured headroom above the state reserve;
- at most ten minutes from retention acquisition to absolute expiry;
- one probe process at a time.

The server may configure lower limits. Awaiting-retention and release-pending
bytes count against every limit. Failure to remain within the bounds makes the
probe unavailable or expired without affecting footage. Lack of bounded cleanup
progress disables future admissions for that node and recording.

## Authored evidence versus independent corroboration

The admitted relay authors capture evidence. Authentication and fencing do not
make it server-derived or independently verified.

Read models expose these separately:

- `capture_attestation_status`: `authored_complete`, `authored_partial`, or
  `authored_unknown` when a relay actually completed an observation;
- absence of a fact after expiry: effective unknown, not authored unknown;
- `nas_corroboration_status`: `matched`, `verifier_disagreement`, or `unknown`
  for each axis.

NAS corroboration recomputes each axis twice with isolated subprocess state on
the same stable `O_NOFOLLOW` file identity and exact SHA-256/size. Persist both
normalized digests, semantic tool tuples, parser versions, timestamps, and file
identity.

- Both results equal the authored fact: `matched`.
- Both agree with each other but differ from the authored fact:
  `verifier_disagreement`.
- Results disagree, an invocation is transiently unsuccessful, file identity
  changes, or compatibility is absent: `unknown`.

A verifier disagreement is not a media defect. It never becomes missing,
duplicate, corruption, re-recording, replacement, deletion, or release. UI
wording is “capture/NAS verifier disagreement.” Qualification consumes only
independently matched axes and current exact NAS byte presence.

## Semantic tool compatibility

Binary SHA is deployment provenance, not a cross-architecture equality gate.
Capture and NAS record a semantic tool tuple:

- FFmpeg and FFprobe releases;
- numeric libavformat, libavcodec, and libavutil versions;
- normalized configure/build-feature digest;
- selected demuxer and decoder implementation names;
- parser schema;
- platform and architecture as diagnostics.

The server owns an explicit capture-to-NAS compatibility matrix per evidence
axis. A row exists only after the pinned corpus produces identical canonical
tuples, digests, and edge facts on both semantic toolchains. A version string
alone never qualifies. An unknown pair makes the affected axis unknown.

The pinned corpus includes video-only MP4, H.264 B-frame plus AAC, VFR,
identity and nonidentity edit lists, priming and padding, fMP4/moov-tail,
missing or conflicting duration/position, raw-extent overlap when empirically
observed, truncated inputs, and hostile resource/reference fixtures.

## Out-of-process probe sandbox

The purpose-built presentation probe is a separately signed,
architecture-specific executable. It is never linked into the recorder daemon.
The release manifest pins its binary SHA, signature/team provenance, build ID,
linked libav versions, and parser schema. An unrecognized artifact is not run.

Its only input is an already validated retained local descriptor, passed as an
fd, or the exact confined task path reopened `O_NOFOLLOW`. It receives no media
URL, credentials, or inherited secret environment. Network access and external
references are disabled. The protocol/demux allowlist is local fd/file plus
MP4/MOV; external drefs, absolute references, concat, crypto, subfiles,
attachments, and nested external references are rejected.

The parent applies process-group, child-count, open-fd, address-space/RSS,
file-size, CPU, wall-time, stdout, stderr, track, dimension, sample, channel,
extradata, and unit limits before allocation or decode. Initial per-phase limits
are 100,000 units, 32 MiB stdout, 64 KiB stderr, and 30 seconds. Output is a
strict incremental typed stream with exact EOF checking. The parent retains
only four leading and four trailing edge facts plus constant rolling state.

Signals, OOM, malformed files, limit breaches, cancellation, and abnormal tool
exit make the affected axis unknown. The whole process group is killed. They do
not crash or restart capture and are never labeled deterministic media defects.

## Independent evidence axes

The contract exposes, without aggregation shortcuts:

- per selected track `demux_payload_status = complete|unknown`;
- per selected track `raw_extent_status = complete|unknown`;
- `video_presentation_status = complete|unknown`;
- `audio_sample_status = complete|unknown|not_present`.

Every unknown state carries one bounded allowlisted reason. Deferred database
checks require the exact child facts for each complete axis and no authoritative
child facts for an unknown axis.

### Demux packets

For each selected track, persist the total packet count, a canonical streaming
digest, and first/last four packet tuples. A tuple contains stream identity,
demux ordinal, integer PTS/DTS/duration and rational time base, flags, relevant
side-data digest, and demux payload SHA-256. Demux-payload completion and NAS
match are mandatory for that media presentation axis to pass.

### Raw MP4 extents

Raw extents are optional corroboration, not demux-payload identity. When
available, validate each `(position, size, raw_extent_sha256)` with nonnegative
position, positive size, checked `position + size <= file_size`, and exact
bounded `pread` under stable file identity.

Do not assume extents are globally nonoverlapping across tracks. Overlap,
missing position, shared/interleaved mux storage, or ambiguity makes only the
raw-extent axis unknown. Footage still ingests, and demux/video/audio evidence
keeps its independent state. The read model never implies raw stored-extent
identity when it is unknown.

### Decoded video

Persist total decoded-frame count, a canonical streaming digest, internal
nonmonotonic/duplicate/hole counters and bounded offending facts, and the first
and last four presentation-frame tuples. A tuple contains deterministic
presentation ordinal and exact rational best-effort PTS and positive duration.
Packet DTS is diagnostic. A decoded-pixel hash is diagnostic and never proves
presence, absence, duplication, or continuity.

Packet-to-frame mapping is authoritative only when unambiguous; one compressed
packet may emit zero, one, or several frames. Ambiguity makes video unknown.
B-frame decode order is never presentation order. VFR uses exact rational
intervals, not average cadence.

### Decoded audio

Persist decoded effective sample count/start/end, a canonical streaming digest,
internal gap/overlap facts, and first/last four sample blocks with exact sample
rate, channel count/layout, PTS, and sample count. PCM hashes are diagnostic.

Audio authority uses the named semantic profile
`decoder-output-effective-samples-v2.0`. A purpose-built libav probe freezes
packet side data and stream/container delay/edit metadata before decode, but
the authoritative blocks are AVFrames emitted after the allowlisted
`send_packet`/`receive_frame` decoder path.

The profile is enabled only when known-sample AAC fixtures prove that its
allowlisted builds apply leading skip and trailing discard exactly once. Under
this profile the verifier performs no second manual trim: effective blocks are
exactly the decoder-output interval
`[pts * time_base * sample_rate, + nb_samples)` on an integral sample grid.

Conflicting aliases, unconsumed side data, negative or excessive trim,
nonintegral grid, decoder disagreement, nonidentity edit mapping, or uncertainty
makes audio unknown. No audio compatibility row is enabled until capture and
NAS agree with the known expected samples.

## Normative presentation math

Timestamps are signed fixed-width integers in reduced rational time bases.
Comparisons across time bases use overflow-safe big-integer cross
multiplication. No per-clip origin is subtracted, and negative continuous
timestamps are allowed.

The v2.0 MP4 edit policy is deliberately conservative. It supports no edit list
or a single rate-one identity edit that maps without trim or offset and agrees
with stream start and the minimum decoded presentation timestamp. Empty edits,
nonzero or negative media-time remapping, multiple edits, non-rate-one edits,
contradictory start/composition normalization, or decoder disagreement make the
affected axis unknown.

For video, an effective frame interval is
`[PTS * time_base, (PTS + duration) * time_base)`. Starts must be strictly
monotonic, durations positive, and every internal end must equal the next
start. A same-attempt compatible seam is exact only when the previous last end
equals the next first start and the required demux/NAS evidence is matched.

For audio, effective sample blocks must be positive, on an exact integer sample
grid, with constant sample rate and channel layout. Internal ends and the
same-attempt seam must equal the next start exactly.

## Stitch-v2 outcome model

Every one of the `n - 1` adjacent clip pairs has separate video and audio facts.
An objective attempt, native signature, or real timeline boundary is recorded
as `not_stitched/not_applicable` with its exact reason and starts a new native
run. Ambiguous evidence inside a claimed run cannot be evaded by splitting every
clip into singleton runs.

Unlike v1 stop-on-first semantics, v2 retains independently established axes:

- exact NAS bytes and strict decode may pass;
- each native-run stream-copy concat and decode may pass;
- video or audio may independently pass, be unknown, or deterministically fail
  for exact missing, duplicate, nonmonotonic, or internal-hole evidence;
- whole-window continuity may be `passed`, `partitioned`, `failed`, or
  `unknown`.

A capture/NAS verifier disagreement is unknown, not a deterministic media
failure. A deterministic presentation failure preserves already proven byte,
decode, and run facts and identifies the exact offending clip or pair. A
partitioned result preserves certified runs but never claims one seamless
window. Full pass requires one native run, one authoritative expected
occurrence, the complete envelope, zero gaps or overlap, every present authored
axis NAS-matched, and every seam exact.

## Canary gates

The first canary is a disposable, non-campaign-protected recording, admitted on
exactly one node. Probe concurrency is one. Source, segment duration, encoding,
and co-resident routes do not change.

Before admission, collect at least 20 consecutive clean, comparable clips for
the canary and every co-resident active job: same worker/build, mode, duration,
and source class; no sequence gap, retry, terminal error, or heartbeat breach.
Compute p95 inter-ingest interval, upload-intent-to-ingest latency, media age at
heartbeat, and ingest age at heartbeat. When 20 comparable clips are
unavailable, use the stricter fixed limits: upload acknowledgment at most 30
seconds; inter-ingest at most `clip_duration + 30s`; media and ingest age at
most `clip_duration + 30s`.

The probe starts only with no pending delivery/upload and healthy state reserve.
It is canceled and future admission for that exact node/recording is disabled
when any co-resident job crosses:

- delivery pending for more than five seconds;
- upload acknowledgment greater than `max(30s, baseline p95 + 10s)`;
- inter-ingest gap greater than `baseline p95 + 10s` or the fixed limit;
- media or ingest age greater than
  `max(clip_duration + 30s, baseline p95 + 10s)`;
- heartbeat age greater than the smaller of two advertised intervals and 120s;
- lease renewal more than ten seconds late;
- a sequence gap, duplicate, upload retry, or terminal capture error;
- a configured CPU/load or state-reserve bound.

Abort only kills the probe process group, safely releases retention through the
ordered protocol, and disables future admission. It never stops or restarts
capture, changes the source, surrenders a lease, deletes facts/media, or affects
co-resident jobs.

Acceptance requires zero lost or duplicate clips, zero quantitative co-resident
regression, independently matched video and audio when present, independent
playback and stream-copy-concat decode, and a forced clean reconnect on only the
disposable recording that proves a new attempt boundary and no false seamless
fact. Native HLS and native YouTube canaries run separately. Rollback disables
future admissions only; immutable evidence remains.

## Required implementation staging

No admission may be enabled until all bounded PRs have merged and deployed:

1. Schema/API for admissions, nonclaimable tasks, idempotent ingest recovery,
   retention activation, terminal facts, release authorization, and read model.
2. Signed sandboxed probe artifact and semantic compatibility corpus/matrix.
3. Relay retention, crash recovery, low-priority task lifecycle, and exact
   canary instrumentation, still feature-disabled.
4. NAS two-pass corroboration and explicit authored/corroborated UI/API axes.
5. Stitch-v2 axis-aware policy, planner, completion, and qualification gates.
6. One exact nonprotected canary admission, evidence review, then separately
   authorized expansion.

Every stage requires real PostgreSQL lifecycle and concurrency tests where
applicable, deterministic crash-boundary tests, hostile media/resource tests,
default-off rollout, and independent review. Merging or deploying code does not
authorize admission or task execution.

## Minimum test matrix

- Every ingest-response-loss, adoption, activation, lease, terminalization,
  authorization, tombstone, unlink, and release-ack crash boundary.
- Exact and differing concurrent ingest, activation, completion, and release
  replay; wrong node, stale token, changed identity, deadline races.
- Crash after ordinary source unlink but before probe; retained inode still
  verifies. Source-name reuse, symlink/path escape, inode mutation, partial
  registry writes, arbitrary sibling preservation.
- Count, byte, reserve, deadline, early-EOF, growth, and retained-orphan bounds.
- Infinite stdout/stderr, timeout, signal, OOM, child/fork attempt, malformed and
  allocation-bomb media, nested/external references, signature mismatch.
- Cross-architecture corpus parity and unknown compatibility pairs.
- Video-only, B-frame+AAC, VFR, edit-list, fMP4/moov-tail, duration alias,
  missing position, legitimate overlap, and known-sample priming/padding.
- Two-pass NAS match, deterministic verifier disagreement, inconsistent repeat,
  and stable-file-identity changes.
- Stitch v2 exact seams, objective boundaries, forged facts, internal video and
  audio gaps, duplicate/nonmonotonic facts, partitioned runs, full-envelope
  pass, and v1 non-regression.
- Canary baseline sufficiency, every numeric abort, probe preemption, forced
  reconnect isolation, and zero co-resident capture regression.
