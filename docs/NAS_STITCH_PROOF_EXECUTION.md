# NAS proof execution and inventory memory

The isolated proof remains default off. A real NAS joined result, native replay,
current output readback and independent review are still required; an offline
measurement or `proof_working` status establishes neither success nor deletion
authority. The controller alone releases and activates a reviewed artifact.

## Inventory exclusion and deadline

Native proof setup and execution hold `Inventory.activity_lock`, the same lock
as a full inventory scan. Waiting polls cancellation in intervals of at most
250 ms. After acquisition, normal resource and remote admission checks run
again. Cleanup retains its existing fence and cancellation priority.

One monotonic deadline is derived from the original task expiry, minus the
existing 30-second margin. It covers waiting, cold tool setup, source hashing,
native tools, publication and local receipt construction. Checkpoint refreshes
cannot extend it. Proof checkpoint, capability, corpus and tool GETs use a small
isolated child with credential stdin, an empty inherited environment and bounded
private disk output. The parent applies the remaining absolute deadline across
DNS, TLS, headers and body reads, polls cleanup cancellation, and kills/reaps a
stalled or trickling child. Credential stdin uses a nonblocking writer and
explicit EOF inside the same deadline, including pipe backpressure. It never
uses a credential file or repeated partial-input communicate calls. Corpus SQLite connection/transaction waits and cached
tool hashing also use the original budget. Copy transport is unchanged.
Transport reserves two seconds for kill/reap inside that original deadline;
insufficient remaining cleanup budget refuses before spawning.
Repeated provenance and embedded-package verification, audit-file reads,
packet/frame iteration, frozen-witness reads and publication hashes use that
same stop object. Cancellation is checked between reads and members; acquiring
the lock near expiry never grants another setup budget.

This is a logical deadline; an uninterruptible kernel filesystem call cannot be
promised a hard kill bound. The separate controller cutoff still requires timely
OFF. Proof control GETs add a short-lived stdlib process; their actual NAS cost
has not been measured and must be included in operational observation.

Receipt transport and preserved-output readback occur outside the activity
lock. They retain all existing source, output, pipeline and registration
checks. A newer artifact is never an alias for the tested producer.

## Scheduling after a scan

A long-running scan can occupy the activity lock for longer than the proof's
entire budget. The proof then refuses cleanly; the bounded wait promises no
successful turn. Do not hold cleanup idle while waiting for such a scan.

Before proposing activation, the controller needs a fresh heartbeat from the
exact stable target artifact and boot, a non-null `inventory_scan_completed_at`
not earlier than the latest `inventory_scan_pass_started_at`, and no newer pass
start in a second fresh observation. A historical generation's completion or an
incomplete pass is insufficient. These are scheduling observations; acquiring
the shared lock and rechecking admission is the atomic exclusion. If a new pass
wins the race, the proof waits only within its original budget and refuses at
expiry. Normal resource, cleanup, storage and custody gates still apply.

The inventory loop waits ten seconds after startup. It holds the activity lock
across dirty-row synchronization and the full scan, then waits the configured
scan interval after the attempt returns, including a failed attempt. The
interval defaults to 24 hours and is validated at a minimum of 300 seconds.
The controller must establish the actual configured interval and allow for
deployment and polling time before choosing that idle window. A timestamp does
not establish the configured interval, lock ownership or a future reservation.

Held production metadata at 2026-11-15T18:17:28Z reports a pass start at
13:49:35.861748Z with completion still null, about 4 hours 28 minutes later.
It establishes incompletion, not continuous scanning, a stuck worker, overlap
with the failed proof, or memory causation. No held progress series or actual
NAS scan throttle/configuration resolves those alternatives.

The default hash throttle is 20 MiB/s despite its `MBPS` variable name. It
limits payload hashing per file, not metadata walking, sidecar reads, SQLite,
uploads or cleanup-fence waits. Verified cache hits can skip payload rehashing
across generations when their full identity and authenticated sidecar evidence
remain equal. Consequently the full NAS byte count cannot be divided by the
throttle to predict each scan. Both walks also wait on the cleanup maintenance
fence, potentially while holding the activity lock. Bounded disk membership
removes scan-wide Python path retention; it promises neither a shorter scan nor
a measured NAS rate. After an authorized release, record pass progress,
completion, actual configuration and process RSS/peak RSS before attributing
the observed memory cycles or estimating scan duration.

## Full-scan path membership

The full scan stores linked-path membership in a private SQLite database under
`state_dir`, with a 2 MiB page-cache target and bounded transactions. Membership
is inserted at the former set insertion point, before subsequent hashing. An
accepted sidecar therefore remains linked even if a later hash fails; skipped
or storage-failed scans still cannot publish completion. Case, Unicode,
duplicate-path and resume behavior remain unchanged.

Before creation and each checkpoint, the client resolves the actual backing
mount and requires btrfs, ext4 or xfs plus at least 1 GiB available. Unknown,
tmpfs, ramfs and overlay backing refuse the pass. No in-memory or `/tmp`
fallback exists. A successful qualification emits filesystem, device and free
bytes through existing client logs, without a path. Normal cancellation or
failure closes and removes only that invocation's scratch directory. A killed
process may leave its non-authoritative cache; this change adds no stale-owner
cleanup.

Actual NAS backing and capacity have not been established by local tests. A
release review must account for this and native activation requires a primary
qualification receipt. The index uses disk proportional to path count; its
cache target is not a bound on all process or kernel memory. Measure live scan
and native phases using the existing client RSS/peak RSS telemetry after an
authorized release.

Copy retains its reviewed four workers, 16 MiB ranges, 768 MiB admission floor,
cleanup priority and stitch-entry yield. This change does not serialize it with
the inventory lock or establish that concurrent memory use is safe.

## Safe failure diagnostics

The proof-only measurement driver captures and checksum-verifies the unchanged
package runner, then calls `runner.run` instead of its catch-all `__main__`.
Policy, native subprocess arguments, timeouts and decision JSON are preserved.
The pinned package manifest, classifier, native producer, tools, frozen receipt
schemas and offline replay verifier are unchanged.

The child emits a bounded diagnostic on a non-join result. It identifies an
allowlisted stage, input side or pair, exception/result class, requested versus
actual-span phase, return code, timeout and separate native error/source
integrity flags. Source hashing and metadata stages have input attribution;
the pair-native measurement has pair attribution. Parent watchdog failures
retain their own class. Raw exception strings, paths, stderr, environment and
credentials never become proof diagnostics or outer proof error logs. The
original refusal remains a refusal; diagnostics grant no admission authority.

New client source/artifact/profile acceptance and a genuinely new native run
remain necessary. Future producer/current-reader profile pairing and an
isolated output namespace require their own reviewed integration; this change
does not make an old failed workspace adoptable by a new identity.

## Combined producer and current reader

The combined `r2-copy-v3` source retains copy control, cleanup item truth/RSS,
the reviewed joined-poll fence fix, and the native proof. Publication still
requires the complete exact artifact/profile/pair review. The shared registry
and API discovery list are empty: every new admission and validation refuses.
The source in Git is `development`; it must never run with a real connection
credential. Stamped releases have a distinct literal and full artifact hash.

Modes are disabled0, bounded native1, and preserved-output readback2. Ordinary
stitching/deletion retain their unchanged entry and authority checks, and remain
off. Only the combined reader receives2;
legacy native-only releases never do. Account GET
`/connections/stitch/evidence/admission` is scoped to connection13/account47.
Native requests carry canonical `run_id` and operator `activation_id`; readback requests carry the
original producer-profile digest. A readback request never leases or creates a
native task, downloads sources/tools, calls ffmpeg, or falls back to stitching.

Native admission preserves exact source0dfa and the two approved clips. It uses
a new `_V85r_proof_keep28_v3/.../run-UUID` directory and plan digest, preserving
all older workspaces. One original activation UUID, window and admission are saved
before native work. The deadline is the earliest original window/task expiry,
with the30-second margin and an absolute14m30s cap. Its original monotonic
deadline and boot are persisted; a wall-clock rollback cannot extend it, and a
changed boot refuses the window. Capturing admission can only shorten that
persisted limit to the task expiry. Mode1's immediate readback shares this cut;
mode2 renewal keeps its own bounded budget for preserved historical output.
A refresh/restart cannot extend it. An interrupted or failed attempt is not automatically reentered;
a fresh reviewed activation UUID uses another run directory. Startup may
adopt a complete already-published registration, reading metadata only. This
neither replays native work nor accepts output integrity on startup.
Mode1 readback requires the registered run/context to match the durably entered
window/admission and current activation. A different UUID cannot renew an old
registered result; mode2 provides historical renewal. A changed selector or
binding refuses before original metadata, output or tool reads.

The private state directory is disk-qualified under the same btrfs/ext4/xfs and
1GiB reserve rules as the scan index. It owns a lock inode that is never unlinked,
create-only SHA-named evidence blobs, and a checked atomic state index. Total
owned evidence is capped at64MiB; exhaustion holds instead of deleting history.
The client logs actual inventory interval/hash/delay and successful storage
qualification without paths. Actual NAS qualification is still unproved.

An active readback has one90-second deadline covering admission, metadata and
output/tool/package hashing and receipt construction. It yields to cleanup and
uses normal resource floors. It rehashes installed bytes without repairing or
downloading anything. Success preserves the original native run/timestamps and
full decoded registration. It records a new real readback ID/time. Readbacks
run immediately after success or identity change and then every6h; transient
holds wait at least60s and never refresh old times. Integrity refusal persists
across restarts. At most four successful summaries enter the8KiB heartbeat;
process startup accepts none until another actual rehash. Changed/unavailable
evidence or execution identity removes the prior positive status subtree.

OFF cancels native/readback and prevents further media hashing. Already-durable
metadata may drain through existing client logs. All evidence shares one
64-chunk/minute pacer, including failed ACKs, and a full quiet minute after
restart. An exact ACK advances a durable cursor; a lost ACK repeats the same
message bytes. Completed queue entries are dropped, while evidence files stay.
Proof status continues after OFF so fresh current-reader facts remain visible.
These metadata writes/reads must be included in the controller's operation
review; they grant no additional NAS/media action.

The new service POST `/service/nas-stitch/readback/validate` accepts a strict
<=2MiB request containing canonical-base64 original task bytes (decoded<=1MiB),
readback and full native-result bytes (each decoded<=64KiB), all with exact hashes. It preserves `media_seconds`; only an
internal projection enters the unchanged native-result validator. GET
`/service/nas-stitch/readback/status?receipt_sha256=...` observes the specific
preserved receipt without refreshing it. Both use read-only PK-scoped SQL with
5s statement/context and500ms lock bounds, DB-derived time, the exact sealed
pair, current version/artifact/size/boot/host, stable/noncandidate release, and
the actual integer-zero inventory counter. Inventory zero is neither a version
counter nor evidence of a recent full census. Heartbeat/host/status must be
within120s; the particular readback is valid at most24h with5min future skew.
Scope, output, tested policy/tools/package and integrity must still match.
Legacy v2 validation remains unchanged. Server identity contains actual runtime
repository/commit/service/instance only; no deployment ID is inferred.

`export_stitch_readback.py` is an offline component assembler, with no network,
credential or activation entry. It takes bounded scoped log pages, original
run/plan/pair/readback/task/boot identities, a dedicated regular-file component
root and explicit reviewed producer/reader executable hashes. It holds byte
snapshots before executing the exact native81ff and distinct readbackf3a22a
verifiers. It rejects unstamped source, aliases, missing/conflicting chunks,
changed metadata and wrong transport identity. Output is create-only metadata,
the exact decoded registration, mechanical replay results, and a server
validation REQUEST. `components.json` states `authority=none`; it is not an
attestation and an explicitly synthetic input remains synthetic. Actual raw
API validation, separate primary provider binding, independent review and all
consumer gates remain necessary. Final R2 exporter/caller integration is not
implemented by this assembler.

Every future release/proof operation must preserve all four permanent incident
envelopes1414/1417/1421/1427 plus fresh exact rosters/row hashes/source holds and
zero-release checks under that window. Historical operation packets are spent.
Old task1 owner closure and0187 remain independent; this NAS-only protocol
neither reconciles them nor grants R2 eligibility. Audio admission and complete
backlog/rate/space/unattended ordinary stitching remain unresolved. No real NAS
success or finish estimate follows from local tests.

This draft's current reader is exactly `r2-copy-v3`. No v8 artifact or pair is
accepted here. A future v8 release must retain proof, copy and cleanup together
and add its own exact reviewed reader/schema/pair support before publication;
it cannot reuse the v3 artifact identity or switch the NAS back to5009.


### One-use operator activation

Native mode requires a fresh reviewed canonical nonzero UUID in
`NAS_STITCH_PROOF_ACTIVATION_ID`. It is distinct from selector/task1, the client
run UUID, boot and Render identities. The server echoes it with native mode;
the client captures mode/cancellation/UUID atomically, persists the UUID and one
run/window before any admission, then commits native entry before its first API
request. OFF, heartbeat outage, startup and restart never clear the spent set,
original deadline or entry. Reuse cannot create work; a new reviewed operation
uses a new UUID. The bounded64-ID journal refuses at capacity and never prunes.

Legacy native admission is retired: clients unable to echo/spend the UUID
receive no native selector/admission. Historical frozen5009 evidence can still
be verified/read back only under a separately accepted pair. Frozen native
runner337c, receipt18ff, verifier81ff and attestation03d9 remain separately pinned.
Readback2 never falls through to native. Fail-safe0 does not depend on the UUID
or successful native admission, and never clears the UUID/journal.

Future controller enable packets must bind the UUID, guarded single-key config
writes while selector0 then1, one pinned deployment, original cutoff and exact
raw-task evidence. Every operation still needs fresh four-incident guards and
independent review; code defaults grant no release/activation authority.

### New server timestamp encoding

New admission, validation and current-context observations use UTC with exactly
six fractional digits, preserving PostgreSQL's microsecond instants. An
unrepresentable precision or timestamp refuses before HTTP200; timestamps are
never rounded and expiry is never extended. This permits the unchanged pinned
Python3.9 reader to consume DB values whose trailing zeros RFC3339Nano removes.
Native task bytes are serialized once and reused as the parsed JSON task,
base64 and SHA; task and outer expiry encode the same instant. Copied original
native/readback timestamp strings remain exact. The client, profile provenance,
native package, policy, tools and original expiry/freshness cuts are unchanged.
