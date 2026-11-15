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
stalled or trickling child. Corpus SQLite connection/transaction waits and cached
tool hashing also use the original budget. Copy transport is unchanged.
Transport reserves two seconds for kill/reap inside that original deadline;
insufficient remaining cleanup budget refuses before spawning.

This is a logical deadline; an uninterruptible kernel filesystem call cannot be
promised a hard kill bound. The separate controller cutoff still requires timely
OFF. Proof control GETs add a short-lived stdlib process; their actual NAS cost
has not been measured and must be included in operational observation.

Receipt transport and preserved-output readback occur outside the activity
lock. They retain all existing source, output, pipeline and registration
checks. A newer artifact is never an alias for the tested producer.

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
