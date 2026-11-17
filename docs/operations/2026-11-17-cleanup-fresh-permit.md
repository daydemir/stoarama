# Fresh NAS cleanup permits

Plan 1439 stopped when seven files consumed the eighth file's shared 15-second
permit window. The client did not restart: one boot ID spans the incident.
Its execution lease still had over 1,600 seconds remaining. Hashing and durable
intents preceded authority; the slow serial NAS namespace checks and syscalls
after authority consumed the deadline. The v2.1 artifact also lacked main's
per-item durable result fix, so the interruption left all eight armed intents
for conservative replay, including seven completed media/sidecar pairs.

The immutable v85r-nas-proof-v2.2 release requests one execution file per lease,
rejects larger execution rosters before journaling, and retains the pre-armed
bulk durability path for that one file. Each file receives a fresh authority
window. Permit age/expiry checks before both syscalls, proofs, holds, local caps,
recovery verification and conservative crash replay remain unchanged. Read-only
dry-run concurrency is unchanged. This trades grouped sync throughput for
independent authority; no permit deadline is lengthened.

Release from reviewed merged main to include the existing per-item result fix.
A successful unlink is acknowledged only after parent directory sync, ledger
fsync and SQLite checkpoint. Before-attempt refusals can be certified only by
the live owner. Possible attempts remain uncertain and never retry.

The runner re-reads its owned plan if the active query becomes empty between
reads, sending a confirmed terminal/pause state through finish (OFF, drain,
recovery and receipt classification). A still-running owned report, unknown
enabled plan or enabled batch remains a hard failure.

Plan 1439 reconciliation must preserve the eight uncertain receipts 335808–335815
(attempt 2), all 1,896 issued-item recovery/NAS reservations, and all source holds.
Only its 1,541 never-issued identities may be replanned after hash-bound operator
closure. Neither this change nor closure authorizes R2 deletion or stitch proof.

Deploy and promote only with master OFF and no enabled plans or batches.
Back up status, preserve all failure latches until their authorized resolution,
install the reviewed runner in its own release directory, and confirm the
executing v2.2 heartbeat before restarting. Keep the original Jelly checkout.
