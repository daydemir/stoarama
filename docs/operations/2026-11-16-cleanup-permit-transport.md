# Cleanup permit transport after idle

Plan 1437's eight-file execution aborted at 2026-11-16T19:29:17Z.
The client measured 18,276 ms for the permit request against its independent
15-second observation limit. No successful deletion receipt was confirmed.
Items 330290–330297 remain permanently held, regardless of diagnostics saying
that this live process had not called unlink.

The NAS began the group at 19:28:26.243Z and reached the permit request at
19:28:58.918Z. Proofs and durable journals took more than the transport's
30-second keep-alive idle limit, forcing a new connection inside the permit
clock. The earlier lease request also took 17,812 ms. The backend registered
the eight observations at 19:29:15.507–15.529Z, and minted their permits at
19:29:16.058–16.443Z. Thus per-file policy/issuance was quick; most of the
client interval preceded observation registration. Historical logs cannot
split DNS, TCP/TLS, client scheduling, and authentication time further.

Bulk cleanup now explicitly connects immediately after its durable intents
and before starting the permit timer. It records `connect_ms` separately and
checks cancellation/lease freshness again after connecting. Connection failure
submits no observation. This preserves the 15-second client refusal guard,
exact proof checks, 30-second server permit expiry, and all journal ordering.
The server's whole execution request budget is reduced from 15 to 10 seconds,
including authentication, recovery reads, lock waits and commit, leaving five
seconds for transit and local checks. Slow requests cannot issue late permits.

The production client patch is `v85r-nas-proof-v2.1`: the immutable v2 artifact
plus this transport change and its exact release label. It contains no v3
copy-control changes. The new server release admits it only for cleanup; COPY
and stitching proof remain held. Source on main also carries the transport fix
for the eventual v3 release.

Deniz authorized closing 1437 with the existing guarded operator override after
all eight R2 recovery objects verify. Closure retains item/receipt evidence and
issued recovery/source holds, and makes the plan terminal. Only after fresh
hold, idle, OFF, client-stability and COPY checks may the stopped runner's
ownership move to its already admitted dry-run successor 1438. Preserve the
failure STOP and prior status as incident history. Any new uncertainty, recovery
failure, unknown enabled plan or data mismatch stops resumption.
