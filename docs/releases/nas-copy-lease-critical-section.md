# COPY lease admission without network lock holds

Lock `7343226011192` protects two invariants:

- A lease grant must count only committed renewals, so no more than four COPY
  leases can be live, including a renewal that began before its old expiry.
- The controller's EXCLUSIVE deploy fence drains grant/control transactions
  before committing `nas_cleanup_maintenance.active=true`. A preparer cannot
  grant authority across that committed fence. A zero-ever-issued assertion
  remains race-free while the fence owns the lock.

It does not protect cleanup's singleton execution lease (#525), which has its
own lock/protocol. Changing lease admission to SHARED would lose the first
invariant. Leases retain EXCLUSIVE only for database revalidation, the live cap
recount and grant/block commit; active controls remain SHARED.

A request first checks the candidate and source in a bounded database
transaction, selecting with `FOR UPDATE SKIP LOCKED`. That transaction commits
before R2 HEAD, and no row/advisory lock or open transaction spans HEAD. Random
token generation is also outside the final transaction. HEAD transport errors
remain resumable; deterministic source/R2 identity failures remain item-scoped
terminal blocks.

After HEAD, the short EXCLUSIVE transaction rechecks maintenance, the four-live
cap, exact item JSON/policy and current eligible row state. It reloads source
identity/holds before granting, so an earlier source snapshot grants no
additional authority. Concurrent row consumption retries at most four candidates;
a source that recovered during preparation obtains a fresh HEAD. No new
capability is returned without a successful commit.

The controller's lock order is unchanged: EXCLUSIVE1192, then the permanent
maintenance singleton `FOR UPDATE`, then guarded assertions/activation and
commit. A HEAD in flight has issued no lease and need not delay that fence;
its late final transaction refuses while maintenance is active. Existing active
control/grant commits still drain before the fence can acquire1192.

This is server-only: no client literal/admission, schema, range size, resource
floor, lease duration or request timeout changes. It can share the exact backend
release containing v5's server support; only the sole controller deploys it.
The full migration delta of that selected backend still needs its normal review.

Tests cover a blocked HEAD while checkpoint/range/completion controls finish,
HEAD in flight followed by fence commit and grant refusal, fence drain of an
in-flight control, concurrent lease/control load, the original expiry/renewal
slot race, and authenticated real client control requests with overlapping HEADs.
The local fixture establishes concurrency/custody behavior; production throughput
remains a controller measurement.
