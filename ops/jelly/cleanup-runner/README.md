# Supervised 20-day cleanup runner

Installation and production operations belong only to the sole controller.
Python 3.9.6 stdlib, psql, Git, the existing recovery CLI and the AWS CLI at
`~/aws-cli/aws` (read-only R2 restore samples) are required.
No R2 delete capability, Render API/configuration access or deployment capability.

Historical approval (Deniz, 2026-11-11), retained verbatim:

> delete everything on the NAS outside each stream's pinned keep28 period (raw clips and old stitched files), without asking per batch, provided every batch passes: (1) the server's dry run AND an independent check that every file is outside its stream's kept period; (2) a verified R2 copy for every file (R2 copies stay); (3) no protected or uncertain files; (4) the 5 TB/day cap, one plan at a time; deletion master switch OFF between batches; fixes deployed only between batches (never pause an enabled plan to deploy); a short daily progress note; any failed check stops the run and goes to the owner.

The rolling 5 TB/day cap in that approval was lifted (Deniz, 2026-11, verbatim:
"lift nas cleanup dialy limit please"). The per-plan byte cap is now the
server's 2 TB, and one plan at a time is unchanged. The server's digest-bound
per-plan lock still fences each batch.

Owner widening to the 20-day window (Deniz, 2026-11, verbatim): "lets do window 20, just ekep cleaning and reducing". Keep Everything streams stay whole in R2; their NAS copies outside the window leave only against a verified live R2 copy, like every other file here.

Redundancy gate (Deniz, 2026-11-27, verbatim: "yuea double check everything be 100% sure redundancy before deleting"): only byte-for-byte raw R2 redundancy qualifies for a NAS day plan. In-window and guard-band days and all incident/delivery holds still stay.

- **Non-KE day cleanup remains disabled.** All 86 non-KE streams were at zero candidate units on the 2026-11-27 07:23–07:38Z pass. The preparer and runner do not prepare/pick non-KE day plans. The API refuses enable and new permits for them, including legacy enabled plans.
- **KE verification requires exact catalogued raw identity.** The catalogue and the present NAS inventory must have the same valid SHA-256 and size. A read-only listing supplies current R2 size evidence; the dry-run API checks the current raw object's ETag and size. A stitched output is never equivalent. Missing raw evidence, differing hashes and hashless files stay. `excluded_no_r2_*`, `excluded_hash_mismatch_*` and `excluded_hashless_*` are disjoint reasons; `observed_hashless_*` also counts eligible present files without both hashes when the listed copy is missing. A matching ETag is not a new body hash, and this is not proof of live custody for every unsampled key.
- **Restore sample:** the runner selects 20 distinct hashed raw keys at random (all keys of a smaller plan), checks the catalogue ETag, performs a conditional full GET of that generation (including VersionId when provided), and computes the SHA-256 and byte count from the read body. It checks the current generation again, re-reads the plan unchanged, and requires one 15-minute deadline for the entire sample. Errors, incomplete samples, drift or expiry refuse the plan. `day_restore_sample` and the dry-run status are verification-only observations, explicitly distinguishing whether the 20-key minimum was met. A smaller sample reads the whole plan but does not claim the minimum was met or grant deletion authority.

**Day mode is verification-only.** Both runner and API refuse new KE activation/permits. The shared durable survivor reservation is deferred until the sibling R2 lane, storage writer/replacer boundary, and NAS client's actual file identity checks participate. A read-only preflight may precede another hold and race it; no new database guard is installed independently of that integration. Existing permits cannot be recalled by these changes: the runner pauses/drains its owned legacy active plan and reconciles receipts. No production execution or deployment is implied by this change. Other per-file operations are not authorized to resume by this PR.

`KE_HOLD` still prevents KE plan selection/preparation. Removing it does not bypass the integration block. Historical day permit shapes and receipts remain readable for reconciliation; no new day permit is issued. Per-file cleanup is unchanged.

The ownership release changes the between-batch mechanism to owned-plan OFF:
exact-digest pause, receipt drain and real recovery. The global runtime master is
controller-owned and must already be ON. Fresh ops review and an explicit
controller GO must approve this changed policy before activation. Merging this
preparation grants no production authority. See [ownership handoff](../finish/OWNERSHIP.md).

The runner takes an exclusive execution flock under the collection snapshot lock,
validates restart latches and the SHA-pinned owner manifest, then durably publishes
its PID before collection can resume. Prior failure, unreadable ownership or
interrupted lookahead requires reconciliation; rejected starts preserve evidence.
A healthy saved enabled plan is resumed only with its exact start digest, valid
approval, certain receipts and master ON. Unknown work is never adopted.
A 60-second heartbeat keeps bounded preparation/verifier calls observable; failure
routes active execution through owned shutdown.

The same explicit D admission/recovery state is checked on startup, each execution
poll and before enable. Problems, identity mismatch, stale/missing evidence or an
admission failure stops cleanup as well as COPY. No success can clear a failure.

Every plan still requires complete fresh server dry-run, independent catalog and
109-window pinned 20-day verification, exact source identities, verified R2
recovery, no protected/uncertain overlap, <=2TB/one stream, and no
other enabled plan/batch. Per-file fresh permits and custody checks are unchanged.
Plans rank by bytes per operation. Ranking caches are hints only, expire after
seven days and are invalidated after admission. Only an evidenced atomic409 may
exclude a stream. Other failures stop; ambiguous admissions are not retried.
At most one successor is admitted for dry-run; it cannot execute until current
recovery/close and a fresh independent enable check. Excluded/reserved/uncertain
supply is never reported as completed cleanup.

Certified non-unlink refusals remain visible and stop when above1% of a plan or
five consecutive ordinals; uncertainty stops immediately. Never retry339081 or
335808–335815 or their RAW sources. Preserve source/incident holds and0209 guards.
Never delete `experimental/v85-retired-WORK-custody/`.

At completion/expiry, pause only if the exact owned plan remains enabled, preserving
an already committed pause reason. Drain receipts, run real recovery and require
issued=verified/problems0. Durably write the real report and atomic checkpoint
before releasing current ownership. Never clear ownership to bypass recovery.
On a failed check, persist the failure and retain supervision until the owned plan
is confirmed stopped. A lost/failed monotonic pause is reconciled through fresh
active-set/digest reads before another attempt. Foreign plans are never mutated.
Disk/email failure cannot skip containment. Shutdown/recovery latches remain.

STOP/SIGTERM prevents successors and drains a healthy active plan naturally;
it never pauses a plan to deploy. Under launchd KeepAlive a SIGTERM drain exits 0
and restarts (immediately after a run over an hour), so only STOP, the latch or
`launchctl bootout` holds the runner. Do not force-kill a draining/containment runner.
A process crash may leave its plan enabled until restart or backend expiry; local
STOP is not a backend deletion switch.

Idempotent reads (API GETs and read-only psql) hold through brief network
failures: up to six attempts with 15, 30, 60, 120 and 120 s backoff (345 s,
enough for a Render API restart's ~4 min of 502s). A retry runs only when its
delay plus its own timeout (HTTP 120 s, psql 330 s) ends within 480 s of the
first attempt. Each timeout is one wall clock over the whole call: an HTTP
request runs in a worker thread joined for 120 s (DNS, connect and the full body,
which urlopen's own per-socket timeout does not bound), and psql runs under
subprocess.run's timeout. An expired GET is transient, an expired mutation is
unconfirmed. So one read returns or halts within 480 s: all six fast HTTP
attempts fit, a timed-out psql attempt is never retried, and the monitor's
safety reads are at most about 480 s late, plus its 60 s poll. The first good
read is checked as usual, so a master-off or mismatch still halts at once.
Only a failure that returned no data retries, the
network-transient class of `../finish/collector.network_kind`: DNS, connection
refused or reset, unreachable network, timeout, and HTTP 5xx; for psql, the
preparer's connection-class SQLSTATEs, statement timeout and process timeout.
A 4xx (408 and 429 included), TLS failure, redirect, bad body or any other
error stops at once. Each retry logs a `step_failed` line with a fixed reason.
Exhaustion latches through the normal failure path. Mutations (controls,
enables, closes, permits) never retry here; recovery verification never
retries. No retry starts once `last_error`, `shutdown_or_recovery_failed` or
`recovery_error` is set, so containment and its own reads are never delayed.

Status is atomic, runner.jsonl and notes.jsonl append-only, fsynced and private.
Confirmed deletion totals come from DB receipts, preserving restart accounting.
A durable startup delivery note is required. Email is optional: empty
CLEANUP_OWNER_EMAIL uses local notes; otherwise existing Resend credentials send
notes/alerts. No credential values, response bodies or private subprocess errors
enter logs. Delivery acknowledgment means provider acceptance, not human reading.

Use the final hashed deployment packet and OWNERSHIP.md for activation/rollback.
Actual installed runner paths come from the loaded plist, not the template.
`--dry-run` can submit metadata and send progress; it is not an offline test.
It never enables a plan and cannot adopt an enabled saved execution.
No automatic rollback, stale status restore, hold removal, failure clear or item
retry is part of installation. All production changes require the sole controller.

Successor enablement waits on the existing D tick lock so D can consume its full
safety snapshot before a new plan appears. A controller's natural STOP uses the
owner-bound JSON in OWNERSHIP.md; it preserves active natural drain and D health.
Foreign or empty STOP content is a safety failure, not a natural-boundary request.

Startup finish hold (owner, verbatim: a runner refusal "latched only because recovery
failed on the network" must "not write a failure STOP that blocks recovery; it should
hold with a distinct reason the collector accepts"; never auto-clear a latch). When
startup finish health refuses for any reason (D latch, recovery_required, interrupted
attempt, missing, failed or >7 h stale evidence) and status names no current plan or
day plan, the runner sets no last_error and makes no backend call: it publishes an
owner-bound `finish_admission_hold` STOP (only when no STOP exists; it alerts once
then), saves phase `stopped` and exits 0. launchd (`KeepAlive` SuccessfulExit,
`ThrottleInterval` 3600) restarts it hourly. A restart that finds its own hold STOP
and a still-refused health re-holds quietly. If D is not latched (`failed` false),
it kickstarts `com.stoarama.finish-recovery` once per unlatched episode, never on
the first (often boot-time) hold; a kickstart failure alerts and keeps the hold. Once health
is clean it removes only its own hold STOP and continues. A natural STOP holds
quietly and stays. A foreign, empty or failure STOP, or an owned current plan or day
plan (which may be enabled and still deleting on server permits), keeps the failure
path: containment, drain, latch and failure STOP.
The runner reuses D's installed admission module to detect abandoned attempts and
orphan directories under its admission gate, including before borrowing its tick
lock. A live recovery can coexist. A stopped selected plan remains pending only
with fresh dry-run/zero-issued proof; neither prepared plan is discarded.
