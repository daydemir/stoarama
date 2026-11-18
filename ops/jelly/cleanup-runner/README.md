# Supervised keep28 cleanup runner

Installation and production operations belong only to the sole controller.
Python 3.9.6 stdlib, psql, Git and the existing recovery CLI are required.
No R2 delete capability, Render API/configuration access or deployment capability.

Historical approval (Deniz, 2026-11-11), retained verbatim:

> delete everything on the NAS outside each stream's pinned keep28 period (raw clips and old stitched files), without asking per batch, provided every batch passes: (1) the server's dry run AND an independent check that every file is outside its stream's kept period; (2) a verified R2 copy for every file (R2 copies stay); (3) no protected or uncertain files; (4) the 5 TB/day cap, one plan at a time; deletion master switch OFF between batches; fixes deployed only between batches (never pause an enabled plan to deploy); a short daily progress note; any failed check stops the run and goes to the owner.

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
109-window pinned keep28 verification, exact source identities, verified R2
recovery, no protected/uncertain overlap, <=100GB/one stream, <=5TB/24h, and no
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
it never pauses a plan to deploy. Do not force-kill a draining/containment runner.
A process crash may leave its plan enabled until restart or backend expiry; local
STOP is not a backend deletion switch.

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
