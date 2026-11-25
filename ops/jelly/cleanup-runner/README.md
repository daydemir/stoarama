# Supervised 20-day cleanup runner

Installation and production operations belong only to the sole controller.
Python 3.9.6 stdlib, psql, Git and the existing recovery CLI are required.
No R2 delete capability, Render API/configuration access or deployment capability.

Historical approval (Deniz, 2026-11-11), retained verbatim:

> delete everything on the NAS outside each stream's pinned keep28 period (raw clips and old stitched files), without asking per batch, provided every batch passes: (1) the server's dry run AND an independent check that every file is outside its stream's kept period; (2) a verified R2 copy for every file (R2 copies stay); (3) no protected or uncertain files; (4) the 5 TB/day cap, one plan at a time; deletion master switch OFF between batches; fixes deployed only between batches (never pause an enabled plan to deploy); a short daily progress note; any failed check stops the run and goes to the owner.

The rolling 5 TB/day cap in that approval was lifted (Deniz, 2026-11, verbatim:
"lift nas cleanup dialy limit please"). The per-plan byte cap is now the
server's 2 TB, and one plan at a time is unchanged. The server's digest-bound
per-plan lock still fences each batch.

Owner widening to the 20-day window (Deniz, 2026-11, verbatim): "lets do window 20, just ekep cleaning and reducing". Keep Everything streams stay whole in R2; their NAS copies outside the window leave only against a verified live R2 copy, like every other file here.

Day-directory mode (`--day-plans`; Deniz, verbatim: "yes, delete"; "if its outside of keep everything, then a nas copy only needs to be checked if its in the keep 20? otherwise it can be deleted right?"; "lift nas cleanup dialy limit please"): non-Keep-Everything raw NAS footage outside the pinned window leaves one stream-day directory per permit, with no R2 copy check and no daily cap. The runner admits one day plan per stream via the preparer's `--day-plans`, independently re-checks every unit (folder map from origin/main, not Keep Everything, whole local day outside the window with a one-day guard band, no held or in-window catalogued clip, digest and file/byte totals), enables it through the same D gate, and pauses and closes it before the next. Only the `r2-copy-v8-tunable-copy`, `r2-copy-v8-tunable-day`, `r2-copy-v8-tunable-modes` and `r2-copy-v8-ke-day` clients receive day permits; a client refusal stops the runner. The day client unlinks without the app-writer fence (that fence guards per-file cleanup's namespace proof, which day units do not make; a day unit unlinks only the names it selected, so later arrivals survive). The tunable-modes client gets each permit in one of two modes, decided by the server from the directory layout: `names` for a flat `recordings/<id>/{continuous,<job>}` directory (the permit lists the exact inventoried media and sidecar names with sizes; the client lists the directory once and unlinks each listed name only while it is a regular file of that size, never touching any other name), and `dir` for a plaza `<Month>/<DD-Weekday>` directory (Deniz: "the nas keeps all keep 20 footage"; the client deletes every media file and sidecar of that date, catalogued or not, and refuses the unit if any visible entry is not a regular media/sidecar file of that layout and date; hidden partials stay). Day permits last 10 minutes (bounded by plan approval), not the 5-minute heartbeat window. The client logs one `NAS day unit` INFO line with per-phase ms per unit (also in the ledger's `day_receipt` `timings`). Per-file and day ownership never overlap. A non-Keep-Everything day is held only by a live NAS cleanup incident hold (migration 0259; Deniz, verbatim: "we need to remove whatever is making us think thats a mistake"; "just drop the recordings its fine, we'll fix the links whenever"): delivery holds, R2 retirement reservations, stitch/joined state and per-file plan source holds no longer keep it; a Keep Everything day keeps all of them. A day admission 409 excludes its stream from day plans only (`day_excluded_streams`); per-file `excluded_streams` never skip a day plan.

Keep Everything days (Deniz, verbatim: "nas shoul dnot have any of the keep everything (except the keep 20)"; "if its the same size and same file name or whatever checks we have already?"): the runner prepares Keep Everything streams after every other stream, one plan at a time, passing `--r2prune ~/stoarama-r2prune-lane/r2prune`. The preparer names a KE file only when its NAS size is the catalogued clip size, its R2 counterpart is live in the catalogue (the raw object neither purged nor retired, or a stitched output listing the clip that is neither retired nor reserved), and a fresh read-only `r2prune list` of just those keys' roots shows the key at that size; the listing is kept as `r2-listing-s<stream>.tsv` in the plan's out dir. (The r2prune lane's own `cycles/<stamp>/snapshot.tsv` lists only its non-KE candidates and is deleted after each cycle, so it never holds a KE counterpart.) Every other file stays and is counted as `excluded_no_r2_*`. The plan digest binds the names; the runner re-checks each name against the catalogue (`unlive` must be 0); the server permits only named files whose counterpart is still live, always in names mode, and only to `r2-copy-v8-ke-day`; that client accepts a KE permit only in names mode. In-window and guard-band days are never planned. The R2 lane deletes no KE object: its candidate query excludes `keep_everything` windows, `r2prune decide()` returns `protect_keep_everything` first, and the cycle aborts on any delete class but `delete_window_20d_on_nas`.

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

Successor enablement waits on the existing D tick lock so D can consume its full
safety snapshot before a new plan appears. A controller's natural STOP uses the
owner-bound JSON in OWNERSHIP.md; it preserves active natural drain and D health.
Foreign or empty STOP content is a safety failure, not a natural-boundary request.
The runner reuses D's installed admission module to detect abandoned attempts and
orphan directories under its admission gate, including before borrowing its tick
lock. A live recovery can coexist. A stopped selected plan remains pending only
with fresh dry-run/zero-issued proof; neither prepared plan is discarded.
