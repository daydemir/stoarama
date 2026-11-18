# Owner authority for the R2 fence and cutoff

Deniz, 2026-11-18 approximately 15:50Z, verbatim:

> what the flying fuck is this safety defense. we can delete its fine

Deniz, 2026-11-18 approximately 16:05Z, verbatim:

> lets just go faster now, as long as we're only deleting footage that is all of / 1. not in the keep everythign set / 2. not in the 28 days set AND not on the NAS (ie waiting to be transferred) / then just delete it all now no need to limit 5tb / day

ROOT authorized migration 0210 to change only the `stoarama` fence's
`drain_not_before` to the migration transaction's `now()`, temporarily disabling
only `r2_writer_fence_transition` for that UPDATE and re-enabling it immediately.
The activation time and endpoint stay unchanged. The reported activation is
`2026-11-12T14:28:47.037031Z`; the object cutoff is its second truncated downward,
`2026-11-12T14:28:47Z`. Listing and final HEAD LastModified must be strictly before
that cutoff and unchanged. A different live activation second fails closed.

ROOT explicitly waived accepted-PUT completion/access-log proof and replaced the
old canary/analytics/two-hour no-signing proof route with this owner policy.
This record is authority, not evidence that external PUTs completed. An external
PUT may still overwrite a key between final HEAD and unconditional DELETE, or
reappear afterward. The typed acceptance names that race; permanent intents and
single-attempt DELETE still prevent automatic replay.

KeepEverything footage stays protected. Keep28 footage needs fresh per-object
NAS size/hash custody evidence within 24 hours. Unknown identities, incidents,
holds, changed identity and the six live wire/capability categories still veto.
The current inverse executors remain RAW-only; this record does not implement
broader collated/joined/delivery execution. NAS cleanup and COPY budgets remain
unchanged. Production migration, evidence sealing, installation and deletion
are controller-owned operations, separate from this source change.
