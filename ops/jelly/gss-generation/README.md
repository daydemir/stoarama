# GSS generation publication

`republish-cycle.sh` is the Mooney-side orchestrator for the public Global Street
Scores video registry. `gss_gen.py` is the Jelly-side stage and pointer tool.
These are source snapshots of the operator scripts, not a deployment mechanism.

A publication moves two pointers: the gzip-compressed `public/current.json`
registry in object storage, and the singleton `gss_public_generation_pin` row
used by the API during the NAS cleanup fence. The candidate's immutable
`generations/<id>.json` registry names the parent it was built from. Both the
parent and candidate must be staged, and the exact registry bytes must match
whichever generation `current.json` names.

## Command contract

The publisher and stager must use the same version of the scripts. The old
one-ID `swap` and `rollback` commands are intentionally no longer supported.

```text
gss_gen.py stage DIR
gss_gen.py verify GENERATION
gss_gen.py swap EXPECTED_PARENT CANDIDATE
gss_gen.py rollback EXPECTED_CANDIDATE PARENT
```

A forward swap requires both pointers to name the expected parent. A rollback
allows either pointer to already name the parent, but refuses any generation
outside the exact candidate/parent pair. Object writes use the ETag returned
with the object bytes as `If-Match`; database writes use a generation-qualified
UPDATE. Both exact pointers are read back before reporting success. On a
failed forward write or uncertain response, the stager attempts conditional
rollback while holding the same Jelly-local lock.

The cycle also holds a Mooney-local lock across the build, swaps, gates, and
ledger publication. It arms rollback before invoking the remote swap, so a
lost SSH response is treated as an uncertain write. It checks the preview,
viewer, download, ledger and availability gates, then verifies both pointers
again before and after copying the ledger. Ledger record copies are backed up
before replacement and restored if publication fails.

| Cycle exit | Meaning |
| --- | --- |
| 0 | Candidate and ledger published; both pointers verified. |
| 3 | Forward precondition/availability refused; no forward write attempted. |
| 4 | Publication failed; both parent pointers verified after rollback. |
| 8 | Unresolved divergence: pointer rollback or ledger restoration could not be verified. |
| Other nonzero | Pre-swap failure, e.g. build, stage, lock or offline check. |

An exit 8 is not a successful rollback. Stop publication attempts until the
actual pointer identities and ledger status are accounted for. Do not use an
unconditional pointer write to repair a conflict. If the state is still within
the original candidate/parent pair, the same conditional rollback can be
retried; otherwise a new recovery decision is needed. An interrupted cycle
may leave `$HOME/.stoarama-gss-cycle.lock` in place; only remove that directory
after confirming the cycle is no longer running and reconciling its pointers
and ledger. The Jelly `generation.lock` file uses an OS lock and should not be
removed. These locks serialize cooperating local writers, not arbitrary external
writers. The object and SQL stores do not share an atomic transaction; the
conditional writes and readbacks detect conflicts and explicitly report when
reconciliation is not proven (they cannot prevent every external race or an
untrappable process termination).

## Offline validation

```sh
sh -n ops/jelly/gss-generation/republish-cycle.sh
python3 -I ops/jelly/gss-generation/test_generation.py -v
```

The tests use in-memory object storage and a mock singleton database row, and
run the shell cycle with isolated temporary files and fake network commands.
No production credentials, remote commands, storage or database are accessed.
