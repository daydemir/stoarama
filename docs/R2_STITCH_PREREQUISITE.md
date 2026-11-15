# R2 stitching prerequisite

The [owner approval of record](records/r2-owner-approval-20261114.txt) requires a
verified real NAS stitch before any R2 deletion. KeepAll, global delivery kept14
periods and dependencies, unresolved identities, kept footage awaiting a fresh
verified NAS copy, and NAS cleanup recovery holds remain protected. The full
pinned keep28 policy applies despite the owner's R2 sentence saying 27 days.

No real artifact was available when this consumer was implemented. Its code,
tests and independent code review establish implementation behavior; the actual
NAS run, fresh readback and independent postrun review are separate prerequisites.
An executing-client release change requires a newly reviewed pin and proof.

## Verification

The read-only command takes the outer artifact path and its SHA from the fresh
independent postrun reviewer:

```text
stoaramactl r2-retire verify-stitch-proof \
  --stitch-proof /absolute/artifact/attestation.json \
  --expected-stitch-proof-sha256 <independently-reviewed-outer-SHA>
```

The frozen outer schema is `stoarama.nas-stitch-attestation.v1`, SHA
`03d9a5fa930b67249036a6b522dcffa59f6bba9744f79abc0747f85d9ee63abc`.
Native receipt schema SHA is
`18fffd8f94334210eb12ccee8c0856a824cd9ae9c3dc97b6fe6124e99a0906d4`.
Canonical regular artifact files are loaded through directory descriptors with
no symlink component, checked against exact sizes/digests, and copied into memory.
Every frozen field is required; duplicates, unknown or case-aliased fields,
nulls, conflicting identities, synthetic artifacts and stale evidence refuse.

Only verifier `843a90384f5e4ae82503bf033be5dba167127cb1bb65fbaac553b17bbb5169bd`
and NAS client `06425b3a518ae327cf322b7ef19d8c35917737e14a78182b9d8671b8f3dfc411`
are recognized by this release. The original `64e0…c73d` client is revoked.
The recognized client code is PR503 head `4e433a0f…`, merge `2ea3ef18…`;
recognizing its bytes does not establish that it executed on the NAS.

The checksum-verified verifier bytes execute directly as an isolated system
Python `-c` argument. Exact client, source-plan and compressed native receipt
bytes go through bounded strict base64 stdin to `replay-stream`. The gate never
reopens executable/input paths, imports the NAS application, downloads media,
uses credentials or substitutes a second V85r algorithm. Output, runtime and
size limits fail closed. Complete local registration must equal the decoded
native receipt, up to 16 MiB; it is not a small summary.

The replay and outer receipts bind connection13/account47, stream17208/
recording444, immutable task1 source plan and exact pair373218+373262, current
policy/package/tools, reviewed pipeline, actual run, inputs and joined output.
Fresh server validation/readback and independently reviewed payload/bundle/code
digests must agree. Reviewer observations are an opaque checksum-bound attachment;
the consumer does not invent machine facts from prose. Readback, validation and
review expire within24h, with at most5min future skew. An old immutable run only
qualifies with fresh actual output rehash and unchanged pipeline/tool/policy.

## Retirement and recovery

Apply requires the same `--stitch-proof` and
`--expected-stitch-proof-sha256`. Only NAS-backed RAW provider-unconditional
HEAD-guard mode remains; legacy and conditional apply paths refuse. Provider
capability proof, owner credential-risk acknowledgment, exact bucket/endpoint,
immutable writer fence and drain deadline, all writer categories, valid indexes,
live multipart inventory, full public delivery dependency closure, conservative
best14 periods, keep28/custody/source/cleanup holds and budgets remain independent.
No provider probe or production operation is run by proof verification.

Manifest age is at most24h. Authoritative DB UTC, anchored before its clock query
to monotonic elapsed time, bounds proof and manifest expiry immediately before
every SDK wire delete. The single-attempt transport takes a private verified
authority; missing, zero, inspection-only or expired authority sends no request.
The real NAS artifact must pass the read-only command before an execution plan
is proposed. A production run still needs its own fresh review and explicit
controller window; the controller alone deploys code.

Version3 ledger records fsync the run's proof authorization before intent or
reservation. Each intent binds manifest SHA, bucket/endpoint, kind/source/
recording/stream IDs, exact key/path/span/size/SHA/ETag and the authorization's
record hash. Key-only history, malformed/torn records and unsupported versions
stop without rewriting the old bytes. Preserve them for separately reviewed
recovery.

An absent object requires an exact prior intent for the same manifest and full
target identity, plus a current verified authorization. Resume appends a new
authorization and an explicit relationship to the original intent and its hash;
it never rewrites or extends old proof expiry. `--record-already-absent` cannot
waive these conditions. An unresolved intent for a still-present object stops
for review rather than retransmitting an ambiguous DELETE. Already-absent bytes
are reported separately and are not counted as fresh wire-deleted bytes/savings.

Legacy SDK single, conditional and bulk deletion refuse, even for empty lists.
Their operator/library entry points refuse before catalog mutation. Account/admin
S3 verification also refuses before creating probes, storing credentials or
creating destinations. Managed lookup only returns an existing verified row;
it does not provision or bill. Separate WebDAV verification is unchanged.
NAS upload-probe requests return the existing disabled response; restore leases
return existing empty tasks/retry120s. Both result handlers refuse before state
changes, and old sweeps are removed. Existing objects and durable rows remain;
nothing is falsely marked deleted or terminal to manufacture writer drain.
