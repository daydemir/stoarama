# Isolated current-policy NAS proof

This is one bounded experiment, not ordinary stitching admission. Owner words:
“prove stitching can work at any speed before we start deleting from R2.”
No proof or deletion authority exists merely because this code is merged.

The sole production controller coordinates every backend deployment, client
release and selector change after fresh independent operational review. Deploy
only after the final copy-v2 change is integrated, exact-head Sol/Astra approval,
and green CI. Preserve cleanup-v7, copy-v2, the immutable 0187 writer fence,
`NAS_STITCH_ENABLED=false`, and `NAS_STITCH_DELETION_ENABLED=false`.

## Scope and custody

Only connection 13/account 47, stream 17208/recording 444, clips 373218 and 373262
are admitted. The approved task-1 source-plan SHA is
`0dfa6b172f33bd55e409729ecd517d492cdee03427b028ac226788e530a72726`.
The pair is 22,620,379 bytes and approximately 120 media seconds. Every input
must already exist with its exact whole-file SHA, size, physical identity,
sidecar binding and pinned keep28 window. No source download or source write
occurs. A missing source is HOLD, not permission to use R2 or copy data.

Output is in a new sibling namespace under the NAS output mount:
`100042_Europe_Czech_republic_Most_Public_square_089_Market_square_V85r_proof_keep28_v2/444/2026-08-09T09-00-00Z/proof-0dfa6b172f33bd55/`.
`proof.mp4` and `receipt.json.gz` are fsynced and published with Linux amd64
`renameat2(RENAME_NOREPLACE)`. Unknown entries, symlinks, aliases or conflicting
final bytes cause HOLD. Only private scratch created in this owned namespace is
removed. No quarantine, global alias ledger, task-1 journal, lease, output
catalogue insert, retirement permit, or R2 media write is used.

The full current V85r runner must nominate this seam as a real join. Singleton,
split, HOLD, repair, missing input and left-out results cannot pass. The exact
pinned media tools perform source EOF decodes, remux, whole-output EOF decode,
independent seeked seam decode, packet payload/order/timing conservation,
decoded pixel/PTS conservation, hidden edit-list comparison and codec identity
checks. Inputs and output are reopened and hashed after publication. The local
receipt preserves all 13 native audit files and 15 measured command results.

## Controller sequence

1. At an agreed natural boundary, prove cleanup completed recovery/retirement,
   runner stopped, global idle and deletion master OFF. Do not signal/pause a
   writer to manufacture this boundary. Keep ordinary stitching OFF and the
   old task-1 owner/journal obligations unresolved unless primary proof exists.
2. Release only the exact reviewed merged client source using the existing
   release mechanism with the distinct immutable label `v85r-nas-proof-v2`.
   The release artifact substitutes that label for the development placeholder;
   record its actual SHA/size and deployment source identities. The controller
   alone stages/promotes it. Keep the proof selector 0 through deployment.
3. Confirm fresh connection-13 client/version/boot/cleanup diagnostics, no owned
   deletion authority or recovery failure, current scope expiry, exact policy,
   corpus and tool hashes, and resource reserve. Re-review the exact activation
   operation. The controller may then set only `NAS_STITCH_PROOF_TASK_ID=1`.
4. Capture GET `/api/v1/service/nas-stitch/proof` on Jelly with its existing
   service token and `User-Agent: stoarama-operator/1`. This is read-only and
   does not claim or renew task 1. Stop on any binding drift or cleanup need.
5. Read existing status and filtered diagnostics. Never start a second worker,
   reconcile the old task, clear a fence, or rerun a successful output to refresh
   its timestamp. There is no NAS shell requirement.

The existing supervised NAS client owns scheduling/restart recovery. Neither
Mac is part of its runtime. It yields to the local cleanup execution fence;
remote cleanup freeze is checked every five seconds during media work.
Heartbeat OFF/update latches cancellation; native process groups are stopped
within the existing 0.5-second watchdog interval after cancellation is observed.
Interrupted private work can be rebuilt; published media is adopted only if
byte-identical. Immutable receipt adoption rechecks the same source/output and
the actual unchanged client, package members, and media tools. It does not
change original run identity or timestamps.

Resource rules are unchanged: nice 19, idle I/O, one codec thread, at least
384 MiB available memory including cgroup headroom, load <=1.5 times effective
CPUs, mounted NAS root and configured free-space reserve plus 2.2 times input.
The owned proof directory is capped at 128 MiB by admission/checkpoint census.
Corpus and tool installation each have 15-minute bounds; native work must finish
within its original 15-minute admission minus 30 seconds. These are cancellation
bounds, not a throughput promise. New work gets no lower threshold.

## Transport and offline replay

The canonical gzip local receipt is <=1 MiB compressed /16 MiB decompressed.
The client emits at most 64 chunks/minute, each <=512 binary bytes encoded as
1024 hex characters, through the existing seven-day client-log endpoint. A maximum-size cycle needs
32 successfully acknowledged batches at a one-minute cadence, plus readback and
HTTP work; cleanup holds or outages can extend this. Each chunk carries plan SHA,
receipt SHA, index and census. The cursor advances only after the server confirms
that every batch line was stored. Lost ACKs retry identical chunks; restart,
background ring eviction and repeating complete cycles cannot skip delivery.
The regular diagnostic ring is not a proof transport queue. Missing/conflicting/truncated/redacted chunks fail.
No redaction bypass exists. Preserve the receipt outside log retention promptly.

GET `/api/v1/service/nas-connections/13/client-logs?since=1h&limit=10000&grep=V85r%20proof-v1`
returns the lines for operator collection. Use bounded reads on Jelly; credential
contents never leave it. Public reviewed source artifacts and credential-free
evidence may be saved in the assessment records on Mooney.

The supported verifier is `../verify_stitch_proof.py`. `collect` uses the same
plan/client/SHA arguments as replay plus `--logs <GET-response.json> --out <new-dir>`
in place of `--bundle`; it exports exact receipt, local registration, source plan,
client source, verifier source, and a bounded server-validation request. There
is no self-approved attestation. Send exported `result.json` to the read-only
`stoaramactl nas-stitch verify-proof --input` command **on Jelly**, using existing
credentials, then save the response. The server requires a matching actual NAS
readback within two minutes, with current boot/run/client/receipt/output identity.

`export-payload --artifact-dir <collected-dir> --server-response <response.json>
--pipeline <release-identities.json>` exports schema-bound payload and validation
files. Pipeline identities are repository, base/reviewed/merged commit SHAs,
client version and actual release-artifact SHA. A fresh independent reviewer must
review the real evidence and bind payload SHA, native bundle SHA and exact code
head. Only then assemble the outer attestation with those file/SHA/size refs.

The R2 gate snapshots and verifies all artifact bytes before starting Python.
It supplies the exact checksum-pinned verifier source as the `-c` argument;
neither the verifier nor its inputs are reopened by pathname. The fixed argv is:

```
python3 -I -S -B -c <verified verifier source string> replay-stream --expected-client-sha256 <pipeline.client_artifact_sha256> --expected-sha256 <native_bundle.sha256> --expected-plan-sha256 <source_plan.sha256>
```

Stdin is one strict JSON object, at most 16 MiB, with exactly `schema` (literal
`stoarama.nas-stitch-replay-input.v1`), `client_source_b64`, `source_plan_b64`,
and `native_bundle_b64`. Values are canonical base64 of the already verified
component bytes: client at most 8 MiB, raw plan at most 1 MiB, gzip at most 1 MiB.
All three hashes are checked again before native replay. The raw plan file SHA
in argv is distinct from its logical proof-plan digest checked by the native
validator. The verifier source is at most 64 KiB UTF-8 without NUL, keeping the
inline argument below Linux and macOS argument limits. Its source hash is checked
by the gate before execve, using a trusted reviewed pin; artifact self-report is
not a code pin. Supply no inherited credentials and enforce bounded runtime and
stdout/stderr. There is no FD path, temporary-file or pathname fallback.

This mode reads only the bounded stdin byte stream. It imports no NAS app, accesses no
credentials, writes no files, downloads no media, starts no subprocess and
activates no NAS work. It executes only checksum-pinned pure inventory functions
from the exact reviewed client source. Success stdout is one JSON object with
schema `stoarama.nas-stitch-native-replay.v1`, `native_replay: passed`, connection,
run, receipt/plan/client/output hashes, output size, media seconds, build wall
seconds and recording-hours-per-wall-hour. Invalid evidence exits nonzero.
`collect` and `replay-bundle` remain operator tools; the R2 gate uses only
`replay-stream`. This change requires a new reviewed verifier pin. The frozen
outer/inner artifact schemas and replay stdout fields are unchanged.

`verified-receipt.json` is the complete decoded native receipt, including audit
inventories, at most 16 MiB. The gate requires exact byte equality with the
validated decompressed native bundle; it is not a small metadata-only file.

Frozen schemas in this directory are byte-identical to the assessment records.
The R2 consumer must reject synthetic fixtures, unknown schemas/fields, duplicate
JSON keys, unsafe paths or changed hashes/sizes. Fresh validation/readback and
independent review expire after 24h at R2 execution (<=5min future skew). Pipeline,
tool or policy drift requires a new proof. This prerequisite grants no custody,
Keep All, delivery-set, fence, drain or per-object eligibility exception.

## Packet conservation and edit lists

The v1 attempt failed because it compared raw packet-listing bytes. A valid
single edit translated every PTS/DTS by1024 ticks without hiding any packet.
V2 preserves both raw listings and directly records the pinned input/output
MP4 clock, track, handler and elst payloads inside the existing JSON audits.
The shared native/offline validator requires one nonempty unit-rate edit, the
same constant PTS and DTS offset on every packet equal to media_time, and full
packet count/order/size/payload/duration/flags conservation with edit-duration
coverage. Empty/multiple edits, missing evidence, cuts or drift are HOLD.
Full frame/EOF/seam decode and all prior source/publication checks remain.
Audio and non-H264 admission remains unchanged; no audio is dropped to pass.
The outer/inner schemas and replay-stream protocol are unchanged; V2 still
requires newly reviewed client/verifier pins and a new real proof. The failed
v1 namespace remains preserved and conveys no success or deletion authority.
