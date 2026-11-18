# B1 non-keep28 key-policy proof

This is an explicit alternative to accepted-PUT completion for **B1 non-keep28
managed RAW keys only**. It makes no claim that external requests have completed.
B2, delivery, collated outputs, unknown keys and NAS-custody decisions cannot use
it. No production action is performed by this change.

## Safety argument and accepted residual

`inverseProtection.classify` in `backend/cmd/stoaramactl/r2_inverse.go` uses the
key's catalog stream, path, period, timezone and protection sets. KE stream and
folder/path protections and keep28 overlap do not inspect object bytes, ETag or
SHA. All aliases must establish eligibility; ambiguous/missing identities hold.
`loadInverseRawTarget` reloads the unique RAW row under lock and reclassifies it;
epoch, hold and delivery checks run again through wire authorization. An external
PUT alone cannot change these catalog facts or turn that key into a KE/keep28
catalog identity. This argument concerns the approved catalog identity, not a
claim that arbitrary bytes can never be placed under the wrong key.

The delivery exclusion **does** include SHA/size aliases (`inverseDeliverySQL`,
`r2_inverse_delivery_held`). An overwrite before listing could leave stale catalog
SHA and evade that content-alias exclusion. Root explicitly accepted that
delivery-order residual against Deniz's words: **"i'm ok with breaking public links"**.
Known delivery keys/dependencies still veto; delivery-last order is not removed.
KE and keep28 protections remain mandatory. Public global-delivery tickets and
streamed files bind the recorded ETag through `HeadExact`,
`PresignGetExactDownloadRequest`, `OpenExact`/`OpenExactRange` in
`backend/internal/api/server_global_delivery.go`; a changed generation cannot be
served as the recorded generation. This is not a universal claim about every
historical public URL or client.

Final HEAD must match partition ETag/size/LastModified and catalog ETag/size/SHA
metadata when available. LastModified must precede partition start and be older
than seven days. This refuses an overwrite observed before final HEAD. R2 DELETE
is still unconditional: a PUT can commit **after that HEAD and before DELETE**,
or reappear after DELETE. The accepted B1 residual is deleting a different
generation at the same already-approved key, temporary inaccurate absence, extra
storage/work, or delivery content being removed early. It is not exact atomic
compare-and-delete. Later reconciliation never retransmits an issued DELETE;
reappearance remains held by the permanent intent/reservation and needs a separate
reviewed cleanup route. Nothing here authorizes automatically deleting it again.

## Writer inventory at production base 29de4531

Paths are relative to the repository. Numbers below refer to that base.

| PUT family | Source | Mutability / scope |
| --- | --- | --- |
| Recording/legacy RAW presign | `backend/internal/api/server_recording_clips.go:896` → `backend/internal/r2/client.go:329` | Ordinary PUT, no checksum/length/no-overwrite condition. Deterministic recording path at :817, replay at :841. |
| Capture, boxed, thumbnail presign | `backend/internal/api/server.go:1727` → same `PresignPut` | Ordinary PUT; capture/thumbnail paths can repeat. |
| Sized presign | `backend/internal/r2/client.go:282` | Signed size, no no-overwrite condition. No non-test call in this base. |
| Joined publish/artifacts | `backend/internal/api/server_recording_joined.go:847` → `client.go:240` | Signed checksum/size and `If-None-Match: *`. |
| Space-audit presign | `backend/internal/api/server_nas_space_audit.go:174` → `client.go:236` | Create-only signed metadata; excluded `nas-space-audits/`. |
| NAS restore | `backend/internal/api/server_nas_restore.go:43` | Retired: lease returns no tasks/capabilities. Historical client uploader at `clients/nas-pull/stoarama_pull.py:51621`; historical capabilities still require TTL bound. |
| Direct bytes | `client.go:357` | Ordinary PUT. Callers: `internal/inferencebox/manager.go:246`, `internal/survey/survey.go:270`, `internal/api/server_imports.go:242`, `server.go:3444`, `cmd/stoaramactl/main.go:3462,4021`, `collation_v2.go:170` (all under `backend/`). |
| Direct reader | `client.go:381` | Ordinary PUT. `backend/internal/api/server.go:2680` ZIP. |
| Direct multipart/small single PUT | `client.go:493` | Ordinary upload. `backend/internal/api/server_recording_export.go:676`, `server_day_zip.go:523`, `cmd/stoaramactl/recorder_transfer.go:279`. |
| Immutable direct publication | `client.go:409` | `If-None-Match: *`, exact read-back/hash; `backend/internal/collation/runner.go:73,85`. |
| Repair signer | `ops/jelly/repair/presign_remote.py:9-21` | Ungated operator, ordinary PUT, 7200s, `repaired/` excluded. |
| Release/probe/operator tools | `ops/jelly/r2-retire/HEAD-GUARD.md` writer inventory | Raw credentials can sign/write without DB admission. Known prefixes stay excluded; every other holder needs controller stop/coverage evidence. |

Global key immutability is false and is not a premise. In `writer_admission.go`,
DB-admitted direct operations have durable terminal records; presigns only record
expiry. All current Go PUT presigns require positive TTL, bounded by seven days.
The cap now also applies to explicitly ungated clients. Their signing contexts
are one minute and refuse results after context expiry. Migration 0187 serializes
admission with fence activation and refuses new admitted writes/capabilities;
0198 still checks durable wire intents and live capabilities. Two minutes are
the existing issuance/signing margin, **not a provider transfer-duration bound**.
Historical effective `R2_SIGN_PUT_TTL` is unknown: the source default and
committed Render declaration are15m, while the proven DB-admitted hard cap is
7days. `upload_intents` is deletable by30-day transient-ledger maintenance;
capability rows have no delete/truncate immutability guard. Neither NULL current
maxima nor source defaults establish historical issuance. These are recorded
facts, not premises of B1 key-policy eligibility. B2 keeps its completion gate.

## Provider facts and supporting observation

[Cloudflare presigned URL docs](https://developers.cloudflare.com/r2/api/s3/presigned-urls/)
bound expiry to 1–604800 seconds. [ExpiredRequest](https://developers.cloudflare.com/r2/api/error-codes/)
is HTTP 403. Neither establishes an accepted transfer's maximum duration.
[R2 limits](https://developers.cloudflare.com/r2/platform/limits/) bound upload
size, not total transfer time. No R2 single-PUT acceptance-to-commit maximum was
established. This route deliberately does not need one.

[R2 analytics](https://developers.cloudflare.com/r2/platform/metrics-analytics/)
documents `r2OperationsAdaptiveGroups`, bucket/time filters, `sum.requests`,
operation, object and response dimensions. [Adaptive sampling](https://developers.cloudflare.com/analytics/graphql-api/sampling/)
precludes treating these counts as a complete accepted-request ledger. No maximum
publication lag or terminal completeness watermark was established. Observed
2026-11-18 08:24:51Z: nine reported successful PUTs in Nov17 08:00–Nov18 08:00Z,
all excluded `nas-releases/`: v4 script/latest-v4/latest.json at Nov17 10:17:36–48Z;
v5 equivalents at19:13:29–19:14:45Z; v6 at Nov18 02:15:41–02:16:20Z. These are
release-publication keys, not group-2 RAW. The dimensions do not identify the
human/process actor. Correlate the controller's release receipts before naming
one. Absence of another reported key is supporting evidence, not zero issuance.

[Event notifications](https://developers.cloudflare.com/r2/buckets/event-notifications/)
report creation/overwrite events, not a pending-accepted set; no hard maximum
delivery latency was established.
[Data Access Logs](https://developers.cloudflare.com/r2/buckets/data-access-logs/)
are asynchronous/best effort, may omit events, exclude errors >=400 and are not
retroactive; they cannot prove a complete terminal set.
[Logpush](https://developers.cloudflare.com/logs/logpush/)
batch delivery is near real time, without historical backfill or a guaranteed
complete accepted-request/terminal pairing. Enabling logging now does not repair
the missing historical admission ledger.

## Required post-fence artifact

The controller seals new evidence **after** the exact immutable deadline, never
restamping the old completion finding. `proof` is mandatory: either
`accepted_put_completion_v1` (substantive completion plus deployed-writer coverage),
or `non28_key_policy_v1` (B1 only). Missing, unknown or mixed routes fail closed.
For B1 leave `accepted_put_completion_evidence` empty; historical
`deployed_writer_coverage_evidence` is not a B1 prerequisite. Supply:

- Existing exact bucket/endpoint/activation/deadline and post-deadline observed_at.
- `key_policy.partition_sha256` and `summary_sha256`, matching this apply's pins.
- SHA-pinned file references `{path, sha256}` for `canary_evidence`,
  `managed_put_analytics` and `operator_signing_evidence`, available unchanged on
  the CLI host. Canary evidence establishes the sole29de API instance and fresh
  installed0209 ledger, both enabled source-hold removal guards, their exact
  unconditional55000 body and origin replication role, plus the live0198 gate.
- A fresh post-fence GraphQL query covering activation through canary, bucket
  stoarama / PutObject / managed/%, with zero reported groups/count. Error,
  truncation, unknown attribution or nonzero holds the next batch for root.
  Sampling/publication lag means zero is anomaly evidence, not completeness.
- Actual controller no-presign_remote use for the two hours before the canary
  observation: operator_signing_stopped_at <= observed_at−2h and
  operator_signing_observed_through equal to observed_at. No inference from
  historical operator issuance logs is required.
- `residual_acceptance` exactly
  `approved_non28_key_overwrite_reappearance_delivery_and_historical_issuance_v1`.

The controller reviews substance; a file hash verifies bytes, not truth. The
explicit residual includes missing continuous historical fence coverage,
per-instance retirement history and historical capability/T_max census. They
cannot change a key's KE/keep28 catalog class. Still-valid old capabilities can
race an unconditional DELETE at an already-approved key, evade a stale delivery
content alias or recreate it afterward: those are the expressly accepted B1
same-key/delivery-order/cost residuals. The immutable fence minimum remains;
expiry is not accepted-request completion. B2 cannot use this alternative.

Fresh canary inputs are required; the historical installation receipt or earlier
zero-reported analytics is not a fresh canary observation. Operator signing
windows are relative to the actual observation, not a hardcoded calendar date.
No B1 route deletes collated/delivery/unknown/KE/keep28 keys or bypasses any live
veto. Permanent intents still prevent retransmission after ambiguous deletion.
