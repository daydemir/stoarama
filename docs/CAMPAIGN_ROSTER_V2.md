# Campaign roster v2

Status: frozen design contract. This document is normative for a future
schema/API implementation. It does not create, seed, or mutate a production
campaign roster.

The abandoned seed in [PR #172](https://github.com/daydemir/stoarama/pull/172)
was closed without merge because its hard-coded two-track model and evidence
binding could not satisfy this contract. The live database had no campaign
tracks or scene attestations when this design was frozen. The strict
qualification cohort and the stitch evidence merged in
[PR #185](https://github.com/daydemir/stoarama/pull/185) remain separate axes.

## Goals

Campaign roster v2 provides durable operational truth for:

- a 30-recording delivery campaign;
- a 21-slot repair plan that may honestly contain 17 admitted recordings and
  four prospective sources;
- ordered reserves and dynamic swaps;
- a separate long-term strict-50 track;
- current protection, evidence, checkpoint, and immutable decision history.

It replaces conversation memory and checked-in membership lists. Repository
catalogs and exported reports are evidence snapshots, never live roster truth.

## Non-goals and hard boundaries

- A roster count is not a delivered-footage or qualification count.
- Scene evidence is not current media health, NAS presence, decode, stitch, or
  strict-window qualification.
- A historical failure never silently becomes a current incident.
- A prospective source is not an admitted recording.
- Campaign retirement, replacement, or removal is never deletion authority.
- No campaign state automatically deletes, releases, re-records, reroutes, or
  mutates clips or NAS media.
- No seed may run until an exact reviewed manifest and all required evidence
  rows exist.

## Campaign definitions

Each campaign has an immutable definition after activation:

- account and server-owned campaign kind;
- policy version, deadline, target count, and grade floor;
- lifecycle state and roster revision;
- server-owned selection group and overlap policy;
- canonical definition and current-roster digests;
- creator and database timestamps.

Allowed lifecycle states are machine-exact, not caller-defined text:

```text
draft -> planning -> active -> complete -> retired
  \-------------------------------> retired
```

Campaign-kind rules determine which transitions are allowed. Definition fields,
target denominator, deadline, policy, and selection group cannot change after
the first active/frozen transition. Completion revalidates all counts and
evidence gates. Retirement is audited and does not erase facts.

### Kind and denominator matrix

| Kind | State allowed for operation | Exact denominator rule |
|---|---|---|
| `delivery30` | active, complete | exactly 30 current filled primary slots in `protect` or `probation` |
| `repair21` | planning | exactly 21 current plan slots; initial reviewed policy may be exactly 17 filled and 4 prospective |
| `repair21` | active, complete | exact policy-defined filled denominator; prospective/vacant/replace/removed never count |
| `reserve` | planning, active | policy-sized ordered backup slots; filled and prospective counts reported separately |
| `strict50` | active, complete | exactly 50 filled members at freeze; separate strict-window policy |

Any later change to the initial 17-plus-4 policy requires a new policy version,
not a mutable target. A denominator cannot shrink because a member fails,
pauses, is replaced, or leaves the current head.

## Append-only slots and current heads

A campaign owns stable slot ordinals. Each immutable slot version contains:

- campaign, ordinal, campaign revision, role, rank, and disposition;
- exactly one subject shape: filled recording, prospective binding, or vacant;
- exact decision evidence and reason codes;
- `effective_from` and optional `superseded_at`;
- actor and canonical request/decision digests.

Allowed roles are `primary`, `backup`, `repair`, and `reserve`. Allowed current
dispositions are `protect`, `probation`, `replace`, `removed`, `prospective`,
and `vacant`, constrained by subject and campaign kind. A filled subject is
required for protect/probation. Prospective and vacant subjects cannot satisfy a
delivery or qualification denominator.

The current head is not an unrestricted mutable pointer. Database constraints
and deferred triggers enforce:

- one head per campaign and ordinal;
- a head belongs to the same campaign and ordinal as its predecessor;
- monotonically increasing slot version and campaign revision;
- every head advance has a matching append-only event in the same transaction;
- no direct subject, status, rank, role, pointer, reparent, delete, truncate, or
  orphan-version bypass;
- an atomic swap preserves exact campaign counts at commit.

History remains queryable after replacement. The board reports original
failures, member entry and supersession times, expected windows available since
entry, and the replacement member's own new streak. A replacement never
inherits or erases the predecessor's streak.

## Subject and evidence shapes

### Filled recording

A filled slot binds the exact:

- account, recording, current stream, and authoritative current source snapshot;
- canonical physical-scene identity;
- `recording_scene_frame_evidence.id`, evidence SHA, successful frame/media
  object identity, verifier, capture and verification times;
- optional exact decision-time health job/calculation, pre-open, NAS, and stitch
  evidence IDs and fact digests.

Use the existing composite identity of scene evidence—ID, account, stream, and
scene hash—not `count(evidence) == 1`. Repeated attestations are valid when the
manifest selects one exact immutable row and the canonical physical scene is
identical.

The mutation transaction locks and verifies that the recording is active,
belongs to the account, still points to the attested stream and authoritative
source snapshot, and has a fresh successful frame and member scene attestation.
It explicitly rejects recording/stream/source divergence. Provider IDs, source
IDs, frame hashes, and URLs are not physical-scene identity.

### Prospective source

A prospective slot binds an immutable candidate-evidence row containing an
official public page, exact authoritative source identity, current successful
frame evidence, canonical physical scene, provider/failure domain, evidence
time, and canonical evidence digest. It has no recording ID and is never
qualified or protected as a recording.

Admission creates a new filled slot version with a real recording and fresh
exact evidence. It never mutates a prospective subject in place.

### Freshness semantics

Freshness is policy-specific and evaluated using database time when a decision
is accepted. Persist both `accepted_at_decision` and the exact evidence times.
Evidence aging later:

- never silently unprotects or removes a filled member;
- appears separately as `evidence_current = false` on the board;
- requires a new immutable evidence row for swap-in, reactivation, admission,
  or strict qualification freeze.

Scene freshness never substitutes for current media health, pre-open evidence,
NAS presence, byte decode, or stitch proof.

## Cross-campaign occupancy and overlap

Disjointness is enforced through transactionally locked occupancy registry rows,
not a cross-table partial unique index. Registry keys include:

- `(account, server-owned selection group, recording_id)`; and
- `(account, server-owned selection group, canonical physical_scene_sha256)`.

Transactions lock keys in deterministic order. This prevents two concurrent
swaps from occupying the same recording or physical scene under different
provider/source IDs.

Overlap policy is a server-owned enum and pairwise allowlist. Callers cannot
invent a text value that bypasses disjointness.

- Delivery, repair, and reserve selection are disjoint under their shared
  operational selection group.
- Strict50 may overlap delivery/repair only through the explicit server policy.
- An overlap never weakens protection; the protected view is distinct by
  recording.

If a backup relationship needs the same recording as a primary, model it as a
relationship inside the owning campaign rather than duplicate occupancy in a
disjoint campaign.

## Protection is a veto only

The stable protected-recording view includes only current filled recording
heads in `protect` or `probation` on active or complete campaigns.

- Prospective, vacant, replace, and removed slots are not protected.
- A paused recording in a protect/probation slot remains protected. Pause is an
  operational state, not disposal approval.
- Complete campaigns remain protected until an audited retirement.
- Retired campaigns do not contribute a campaign veto.

Removing that veto never authorizes deletion. Any cleanup still requires its
own immutable exact file manifest—connection, confined path, size, current
SHA-256 and recoverability evidence—plus explicit one-time Deniz approval
through the supported operator channel, final path/byte/mount/backend
revalidation, and the cleanup product's audit and rollback rules. There is no
automatic NAS delete.

Cleanup candidate creation records the current campaign/protection revision or
locks the occupancy. Finalization runs serializably, locks the recording and all
campaign heads/occupancy, and rejects any intervening protection or revision.
Abandoned candidates expire without mutation. Tests must prove retirement alone
cannot release or delete media.

## Authenticated mutation contract

All writes use an authenticated operator API with a CLI wrapper. The server
derives the actor from the principal and decision time from the database.

The workflow is plan/apply:

1. Dry-run accepts typed stable fields and returns the canonical plan, current
   campaign revision, and plan SHA-256.
2. Apply requires the expected revision, exact plan SHA, and an idempotency key.
3. A serializable transaction locks campaign, current heads, occupancy,
   recordings, streams/sources, and exact evidence rows in deterministic order.
4. It exact-compares the entire manifest with no missing or extra slots, writes
   immutable versions, advances heads/revision, updates occupancy, and appends
   events atomically.

An identical replay returns the stored response. A changed replay conflicts.
Idempotency is unique by account, campaign, and key, and stores request and
response hashes. Canonical encoding uses typed ordered fields and normalized UTC
times; unordered JSON maps and implicit timezone formatting are forbidden.

Actor IDs remain in audit history even if membership is later removed. Composite
account foreign keys cover campaign, slot versions, evidence, recording,
stream/source, occupancy, and idempotency records.

### Audited swaps

A swap names exact old and new slot/head versions, subject/evidence identities,
reason codes, and expected campaign revision. It writes the successor slot
version, predecessor supersession time, occupancy release/acquisition, campaign
revision, and one logical decision event in the same transaction. Deferred
checks require the exact denominator at commit.

## Immutable audit

Every logical decision event stores:

- a stable decision UUID and campaign revision;
- exact before and after slot-version IDs;
- subject, role, rank, disposition, and canonical physical scene;
- exact evidence IDs and digests;
- bounded allowlisted reason codes;
- actor and database decision time;
- canonical request/plan digest and idempotency identity.

Events and slot versions reject update, delete, truncate, reparent, and direct
head bypass. Current heads are a derived operational index over immutable
versions, not the historical authority.

## Read board semantics

The account/operator board reports, separately:

- target, filled, prospective, vacant, replaced, and delivered/qualified counts;
- current protection and disposition;
- accepted-at-decision evidence and current freshness;
- current live/operational health versus historical completed-window risk;
- strict expected-window streak since this member's entry;
- current NAS presence, byte/decode proof, native-run concat, video/audio
  continuity, and whole-window continuity as independent axes;
- deadline, remaining authoritative occurrences, risk, and ordered backups;
- pre-open early/confirm and first-three-clip checkpoints.

Schedule and checkpoint facts come from the shared scheduler and frozen
timezone/DST occurrence evidence. Missing pre-open, first-three, health, NAS, or
stitch evidence remains independently unknown. A current stream, fresh scene,
or filled roster does not imply qualification readiness. The report never says
“30 delivered” merely because 30 roster heads exist.

## Required implementation staging

Implementation is separate from this docs PR and should be split into bounded
reviewable changes:

1. Campaign/slot-version/event/occupancy/idempotency schema and direct database
   invariants, with no seed.
2. Read-only board derived from empty/live schema and exact checkpoint evidence.
3. Authenticated plan/apply and atomic swap API/CLI, still with no production
   manifest.
4. Prospective candidate evidence/admission integration.
5. Cleanup consumers changed to use the veto at candidate creation and final
   authorization, without adding deletion authority.
6. A separately reviewed exact manifest and member scene attestations, followed
   by explicit seed authorization and production verification.

No conversation-derived list is seed authority. No stage may mutate production
membership merely because its code merged.

## Minimum test matrix

- Fresh delivery30 and repair21 planning with exactly 17 filled plus four
  prospective slots; strict50 freeze with 50 filled.
- Kind/state/role/status/subject negative matrix and immutable denominator,
  deadline, policy, and selection group.
- Exact repeated scene-attestation row succeeds; stale, foreign, mismatched,
  source-divergent, provider-hash-as-scene, and cross-tenant evidence reject.
- Two-connection same-recording and same-physical-scene occupancy races;
  allowed strict50 overlap and forbidden delivery/repair/reserve overlap.
- Direct pointer rewrite, skipped revision, orphan version, reparent, update,
  delete, truncate, unauthorized actor, and lifecycle bypass reject.
- Fresh apply, exact replay, changed replay, partial second-campaign rollback,
  concurrent swap/revision conflict, exact target preservation at commit.
- Prospective and vacant never protected; paused filled stays protected;
  replace/remove and retirement remove only campaign veto; complete remains
  protected; evidence aging never silently unprotects.
- Replacement preserves predecessor failures/times and starts the new member's
  streak at its own authoritative occurrences.
- Cleanup candidate/finalization race rejects new protection; candidate expiry
  mutates nothing; campaign retirement alone cannot release/delete.
- Board uses frozen DST/timezone occurrences and preserves independent unknown
  pre-open, first-three, health, NAS, stitch, and qualification axes.
