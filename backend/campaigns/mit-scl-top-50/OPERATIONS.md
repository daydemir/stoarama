# MIT SCL campaign decisions and source evidence

The database is the sole live truth for the `delivery30`, `repair21`, and
`strict50` rosters. Read the current board with:

```text
stoaramactl recordings campaign-tracks report
```

That command reads `GET /api/v1/account/recordings/campaign-tracks`, backed by
`recording_campaign_tracks`, `recording_campaign_roster_entries`, and the
append-only `recording_campaign_roster_events`. Cleanup and admission code must
use `protected_campaign_recordings`; it must not parse this directory.

`source-evidence.json` is a versioned scouting catalog. It records public source
bindings, provider failure domains, caps, and rejects. It intentionally does not
contain live HLS URLs, signed URLs, tokens, current health, or roster membership.
Prospective streams remain evidence only until a recording is created and an
audited roster event admits it.

## Evidence and grade boundaries

- A pre-open PASS proves only that the bounded source or intended relay path was
  playable for that check. It is an alerting preflight, not admission.
- GREAT/GOOD/ACCEPTABLE are timeline candidates from exact union coverage, gaps,
  and overlap counts. They do not prove native stitchability.
- Delivery claims additionally require exact NAS path, byte size, SHA-256,
  decode/timestamp validation, native signature runs, and lossless concat proof.
- Missing, stale, conflicting, duplicated, or late evidence is UNKNOWN. Do not
  average it away or shrink the denominator.
- Never delete footage automatically. Cleanup requires an immutable manifest and
  explicit Deniz approval through the supported operator channel. Never reencode.

## Delivery protection policy

Protected open windows are watched every one to two minutes. Preserve a fresh
capture even when a completed-window alert is open; historical alerts alone are
not restart or swap authorization.

Escalate and consider an audited roster swap when any of these occurs:

1. fresh ingest or media is absent for more than five minutes;
2. strict GOOD becomes mathematically impossible in the current window;
3. a new overlap appears;
4. the authoritative source binding is lost;
5. a sequence that already used its one ACCEPTABLE allowance receives another;
6. pre-open is FAIL or UNKNOWN and remains unresolved before the window opens.

A swap changes only roster priority. It does not pause, restart, delete, or move
a recording. Capture repair uses existing bounded controls and must protect other
jobs in the same relay failure domain.

## Provider diversity

Until clean completed windows demonstrate independence, admit no more than three
probationary shared YouTube scenes, three shared Skyline scenes, or two shared
Seattle StreamLock scenes. Do not bulk-admit SDOT. Admit SDOT feeds one at a time
only after fresh official binding, two separated native copy/decode probes, and
capacity-safe probation.

Scene identity is deduplicated by current-frame evidence, coordinates, name,
provider camera identity, and source ancestry—not by Stoarama stream or recording
ID. A replacement may be a better distinct scene; preserve the prior recording's
history and never claim that distinct scenes are visually identical.

## Generated snapshots

Operational reports may be exported from the campaign-tracks endpoint for an
incident or delivery handoff. Every export must include `generated_at`, campaign
deadline, role/rank/status/reason codes, and the source window/job/health evidence
IDs where available. Treat it as a time-stamped snapshot; query the database again
before acting. Never check a manually maintained `primary30` list into the repo.
