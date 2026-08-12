# Recording campaign tracks

The database is the sole live truth for delivery and repair rosters. Checked-in
catalogs and exported reports are evidence snapshots, never membership.

Active tracks:

- `delivery30`: 30 recordings required to produce GOOD/GREAT windows by
  2026-08-19 23:59:59 UTC. Rank controls protection order; history alone does
  not remove an otherwise usable, currently stable primary.
- `repair21`: the remaining 17 recordings plus four prospective additions,
  targeted for GOOD after repair/admission by 2026-08-26 23:59:59 UTC.
- `strict50`: the separate long-term objective of 14 consecutive expected
  windows under the strict timeline policy. Campaign delivery never implies
  strict-14 qualification, NAS-byte proof, or stitch certification.

`recording_campaign_roster_entries` stores current role, rank, disposition and
reason codes. Its trigger appends every decision to
`recording_campaign_roster_events`; events are immutable. Active `protect` and
`probation` rows appear in `protected_campaign_recordings`, the stable predicate
that cleanup/admission code must check at candidate creation and finalization.

Read the live board with:

```sh
stoaramactl recordings campaign-tracks report
```

The report exposes the next checkpoint: pre-open T-2h, pre-open T-30m, or first
three clips. Operations poll protected live recordings every 1–2 minutes during
an open window. A swap is an explicit audited roster decision; a historical
failure by itself is not a swap trigger. Prospective stream-only evidence stays
in the source catalog until a recording exists and is admitted.
