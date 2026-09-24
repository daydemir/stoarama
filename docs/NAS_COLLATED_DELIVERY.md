# Collated-only NAS delivery

Operators can switch a `nas_pull` recording to `collated_only`. From then on the
NAS stops pulling that recording's raw 1-minute clips; the raw clips stay in R2
until they are collated, and the NAS receives the recording only as joined hour
files. Every recording defaults to `raw`.

```sh
stoaramactl recordings nas-delivery set --account-id 47 --recording-ids 1,2,3 \
  --mode collated_only --reason "collated rollout"            # dry run
stoaramactl recordings nas-delivery set ... --apply            # change it
stoaramactl recordings nas-delivery status --account-id 47     # held backlog
```

The API is admin-only: `POST/GET /api/v1/recordings/nas-delivery-mode`.
It refuses recordings from another account, `managed` recordings, and
`yearly_prepaid` recordings.

## Per-clip hold

The mode is frozen onto each clip when it is inserted, as storage billing
contract mode `nas_collated_hold` (`clip_storage_billing_contracts`, migration
0158). Consequences:

- Only clips recorded after the switch are held. Clips already offered to the
  NAS keep flowing raw, and held clips stay held if the recording switches back.
- The NAS raw feed (`/account/clips`), connection pending counts, the native
  stitch backlog gate, and NAS inventory coverage all skip held clips
  (`nasdelivery.NotHeld*`). A held clip is never NAS backlog.
- Held clips are never released and never hard-deleted by this feature. The
  joined-source purge still requires a verified external (NAS) copy, so held raw
  stays in R2.

## Billing

Account 47 has a Stripe subscription with metered `recording_hour` and
`stream_hour_month` prices. Managed-storage metering counts unreleased clips;
a `nas_pull` clip is free only for its 24h staging grace. A held clip is
unreleased indefinitely, so without the hold mode it would be metered as
managed storage. The Stripe facts (`reconstructStorageDailyFacts`) and the
display snapshot (`snapshotManagedStorageSQL`) both exclude
`nas_collated_hold`. Recording-hour metering does not depend on delivery and is
unchanged. Because the hold is a billing exemption, only operators can set it.
