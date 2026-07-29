# MIT SCL top-50 recording campaign

These eight strict `schedule-batch` specs cover 50 streams for 18 complete local
days, August 1 through August 18, 2026. Each records daily from 08:00 through
20:00 local time in 60-second clips with Plaza Hourly naming and NAS pull.

The files are intentionally non-executable templates. Their two destination
fields must resolve as follows:

- `dry_run` is `true`.
- `storage_destination_id` is the valid integer placeholder
  `9223372036854775807`. It passes local strict decoding but cannot identify an
  MIT SCL destination, so the account-scoped API rejects it.
- `delivery_storage_destination_id` is `0` and must remain `0`; NAS pull has no
  external delivery destination.

Set the MIT SCL managed staging destination ID in every file, run every cohort
with `stoaramactl recordings schedule-batch --spec FILE --dry-run --json`, and
retain each JSON response. Only after all eight dry runs pass should `dry_run`
be changed to `false`.

After scheduling, verify the combined recording IDs with:

```text
stoaramactl recordings campaign-postflight \
  --recording-ids ID,ID,... \
  --session-cookie-file /secure/path/stoarama-session-cookie \
  --backend-api-url https://stoarama.com/api/v1 \
  --api-token "$API_TOKEN"
```

Pass all 50 recording IDs returned by the live cohort runs. Run postflight
during a local 08:00–20:00 capture window so relay-backed recordings are
expected to be ready. It exits nonzero for missing/inactive recordings,
critical or unavailable capture health, missing relay readiness for any
relay-backed recording, non-Plaza naming, non-NAS delivery, an unhealthy NAS,
or a NAS backlog above `--max-nas-pending-clips` (zero by default).
