# Deploy Stoarama On Render

## What this phase includes

- fresh Render Postgres
- `stoarama-api` service from `render.yaml`
- fresh Stoarama R2 bucket wired via env vars
- email left in `log` mode until domain and Resend are connected

## render.yaml is not a mirror of live env

`render.yaml` is per-service and is NOT a complete picture of what a given service reads. All 20 `DROPLET_POOL_*` keys are declared only in the `stoarama-recorder-control` block; the `stoarama-api` block declares none, even though the API enforces the droplet admission ceiling from `DROPLET_POOL_MAX`/`DROPLET_POOL_CAPACITY`. Before reasoning about production behavior, read the live env vars from the REST API (recipe in `AGENTS.md`, "Render Operator Access") rather than inferring them from the blueprint.

Live services:

- `stoarama-api` — `srv-d6usqn94tr6s73d94cd0`, web, admission ceiling
- `stoarama-recorder-control` — `srv-d8vcdspo3t8c73far4e0`, worker, droplet pool scaler
- crons `stoarama-recording-health`, `stoarama-relay-connectivity`

## Merging capture code does not deploy it

A Render deploy ships the API and the control worker. It does NOT ship the code
that actually records, which runs in two places that update on their own terms:

- **Cloud recorders.** Each droplet clones `DROPLET_POOL_REPO_REF` (live value:
  `main`) and builds at provision time, so a droplet runs whatever `main` was
  when it was created and never updates in place. `DROPLET_POOL_MIN=0` and a
  600s idle grace mean a droplet holding zero leased jobs is drained and
  destroyed, and the next one builds current `main` — so a capture fix reaches
  cloud only after every existing droplet has recycled. Check `recorder_droplets`
  `created_at` against the merge you care about before claiming a fix is live.
- **Relays.** Enrolled Macs and Pis self-update from the signed manifest at
  `https://stoarama.com/relay/download/latest.json`, so a capture fix reaches
  them only when a new relay release is published and promoted.

Both cost a capture interruption when they roll: a droplet forced past
`DROPLET_POOL_DRAIN_TIMEOUT_SEC` (600s) drops its live windows, and a relay
restart cost 5-9 minutes on nine streams during the `6c25bbb` rollout on
2026-07-31. Never force either while chasing a cosmetic fix.

## Relay fleet gotchas learned the hard way

- **Relay temp cleanup is not crash-safe.** A reboot, kill, or updater exit leaves whole
  `capture-continuous-*` directories behind, and the startup scavenger is the only thing that
  removes them. `deniz-mini-r` was driven to ~1 GB free this way. Note `capture_temp_bytes` in
  node health counts only recognized live dirs, so it can read 0 while orphans fill the disk —
  do not conclude from it that Stoarama is not the cause. Any scavenger must delete only
  recognized Stoarama capture dirs, never arbitrary temp files.
- **Never delete a segment until ingestion is acknowledged**, and retry only explicitly
  retryable errors. Treating "any 409" as success loses segments.
- **Do not count machines behind the same uplink as independent relay-group capacity.** Two
  ungrouped MIT machines once let the backend schedule up to 9 concurrent streams onto a single
  uplink.
- **Capture percentage is not a footage measure.** It is clips received / clips expected, so a
  2-second clip counts as a whole clip and overlapping clips can exceed 100%. Use it as a
  liveness signal, not as "how much video do we have".
- **Some relay binaries cannot self-update.** Builds predating the embedded signing key (e.g.
  `85c91b6e`) deliberately disable self-update and cannot pull signed releases on their own.
  Avoid shipping service-unit directives that older Pis cannot parse — that strands them.

Relay recordings (`capture_via != 'cloud'`) are excluded from the droplet forecast (`backend/internal/dropletpool/forecast.go:71`), so the cloud ceiling never blocks relay-only streams. Do not report relay work as pool-blocked.

## Required env vars

- `R2_ACCOUNT_ID`
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- `R2_BUCKET`
- `BOOTSTRAP_ADMIN_EMAIL`
- optionally `R2_ENDPOINT`
- optionally `SERVICE_TOKEN` for machine-to-machine runtime paths before node/service enrollment is expanded
- optionally `APP_BASE_URL` if the Render hostname differs from the blueprint default
- `DROPLET_POOL_MAX` and `DROPLET_POOL_CAPACITY` — must be set **identically on both** `stoarama-api` (`srv-d6usqn94tr6s73d94cd0`) and `stoarama-recorder-control` (`srv-d8vcdspo3t8c73far4e0`). recorder-control scales the pool; the API enforces the admission ceiling as `DROPLET_POOL_MAX * DROPLET_POOL_CAPACITY` (`backend/internal/api/server_recordings_batch.go:320`, `server_recordings.go:2559,2629`). If absent on either service, that service silently falls back to `config.go` defaults MAX=5 x CAPACITY=1 = 5, so the API rejects batch schedules against a phantom ceiling far below what the pool can really run. Read the live values from the REST API rather than trusting a number written here; a figure in docs is wrong as soon as an operator changes it.
  - The C-cap guard requiring `DROPLET_POOL_CAPACITY == RECORDING_WORKER_CONCURRENCY` (`config.go:364`) is gated behind `DROPLET_POOL_ENABLED`, which is unset on `stoarama-api` — so setting CAPACITY there does not require also setting `RECORDING_WORKER_CONCURRENCY` or enabling the pool on the API service.
  - Set via the REST API (see `AGENTS.md`, "Render Operator Access"); the Render CLI cannot do it.

## Recommended initial values

- `AUTO_MIGRATE=true` for first boot, then optional to turn off after schema is up
- `EMAIL_PROVIDER=log`
- `BOX_WORKER_EMBEDDED=true`

## First boot checks

After Render deploys:

1. open the service URL
2. verify `/account` loads
3. request a magic link for `BOOTSTRAP_ADMIN_EMAIL`
4. verify that signed-in bootstrap account can open `/dashboard`
5. verify the API process starts without migration/runtime errors

## What comes next

After Render is live, the next phase is:

- Cloudflare DNS for `stoarama.com`
- `api.stoarama.com`
- Resend domain verification
- live stream/artifact migration
