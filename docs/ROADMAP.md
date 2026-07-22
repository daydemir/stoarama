# Stoarama Roadmap

## Phase 1

Build everything that does not require Render, Cloudflare, or Resend intervention:

- bootstrap the new Stoarama repo
- expose only `stoarama-api`, `stoaramactl`, and `stoarama`
- keep the repo CLI-first
- keep Render deployment ready without custom domain
- keep auth in `EMAIL_PROVIDER=log` mode
- add account auth, API keys, and initial node enrollment
- preserve importer/migration seams for moving live streams from the old system

## Phase 1 Architecture

Primary surfaces:

- `stoarama-api`: app + API service
- `stoaramactl`: internal control CLI and acceptance surface
- `stoarama`: user/node CLI

Core identities:

- browser session
- user API key
- node token

Core platform objects:

- accounts
- API keys
- node enrollment tokens
- nodes
- streams
- pipelines
- runs/results with provenance

## Phase 2

After the repo is pushed and you deploy Render:

- create fresh Stoarama Postgres
- create fresh Stoarama R2 bucket
- deploy services from `render.yaml`
- verify app boot and migrations
- wire Cloudflare DNS
- wire Resend and switch email out of log mode

## Phase 3

Once Render, domain, and email are live:

- import only live streams from the old system
- migrate selected artifacts into the new bucket
- enroll first local recorder and inference nodes
- move active workloads from the old stack
- refine the web IA so it mirrors `stoaramactl`

## Operational TODOs

- [ ] Diagnose the prolonged `streetscore-2` outage after the relay rollout:
  correlate relay heartbeat attempts and recording load with backend heartbeat
  receipts and freshness/status calculations, and compare Streetscore 1/3. Keep
  this read-only until we distinguish relay/network loss from backend ingestion
  or display errors.
- [ ] Complete relay recovery observability before changing load limits or
  network assumptions: persist boot ID, process start/clean-shutdown markers,
  systemd service result/exit code/signal, last successful heartbeat/capture/
  upload/update timestamps, and a bounded relay error tail. The first
  successful heartbeat after recovery must atomically include the recovery
  metadata (`recovered_at`, outage class/duration, and bounded error tail),
  alongside backend receipt/rejection/latency metrics keyed by node (never
  credentials). Test process crash, reboot, OOM/signal, DNS failure, timeout,
  API rejection, and clean restart in an emulator or disposable relay before
  fleet rollout; it must not require inbound SSH.
