# Stoarama

Stoarama is a CLI-first platform for stream capture, node enrollment, and provenance-aware inference results.

This repo is the new product/control-plane codebase. It intentionally excludes thesis-specific pipelines and research experiments, which should remain in the separate private `social-isolation` repo and consume Stoarama through APIs and CLIs.

## Repo Shape

- `backend/`: Go API, internal services, web UI, and CLIs
- `infra/sql/`: Postgres migrations
- `docs/`: deployment and migration docs
- `render.yaml`: Render blueprint for the first Stoarama deployment

Primary binaries:

- `stoarama-api`: main web/API service
- `stoaramactl`: internal control CLI used to build and test all product workflows first
- `stoarama`: user-facing machine/node CLI

## Product Direction

Stoarama v1 is:

- admin-curated streams
- individual user accounts with magic-link auth
- hosted capture with simple per-user quotas
- user-enrolled YouTube relay source nodes
- BYOC inference nodes
- metadata-registered pipelines
- private-by-default results with provenance

## CLI-First Rule

All product functionality must exist in `stoaramactl` before it is considered complete in the web UI.

Feature sequence:

1. API behavior exists
2. `stoaramactl` command works end-to-end
3. tests cover API + CLI behavior
4. web UI is added as a thin client over the same behavior

## Local Dev

### Prereqs

- Go `1.24+`
- Postgres
- Cloudflare R2-compatible bucket
- `ffmpeg` and `yt-dlp` on machines that run capture/relay flows

### Minimal env

```bash
export DATABASE_URL='postgres://...'
export API_TOKEN='dev-token'

export R2_ACCOUNT_ID='...'
export R2_ACCESS_KEY_ID='...'
export R2_SECRET_ACCESS_KEY='...'
export R2_BUCKET='stoarama-dev'
export R2_REGION='auto'
export R2_ENDPOINT='https://<account>.r2.cloudflarestorage.com'

export AUTO_MIGRATE='true'
export PORT='8080'

export APP_BASE_URL='http://127.0.0.1:8080'
export EMAIL_PROVIDER='log'
```

### Run API

```bash
cd backend
go run ./cmd/stoarama-api
```

### Run internal CLI

```bash
cd backend
go run ./cmd/stoaramactl --help
```

### Run user CLI

```bash
cd backend
go run ./cmd/stoarama --help
```

### Example user CLI flow

```bash
cd backend
go run ./cmd/stoarama auth request-link --email you@example.com
go run ./cmd/stoarama auth configure --api-base-url http://127.0.0.1:8080 --api-key sir_...
go run ./cmd/stoarama node enrollment-tokens create --node-type yt_relay_source
go run ./cmd/stoarama node enroll --token sie_... --node-type yt_relay_source
go run ./cmd/stoarama node doctor
```

## Render

Use the root `render.yaml` for the first Stoarama deployment.

Expected deployment shape:

- fresh `stoarama-db`
- `stoarama-api` web service
- fresh Stoarama R2 bucket configured through env vars
- email can remain in `log` mode until custom domain and Resend are wired

See [docs/DEPLOY_RENDER.md](/Users/deniz/Build/thesis/stoarama/docs/DEPLOY_RENDER.md) and [docs/ROADMAP.md](/Users/deniz/Build/thesis/stoarama/docs/ROADMAP.md).
