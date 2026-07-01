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
- user-enrolled local recorder nodes
- BYOC inference nodes
- metadata-registered pipelines
- private-by-default results with provenance

## Public Data API

A hosted instance runs at `https://stoarama.com` (browse it in a
browser at `/streams`). Stream metadata, per-stream sample frames, and recorded
clips are **public reads — no account or key required.** The endpoints below are
the canonical contract; the server also self-documents at
`GET /api/v1/data-access-spec`.

Presigned download URLs (`download_url`, `thumbnail_download_url`) point straight
at object storage and expire after ~10 minutes, so fetch them, then download.

```bash
BASE=https://stoarama.com
```

### Browse and filter the catalog

`GET /api/v1/dashboard/streams` returns `{ items, total, limit, offset }`, where
each item has a `stream` object plus capture/recording stats.

```bash
# Free-text search
curl "$BASE/api/v1/dashboard/streams?q=seoul&limit=20"

# Filters (combine freely): country (full name), capture_type, source,
# tags, recording_state=on, plus limit/offset (max limit 2000)
curl "$BASE/api/v1/dashboard/streams?country=South%20Korea&capture_type=hls&limit=50"
```

Filter facets (valid values) come from `GET /api/v1/dashboard/{countries,cities,sources,tags}`.

### Inspect one stream

```bash
# Full detail
curl "$BASE/api/v1/dashboard/streams/415"

# A few representative sample frames across the stream's history
# -> items[]: { day, captured_at, object_key, download_url }
curl "$BASE/api/v1/dashboard/streams/415/capture-samples?count=6"
```

### List and download recorded clips

`GET /api/v1/streams/{id}/clips` returns `{ items, total }`. Each clip already
includes a ready-to-fetch `download_url` and `thumbnail_download_url`.

```bash
# Newest clips for a stream (limit max 200)
curl "$BASE/api/v1/streams/415/clips?limit=20"
```

For a batch, `POST /api/v1/clips/download-prepare` mints presigned URLs for up to
120 segments at once:

```bash
curl -X POST "$BASE/api/v1/clips/download-prepare" \
  -H 'content-type: application/json' \
  -d '{"stream_id":415,"segment_ids":[4252509,4252507]}'
# -> items[]: { id, download_url, thumbnail_download_url, filename }
```

Authenticated mirrors of the clip endpoints live under `/api/v1/account/...` and
accept a session cookie or an API key (`stoarama auth api-keys create`); they
exist for per-account download history and scripted access, but the public
endpoints above cover read-only browsing and download.

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
- `ffmpeg` and `yt-dlp` on machines that run local capture flows

### Minimal env

```bash
export DATABASE_URL='postgres://...'

export R2_ACCOUNT_ID='...'
export R2_ACCESS_KEY_ID='...'
export R2_SECRET_ACCESS_KEY='...'
export R2_BUCKET='stoarama-dev'
export R2_REGION='auto'
export R2_ENDPOINT='https://<account>.r2.cloudflarestorage.com'

export AUTO_MIGRATE='true'
export PORT='8080'

export BOOTSTRAP_ADMIN_EMAIL='you@example.org'
export APP_BASE_URL='http://127.0.0.1:8080'
export EMAIL_PROVIDER='log'

# Optional until machine workers are attached:
# export SERVICE_TOKEN='runtime-service-token'
```

Keep real credentials out of git. Copy any `local/*.env.example` to the same
name without the `.example` suffix and fill in real values there; those files are
gitignored. See [SECURITY.md](SECURITY.md) for the pre-commit setup
(`pre-commit install`) and note that CI runs gitleaks and fails on any secret
regardless.

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
go run ./cmd/stoarama recording youtube run --stream-id 123 --cookies-from-browser chrome
```

## Render

Use the root `render.yaml` for the first Stoarama deployment.

Expected deployment shape:

- fresh `stoarama-db`
- `stoarama-api` web service
- fresh Stoarama R2 bucket configured through env vars
- email can remain in `log` mode until custom domain and Resend are wired

See [docs/DEPLOY_RENDER.md](docs/DEPLOY_RENDER.md) and [docs/ROADMAP.md](docs/ROADMAP.md).

## License

Apache License 2.0. See [LICENSE](LICENSE).
