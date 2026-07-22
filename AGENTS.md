# AGENTS: Stoarama

Stoarama is the new product/control-plane repo. It is CLI-first and Render-first in v1.

## Supported Surfaces

- API service: `backend/cmd/stoarama-api`
- Internal control CLI: `backend/cmd/stoaramactl`
- User/node CLI: `backend/cmd/stoarama`
- SQL migrations: `infra/sql/migrations`
- Render blueprint: `render.yaml`

The old `social-isolation` repo remains the private research/thesis repo. Do not reintroduce thesis-only pipelines or one-off ops workflows here as primary product surfaces.

## Development Rule

Every product workflow ships in this order:

1. API behavior exists
2. `stoaramactl` can exercise it end-to-end
3. tests cover it
4. the web UI follows the same model

The public `stoarama` CLI is narrower and is for account/node workflows, not operator administration.

## V1 Product Shape

- admin-curated streams
- magic-link accounts
- account API keys
- enrolled source/inference nodes
- hosted capture
- BYOC inference metadata and provenance

## Core Commands

- Run API:
  - `cd backend && go run ./cmd/stoarama-api`
- Run internal CLI:
  - `cd backend && go run ./cmd/stoaramactl --help`
- Run user CLI:
  - `cd backend && go run ./cmd/stoarama --help`
- Apply migrations:
  - `cd backend && go run ./cmd/stoaramactl migrate up`
- Test:
  - `cd backend && go test ./...`

## Deployment

- Render blueprint: `render.yaml`
- Fresh Stoarama Postgres
- Fresh Stoarama R2 bucket
- Email can stay in `EMAIL_PROVIDER=log` until domain and Resend are wired

## Git Safety

- keep secrets out of git
- prefer small checkpoints
- do not mix thesis experiments into this repo

## Local Credentials

Runtime secrets live in `local/*.env` (gitignored; committed `.example` files show the shape). Check there before asking for credentials or reaching for Render:

- `render.env` — production env mirror, incl. R2: `R2_ACCOUNT_ID`, `R2_BUCKET`, `R2_REGION`, and `R2_ACCESS_KEY_ID`/`R2_SECRET_ACCESS_KEY` (map to `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` for AWS-style tooling)
- `recording-supervisor.env`, `youtube-relay-source.env` — Stoarama operator API keys
- `do-capture.env` — DigitalOcean capture box

Load with: `set -a; . local/render.env; set +a`. Never print values; never copy between machines without Deniz approval.
