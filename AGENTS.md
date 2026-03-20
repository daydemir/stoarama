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
- research API keys
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
