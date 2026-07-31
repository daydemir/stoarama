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

- `render.env` — production env mirror: `SERVICE_TOKEN` plus R2 (`R2_ACCOUNT_ID`, `R2_BUCKET`, `R2_REGION`, and `R2_ACCESS_KEY_ID`/`R2_SECRET_ACCESS_KEY`, which map to `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` for AWS-style tooling)
- `recording-supervisor.env` — operator API key AND the production Postgres `DATABASE_URL` (the only local source of prod DB access)
- `youtube-relay-source.env` — operator API key + relay topology config
- `mitscl.env` — account-47 API token (`BACKEND_API_URL` + `MITSCL_API_TOKEN`)
- `do-capture.env` — `DIGITALOCEAN_TOKEN` + `TF_VAR_*` for the DigitalOcean capture boxes
- `relay-signing-private.key` — relay payload signing key

Load with: `set -a; . local/render.env; set +a`. Never print values; never copy between machines without Deniz approval.

## Render Operator Access

The Render CLI is installed at `~/.local/bin/render` and is already authenticated (workspace `ay` / `tea-d1g1uevfte5s7384s1q0`, deniz@denizay.org). Its config is `~/.render/cli.yaml`; the bearer token is at YAML path `.api.key` (NOT `.profiles.default.apiKey` — that path does not exist, and `yq` is not installed on this machine).

The CLI has **no** `env-var` subcommand. Read/write service env vars through the REST API:

    rkey() { awk '/^ *key:/{print $2}' ~/.render/cli.yaml; }   # re-read per call
    curl -fsS -H "Authorization: Bearer $(rkey)" \
      "https://api.render.com/v1/services/$SERVICE_ID/env-vars?limit=100" \
      | python3 -c 'import json,sys; [print(e.get("envVar",e)["key"]) for e in json.load(sys.stdin)]'

Print KEYS ONLY, as above. The raw response contains every production secret
value, so piping it to stdout leaks the lot into a transcript. If you need one
value, select that single key and use it without echoing it.

Service IDs: `stoarama-api` = `srv-d6usqn94tr6s73d94cd0`, `stoarama-recorder-control` = `srv-d8vcdspo3t8c73far4e0`.

Writing an env var (`PUT .../env-vars/KEY`) does **not** trigger a deploy, and `POST .../restart` does **not** reload env — the process keeps the values it booted with. To make an env change take effect, trigger a deploy of the same commit:

    rkey() { awk '/^ *key:/{print $2}' ~/.render/cli.yaml; }   # short-lived, re-read per call

    LIVE=$(curl -fsS -H "Authorization: Bearer $(rkey)" \
      "https://api.render.com/v1/services/$SERVICE_ID/deploys?limit=100" \
      | python3 -c 'import json,sys
live=[d for d in (e.get("deploy",e) for e in json.load(sys.stdin)) if d.get("status")=="live"]
assert live, "no live deploy in this page; page further with ?cursor="
print(live[0]["commit"]["id"])')

    curl -fsS --show-error -X POST -H "Authorization: Bearer $(rkey)" \
      -H 'Content-Type: application/json' \
      -d "{\"clearCache\":\"do_not_clear\",\"commitId\":\"$LIVE\"}" \
      "https://api.render.com/v1/services/$SERVICE_ID/deploys" \
      | LIVE="$LIVE" python3 -c 'import json,os,sys
d=json.load(sys.stdin)
assert d.get("id") and d.get("status"), d
got=d.get("commit",{}).get("id")
assert got==os.environ["LIVE"], "deployed %s, asked for %s" % (got, os.environ["LIVE"])
print(d["id"], d["status"], got[:8])'

Three traps here. `curl -s` alone exits 0 on an HTTP 400+, so a bad service id or
an expired token looks like a successful deploy trigger — hence `-f` and the
assertion on the response rather than eyeballing it. A deploy POSTed without
`commitId` takes whatever is newest on the connected branch, so anything pushed
between your check and the POST ships too; pin it to the commit you verified.
And the newest deploy is not necessarily the running one — it may be failed or
still building — so filter for `status == "live"` rather than taking the first
element.

`AUTO_MIGRATE=true` on `stoarama-api`, so a deploy runs migrations on boot. Before deploying, confirm `origin/main` matches the live commit and that no migration is unapplied, or you will ship more than the env change.

`.api.key` is short-lived; `expires_at` sits beside it in the same file, so check there rather than trusting a date written down here. Re-read it from the file on each use rather than caching it; on a 401 refresh it with the CLI instead of asking Deniz for a dashboard value.
