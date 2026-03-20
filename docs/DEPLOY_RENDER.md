# Deploy Stoarama On Render

## What this phase includes

- fresh Render Postgres
- `stoarama-api` service from `render.yaml`
- fresh Stoarama R2 bucket wired via env vars
- email left in `log` mode until domain and Resend are connected

## Required env vars

- `API_TOKEN`
- `R2_ACCOUNT_ID`
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- `R2_BUCKET`
- optionally `R2_ENDPOINT`
- optionally `APP_BASE_URL` if the Render hostname differs from the blueprint default

## Recommended initial values

- `AUTO_MIGRATE=false`
- `EMAIL_PROVIDER=log`
- `BOX_WORKER_EMBEDDED=true`

## First boot checks

After Render deploys:

1. open the service URL
2. verify the root app responds
3. verify `/research` loads
4. verify the API process starts without migration/runtime errors
5. run a simple `stoaramactl` call against the deployed API once credentials are set

## What comes next

After Render is live, the next phase is:

- Cloudflare DNS for `stoarama.com`
- `api.stoarama.com`
- Resend domain verification
- live stream/artifact migration
