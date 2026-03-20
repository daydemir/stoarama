# Deploy Stoarama On Render

## What this phase includes

- fresh Render Postgres
- `stoarama-api` service from `render.yaml`
- fresh Stoarama R2 bucket wired via env vars
- email left in `log` mode until domain and Resend are connected

## Required env vars

- `R2_ACCOUNT_ID`
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- `R2_BUCKET`
- `BOOTSTRAP_ADMIN_EMAIL`
- optionally `R2_ENDPOINT`
- optionally `SERVICE_TOKEN` for machine-to-machine runtime paths before node/service enrollment is expanded
- optionally `APP_BASE_URL` if the Render hostname differs from the blueprint default

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
