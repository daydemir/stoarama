# DigitalOcean Capture Fleet

This stack provisions non-YouTube capture servers on DigitalOcean and connects them to the Render backend control plane.
For the dedicated non-recording catalog sweeper node, use `infra/do-capture-catalog` and `docs/CATALOG_SWEEPERS.md`.

## Scope

- Execution classes: `video_live,image_poll`
- Control plane: Render backend API
- Assignment model: backend-managed `recording_assignments`

## Files

- Terraform root: `infra/do-capture`
- Cloud-init template: `infra/do-capture/cloud-init.yaml.tftpl`
- Startup script: `backend/scripts/start-capture-server.sh`

## Prereqs

Create gitignored env file:

```bash
cat > local/do-capture.env <<'EOF'
export DIGITALOCEAN_TOKEN='...'
export AWS_ACCESS_KEY_ID='...'
export AWS_SECRET_ACCESS_KEY='...'
export TF_VAR_project_name='Social Isolation'
export TF_VAR_region='nyc3'
export TF_VAR_droplet_count='4'
export TF_VAR_droplet_size='s-2vcpu-4gb'
export TF_VAR_backend_api_url='https://stoarama-api.onrender.com'
export TF_VAR_backend_api_token='...'
export TF_VAR_ssh_public_key='ssh-ed25519 ...'
export TF_VAR_repo_clone_token='optional-github-token-for-private-repo'
EOF

chmod 600 local/do-capture.env
```

## Deploy

```bash
cd infra/do-capture
source ../../local/do-capture.env

terraform init
terraform plan
terraform apply
```

## Expected Runtime

Each droplet starts `stoarama-capture.service`, which runs:

```bash
/opt/stoarama/backend/scripts/start-capture-server.sh
```

That command starts `stoaramactl capture-server run` with assignment-managed polling.
`start-capture-server.sh` now fails fast unless it resolves a stable `CAPTURE_SERVER_ID`
(from env or stable hostname), so one machine cannot appear as multiple servers.
The DO fleet now pins `CAPTURE_SERVER_EXECUTION_CLASSES=video_live,image_poll` in node env
so restarts cannot silently narrow non-YouTube capacity.

## Verify (CLI-first)

```bash
cd backend
source ../local/operator.env

go run ./cmd/stoaramactl servers list --backend-api-url "$BACKEND_API_URL" --api-token "$API_TOKEN" --hours 168
go run ./cmd/stoaramactl servers capacity list --backend-api-url "$BACKEND_API_URL" --api-token "$API_TOKEN"
go run ./cmd/stoaramactl servers assignments --backend-api-url "$BACKEND_API_URL" --api-token "$API_TOKEN"
```

After assignment, confirm fresh frames:

```bash
curl -sS -H "Authorization: Bearer $API_TOKEN" \
  "$BACKEND_API_URL/api/v1/frames?stream_id=<id>&limit=20&offset=0"
```
