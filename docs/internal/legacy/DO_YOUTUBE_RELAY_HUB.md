# DigitalOcean YouTube Relay Hub

This provisions a dedicated DO sink fleet for `youtube_relay` mode while Mac minis act as YouTube source servers.

## Architecture

- Mac mini(s): `stoaramactl youtube-relay source run`
  - holds yt-dlp cookies/account auth
  - resolves YouTube URLs
  - serves relay pull URLs (`/relay/{stream_id}?token=...`)
- DO sink fleet: `stoaramactl youtube-relay sink run`
  - receives assignment-managed `youtube_relay` streams
  - pulls bytes from source relay URL
  - ingests frames through backend capture API
- Backend: assignment control plane + telemetry

## Terraform Stack

- Root: `infra/do-youtube-relay`
- Service on droplet: `stoarama-youtube-relay-sink.service`
- Startup script: `backend/scripts/start-youtube-relay-sink.sh`

## Prereqs

Use local env files (gitignored):

- `local/operator.env` with `BACKEND_API_URL` and `API_TOKEN`
- `local/do-capture.env` with `DIGITALOCEAN_TOKEN`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `TF_VAR_ssh_public_key`

You must also provide a source URL reachable from DO sinks:

- `YOUTUBE_RELAY_SOURCE_PUBLIC_BASE_URL`
- Example: `https://relay-source.example.com/ytrelay` (reachable relay source base URL)

## Bootstrap env file (CLI)

```bash
cd /Users/deniz/Build/thesis/stoarama
export YOUTUBE_RELAY_SOURCE_PUBLIC_BASE_URL='https://relay-source.example.com/ytrelay'
backend/scripts/bootstrap-do-youtube-relay-env.sh
```

This writes `local/do-youtube-relay.env` with a generated shared token (if missing):

- `TF_VAR_youtube_relay_shared_token`
- `TF_VAR_youtube_relay_source_public_base_url`
- `TF_VAR_backend_api_url`
- `TF_VAR_backend_api_token`
- `TF_VAR_youtube_relay_network_transport` (`wireguard`)
- `TF_VAR_youtube_relay_topology_id` / `TF_VAR_youtube_relay_hub_server_id`
- `TF_VAR_youtube_relay_source_server_id`
- `TF_VAR_youtube_relay_wg_interface` / `TF_VAR_youtube_relay_wg_source_ip`
- `TF_VAR_youtube_relay_wg_sink_cidr` / `TF_VAR_youtube_relay_wg_sink_offset`

Optional token-only generation:

```bash
backend/scripts/generate-youtube-relay-token.sh
```

## Deploy DO sink fleet

```bash
cd /Users/deniz/Build/thesis/stoarama/infra/do-youtube-relay
source ../../local/do-youtube-relay.env
terraform init
terraform plan
terraform apply
```

Outputs include sink `recording_server_ids` in format `do-<droplet_id>-yt-relay`.

## Configure Mac mini source

Create `local/youtube-relay-source.env` (gitignored) with matching token and source URL:

```bash
cd /Users/deniz/Build/thesis/stoarama
backend/scripts/bootstrap-youtube-relay-source-env.sh
```

Start once (foreground):

```bash
SI_ENV_FILE='local/youtube-relay-source.env' \
  backend/scripts/start-youtube-relay-source.sh
```

Install always-on launchd (recommended on Mac mini):

```bash
SI_ENV_FILE='local/youtube-relay-source.env' \
  backend/scripts/install-local-youtube-relay-source-launchd.sh
```

## Verify (CLI-first)

```bash
cd /Users/deniz/Build/thesis/stoarama/backend
source ../local/operator.env

# active servers and processes
go run ./cmd/stoaramactl servers list --backend-api-url "$BACKEND_API_URL" --api-token "$API_TOKEN" --hours 24 --show-processes

# youtube relay capacity heartbeats
go run ./cmd/stoaramactl servers capacity list --backend-api-url "$BACKEND_API_URL" --api-token "$API_TOKEN"

# assignments and routes
go run ./cmd/stoaramactl servers assignments --backend-api-url "$BACKEND_API_URL" --api-token "$API_TOKEN" --execution-class youtube_relay
go run ./cmd/stoaramactl youtube-relay routes --backend-api-url "$BACKEND_API_URL" --api-token "$API_TOKEN"
```

Expected:

- one source server heartbeat row with positive capacity
- sink servers heartbeat as `youtube_relay` capacity
- assigned YouTube streams promoted to `youtube_relay`
- routes move to `running`
- `servers list` prints `connections=...` with transport/topology/hub/wg/source metadata

## Scale-out

- Add more DO sinks: increase `TF_VAR_droplet_count`, re-apply Terraform.
- Add more Mac minis: run another source with unique:
  - `YOUTUBE_RELAY_SOURCE_SERVER_ID`
  - `YOUTUBE_RELAY_SOURCE_SHARD_ID`
- Keep same `YOUTUBE_RELAY_SHARED_TOKEN` only within trusted relay network.
