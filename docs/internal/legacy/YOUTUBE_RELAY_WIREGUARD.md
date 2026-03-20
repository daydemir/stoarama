# YouTube Relay Over WireGuard

This is the production path for scaling YouTube capture:

- Mac mini runs `youtube-relay source` (holds yt-dlp cookies and resolves YouTube URLs).
- DO servers run `youtube-relay sink` (pull relay URLs and ingest frames to backend).
- Data plane is private HTTP over WireGuard.

## 1) WireGuard Topology

- Mac mini: `wg0` private IP, example `10.77.0.2`
- DO sink-1: `wg0` private IP, example `10.77.0.11`
- DO sink-2: `wg0` private IP, example `10.77.0.12`

Use a hub-and-spoke or full-mesh WG setup. The critical requirement is:
- every sink can reach source `http://10.77.0.2:18080`

## 2) Source (Mac mini)

Create `local/youtube-relay-source.env` (gitignored):

```bash
export BACKEND_API_URL='https://stoarama-api.onrender.com'
export API_TOKEN='replace-me'

export YOUTUBE_RELAY_SOURCE_SERVER_ID='mini-m1-yt-source'
export YOUTUBE_RELAY_SOURCE_SHARD_ID='yt-account-1'
export YOUTUBE_RELAY_SOURCE_CAPACITY='8'
export YOUTUBE_RELAY_SOURCE_HEARTBEAT_SEC='15'
export YOUTUBE_RELAY_SOURCE_LEASE_SEC='45'
export YOUTUBE_RELAY_SOURCE_REFRESH_SEC='20'
export YOUTUBE_RELAY_SOURCE_RESOLVE_TIMEOUT_SEC='60'
export YOUTUBE_RELAY_SOURCE_RESOLVE_FAILURE_THRESHOLD='3'

export YOUTUBE_RELAY_SOURCE_BIND_ADDR='0.0.0.0:18080'
export YOUTUBE_RELAY_SOURCE_PUBLIC_BASE_URL='http://10.77.0.2:18080'
export YOUTUBE_RELAY_SHARED_TOKEN='replace-with-long-random-token'
export YOUTUBE_RELAY_NETWORK_TRANSPORT='wireguard'
export YOUTUBE_RELAY_TOPOLOGY_ID='do-youtube-relay-hub'
export YOUTUBE_RELAY_TOPOLOGY_ROLE='source'
export YOUTUBE_RELAY_HUB_SERVER_ID='do-youtube-relay-hub'
export YOUTUBE_RELAY_WG_INTERFACE='wg0'
export YOUTUBE_RELAY_WG_IP='10.77.0.2'
export YOUTUBE_RELAY_SOURCE_ENDPOINT='10.77.0.2:18080'

export YT_DLP_COOKIES_FROM_BROWSER='chrome'
# or:
# export YT_DLP_COOKIES_FILE='/absolute/path/youtube-cookies.txt'
```

Run:

```bash
cd /Users/deniz/Build/thesis/stoarama
SI_ENV_FILE='local/youtube-relay-source.env' \
  backend/scripts/start-youtube-relay-source.sh
```

Always-on launchd on Mac:
```bash
cd /Users/deniz/Build/thesis/stoarama
SI_ENV_FILE='local/youtube-relay-source.env' \
  backend/scripts/install-local-youtube-relay-source-launchd.sh
```

## 3) Sink (DO server)

Create `/opt/stoarama/local/youtube-relay-sink.env`:

```bash
export BACKEND_API_URL='https://stoarama-api.onrender.com'
export API_TOKEN='replace-me'

export YOUTUBE_RELAY_SINK_SERVER_ID='do-yt-relay-01'
export YOUTUBE_RELAY_SINK_WORKER_ID='yt-relay-sink-do-yt-relay-01'
export YOUTUBE_RELAY_SINK_CAPACITY='8'
export YOUTUBE_RELAY_SINK_HEARTBEAT_SEC='15'
export YOUTUBE_RELAY_SINK_LEASE_SEC='45'
export YOUTUBE_RELAY_SINK_REFRESH_SEC='5'
export YOUTUBE_RELAY_SINK_UNSUPPORTED_THRESHOLD='8'
export YOUTUBE_RELAY_NETWORK_TRANSPORT='wireguard'
export YOUTUBE_RELAY_TOPOLOGY_ID='do-youtube-relay-hub'
export YOUTUBE_RELAY_TOPOLOGY_ROLE='sink'
export YOUTUBE_RELAY_HUB_SERVER_ID='do-youtube-relay-hub'
export YOUTUBE_RELAY_WG_INTERFACE='wg0'
export YOUTUBE_RELAY_WG_IP='10.77.0.11'
export YOUTUBE_RELAY_SOURCE_SERVER_ID='mini-m1-yt-source'
```

Run:

```bash
cd /opt/stoarama
SI_ENV_FILE='local/youtube-relay-sink.env' \
  backend/scripts/start-youtube-relay-sink.sh
```

Terraform-managed DO sink fleet:
- `infra/do-youtube-relay`
- `docs/DO_YOUTUBE_RELAY_HUB.md`

## 4) Verify

```bash
source /Users/deniz/Build/thesis/stoarama/local/operator.env
cd /Users/deniz/Build/thesis/stoarama/backend

go run ./cmd/stoaramactl servers capacity list --backend-api-url "$BACKEND_API_URL" --api-token "$API_TOKEN"
go run ./cmd/stoaramactl youtube-relay routes --backend-api-url "$BACKEND_API_URL" --api-token "$API_TOKEN"
go run ./cmd/stoaramactl servers assignments --backend-api-url "$BACKEND_API_URL" --api-token "$API_TOKEN" --execution-class youtube_relay
go run ./cmd/stoaramactl servers list --backend-api-url "$BACKEND_API_URL" --api-token "$API_TOKEN" --hours 24 --show-processes
```

Expected:
- source appears via relay source heartbeats
- sink appears with `youtube_relay` capacity
- assigned routes show `source_ready` then `running`
- stream runtime updates and frames are ingested normally
- server rows include explicit `connections` topology (`transport`, `topology`, `hub`, `wg`, `source`)

## 5) Failure Model

- Missing relay source capacity: assignment fails with `youtube_relay_source_unavailable`
- Missing/invalid `relay_pull_url`: sink fails fast (`youtube_relay requires capture_config.relay_pull_url`)
- Source resolve failure: route status becomes `failed` with explicit error text
- Source stop: source routes are marked failed immediately by backend stop endpoint
