# Capture Server CLI

`stoaramactl capture-server run` is the non-YouTube assignment-managed capture supervisor for server nodes.

## Behavior

- Announces shared capture-group server capacity heartbeat to backend (`server_execution_capacity`).
- Announces capture worker heartbeats per active execution class (`capture_worker_heartbeats`, `processing_worker_heartbeats`).
- Runs one persistent capture manager per active execution class with assignment polling:
  - source: `GET /api/v1/recording/assignments?server_id=...`
  - only assigned streams for that server are captured.
- On shutdown, sends:
  - `/api/v1/capture/worker-stopped` per active mode
  - `/api/v1/recording/servers/stopped` for server capacity cleanup.

Execution-class behavior:
- `video_live`: resolves HLS manifests when needed and otherwise passes direct video URLs through FFmpeg; still-image endpoints must use `image_poll`.
- `image_poll`: polls still-image sources directly.

## Usage

```bash
cd backend
go run ./cmd/stoaramactl capture-server run \
  --backend-api-url "$BACKEND_API_URL" \
  --api-token "$API_TOKEN" \
  --server-id "do-123456789" \
  --worker-id "capture-server-do-123456789" \
  --capture-shared-capacity 6 \
  --heartbeat-sec 15 \
  --lease-sec 45 \
  --refresh-sec 5 \
  --unsupported-threshold 8 \
  --frame-queue-size 64 \
  --frame-enqueue-timeout-sec 3 \
  --frame-writer-workers 2
```

## Required Flags

- `--backend-api-url`
- `--api-token`
- `--capture-shared-capacity` (>0; shared across `video_live` and `image_poll`)

## Optional Flags

- `--server-id`:
  - default: `do-<metadata-id>` on DO hosts, otherwise local hostname fallback.
- `--worker-id`:
  - default: `capture-server-<hostname>`.
- `--draining-execution-classes`:
  - optional execution-class set marked draining in server heartbeat.
- `--metadata-json`:
  - merged into heartbeat metadata.
- `--duration`:
  - optional bounded run (for smoke tests).

## Wrapper Script

Use the launcher to avoid long CLI flags:

```bash
cd backend
./scripts/start-capture-server.sh
```

The launcher reads `SI_ENV_FILE` (default: `local/capture-server.env`).
