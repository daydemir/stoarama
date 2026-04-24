# Capture Server CLI

`stoaramactl capture-server run` is the assignment-managed `video_live` recording supervisor for server nodes.

## Behavior

- Announces shared `video_live` server capacity heartbeat to backend (`server_execution_capacity`).
- Announces capture worker heartbeats per active execution class (`capture_worker_heartbeats`, `processing_worker_heartbeats`).
- Runs one persistent capture manager per active execution class with assignment polling:
  - source: `GET /api/v1/recording/assignments?server_id=...`
  - only assigned streams for that server are captured.
- On shutdown, sends:
  - `/api/v1/capture/worker-stopped` per active mode
  - `/api/v1/recording/servers/stopped` for server capacity cleanup.

Execution-class behavior:
- `video_live`: records HLS, HTTP video, RTSP, and RTMP streams as source-native fixed-length clips.
- `image_poll`: catalog/probe only; it is not recording capacity.

Clip capture is a hard source-copy/remux path (`ffmpeg -c copy`), not a 30fps transcode. The recorder preserves source cadence/codecs and stores `actual_fps` when `ffprobe` can report it. Normalized derivatives can be produced later by downstream jobs. Clip bytes are uploaded from the capture node directly to R2 through presigned URLs. Render only issues upload intents and records metadata.

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
- `--capture-shared-capacity` (>0; `video_live` recording slots)

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

## Refresh a DO Capture Node

DigitalOcean capture nodes run from the repo checked out at `/opt/stoarama`. After a capture-runtime fix lands, refresh the node and restart the service so it actually runs the new code:

```bash
sudo /opt/stoarama/backend/scripts/refresh-do-capture-node.sh main
```

This command:

- fetches the target ref
- hard-resets `/opt/stoarama` to `origin/<ref>`
- restarts `stoarama-capture.service`
- prints the active git SHA after restart

Use this before validating direct-video fixes on assigned streams such as London `http_video` cameras.
