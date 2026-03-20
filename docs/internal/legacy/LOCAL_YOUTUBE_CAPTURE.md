# Local YouTube Capture Worker (Mac)

Use this when YouTube capture is blocked on Render but non-YouTube capture keeps running on Render.

## Important Behavior
- This path uses the same persistent capture engine as `capture-worker`:
  - `go run ./cmd/stoaramactl youtube-relay source run ...` on the Mac and `youtube-relay sink run ...` on DO
- Local API mode does **not** require local DB or R2 secrets.
- Capture writes are sent to backend API capture ingest endpoints, and backend persists to Postgres + R2.
- Worker auto-discovery reads `GET /api/v1/recording/assignments?server_id=...` (assignment control-plane endpoint), not dashboard stream list queries.
- YouTube recording ownership is assignment-managed:
  - each runner announces `--capacity N`
  - backend stores live per-server execution-class capacity heartbeats
  - operators assign/unassign streams explicitly via `/api/v1/recording/streams/{id}/assign|unassign`
  - workers only capture streams assigned to their `server_id`

## YouTube Relay Split (New)
When using source/sink split:
- run `stoaramactl youtube-relay source run` on Mac machines with yt-dlp cookies
- run `stoaramactl youtube-relay sink run` on server nodes
- assignment execution class can be `youtube_relay` while stream source remains YouTube

Quick commands:
```bash
cd backend
go run ./cmd/stoaramactl youtube-relay source run \
  --backend-api-url "$BACKEND_API_URL" \
  --api-token "$API_TOKEN" \
  --server-id "${YOUTUBE_SERVER_ID:-$(hostname -s)}" \
  --shard-id "yt-account-1" \
  --capacity 8

go run ./cmd/stoaramactl youtube-relay routes \
  --backend-api-url "$BACKEND_API_URL" \
  --api-token "$API_TOKEN"
```

Production launcher:
```bash
cd /Users/deniz/Build/thesis/stoarama
backend/scripts/bootstrap-youtube-relay-source-env.sh
SI_ENV_FILE='local/youtube-relay-source.env' \
  backend/scripts/start-youtube-relay-source.sh
```

Mac launchd installer:
```bash
cd /Users/deniz/Build/thesis/stoarama
SI_ENV_FILE='local/youtube-relay-source.env' \
  backend/scripts/install-local-youtube-relay-source-launchd.sh
```

WireGuard deployment guide:
- `docs/YOUTUBE_RELAY_WIREGUARD.md`
- `docs/DO_YOUTUBE_RELAY_HUB.md`

## Required Local Env
Create a local file (gitignored), for example `local/youtube-worker.env`:

```bash
export BACKEND_API_URL='https://stoarama-api.onrender.com'
export API_TOKEN='replace-me'

# one of the two auth options below is required
export YT_DLP_COOKIES_FILE='/absolute/path/to/youtube-cookies.txt'
# export YT_DLP_COOKIES_FROM_BROWSER='chrome'

# optional tuning
# stable per machine; this is the recording server identity shown in dashboard/servers
export YOUTUBE_SERVER_ID='mini-m1'
export CAPTURE_TICK_SEC='5'
export CAPTURE_CONCURRENCY='4'
# optional; default behavior:
# - if YOUTUBE_STREAM_IDS is set, max_sessions defaults to CAPTURE_CONCURRENCY
# - if YOUTUBE_STREAM_IDS is empty, max_sessions defaults to YOUTUBE_RUNNER_CAPACITY
#   (all assigned youtube streams for this server_id)
# export CAPTURE_MAX_SESSIONS='4'
export CAPTURE_UNSUPPORTED_THRESHOLD='8'
export CAPTURE_RECORDING_HEARTBEAT='0'
export CAPTURE_FRAME_QUEUE_SIZE='256'
export CAPTURE_FRAME_ENQUEUE_TIMEOUT_SEC='8'
export CAPTURE_FRAME_WRITERS='6'
export YOUTUBE_RUNNER_CAPACITY='10'
export YOUTUBE_WORKER_HEARTBEAT_SEC='15'

# high-resolution throughput tuning (optional)
# prefer AVC/H.264 video tracks (usually cheaper decode than VP9/AV1)
export YT_DLP_FORMAT='bestvideo[vcodec^=avc1]/bestvideo/best'
# optional yt-dlp sorting expression
# export YT_DLP_FORMAT_SORT='res,fps'
# JPEG quality 1..31 (lower is higher quality and more CPU)
export CAPTURE_FFMPEG_JPEG_Q='4'
export CAPTURE_FFMPEG_THREADS='1'
# optional hardware decode on Apple Silicon
export CAPTURE_FFMPEG_HWACCEL='videotoolbox'
export CAPTURE_FFMPEG_RECONNECT='true'
export CAPTURE_FFMPEG_RECONNECT_DELAY_MAX_SEC='2'

# optional explicit runner mode
# export CAPTURE_RUNNER_MODE='api'
```

Runner mode is intentionally API-only:
- `CAPTURE_RUNNER_MODE` may be omitted or set to `api`.
- Any other value fails fast.

## Start Worker
```bash
cd backend
./scripts/start-local-youtube-worker.sh --duration 5m
```

Detached background (quick/manual):
```bash
cd /Users/deniz/Build/thesis/stoarama
tmux new-session -d -s si_yt5 \
  "cd /Users/deniz/Build/thesis/stoarama && \
   SI_ENV_FILE='local/youtube-worker.env' \
   CAPTURE_RUNNER_MODE='api' \
   YOUTUBE_STREAM_IDS='3550,3552,3555,3556,3564' \
   YT_DLP_COOKIES_FROM_BROWSER='chrome' \
   backend/scripts/start-local-youtube-worker.sh"

tmux capture-pane -pt si_yt5:0 | tail -n 80
```

## Always-On Mode (launchd, recommended)
Install persistent worker + watchdog launch agents:

```bash
cd /Users/deniz/Build/thesis/stoarama
SI_ENV_FILE='local/youtube-worker.env' \
  backend/scripts/install-local-youtube-launchd.sh
```

What this installs:
- `io.stoarama.youtube-capture`:
  - starts on login
  - restarts automatically on crash/exit
  - runs `backend/scripts/start-local-youtube-worker.sh` in API mode
- `io.stoarama.youtube-capture-watchdog`:
  - runs every 60s (configurable)
  - checks recorded `youtube_live` stream freshness from dashboard API
  - force-restarts the worker only when all recorded YouTube streams are stale
  - skips restart when backend still reports active `youtube_live` workers (heartbeat-managed capacity), to avoid restart thrash during temporary ingest lag

Useful checks:
```bash
launchctl print "gui/$(id -u)/io.stoarama.youtube-capture" | head -n 60
launchctl print "gui/$(id -u)/io.stoarama.youtube-capture-watchdog" | head -n 60

tail -n 80 local/logs/launchd/youtube-worker.out.log
tail -n 80 local/logs/launchd/youtube-worker.err.log
tail -n 80 local/logs/launchd/youtube-healthcheck.out.log
tail -n 80 local/logs/launchd/youtube-healthcheck.err.log
```

Fail-fast check:
- if `youtube-worker.err.log` shows repeated `worker heartbeat loop: heartbeat failed consecutive=...`, the worker exits after 2 consecutive heartbeat failures and launchd restarts it.
- this is expected fail-fast behavior; fix backend heartbeat endpoint and redeploy before continuing.

Uninstall:
```bash
cd /Users/deniz/Build/thesis/stoarama
backend/scripts/uninstall-local-youtube-launchd.sh
```

## Non-Recording YouTube Catalog Sweeper

For preview/image freshness on `recording_state=off` YouTube streams:

```bash
cd /Users/deniz/Build/thesis/stoarama
SI_ENV_FILE='local/youtube-worker.env' \
  backend/scripts/start-local-youtube-catalog-sweeper.sh
```

Install as separate always-on launchd process:

```bash
cd /Users/deniz/Build/thesis/stoarama
SI_ENV_FILE='local/youtube-worker.env' \
  backend/scripts/install-local-youtube-catalog-launchd.sh
```

Remove it:

```bash
cd /Users/deniz/Build/thesis/stoarama
backend/scripts/uninstall-local-youtube-catalog-launchd.sh
```

Optional scope limit:
```bash
export YOUTUBE_STREAM_IDS='3550,3552'
cd backend
./scripts/start-local-youtube-worker.sh
```

Equivalent raw CLI (without wrapper script):
```bash
cd backend
go run ./cmd/stoaramactl youtube-relay source run \
  --backend-api-url "$BACKEND_API_URL" \
  --api-token "$API_TOKEN" \
  --server-id "${YOUTUBE_SERVER_ID:-$(hostname -s)}" \
  --worker-id "local-youtube-worker-${YOUTUBE_SERVER_ID:-$(hostname -s)}" \
  --capacity "${YOUTUBE_RUNNER_CAPACITY:-10}" \
  --heartbeat-sec "${YOUTUBE_WORKER_HEARTBEAT_SEC:-15}" \
  --frame-queue-size "${CAPTURE_FRAME_QUEUE_SIZE:-256}" \
  --frame-enqueue-timeout-sec "${CAPTURE_FRAME_ENQUEUE_TIMEOUT_SEC:-8}" \
  --frame-writer-workers "${CAPTURE_FRAME_WRITERS:-6}" \
  --recording-heartbeat=false \
  --yt-dlp-cookies-file "${YT_DLP_COOKIES_FILE}" \
  --yt-dlp-format "${YT_DLP_FORMAT:-bestvideo[vcodec^=avc1]/bestvideo/best}" \
  --yt-dlp-format-sort "${YT_DLP_FORMAT_SORT:-}" \
  --ffmpeg-jpeg-quality "${CAPTURE_FFMPEG_JPEG_Q:-4}" \
  --ffmpeg-threads "${CAPTURE_FFMPEG_THREADS:-1}" \
  --ffmpeg-hwaccel "${CAPTURE_FFMPEG_HWACCEL:-}" \
  --ffmpeg-reconnect "${CAPTURE_FFMPEG_RECONNECT:-true}" \
  --ffmpeg-reconnect-delay-max-sec "${CAPTURE_FFMPEG_RECONNECT_DELAY_MAX_SEC:-2}" \
  --duration 5m
```

## CLI-First Validation
Use API-only reads to validate freshness after starting local worker:

```bash
source local/operator.env

# runtime view
curl -sS -H "Authorization: Bearer $API_TOKEN" \
  "$BACKEND_API_URL/api/v1/capture/runtime?limit=200&offset=0"

# stream detail (replace id)
curl -sS -H "Authorization: Bearer $API_TOKEN" \
  "$BACKEND_API_URL/api/v1/dashboard/streams/<id>?include_image_urls=false"

# latest raw frames (replace id)
curl -sS -H "Authorization: Bearer $API_TOKEN" \
  "$BACKEND_API_URL/api/v1/frames?stream_id=<id>&limit=20&offset=0"
```

Expected:
- `capture_runtime.status` becomes `running`
- `capture_runtime.last_frame_at` updates continuously
- `/frames` shows new `capture_status=success` rows with recent `captured_at`
- assigned streams project to `streams.recording_state='on'` automatically via DB trigger
- if `YOUTUBE_STREAM_IDS` is set, runner captures only that explicit subset (assignment still required)
- launcher enforces stable identity: `WORKER_ID` must be `local-youtube-worker-${YOUTUBE_SERVER_ID}`

Cadence check (per stream, recent success rate and spacing):
```bash
source local/operator.env
for id in 3550 3552 3555 3556 3564; do
  echo "== $id =="
  curl -sS -H "Authorization: Bearer $API_TOKEN" \
    "$BACKEND_API_URL/api/v1/frames?stream_id=$id&limit=220" |
    jq -r '
      def to_ts: sub("\\.[0-9]+Z$";"Z") | fromdateiso8601;
      def deltas(a): [range(1; a|length) as $i | (a[$i]-a[$i-1])];
      (.items // []) as $items
      | ($items|map(select(.capture_status=="success") | (.captured_at|to_ts))|sort) as $succ
      | ($succ|map(select(. >= (now-60)))) as $s60
      | {
          latest_success:(if ($s60|length)>0 then ($s60[-1]|todateiso8601) else null end),
          success_last_60s:($s60|length),
          avg_delta_success_60s:(if ($s60|length)>1 then (deltas($s60)|add/length) else null end)
        }'
done
```
