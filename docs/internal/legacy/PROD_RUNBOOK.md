# Production Runbook (Capture + Dashboard)

## Prereq
- `source local/operator.env`
- Requires `BACKEND_API_URL` and `API_TOKEN`.

## Health Checks
```bash
curl -sS "$BACKEND_API_URL/healthz"
curl -sS -H "Authorization: Bearer $API_TOKEN" "$BACKEND_API_URL/api/v1/dashboard/overview"
curl -sS -H "Authorization: Bearer $API_TOKEN" "$BACKEND_API_URL/api/v1/dashboard/queue-health"
curl -sS -H "Authorization: Bearer $API_TOKEN" "$BACKEND_API_URL/api/v1/dashboard/recording/summary?hours=24&runs_limit=20&events_limit=20"
curl -sS -H "Authorization: Bearer $API_TOKEN" "$BACKEND_API_URL/api/v1/dashboard/servers?hours=168" | jq '{active,total}'
curl -sS -H "Authorization: Bearer $API_TOKEN" "$BACKEND_API_URL/api/v1/dashboard/pipelines/overview?include_inactive=true" | jq '{pipelines_total,backlog_frames_total,active_claims_total}'

# inference queue health (boxed jobs pending)
curl -sS -H "Authorization: Bearer $API_TOKEN" \
  "$BACKEND_API_URL/api/v1/dashboard/inference?status=queued_boxed&limit=1&offset=0" |
  jq '{queued_boxed_total:.total}'
```

If `queued_boxed` rows keep growing, verify at least one box worker path is running:
- standalone service `stoarama-inference-box-worker`, or
- embedded API worker (`BOX_WORKER_EMBEDDED=true` on web service).

CLI equivalent (same model as dashboard `Overview/Pipelines/Servers/Recording` tabs):
```bash
cd backend
go run ./cmd/stoaramactl overview status --backend-api-url "$BACKEND_API_URL" --api-token "$API_TOKEN"
go run ./cmd/stoaramactl overview queue-health --backend-api-url "$BACKEND_API_URL" --api-token "$API_TOKEN"
go run ./cmd/stoaramactl pipelines list
go run ./cmd/stoaramactl servers list --backend-api-url "$BACKEND_API_URL" --api-token "$API_TOKEN" --hours 168 --show-processes
go run ./cmd/stoaramactl recording status --backend-api-url "$BACKEND_API_URL" --api-token "$API_TOKEN"
go run ./cmd/stoaramactl recording queue --backend-api-url "$BACKEND_API_URL" --api-token "$API_TOKEN"
go run ./cmd/stoaramactl recording runs --limit 50 --backend-api-url "$BACKEND_API_URL" --api-token "$API_TOKEN"
go run ./cmd/stoaramactl recording coverage --id <stream_id> --days 365 --backend-api-url "$BACKEND_API_URL" --api-token "$API_TOKEN"
go run ./cmd/stoaramactl recording samples --id <stream_id> --count 42 --backend-api-url "$BACKEND_API_URL" --api-token "$API_TOKEN"
```

Servers model:
- servers are grouped by machine/server identity (host/server_id), not raw process IDs.
- each server row now includes a `processes` list showing capture/inference processes and lease activity.

## Stream Inventory Checks
```bash
# recorded scope
curl -sS -H "Authorization: Bearer $API_TOKEN" \
  "$BACKEND_API_URL/api/v1/dashboard/streams?recording_state=on&limit=200&offset=0&include_image_urls=false"

# tag-scoped scope (match any tag)
curl -sS -H "Authorization: Bearer $API_TOKEN" \
  "$BACKEND_API_URL/api/v1/dashboard/streams?tags=youtube,seoul&limit=200&offset=0&include_image_urls=false"

# tag-scoped city options (for dashboard filters)
curl -sS -H "Authorization: Bearer $API_TOKEN" \
  "$BACKEND_API_URL/api/v1/dashboard/cities?scope=all&tags=youtube,seoul"

# country options (for dashboard location filters)
curl -sS -H "Authorization: Bearer $API_TOKEN" \
  "$BACKEND_API_URL/api/v1/dashboard/countries?scope=all"
```

## Stream Tagging (CLI-first)
```bash
cd backend
source ../local/youtube-worker.env

# add tags
go run ./cmd/stoaramactl streams tags-add --id <stream_id> --tags seoul,crosswalk

# remove tags
go run ./cmd/stoaramactl streams tags-remove --id <stream_id> --tags crosswalk

# list with tag filter
go run ./cmd/stoaramactl streams list --tags seoul --limit 200

# set explicit location hierarchy (non-tag location system)
go run ./cmd/stoaramactl streams update --id <stream_id> \
  --location-country "South Korea" \
  --location-country-code KR \
  --location-region Seoul \
  --location-city Seoul \
  --location-locality Gangnam \
  --location-source manual
```

## Manual Stream Review (Flagged/Unsupported)
Use latest successful frame to validate that a stream is still a public-space camera:

```bash
source local/operator.env

# inspect assigned streams with unsupported runtime
curl -sS -H "Authorization: Bearer $API_TOKEN" \
  "$BACKEND_API_URL/api/v1/capture/runtime?status=unsupported&limit=200&offset=0" |
  jq -r '.items[] | [.stream_id, .name, .execution_class, (.last_error_text // "")] | @tsv'

# fetch one latest success frame URL for a stream
STREAM_ID=3552
curl -sS -H "Authorization: Bearer $API_TOKEN" \
  "$BACKEND_API_URL/api/v1/frames?stream_id=$STREAM_ID&limit=300" |
  jq -r '.items[] | select(.capture_status=="success") | .download_url' | head -n 1
```

If a flagged stream is invalid/unusable (for example persistent placeholder/loading feed), unassign recording and move it to `archive`:

```bash
STREAM_ID=3552
curl -sS -X POST -H "Authorization: Bearer $API_TOKEN" -H "Content-Type: application/json" \
  -d '{"confirm":"unassign:'"$STREAM_ID"'","reason":"operator archive","actor":"runbook"}' \
  "$BACKEND_API_URL/api/v1/recording/streams/$STREAM_ID/unassign"
curl -sS -X PATCH -H "Authorization: Bearer $API_TOKEN" -H "Content-Type: application/json" \
  -d '{"tags":["archive"]}' \
  "$BACKEND_API_URL/api/v1/streams/$STREAM_ID"
```

## Stream Metadata Audit (CLI-first)
```bash
cd backend
source ../local/operator.env

# dry-run: report missing/mismatched metadata city and generic-location mismatches
go run ./cmd/stoaramactl streams metadata-audit --page-size 500 --sample-limit 120

# safe apply: updates metadata_json.city from location_text city token
# (does NOT rewrite generic location_text buckets unless explicitly enabled)
go run ./cmd/stoaramactl streams metadata-audit --page-size 500 --apply --max-updates 300

# optional: include generic city buckets in metadata backfill
go run ./cmd/stoaramactl streams metadata-audit --page-size 500 --allow-generic-location-city --apply --max-updates 300

# optional/high-risk: also rewrite generic location_text using inferred city from stream name
go run ./cmd/stoaramactl streams metadata-audit --page-size 500 --apply --apply-generic-location-fixes --max-updates 50
```

## Canonical Stream Migration
```bash
cd backend
source ../local/operator.env

# dry-run and write report
go run ./cmd/stoaramactl streams migrate-v2 --json --report-json ../local/stream-migrate-v2.json

# apply only after review_required=0
go run ./cmd/stoaramactl streams migrate-v2 --apply
```

## Discovery Candidate Review / Import
```bash
cd backend
source ../local/operator.env

# review pending candidates
go run ./cmd/stoaramactl discovery candidates list --review-status pending --limit 100

# accept one candidate
go run ./cmd/stoaramactl discovery candidates review --id <candidate_id> --status accepted --reason "operator review"

# import accepted candidate as a canonical stream
go run ./cmd/stoaramactl discovery candidates import --id <candidate_id> --tags discovery,reviewed
```

## Recording Controls
```bash
# inspect live server capacity (assignment targets)
curl -sS -H "Authorization: Bearer $API_TOKEN" \
  "$BACKEND_API_URL/api/v1/dashboard/recording/server-capacity"

# assign one stream to one server (source of truth for workers)
curl -sS -X POST -H "Authorization: Bearer $API_TOKEN" -H "Content-Type: application/json" \
  -d '{"server_id":"do-555140998","reason":"operator assign","actor":"runbook"}' \
  "$BACKEND_API_URL/api/v1/recording/streams/<id>/assign"

# unassign one stream (destructive)
curl -sS -X POST -H "Authorization: Bearer $API_TOKEN" -H "Content-Type: application/json" \
  -d '{"confirm":"unassign:<id>","reason":"operator stop","actor":"runbook"}' \
  "$BACKEND_API_URL/api/v1/recording/streams/<id>/unassign"

# list assignments
curl -sS -H "Authorization: Bearer $API_TOKEN" \
  "$BACKEND_API_URL/api/v1/recording/assignments?limit=200&offset=0"

# read global recording interval
curl -sS -H "Authorization: Bearer $API_TOKEN" \
  "$BACKEND_API_URL/api/v1/dashboard/recording/settings"

# update global recording interval
curl -sS -X PUT -H "Authorization: Bearer $API_TOKEN" -H "Content-Type: application/json" \
  -d '{"interval_sec":1}' \
  "$BACKEND_API_URL/api/v1/dashboard/recording/settings"
```

Capacity groups (admission control):
```bash
# view capacity-group totals + active usage
curl -sS -H "Authorization: Bearer $API_TOKEN" \
  "$BACKEND_API_URL/api/v1/dashboard/recording/capacity"

# capacity writes are intentionally disabled (conflict)
curl -sS -X PUT -H "Authorization: Bearer $API_TOKEN" -H "Content-Type: application/json" \
  -d '{"items":[{"execution_class":"video_live","max_active":6}]}' \
  "$BACKEND_API_URL/api/v1/dashboard/recording/capacity"
```

Change capacity at server monitor startup (`capture-server run --capture-shared-capacity ...` / `youtube-relay sink run --capacity ...`); backend capacity is derived from active heartbeat rows only.
`video_live` + `image_poll` share one per-server capacity group (`capture_shared`) for assignment/free-slot accounting.

`POST /api/v1/recording/streams/<id>/assign` returns `409` with explicit `error_code` (`server_unavailable`, `server_draining`, `capacity_reached`, `recording_mode_unsupported`) when assignment cannot be admitted.

## Capture Server Execution-Class Split
- Non-YouTube capture runs on DO capture servers via `stoaramactl capture-server run` with:
  - `--capture-shared-capacity 6`
  - `--heartbeat-sec 15 --lease-sec 45`
  - fail-fast stable server identity via `CAPTURE_SERVER_ID` (defaulting to stable hostname if unset).
- `video_live` uses the same capture-server CLI/process model for HLS and direct-video streams; URL resolution differs by `capture_type`.
- Local Mac worker should run:
  - YouTube direct/relay execution classes only
  - `backend/scripts/start-local-youtube-worker.sh` (same persistent manager path, API persistence mode only)

## DO Capture Fleet (Non-YouTube)
- Terraform root: `infra/do-capture`
- Per-node service: `stoarama-capture.service`
- Runtime command: `backend/scripts/start-capture-server.sh` -> `stoaramactl capture-server run`
- Capacity and assignment are backend-managed:
  - capacity heartbeat (`server_execution_capacity`)
  - assignments (`recording_assignments`)
 - `start-capture-server.sh` fails fast when stable `CAPTURE_SERVER_ID` cannot be resolved.
 - `CAPTURE_SERVER_STREAM_IDS` is disabled in the default launcher; use assignment-managed mode in prod.

Quick verify:
```bash
cd backend
source ../local/operator.env
go run ./cmd/stoaramactl servers list --backend-api-url "$BACKEND_API_URL" --api-token "$API_TOKEN" --hours 168
go run ./cmd/stoaramactl servers capacity list --backend-api-url "$BACKEND_API_URL" --api-token "$API_TOKEN"
go run ./cmd/stoaramactl servers assignments --backend-api-url "$BACKEND_API_URL" --api-token "$API_TOKEN"
```

## Local YouTube Worker (Persistent)
- Full setup: `docs/LOCAL_YOUTUBE_CAPTURE.md`
- API mode default: local worker needs only `BACKEND_API_URL` + `API_TOKEN` (+ yt-dlp cookies).
- Backend API persists frames/runtime to Postgres + R2 via capture ingest endpoints.
- Local worker should be assigned streams via `/api/v1/recording/streams/<id>/assign`; assignment scope is authoritative.
- When `YOUTUBE_STREAM_IDS` is empty, local worker defaults to `max_sessions=YOUTUBE_RUNNER_CAPACITY` over assigned YouTube streams for that server.
- `YOUTUBE_STREAM_IDS` is disabled in the default launcher unless `ALLOW_UNMANAGED_STREAM_FILTER=1` is set for explicit debug-only runs.
- Auto-discovery uses assignment endpoint `GET /api/v1/recording/assignments?server_id=...` rather than dashboard stream list endpoints.
- Throughput tuning without lowering output resolution is available via env/CLI:
  - `YT_DLP_FORMAT` / `--yt-dlp-format` (recommended default: `bestvideo[vcodec^=avc1]/bestvideo/best`)
  - `CAPTURE_FFMPEG_JPEG_Q` / `--ffmpeg-jpeg-quality` (recommended: `4`)
  - `CAPTURE_FFMPEG_HWACCEL` / `--ffmpeg-hwaccel` (Apple Silicon: `videotoolbox`)
  - `CAPTURE_FFMPEG_THREADS`, `CAPTURE_FFMPEG_RECONNECT`, `CAPTURE_FFMPEG_RECONNECT_DELAY_MAX_SEC`

```bash
cd backend
./scripts/start-local-youtube-worker.sh --duration 5m
```

Optional stream subset:
```bash
export YOUTUBE_STREAM_IDS='3550,3552'
cd backend
./scripts/start-local-youtube-worker.sh
```

Detached long-running local worker (recommended):
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

Raw CLI equivalent:
```bash
cd backend
go run ./cmd/stoaramactl youtube-relay source run \
  --backend-api-url "$BACKEND_API_URL" \
  --api-token "$API_TOKEN" \
  --server-id "${YOUTUBE_SERVER_ID:-$(hostname -s)}" \
  --worker-id "local-youtube-worker-${YOUTUBE_SERVER_ID:-$(hostname -s)}" \
  --capacity "${YOUTUBE_RUNNER_CAPACITY:-4}" \
  --heartbeat-sec "${YOUTUBE_HEARTBEAT_SEC:-15}" \
  --lease-sec "${YOUTUBE_LEASE_SEC:-45}" \
  --refresh-sec "${CAPTURE_TICK_SEC:-5}" \
  --frame-queue-size "${CAPTURE_FRAME_QUEUE_SIZE:-256}" \
  --frame-enqueue-timeout-sec "${CAPTURE_FRAME_ENQUEUE_TIMEOUT_SEC:-8}" \
  --frame-writer-workers "${CAPTURE_FRAME_WRITERS:-6}" \
  --recording-heartbeat=false \
  --yt-dlp-cookies-file "$YT_DLP_COOKIES_FILE"
```

## YouTube Relay (Source/Sink Split)
`0020_youtube_relay_control_plane.sql` adds:
- mode: `youtube_relay`
- control-plane tables: `youtube_relay_sources`, `youtube_relay_routes`, `youtube_relay_events`

CLI-first flow:
```bash
source local/operator.env
cd backend

# source node (Mac with yt-dlp cookies): resolves YouTube URLs and heartbeats source capacity
go run ./cmd/stoaramactl youtube-relay source run \
  --backend-api-url "$BACKEND_API_URL" \
  --api-token "$API_TOKEN" \
  --server-id "mini-source-1" \
  --shard-id "yt-account-1" \
  --capacity 8

# sink node (server): pulls relay URLs and ingests frames
go run ./cmd/stoaramactl youtube-relay sink run \
  --backend-api-url "$BACKEND_API_URL" \
  --api-token "$API_TOKEN" \
  --server-id "do-sink-1" \
  --worker-id "yt-relay-sink-do-sink-1" \
  --capacity 8

# inspect active routes
go run ./cmd/stoaramactl youtube-relay routes \
  --backend-api-url "$BACKEND_API_URL" \
  --api-token "$API_TOKEN"
```

Wrapper scripts:
- Source: `backend/scripts/start-youtube-relay-source.sh`
- Sink: `backend/scripts/start-youtube-relay-sink.sh`
- Source launchd install (Mac): `backend/scripts/install-local-youtube-relay-source-launchd.sh`
- Source launchd uninstall (Mac): `backend/scripts/uninstall-local-youtube-relay-source-launchd.sh`
- DO env bootstrap: `backend/scripts/bootstrap-do-youtube-relay-env.sh`
- Source env bootstrap: `backend/scripts/bootstrap-youtube-relay-source-env.sh`

Network/deploy reference:
- `docs/YOUTUBE_RELAY_WIREGUARD.md`
- `docs/DO_YOUTUBE_RELAY_HUB.md`

Assignment behavior:
- when assigning a `youtube_live` stream to a server with `youtube_relay` capacity, assignment mode is promoted to `youtube_relay`
- assignment fails fast with `youtube_relay_source_unavailable` when no active source capacity exists
- unassigning relay streams clears route rows and emits relay events

## Local CLI Capture Smoke (Non-mutating Probe)
```bash
cd backend
go run ./cmd/stoaramactl capture probe --provider KBS --source-url "<stream_url_or_m3u8>" --capture-type hls
go run ./cmd/stoaramactl capture probe --provider GIGAEYES --source-url "<youtube_watch_url>" --capture-type youtube_watch --execution-class youtube_relay
go run ./cmd/stoaramactl capture probe --id <stream_id> --capture-timeout-sec 45 --save-frame-dir ../local/reports/probe_frames
```

## Non-YouTube Runtime Validation
If you enable additional `hls_live`, `image_poll`, or `ffmpeg_direct` streams by setting recording `on`, Render worker should pick them up after recording apply.

```bash
source local/operator.env
curl -sS -H "Authorization: Bearer $API_TOKEN" \
  "$BACKEND_API_URL/api/v1/capture/runtime?limit=200&offset=0"

curl -sS -H "Authorization: Bearer $API_TOKEN" \
  "$BACKEND_API_URL/api/v1/dashboard/streams/<id>?include_image_urls=false"

curl -sS -H "Authorization: Bearer $API_TOKEN" \
  "$BACKEND_API_URL/api/v1/dashboard/streams/<id>/coverage?days=365" | jq '{days,start_day,end_day,summary}'

curl -sS -H "Authorization: Bearer $API_TOKEN" \
  "$BACKEND_API_URL/api/v1/dashboard/streams/<id>/capture-samples?count=24" | jq '{requested_count,available_days,selected_days,samples:(.items|length)}'
```

Per-stream cadence check (recent success frames):
```bash
source local/operator.env
for id in 3550 3552 3555 3556 3564; do
  curl -sS -H "Authorization: Bearer $API_TOKEN" \
    "$BACKEND_API_URL/api/v1/frames?stream_id=$id&limit=220" |
    jq '{stream:'"$id"', success_60s:([.items[] | select(.capture_status=="success") | .captured_at] | length)}'
done
```

## Backfill + Inference Image Validation
Validate boxed coverage for inference rows:

```bash
source local/operator.env
curl -sS -H "Authorization: Bearer $API_TOKEN" \
  "$BACKEND_API_URL/api/v1/dashboard/inference?limit=200&offset=0" |
  jq '{
    total: (.items|length),
    queued_boxed: ([.items[] | select((.status // "") == "queued_boxed")] | length),
    boxed_present: ([.items[] | select((.boxed_object_key // "") != "")] | length),
    boxed_missing_with_raw: ([.items[] | select((.boxed_object_key // "") == "" and (.raw_object_key // "") != "")] | length)
  }'
```

Expected:
- `status=queued_boxed` rows are expected transiently while backend boxing worker is rendering/uploading boxed artifacts.
- `boxed_object_key` must be present for `status=success` rows with detections.
- `boxed_object_key` can be empty only for no-detection `success` rows and `error` rows.
- `raw_object_key` should still be present when a frame exists.
- Dashboard should show boxed image for `success` detection rows; `queued_boxed` rows should show queued state until boxed artifact lands.

Requeue invalid historical rows (success + detections + missing boxed) into backend boxing queue:
```bash
source local/operator.env
curl -sS -X POST -H "Authorization: Bearer $API_TOKEN" \
  "$BACKEND_API_URL/api/v1/dashboard/inference/cleanup-unboxed?pipeline_id=yolo11x__tile640-o25-img1280__balanced&mode=requeue&dry_run=true" | jq
curl -sS -X POST -H "Authorization: Bearer $API_TOKEN" \
  "$BACKEND_API_URL/api/v1/dashboard/inference/cleanup-unboxed?pipeline_id=yolo11x__tile640-o25-img1280__balanced&mode=requeue" | jq
```

Hard delete mode remains available for manual remediation only:
```bash
curl -sS -X POST -H "Authorization: Bearer $API_TOKEN" \
  "$BACKEND_API_URL/api/v1/dashboard/inference/cleanup-unboxed?pipeline_id=yolo11x__tile640-o25-img1280__balanced&mode=delete&dry_run=true" | jq
```

## Inference CLI (Backlog Drain)
Canonical service setup and scheduled launchd runs on other computers: `docs/INFERCTL_SERVICE.md`.

Use local inference runner on a RAM-heavy machine to process frames without a successful result for a pipeline.
Frames with only error results remain claimable so backlog drain can retry them.

Setup:
```bash
cd /Users/deniz/Build/thesis/stoarama
python3 -m venv local/.venv-inferctl
source local/.venv-inferctl/bin/activate
python -m pip install -e inferctl
source local/operator.env
export INFERCTL_SERVER_ID='mini-a'
inferctl pipelines sync --api-url "$INFERCTL_API_URL" --token "$INFERCTL_API_TOKEN" --file inferctl/pipelines/vlm-pipelines.json
```

VLM smoke runs (same claim/commit path as detector pipelines):
```bash
inferctl run \
  --api-url "$INFERCTL_API_URL" \
  --token "$INFERCTL_API_TOKEN" \
  --server-id "$INFERCTL_SERVER_ID" \
  --pipeline-id siglip__people-presence__v1 \
  --stream-ids 3557,3550,3518 \
  --limit 100 \
  --workers 1 \
  --boxed-mode none

inferctl run \
  --api-url "$INFERCTL_API_URL" \
  --token "$INFERCTL_API_TOKEN" \
  --server-id "$INFERCTL_SERVER_ID" \
  --pipeline-id clipseg__person-mask__v1 \
  --stream-ids 3557,3550,3518 \
  --limit 100 \
  --workers 1 \
  --boxed-mode none
```

SAM3 note:
- `sam3__auto-mask__v1` (official CUDA path) is intentionally inactive by default.
- `sam3__maskgen-modelscope__v1` (transformers mask-generation + ModelScope mirror) is also inactive by default.
- HF `facebook/sam3` is gated; without approved HF token, pipeline init fails.
- ModelScope downloads are large; validate full artifact availability + runtime on the target host before activation.
- `sam3__maskgen-modelscope__v1` supports strict mirror validation:
  - `modelscope_verify=size` (default) for manifest size checks.
  - `modelscope_verify=sha256` for full hash checks (slow but strict).

Candidate check:
```bash
inferctl run \
  --api-url "$INFERCTL_API_URL" \
  --token "$INFERCTL_API_TOKEN" \
  --server-id "$INFERCTL_SERVER_ID" \
  --pipeline-id yolo11x__tile640-o25-img1280__balanced \
  --stream-ids 3550,3552,3555,3556,3564 \
  --limit 50 \
  --dry-run
```

Drain selected stream backlog:
```bash
inferctl run \
  --api-url "$INFERCTL_API_URL" \
  --token "$INFERCTL_API_TOKEN" \
  --server-id "$INFERCTL_SERVER_ID" \
  --pipeline-id yolo11x__tile640-o25-img1280__balanced \
  --stream-ids 3550,3552,3555,3556,3564 \
  --limit 200 \
  --workers 1 \
  --until-empty
```

Fail-fast note:
- If a full batch fails with zero successes while `--until-empty` is enabled, `inferctl` exits early with non-zero status so failures are explicit and do not spin indefinitely.
- Backend enforces claim ownership (`claimed_by`) on claim/commit/fail, so leases cannot be committed by a different runner/machine.

Overnight bounded run (8h window):
```bash
inferctl run \
  --api-url "$INFERCTL_API_URL" \
  --token "$INFERCTL_API_TOKEN" \
  --server-id "$INFERCTL_SERVER_ID" \
  --pipeline-id yolo11x__tile640-o25-img1280__balanced \
  --stream-ids 3550,3552,3555,3556,3564 \
  --limit 200 \
  --workers 1 \
  --until-empty \
  --idle-max-polls 960 \
  --poll-sec 30 \
  --max-batches 5000
```

Parallel shards on one machine:
```bash
inferctl run --api-url "$INFERCTL_API_URL" --token "$INFERCTL_API_TOKEN" --server-id "$INFERCTL_SERVER_ID" --pipeline-id yolo11x__tile640-o25-img1280__balanced --stream-ids 3550,3552 --limit 100 --workers 1 --until-empty --claimed-by inferctl:night-a
inferctl run --api-url "$INFERCTL_API_URL" --token "$INFERCTL_API_TOKEN" --server-id "$INFERCTL_SERVER_ID" --pipeline-id yolo11x__tile640-o25-img1280__balanced --stream-ids 3555,3556,3564 --limit 100 --workers 1 --until-empty --claimed-by inferctl:night-b
```

Post-run verification:
```bash
for id in 3550 3552 3555 3556 3564; do
  curl -sS -H "Authorization: Bearer $API_TOKEN" \
    "$BACKEND_API_URL/api/v1/dashboard/streams/$id/detections?pipeline_id=yolo11x__tile640-o25-img1280__balanced&limit=1" |
    jq '{stream:'"$id"', latest_result:.latest_result}'
done
```

## Local Throughput Supervisor (inferctl native)
Use `inferctl supervisor run` for long-running local inference with staged worker ramp and automatic reconciliation.

Start now (foreground):
```bash
inferctl supervisor run \
  --api-url "$INFERCTL_API_URL" \
  --token "$INFERCTL_API_TOKEN" \
  --server-id "$INFERCTL_SERVER_ID" \
  --pipeline-id yolo11x__tile640-o25-img1280__balanced \
  --stream-ids 3550,3552,3555,3556,3564 \
  --shards 4 \
  --start-workers 4 \
  --target-workers 12 \
  --ramp-step 1 \
  --ramp-every-sec 180 \
  --limit 80 \
  --run-workers 1 \
  --batch-size 1 \
  --idle-max-polls 240 \
  --poll-sec 15 \
  --worker-log-dir local/logs/inferctl-supervisor
```

Scheduled overnight (9pm-9am, plugged in):
```bash
inferctl supervisor run \
  --api-url "$INFERCTL_API_URL" \
  --token "$INFERCTL_API_TOKEN" \
  --server-id "$INFERCTL_SERVER_ID" \
  --pipeline-id yolo11x__tile640-o25-img1280__balanced \
  --stream-ids 3550,3552,3555,3556,3564 \
  --shards 4 \
  --start-workers 4 \
  --target-workers 12 \
  --window-start 21:00 \
  --window-end 09:00 \
  --require-ac-power \
  --limit 80 \
  --run-workers 1 \
  --batch-size 1 \
  --idle-max-polls 240 \
  --poll-sec 15 \
  --worker-log-dir local/logs/inferctl-supervisor
```

Install launchd service:
```bash
inferctl supervisor launchd-install \
  --env-file local/inferctl.env \
  --label io.stoarama.inferctl-supervisor \
  --working-dir /Users/deniz/Build/thesis/stoarama \
  -- --pipeline-id yolo11x__tile640-o25-img1280__balanced --stream-ids 3550,3552,3555,3556,3564 --shards 4 --start-workers 4 --target-workers 12 --window-start 21:00 --window-end 09:00 --require-ac-power --limit 80 --run-workers 1 --batch-size 1 --idle-max-polls 240 --poll-sec 15 --worker-log-dir local/logs/inferctl-supervisor
```

Check/remove launchd service:
```bash
inferctl supervisor launchd-status --label io.stoarama.inferctl-supervisor
inferctl supervisor launchd-uninstall --label io.stoarama.inferctl-supervisor
```

Notes:
- Keep worker startup staggered (`--start-workers`, `--ramp-step`, `--ramp-every-sec`) to reduce claim/lease contention.
- Keep per-worker claim limit moderate (`--limit 80` baseline from recent sustained tests).
- Default behavior is fail-fast on any non-zero worker exit; use `--continue-on-worker-error` only when explicitly desired.

Count inferences committed in a `created_at` window (not `captured_at`):
```bash
cd /Users/deniz/Build/thesis/stoarama
./local/scripts/inference_count_created_window.sh --hours 8 \
  --pipeline-id yolo11x__tile640-o25-img1280__balanced \
  --statuses success,queued_boxed,error
```

## Add Streams Safely (Support + Onboarding)
1) Classify current support mix:
```bash
cd backend
go run ./cmd/stoaramactl streams migrate-v2 --json |
  jq '{total:(.items|length), by_capture_type:(.items|group_by(.proposed_capture_type)|map({capture_type:.[0].proposed_capture_type,count:length}))}'
```

Runtime unsupported/error list:
```bash
go run ./cmd/stoaramactl capture runtime list --limit 5000 --json |
  jq '.items[] | select(.status=="unsupported" or .status=="error" or .status=="stopped") |
      {stream_id,slug,status,execution_class,last_error_text}'
```

2) Probe candidate stream before recording enable:
```bash
go run ./cmd/stoaramactl capture probe --id <stream_id> --capture-timeout-sec 45
```

3) Assign stream to a live recording server:
```bash
go run ./cmd/stoaramactl servers capacity list --backend-api-url "$BACKEND_API_URL" --api-token "$API_TOKEN"
go run ./cmd/stoaramactl servers assign --id <stream_id> --server-id <server_id> \
  --backend-api-url "$BACKEND_API_URL" --api-token "$API_TOKEN"
```

4) Verify runtime/freshness:
```bash
go run ./cmd/stoaramactl capture runtime show --id <stream_id> --json
curl -sS -H "Authorization: Bearer $API_TOKEN" \
  "$BACKEND_API_URL/api/v1/frames?stream_id=<stream_id>&limit=30"
```

5) Keep workers split by execution class:
- DO capture worker: `video_live,image_poll`
- Local Mac source: `youtube_relay`

## Known Failure Mode
- YouTube on Render may fail with:
  - `Sign in to confirm you’re not a bot`
- This is expected for server egress in current setup; use local YouTube worker.
