# Catalog Sweepers (Non-Recording Coverage)

These sweepers keep recent preview images flowing for streams that are **not** explicitly recording.

## 1) DO Catalog Sweeper (HLS/Image/FFmpeg)

Runs on a dedicated DO droplet and loops over:
- `recording_state=off`
- `execution_class in (video_live,image_poll)`
- ordered by `latest_captured_at ASC` (oldest first)

Runtime script:
- `backend/scripts/start-capture-catalog-sweeper.sh`

Terraform stack:
- `infra/do-capture-catalog`

Deploy:
```bash
cd infra/do-capture-catalog
source ../../local/do-capture.env
terraform init
terraform plan
terraform apply
```

Key env knobs (set in cloud-init or env file):
- `CAPTURE_CATALOG_SWEEP_EXECUTION_CLASSES` (default `video_live,image_poll`)
- `CAPTURE_CATALOG_SWEEP_BATCH_PER_CLASS` (default `10`)
- `CAPTURE_CATALOG_SWEEP_MAX_STREAMS` (default `30`)
- `CAPTURE_CATALOG_SWEEP_DURATION` (default `4m`)

## 2) Mac Catalog Sweeper (YouTube)

Runs on Mac mini and loops over:
- `recording_state=off`
- `execution_class=youtube_relay`
- ordered by `latest_captured_at ASC`

Runtime script:
- `backend/scripts/start-local-youtube-catalog-sweeper.sh`

Manual start:
```bash
cd /Users/deniz/Build/thesis/stoarama
SI_ENV_FILE='local/youtube-worker.env' \
  backend/scripts/start-local-youtube-catalog-sweeper.sh
```

Launchd install/uninstall:
```bash
SI_ENV_FILE='local/youtube-worker.env' \
  backend/scripts/install-local-youtube-catalog-launchd.sh

backend/scripts/uninstall-local-youtube-catalog-launchd.sh
```

Key env knobs:
- `YOUTUBE_CATALOG_SWEEP_MAX_STREAMS` (default `CAPTURE_MAX_SESSIONS`, else `CAPTURE_CONCURRENCY`, else `4`)
- `YOUTUBE_CATALOG_SWEEP_BATCH_SIZE` (default equals `YOUTUBE_CATALOG_SWEEP_MAX_STREAMS`)
- `YOUTUBE_CATALOG_SWEEP_DURATION` (default `4m`)

## 3) Location AI Sweeper (All Streams)

Runs OpenAI-based location correction continuously:
- reads stream metadata + latest 3 captured images
- infers most likely `country/city`
- writes decision details to `metadata_json.geo_ai_v1`
- applies location field updates above confidence threshold

Runtime script:
- `backend/scripts/start-location-ai-sweeper.sh`

Manual start:
```bash
cd /Users/deniz/Build/thesis/stoarama
source local/operator.env
backend/scripts/start-location-ai-sweeper.sh
```

Launchd install/uninstall:
```bash
SI_ENV_FILE='local/operator.env' \
  backend/scripts/install-location-ai-launchd.sh

backend/scripts/uninstall-location-ai-launchd.sh
```

Required env:
- `BACKEND_API_URL`
- `API_TOKEN`
- `OPENAI_API_KEY`

Key env knobs:
- `LOCATION_AI_SWEEP_MODEL` (default `gpt-4o-mini`)
- `LOCATION_AI_SWEEP_MAX_STREAMS_PER_PASS` (default `120`)
- `LOCATION_AI_SWEEP_MIN_CONFIDENCE` (default `0.72`)
- `LOCATION_AI_SWEEP_RECHECK_HOURS` (default `168`)
- `LOCATION_AI_SWEEP_LOOP_SEC` (default `120`)

## 4) Location Claude Sweeper (Parallel Provider)

Runs Claude Code-based location correction continuously:
- reads stream metadata + latest 3 captured image URLs
- infers most likely `country/city`
- writes decision details to `metadata_json.geo_ai_claude_v1`
- applies location updates above confidence threshold
- defaults to skipping streams already processed by OpenAI (`geo_ai_v1`)

Runtime script:
- `backend/scripts/start-location-claude-sweeper.sh`

Manual start:
```bash
cd /Users/deniz/Build/thesis/stoarama
source local/operator.env
backend/scripts/start-location-claude-sweeper.sh
```

Launchd install/uninstall:
```bash
SI_ENV_FILE='local/operator.env' \
  backend/scripts/install-location-claude-launchd.sh

backend/scripts/uninstall-location-claude-launchd.sh
```

Required env:
- `BACKEND_API_URL`
- `API_TOKEN`
- Claude Code auth on host (`claude -p` works)

Key env knobs:
- `LOCATION_CLAUDE_SWEEP_MODEL` (default `haiku`)
- `LOCATION_CLAUDE_SWEEP_MAX_STREAMS_PER_PASS` (default `20`)
- `LOCATION_CLAUDE_SWEEP_MIN_CONFIDENCE` (default `0.72`)
- `LOCATION_CLAUDE_SWEEP_RECHECK_HOURS` (default `168`)
- `LOCATION_CLAUDE_SWEEP_LOOP_SEC` (default `180`)
- `LOCATION_CLAUDE_SWEEP_SKIP_OPENAI_PROCESSED` (default `1`)
