# Stream Location AI Enrichment

This adds AI-based location correction passes for stream metadata.

What it does per stream:
1. Reads stream title/URLs/provider/tags/current location + metadata.
2. Pulls latest 3 captured frame images from `/api/v1/frames`.
3. Sends that bundle to an LLM for best-guess `country/country_code/city`.
4. Writes decision details into metadata (`geo_ai_v1` for OpenAI, `geo_ai_claude_v1` for Claude).
5. If confidence is high enough, updates stream location fields.

## Required env

Use `local/operator.env` (gitignored):

```bash
export BACKEND_API_URL='https://stoarama-api.onrender.com'
export API_TOKEN='replace-me'
export OPENAI_API_KEY='replace-me'
# optional for Claude Code runner:
export CLAUDE_BIN='claude'
```

## One-shot run (all streams, bounded batch)

```bash
cd /Users/deniz/Build/thesis/stoarama
source local/operator.env

python3 tools/enrich_stream_locations_openai.py \
  --backend-api-url "$BACKEND_API_URL" \
  --api-token "$API_TOKEN" \
  --openai-api-key "$OPENAI_API_KEY" \
  --model gpt-4o-mini \
  --max-streams 300 \
  --frame-count 3 \
  --min-confidence-apply 0.72 \
  --recheck-hours 168 \
  --apply \
  --report-path local/reports/location-ai-manual.json
```

Claude Code (Haiku default):

```bash
cd /Users/deniz/Build/thesis/stoarama
source local/operator.env

python3 tools/enrich_stream_locations_claude_code.py \
  --backend-api-url "$BACKEND_API_URL" \
  --api-token "$API_TOKEN" \
  --claude-bin "${CLAUDE_BIN:-claude}" \
  --model haiku \
  --max-streams 100 \
  --frame-count 3 \
  --min-confidence-apply 0.72 \
  --skip-openai-processed \
  --apply \
  --report-path local/reports/location-claude-manual.json
```

## Continuous sweeper (ingestion integration)

Run as long-lived process so newly imported streams are auto-corrected.

```bash
cd /Users/deniz/Build/thesis/stoarama
source local/operator.env
backend/scripts/start-location-ai-sweeper.sh
```

Install as launchd service on Mac mini:

```bash
cd /Users/deniz/Build/thesis/stoarama
SI_ENV_FILE='local/operator.env' \
  backend/scripts/install-location-ai-launchd.sh
```

Uninstall:

```bash
cd /Users/deniz/Build/thesis/stoarama
backend/scripts/uninstall-location-ai-launchd.sh
```

Defaults:
- model: `gpt-4o-mini`
- images per stream: `3`
- max streams per pass: `120`
- confidence threshold to apply location fields: `0.72`
- loop interval: `120s`

Tunable env vars:
- `LOCATION_AI_SWEEP_MODEL`
- `LOCATION_AI_SWEEP_MAX_STREAMS_PER_PASS`
- `LOCATION_AI_SWEEP_MIN_CONFIDENCE`
- `LOCATION_AI_SWEEP_RECHECK_HOURS`
- `LOCATION_AI_SWEEP_LOOP_SEC`
- `LOCATION_AI_SWEEP_TAGS` (optional CSV tag filter)

Claude sweeper (parallel option):

```bash
cd /Users/deniz/Build/thesis/stoarama
source local/operator.env
backend/scripts/start-location-claude-sweeper.sh
```

Install as launchd service on Mac mini:

```bash
cd /Users/deniz/Build/thesis/stoarama
SI_ENV_FILE='local/operator.env' \
  backend/scripts/install-location-claude-launchd.sh
```

Uninstall:

```bash
cd /Users/deniz/Build/thesis/stoarama
backend/scripts/uninstall-location-claude-launchd.sh
```

Key env knobs:
- `LOCATION_CLAUDE_SWEEP_MODEL` (default `haiku`)
- `LOCATION_CLAUDE_SWEEP_MAX_STREAMS_PER_PASS` (default `20`)
- `LOCATION_CLAUDE_SWEEP_MIN_CONFIDENCE` (default `0.72`)
- `LOCATION_CLAUDE_SWEEP_RECHECK_HOURS` (default `168`)
- `LOCATION_CLAUDE_SWEEP_LOOP_SEC` (default `180`)
- `LOCATION_CLAUDE_SWEEP_SKIP_OPENAI_PROCESSED` (default `1`)

## Metadata contract

Every processed stream gets:
- `metadata_json.geo_ai_pending=false`
- `metadata_json.geo_ai_v1` object with confidence, rationale, prior location, and decision.

For newly imported streams, importers now set:
- `metadata_json.geo_ai_pending=true`

That makes continuous sweeps part of ingestion by default.
