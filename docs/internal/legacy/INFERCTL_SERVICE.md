# Inferctl Service Setup (Other Computers)

Use this guide to run inference as a scheduled, set-and-forget local service on another Mac.

## 1) One-Time Setup Per Computer

```bash
cd /absolute/path/to
git clone https://github.com/daydemir/stoarama.git stoarama
cd stoarama
git checkout main
git pull --ff-only origin main
```

Create Python env and install `inferctl`:

```bash
python3 -m venv local/.venv-inferctl
source local/.venv-inferctl/bin/activate
python -m pip install -U pip
python -m pip install -e inferctl
```

Create machine-local env file (do not commit):

```bash
cat > local/inferctl.env <<'EOF'
export INFERCTL_API_URL='https://stoarama-api.onrender.com'
export INFERCTL_API_TOKEN='replace-me'
export INFERCTL_SERVER_ID='<machine-name>'
EOF
```

`INFERCTL_SERVER_ID` must stay constant per physical machine. Use short lowercase IDs (for example `mini-a`, `mbp64-a`).

## 2) Validate Before Scheduling

```bash
source local/.venv-inferctl/bin/activate
source local/inferctl.env

inferctl doctor
inferctl pipelines list
inferctl pipelines sync --file inferctl/pipelines/vlm-pipelines.json
cd backend && go run ./cmd/stoaramactl servers list --backend-api-url "$INFERCTL_API_URL" --api-token "$INFERCTL_API_TOKEN" --hours 24 --show-processes && cd ..
inferctl run --pipeline-id yolo11x__tile640-o25-img1280__balanced --stream-ids 3550,3552 --limit 10 --dry-run
inferctl run --pipeline-id siglip__people-presence__v1 --stream-ids 3557,3550 --limit 5 --dry-run
inferctl stream-tags run --max-streams 3 --samples-per-stream 2 --dry-run
```

Fail fast rule: if any command above fails, stop and fix before installing launchd.

## 3) Install Scheduled Service (9pm-9am, AC power only)

Use a unique label per computer:

```bash
source local/.venv-inferctl/bin/activate

inferctl supervisor launchd-install \
  --env-file local/inferctl.env \
  --label io.stoarama.inferctl-supervisor.<machine-name> \
  --working-dir /absolute/path/to/stoarama \
  -- --pipeline-id yolo11x__tile640-o25-img1280__balanced --server-id "$INFERCTL_SERVER_ID" --stream-ids 3550,3552,3555,3556,3564 --shards 4 --start-workers 4 --target-workers 12 --ramp-step 1 --ramp-every-sec 180 --window-start 21:00 --window-end 09:00 --require-ac-power --limit 80 --run-workers 1 --batch-size 1 --idle-max-polls 240 --poll-sec 15 --worker-log-dir local/logs/inferctl-supervisor
```

## 4) Status, Logs, Restart

```bash
source local/.venv-inferctl/bin/activate

inferctl supervisor launchd-status --label io.stoarama.inferctl-supervisor.<machine-name>

tail -n 120 local/logs/launchd/io.stoarama.inferctl-supervisor.<machine-name>.out.log
tail -n 120 local/logs/launchd/io.stoarama.inferctl-supervisor.<machine-name>.err.log
tail -n 120 local/logs/inferctl-supervisor/inferctl-supervisor-w0.log
```

Force restart:

```bash
launchctl kickstart -k "gui/$(id -u)/io.stoarama.inferctl-supervisor.<machine-name>"
```

## 5) Update Service After New Pushes

```bash
cd /absolute/path/to/stoarama
git pull --ff-only origin main
source local/.venv-inferctl/bin/activate
python -m pip install -e inferctl
launchctl kickstart -k "gui/$(id -u)/io.stoarama.inferctl-supervisor.<machine-name>"
```

## 6) Uninstall

```bash
source local/.venv-inferctl/bin/activate
inferctl supervisor launchd-uninstall --label io.stoarama.inferctl-supervisor.<machine-name>
```

## Notes
- `supervisor run` is fail-fast by default on non-zero worker exit.
- `--continue-on-worker-error` is available but not default.
- Backend enforces claim ownership: each lease is bound to `claimed_by`, and only that runner can commit/fail the claim.
- Keep secrets only in `local/inferctl.env`; never commit tokens.
- On non-Mac hosts, run `inferctl supervisor run` directly (no launchd commands).
- VLM runs (`siglip__people-presence__v1`, `clipseg__person-mask__v1`) use the exact same claim/commit path as detector pipelines.
- `inferctl stream-tags run` is separate from frame-claim inference: it samples existing frames per stream and PATCHes stream-level tags (`scene:*`, `height:*`, `camera:*`).
- For large catalogs, batch it with `--stream-offset N --max-streams M` and iterate offsets.
- Use `inferctl/scripts/run_stream_tags_full.sh` for a resumable full-catalog loop (`BATCH_SIZE`, `START_OFFSET`, `EXTRA_ARGS` supported via env vars).
- SAM3 has two inactive candidates in `inferctl/pipelines/vlm-pipelines.json`:
  - `sam3__auto-mask__v1`: official CUDA package path (`checkpoint_path` + `config_path`).
  - `sam3__maskgen-modelscope__v1`: transformers mask-generation path with optional ModelScope mirror download.
- Mirror validation options for `sam3__maskgen-modelscope__v1`:
  - `modelscope_verify=size` (default): required files must exist and match ModelScope manifest size.
  - `modelscope_verify=sha256`: strict mode, also checks SHA256 from ModelScope manifest.
  - `modelscope_verify=none`: existence-only check (avoid unless debugging).
- Keep SAM3 pipelines inactive until model weights are fully available on disk and runtime latency is validated on your host.
