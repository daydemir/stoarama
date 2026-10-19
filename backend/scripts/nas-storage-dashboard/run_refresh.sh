#!/bin/bash
set -euo pipefail
set -a
. /Users/jelly/Build/stoarama/local/render.env
. /Users/jelly/Build/stoarama/local/recording-supervisor.env
set +a
export PATH="/Users/jelly/.local/bin:/opt/homebrew/opt/libpq/bin:/opt/homebrew/bin:/usr/bin:/bin"
exec /usr/bin/python3 /Users/jelly/.stoarama/nas-storage-dashboard/app/refresh.py \
  --state /Users/jelly/.stoarama/nas-storage-dashboard/state \
  --publish /Users/jelly/.stoarama/seam-review/app/stoarama-proof/review/vid/nas-storage
