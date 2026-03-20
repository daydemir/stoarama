#!/usr/bin/env bash
set -euo pipefail

YTDLP_BIN="/tmp/yt-dlp"
if [ ! -x "${YTDLP_BIN}" ]; then
  echo "Bootstrapping yt-dlp..."
  curl -fsSL "https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp" -o "${YTDLP_BIN}"
  chmod +x "${YTDLP_BIN}"
fi

export YT_DLP_BIN="${YTDLP_BIN}"

if [ -n "${YT_DLP_COOKIES_B64:-}" ]; then
  COOKIES_FILE="/tmp/yt-dlp-cookies.txt"
  printf '%s' "${YT_DLP_COOKIES_B64}" | base64 -d > "${COOKIES_FILE}"
  chmod 600 "${COOKIES_FILE}"
  export YT_DLP_COOKIES_FILE="${COOKIES_FILE}"
fi

exec ./bin/capture-worker
