#!/usr/bin/env bash
set -euo pipefail

mode="${1:-required}"
archive_url="${JOINED_RECORDING_FFMPEG_ARCHIVE_URL:-}"
archive_sha="${JOINED_RECORDING_FFMPEG_ARCHIVE_SHA256:-}"
ffmpeg_sha="${JOINED_RECORDING_FFMPEG_BINARY_SHA256:-}"
ffprobe_sha="${JOINED_RECORDING_FFPROBE_BINARY_SHA256:-}"

if [[ -z "$archive_url$archive_sha$ffmpeg_sha$ffprobe_sha" && "$mode" == optional ]]; then
  exit 0
fi
[[ "$mode" == required || "$mode" == optional ]] || { echo "invalid pinned media tool install mode" >&2; exit 1; }
: "${archive_url:?missing joined FFmpeg archive URL}"
: "${archive_sha:?missing joined FFmpeg archive SHA-256}"
: "${ffmpeg_sha:?missing joined FFmpeg binary SHA-256}"
: "${ffprobe_sha:?missing joined FFprobe binary SHA-256}"
[[ "$archive_url" == https://* && "$archive_url" != *[[:space:]@?#]* ]] || { echo "joined FFmpeg archive URL must be plain HTTPS" >&2; exit 1; }
[[ "${archive_url,,}" != *"/latest/"* ]] || { echo "joined FFmpeg archive URL must be immutable" >&2; exit 1; }
for digest in "$archive_sha" "$ffmpeg_sha" "$ffprobe_sha"; do
  [[ "$digest" =~ ^[0-9a-f]{64}$ ]] || { echo "joined FFmpeg checksums must be lowercase SHA-256" >&2; exit 1; }
done

scratch="$(mktemp -d)"
trap 'rm -rf -- "$scratch"' EXIT
curl --proto '=https' --proto-redir '=https' -fsSL -o "$scratch/archive.tar" "$archive_url"
printf '%s  %s\n' "$archive_sha" "$scratch/archive.tar" | sha256sum -c -
mkdir -p "$scratch/extract" bin
tar -xf "$scratch/archive.tar" -C "$scratch/extract"
ffmpeg_src="$(find "$scratch/extract" -type f -path '*/bin/ffmpeg' -print -quit)"
ffprobe_src="$(find "$scratch/extract" -type f -path '*/bin/ffprobe' -print -quit)"
[[ -n "$ffmpeg_src" && -n "$ffprobe_src" ]] || { echo "joined FFmpeg archive lacks ffmpeg or ffprobe" >&2; exit 1; }
install -m 0755 "$ffmpeg_src" bin/ffmpeg
install -m 0755 "$ffprobe_src" bin/ffprobe
printf '%s  %s\n' "$ffmpeg_sha" bin/ffmpeg | sha256sum -c -
printf '%s  %s\n' "$ffprobe_sha" bin/ffprobe | sha256sum -c -
./bin/ffmpeg -version >/dev/null
./bin/ffprobe -version >/dev/null
