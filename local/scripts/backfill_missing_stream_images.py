#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


def fetch_json(url: str) -> dict:
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "stoarama-missing-image-backfill/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def download_bytes(url: str) -> bytes:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "stoarama-missing-image-backfill/1.0"},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read()


def parse_segment_time(raw: str) -> datetime:
    value = str(raw or "").strip()
    if not value:
        return datetime.now(timezone.utc)
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def extract_frame_from_clip(ffmpeg_bin: str, clip_url: str, out_path: Path) -> None:
    cmd = [
        ffmpeg_bin,
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-ss",
        "1",
        "-i",
        clip_url,
        "-frames:v",
        "1",
        "-vf",
        "scale=240:-1",
        str(out_path),
    ]
    subprocess.run(cmd, check=True, timeout=90)


def load_state(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def save_state(path: Path, state: dict[str, dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True))
    tmp.replace(path)


def list_recording_streams(base_url: str) -> list[dict]:
    items: list[dict] = []
    offset = 0
    limit = 200
    while True:
        url = (
            f"{base_url.rstrip('/')}/api/v1/dashboard/streams"
            f"?recording_state=on&limit={limit}&offset={offset}"
        )
        payload = fetch_json(url)
        batch = payload.get("items") or []
        if not isinstance(batch, list) or not batch:
            break
        items.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
    return items


def latest_segment(base_url: str, stream_id: int) -> dict:
    url = f"{base_url.rstrip('/')}/api/v1/capture/streams/{stream_id}/segments/latest"
    return fetch_json(url)


def write_preview_image(
    pending_root: Path,
    slug: str,
    segment: dict,
    ffmpeg_bin: str,
) -> tuple[Path, str]:
    segment_id = str(segment.get("id") or "").strip()
    timestamp = parse_segment_time(segment.get("segment_start_at"))
    ts = timestamp.strftime("%Y%m%d-%H%M%S")
    out_dir = pending_root / slug
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{ts}.jpg"

    thumb_url = str(segment.get("thumbnail_download_url") or "").strip()
    if thumb_url:
        out_path.write_bytes(download_bytes(thumb_url))
        return out_path, segment_id

    clip_url = str(segment.get("download_url") or "").strip()
    if not clip_url:
        raise RuntimeError("latest clip has neither thumbnail_download_url nor download_url")
    extract_frame_from_clip(ffmpeg_bin, clip_url, out_path)
    return out_path, segment_id


def run_backfill(stoarama_bin: str, snapshot_root: Path) -> None:
    cmd = [
        stoarama_bin,
        "media",
        "backfill",
        "--snapshot-root",
        str(snapshot_root),
        "--concurrency",
        "4",
    ]
    subprocess.run(cmd, check=True, timeout=300)


def process_once(args: argparse.Namespace) -> int:
    state_path = Path(args.state_file).expanduser()
    pending_root = Path(args.snapshot_root).expanduser()
    pending_root.mkdir(parents=True, exist_ok=True)
    state = load_state(state_path)
    streams = list_recording_streams(args.backend_base_url)
    created = 0

    for stream in streams:
        stream_id = int(stream.get("id") or 0)
        if stream_id <= 0:
            continue
        latest_frame_url = str(stream.get("latest_frame_url") or "").strip()
        if latest_frame_url:
            continue
        slug = str(stream.get("slug") or "").strip()
        if not slug:
            continue
        try:
            segment = latest_segment(args.backend_base_url, stream_id)
        except Exception as exc:
            print(f"stream {stream_id}: latest segment lookup failed: {exc}", file=sys.stderr)
            continue

        segment_id = str(segment.get("id") or "").strip()
        if not segment_id:
            continue
        prev = state.get(str(stream_id), {})
        if str(prev.get("segment_id") or "") == segment_id:
            continue
        try:
            path, processed_segment_id = write_preview_image(
                pending_root,
                slug,
                segment,
                args.ffmpeg_bin,
            )
            print(f"prepared stream_id={stream_id} slug={slug} path={path.name} segment_id={processed_segment_id}")
            state[str(stream_id)] = {
                "segment_id": processed_segment_id,
                "prepared_at": datetime.now(timezone.utc).isoformat(),
            }
            created += 1
        except Exception as exc:
            print(f"stream {stream_id}: preview preparation failed: {exc}", file=sys.stderr)

    if created > 0:
        run_backfill(args.stoarama_bin, pending_root)
        shutil.rmtree(pending_root)
        pending_root.mkdir(parents=True, exist_ok=True)
        save_state(state_path, state)
    return created


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend-base-url", default="https://stoarama.com")
    parser.add_argument(
        "--snapshot-root",
        default=str(Path.home() / "Library/Application Support/stoarama/missing-image-backfill/pending"),
    )
    parser.add_argument(
        "--state-file",
        default=str(Path.home() / "Library/Application Support/stoarama/missing-image-backfill/state.json"),
    )
    parser.add_argument("--poll-sec", type=int, default=60)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--ffmpeg-bin", default="ffmpeg")
    parser.add_argument(
        "--stoarama-bin",
        default=str(Path(__file__).resolve().parents[2] / "local/bin/stoarama"),
    )
    args = parser.parse_args()

    if args.poll_sec <= 0:
        parser.error("--poll-sec must be > 0")

    while True:
        try:
            created = process_once(args)
            print(
                f"cycle complete created={created} at={datetime.now(timezone.utc).isoformat()}",
                flush=True,
            )
        except subprocess.CalledProcessError as exc:
            print(f"command failed rc={exc.returncode}: {exc}", file=sys.stderr, flush=True)
        except Exception as exc:
            print(f"worker cycle failed: {exc}", file=sys.stderr, flush=True)
        if args.once:
            return 0
        time.sleep(args.poll_sec)


if __name__ == "__main__":
    raise SystemExit(main())
