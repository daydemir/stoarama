#!/usr/bin/env python3
"""Read-only per-source media proof for a frozen generation-2 delivery part."""

import argparse
import hashlib
import json
import os
import re
import shutil
import stat
import struct
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import ExitStack
from dataclasses import dataclass
from fractions import Fraction
from pathlib import Path

from stoarama_pull import (
    InventoryScanStopped,
    MediaCertificationError,
    StitchHold,
    ToolProcessError,
    cancellable_tool_output,
    certification_identity,
    stitch_editlist_metadata,
    stitch_proof_json,
)

MOV_INPUT = ("-protocol_whitelist", "file,pipe", "-f", "mov",
             "-enable_drefs", "0", "-use_absolute_path", "0")
DECODE = ("-nostdin", "-v", "error", "-xerror", "-err_detect", "explode", "-threads", "1")
VIDEO_CONFIG = ("codec_name", "codec_tag_string", "profile", "level", "extradata_hash",
                "nal_length_size", "width", "height", "pix_fmt", "sample_aspect_ratio",
                "field_order", "chroma_location", "color_range", "color_space",
                "color_transfer", "color_primaries")
AUDIO_CONFIG = ("codec_name", "codec_tag_string", "profile", "extradata_hash",
                "sample_rate", "channels", "channel_layout")
AAC_SAMPLE_RATES = (96000, 88200, 64000, 48000, 44100, 32000, 24000,
                    22050, 16000, 12000, 11025, 8000, 7350)
AAC_FRAME_SAMPLES = 1024
UNITY_MATRIX = (65536, 0, 0, 0, 65536, 0, 0, 0, 1073741824)
TIMEOUT = 1800
METADATA_LIMIT = 128 * 1024 * 1024
MANIFEST_LIMIT = 8 * 1024 * 1024
PACKET_LIMIT = 500_000
# ashowinfo emits one line per verified PCM frame; the bound is finite and
# follows the already verified frame count, so hour-scale AAC still fits.
ASHOWINFO_BASE = 64 * 1024
ASHOWINFO_PER_BLOCK = 1024


class CoverageError(ValueError):
    """The available evidence cannot prove coverage."""


class ProofStopped(RuntimeError):
    """The attempt was cancelled or its absolute deadline expired.

    This is never a per-source verdict: it aborts the whole proof attempt and
    intentionally escapes the per-source verdict catch.
    """


def require(condition, reason):
    if not condition:
        raise CoverageError(reason)


class Run:
    """One attempt's explicit cancel object and absolute monotonic deadline.

    Instances are accepted anywhere ``stoarama_pull`` expects a cancel event
    (``is_set``/``wait``); the deadline rides along as bounded per-tool time.
    """

    def __init__(self, cancel=None, deadline=None):
        self.cancel = cancel if cancel is not None else threading.Event()
        self.deadline = deadline

    @property
    def reason(self):
        """The stop string, or ``None`` while the attempt may continue.

        This is deliberately a string property: ``stoarama_pull.stitch_check``
        reads ``stop.reason`` directly to label the hold it raises, so a bound
        method would leak into every native-hold message.
        """
        if self.cancel.is_set():
            return "proof cancelled"
        if self.deadline is not None and time.monotonic() >= self.deadline:
            return "proof deadline exceeded"
        return None

    def is_set(self):
        return self.reason is not None

    def wait(self, timeout):
        if self.deadline is not None:
            timeout = min(timeout, max(self.deadline - time.monotonic(), 0))
        return self.cancel.wait(timeout)

    def check(self):
        reason = self.reason
        if reason is not None:
            raise ProofStopped(reason)

    def budget(self):
        """Exact per-tool timeout: the remaining attempt budget, else TIMEOUT.

        This is only ever the tool's own time limit. Whole-attempt expiry stays
        with ``check``/``reason`` so a verdict is never built from it.
        """
        self.check()
        if self.deadline is None:
            return TIMEOUT
        return min(TIMEOUT, self.deadline - time.monotonic())


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def digest_rows(rows):
    digest = hashlib.sha256()
    for row in rows:
        digest.update(json.dumps(row, separators=(",", ":"), sort_keys=True).encode("ascii") + b"\n")
    return digest.hexdigest()


def good_sha(value):
    return isinstance(value, str) and re.fullmatch("[a-f0-9]{64}", value) is not None


def fields(row, names):
    return {name: row.get(name) for name in names}


class Input:
    def __init__(self, stack, path=None, expected=None, *, run=None, descriptor=None):
        # Either an explicit no-follow path open or a borrowed integer
        # descriptor, duplicated first so the caller keeps ownership.
        self.run = run if run is not None else Run()
        if (path is None) == (descriptor is None):
            raise TypeError("supply exactly one of path or descriptor")
        if descriptor is not None:
            require(isinstance(descriptor, int) and not isinstance(descriptor, bool) and descriptor >= 0,
                    "borrowed input descriptor is invalid")
            self.file = stack.enter_context(os.fdopen(os.dup(descriptor), "rb"))
        else:
            self.file = stack.enter_context(os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), "rb"))
        self.fd = self.file.fileno()
        before = os.fstat(self.fd)
        require(stat.S_ISREG(before.st_mode) and before.st_size > 0, "input is not a nonempty regular file")
        if descriptor is not None:
            # The dup shares the caller's file description, so this reads the
            # shared offset now and restores it in an exit callback that runs
            # while the duplicate is still open (callbacks unwind LIFO, before
            # the fdopen context that owns the duplicate closes it).
            stack.callback(os.lseek, self.fd, os.lseek(self.fd, 0, os.SEEK_CUR), os.SEEK_SET)
        self.identity = certification_identity(before)
        self.size = before.st_size
        self.path = "/dev/fd/" + str(self.fd)
        self.sha = self.digest()
        if expected is not None:
            require((self.size, self.sha) == (expected["size_bytes"], expected["sha256"]),
                    "local media differs from the frozen object identity")

    def digest(self):
        digest = hashlib.sha256()
        offset = 0
        while offset < self.size:
            self.run.check()
            block = os.pread(self.fd, min(1024 * 1024, self.size - offset), offset)
            require(block, "input truncated during SHA-256 readback")
            digest.update(block)
            offset += len(block)
        self.check()
        return digest.hexdigest()

    def check(self):
        require(self.identity == certification_identity(os.fstat(self.fd)), "input changed during proof")

    def payload(self, packet):
        self.run.check()
        data = os.pread(self.fd, packet.size, packet.position)
        require(sha256(data) == packet.digest, "packet payload differs from probe hash")
        return data


@dataclass(frozen=True)
class Packet:
    pts: int
    dts: int
    duration: int
    size: int
    position: int
    flags: str
    digest: str

    @classmethod
    def parse(cls, row, size):
        digest = row["data_hash"].removeprefix("SHA256:")
        result = cls(int(row["pts"]), int(row["dts"]), int(row["duration"]), int(row["size"]),
                     int(row["pos"]), row["flags"], digest)
        require(not row.get("side_data_list") and good_sha(digest) and result.duration > 0
                and 0 < result.size <= size and 0 <= result.position <= size - result.size
                and re.fullmatch("K?_+|K", result.flags), "unsupported packet identity or side data")
        return result

    def content(self):
        return self.size, self.digest, self.flags.startswith("K")


@dataclass(frozen=True)
class Frame:
    pts: int
    packet: int
    duration: int


@dataclass(frozen=True)
class AudioBlock:
    pts: int
    skip: int
    samples: int
    decoded_samples: int
    offset: int


@dataclass
class Track:
    config: dict
    time_base: Fraction
    packets: list
    positions: dict
    track_id: int


def tool_output(command, fd, limit=METADATA_LIMIT, *, stdout_file=None, stderr_file=None, run=None,
                stderr_limit=None):
    """Run one bounded tool; the attempt budget, not a verdict, owns expiry."""
    run = run if run is not None else Run()
    options = {} if stderr_limit is None else {"stderr_limit": stderr_limit}
    try:
        return cancellable_tool_output(command, run, run.budget(), stdout_limit=limit,
                                       pass_fds=(fd,), reject_stderr=stderr_file is None,
                                       stdout_file=stdout_file, stderr_file=stderr_file, **options)
    except InventoryScanStopped as error:
        raise ProofStopped(run.reason or "proof cancelled") from error
    except MediaCertificationError:
        # A tool that spent the attempt's remaining budget must abort the
        # attempt; the per-tool TIMEOUT keeps its own verdict otherwise.
        run.check()
        raise


def probe(ffprobe, source, *args):
    raw = tool_output([ffprobe, "-v", "error", "-err_detect", "explode", *MOV_INPUT,
                       *args, "-of", "json", source.path], source.fd, run=source.run)
    return json.loads(raw)


def read_packet_rows(ffprobe, source):
    with tempfile.TemporaryFile() as rows:
        tool_output([ffprobe, "-v", "error", *MOV_INPUT, "-ignore_editlist", "1",
                     "-show_packets", "-show_data_hash", "sha256", "-show_entries",
                     "packet=stream_index,pts,dts,duration,size,pos,flags,data_hash:packet_side_data",
                     "-of", "compact=p=0", source.path], source.fd, stdout_file=rows, run=source.run)
        rows.seek(0)
        for count, line in enumerate(rows, 1):
            source.run.check()
            require(count <= PACKET_LIMIT and len(line) <= 1024, "packet metadata exceeds bound")
            pairs = [entry.split("=", 1) for entry in line.decode("ascii").strip().split("|")]
            require(len(pairs) == 8 and all(len(pair) == 2 for pair in pairs),
                    "unsupported packet metadata or side data")
            fields = dict(pairs)
            require(set(fields) == {"stream_index", "pts", "dts", "duration", "size", "pos", "flags", "data_hash"},
                    "incomplete packet metadata")
            yield fields


def read_tracks(ffprobe, source):
    data = probe(ffprobe, source, "-ignore_editlist", "1", "-show_streams", "-show_data_hash", "sha256")
    tracks = {}
    by_index = {}
    for stream in data["streams"]:
        kind = stream["codec_type"]
        require(kind in ("video", "audio") and kind not in tracks, "unsupported track layout")
        require(not stream.get("side_data_list")
                and not any(stream.get("disposition", {}).get(flag)
                            for flag in ("attached_pic", "dependent", "still_image", "multilayer")),
                "unsupported stream side data or disposition")
        config = fields(stream, VIDEO_CONFIG if kind == "video" else AUDIO_CONFIG)
        require(good_sha((config["extradata_hash"] or "").removeprefix("SHA256:")),
                "missing codec configuration hash")
        if kind == "video":
            require((config["codec_name"], config["codec_tag_string"]) == ("h264", "avc1")
                    and int(config["nal_length_size"]) in (1, 2, 4), "unsupported H.264 framing")
        else:
            require((config["codec_name"], config["codec_tag_string"], config["profile"])
                    == ("aac", "mp4a", "LC") and int(config["sample_rate"]) in AAC_SAMPLE_RATES
                    and (int(config["channels"]), config["channel_layout"]) in ((1, "mono"), (2, "stereo")),
                    "unsupported AAC configuration")
        track = Track(config, Fraction(stream["time_base"]), [], {}, int(stream["id"], 16))
        require(track.time_base > 0, "invalid track time base")
        if kind == "audio":
            require(track.time_base == Fraction(1, int(config["sample_rate"])),
                    "AAC track clock is not in samples")
        tracks[kind] = track
        index = int(stream["index"])
        require(index not in by_index, "duplicate stream index")
        by_index[index] = track
    require("video" in tracks, "input has no video track")
    for row in read_packet_rows(ffprobe, source):
        source.run.check()
        track = by_index[int(row["stream_index"])]
        packet = Packet.parse(row, source.size)
        require(packet.position not in track.positions
                and (not track.packets or packet.dts > track.packets[-1].dts),
                "packet order or file position is ambiguous")
        track.positions[packet.position] = len(track.packets)
        track.packets.append(packet)
    require(all(track.packets for track in tracks.values()), "track has no packets")
    return tracks


@dataclass(frozen=True)
class Edit:
    offset: int
    start: int
    end: Fraction
    display_size: tuple


def media_edits(source, tracks):
    def number(header, offset, duration=False):
        require(header and header[0] in (0, 1), "unsupported MP4 clock header")
        wide = header[0] == 1
        return struct.unpack_from(">Q" if wide and duration else ">I", header, offset + 8 * wide)[0]

    edits = {}
    # A stop inside the shared MP4 reader surfaces as StitchHold, whose reason
    # is the stop string. run.check() turns a real stop into the documented
    # ProofStopped; an ordinary hold keeps its normal verdict meaning.
    try:
        headers = stitch_editlist_metadata(source.path, source.run, all_tracks=True)
    except StitchHold:
        source.run.check()
        raise
    for header in headers:
        source.run.check()
        media = bytes.fromhex(header["media_header"])
        tkhd = bytes.fromhex(header["track_header"])
        mvhd = bytes.fromhex(header["movie_header"])
        handler = {b"vide": "video", b"soun": "audio"}.get(bytes.fromhex(header["handler"])[8:12])
        require(handler in tracks and handler not in edits, "MP4 track and probe layout differ")
        require(int.from_bytes(tkhd[1:4], "big") & 3 == 3, "MP4 track is disabled or excluded from playback")
        require(tracks[handler].track_id == number(tkhd, 12), "MP4 track identifiers differ")
        clock = number(media, 12)
        require(clock and tracks[handler].time_base == Fraction(1, clock), "MP4 track clock differs")
        movie_clock = number(mvhd, 12)
        movie_duration = number(mvhd, 16, duration=True)
        track_duration = number(tkhd, 20, duration=True)
        require(movie_clock > 0 and movie_duration >= track_duration > 0,
                "MP4 movie duration does not cover its track")
        movie_rate_offset = 20 + 12 * mvhd[0]
        track_volume_offset = 36 + 12 * tkhd[0]
        movie_rate, movie_volume = struct.unpack_from(">ih", mvhd, movie_rate_offset)
        track_volume = struct.unpack_from(">h", tkhd, track_volume_offset)[0]
        require((movie_rate, movie_volume) == (65536, 256)
                and track_volume == (256 if handler == "audio" else 0),
                "unsupported MP4 playback rate or volume")
        require(struct.unpack_from(">9i", mvhd, movie_rate_offset + 16) == UNITY_MATRIX
                and struct.unpack_from(">9i", tkhd, track_volume_offset + 4) == UNITY_MATRIX,
                "unsupported MP4 display transform")
        display_size = struct.unpack_from(">II", tkhd, track_volume_offset + 40)
        raw = header["edit_list"]
        if raw is None:
            end = min(Fraction(track_duration * clock, movie_clock), number(media, 16, duration=True))
            require(end > 0, "missing MP4 media duration")
            edits[handler] = Edit(0, 0, end, display_size)
            continue
        data = bytes.fromhex(raw)
        require(len(data) >= 8 and data[0] in (0, 1) and data[1:4] == bytes(3), "unsupported MP4 edit header")
        decoder = struct.Struct(">Qqi" if data[0] else ">Iii")
        count = int.from_bytes(data[4:8], "big")
        require(count in (1, 2) and len(data) == 8 + count * decoder.size, "unsupported MP4 edit list shape")
        entries = list(decoder.iter_unpack(data[8:]))
        require(all(segment > 0 and rate == 65536 for segment, _, rate in entries),
                "unsupported MP4 dwell or non-unit edit rate")
        duration, media_start, _ = entries[-1]
        require(media_start >= 0 and (count == 1 or entries[0][1] == -1), "unsupported MP4 media edits")
        require(sum(segment for segment, _, _ in entries) == track_duration, "MP4 edit and track durations differ")
        start = Fraction(entries[0][0] * clock, movie_clock) if count == 2 else 0
        end = start + Fraction(duration * clock, movie_clock)
        require(int(start) == start, "fractional start of leading empty MP4 edit")
        edits[handler] = Edit(int(start) - media_start, int(start), end, display_size)
    require(edits.keys() == tracks.keys(), "MP4 track and probe layout differ")
    return edits


def first_video_idr(source, track):
    packet = track.packets[0]
    data = source.payload(packet)
    width = int(track.config["nal_length_size"])
    offset = 0
    while offset < len(data):
        size = int.from_bytes(data[offset:offset + width], "big")
        offset += width
        require(size > 0 and offset + size <= len(data), "invalid AVC packet framing")
        nal_type = data[offset] & 31
        if 1 <= nal_type <= 5:
            return nal_type == 5 and packet.flags.startswith("K")
        offset += size
    return False


def video_frames(ffmpeg, ffprobe, source, track, edit):
    data = probe(ffprobe, source, "-select_streams", "v:0", "-show_frames",
                 "-show_entries", "frame=pts,pkt_pos,duration,pkt_duration,width,height,pix_fmt")
    frames = []
    seen = set()
    for row in data["frames"]:
        source.run.check()
        packet = track.positions[int(row["pkt_pos"])]
        duration = int(row.get("duration", row.get("pkt_duration", 0)))
        require(packet not in seen and duration > 0 and all(row[key] == track.config[key]
                for key in ("width", "height", "pix_fmt")), "ambiguous or changing video frame format")
        seen.add(packet)
        frames.append(Frame(int(row["pts"]), packet, duration))
    hashes = tool_output([ffmpeg, *DECODE, "-copyts", "-filter_threads", "1", *MOV_INPUT,
                          "-i", source.path, "-map", "0:v:0", "-fps_mode", "passthrough",
                          "-enc_time_base", f"{track.time_base.numerator}:{track.time_base.denominator}", "-pix_fmt", "+" + track.config["pix_fmt"],
                          "-c:v", "rawvideo", "-threads:v", "1", "-f", "framehash",
                          "-hash", "sha256", "-"], source.fd, run=source.run)
    time_base = None
    decoded = []
    for line in hashes.decode("ascii").splitlines():
        source.run.check()
        if line.startswith("#tb 0:"):
            time_base = Fraction(line.split(":", 1)[1].strip())
        if line and not line.startswith("#"):
            stream, _, pts, _, size, digest = (value.strip() for value in line.split(","))
            require(stream == "0" and int(size) > 0 and good_sha(digest), "invalid decoded video hash")
            decoded.append((int(pts), digest))
    require(time_base == track.time_base and frames and len(frames) == len(decoded),
            "video presentation clock or frame count differs between decoders")
    previous_pts = None
    result = {}
    for index, (frame, (pts, digest)) in enumerate(zip(frames, decoded)):
        source.run.check()
        require(pts == frame.pts and (previous_pts is None or pts > previous_pts),
                "video presentation order is ambiguous")
        previous_pts = pts
        offset = edit.offset
        require(pts == track.packets[frame.packet].pts + offset, "video edit differs from packet presentation")
        duration = frame.duration
        if index + 1 < len(frames):
            duration = min(duration, frames[index + 1].pts - pts)
        start = max(pts, edit.start)
        end = min(pts + duration, edit.end)
        if end > start:
            result[frame.packet] = (start, end - start, digest)
    require(result, "no video frames visible through the MP4 edits")
    return result


def audio_blocks(ffprobe, source, track, edit):
    data = probe(ffprobe, source, "-select_streams", "a:0", "-show_packets", "-show_frames",
                 "-show_entries", "packet=pts,pos,duration:packet_side_data=side_data_type,skip_samples,discard_padding"
                 ":frame=pts,pkt_pos,nb_samples,channels,channel_layout")
    packets = {}
    blocks = {}
    offset = 0
    for row in data["packets_and_frames"]:
        source.run.check()
        if row["type"] == "packet":
            index = track.positions[int(row["pos"])]
            side_data = row.get("side_data_list", [])
            require(index not in packets and len(side_data) <= 1
                    and all(item["side_data_type"] == "Skip Samples" for item in side_data),
                    "ambiguous audio edit or packet side data")
            edits = side_data[0] if side_data else {}
            packets[index] = (int(row["pts"]), int(row["duration"]), int(edits.get("skip_samples", 0)),
                              int(edits.get("discard_padding", 0)))
            continue
        require(row["type"] == "frame", "invalid audio presentation record")
        index = track.positions[int(row["pkt_pos"])]
        pts, duration, skip, discard = packets[index]
        samples = int(row["nb_samples"])
        require(pts == track.packets[index].pts + edit.offset,
                "AAC edit differs from packet presentation")
        require(index not in blocks and int(row["pts"]) == pts + skip
                and 0 <= skip < AAC_FRAME_SAMPLES and 0 <= discard < AAC_FRAME_SAMPLES
                and AAC_FRAME_SAMPLES - skip - discard == samples > 0
                and int(row["channels"]) == int(track.config["channels"])
                and row["channel_layout"] == track.config["channel_layout"],
                "unsupported AAC edit or changing sample format")
        start = pts + skip
        end = pts + min(skip + samples, duration)
        require(start >= edit.start, "unsupported partial AAC start edit")
        end = min(end, int(edit.end))
        require(end > start, "invalid AAC presentation interval")
        blocks[index] = AudioBlock(start, skip, end - start, samples, offset)
        offset += samples
    require(blocks and list(blocks) == sorted(blocks), "audio presentation order is ambiguous")
    previous_end = None
    for block in blocks.values():
        source.run.check()
        require(previous_end is None or block.pts >= previous_end, "visible audio samples overlap")
        previous_end = block.pts + block.samples
    return blocks, offset


def audio_pcm(stack, ffmpeg, ffprobe, source, track, edit):
    blocks, samples = audio_blocks(ffprobe, source, track, edit)
    frame_size = int(track.config["channels"]) * 4
    expected = samples * frame_size
    pcm = stack.enter_context(tempfile.TemporaryFile())
    command = [ffmpeg, *DECODE, "-hide_banner", "-v", "info", "-copyts", *MOV_INPUT,
               "-c:a", "aac_fixed", "-i", source.path, "-map", "0:a:0", "-af", "ashowinfo",
               "-c:a", "pcm_s32le", "-fs", str(expected + 1), "-f", "s32le", "-"]
    # Bind PCM byte offsets to the exact integer decoder's own frame geometry.
    # The ashowinfo lines land in a bounded temporary sink, never in memory,
    # and the bound follows the verified frame count so hour-scale AAC fits.
    limit = ASHOWINFO_BASE + ASHOWINFO_PER_BLOCK * len(blocks)
    with tempfile.TemporaryFile() as log:
        try:
            tool_output(command, source.fd, expected + 1, stdout_file=pcm,
                        stderr_file=log, stderr_limit=limit, run=source.run)
        except ToolProcessError as error:
            source.run.check()
            raise CoverageError("strict integer AAC decode failed") from error
        # The helper has joined its drains, so the sinks hold every byte.
        pcm.flush()
        log.flush()
        log.seek(0)
        reported = (line for line in log if b"Parsed_ashowinfo_" in line)
        for block in blocks.values():
            source.run.check()
            line = next(reported, b"")
            row = dict(re.findall(rb"([a-z_]+):([^ ]+)", line))
            require(row.get(b"pts") == str(block.pts).encode("ascii")
                    and row.get(b"nb_samples") == str(block.decoded_samples).encode("ascii")
                    and row.get(b"fmt") == b"s32p"
                    and row.get(b"rate") == track.config["sample_rate"].encode("ascii")
                    and row.get(b"channels") == str(track.config["channels"]).encode("ascii"),
                    "integer AAC decode frame geometry differs")
        require(not next(reported, b""), "integer AAC decode emitted extra frames")
    require(os.fstat(pcm.fileno()).st_size == expected, "AAC decode did not match sample counts")
    return pcm.fileno(), blocks, frame_size


def adts_packet(payload, config):
    rate = AAC_SAMPLE_RATES.index(int(config["sample_rate"]))
    channels = int(config["channels"])
    length = len(payload) + 7
    require(length <= 8191, "AAC packet cannot be framed for the seam reference")
    return bytes((255, 241, 64 | (rate << 2) | (channels >> 2),
                  ((channels & 3) << 6) | (length >> 11),
                  (length >> 3) & 255, ((length & 7) << 5) | 31, 252)) + payload


def prime_aac(ffmpeg, config, prior, current, frame_size, run=None):
    run = run if run is not None else Run()
    # ADTS can express default AAC-LC only. The full audio config is already
    # byte-identical. Check that the actual configuration is representable too.
    expected = (2 << 11) | (AAC_SAMPLE_RATES.index(int(config["sample_rate"])) << 7) | (
        int(config["channels"]) << 3)
    header = expected.to_bytes(2, "big")
    require(config["extradata_hash"] in ("SHA256:" + sha256(header),
                                         "SHA256:" + sha256(header + bytes.fromhex("56e500"))),
            "AAC configuration cannot be represented by the seam reference")
    with tempfile.TemporaryFile() as reference:
        reference.write(adts_packet(prior, config) + adts_packet(current, config))
        reference.flush()
        data = tool_output([ffmpeg, *DECODE, "-protocol_whitelist", "file,pipe", "-f", "aac",
                            "-c:a", "aac_fixed", "-i", "/dev/fd/" + str(reference.fileno()),
                            "-map", "0:a:0", "-c:a", "pcm_s32le", "-f", "s32le", "-"],
                           reference.fileno(), AAC_FRAME_SAMPLES * frame_size * 2, run=run)
    stride = AAC_FRAME_SAMPLES * frame_size
    require(len(data) == stride * 2, "AAC seam reference must decode two full frames")
    return data[stride:]


def packet_mapping(source, output, start):
    end = start + len(source.packets)
    target = output.packets[start:end]
    require(source.config == output.config and len(target) == len(source.packets),
            "output codec configuration or track packet count differs")
    delta = target[0].pts * output.time_base - source.packets[0].pts * source.time_base
    extension = Fraction(0)
    for index, (original, delivered) in enumerate(zip(source.packets, target)):
        require(original.content() == delivered.content(), "output packet bytes or flags differ")
        require(delivered.pts * output.time_base - original.pts * source.time_base == delta
                and delivered.dts * output.time_base - original.dts * source.time_base == delta,
                "output packet PTS/DTS mapping is not constant")
        duration_change = delivered.duration * output.time_base - original.duration * source.time_base
        if duration_change:
            require(start + index + 1 < len(output.packets) and index + 1 == len(source.packets)
                    and duration_change > 0 and delivered.dts + delivered.duration == output.packets[start + index + 1].dts,
                    "output packet duration differs outside a seam extension")
            extension = duration_change
    return end, delta, extension


def prove_video(source, output, start, count, source_base, output_base, run=None):
    run = run if run is not None else Run()
    delta = None
    pairs = []
    for index, (pts, duration, digest) in source.items():
        run.check()
        require(start + index in output, "a visible source video frame is absent")
        target_pts, target_duration, target_digest = output[start + index]
        shift = target_pts * output_base - pts * source_base
        if delta is None:
            delta = shift
        require(shift == delta and target_duration * output_base >= duration * source_base
                and digest == target_digest, "visible video frame, duration or presentation shift differs")
        pairs.append((index, pts, duration, digest))
    extras = sum(index in output for index in range(start, start + count)) - len(pairs)
    last = pairs[-1]
    return delta, {"frame_count": len(pairs), "extra_output_frames": extras,
                   "time_base": str(source_base), "first_pts": str(pairs[0][1]), "end_pts": str(last[1] + last[2]),
                   "frame_chain_sha256": digest_rows((i, str(pts), str(duration), sha)
                                                       for i, pts, duration, sha in pairs)}


def prove_audio(ffmpeg, source_file, output_file, source_track, output_track, source, output, start):
    source_fd, source_blocks, frame_size = source
    output_fd, output_blocks, target_frame_size = output
    run = source_file.run
    require(frame_size == target_frame_size, "AAC output channel geometry differs")
    seam = None
    if start:
        seam = prime_aac(ffmpeg, source_track.config,
                         output_file.payload(output_track.packets[start - 1]),
                         source_file.payload(source_track.packets[0]), frame_size, run)
        first = output_blocks.get(start)
        require(first is not None, "first AAC packet after the seam is absent from delivery")
        run.check()
        actual = os.pread(output_fd, first.decoded_samples * frame_size, first.offset * frame_size)
        require(actual == seam[first.skip * frame_size:(first.skip + first.decoded_samples) * frame_size],
                "first AAC frame differs from the primed seam reference")
    delta = None
    rows = []
    for index, block in source_blocks.items():
        run.check()
        target = output_blocks.get(start + index)
        require(target is not None and target.skip <= block.skip
                and target.skip + target.samples >= block.skip + block.samples,
                "visible source audio samples are absent")
        skip_difference = block.skip - target.skip
        shift = target.pts + skip_difference - block.pts
        if delta is None:
            delta = shift
        require(shift == delta, "output audio sample mapping is not constant")
        sample_bytes = block.samples * frame_size
        run.check()
        written = os.pread(output_fd, sample_bytes, (target.offset + skip_difference) * frame_size)
        if index == 0 and seam is not None:
            wanted = seam[block.skip * frame_size:(block.skip + block.samples) * frame_size]
        else:
            run.check()
            wanted = os.pread(source_fd, sample_bytes, block.offset * frame_size)
        require(len(written) == sample_bytes and written == wanted, "visible audio samples differ")
        rows.append((index, block.pts, block.samples, sha256(written)))
    return Fraction(delta, int(source_track.config["sample_rate"])), {
        "rule": "seam_primed" if seam is not None else "standalone_exact",
        "sample_count": sum(row[2] for row in rows),
        "sample_rate": int(source_track.config["sample_rate"]),
        "first_sample": rows[0][1], "end_sample": rows[-1][1] + rows[-1][2],
        "extra_output_samples": sum(output_blocks[index].samples for index in
                                    range(start, start + len(source_track.packets)) if index in output_blocks)
                                - sum(row[2] for row in rows),
        "pcm_chain_sha256": digest_rows(rows),
        "seam_primer_packets": int(seam is not None),
    }


def manifest_part(manifest, part):
    require((manifest["schema_version"], manifest["policy_version"], manifest["generation"],
             manifest["status"]) == (1, "collation-v2", 2, "collated"), "unsupported collation manifest")
    outputs = manifest["outputs"]
    require(1 <= part <= len(outputs), "part missing from manifest")
    output = outputs[part - 1]
    require(output["part"] == part and output["parts"] == len(outputs) and output["nas_relative_path"]
            and output["object_key"] and good_sha(output["sha256"]) and output["size_bytes"] > 0,
            "invalid frozen output identity")
    selected = {}
    seen = set()
    for row in manifest["clips"]:
        clip = row["clip"]
        identifier = clip["clip_id"]
        require(type(identifier) is int and identifier > 0 and identifier not in seen, "duplicate or invalid source ID")
        seen.add(identifier)
        if row["disposition"] != "included" or row.get("part") != part:
            continue
        require(clip["recording_id"] == manifest["recording_id"] and not clip.get("purged")
                and clip["bucket"] and clip["object_key"] and clip["etag"] and good_sha(clip["sha256"])
                and clip["size_bytes"] > 0, "invalid frozen source identity")
        selected[identifier] = fields(clip, ("clip_id", "recording_id", "bucket", "object_key", "etag",
                                             "size_bytes", "sha256", "start_utc", "end_utc"))
    order = output["source_clip_ids"]
    require(order and len(order) == len(selected) and set(order) == selected.keys(),
            "manifest source membership or order is ambiguous")
    return output, [selected[identifier] for identifier in order]


def prove_source(stack, ffmpeg, ffprobe, source, tracks, mapping, output, target_tracks,
                 target_edits, target_video, target_audio):
    source.run.check()
    edits = media_edits(source, tracks)
    require(edits["video"].display_size == target_edits["video"].display_size,
            "output MP4 video display geometry differs")
    require(first_video_idr(source, tracks["video"]), "source does not start at an H.264 IDR")
    result = {}
    for kind, track in tracks.items():
        source.run.check()
        target_track = target_tracks[kind]
        start, packet_delta, packet_extension = mapping[kind]
        if kind == "video":
            visible = video_frames(ffmpeg, ffprobe, source, track, edits[kind])
            delta, facts = prove_video(visible, target_video, start, len(track.packets),
                                       track.time_base, target_track.time_base, source.run)
        else:
            pcm = audio_pcm(stack, ffmpeg, ffprobe, source, track, edits[kind])
            delta, facts = prove_audio(ffmpeg, source, output, track, target_track, pcm, target_audio, start)
        result[kind] = dict(
            facts, packet_count=len(track.packets), packet_start=start,
            packet_chain_sha256=digest_rows((packet.pts, packet.dts, packet.duration,
                                            packet.content()) for packet in track.packets),
            packet_time_shift=str(packet_delta), visible_time_shift=str(delta),
            last_packet_duration_extension=str(packet_extension),
            codec_config_sha256=digest_rows([track.config]),
        )
    if "audio" in tracks:
        shifts = [Fraction(result[kind]["visible_time_shift"]) for kind in ("video", "audio")]
        require(abs(shifts[0] - shifts[1]) <= Fraction(1, 1000), "audio/video placement differs by over 1 ms")
    return result


def prove_ordered(target, candidates, inputs, ffmpeg, ffprobe, *, cancel=None, deadline=None):
    """Prove one output part against its sources in manifest order.

    ``inputs`` are the already-open output followed by every source, in order,
    accepting either no-follow paths or borrowed integer descriptors (duplicated
    per use, never closed). The returned verdict rows are identical for both.
    Cancellation or deadline expiry aborts the whole attempt as ``ProofStopped``.
    """
    run = Run(cancel, deadline)
    require(len(inputs) == len(candidates) + 1, "supply the output followed by every source in manifest order")
    run.check()
    with ExitStack() as stack:
        def opened(item, expected=None):
            run.check()
            if isinstance(item, int) and not isinstance(item, bool):
                return Input(stack, expected=expected, run=run, descriptor=item)
            return Input(stack, item, expected, run=run)

        output = opened(inputs[0], target)
        target_tracks = read_tracks(ffprobe, output)
        target_edits = media_edits(output, target_tracks)
        target_audio = (audio_pcm(stack, ffmpeg, ffprobe, output, target_tracks["audio"], target_edits["audio"])
                        if "audio" in target_tracks else None)
        target_video = video_frames(ffmpeg, ffprobe, output, target_tracks["video"], target_edits["video"])
        starts = dict.fromkeys(target_tracks, 0)
        mapped = []
        for item, candidate in zip(inputs[1:], candidates):
            source = opened(item, candidate)
            tracks = read_tracks(ffprobe, source)
            require(tracks.keys() == target_tracks.keys(), "source and output have different track layouts")
            mapping = {}
            for kind, track in tracks.items():
                run.check()
                start = starts[kind]
                end, packet_delta, packet_extension = packet_mapping(track, target_tracks[kind], start)
                mapping[kind] = (start, packet_delta, packet_extension)
                starts[kind] = end
            mapped.append((candidate, source, tracks, mapping))
        require(all(starts[kind] == len(track.packets) for kind, track in target_tracks.items()),
                "output contains unmapped packets")

        result = []
        for ordinal, (candidate, source, tracks, mapping) in enumerate(mapped, 1):
            run.check()
            row = {"source": candidate, "ordinal": ordinal}
            try:
                with ExitStack() as raw_stack:
                    facts = prove_source(raw_stack, ffmpeg, ffprobe, source, tracks, mapping,
                                         output, target_tracks, target_edits, target_video, target_audio)
            except (CoverageError, MediaCertificationError, StitchHold, KeyError, struct.error) as error:
                run.check()
                row.update(verdict="not_proven", reason=str(error))
            else:
                row.update(verdict="proven", tracks=facts)
            source.check()
            result.append(row)
        output.check()
        # A stop that lands after the last identity readback must still abort:
        # a final source computing just before the stop is never a verdict.
        run.check()
    return result


def prove_part(manifest, part, paths, ffmpeg, ffprobe):
    target, candidates = manifest_part(manifest, part)
    return target, prove_ordered(target, candidates, paths, ffmpeg, ffprobe)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--manifest-sha256", required=True, help="the SHA-256 pinned in the collation catalog")
    parser.add_argument("--part", required=True, type=int)
    parser.add_argument("--ffmpeg", default="ffmpeg")
    parser.add_argument("--ffprobe", default="ffprobe")
    parser.add_argument("media", nargs="+", type=Path,
                        help="exact local output bytes followed by all its sources in manifest order")
    args = parser.parse_args()
    try:
        with os.fdopen(os.open(args.manifest, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), "rb") as file:
            info = os.fstat(file.fileno())
            require(stat.S_ISREG(info.st_mode) and 0 < info.st_size <= MANIFEST_LIMIT,
                    "manifest must be a bounded regular file")
            manifest_bytes = file.read(MANIFEST_LIMIT + 1)
            require(len(manifest_bytes) == info.st_size, "manifest changed during read")
        require(good_sha(args.manifest_sha256) and sha256(manifest_bytes) == args.manifest_sha256,
                "manifest differs from the pinned SHA-256")
        manifest = stitch_proof_json(manifest_bytes)
        with ExitStack() as stack:
            tools = [shutil.which(name) for name in (args.ffmpeg, args.ffprobe)]
            require(all(tools), "FFmpeg and FFprobe must be available")
            paths = [str(Path(tool).resolve(strict=True)) for tool in tools]
            pins = [Input(stack, path) for path in paths]
            output, rows = prove_part(manifest, args.part, args.media, *paths)
            for pin, path in zip(pins, paths):
                pin.check()
                require(certification_identity(os.stat(path)) == pin.identity, "media tool changed during proof")
        print(json.dumps({
            "schema_version": 1,
            "manifest_sha256": args.manifest_sha256,
            "batch_id": manifest["batch_id"],
            "hour_id": manifest["hour_id"],
            "output": fields(output, ("part", "parts", "object_key", "nas_relative_path", "size_bytes", "sha256")),
            "ffmpeg_sha256": pins[0].sha,
            "ffprobe_sha256": pins[1].sha,
            "verifier_sha256": sha256(Path(__file__).read_bytes()),
            "verifier_support_sha256": sha256(Path(__file__).with_name("stoarama_pull.py").read_bytes()),
            "sources": rows,
        }, sort_keys=True, separators=(",", ":")))
    except (MediaCertificationError, StitchHold, ProofStopped, OSError, ValueError, TypeError, KeyError,
            IndexError, struct.error, subprocess.SubprocessError) as error:
        print(f"delivery coverage unproven: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
