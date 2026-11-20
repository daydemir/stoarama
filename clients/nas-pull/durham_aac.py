"""Offline exact AAC concat for Durham raw clips.

One ordered run of H.264 + AAC-LC clips is copied through the concat demuxer
with original timestamps preserved, then proven against the exact input bytes
with ``delivery_coverage.prove_ordered``. Nothing is published unless every
input, every visible H.264 frame and both AAC rules verify.

This helper is deliberately offline: it takes retained input descriptors or
no-follow paths, a private scratch output path and the caller's tools, cancel
object and absolute monotonic deadline. Wiring and admission stay elsewhere.
"""
import math
import os
import tempfile
from collections import namedtuple
from contextlib import ExitStack
from fractions import Fraction
from pathlib import Path

import delivery_coverage as coverage
import stoarama_pull as pull


SIGNED_CLOCK_LIMIT = 2 ** 31 - 1
MICROSECONDS = 1000000
MAX_INPUTS = 200
# Per-track decoder configuration and clock kept after the pre-mux scan.
Clock = namedtuple("Clock", "config time_base")


class DurhamAacError(ValueError):
    """An input, build or proof is not an exact two-track AAC concat."""


def require(condition, reason):
    if not condition:
        raise DurhamAacError(reason)


def span(track):
    """Rational required concat span for one track: DTS or PTS extent.

    Packets are already proven strictly DTS-ordered, so the first and last
    packet bound both extents.
    """
    packets = track.packets
    require(packets, "track has no packets")
    first, last = packets[0], packets[-1]
    dts = (last.dts + last.duration) - first.dts
    pts = (max(packet.pts + packet.duration for packet in packets)
           - min(packet.pts for packet in packets))
    return max(dts, pts) * track.time_base


def scan(ffprobe, source):
    """Pre-mux readback: keep only per-track clocks and the rational span.

    The full packet inventory is deliberately dropped here so that the later
    proof does not hold two complete packet indexes for the whole run on the
    8 GB worker.
    """
    found = coverage.read_tracks(ffprobe, source)
    require(found.keys() == {"video", "audio"},
            "each input needs exactly one H.264 and one AAC-LC track")
    tracks = {kind: Clock(track.config, track.time_base) for kind, track in found.items()}
    clip_span = max(span(found["video"]), span(found["audio"]))
    found.clear()
    return tracks, clip_span


def clocks(tracks):
    """Original video clock and the explicit movie clock, both signed-int32."""
    video = tracks["video"].time_base.denominator
    audio = tracks["audio"].time_base.denominator
    movie = math.lcm(video, audio)
    require(0 < video <= SIGNED_CLOCK_LIMIT and 0 < movie <= SIGNED_CLOCK_LIMIT,
            "track clocks exceed the signed range")
    return video, movie


def durations(spans):
    """Concat duration differences from microsecond ceilings of cumulative spans."""
    waits, cumulative, previous = [], Fraction(0), 0
    for value in spans:
        cumulative += value
        ceiling = math.ceil(cumulative * MICROSECONDS)
        require(ceiling > previous, "concat span does not advance the timeline")
        waits.append(ceiling - previous)
        previous = ceiling
    return waits


def run_tool(command, fds, run):
    """Run one bounded tool with every retained descriptor passed through."""
    try:
        pull.cancellable_tool_output(command, run, run.budget(), stdout_limit=0,
                                     pass_fds=tuple(fds), reject_stderr=True)
    except pull.InventoryScanStopped as error:
        raise coverage.ProofStopped(run.reason or "proof cancelled") from error
    except pull.MediaCertificationError as error:
        run.check()
        raise DurhamAacError("concat build failed") from error


def verify(rows, count):
    """Require every input proven with exact video frames and both AAC rules."""
    require(len(rows) == count, "proof covered a different input count")
    for ordinal, row in enumerate(rows):
        require(row.get("verdict") == "proven" and row.get("ordinal") == ordinal + 1,
                "an input was not proven")
        tracks = row["tracks"]
        video, audio = tracks["video"], tracks["audio"]
        require(video["frame_count"] == video["packet_count"] and video["extra_output_frames"] == 0,
                "H.264 frame count differs from its packet count")
        if ordinal == 0:
            require(audio["rule"] == "standalone_exact" and audio["seam_primer_packets"] == 0,
                    "the first AAC clip must be standalone exact")
        else:
            require(audio["rule"] == "seam_primed" and audio["seam_primer_packets"] == 1,
                    "each later AAC clip must be seam primed")


def timing(rows, tracks, waits):
    """Certify every later input's placement against the assigned concat offsets.

    Each input is proven affine on its own, so a mux that shifts a whole input
    shorter still maps constantly. Only the absolute shift, measured from the
    first input's shift, can prove the cumulative concat offsets.
    """
    base = {kind: Fraction(rows[0]["tracks"][kind]["visible_time_shift"])
            for kind in ("video", "audio")}
    tick = {kind: tracks[0][kind].time_base for kind in ("video", "audio")}
    cumulative = 0
    for index, row in enumerate(rows[1:], 1):
        cumulative += waits[index - 1]
        expected = Fraction(cumulative, MICROSECONDS)
        for kind in ("video", "audio"):
            observed = Fraction(row["tracks"][kind]["visible_time_shift"]) - base[kind]
            require(abs(observed - expected) <= tick[kind] + Fraction(1, MICROSECONDS),
                    "input placement differs from the assigned concat offsets")


def facts(source, row, clip_span, wait):
    """Per-input identity plus the full coverage facts for both tracks."""
    return dict(size_bytes=source.size, sha256=source.sha, span=str(clip_span),
                concat_microseconds=wait, tracks=row["tracks"])


def release(output, attempt):
    """Roll back only the exact inode this attempt linked at ``output``."""
    owned = attempt["owned"]
    if owned is None:
        return
    try:
        info = os.lstat(output)
    except OSError:
        return
    if (info.st_dev, info.st_ino) != owned:
        return
    try:
        os.unlink(output)
        pull.fsync_dir(output.parent)
    except OSError:
        # The requested path already lost our inode or the parent is gone.
        pass


def publish(staged, output, attempt):
    """Exclusively link the held staging inode at ``output`` and record it.

    ``attempt["owned"]`` stays ``None`` until the link succeeds, so the single
    cleanup path can only ever remove an inode this call linked: a pre-existing
    file was never linked and stays intact. ``attempt["fd"]`` is still open
    here, so the recorded device/inode cannot be recycled after the staging
    name is dropped.
    """
    info = os.fstat(attempt["fd"])
    owned = (info.st_dev, info.st_ino)
    try:
        os.link(staged, output)
    except FileExistsError as error:
        raise DurhamAacError("requested output already exists") from error
    attempt["owned"] = owned
    linked = os.lstat(output)
    require(owned == (linked.st_dev, linked.st_ino),
            "published output is not the staged inode")


def concat_aac(inputs, output, tools, *, cancel=None, deadline=None):
    """Build and prove one ordered AAC concat into ``output``.

    ``inputs`` is an ordered sequence of ``{"path": ...}`` or
    ``{"descriptor": ...}`` entries, each with the exact ``size_bytes`` and
    ``sha256`` identity. ``output`` is the private scratch path to publish.
    Returns the exact output identity and the vetted per-input facts. A failed
    proof or unsupported input raises and leaves no requested output.
    """
    run = coverage.Run(cancel, deadline)
    tools = dict(tools or {})
    require(all(tools.get(name) for name in ("ffmpeg", "ffprobe")), "FFmpeg and FFprobe are required")
    ffmpeg, ffprobe = str(tools["ffmpeg"]), str(tools["ffprobe"])
    require(1 <= len(inputs) <= MAX_INPUTS, "between 1 and 200 inputs are supported")
    require(sum(item["size_bytes"] for item in inputs) <= pull.STITCH_MAX_INPUT,
            "inputs exceed the stitch byte limit")
    output = Path(output)
    require(output.parent.is_dir() and not output.parent.is_symlink() and not output.is_symlink(),
            "output parent must be a real private directory")
    # One attempt record for the single cleanup path: it is empty until this
    # call links, so nothing before the link can ever remove a file.
    attempt = {"fd": None, "owned": None}
    try:
        with ExitStack() as stack:
            opened = []
            for item in inputs:
                run.check()
                expected = {"size_bytes": item["size_bytes"], "sha256": item["sha256"]}
                opened.append(coverage.Input(stack, expected=expected, run=run, descriptor=item["descriptor"])
                              if "descriptor" in item else coverage.Input(stack, Path(item["path"]), expected, run=run))
            # Packets are dropped per clip as soon as its clock and span are
            # known, so the proof below never holds two full packet indexes.
            scanned, spans = [], []
            for source in opened:
                found, clip_span = scan(ffprobe, source)
                scanned.append(found)
                spans.append(clip_span)
            for kind in ("video", "audio"):
                configs = [found[kind].config for found in scanned]
                bases = [found[kind].time_base for found in scanned]
                require(all(config == configs[0] for config in configs)
                        and all(base == bases[0] for base in bases),
                        "track configuration or time base changed across the run")
            video_clock, movie_clock = clocks(scanned[0])
            waits = durations(spans)
            with tempfile.TemporaryDirectory(dir=str(output.parent), prefix=".durham-aac-") as scratch:
                manifest = Path(scratch) / "concat.txt"
                manifest.write_text("ffconcat version 1.0\n" + "".join(
                    pull.concat_manifest_line(source.path) + "duration %d.%06d\n" % divmod(wait, MICROSECONDS)
                    for source, wait in zip(opened, waits)))
                staged = Path(scratch) / "output.mp4"
                run_tool([ffmpeg, *coverage.DECODE, "-protocol_whitelist", "file,pipe",
                          "-f", "concat", "-auto_convert", "0", "-safe", "0", "-i", str(manifest),
                          "-copyts", "-map", "0:v:0", "-map", "0:a:0", "-c", "copy",
                          "-video_track_timescale", str(video_clock), "-movie_timescale", str(movie_clock),
                          "-y", str(staged)], [source.fd for source in opened], run)
                # Hold the staged inode open from its first readback until the
                # rollback decision, so its device/inode pair cannot be reused.
                attempt["fd"] = os.open(str(staged), os.O_RDONLY | os.O_NOFOLLOW)
                size, sha = pull.stitch_proof_digest(staged, run)
                rows = coverage.prove_ordered(
                    {"size_bytes": size, "sha256": sha},
                    [{"size_bytes": source.size, "sha256": source.sha} for source in opened],
                    [staged] + [source.fd for source in opened], ffmpeg, ffprobe,
                    cancel=cancel, deadline=deadline)
                run.check()
                verify(rows, len(opened))
                timing(rows, scanned, waits)
                for source in opened:
                    source.check()
                # Every readback and the whole result are final before the name
                # is published, so nothing after the link can invalidate them.
                result = dict(
                    output=dict(size_bytes=size, sha256=sha),
                    timeline=dict(video_track_timescale=video_clock, movie_timescale=movie_clock,
                                  concat_microseconds=waits),
                    inputs=[facts(source, row, clip_span, wait)
                            for source, row, clip_span, wait in zip(opened, rows, spans, waits)],
                )
                os.fsync(attempt["fd"])
                publish(staged, output, attempt)
                # Drop the staging name so the published file is a single-link
                # private regular file for the pinned digest check.
                os.unlink(staged)
                pull.fsync_dir(output.parent)
                require(pull.stitch_proof_digest(output, run) == (size, sha),
                        "published output identity differs")
                run.check()
                os.close(attempt["fd"])
                attempt["fd"] = None
                return result
    except BaseException as error:
        # The single cleanup path for every failure, normalized or not: a
        # requested output this attempt linked never survives, and a
        # pre-existing file was never linked.
        release(output, attempt)
        if isinstance(error, coverage.CoverageError):
            raise DurhamAacError(str(error)) from error
        raise
    finally:
        if attempt["fd"] is not None:
            os.close(attempt["fd"])
            attempt["fd"] = None
