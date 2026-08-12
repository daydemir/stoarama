# Native stitch certification

This verifier is read-only. It never rewrites source clips, uploads media,
advances delivery, or deletes anything. Deployment defaults
`STOARAMA_NATIVE_STITCH_ENABLED=false`.

Evidence is reported on five separate axes: exact NAS bytes plus strict clip
decode, stream-copy concat plus decode for each native run, video-frame
adjacency inside each run, audio-sample continuity inside each run, and seamless
whole-window continuity. Every adjacent frozen pair has both a video and audio
fact. Objective capture-attempt, native-layout, or real timeline boundaries are
`not_applicable` seams between independently certified runs; they never get
silently bridged.

`passed` is intentionally strict: one run, all five axes passed (or audio
truthfully absent), and every stored video/audio boundary derives from immutable
continuous-source timestamp provenance. A multi-run result is terminal
`partial` with `whole_window_continuity=partitioned`, even when bytes, decode,
each run, and every within-run seam pass. Historical reset-timestamp clips may
still earn immutable byte/decode/run facts, but video/audio adjacency remains
terminal `unknown`; it is never retried endlessly or promoted to seamless.
Logical clip wall times and reset per-file PTS do not establish adjacency.

## Prospective capture contract

The existing segmented capture uses reset timestamps. A separate capture PR
may evaluate continuous per-generation presentation timestamps, but only after
source-native canaries cover HLS and YouTube, B-frame and VFR presentation
order, audio sample continuity, independent segment playback, lossless concat,
upload/ingest, reconnect generation changes, and safe rollback. Protected live
windows must not be the first canary.

## Segment-duration study

Longer source-native segments reduce seams, reservations, hashes, probes, and
HTTP requests, but enlarge the loss and retry unit. At 60 seconds, one failed
unit costs at most about one minute; 2- and 5-minute units increase reconnect,
upload retry, temporary-space, watchdog, and handoff exposure proportionally.
No duration change belongs in the verifier rollout. Measure native bitrate and
PUT/probe latency first, then canary 2 minutes on a low-risk source. Admit 5
minutes only if worst-case retry plus upload remains within delivery and disk
headroom gates, and recovery testing proves the larger gap risk acceptable.
