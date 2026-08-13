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

`passed` is intentionally strict: one run, the complete scheduled envelope,
all five axes passed (or audio truthfully absent), and every stored video/audio
boundary derives from immutable capture provenance. The currently merged
`continuous-source-pts-v1` capture contract freezes rational endpoints but not
packet-edge byte identities, so it cannot earn frame-perfect video or
whole-window PASS. Its exact NAS bytes, decode, native-run, timeline, and local
edge observations remain useful terminal partial evidence. A separate future
capture contract must freeze bounded edge packet identities before the PASS
gate can recognize it; decoded-pixel hashes remain verifier observations. A
multi-run result is terminal
`partial` with `whole_window_continuity=partitioned`, even when bytes, decode,
each run, and every within-run seam pass. Historical reset-timestamp clips may
still earn immutable byte/decode/run facts, but video/audio adjacency remains
terminal `unknown`; it is never retried endlessly or promoted to seamless.
Logical clip wall times and reset per-file PTS do not establish adjacency.

The read API exposes the frozen `qualification_scope` for every task. Missing
frozen qualification-window authority is `byte_run_audit`, never campaign
qualification. The server-derived `qualification_eligible` flag is true only
for an `authoritative_occurrence` whose task and certification are fully
passed and whose exact NAS inventory proof is current; consumers must use this
flag rather than infer qualification from media facts.

A retryable `unknown` attempt asserts no completed axis, including audio
absence; it carries no partial clip/run facts and is safe to repeat. A
deterministic `failed` attempt records only the first canonical clip-decode or
run-concat defect that stopped verification. Every unexecuted later axis stays
`unknown`, so failure evidence never masquerades as an exhaustive sweep.
The worker stops new verification work after 35 minutes of a 45-minute server
lease, reserves at least five minutes for exact-report submission, and retries
only the byte-identical completion. The server returns the already committed
result for an authenticated identical replay; a changed replay is rejected.
Signals, resource exhaustion, I/O or dynamic-library failures, timeouts, and
cancellation remain retryable `unknown`. A terminal media failure requires the
same affirmative corrupt-byte diagnostic on two exact-byte validation passes.

## Prospective capture contract

Capture supports a default-off, exact node-plus-recording admission for
`continuous-source-pts-v1`; every unadmitted recording retains legacy reset
timestamps. Enabling an admission still requires source-native canaries covering
HLS and YouTube, B-frame and VFR presentation order, audio sample continuity,
independent segment playback, lossless concat, upload/ingest, reconnect attempt
boundaries, and safe rollback. Protected live windows must not be the first
canary.

## Segment-duration study

Longer source-native segments reduce seams, reservations, hashes, probes, and
HTTP requests, but enlarge the loss and retry unit. At 60 seconds, one failed
unit costs at most about one minute; 2- and 5-minute units increase reconnect,
upload retry, temporary-space, watchdog, and handoff exposure proportionally.
No duration change belongs in the verifier rollout. Measure native bitrate and
PUT/probe latency first, then canary 2 minutes on a low-risk source. Admit 5
minutes only if worst-case retry plus upload remains within delivery and disk
headroom gates, and recovery testing proves the larger gap risk acceptable.
