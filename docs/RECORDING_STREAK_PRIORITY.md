# Dynamic recording streak priority

The operating target is not a frozen cohort. Every active, probation, or paused
candidate recording owns its own consecutive completed-window streak. Replacing
a failed source never edits history and the replacement starts at zero.

`GET /api/v1/account/recordings/streak-priority` is the read-only board. It uses
the same GREAT, GOOD, ACCEPTABLE, FAILED, and UNKNOWN timeline thresholds as the
qualification report. A streak may contain at most one ACCEPTABLE window;
FAILED, UNKNOWN, a second ACCEPTABLE, a duplicate/missing job, stale metrics,
late clips, or any overlap ends it. NAS byte proof and native stitch
certification remain explicitly separate.

Operate from the top down:

1. Protect 13/14 and qualified streams. Do not restart, migrate, probe, or make
   cosmetic changes unless current/future bytes are becoming unusable.
2. Protect 10–12 next, then 5–9. Use drained handoffs for necessary work.
3. A FAILED/UNKNOWN result during day one or two triggers immediate source
   identity/reprobe review. Repeated failures in two of the latest three windows
   trigger source repair or replacement review instead of consuming capacity.
4. Admit a replacement only after frame identity, bounded native capture,
   relay/cloud headroom, and NAS gates pass. Its streak begins independently.
5. Keep failed windows and replaced recordings visible. Never rewrite them into
   a successor's streak or call timeline candidates NAS/stitch certified.

Review the board at least every eight hours with the health digest. The digest
remains the current-incident alerting view; this board is the longitudinal
priority view.
