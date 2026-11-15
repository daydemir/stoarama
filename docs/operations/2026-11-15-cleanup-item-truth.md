# Durable per-item results after a cleanup group stops

A bulk group can reach its deadline after one file has been deleted. Earlier
clients left every member at its pre-armed intent when any member had attempted
an unlink. Journal replay consequently reported the entire group as uncertain,
including files the live worker knew it had never touched.

The next release, `cleanup-batch-v8`, preserves each member's own result before
propagating the interruption. It does not change the permit clock or deadline
checks before each media and sidecar syscall:

- A completed file becomes `deleted` only after its parent directory sync.
- A successful media unlink followed by an unsuccessful required sidecar step
  remains `media_deleted_sidecar_failed`.
- A member whose unlink syscall may have run without a known result remains
  `unverified`. Failure to sync a parent makes every attempted member in that
  parent uncertain; other parents retain their own results.
- A live worker that attempted neither unlink records protocol-1
  `unlink_attempted=false` with its refusal reason. The server preserves it as
  `keep`; it is never leased again by this plan.

All touched parent inodes are synced, including a parent reached by a failing
`delete_exact`. The local ledger records the media and sidecar flags and refusal
provenance, then fsyncs before the owned SQLite result transaction. The wire
receipt keeps its existing schema. A failed durability checkpoint leaves the
conservative armed intents. A crash cannot manufacture a certified refusal.
Replay sends stored results and never performs another unlink.

A mixed group still raises its original interruption after the checkpoint. The
runner's uncertainty and refusal thresholds remain unchanged: uncertainty stops
immediately; more than 1% refusals or five consecutive refused ordinals stops the
run through the existing OFF/recovery path. This change does not authorize retry
of historical uncertain or refused items, or closure of any incident.

The heartbeat also reports optional numeric `client_rss_bytes` and
`client_peak_rss_bytes`, read from `VmRSS` and `VmHWM` in `/proc/self/status` on
each heartbeat. Missing or malformed values are omitted. These are the client's
own process values, not the NAS services or child media tools. No resource gate
uses these fields; all existing copy and proof resource floors stay unchanged.

The already reviewed `v85r-nas-proof-v2` artifact remains immutable. Release v8
only after that client, through a separate reviewed operation between cleanup
batches. Its exact capability entry preserves v7, copy-v2 and both proof labels.
No server receipt state machine, policy, recovery reservation, proof audit,
copy behavior or media-tool package changes are part of this fix.
