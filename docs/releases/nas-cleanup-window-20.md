# NAS cleanup: window 20

Owner decision (2026-11, verbatim): "lets do window 20, just ekep cleaning and reducing".
The NAS cleanup period narrows from 28 to **20 local days** per stream. This is
the policy/window change only; it makes no production operation, publishes no
release and activates no NAS epoch.

## What changes

- **Active cleanup selection.** `backend/internal/api/nas_cleanup_keep20_policy.json`
  is the exact `$policy$` manifest that `infra/sql/migrations/0236_nas_media_work_keep20.sql`
  freezes: 109 windows, 23 `keep_everything`, SHA-256
  `bcc670a34024c2f8e2ba3df2566397f370d03e93ff33a7bb828c4b83eb123393`. Each
  window is `[best_start-3d, best_start+17d)` in the stream's canonical local
  timezone, lands on local midnights, and is contained in the reviewed 28-day
  window. `keep_everything` streams stay whole in R2; their NAS copies are eligible
  only against a live, verified R2 copy (SHA-256, or a bound MD5/ETag match),
  never via the certified-stitch path.
- **DST.** Window length is 20 *local* days. Streams 94 and 17337 cross a
  fall-back transition, so their windows span 481 UTC hours. No code path may
  assume a fixed 480 hours; the Go parser and the Python loader both validate
  local days.
- **Derive and validate, never trust.** The Go parser derives each window from
  the retained legacy `best_start` and refuses any drift; the Python client and
  the plan preparer do the same. A drift keeps everything.
- **Binding.** Modern cleanup lanes (prepare, lease, observe, control, ranking,
  plan digest, report, batch) use and check the 20-day pin consistently. Every
  keep28-bound plan is inert: no new lease, control, permit, recheck or batch,
  and honest old receipts and item evidence are preserved verbatim.
- **`retire-superseded` stays narrow.** Activating the new pin supersedes every
  keep28 plan at once, so retirement never bulk-retires an unresolved item
  (`leased`, `delete_issued`, `unverified`, `conflict`, `missing`), an
  ever-issued plan, an incident-roster identity or a delivery-held file. Those
  are reported as `not_retirable`.
- **Client.** The independent deletion guard reads the 20-day pin for new
  cleanup targets and refuses keep-everything streams on the certified-stitch path. Native stitch and R2
  copy/keep28 retention keep their exact 28-day binding through a separate
  pinned loader.

## What does not change

- The legacy 28-day JSON and its SHA stay byte-for-byte, and
  `api.Keep28Policy()` keeps returning it for the delivery retirement fence, R2
  copy fallback/refresh, keep28 retention, native stitch plans and the read-only
  backlog.
- The 0236 active COPY/stitch epoch bytes are untouched.
- `allow_nas_only` remains rejected; no new scope or authority.
- No low-memory limit, release publication, queue, retirement, restore or
  planner change. The separate planner/KEEP-vs-eligible decision stays in the
  in-flight scope branch.

## Deploy order

1. Backend (both policy files and the inert-plan binding). Every keep28 plan
   becomes inert at once. The deployed `r2-copy-v8-low-memory-cleanup` client
   still guards with the 28-day windows, which contain the 20-day ones, so it
   is stricter: files in the 8-day margin get a conflict observation and are
   never deleted.
2. Client artifact built with the 20-day pin (its own version literal and
   admitting migration).
3. Re-initialize the runner owner manifest (`ownership.py` pins the 20-day
   SHA and refuses the old one), then let the runner pick up the 20-day pin.
   Until step 2 is live, hold the runner: a v8 client would report the margin
   as conflicts and the runner would stop. That stop is expected, not an
   incident.

The 0236 `nas_media_keep20_epoch` activation is a separate, drained,
operator-run step; this change neither activates it nor touches its
`$policy$` bytes.
