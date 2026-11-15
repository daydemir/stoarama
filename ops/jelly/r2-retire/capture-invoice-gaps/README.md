# Historical capture invoice gaps

This operator CLI preserves exactly seven DigitalOcean GET responses: original
AY account before/after, February/March/April 2026 invoice page 1, and
August/September page 2. It reparses raw bodies and checks them against pinned
existing historical records. It grants no placement, owner-death, writer-drain
or cancellation authority. It has no schedule or automatic caller.

Code approval or merge grants **no staging, collection, export or production
operation**. A fresh exact-byte operational review and controller timing grant
must bind the source and transport hashes, argv, new run/capture paths, original
account and complete baseline. Existing R2 index/query grants cannot be reused.
Do not execute either collection command while preparing that review.

## Local preparation

Run the offline synthetic suite, with no credentials or provider access:

```sh
python3 -B -m unittest discover -s ops/jelly/r2-retire/capture-invoice-gaps -p 'test_*.py' -v
```

`transport.py plan --collector-file /absolute/capture_invoice_gaps.py
--source-sha SHA --transport-file /absolute/transport.py --transport-sha SHA`
prints a data-only SSH argv. Both SHA values must come from the reviewed exact
files; they are not self-asserted authority. A future operation separately
stages public source and `baseline-pins.json` without moving credentials.
The CLI does not stage, repair or recreate remote inputs.

The finite paths are constants in both scripts. Baseline files remain on Jelly
under `~/stoarama-evidence/do-capture-provenance-20261115/ay-team/`. The committed
pin manifest contains filenames, sizes and hashes, not invoice bodies or auth.
Any changed remote path, baseline or executable bytes requires a new pinned
packet and review. The output's timestamp-shaped name is only an identifier,
not evidence freshness or a grant. An existing output path is never reused.

## Future separately authorized invocation

Only after that operational grant, `transport.py collect` takes the same four
public-file/pin arguments as `plan`, plus `--capture-dir /new/local/path`.
It reserves that local directory exclusively before starting SSH, and preserves
private intent, stdout, stderr and terminal-result receipts. A second invocation
with that path refuses without SSH. SSH agent forwarding is disabled; no token
is read or moved on the control host.

On Jelly the inline, checksum-bound launcher checks the public collector, then
runs a preparer child and observes its exit. The child exclusively reserves the
remote output and binds its **parent PID** to the later exec. Only afterward
does the launcher open Jelly's existing `local/do-capture.env`, with literal-only
parsing, selecting `DIGITALOCEAN_TOKEN` into a whitelisted exec environment.
The launcher execs the already verified collector bytes in memory, preserving
its PID. There is no executable-path reopen, shell sourcing, account fallback,
redirect, proxy, retry or next-page discovery.

The one absolute 55-second monotonic budget covers reservation, baseline reads,
credential parsing, exec, seven requests and durable completion. It cannot be
reset by exec or a slow fsync. Normal work stops at 50 seconds, reserving up to
5 seconds **inside** that same hard deadline for cooperative cleanup. A second
alarm protects any failure persistence until the original hard end; expired
attempts skip durable failure writes and leave partial evidence/HOLD. The armed
timer survives exec; interruption during
interpreter startup may leave only a reserved/partial attempt. That is HOLD,
never inferred success. Per-request connect/read limits are 3/6 seconds.
Each response must have one decimal Content-Length between 1 byte and 2 MiB
and no Transfer-Encoding. The parser must receive exactly that complete body;
premature EOF, ambiguous framing and close-delimited/chunked bodies refuse
before retention, even if the bytes already form valid JSON. Actual provider
framing compatibility remains unproved until the separate operational phase.
The input-channel watcher stops the owned process on EOF, unexpected input or
channel failure. It stays active through manifest/checksum writes; its exit and
a final channel check precede emitting success. A durable manifest can therefore
exist in a failed attempt; it never replaces the successful terminal receipt.
Artifact, receipt and stdout writes use single raw descriptor writes. Partial
writes mean HOLD; no buffered close, deallocation or interpreter-shutdown flush
can retry them after cancellation. Remote Python also uses `-u`. The watcher
never consumes forbidden input: readiness/error is latched independently of
shutdown, even when a late signal is suppressed.

The local SSH wait is capped at 60 seconds. Timeout or interruption closes its
owned input and cooperatively TERM/reaps only its own SSH process, with a
5-second reap limit and **no SIGKILL or remote/session kill**. A process that
cannot be reaped is explicitly recorded as such; no exit is inferred. Such a
receipt requires operator reconciliation, and the attempt stays consumed.
Remote preparer status is independent of SSH exit. A bounded nonblocking pipe
receipt explicitly reports whether the preparer's exit was observed, including
false/null when unproved. Missing, partial or invalid status stays explicitly
unproved in the local receipt; there is no late fsync or status-write retry.

Accept collection only with the complete terminal exit-zero/empty-stderr
transport receipt and its bound manifest, raw bodies, envelopes and checksums.
A manifest file alone, partial output, timeout, nonzero result or missing reply
means **HOLD: preserve state, no retry, no authority**. Remote `collect` also
refuses an existing `started.json`; it never resumes partial work.

Retrieval is a different, explicitly granted nonsecret export. Then
`capture_invoice_gaps.py validate --baseline-root DIR --pins FILE
--capture-root DIR` checks all seven preserved body/envelope identities and
page closure offline. It does not upgrade a failed transport to success or
establish ownership. Actual placement coverage and fresh provider stop proofs
need their own reviewed formats, collection plans and grants. No SQL, writer
transition, deployment, NAS change, R2 deletion or cleanup-plan mutation is
available here.
