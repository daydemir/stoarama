# Quality stream status

Snapshot: 2026-08-26 15:20 UTC (11:20 EDT)

This is an operational snapshot for account 47’s continuous recordings. It is
not a permanent label: the lists should be regenerated after new window-health
rows arrive.

## Definitions

- **Completed** means the recording has at least one exact 14-calendar-day
  contiguous window meeting the tier’s rules below.
- **Candidate** means the recording is active, has not completed that tier,
  and currently has at least 14 scheduled windows remaining through the current
  protected runway (2026-09-09). A candidate is a viable path, not a guarantee;
  future window grades can still disqualify it.
- A recording can be completed for a lower tier and remain a candidate for a
  higher tier.

## Tier rules

Daily grades are derived from the recorded-window health metrics:

- A: zero overlap, coverage ≥99%, largest gap ≤120 seconds.
- B: zero overlap, coverage ≥95%, largest gap ≤900 seconds, at most one gap
  over five minutes, and at most six gaps over 30 seconds.
- C: zero overlap, coverage ≥90%, largest gap ≤1800 seconds, and at most two
  gaps over five minutes.
- D: all other non-E/F/unknown windows.
- E: coverage below 80%.
- F: no clips or zero coverage.
- Unknown: missing or outdated health metrics.

For every tier, the qualifying window must contain 14 contiguous local-calendar
days. Fine+ accepts any grades. Good+ allows no F/unknown days and at most two
E days. Great+ allows only A/B/C days.

## Fine+

Completed (56):

`335, 337, 348, 349, 350, 355, 377, 379, 380, 381, 382, 383, 384, 385, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418, 419, 420, 421, 422, 423, 424, 425, 427, 428, 429, 430, 431, 432, 433, 434, 435, 436, 437, 439, 440, 441, 442, 444, 445`

Candidates (22):

`339, 351, 352, 353, 354, 378, 386, 387, 388, 389, 390, 392, 393, 394, 395, 396, 397, 398, 399, 400, 426, 438`

## Good+

Completed (40):

`335, 337, 348, 350, 355, 377, 379, 380, 382, 383, 384, 385, 401, 403, 404, 406, 408, 409, 413, 416, 417, 418, 419, 420, 421, 422, 423, 424, 425, 427, 428, 429, 430, 431, 437, 439, 440, 441, 444, 445`

Candidates (37):

`339, 349, 351, 352, 353, 354, 378, 381, 386, 387, 388, 389, 390, 392, 393, 394, 395, 396, 397, 398, 399, 400, 402, 405, 407, 410, 411, 412, 414, 426, 432, 433, 434, 435, 436, 438, 442`

## Great+

Completed (22):

`335, 377, 379, 380, 383, 401, 404, 406, 413, 416, 417, 421, 422, 423, 424, 425, 429, 430, 439, 440, 441, 444`

Candidates (52):

`337, 339, 348, 349, 350, 351, 352, 353, 354, 355, 378, 381, 382, 384, 385, 386, 387, 388, 389, 390, 392, 393, 394, 395, 396, 397, 398, 399, 400, 402, 403, 405, 407, 408, 409, 410, 411, 412, 414, 418, 419, 420, 426, 427, 428, 431, 432, 433, 434, 435, 436, 438, 442`

## Monitoring cadence

When no gate needs action, quality/runway/capacity checks use a 30-minute idle
cadence. Safety boundaries remain immediate: a failed health gate, stale relay,
pending upload backlog, source failure, schedule boundary, or capacity/storage
warning triggers an immediate check and response. The existing recording and
runway workers remain unchanged; this cadence does not add a second scheduler.
