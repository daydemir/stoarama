# Cloud recording gaps, 2026-09-30

Ubuntu unattended upgrades restart the whole `stoarama-recording.service` when
`needrestart` detects replaced libraries in its FFmpeg children. The Go worker
and all captures in its cgroup stop together. Their database leases remain live
until expiry, then the normal expired-lease reclaim makes them available again.
Repeated package transactions restart workers repeatedly and extend the gaps.

The incident is a provisioning-policy defect, rather than a pool rebalance or an
insufficient number of provisioned slots. All production inspection was read-only;
no environment, database, service, or droplet configuration was changed.

## Evidence

Inspection used `origin/main` at `68a9b5c2`, production PostgreSQL with
`default_transaction_read_only=on`, Render's logs API, and read-only SSH log
inspection on all seven active recorder droplets. Journal records before some
package-induced journald restarts were missing; `/var/log/syslog` retained them.
All times below are UTC.

The installed `apt-daily-upgrade.timer` specifies:

```ini
OnCalendar=*-*-* 6:00
RandomizedDelaySec=60m
Persistent=true
```

This explains the daily 06:00–07:00 clustering independently of recording schedules.
The upgraded packages included OpenSSL/libssl on 2471 and 2475. The newer workers
2476 and 2478 ran many package transactions, each capable of restarting services.
Their unattended-upgrades dpkg logs explicitly contain
`systemctl restart fwupd.service packagekit.service stoarama-recording.service`.

| Droplet / node | Upgrade starts | Recording worker restarts | Affected recordings |
| --- | --- | --- | --- |
| 2471 / 2510 | 06:17:40 | 06:18:05 | 337, 355, 385, 453 |
| 2475 / 2514 | 06:19:14 | 06:19:31 | 439, 441, 454, 456 |
| 2478 / 2517 | 06:20:31 | 06:24:20, 06:25:45, 06:29:23, 06:33:59 | 335, 339, 377, 408, 413 |
| 2476 / 2515 | 06:43:01 | 06:46:06, 06:48:05, 06:49:56, 06:54:44 | 339, 440, 444, 455, 457 |

2477, 2479, and 2480 also ran their daily upgrade jobs, but their recorder services
did not restart during the inspected interval. Their recordings avoided this
incident. Both older and desired-build workers restarted; stale build retirement
is not the trigger. On September 29, retained logs on 2471 show worker restarts
at 06:45:52, 06:48:09, and 06:51:43 during unattended upgrades; 2475 restarted at
06:31:45 and 06:32:18. This independently confirms the same daily mechanism on
the preceding day; the destroyed September 29 workers cannot be inspected by SSH.

### Lease expiry and reacquisition

These jobs have 60-second clips. The API renews leases for
`60 + recordingCaptureTimeoutMarginSec(90) + recordingUploadMarginSec(60) = 210`
seconds. A stopped worker cannot resume its in-memory jobs after restarting; its
new polling loop waits for those still-leased slots to expire. For example:

- 2471 restarts at 06:18:05; new media starts at 06:21:48–51, 223–226 seconds later.
- 2475 restarts at 06:19:31; new media starts at 06:23:04–07, 213–216 seconds later.
- 2476 restarts at 06:49:56; new media starts at 06:53:32–36, 216–220 seconds later.
- 2476 restarts again at 06:54:44; new media starts at 06:58:16–19.

The successful reclaim UPDATE is not logged, so its exact executing process
cannot be assigned from persisted evidence. Both
`dropletpool.Store.ReclaimExpiredLeases` and `recsched.EnqueueDueRecordingJobs`
use the same expired-only requeue predicate. It clears lease owner/token and
returns the job to pending. The subsequent cloud lease UPDATE increments the
attempt count and generates the next token. The timing fits this mechanism.

Joining database clip timestamps/job IDs to worker ingest logs identifies the
droplet for both sides of every gap: **23 of 26 reacquisitions stayed on the same
droplet; only three moved**. A token change establishes a new lease generation,
not necessarily a different worker. Three movements were 413 (2478 → 2471),
335 (2478 → 2475), and 339 (2478 → 2476).

The 26 generation boundaries below affect 17 recordings and sum to **161.73
minutes**, reproducing the reported September 30 loss. Media gaps can begin
before the actual service stop because partial/buffered segments never became
accepted clips. Times in the table omit fractional seconds; durations retain them.

| Recording | Old generation ends | New generation starts | Gap seconds | Droplet before → after |
| --- | --- | --- | --- | --- |
| 385 | 06:15:43 | 06:21:50 | 366.3 | 2471 → 2471 |
| 453 | 06:15:44 | 06:21:51 | 366.1 | 2471 → 2471 |
| 337 | 06:17:39 | 06:21:49 | 249.1 | 2471 → 2471 |
| 454 | 06:17:44 | 06:23:07 | 322.7 | 2475 → 2475 |
| 456 | 06:17:46 | 06:23:07 | 320.8 | 2475 → 2475 |
| 355 | 06:18:00 | 06:21:48 | 227.4 | 2471 → 2471 |
| 441 | 06:18:25 | 06:23:04 | 279.0 | 2475 → 2475 |
| 439 | 06:19:22 | 06:23:05 | 222.5 | 2475 → 2475 |
| 408 | 06:23:18 | 06:28:02 | 284.0 | 2478 → 2478 |
| 413 | 06:23:18 | 06:28:02 | 284.0 | 2478 → 2471 |
| 377 | 06:23:18 | 06:28:02 | 283.3 | 2478 → 2478 |
| 335 | 06:23:20 | 06:28:02 | 281.6 | 2478 → 2478 |
| 339 | 06:24:19 | 06:28:01 | 221.7 | 2478 → 2478 |
| 339 | 06:29:01 | 06:37:35 | 513.8 | 2478 → 2476 |
| 377 | 06:29:01 | 06:37:36 | 514.0 | 2478 → 2478 |
| 408 | 06:29:02 | 06:37:36 | 514.0 | 2478 → 2478 |
| 335 | 06:29:03 | 06:37:34 | 510.6 | 2478 → 2475 |
| 455 | 06:42:42 | 06:53:36 | 653.5 | 2476 → 2476 |
| 457 | 06:42:43 | 06:58:19 | 935.7 | 2476 → 2476 |
| 444 | 06:45:22 | 06:53:35 | 493.0 | 2476 → 2476 |
| 440 | 06:45:22 | 06:53:34 | 492.0 | 2476 → 2476 |
| 339 | 06:45:36 | 06:53:32 | 475.7 | 2476 → 2476 |
| 339 | 06:54:32 | 06:58:16 | 223.8 | 2476 → 2476 |
| 440 | 06:54:34 | 06:58:18 | 224.0 | 2476 → 2476 |
| 444 | 06:54:35 | 06:58:18 | 223.0 | 2476 → 2476 |
| 455 | 06:54:36 | 06:58:19 | 222.1 | 2476 → 2476 |

## Competing hypotheses

- **Pool rebalance / activation / stale builds:** 2478–2480 activated at
  05:36:15, 05:36:23, and 05:37:18, well before the gaps. No droplet drained or
  was destroyed in the incident hour. Build rollout only drains idle workers;
  expired-only reclaim cannot evict healthy leases.
- **Provisioned capacity:** 33 overlapping cloud jobs, of which 23 start at
  06:00, fit seven active capacity-5 workers (35 slots). No kernel OOM evidence
  was found in the inspected upgrade logs. Nominal capacity alone does not prove
  unlimited CPU/network headroom, but these gaps have explicit service-stop
  evidence. Additional droplets do not fix that trigger. Recommended pool
  configuration change: none; additional daily droplet spend: $0.
- **No-progress surrender / #384 / handoff_owner:** the clustered jobs were
  interrupted externally. Their 210-second recovery boundary matches expiry,
  rather than the five-minute no-progress surrender threshold. No failure rows
  were present for 05:55–07:05. Recording 438 separately retains a five-minute
  no-progress error caused by source HTTP 404s; it has no accepted clips in the
  reconstructed gap set and is a different failure.
- **DNS / firewall:** immediate DNS connection-refused errors accompany
  `systemd-resolved` package restarts. They are part of the upgrade event; there
  is no evidence that the egress refresh initiated the worker stops. Its rule
  updates are atomic and its service does not restart the recording worker.
- **Heartbeat expiry:** confirmed downstream of worker termination, rather
  than an otherwise-running worker's unexplained stalled renewal loop.

Render `/v1/logs`, queried for both requested resource IDs and their owner over
05:55–07:05, returned 42 recorder-control application records (all metering) and
zero API records, including without an app-type filter. No successful tick or
reclaim audit was available there. Current `recorder_droplets.last_seen_at`,
`idle_since`, and `recording_jobs` lease fields are mutable snapshots, not a
historical heartbeat ledger. `capture_session_leases` had no rows for these
recordings; recording workers use `recording_jobs` for this lease path.

## Fix and rollout boundary

Cloud-init now writes `/etc/needrestart/conf.d/stoarama-recording.conf` with an
exact service override:

```perl
$nrconf{override_rc}->{qr(^stoarama-recording\.service$)} = 0;
```

This is the [Ubuntu-supported per-service exception](https://discourse.ubuntu.com/t/needrestart-changes-in-ubuntu-24-04-service-restarts/44671).
Automatic OS updates and other service restarts continue. Existing FFmpeg
processes retain loaded libraries until their capture exits; later capture
processes load updated libraries. Worker replacement remains owned by the pool's
idle drain/build rollout. The generated drop-in is evaluated by a Perl regression
test that verifies its syntax, exact service matching, and preservation of existing
overrides.

The change only affects newly provisioned droplets. Existing droplets require a
separately authorized rollout or configuration update; merely deploying the
controller does not rewrite their cloud-init. No rollout was performed in this
investigation. After an approved rollout, verify the drop-in on new workers and
check that the next daily apt run leaves recorder service PIDs and lease
generations intact.
