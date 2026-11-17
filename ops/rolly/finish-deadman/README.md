# Rolly finish dead-man (independent watchdog for the Jelly finish tick)

Runs on Rolly, not Jelly, so it survives a dead or wedged Jelly. Every 300 s it
reads `<status>/status.json` over SSH with a read-only `cat -- <path>` and checks
the age of `updated_at` and the presence of `stop_reason`. It never touches Jelly
state, launchd or the tick; the only write is its own local state directory.

One tick may legitimately run about 450 s, and lock-contended launches write
nothing at all, so `--max-age-seconds` defaults to 900 s — three Jelly launch
intervals plus a full tick's own budget, without reporting a healthy-but-busy tick.

## Conditions

Checked in this order, first match wins:

| Condition | Meaning |
|---|---|
| `unreachable` | `ssh` timed out or exited 255 (host down, network, auth) |
| `missing` | any other nonzero exit: status file absent or unreadable |
| `malformed` | not a JSON object, wrong `schema`, unparseable `updated_at`, or non-string `stop_reason` |
| `future` | `updated_at` more than 30 s ahead of Rolly's clock |
| `stopped` | `stop_reason` present; detail is the reason, or `unknown:<value>` |
| `stale` | `updated_at` older than `--max-age-seconds` |
| `ok` | fresh and no `stop_reason` |

`stopped` is checked before `stale`: a stopped tick is the more specific report,
even when its timestamp is old.

## Alerts

All messages go to `--notify-agent` through `darma msg send`.

- **Entering** a non-OK condition, or a changed condition/detail, sends one alert
  immediately — except `unreachable`, which alerts only on the **second**
  consecutive unreachable run, so a single network blip is silent.
- A condition that stays the same sends a **reminder** once
  `--remind-seconds` (default 21600 s, six hours) have passed since the last alert.
- Returning to `ok` after an alerted non-OK condition sends **one recovery**
  message. A silent `ok` run never messages.
- If `darma msg send` fails, `last_alert_at` is not recorded, so the next run
  retries, and the run exits 2 with `deadman notification_failed`.

Output is one line, `deadman <condition> [<detail>]`, using typed values only —
never stderr or file contents, which could leak host or path detail. Exit 0 when
OK, 1 when non-OK, 2 when the state directory or notification failed. Durable
state lives in `<state>/deadman.json`, replaced atomically and fsynced.

## Install on Rolly

1. Copy `deadman.py` to a private install directory, e.g. `~/stoarama-deadman/bin/`
   (mode 700). Nothing else from the repo is needed.
2. Create the private state directory and its log:
   `mkdir -m 700 -p ~/stoarama-deadman/state`.
3. Render the plist with **XML-escaped, absolute** values:
   `__INSTALL__` (install dir), `__SSH_TARGET__` (e.g. `jelly`), `__STATUS_PATH__`
   (absolute path of the Jelly status file), `__STATE__` (the state dir) and
   `__NOTIFY_AGENT__` (Deniz's `darma msg` target). Render to
   `~/Library/LaunchAgents/com.stoarama.finish-deadman.plist`.
4. Validate: `plutil -lint ~/Library/LaunchAgents/com.stoarama.finish-deadman.plist`.
5. Confirm key-based SSH works non-interactively from Rolly first
   (`BatchMode=yes` is forced; there is no password prompt).
6. Load it: `launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.stoarama.finish-deadman.plist`.
7. Verify the first run: check the last line of `<state>/deadman.log` is
   `deadman ok` and that `<state>/deadman.json` exists with
   `"condition": "ok"`. Then confirm the next automatic run a few minutes later.

Stop it with `launchctl bootout gui/$(id -u)/com.stoarama.finish-deadman`.

## Limits

Nothing watches the dead-man itself. It is deliberately the last line: if Rolly
is off, or this launchd job is unloaded, failures on Jelly go unreported. Its own
`RunAtLoad` plus 300 s `StartInterval` job is the final layer, so check its log
and `deadman.json` by hand after reinstalling or rebooting Rolly.
