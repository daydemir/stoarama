# GSS delivery uptime probe (Mooney)

An external check on the public Global Street Scores delivery that researchers
use. Runs on Mooney, outside Render, every 60 s as the user launchd job
`com.stoarama.gss-uptime`. Each run makes two read-only GETs:

- `https://stoarama.com/global-street-scores-delivery` (the page)
- `https://stoarama.com/api/v1/global-street-scores-delivery/metadata?path=%2Fpublic%2Fmetadata.json`
  (the metadata API; the body must parse as JSON with a `streams` list, so a
  truncated response counts as down)

An endpoint is down on a non-200, a connection failure, bad JSON, or a response
slower than `--slow-seconds` (default 10). Two consecutive down runs (about a
minute) send one `darma msg send <agent>` alert, the same path and agent
(`mooney`) as Rolly's finish dead-man. An unchanged outage reminds every six
hours; a changed condition alerts again; the first passing run after an alert
sends one recovery. A send is tried 3 times (waits 5 s, then 15 s); each failed try logs `rc` and a sanitized stderr tail to the probe log. If all fail, the next run retries (exit 2).

Output is one line per run in `<state>/probe.log`: `probe ok` or
`probe metadata=http_502 page=http_502`. State is `<state>/probe.json`.

## Install on Mooney

```sh
mkdir -m 700 -p ~/stoarama-gss-uptime/bin ~/stoarama-gss-uptime/state
cp ops/mooney/gss-uptime/probe.py ~/stoarama-gss-uptime/bin/
sed -e "s#__INSTALL__#$HOME/stoarama-gss-uptime/bin#; s#__STATE__#$HOME/stoarama-gss-uptime/state#g; s#__NOTIFY_AGENT__#mooney#; s#__HOME__#$HOME#" \
  ops/mooney/gss-uptime/com.stoarama.gss-uptime.plist.template > ~/Library/LaunchAgents/com.stoarama.gss-uptime.plist
plutil -lint ~/Library/LaunchAgents/com.stoarama.gss-uptime.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.stoarama.gss-uptime.plist
tail -1 ~/stoarama-gss-uptime/state/probe.log   # probe ok
```

Stop it with `launchctl bootout gui/$(id -u)/com.stoarama.gss-uptime`. Nothing
watches the probe itself; if Mooney is off, outages go unreported.
