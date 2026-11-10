#!/bin/bash
# progress: counts by status, rate over last 10 min, ETA
D=${REPAIR_BASE:-$(cd "$(dirname "$0")/.." && pwd)}
sqlite3 $D/ledger.sqlite "select status,count(*),printf('%.1f GB',sum(db_size)/1e9) from clips group by 1; select 'last10min done:',count(*) from clips where status='done' and done_at>datetime('now','-10 minutes'); select 'inside-keep pending:',count(*) from clips where prio=0 and status in ('pending','inprogress');"
R=$(sqlite3 $D/ledger.sqlite "select count(*) from clips where status='done' and done_at>datetime('now','-10 minutes')"); P=$(sqlite3 $D/ledger.sqlite "select count(*) from clips where status in ('pending','inprogress')")
[ "$R" -gt 0 ] && echo "rate $((R*6))/h  ETA ~ $((P/(R*6))) h"
launchctl list | grep stoarama.repair; tail -3 $D/run.log
