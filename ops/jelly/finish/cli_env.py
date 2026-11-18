"""Keep the existing DB identity shim; execute only the shared bound CLI."""
import os
import subprocess
import sys
import urllib.parse
from cli_binding import bound_cli


def main():
    cli = bound_cli()
    u = urllib.parse.urlsplit(os.environ['DATABASE_URL'])
    opts = ('-c role=stoarama -c statement_timeout=10000 -c lock_timeout=2000 '
            '-c work_mem=16MB -c jit=off -c max_parallel_workers_per_gather=0 '
            '-c default_transaction_isolation=read\\ committed')
    env = os.environ.copy()
    env.update(PGHOST=u.hostname, PGPORT=str(u.port or 5432),
               PGUSER=urllib.parse.unquote(u.username), PGPASSWORD=urllib.parse.unquote(u.password),
               PGDATABASE=u.path.lstrip('/'), PGOPTIONS=opts, PGCONNECT_TIMEOUT='10')
    for key, value in urllib.parse.parse_qsl(u.query):
        if key in ('sslmode', 'connect_timeout'):
            env['PG' + key.upper()] = value
    sql = ("BEGIN READ ONLY;DO $$BEGIN IF session_user<>'stoarama_20260821' THEN RAISE EXCEPTION 'session';END IF;"
           "IF current_user<>'stoarama' THEN RAISE EXCEPTION 'role';END IF;"
           "IF current_database()<>'stoarama' THEN RAISE EXCEPTION 'database';END IF;"
           "IF current_setting('transaction_isolation')<>'read committed' THEN RAISE EXCEPTION 'isolation';END IF;END$$;ROLLBACK;")
    result = subprocess.run(['psql', '-XqAt', '-v', 'ON_ERROR_STOP=1'], input=sql, env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True, timeout=25)
    if result.returncode:
        return 1
    query = dict(urllib.parse.parse_qsl(u.query))
    query['options'] = opts
    os.environ['DATABASE_URL'] = urllib.parse.urlunsplit(u._replace(query=urllib.parse.urlencode(query)))
    os.environ['PGOPTIONS'] = opts
    os.execv(str(cli), [str(cli)] + sys.argv[1:])


if __name__ == '__main__':
    raise SystemExit(main())
