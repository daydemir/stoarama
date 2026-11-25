"""usage: python3 -I wait_dns.py COMMAND [ARG...]

A cold boot starts launchd jobs before DNS is up; the runner then fails on a
transient. Wait while the production DB host does not resolve, then exec the
command. Only a resolver miss waits, for at most DEADLINE_SEC; after that it
exits EX_TEMPFAIL (75), which launchd does not restart, so the outage stays
visible. A missing env file or DATABASE_URL fails at once."""
import os
import socket
import sys
import time
import urllib.parse

ENV = os.path.expanduser('~/Build/stoarama/local/recording-supervisor.env')
DEADLINE_SEC = 600
EX_TEMPFAIL = 75


def db_host():
    for line in open(ENV):
        key, _, value = line.strip().removeprefix('export ').partition('=')
        if key == 'DATABASE_URL':
            return urllib.parse.urlsplit(value.strip().strip('"\'')).hostname
    raise SystemExit('wait_dns: DATABASE_URL missing from ' + ENV)


def main():
    host = db_host()
    deadline = time.monotonic() + DEADLINE_SEC
    while True:
        try:
            socket.getaddrinfo(host, 5432)
            break
        except socket.gaierror:
            if time.monotonic() >= deadline:
                print('wait_dns: DB host did not resolve within %d s' % DEADLINE_SEC, file=sys.stderr)
                sys.exit(EX_TEMPFAIL)
            time.sleep(15)
    os.execvp(sys.argv[1], sys.argv[1:])


if __name__ == '__main__':
    main()
