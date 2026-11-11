#!/bin/sh
# Source credentials only on Jelly, without emitting values or putting them in argv.
set +x
set -eu
umask 077
set -a
. "$HOME/Build/stoarama/local/recording-supervisor.env" >/dev/null 2>&1
. "$HOME/Build/stoarama/local/render.env" >/dev/null 2>&1
. "$HOME/stoarama-cleanup-runner/owner.env" >/dev/null 2>&1
set +a
exec /opt/homebrew/bin/python3 -I "$HOME/Build/stoarama/ops/jelly/cleanup-runner/runner.py" "$@"
