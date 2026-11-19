#!/bin/sh
set -eu
export PATH=/usr/bin:/bin:/usr/sbin:/sbin
export LIMA_HOME=/Users/deniz/.lima-stoarama
exec /Users/deniz/.local/share/stoarama-durham/bin/limactl start --tty=false stoarama-stitch
