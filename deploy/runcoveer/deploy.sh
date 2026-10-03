#!/usr/bin/env bash
set -euo pipefail
HOST=${RUNCOVEER_HOST:-runcoveer-production}
ROOT=/opt/runcoveer/deployment
# Restore is deliberately separate: repeat deployment never overwrites data.
rsync -a "$(dirname "$0")/" "$HOST:$ROOT/"
ssh "$HOST" "cd $ROOT && sudo docker compose up -d --wait --wait-timeout 240"
