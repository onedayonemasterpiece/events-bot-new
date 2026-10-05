#!/usr/bin/env bash
set -euo pipefail
ROOT=${1:?managed artifact root required}; shift
DIR=$(cd -- "$(dirname -- "$0")" && pwd)
export PYTHONPATH="$ROOT/deps${PYTHONPATH:+:$PYTHONPATH}"
exec /home/dev/.local/bin/telegram-e2e-run -- /usr/bin/python3 "$DIR/ultra_assemble_albums.py" --root "$ROOT" "$@"
