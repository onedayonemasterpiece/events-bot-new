#!/usr/bin/env bash
set -euo pipefail
ROOT=${1:?managed artifact root required}; shift
DIR=$(cd -- "$(dirname -- "$0")" && pwd)
/usr/bin/python3 - "$ROOT" <<'PY'
import pathlib,sys
p=pathlib.Path(sys.argv[1]).resolve()
assert p.is_relative_to('/home/dev/artifacts') and (p/'.artifact.json').is_file()
PY
if ! PYTHONPATH="$ROOT/deps" /usr/bin/python3 -c 'import telethon' >/dev/null 2>&1; then
  UV=$(command -v uv || true)
  if [[ -z "$UV" && -x /home/dev/.local/bin/uv ]]; then UV=/home/dev/.local/bin/uv; fi
  if [[ -z "$UV" ]]; then echo 'Local uv package installer unavailable' >&2; exit 2; fi
  "$UV" pip install --no-cache --python /usr/bin/python3 --target "$ROOT/deps" Telethon==1.42.0
fi
export PYTHONPATH="$ROOT/deps${PYTHONPATH:+:$PYTHONPATH}"
exec /home/dev/.local/bin/telegram-e2e-run -- /usr/bin/python3 "$DIR/ultra_component_lab.py" capture-tg --root "$ROOT" "$@"
