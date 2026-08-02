#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
OUT="$ROOT/artifacts/codex/focus-auth-email-hook"
STAGE=$(mktemp -d)
trap 'rm -rf "$STAGE"' EXIT
mkdir -p "$OUT"
cp "$ROOT/serverless/focus_auth_email_hook/index.py" "$STAGE/index.py"
cp "$ROOT/serverless/focus_auth_email_hook/requirements.txt" "$STAGE/requirements.txt"
python3 -m compileall -q "$STAGE"
STAGE="$STAGE" OUT="$OUT" python3 - <<'PY'
import os
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo
stage = Path(os.environ["STAGE"])
target = Path(os.environ["OUT"]) / "function.zip"
with ZipFile(target, "w", compression=ZIP_DEFLATED, compresslevel=9) as archive:
    for name in ("index.py", "requirements.txt"):
        info = ZipInfo(name, date_time=(2026, 1, 1, 0, 0, 0))
        info.compress_type = ZIP_DEFLATED
        info.external_attr = 0o100644 << 16
        archive.writestr(info, (stage / name).read_bytes())
PY
sha256sum "$OUT/function.zip" > "$OUT/function.zip.sha256"
echo "$OUT/function.zip"
