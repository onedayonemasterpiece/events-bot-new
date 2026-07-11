#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
SOURCE="$ROOT/serverless/email_inbound"
OUT="${1:-$ROOT/artifacts/codex/email-inbound-functions}"

rm -rf "$OUT"
mkdir -p "$OUT/intake/common" "$OUT/delivery/common"

for target in intake delivery; do
  cp "$SOURCE/$target/index.py" "$OUT/$target/index.py"
  cp "$SOURCE/$target/requirements.txt" "$OUT/$target/requirements.txt"
  cp "$SOURCE/common/"*.py "$OUT/$target/common/"
  (
    cd "$OUT/$target"
    python3 -m py_compile index.py common/*.py
    find . -type d -name __pycache__ -prune -exec rm -rf {} +
    python3 -m zipfile -c "../email-inbound-$target.zip" \
      index.py requirements.txt common
  )
done

printf 'Built:\n  %s\n  %s\n' \
  "$OUT/email-inbound-intake.zip" \
  "$OUT/email-inbound-delivery.zip"
