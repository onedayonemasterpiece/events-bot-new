#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
SOURCE="$ROOT/serverless/email_inbound"
OUT="${1:-$ROOT/artifacts/codex/email-inbound-functions}"

rm -rf "$OUT"
mkdir -p "$OUT/intake/common" "$OUT/delivery/common" "$OUT/adapter/common" "$OUT/collector/common"

for target in intake delivery adapter collector; do
  cp "$SOURCE/$target/index.py" "$OUT/$target/index.py"
  cp "$SOURCE/$target/requirements.txt" "$OUT/$target/requirements.txt"
  cp "$SOURCE/common/"*.py "$OUT/$target/common/"
  members=(index.py requirements.txt common)
  if [[ "$target" == "collector" ]]; then
    mkdir -p "$OUT/$target/intake"
    cp "$SOURCE/intake/index.py" "$OUT/$target/intake/index.py"
    cp "$SOURCE/intake/__init__.py" "$OUT/$target/intake/__init__.py"
    members+=(intake)
  fi
  (
    cd "$OUT/$target"
    compile_sources=(index.py common/*.py)
    [[ "$target" == "collector" ]] && compile_sources+=(intake/index.py)
    python3 -m py_compile "${compile_sources[@]}"
    find . -type d -name __pycache__ -prune -exec rm -rf {} +
    python3 -m zipfile -c "../email-inbound-$target.zip" "${members[@]}"
  )
done

printf 'Built:\n  %s\n  %s\n' \
  "$OUT/email-inbound-intake.zip" \
  "$OUT/email-inbound-delivery.zip"
printf '  %s\n' "$OUT/email-inbound-adapter.zip"
printf '  %s\n' "$OUT/email-inbound-collector.zip"
