#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  check-tile-mosaic-launch-playwright.sh <base-url> [artifact-directory] [--photo-path <local-path>]

Alternatively set TILE_MOSAIC_BASE_URL and TILE_MOSAIC_ARTIFACT_DIR.
Set TILE_MOSAIC_LIVE_EMAIL only when an explicit real-backend success/duplicate
probe is intended; its value is never printed or written to the JSON report.

This produces deterministic headless Chromium L1 evidence. It does not claim
native Android/iOS L2 coverage.
EOF
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi

BASE_URL="${TILE_MOSAIC_BASE_URL:-}"
if [[ -n "${1:-}" && "${1:-}" != --* ]]; then
  BASE_URL="$1"
  shift
fi
if [[ -z "$BASE_URL" ]]; then
  usage >&2
  exit 64
fi

ARTIFACT_DIR="${TILE_MOSAIC_ARTIFACT_DIR:-}"
if [[ -n "${1:-}" && "${1:-}" != --* ]]; then
  ARTIFACT_DIR="$1"
  shift
fi
if [[ -z "$ARTIFACT_DIR" ]]; then
  SAFE_STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
  ARTIFACT_DIR="artifacts/codex/tile-mosaic-v2-l1-$SAFE_STAMP"
fi

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SITE_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
RUNNER="$SITE_DIR/tests/tile-mosaic-launch.test.mjs"

if [[ ! -f "$RUNNER" ]]; then
  echo "Missing runner: $RUNNER" >&2
  exit 66
fi

if PLAYWRIGHT_ENTRY="$(cd "$SITE_DIR" && node -p "require.resolve('playwright')" 2>/dev/null)"; then
  :
else
  GLOBAL_ROOT="$(npm root -g 2>/dev/null || true)"
  if [[ -n "$GLOBAL_ROOT" && -e "$GLOBAL_ROOT/playwright" ]]; then
    PLAYWRIGHT_ENTRY="$GLOBAL_ROOT/playwright"
  else
    echo "Playwright is unavailable. Use the repository dependency or the centrally installed package." >&2
    exit 69
  fi
fi

mkdir -p -- "$ARTIFACT_DIR"
ARTIFACT_DIR="$(cd -- "$ARTIFACT_DIR" && pwd)"

export PLAYWRIGHT_REQUIRE_PATH="$PLAYWRIGHT_ENTRY"
if ! compgen -G "${PLAYWRIGHT_BROWSERS_PATH:-/nonexistent}/chromium-*" >/dev/null \
  && compgen -G "/opt/ms-playwright/chromium-*" >/dev/null; then
  export PLAYWRIGHT_BROWSERS_PATH=/opt/ms-playwright
fi
cd -- "$SITE_DIR"
exec node "$RUNNER" \
  --base-url "$BASE_URL" \
  --artifacts "$ARTIFACT_DIR" \
  "$@"
