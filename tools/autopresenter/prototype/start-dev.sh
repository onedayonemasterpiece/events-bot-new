#!/usr/bin/env bash
set -Eeuo pipefail

PROTOTYPE_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$PROTOTYPE_DIR/../../.." && pwd)"
SITE_DIR="$REPO_ROOT/site"
AGENT_DIR="$REPO_ROOT/tools/autopresenter/agent"
RELAY_SERVER="$REPO_ROOT/tools/autopresenter/relay/server.py"
RUNTIME_DIR="$PROTOTYPE_DIR/.runtime"
VENV_DIR="$PROTOTYPE_DIR/.venv"

SITE_PORT="${AUTOPRESENTER_SITE_PORT:-4321}"
RELAY_PORT="${AUTOPRESENTER_RELAY_PORT:-8787}"
RELAY_HOST="${AUTOPRESENTER_RELAY_HOST:-127.0.0.1}"
RELAY_URL="${AUTOPRESENTER_RELAY_URL:-http://127.0.0.1:${RELAY_PORT}}"
STAGE_URL="${AUTOPRESENTER_STAGE_URL:-http://127.0.0.1:${SITE_PORT}/internal/presenter-stage/}"
CONTROL_URL="${AUTOPRESENTER_CONTROL_URL:-http://127.0.0.1:${RELAY_PORT}/control/}"
SKIP_INSTALL="${AUTOPRESENTER_SKIP_INSTALL:-0}"

mkdir -p "$RUNTIME_DIR"
PIDS=()

cleanup() {
  local exit_code=$?
  trap - EXIT INT TERM
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  for pid in "${PIDS[@]:-}"; do
    wait "$pid" 2>/dev/null || true
  done
  exit "$exit_code"
}
trap cleanup EXIT INT TERM

wait_for_url() {
  local url=$1
  local label=$2
  for _ in {1..120}; do
    if curl --fail --silent --show-error "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep .25
  done
  printf 'Timeout waiting for %s: %s\n' "$label" "$url" >&2
  return 1
}

if [[ "$SKIP_INSTALL" != "1" ]]; then
  if [[ ! -d "$SITE_DIR/node_modules" ]]; then
    printf 'Installing pinned Astro dependencies…\n'
    npm --prefix "$SITE_DIR" ci
  fi
  if [[ ! -d "$AGENT_DIR/node_modules" ]]; then
    printf 'Installing pinned Playwright agent dependencies…\n'
    npm --prefix "$AGENT_DIR" ci
  fi
  printf 'Checking the pinned Playwright-managed browser…\n'
  npm --prefix "$AGENT_DIR" exec -- playwright install chromium
fi

PYTHON_BIN="${AUTOPRESENTER_PYTHON:-}"
if [[ -z "$PYTHON_BIN" ]] && python3 -c 'import aiohttp' >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python3)"
fi
if [[ -z "$PYTHON_BIN" ]]; then
  if [[ ! -x "$VENV_DIR/bin/python" ]]; then
    printf 'Creating the relay virtual environment…\n'
    python3 -m venv "$VENV_DIR"
    "$VENV_DIR/bin/python" -m pip install --disable-pip-version-check \
      --requirement "$PROTOTYPE_DIR/requirements.txt"
  fi
  PYTHON_BIN="$VENV_DIR/bin/python"
fi
"$PYTHON_BIN" -c 'import aiohttp' >/dev/null

printf 'Starting Astro site on %s\n' "$STAGE_URL"
npm --prefix "$SITE_DIR" run dev -- --port "$SITE_PORT" &
PIDS+=("$!")

printf 'Starting relay on %s\n' "$CONTROL_URL"
"$PYTHON_BIN" "$RELAY_SERVER" --host "$RELAY_HOST" --port "$RELAY_PORT" &
PIDS+=("$!")

wait_for_url "http://127.0.0.1:${SITE_PORT}/internal/presenter-stage/" "Astro stage"
wait_for_url "http://127.0.0.1:${RELAY_PORT}/healthz" "relay"

export AUTOPRESENTER_RELAY_URL="$RELAY_URL"
export AUTOPRESENTER_STAGE_URL="$STAGE_URL"

printf '\nAutopresenter is ready.\n'
printf 'Control: %s\n' "$CONTROL_URL"
printf 'Stage:   %s\n' "$STAGE_URL"
printf 'Stop all processes with Ctrl+C.\n\n'

if [[ "${AUTOPRESENTER_HEADLESS:-0}" != "1" && -z "${DISPLAY:-}" && -x "$(command -v xvfb-run || true)" ]]; then
  printf 'DISPLAY is absent; launching headed Chromium inside Xvfb for this dev host.\n'
  xvfb-run -a -s "-screen 0 1920x1080x24" npm --prefix "$AGENT_DIR" start &
else
  npm --prefix "$AGENT_DIR" start &
fi
AGENT_PID=$!
PIDS+=("$AGENT_PID")

wait "$AGENT_PID"
