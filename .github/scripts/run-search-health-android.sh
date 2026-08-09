#!/usr/bin/env bash
set -euo pipefail

appium_pid=''
export E2E_APPIUM_LOG_PATH="${RUNNER_TEMP}/appium-search-health-android.log"
export E2E_APPIUM_STATUS_READY=0
cleanup_appium() {
  if [[ -n "${appium_pid:-}" ]]; then
    kill "$appium_pid" || true
    wait "$appium_pid" || true
  fi
  rm -f "$E2E_APPIUM_LOG_PATH" \
    "${RUNNER_TEMP}/appium-search-health-log-filters.json"
}
trap cleanup_appium EXIT

adb shell settings put secure show_ime_with_hard_keyboard 1
cat > "${RUNNER_TEMP}/appium-search-health-log-filters.json" <<'JSON'
[{"pattern":"https?://[^\\s\\\"]+","flags":"i","replacer":"**URL_REDACTED**"}]
JSON
npx appium --base-path /wd/hub --port 4723 \
  --log-level error --log-no-colors \
  --log-filters "${RUNNER_TEMP}/appium-search-health-log-filters.json" \
  --allow-insecure uiautomator2:chromedriver_autodownload \
  > "$E2E_APPIUM_LOG_PATH" 2>&1 &
appium_pid=$!
ready=0
for _ in $(seq 1 60); do
  if curl --max-time 2 -fsS http://127.0.0.1:4723/wd/hub/status >/dev/null; then
    ready=1
    break
  fi
  sleep 1
done
test "$ready" = 1
export E2E_APPIUM_STATUS_READY=1
node e2e/search/production-health-run.mjs
