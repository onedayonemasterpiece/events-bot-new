#!/usr/bin/env bash
# Live E2E driver for the partner promo flow.
#
# Prereqs in .env (or process env):
#   - TELEGRAM_AUTH_BUNDLE_E2E  (already set in this repo)
#   - TELEGRAM_API_ID / TG_API_ID
#   - TELEGRAM_API_HASH / TG_API_HASH
#   - TELEGRAM_BOT_TOKEN — preferably the @eventsbotTestBot token, NOT prod,
#     to avoid fighting webhook with prod.
#
# Effect:
#   1. Refresh prod DB snapshot (skipped if <6h fresh).
#   2. Prepare isolated E2E copy and set DB_PATH to it.
#   3. Seed the E2E user as partner of "Научная библиотека" in that copy.
#   4. Run the bot in polling mode (background) against DB_PATH.
#   5. Run the behave feature for partner promo.
#   6. Revert the partner role and stop the bot — even on failure.

set -euo pipefail

cd "$(dirname "$0")/.."

# Pull .env into the process so child commands see it.
if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

required=(TELEGRAM_AUTH_BUNDLE_E2E TELEGRAM_BOT_TOKEN)
for v in "${required[@]}"; do
  if [ -z "${!v:-}" ]; then
    echo "FATAL: $v is required in .env (or env)." >&2
    exit 2
  fi
done
if [ -z "${TELEGRAM_API_ID:-}${TG_API_ID:-}" ]; then
  echo "FATAL: TELEGRAM_API_ID (or TG_API_ID) is required." >&2
  exit 2
fi
if [ -z "${TELEGRAM_API_HASH:-}${TG_API_HASH:-}" ]; then
  echo "FATAL: TELEGRAM_API_HASH (or TG_API_HASH) is required." >&2
  exit 2
fi

# Step 1+2: snapshot + isolated copy.
eval "$(./scripts/prepare_e2e_db_from_prod_snapshot.sh --max-age-hours 6)"
echo "DB_PATH=$DB_PATH"

# Step 3: seed E2E user as partner. user_id is auto-derived from Telethon.
ORG="${PARTNER_E2E_ORG:-Научная библиотека}"
USER_ID="${PARTNER_E2E_USER_ID:-}"
if [ -z "$USER_ID" ]; then
  USER_ID=$(.venv/bin/python -c "
import asyncio, base64, json, os
from telethon import TelegramClient
from telethon.sessions import StringSession

async def main():
    raw = os.environ['TELEGRAM_AUTH_BUNDLE_E2E']
    bundle = json.loads(base64.urlsafe_b64decode(raw.encode('ascii')).decode('utf-8'))
    api_id = int(os.environ.get('TELEGRAM_API_ID') or os.environ.get('TG_API_ID'))
    api_hash = os.environ.get('TELEGRAM_API_HASH') or os.environ.get('TG_API_HASH')
    client = TelegramClient(StringSession(bundle['session']), api_id, api_hash,
                            device_model=bundle.get('device_model','PC'),
                            system_version=bundle.get('system_version','Linux'),
                            app_version=bundle.get('app_version','1.0'),
                            lang_code=bundle.get('lang_code','en'),
                            system_lang_code=bundle.get('system_lang_code','en'))
    await client.connect()
    me = await client.get_me()
    print(me.id)
    await client.disconnect()

asyncio.run(main())
")
fi
echo "E2E user_id=$USER_ID"
.venv/bin/python scripts/dev_set_partner_role.py set \\
    --db "$DB_PATH" \\
    --user-id "$USER_ID" \\
    --organization "$ORG"

# Ensure cleanup runs even on failure.
BOT_PID=
cleanup() {
  if [ -n "$BOT_PID" ] && kill -0 "$BOT_PID" 2>/dev/null; then
    kill "$BOT_PID" 2>/dev/null || true
    wait "$BOT_PID" 2>/dev/null || true
  fi
  .venv/bin/python scripts/dev_set_partner_role.py revert --db "$DB_PATH" || true
}
trap cleanup EXIT

# Step 4: start bot in polling mode against the isolated DB.
mkdir -p artifacts/test-results
LOG="artifacts/test-results/partner_promo_bot.log"
echo "Starting bot, log=$LOG"
DEV_MODE=1 DISABLE_PAGE_JOBS=1 ENABLE_JOB_OUTBOX_WORKER=0 \\
DB_PATH="$DB_PATH" .venv/bin/python main.py > "$LOG" 2>&1 &
BOT_PID=$!
echo "bot PID=$BOT_PID"

# Wait for "/start" handler to be ready.
for _ in $(seq 1 30); do
  if grep -q "Mode: DEV_MODE" "$LOG" 2>/dev/null || grep -q "polling" "$LOG" 2>/dev/null; then
    break
  fi
  sleep 1
done

# Step 5: run behave for the partner promo feature.
TR="artifacts/test-results/partner_promo_e2e.txt"
.venv/bin/python -m behave tests/e2e/features/partner_promo.feature \\
    --no-capture --no-capture-stderr 2>&1 | tee "$TR"
