#!/usr/bin/env bash
set -euo pipefail

APP="${FLY_APP_NAME:-events-bot-new-wngqia}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

git fetch origin --prune

if [[ -n "$(git status --porcelain)" ]]; then
  echo "Refusing deploy: worktree is not clean." >&2
  exit 2
fi

HEAD_SHA="$(git rev-parse HEAD)"
MAIN_SHA="$(git rev-parse origin/main)"
if [[ "$HEAD_SHA" != "$MAIN_SHA" ]]; then
  echo "Refusing deploy: HEAD is not exact origin/main." >&2
  exit 2
fi
if [[ ! "$HEAD_SHA" =~ ^[0-9a-f]{40}$ ]]; then
  echo "Refusing deploy: invalid Git revision." >&2
  exit 2
fi

for arg in "$@"; do
  case "$arg" in
    --image|--image=*|--build-arg|--build-arg=*)
      echo "Refusing deploy option $arg: image identity is owned by this script." >&2
      exit 2
      ;;
  esac
done

if [[ -f /home/dev/.config/fly/release.env ]]; then
  # shellcheck disable=SC1091
  set -a
  source /home/dev/.config/fly/release.env
  set +a
fi

if [[ -x "$HOME/.fly/bin/flyctl" ]]; then
  FLYCTL="$HOME/.fly/bin/flyctl"
elif command -v flyctl >/dev/null 2>&1; then
  FLYCTL="$(command -v flyctl)"
else
  echo "flyctl is not installed." >&2
  exit 2
fi

"$FLYCTL" auth whoami >/dev/null
echo "Deploying exact origin/main $HEAD_SHA to $APP"
exec "$FLYCTL" deploy "$ROOT" \
  --app "$APP" \
  --build-arg "STATIC_SITE_IMAGE_REPO_SHA=$HEAD_SHA" \
  "$@"
