#!/usr/bin/env bash
set -euo pipefail

APP="${FLY_APP_NAME:-events-bot-new-wngqia}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SEARCH_VALIDATION_PROFILE="${SEARCH_VALIDATION_PROFILE:-none}"
SEARCH_BACKEND_REVISION="${SEARCH_BACKEND_REVISION:-}"
SEARCH_DEPLOYMENT_RUN_ID="${SEARCH_DEPLOYMENT_RUN_ID:-}"
SEARCH_CHANGED_SURFACES=()
if [[ -n "${SEARCH_CHANGED_SURFACES:-}" ]]; then
  IFS=',' read -r -a SEARCH_CHANGED_SURFACES <<<"$SEARCH_CHANGED_SURFACES"
fi
FLY_ARGS=()

while (($#)); do
  case "$1" in
    --search-validation-profile)
      (($# >= 2)) || { echo 'Missing value for --search-validation-profile.' >&2; exit 2; }
      SEARCH_VALIDATION_PROFILE="$2"; shift 2 ;;
    --search-validation-profile=*)
      SEARCH_VALIDATION_PROFILE="${1#*=}"; shift ;;
    --search-backend-revision)
      (($# >= 2)) || { echo 'Missing value for --search-backend-revision.' >&2; exit 2; }
      SEARCH_BACKEND_REVISION="$2"; shift 2 ;;
    --search-backend-revision=*)
      SEARCH_BACKEND_REVISION="${1#*=}"; shift ;;
    --search-deployment-run-id)
      (($# >= 2)) || { echo 'Missing value for --search-deployment-run-id.' >&2; exit 2; }
      SEARCH_DEPLOYMENT_RUN_ID="$2"; shift 2 ;;
    --search-deployment-run-id=*)
      SEARCH_DEPLOYMENT_RUN_ID="${1#*=}"; shift ;;
    --search-changed-surface)
      (($# >= 2)) || { echo 'Missing value for --search-changed-surface.' >&2; exit 2; }
      SEARCH_CHANGED_SURFACES+=("$2"); shift 2 ;;
    --search-changed-surface=*)
      SEARCH_CHANGED_SURFACES+=("${1#*=}"); shift ;;
    *) FLY_ARGS+=("$1"); shift ;;
  esac
done

case "$SEARCH_VALIDATION_PROFILE" in
  none|standard|full) ;;
  *) echo 'Search validation profile must be none, standard, or full.' >&2; exit 2 ;;
esac

if [[ "$SEARCH_VALIDATION_PROFILE" != none ]]; then
  [[ -n "$SEARCH_BACKEND_REVISION" ]] || {
    echo 'Search backend revision is required for standard/full validation.' >&2; exit 2;
  }
  [[ -n "$SEARCH_DEPLOYMENT_RUN_ID" ]] || {
    echo 'Search deployment run id is required for standard/full validation.' >&2; exit 2;
  }
  ((${#SEARCH_CHANGED_SURFACES[@]} > 0)) || {
    echo 'At least one changed surface is required for standard/full validation.' >&2; exit 2;
  }
fi

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

for arg in "${FLY_ARGS[@]}"; do
  case "$arg" in
    --image|--image=*|--build-arg|--build-arg=*)
      echo "Refusing deploy option $arg: image identity is owned by this script." >&2
      exit 2
      ;;
  esac
done

GH_BIN=''
dispatch_file=''
if [[ "$SEARCH_VALIDATION_PROFILE" != none ]]; then
  if command -v gh >/dev/null 2>&1; then
    GH_BIN="$(command -v gh)"
  elif [[ -x "$HOME/.local/bin/gh" ]]; then
    GH_BIN="$HOME/.local/bin/gh"
  else
    echo 'gh is required before a Search validation deployment.' >&2
    exit 2
  fi
  "$GH_BIN" auth status >/dev/null
  dispatch_file="$(mktemp)"
  cleanup_dispatch() { rm -f "$dispatch_file"; }
  trap cleanup_dispatch EXIT
  dispatch_args=(
    --site-runtime-sha "$HEAD_SHA"
    --search-backend-revision "$SEARCH_BACKEND_REVISION"
    --validation-profile "$SEARCH_VALIDATION_PROFILE"
    --deployment-run-id "$SEARCH_DEPLOYMENT_RUN_ID"
  )
  for surface in "${SEARCH_CHANGED_SURFACES[@]}"; do
    dispatch_args+=(--changed-surface "$surface")
  done
  node scripts/search-runtime-deploy-dispatch.mjs "${dispatch_args[@]}" >"$dispatch_file"
fi

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
"$FLYCTL" deploy "$ROOT" \
  --app "$APP" \
  --build-arg "STATIC_SITE_IMAGE_REPO_SHA=$HEAD_SHA" \
  "${FLY_ARGS[@]}"

if [[ "$SEARCH_VALIDATION_PROFILE" == none ]]; then
  echo 'Fly deploy succeeded; Search validation marker disabled (none).'
  exit 0
fi

GITHUB_REPOSITORY="${GITHUB_REPOSITORY:-onedayonemasterpiece/events-bot-new}"
"$GH_BIN" api --method POST "repos/$GITHUB_REPOSITORY/dispatches" --input "$dispatch_file"
echo "Fly deploy succeeded; emitted one Search $SEARCH_VALIDATION_PROFILE validation marker."
