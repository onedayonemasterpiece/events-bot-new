#!/usr/bin/env bash
set -euo pipefail
set +x

: "${FLY_APP_NAME:?FLY_APP_NAME is required}"
: "${FLY_API_TOKEN:?FLY_API_TOKEN is required}"
: "${GITHUB_ENV:?GITHUB_ENV is required}"

tmp_root="${RUNNER_TEMP:-${TMPDIR:-/tmp}}"
raw_file="${tmp_root}/search-current-candidate-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}.json"
parsed_file="${raw_file}.parsed"
cleanup() { rm -f "$raw_file" "$parsed_file"; }
trap cleanup EXIT
umask 077

NO_COLOR=1 flyctl ssh console \
  --app "$FLY_APP_NAME" \
  --pty=false \
  --command "python3 scripts/request_static_site_build.py --db /data/db.sqlite --show-current-review" \
  >"$raw_file"

python3 - "$raw_file" "$parsed_file" <<'PY'
import json
import re
import sys
from pathlib import Path
from urllib.parse import urlsplit

source = Path(sys.argv[1]).read_text(encoding="utf-8", errors="strict")
objects = []
for line in source.splitlines():
    line = line.strip()
    if not line.startswith("{"):
        continue
    try:
        objects.append(json.loads(line))
    except json.JSONDecodeError:
        pass
if not objects:
    raise SystemExit("current_candidate_resolver_invalid")
row = objects[-1]
if row.get("ok") is not True or row.get("status") != "current_review_ready":
    raise SystemExit("current_candidate_unavailable")
public_url = str(row.get("public_url") or "")
repo_sha = str(row.get("repo_sha") or "").lower()
parsed = urlsplit(public_url)
if (
    parsed.scheme != "https"
    or parsed.netloc != "kenigevents.ru"
    or parsed.query
    or parsed.fragment
    or not re.fullmatch(r"/_review/[A-Za-z0-9_-]{43}/", parsed.path)
):
    raise SystemExit("current_candidate_url_invalid")
if not re.fullmatch(r"[0-9a-f]{40}", repo_sha):
    raise SystemExit("current_candidate_sha_invalid")
target = public_url + "poisk/"
Path(sys.argv[2]).write_text(target + "\n" + repo_sha + "\n", encoding="utf-8")
PY

{
  IFS= read -r target_url
  IFS= read -r repo_sha
} <"$parsed_file"
test -n "$target_url" && test -n "$repo_sha"

# The review prefix is a bearer secret. Mask it before it is copied into the
# job environment, and never expose the resolver output as a workflow output or
# artifact.
printf '::add-mask::%s\n' "$target_url"
printf 'E2E_SEARCH_TARGET_URL=%s\n' "$target_url" >>"$GITHUB_ENV"
printf 'E2E_EXPECTED_REPO_SHA=%s\n' "$repo_sha" >>"$GITHUB_ENV"
printf 'Search exact-target resolver PASS (repo SHA %s).\n' "$repo_sha"
