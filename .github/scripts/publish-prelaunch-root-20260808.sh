#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

TARGET_BRANCH='agent/prelaunch-static-background-20260807'
TARGET_SHA='9d8fc9203a69f385407a57e23310bb47f2db4e2d'
VISUAL_RUN_ID='31224605480'
VISUAL_ARTIFACT_DIGEST='sha256:5f9bc73f66ff568b43a5e08a9e98cae6f8997fecbe73f7cce6d634c65eb0768e'
export TARGET_SHA
ROOT_ORIGIN='https://kenigevents.ru'
EVIDENCE_ROOT='artifacts/prelaunch-root-release'
BUILD_ROOT='site/dist'
mkdir -p "$EVIDENCE_ROOT"/{source-evidence,local-browser,live-browser,backup}

required=(
  KENIGEVENTS_SITE_YC_BUCKET
  KENIGEVENTS_SITE_YC_ACCESS_KEY_ID
  KENIGEVENTS_SITE_YC_SECRET_ACCESS_KEY
  PUBLIC_PERSONALIZATION_SUPABASE_URL
  PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY
)
missing=()
for name in "${required[@]}"; do
  [[ -n "${!name:-}" ]] || missing+=("$name")
done
if (( ${#missing[@]} )); then
  printf 'Missing required production configuration: %s\n' "${missing[*]}" >&2
  exit 1
fi

ENDPOINT="${KENIGEVENTS_SITE_YC_ENDPOINT:-https://storage.yandexcloud.net}"
REGION="${KENIGEVENTS_SITE_YC_REGION:-ru-central1}"
BUCKET="$KENIGEVENTS_SITE_YC_BUCKET"
PUBLIC_BASE="${KENIGEVENTS_SITE_PUBLIC_BASE_URL:-$ROOT_ORIGIN}"
PUBLIC_BASE="${PUBLIC_BASE%/}"
[[ "$PUBLIC_BASE" == "$ROOT_ORIGIN" ]] || {
  echo "Unexpected production public base: $PUBLIC_BASE" >&2
  exit 1
}

export AWS_ACCESS_KEY_ID="$KENIGEVENTS_SITE_YC_ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$KENIGEVENTS_SITE_YC_SECRET_ACCESS_KEY"
export AWS_DEFAULT_REGION="$REGION"

aws_cmd() {
  aws --endpoint-url "$ENDPOINT" "$@"
}

sha256_file() {
  sha256sum "$1" | cut -d' ' -f1
}

json_field() {
  local row="$1"
  local field="$2"
  printf '%s' "$row" | base64 --decode | jq -r ".$field"
}

# Bind the release to the already accepted exact-SHA visual evidence.
run_json="$(gh api "repos/${GITHUB_REPOSITORY}/actions/runs/${VISUAL_RUN_ID}")"
test "$(jq -r .status <<<"$run_json")" = completed
test "$(jq -r .conclusion <<<"$run_json")" = success
test "$(jq -r .name <<<"$run_json")" = 'Prelaunch visual review'
test "$(jq -r .event <<<"$run_json")" = push
test "$(jq -r .head_branch <<<"$run_json")" = "$TARGET_BRANCH"
test "$(jq -r .head_sha <<<"$run_json")" = "$TARGET_SHA"
artifact_name="prelaunch-evidence-${VISUAL_RUN_ID}"
artifacts_json="$(gh api "repos/${GITHUB_REPOSITORY}/actions/runs/${VISUAL_RUN_ID}/artifacts?per_page=100")"
artifact="$(jq -c --arg name "$artifact_name" '.artifacts[] | select(.name == $name)' <<<"$artifacts_json")"
test -n "$artifact"
test "$(jq -r .expired <<<"$artifact")" = false
test "$(jq -r .digest <<<"$artifact")" = "$VISUAL_ARTIFACT_DIGEST"
gh run download "$VISUAL_RUN_ID" --repo "$GITHUB_REPOSITORY" --name "$artifact_name" --dir "$EVIDENCE_ROOT/source-evidence"

gates="$(find "$EVIDENCE_ROOT/source-evidence" -name github-actions-gates.json -print -quit)"
scene="$(find "$EVIDENCE_ROOT/source-evidence" -name prelaunch-scene-summary.json -print -quit)"
viewport="$(find "$EVIDENCE_ROOT/source-evidence" -name prelaunch-viewport-fit-summary.json -print -quit)"
form="$(find "$EVIDENCE_ROOT/source-evidence" -name prelaunch-form-security-summary.json -print -quit)"
test -n "$gates" && test -n "$scene" && test -n "$viewport" && test -n "$form"
jq -e --arg sha "$TARGET_SHA" '
  .repository_sha == $sha
  and .scene_exit == 0
  and .viewport_fit_exit == 0
  and .form_security_exit == 0
  and .scene_architecture == "generated-responsive-background-live-form"
' "$gates" >/dev/null
jq -e '.ok == true and (.failures | length) == 0' "$scene" >/dev/null
jq -e '.ok == true and (.failures | length) == 0' "$viewport" >/dev/null
jq -e '.ok == true and (.failures | length) == 0' "$form" >/dev/null

git fetch --no-tags origin "+refs/heads/${TARGET_BRANCH}:refs/remotes/origin/${TARGET_BRANCH}"
test "$(git rev-parse "refs/remotes/origin/${TARGET_BRANCH}")" = "$TARGET_SHA"
git checkout --detach "$TARGET_SHA"
test "$(git rev-parse HEAD)" = "$TARGET_SHA"

npm ci --prefix site --no-audit --no-fund
(cd site && npx playwright install --with-deps chromium)
aws --version

npm --prefix site run test:static-release

# Build only the approved prelaunch root. No catalogue route, media, stable ICS,
# robots.txt or sitemap key is touched by this controlled root promotion.
export PUBLIC_PRELAUNCH_MODE='on'
export PUBLIC_SITE_MODE='production'
export PUBLIC_SITE_ORIGIN="$ROOT_ORIGIN"
export SITE_BASE_PATH='/'
export PUBLIC_ASTRO_ASSET_BASE_URL=''
export PUBLIC_ICS_BASE_URL=''
export PUBLIC_PREVIEW_BUILD_ID=''
export PUBLIC_ROOT_PREVIEW_HREF=''

full_pages="site/.prelaunch-root-full-pages-${GITHUB_RUN_ID}"
rm -rf "$full_pages"
mv site/src/pages "$full_pages"
mkdir -p site/src/pages
cp "$full_pages/index.astro" site/src/pages/index.astro
restore_pages() {
  if [[ -d "$full_pages" ]]; then
    rm -rf site/src/pages
    mv "$full_pages" site/src/pages
  fi
}
trap restore_pages EXIT
npm --prefix site run build
restore_pages
trap - EXIT

index="$BUILD_ROOT/index.html"
test -f "$index"
grep -q 'data-prelaunch-page' "$index"
grep -q 'data-static-background="approved-desktop-mobile-v2"' "$index"
grep -q '<title>Полюбить Калининград Анонсы — запуск 1 сентября</title>' "$index"
grep -q '<h1 id="prelaunch-title"><span>Запуск</span> <time datetime="2026-09-01">1 сентября</time></h1>' "$index"
grep -q '<link rel="canonical" href="https://kenigevents.ru/">' "$index"
grep -q 'content="index,follow,max-snippet:-1,max-image-preview:large,max-video-preview:-1"' "$index"
if grep -q 'noindex' "$index"; then
  echo 'Production root unexpectedly contains noindex' >&2
  exit 1
fi
if grep -q 'data-prelaunch-tile' "$index"; then
  echo 'Rejected dynamic tile implementation leaked into production root' >&2
  exit 1
fi

# Exact dependency closure for this one-page build: root HTML, all content-hashed
# Astro bundles, two approved backgrounds and the two published brand images.
node --input-type=module <<'NODE'
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, readdirSync, statSync, writeFileSync } from 'node:fs';
import { extname, join, relative } from 'node:path';

const root = 'site/dist';
const output = 'artifacts/prelaunch-root-release/deployment-manifest.json';
const required = new Set([
  'index.html',
  'assets/prelaunch/prelaunch-scene-desktop.webp',
  'assets/prelaunch/prelaunch-scene-mobile.webp',
  'assets/pwa/announcements-brand-v2-192.png',
  'assets/pwa/announcements-brand-v2-512.png',
]);

function walk(directory) {
  for (const name of readdirSync(directory, { withFileTypes: true })) {
    const full = join(directory, name.name);
    if (name.isDirectory()) walk(full);
    else required.add(relative(root, full).replaceAll('\\', '/'));
  }
}
const astro = join(root, '_astro');
if (!existsSync(astro)) throw new Error('Missing _astro output');
walk(astro);

const contentTypes = new Map([
  ['.html', 'text/html; charset=utf-8'],
  ['.css', 'text/css; charset=utf-8'],
  ['.js', 'text/javascript; charset=utf-8'],
  ['.mjs', 'text/javascript; charset=utf-8'],
  ['.json', 'application/json; charset=utf-8'],
  ['.png', 'image/png'],
  ['.webp', 'image/webp'],
  ['.svg', 'image/svg+xml'],
  ['.woff2', 'font/woff2'],
  ['.woff', 'font/woff'],
]);

const files = [...required].sort().map((key) => {
  if (key.startsWith('/') || key.includes('..') || key.includes('\\')) throw new Error(`Unsafe key ${key}`);
  const path = join(root, key);
  if (!existsSync(path) || !statSync(path).isFile()) throw new Error(`Required release file missing: ${key}`);
  const bytes = readFileSync(path);
  const hashed = key.startsWith('_astro/');
  return {
    key,
    local_path: path,
    size: bytes.length,
    sha256: createHash('sha256').update(bytes).digest('hex'),
    content_type: contentTypes.get(extname(key).toLowerCase()) || 'application/octet-stream',
    cache_control: key === 'index.html'
      ? 'public, max-age=0, must-revalidate'
      : hashed
        ? 'public, max-age=31536000, immutable'
        : 'public, max-age=3600, must-revalidate',
    promotion_class: key === 'index.html' ? 'root_html' : hashed ? 'immutable_asset' : 'supporting',
  };
});
const manifest = {
  schema_version: 'prelaunch_root_release_manifest_v1',
  release_id: `prelaunch-root-20260808-${process.env.GITHUB_RUN_ID}-${process.env.GITHUB_RUN_ATTEMPT}`,
  repo_sha: process.env.TARGET_SHA || '9d8fc9203a69f385407a57e23310bb47f2db4e2d',
  source_branch: 'agent/prelaunch-static-background-20260807',
  visual_run_id: 31224605480,
  public_url: 'https://kenigevents.ru/',
  generated_at: new Date().toISOString(),
  files,
};
mkdirSync('artifacts/prelaunch-root-release', { recursive: true });
writeFileSync(output, `${JSON.stringify(manifest, null, 2)}\n`);
NODE

manifest="$EVIDENCE_ROOT/deployment-manifest.json"
release_id="$(jq -r .release_id "$manifest")"
stage_prefix="_static/prelaunch-root-releases/${release_id}/root"
stage_manifest_key="_static/prelaunch-root-releases/${release_id}/release-manifest.json"
backup_prefix="_static/prelaunch-root-backups/${release_id}/root"
backup_inventory_key="_static/prelaunch-root-backups/${release_id}/backup-inventory.json"
pointer_key="_static/prelaunch-root/current.json"

# Local browser proof of the exact production-root projection.
python3 -m http.server 4173 --directory "$BUILD_ROOT" >"$EVIDENCE_ROOT/local-http.log" 2>&1 &
local_server_pid=$!
cleanup_local_server() {
  kill "$local_server_pid" >/dev/null 2>&1 || true
  wait "$local_server_pid" >/dev/null 2>&1 || true
}
trap cleanup_local_server EXIT
for _ in {1..30}; do
  curl --fail --silent --show-error http://127.0.0.1:4173/ >/dev/null && break
  sleep 1
done
npm --prefix site run check:prelaunch-scene -- --url http://127.0.0.1:4173/ --artifact-dir "../$EVIDENCE_ROOT/local-browser"
node site/scripts/check-prelaunch-viewport-fit.mjs --url http://127.0.0.1:4173/ --artifact-dir "$EVIDENCE_ROOT/local-browser"
cleanup_local_server
trap - EXIT

# Non-writing live transport probe: the honeypot contract must accept without
# storing a subscriber row. Relay remains optional, matching the approved page.
curl --fail-with-body --silent --show-error \
  --retry 2 --retry-delay 1 --retry-all-errors --max-time 30 \
  --request POST \
  --header "apikey: ${PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY}" \
  --header "Authorization: Bearer ${PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY}" \
  --header 'Accept: application/json' \
  --header 'Content-Type: application/json' \
  --data '{"p_email":"production-root-probe.invalid","p_source":"prelaunch_root_release_probe","p_consent_version":"prelaunch-updates-2026-v1","p_website":"github-actions-honeypot"}' \
  "${PUBLIC_PERSONALIZATION_SUPABASE_URL%/}/rest/v1/rpc/register_prelaunch_notification_v1" \
  > "$EVIDENCE_ROOT/transport-direct.json"
jq -e '
  .accepted == true
  and .status == "registered"
  and .launch_date == "2026-09-01"
  and .consent_version == "prelaunch-updates-2026-v1"
' "$EVIDENCE_ROOT/transport-direct.json" >/dev/null
if [[ -n "${PUBLIC_PERSONALIZATION_SUPABASE_RELAY_URL:-}" ]]; then
  curl --fail-with-body --silent --show-error \
    --retry 2 --retry-delay 1 --retry-all-errors --max-time 30 \
    --request POST \
    --header "apikey: ${PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY}" \
    --header "Authorization: Bearer ${PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY}" \
    --header 'Accept: application/json' \
    --header 'Content-Type: application/json' \
    --data '{"p_email":"production-root-relay-probe.invalid","p_source":"prelaunch_root_release_probe","p_consent_version":"prelaunch-updates-2026-v1","p_website":"github-actions-honeypot"}' \
    "${PUBLIC_PERSONALIZATION_SUPABASE_RELAY_URL%/}/rest/v1/rpc/register_prelaunch_notification_v1" \
    > "$EVIDENCE_ROOT/transport-relay.json"
  jq -e '.accepted == true and .status == "registered"' "$EVIDENCE_ROOT/transport-relay.json" >/dev/null
else
  cat > "$EVIDENCE_ROOT/transport-relay.json" <<'JSON'
{"configured":false,"ok":true,"status":"not_configured","stored_row":false}
JSON
fi

aws_cmd s3api head-bucket --bucket "$BUCKET" >/dev/null
if aws_cmd s3api head-object --bucket "$BUCKET" --key "$stage_manifest_key" >/dev/null 2>&1; then
  echo "Immutable root release already exists: $stage_manifest_key" >&2
  exit 1
fi

# Stage immutable bytes first and verify each byte through S3 before touching root.
while IFS= read -r encoded; do
  key="$(json_field "$encoded" key)"
  local_path="$(json_field "$encoded" local_path)"
  content_type="$(json_field "$encoded" content_type)"
  cache_control="$(json_field "$encoded" cache_control)"
  expected_sha="$(json_field "$encoded" sha256)"
  stage_key="${stage_prefix}/${key}"
  aws_cmd s3 cp "$local_path" "s3://${BUCKET}/${stage_key}" \
    --content-type "$content_type" \
    --cache-control "$cache_control" \
    --no-progress
  actual_sha="$(aws_cmd s3 cp "s3://${BUCKET}/${stage_key}" - --no-progress | sha256sum | cut -d' ' -f1)"
  test "$actual_sha" = "$expected_sha"
done < <(jq -r '.files[] | @base64' "$manifest")
aws_cmd s3 cp "$manifest" "s3://${BUCKET}/${stage_manifest_key}" \
  --content-type 'application/json; charset=utf-8' \
  --cache-control 'public, max-age=31536000, immutable' \
  --no-progress

# Backup every target object with metadata and capture absence explicitly.
backup_lines="$EVIDENCE_ROOT/backup/objects.ndjson"
: > "$backup_lines"
while IFS= read -r encoded; do
  key="$(json_field "$encoded" key)"
  if head_json="$(aws_cmd s3api head-object --bucket "$BUCKET" --key "$key" 2>/dev/null)"; then
    printf '%s\n' "$head_json" > "$EVIDENCE_ROOT/backup/$(printf '%s' "$key" | sha256sum | cut -d' ' -f1).head.json"
    aws_cmd s3 cp "s3://${BUCKET}/${key}" "s3://${BUCKET}/${backup_prefix}/${key}" \
      --metadata-directive COPY \
      --no-progress
    jq -nc --arg key "$key" --arg backup_key "${backup_prefix}/${key}" \
      --arg etag "$(jq -r .ETag <<<"$head_json")" \
      '{key:$key,existed:true,backup_key:$backup_key,etag:$etag}' >> "$backup_lines"
  else
    jq -nc --arg key "$key" '{key:$key,existed:false,backup_key:null,etag:null}' >> "$backup_lines"
  fi
done < <(jq -r '.files[] | @base64' "$manifest")
jq -s --arg release_id "$release_id" --arg captured_at "$(date -u +%FT%TZ)" \
  '{schema_version:"prelaunch_root_backup_inventory_v1",release_id:$release_id,captured_at:$captured_at,objects:.}' \
  "$backup_lines" > "$EVIDENCE_ROOT/backup-inventory.json"
aws_cmd s3 cp "$EVIDENCE_ROOT/backup-inventory.json" "s3://${BUCKET}/${backup_inventory_key}" \
  --content-type 'application/json; charset=utf-8' \
  --cache-control 'no-store' \
  --no-progress

# Fail closed if root changed after backup and before promotion.
before_index_etag="$(jq -r '.objects[] | select(.key=="index.html") | .etag // ""' "$EVIDENCE_ROOT/backup-inventory.json")"
if [[ -n "$before_index_etag" ]]; then
  current_index_etag="$(aws_cmd s3api head-object --bucket "$BUCKET" --key index.html | jq -r .ETag)"
  test "$current_index_etag" = "$before_index_etag"
fi

promoted=0
rollback_required=0
rollback_root() {
  local status=$?
  if [[ "$rollback_required" == 1 ]]; then
    set +e
    echo "Release verification failed; restoring backed-up root objects" >&2
    while IFS= read -r encoded; do
      key="$(json_field "$encoded" key)"
      existed="$(json_field "$encoded" existed)"
      backup_key="$(json_field "$encoded" backup_key)"
      if [[ "$existed" == true ]]; then
        aws_cmd s3 cp "s3://${BUCKET}/${backup_key}" "s3://${BUCKET}/${key}" \
          --metadata-directive COPY \
          --no-progress
      else
        aws_cmd s3 rm "s3://${BUCKET}/${key}" --no-progress
      fi
    done < <(jq -r '.objects[] | @base64' "$EVIDENCE_ROOT/backup-inventory.json")
    jq -n \
      --arg schema_version prelaunch_root_release_receipt_v1 \
      --arg result rolled_back \
      --arg release_id "$release_id" \
      --arg repo_sha "$TARGET_SHA" \
      --arg public_url "${PUBLIC_BASE}/" \
      --arg failed_at "$(date -u +%FT%TZ)" \
      --argjson exit_code "$status" \
      '{schema_version:$schema_version,result:$result,release_id:$release_id,repo_sha:$repo_sha,public_url:$public_url,failed_at:$failed_at,exit_code:$exit_code}' \
      > "$EVIDENCE_ROOT/release-receipt.json"
  fi
  exit "$status"
}
trap rollback_root ERR INT TERM

# Promote supporting and immutable assets first; root index is the final commit.
rollback_required=1
for class in immutable_asset supporting root_html; do
  while IFS= read -r encoded; do
    key="$(json_field "$encoded" key)"
    stage_key="${stage_prefix}/${key}"
    aws_cmd s3 cp "s3://${BUCKET}/${stage_key}" "s3://${BUCKET}/${key}" \
      --metadata-directive COPY \
      --no-progress
    [[ "$key" == index.html ]] && promoted=1
  done < <(jq -r --arg class "$class" '.files[] | select(.promotion_class == $class) | @base64' "$manifest")
done

# S3 byte verification of all promoted objects.
while IFS= read -r encoded; do
  key="$(json_field "$encoded" key)"
  expected_sha="$(json_field "$encoded" sha256)"
  actual_sha="$(aws_cmd s3 cp "s3://${BUCKET}/${key}" - --no-progress | sha256sum | cut -d' ' -f1)"
  test "$actual_sha" = "$expected_sha"
done < <(jq -r '.files[] | @base64' "$manifest")

# Wait for the plain canonical URL, not a cache-busting review URL.
expected_index_sha="$(jq -r '.files[] | select(.key=="index.html") | .sha256' "$manifest")"
http_ready=0
for attempt in $(seq 1 180); do
  if curl --fail --location --silent --show-error \
      --max-time 30 \
      --header 'Cache-Control: no-cache' \
      --header 'Pragma: no-cache' \
      "${PUBLIC_BASE}/" \
      -D "$EVIDENCE_ROOT/live-root.headers" \
      -o "$EVIDENCE_ROOT/live-root.html"; then
    live_sha="$(sha256_file "$EVIDENCE_ROOT/live-root.html")"
    if [[ "$live_sha" == "$expected_index_sha" ]] \
      && grep -q 'data-prelaunch-page' "$EVIDENCE_ROOT/live-root.html" \
      && grep -q '<title>Полюбить Калининград Анонсы — запуск 1 сентября</title>' "$EVIDENCE_ROOT/live-root.html" \
      && grep -q '<link rel="canonical" href="https://kenigevents.ru/">' "$EVIDENCE_ROOT/live-root.html"; then
      http_ready=1
      break
    fi
  fi
  sleep 10
done
test "$http_ready" = 1

# Verify every public dependency on the same stable production origin.
while IFS= read -r encoded; do
  key="$(json_field "$encoded" key)"
  [[ "$key" == index.html ]] && continue
  expected_sha="$(json_field "$encoded" sha256)"
  output="$EVIDENCE_ROOT/live-$(printf '%s' "$key" | sha256sum | cut -d' ' -f1)"
  curl --fail --location --silent --show-error \
    --max-time 30 \
    --header 'Cache-Control: no-cache' \
    --header 'Pragma: no-cache' \
    "${PUBLIC_BASE}/${key}" \
    -o "$output"
  test "$(sha256_file "$output")" = "$expected_sha"
done < <(jq -r '.files[] | @base64' "$manifest")

npm --prefix site run check:prelaunch-scene -- --url "${PUBLIC_BASE}/" --artifact-dir "../$EVIDENCE_ROOT/live-browser"
node site/scripts/check-prelaunch-viewport-fit.mjs --url "${PUBLIC_BASE}/" --artifact-dir "$EVIDENCE_ROOT/live-browser"

jq -e '.ok == true and (.failures | length) == 0' "$EVIDENCE_ROOT/live-browser/prelaunch-scene-summary.json" >/dev/null
jq -e '.ok == true and (.failures | length) == 0' "$EVIDENCE_ROOT/live-browser/prelaunch-viewport-fit-summary.json" >/dev/null

published_at="$(date -u +%FT%TZ)"
jq -n \
  --arg schema_version prelaunch_root_release_receipt_v1 \
  --arg result published \
  --arg release_id "$release_id" \
  --arg repo_sha "$TARGET_SHA" \
  --arg source_branch "$TARGET_BRANCH" \
  --arg public_url "${PUBLIC_BASE}/" \
  --arg stage_prefix "$stage_prefix" \
  --arg backup_prefix "$backup_prefix" \
  --arg manifest_sha256 "$(sha256_file "$manifest")" \
  --arg index_sha256 "$expected_index_sha" \
  --arg published_at "$published_at" \
  --argjson visual_run_id "$VISUAL_RUN_ID" \
  --argjson file_count "$(jq '.files | length' "$manifest")" \
  '{
    schema_version:$schema_version,
    result:$result,
    release_id:$release_id,
    repo_sha:$repo_sha,
    source_branch:$source_branch,
    public_url:$public_url,
    stage_prefix:$stage_prefix,
    backup_prefix:$backup_prefix,
    manifest_sha256:$manifest_sha256,
    index_sha256:$index_sha256,
    published_at:$published_at,
    visual_run_id:$visual_run_id,
    file_count:$file_count,
    source_tests:"ok",
    local_scene:"ok",
    local_viewport_fit:"ok",
    transport_probe:"ok",
    s3_stage_verification:"ok",
    s3_root_verification:"ok",
    public_http_verification:"ok",
    live_scene:"ok",
    live_viewport_fit:"ok"
  }' > "$EVIDENCE_ROOT/release-receipt.json"

# Preserve previous pointer, then make the verified root release current.
if previous_pointer="$(aws_cmd s3 cp "s3://${BUCKET}/${pointer_key}" - --no-progress 2>/dev/null)"; then
  printf '%s\n' "$previous_pointer" > "$EVIDENCE_ROOT/previous-pointer.json"
  aws_cmd s3 cp "$EVIDENCE_ROOT/previous-pointer.json" \
    "s3://${BUCKET}/_static/prelaunch-root/previous.json" \
    --content-type 'application/json; charset=utf-8' \
    --cache-control 'no-store' \
    --no-progress
fi
aws_cmd s3 cp "$EVIDENCE_ROOT/release-receipt.json" "s3://${BUCKET}/${pointer_key}" \
  --content-type 'application/json; charset=utf-8' \
  --cache-control 'no-store' \
  --no-progress

rollback_required=0
trap - ERR INT TERM
echo "PUBLISHED_ROOT_URL=${PUBLIC_BASE}/"
echo "PUBLISHED_RELEASE_ID=${release_id}"
echo "PUBLISHED_REPO_SHA=${TARGET_SHA}"
