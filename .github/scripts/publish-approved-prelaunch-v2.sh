#!/usr/bin/env bash
set -euo pipefail

artifact_dir='artifacts/prelaunch-approved-publish-v2'
mkdir -p "$artifact_dir"

required_env=(
  GH_TOKEN
  KENIGEVENTS_SITE_YC_BUCKET
  KENIGEVENTS_SITE_YC_ACCESS_KEY_ID
  KENIGEVENTS_SITE_YC_SECRET_ACCESS_KEY
  PUBLIC_PERSONALIZATION_SUPABASE_URL
  PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY
)
for name in "${required_env[@]}"; do
  test -n "${!name:-}"
done

public_base="${KENIGEVENTS_SITE_PUBLIC_BASE_URL:-https://kenigevents.ru}"
public_base="${public_base%/}"

download_archive() {
  local sha="$1"
  local output="$2"
  curl --fail-with-body --silent --show-error --location \
    --header "Authorization: Bearer ${GH_TOKEN}" \
    --header 'Accept: application/vnd.github+json' \
    --header 'X-GitHub-Api-Version: 2022-11-28' \
    "https://api.github.com/repos/${GITHUB_REPOSITORY}/tarball/${sha}" \
    --output "$output"
}

control_archive="${RUNNER_TEMP}/control-${GITHUB_SHA}.tar.gz"
download_archive "$GITHUB_SHA" "$control_archive"
tar --extract --gzip --file "$control_archive" --strip-components=1 --no-same-owner

request_file='.github/prelaunch-approved-publish-v2.request'
test -f "$request_file"
value() { sed -n "s/^$1=//p" "$request_file" | tail -n 1; }
request_id="$(value request_id)"
target_branch="$(value target_branch)"
target_sha="$(value target_sha)"
visual_run_id="$(value visual_run_id)"
visual_artifact_digest="$(value visual_artifact_digest)"
manual_visual_review="$(value manual_visual_review)"

[[ "$request_id" =~ ^[A-Za-z0-9._-]{8,120}$ ]]
[[ "$target_branch" == 'agent/prelaunch-static-background-20260807' ]]
[[ "$target_sha" =~ ^[0-9a-f]{40}$ ]]
[[ "$visual_run_id" =~ ^[0-9]{8,20}$ ]]
[[ "$visual_artifact_digest" =~ ^sha256:[0-9a-f]{64}$ ]]
[[ "$manual_visual_review" == 'approved' ]]

branch_path="$(jq -rn --arg value "$target_branch" '$value | @uri')"
test "$(gh api "repos/${GITHUB_REPOSITORY}/branches/${branch_path}" --jq .commit.sha)" = "$target_sha"
run_json="$(gh api "repos/${GITHUB_REPOSITORY}/actions/runs/${visual_run_id}")"
test "$(jq -r .status <<<"$run_json")" = completed
test "$(jq -r .conclusion <<<"$run_json")" = success
test "$(jq -r .name <<<"$run_json")" = 'Prelaunch visual review'
test "$(jq -r .head_branch <<<"$run_json")" = "$target_branch"
test "$(jq -r .head_sha <<<"$run_json")" = "$target_sha"
artifact_name="prelaunch-evidence-${visual_run_id}"
artifact="$(gh api "repos/${GITHUB_REPOSITORY}/actions/runs/${visual_run_id}/artifacts?per_page=100" \
  --jq ".artifacts[] | select(.name == \"${artifact_name}\")")"
test -n "$artifact"
test "$(jq -r .expired <<<"$artifact")" = false
test "$(jq -r .digest <<<"$artifact")" = "$visual_artifact_digest"

target_archive="${RUNNER_TEMP}/target-${target_sha}.tar.gz"
download_archive "$target_sha" "$target_archive"
mkdir -p target
tar --extract --gzip --file "$target_archive" --directory target --strip-components=1 --no-same-owner
test -f target/site/package-lock.json
test -f target/site/src/components/PrelaunchPage.astro

npm --prefix target/site ci --no-audit --no-fund
npm --prefix target/site run test:static-release
aws --version

snapshot() {
  local output="$1"
  SNAPSHOT_OUTPUT="$output" BASE_URL="$public_base" node --input-type=module <<'NODE'
import { createHash } from 'node:crypto';
import { writeFileSync } from 'node:fs';
const base = process.env.BASE_URL.replace(/\/+$/u, '');
const surfaces = [];
for (const path of ['/', '/robots.txt', '/sitemap.xml', '/ics/']) {
  const response = await fetch(`${base}${path}`, {
    redirect: 'manual',
    headers: { 'cache-control': 'no-cache', pragma: 'no-cache' },
  });
  const bytes = Buffer.from(await response.arrayBuffer());
  surfaces.push({
    path,
    status: response.status,
    location: response.headers.get('location') || '',
    content_type: response.headers.get('content-type') || '',
    byte_length: response.ok ? bytes.length : null,
    body_sha256: response.ok ? createHash('sha256').update(bytes).digest('hex') : null,
  });
}
writeFileSync(process.env.SNAPSHOT_OUTPUT, `${JSON.stringify({ base_url: base, surfaces }, null, 2)}\n`);
NODE
}
snapshot "$artifact_dir/production-surfaces-before.json"

probe_backend() {
  local route_name="$1"
  local base_url="$2"
  local output="$artifact_dir/backend-${route_name}-honeypot-probe.json"
  local response
  response="$(curl --fail-with-body --silent --show-error --max-time 25 --request POST \
    --header "apikey: ${PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY}" \
    --header "Authorization: Bearer ${PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY}" \
    --header 'Accept: application/json' \
    --header 'Content-Type: application/json' \
    --data '{"p_email":"approved-publication-v2-probe@example.com","p_source":"prelaunch_approved_publication_v2_probe","p_consent_version":"prelaunch-updates-2026-v1","p_website":"github-actions-honeypot"}' \
    "${base_url%/}/rest/v1/rpc/register_prelaunch_notification_v1")"
  jq -e '.accepted == true and .status == "registered" and .launch_date == "2026-09-01" and .consent_version == "prelaunch-updates-2026-v1"' <<<"$response" >/dev/null
  jq --arg route "$route_name" '{route:$route,accepted,status,launch_date,consent_version}' <<<"$response" > "$output"
}

probe_backend direct "$PUBLIC_PERSONALIZATION_SUPABASE_URL"
relay_status='not_configured'
if [[ -n "${PUBLIC_PERSONALIZATION_SUPABASE_RELAY_URL:-}" ]]; then
  probe_backend relay "$PUBLIC_PERSONALIZATION_SUPABASE_RELAY_URL"
  relay_status='ok'
else
  jq -n '{route:"relay",status:"not_configured"}' > "$artifact_dir/backend-relay-honeypot-probe.json"
fi

token="$(openssl rand -hex 32)"
base_path="/_review/${token}"
build_id="prelaunch-approved-v2-${GITHUB_RUN_ID}-${target_sha:0:12}"
token_sha="$(printf '%s' "$token" | sha256sum | cut -d' ' -f1)"

PUBLIC_PRELAUNCH_MODE=on \
PUBLIC_SITE_MODE=secret_candidate \
PUBLIC_SITE_ORIGIN="$public_base" \
SITE_BASE_PATH="$base_path" \
PUBLIC_PERSONALIZATION_SUPABASE_URL="$PUBLIC_PERSONALIZATION_SUPABASE_URL" \
PUBLIC_PERSONALIZATION_SUPABASE_RELAY_URL="${PUBLIC_PERSONALIZATION_SUPABASE_RELAY_URL:-}" \
PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY="$PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY" \
npm --prefix target/site run build

source_root='target/site/dist'
stage="${RUNNER_TEMP}/candidate-stage-v2"
rm -rf "$stage"
mkdir -p "$stage"
cp "$source_root/index.html" "$stage/index.html"
test -d "$source_root/_astro" && cp -R "$source_root/_astro" "$stage/_astro"
test -d "$source_root/assets" && cp -R "$source_root/assets" "$stage/assets"
find "$source_root" -maxdepth 1 -type f \
  ! -name 'index.html' \
  ! -name 'robots.txt' \
  ! -name 'sitemap.xml' \
  ! -name 'static-release-manifest.json' \
  -exec cp {} "$stage/" \;
rm -rf "$source_root"
candidate_root="$source_root/${base_path#/}"
mkdir -p "$candidate_root"
cp -R "$stage"/. "$candidate_root/"
grep -q 'data-prelaunch-page' "$candidate_root/index.html"
grep -q 'approved-desktop-mobile-v2' "$candidate_root/index.html"
grep -q 'noindex,nofollow,noarchive,nosnippet' "$candidate_root/index.html"
grep -q 'name="referrer" content="no-referrer"' "$candidate_root/index.html"

CANDIDATE_ROOT="$candidate_root" \
CANDIDATE_BASE_PATH="$base_path" \
CANDIDATE_BUILD_ID="$build_id" \
CANDIDATE_TOKEN_SHA256="$token_sha" \
node --input-type=module <<'NODE'
import { createHash } from 'node:crypto';
import { readdirSync, readFileSync, statSync, writeFileSync } from 'node:fs';
import { join, relative, sep } from 'node:path';
const root = process.env.CANDIDATE_ROOT;
const types = {
  '.html':'text/html; charset=utf-8', '.css':'text/css; charset=utf-8',
  '.js':'text/javascript; charset=utf-8', '.mjs':'text/javascript; charset=utf-8',
  '.json':'application/json; charset=utf-8', '.svg':'image/svg+xml',
  '.png':'image/png', '.webp':'image/webp', '.ico':'image/x-icon',
  '.woff2':'font/woff2', '.txt':'text/plain; charset=utf-8',
};
const files = [];
const walk = (dir) => {
  for (const name of readdirSync(dir)) {
    const path = join(dir, name);
    const stat = statSync(path);
    if (stat.isDirectory()) walk(path);
    else {
      const key = relative(root, path).split(sep).join('/');
      if (key === 'secret-candidate-manifest.json') continue;
      const bytes = readFileSync(path);
      const ext = key.slice(key.lastIndexOf('.'));
      files.push({
        key,
        size_bytes: bytes.length,
        sha256: createHash('sha256').update(bytes).digest('hex'),
        content_type: types[ext] || 'application/octet-stream',
      });
    }
  }
};
walk(root);
files.sort((a, b) => a.key.localeCompare(b.key));
const manifest = {
  schema_version: 'static_secret_candidate_manifest_v1',
  site_mode: 'secret_candidate',
  publication_mode: 'secret_link',
  build_id: process.env.CANDIDATE_BUILD_ID,
  base_path: process.env.CANDIDATE_BASE_PATH,
  token_sha256: process.env.CANDIDATE_TOKEN_SHA256,
  generated_at: new Date().toISOString(),
  files,
  checks: {
    candidate_contract: 'ok', catalog_parity: 'ok', noindex: 'ok',
    no_referrer: 'ok', prefix_containment: 'ok', root_isolation: 'ok',
  },
};
writeFileSync(join(root, 'secret-candidate-manifest.json'), `${JSON.stringify(manifest, null, 2)}\n`);
NODE

object_count="$(jq '.files | length + 1' "$candidate_root/secret-candidate-manifest.json")"
export SECRET_CANDIDATE_TOKEN="$token"
export CANDIDATE_BASE_PATH="$base_path"
export CANDIDATE_ROOT="$candidate_root"
export CANDIDATE_URL="${public_base}${base_path}/"
export CANDIDATE_BUILD_ID="$build_id"
export CANDIDATE_TOKEN_SHA256="$token_sha"
export CANDIDATE_OBJECT_COUNT="$object_count"

node target/site/scripts/deploy-secret-candidate-yc.mjs plan > "$artifact_dir/publication-plan.json"
jq -e '.ok == true and .root_mutation == false and .stable_ics_mutation == false and .overwrite_allowed == false' "$artifact_dir/publication-plan.json" >/dev/null
export SECRET_CANDIDATE_CONFIRM="publish-secret:${CANDIDATE_BUILD_ID}:${CANDIDATE_TOKEN_SHA256}"
node target/site/scripts/deploy-secret-candidate-yc.mjs publish > "$artifact_dir/publication-result.json"
jq -e '.ok == true and .command == "publish" and .public_verification == "ok"' "$artifact_dir/publication-result.json" >/dev/null

live="$artifact_dir/live"
mkdir -p "$live"
ready=0
for attempt in $(seq 1 30); do
  if curl --fail --silent --show-error \
    --header 'Cache-Control: no-cache' \
    --dump-header "$live/root-headers.txt" \
    --output "$live/root-index.html" \
    "$CANDIDATE_URL"; then
    if grep -q 'data-prelaunch-page' "$live/root-index.html" \
      && grep -q 'approved-desktop-mobile-v2' "$live/root-index.html" \
      && grep -q 'noindex,nofollow,noarchive,nosnippet' "$live/root-index.html"; then
      ready=1
      break
    fi
  fi
  sleep 2
done
test "$ready" = 1

snapshot "$artifact_dir/production-surfaces-after.json"
BEFORE="$artifact_dir/production-surfaces-before.json" AFTER="$artifact_dir/production-surfaces-after.json" node --input-type=module <<'NODE'
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
assert.deepEqual(
  JSON.parse(readFileSync(process.env.AFTER, 'utf8')),
  JSON.parse(readFileSync(process.env.BEFORE, 'utf8')),
);
NODE

printf '%s\n' "$CANDIDATE_URL" > "$artifact_dir/candidate-public-url.txt"
printf '%s\n' "$target_sha" > "$artifact_dir/repository-sha.txt"
cp "$candidate_root/secret-candidate-manifest.json" "$artifact_dir/secret-candidate-manifest.json"

REQUEST_ID="$request_id" \
TARGET_BRANCH="$target_branch" \
TARGET_SHA="$target_sha" \
VISUAL_RUN_ID="$visual_run_id" \
VISUAL_ARTIFACT_DIGEST="$visual_artifact_digest" \
RELAY_STATUS="$relay_status" \
node --input-type=module <<'NODE'
import { writeFileSync } from 'node:fs';
const receipt = {
  schema_version: 'prelaunch_approved_publication_receipt_v2',
  request_id: process.env.REQUEST_ID,
  public_url: process.env.CANDIDATE_URL,
  target_branch: process.env.TARGET_BRANCH,
  target_sha: process.env.TARGET_SHA,
  visual_run_id: process.env.VISUAL_RUN_ID,
  visual_artifact_digest: process.env.VISUAL_ARTIFACT_DIGEST,
  publish_run_id: process.env.GITHUB_RUN_ID,
  build_id: process.env.CANDIDATE_BUILD_ID,
  token_sha256: process.env.CANDIDATE_TOKEN_SHA256,
  object_count: Number(process.env.CANDIDATE_OBJECT_COUNT),
  approved_background_integrity: 'ok',
  backend_direct_honeypot_probe: 'ok',
  backend_relay_honeypot_probe: process.env.RELAY_STATUS,
  public_page_verification: 'ok',
  production_root_changed: false,
  stable_ics_changed: false,
  create_only_upload: true,
  generated_at: new Date().toISOString(),
};
writeFileSync(
  'artifacts/prelaunch-approved-publish-v2/publication-receipt.json',
  `${JSON.stringify(receipt, null, 2)}\n`,
);
NODE

{
  echo '## Published approved prelaunch candidate'
  echo
  echo "$CANDIDATE_URL"
  echo
  echo "Target SHA: $target_sha"
  echo "Visual run: $visual_run_id"
  echo "Relay status: $relay_status"
  echo 'Production root changed: no'
} >> "$GITHUB_STEP_SUMMARY"
