#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

: "${EXPECTED_SHA:?EXPECTED_SHA is required}"
: "${KENIGEVENTS_SITE_YC_BUCKET:?bucket is required}"
: "${KENIGEVENTS_SITE_YC_ACCESS_KEY_ID:?access key is required}"
: "${KENIGEVENTS_SITE_YC_SECRET_ACCESS_KEY:?secret key is required}"
: "${PUBLIC_PERSONALIZATION_SUPABASE_URL:?Supabase URL is required}"
: "${PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY:?publishable key is required}"
[[ "$EXPECTED_SHA" =~ ^[0-9a-f]{40}$ ]] || { echo 'expected_sha must be a full commit SHA' >&2; exit 2; }

ROOT_ORIGIN='https://kenigevents.ru'
PROD_VISUAL_SHA='9d8fc9203a69f385407a57e23310bb47f2db4e2d'
EVIDENCE='artifacts/prelaunch-root-release'
ENDPOINT="${KENIGEVENTS_SITE_YC_ENDPOINT:-https://storage.yandexcloud.net}"
BUCKET="$KENIGEVENTS_SITE_YC_BUCKET"
mkdir -p "$EVIDENCE"/{local-browser,live-browser,backup}

# Steady-state governance: only the exact current origin/main may publish.
git fetch origin main --no-tags
test "$(git rev-parse HEAD)" = "$EXPECTED_SHA"
test "$(git rev-parse origin/main)" = "$EXPECTED_SHA"
test "${GITHUB_REF:-refs/heads/main}" = refs/heads/main
test -z "$(git status --porcelain)"

# The form may change; visual, background, assets and SEO/GEO bytes may not.
git diff --exit-code "$PROD_VISUAL_SHA" -- \
  site/src/assets/prelaunch-approved \
  site/src/components/PrelaunchPage.astro \
  site/src/layouts/PrelaunchLayout.astro \
  site/src/pages/index.astro \
  site/src/styles/prelaunch-calibration.css \
  site/src/styles/prelaunch-consent.css \
  site/src/styles/prelaunch-page.css \
  site/src/styles/prelaunch-polish.css \
  site/src/styles/prelaunch-static.css \
  site/scripts/prepare-prelaunch-artwork.mjs

npm --prefix site run test:prelaunch-form

# Build the exact root dependency closure, not the unrelated catalogue routes.
all_pages="site/.prelaunch-root-pages-${GITHUB_RUN_ID:-local}"
mv site/src/pages "$all_pages"
mkdir site/src/pages
cp "$all_pages/index.astro" site/src/pages/index.astro
restore_pages() { test ! -d "$all_pages" || { rm -rf site/src/pages; mv "$all_pages" site/src/pages; }; }
trap restore_pages EXIT
PUBLIC_PRELAUNCH_MODE=on PUBLIC_SITE_MODE=production PUBLIC_SITE_ORIGIN="$ROOT_ORIGIN" \
  SITE_BASE_PATH=/ npm --prefix site run build
restore_pages
trap - EXIT

grep -q 'data-prelaunch-page' site/dist/index.html
grep -q '<title>Полюбить Калининград Анонсы — запуск 1 сентября</title>' site/dist/index.html
if grep -q 'noindex' site/dist/index.html; then echo 'Production root contains noindex' >&2; exit 1; fi

python3 -m http.server 4173 --directory site/dist >"$EVIDENCE/local-http.log" 2>&1 &
server_pid=$!
trap 'kill "$server_pid" 2>/dev/null || true' EXIT
for _ in {1..30}; do curl -fsS http://127.0.0.1:4173/ >/dev/null && break; sleep 1; done
npm --prefix site run check:prelaunch-form -- \
  --url http://127.0.0.1:4173/ --artifact-dir "../$EVIDENCE/local-browser"
kill "$server_pid" 2>/dev/null || true
wait "$server_pid" 2>/dev/null || true
trap - EXIT

# Manifest is deliberately limited to root HTML, its hashed Astro closure and
# the approved prelaunch/brand images referenced by that HTML/CSS.
EXPECTED_SHA="$EXPECTED_SHA" node --input-type=module <<'NODE'
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, readdirSync, statSync, writeFileSync } from 'node:fs';
import { extname, join, relative } from 'node:path';
const root='site/dist';
const keys=new Set(['index.html','assets/prelaunch/prelaunch-scene-desktop.webp','assets/prelaunch/prelaunch-scene-mobile.webp','assets/pwa/announcements-brand-v2-192.png','assets/pwa/announcements-brand-v2-512.png']);
function walk(dir){for(const e of readdirSync(dir,{withFileTypes:true})){const p=join(dir,e.name);e.isDirectory()?walk(p):keys.add(relative(root,p).replaceAll('\\','/'));}}
walk(join(root,'_astro'));
const types={'.html':'text/html; charset=utf-8','.css':'text/css; charset=utf-8','.js':'text/javascript; charset=utf-8','.png':'image/png','.webp':'image/webp','.svg':'image/svg+xml','.woff2':'font/woff2'};
const files=[...keys].sort().map(key=>{const path=join(root,key);if(!existsSync(path)||!statSync(path).isFile())throw new Error(`missing ${key}`);const b=readFileSync(path);return {key,path,sha256:createHash('sha256').update(b).digest('hex'),content_type:types[extname(key)]||'application/octet-stream',cache_control:key==='index.html'?'public, max-age=0, must-revalidate':key.startsWith('_astro/')?'public, max-age=31536000, immutable':'public, max-age=3600, must-revalidate'};});
mkdirSync('artifacts/prelaunch-root-release',{recursive:true});
writeFileSync('artifacts/prelaunch-root-release/deployment-manifest.json',JSON.stringify({schema_version:'prelaunch_root_release_v2',repo_sha:process.env.EXPECTED_SHA,files},null,2)+'\n');
NODE

export AWS_ACCESS_KEY_ID="$KENIGEVENTS_SITE_YC_ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$KENIGEVENTS_SITE_YC_SECRET_ACCESS_KEY"
export AWS_DEFAULT_REGION="${KENIGEVENTS_SITE_YC_REGION:-ru-central1}"
aws_cmd(){ aws --endpoint-url "$ENDPOINT" "$@"; }
manifest="$EVIDENCE/deployment-manifest.json"
release="prelaunch-main-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}"
stage="_static/prelaunch-root-releases/$release/root"

# Stage and verify immutable copies before touching live keys.
while IFS=$'\t' read -r key path type cache sha; do
  aws_cmd s3 cp "$path" "s3://$BUCKET/$stage/$key" --content-type "$type" --cache-control "$cache" --no-progress
  test "$(aws_cmd s3 cp "s3://$BUCKET/$stage/$key" - --no-progress | sha256sum | cut -d' ' -f1)" = "$sha"
done < <(jq -r '.files[]|[.key,.path,.content_type,.cache_control,.sha256]|@tsv' "$manifest")

# Backup every target and restore it automatically if promotion/live proof fails.
: > "$EVIDENCE/backup/inventory.tsv"
while IFS= read -r key; do
  safe=$(printf %s "$key"|sha256sum|cut -d' ' -f1)
  if aws_cmd s3api head-object --bucket "$BUCKET" --key "$key" >/dev/null 2>&1; then
    aws_cmd s3 cp "s3://$BUCKET/$key" "$EVIDENCE/backup/$safe" --no-progress
    printf '1\t%s\t%s\n' "$key" "$safe" >> "$EVIDENCE/backup/inventory.tsv"
  else printf '0\t%s\t-\n' "$key" >> "$EVIDENCE/backup/inventory.tsv"; fi
done < <(jq -r '.files[].key' "$manifest")
rollback=1
rollback_live(){ status=$?; if [[ "$rollback" = 1 ]]; then while IFS=$'\t' read -r existed key safe; do if [[ "$existed" = 1 ]]; then aws_cmd s3 cp "$EVIDENCE/backup/$safe" "s3://$BUCKET/$key" --no-progress; else aws_cmd s3 rm "s3://$BUCKET/$key" --no-progress || true; fi; done < "$EVIDENCE/backup/inventory.tsv"; fi; exit "$status"; }
trap rollback_live ERR INT TERM

# Root HTML is promoted last.
for root_only in false true; do
  while IFS= read -r key; do
    if [[ "$root_only" = true ]]; then
      [[ "$key" = index.html ]] || continue
    else
      [[ "$key" != index.html ]] || continue
    fi
    aws_cmd s3 cp "s3://$BUCKET/$stage/$key" "s3://$BUCKET/$key" --metadata-directive COPY --no-progress
  done < <(jq -r '.files[].key' "$manifest")
done

for _ in {1..90}; do curl -fsS -H 'Cache-Control: no-cache' "$ROOT_ORIGIN/" -o "$EVIDENCE/live-root.html" && grep -q 'data-prelaunch-page' "$EVIDENCE/live-root.html" && break; sleep 5; done
grep -q 'data-prelaunch-page' "$EVIDENCE/live-root.html"
npm --prefix site run check:prelaunch-form -- --url "$ROOT_ORIGIN/" --artifact-dir "../$EVIDENCE/live-browser"

jq -n --arg repo_sha "$EXPECTED_SHA" --arg release_id "$release" --arg published_at "$(date -u +%FT%TZ)" '{schema_version:"prelaunch_root_release_receipt_v2",result:"published",repo_sha:$repo_sha,release_id:$release_id,published_at:$published_at,source_branch:"main",local_form_gate:"ok",live_form_gate:"ok"}' > "$EVIDENCE/release-receipt.json"
rollback=0
trap - ERR INT TERM
echo "PUBLISHED_REPO_SHA=$EXPECTED_SHA"
echo "PUBLISHED_ROOT_URL=$ROOT_ORIGIN/"
