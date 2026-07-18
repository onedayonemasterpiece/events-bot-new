#!/usr/bin/env bash
set -euo pipefail

: "${STATIC_SITE_REVIEW_BASE_URL:?Set STATIC_SITE_REVIEW_BASE_URL to the immutable preview/candidate base URL}"
command -v playwright-cli >/dev/null 2>&1 || {
  echo "playwright-cli is required for desktop CTA geometry acceptance" >&2
  exit 2
}

session="static-cta-geometry-$$"
cleanup() { status=$?; trap - EXIT; playwright-cli -s="$session" close >/dev/null 2>&1 || true; exit "$status"; }
trap cleanup EXIT
base="${STATIC_SITE_REVIEW_BASE_URL%/}"
browser="${STATIC_SITE_PLAYWRIGHT_BROWSER:-chromium}"
playwright-cli -s="$session" open --browser="$browser" >/dev/null
playwright-cli -s="$session" resize 1536 864 >/dev/null

for slug in \
  "kontsert-more-muzyki-svetlogorsk-6551" \
  "tribyut-linkin-park-ot-yalta-band-pos-romanovo-5374"
do
  playwright-cli -s="$session" goto "$base/sobytiya/$slug/" >/dev/null
  result="$(playwright-cli -s="$session" --raw eval "(()=>{const p=document.querySelector('[data-desktop-action-panel]');const r=document.querySelector('[data-desktop-action-row=\"calendar-share-like\"]');const controls=r?[...r.children].filter(e=>e instanceof HTMLElement):[];if(!p||!r||controls.length!==3)return {ok:false,reason:'missing invariant panel/row/controls'};const pr=p.getBoundingClientRect(),rr=r.getBoundingClientRect(),cr=controls.map(e=>e.getBoundingClientRect());const aligned=Math.max(...cr.map(x=>x.top))-Math.min(...cr.map(x=>x.top))<=1&&Math.max(...cr.map(x=>x.bottom))-Math.min(...cr.map(x=>x.bottom))<=1;const contained=cr.every(x=>x.left>=pr.left-1&&x.right<=pr.right+1)&&rr.bottom<=pr.bottom+1;const bottom=Math.abs(rr.bottom-pr.bottom)<=32;const overflow=p.scrollWidth<=p.clientWidth+1&&r.scrollWidth<=r.clientWidth+1;return {ok:aligned&&contained&&bottom&&overflow,aligned,contained,bottom,overflow,panel:{w:pr.width,h:pr.height},row:{w:rr.width,h:rr.height},controls:cr.map(x=>({w:x.width,h:x.height,top:x.top,bottom:x.bottom}))};})()")"
  node -e 'const [slug,raw]=process.argv.slice(1);const value=JSON.parse(raw);if(!value.ok){console.error(slug,JSON.stringify(value));process.exit(1)}console.log(slug,JSON.stringify(value))' "$slug" "$result"
done
