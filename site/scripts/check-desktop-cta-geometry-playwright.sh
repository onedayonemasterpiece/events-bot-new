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
# 1920x1080 at 125% desktop scaling exposes a 1536x864 CSS-pixel viewport.
playwright-cli -s="$session" resize 1536 864 >/dev/null

check_geometry() {
  local slug="$1"
  local expected_family="$2"
  playwright-cli -s="$session" goto "$base/sobytiya/$slug/" >/dev/null
  result="$(playwright-cli -s="$session" --raw eval "(()=>{const expected='$expected_family';const root=document.querySelector('[data-desktop-clean-event]');const p=document.querySelector('[data-desktop-action-panel]');const admission=p?.querySelector(':scope > p:first-child');const primary=p?.querySelector(':scope > .desktop-prototype__primary-action');const r=p?.querySelector(':scope > [data-desktop-action-row=\"calendar-share-like\"]');const controls=r?[...r.children].filter(e=>e instanceof HTMLElement):[];if(!root||!p||!admission||!primary||!r||controls.length!==3)return {ok:false,reason:'missing root/panel/three actions'};const pr=p.getBoundingClientRect(),ar=admission.getBoundingClientRect(),xr=primary.getBoundingClientRect(),rr=r.getBoundingClientRect(),cr=controls.map(e=>e.getBoundingClientRect());const actualFamily=root.dataset.desktopFamily;const panelFamily=p.dataset.actionFamily;const layout=p.dataset.actionLayout;const controlsAligned=Math.max(...cr.map(x=>x.top))-Math.min(...cr.map(x=>x.top))<=1&&Math.max(...cr.map(x=>x.bottom))-Math.min(...cr.map(x=>x.bottom))<=1;const contained=[ar,xr,rr,...cr].every(x=>x.left>=pr.left-1&&x.right<=pr.right+1&&x.top>=pr.top-1&&x.bottom<=pr.bottom+1);const overflow=p.scrollWidth<=p.clientWidth+1&&r.scrollWidth<=r.clientWidth+1;const centers=[ar,xr,rr].map(x=>(x.top+x.bottom)/2);const oneRow=Math.max(...centers)-Math.min(...centers)<=2&&ar.right<=xr.left+1&&xr.right<=rr.left+1&&pr.height<=128;const threeRows=ar.bottom<=xr.top+1&&xr.bottom<=rr.top+1&&Math.abs(rr.bottom-(pr.bottom-parseFloat(getComputedStyle(p).paddingBottom)))<=2&&pr.height>=160;const geometry=expected==='split'?oneRow:threeRows;return {ok:actualFamily===expected&&panelFamily===expected&&layout===(expected==='split'?'inline':'stacked')&&controlsAligned&&contained&&overflow&&geometry,expected,actualFamily,panelFamily,layout,controlsAligned,contained,overflow,oneRow,threeRows,panel:{w:pr.width,h:pr.height},admission:{l:ar.left,r:ar.right,t:ar.top,b:ar.bottom},primary:{l:xr.left,r:xr.right,t:xr.top,b:xr.bottom},row:{l:rr.left,r:rr.right,t:rr.top,b:rr.bottom},controls:cr.map(x=>({w:x.width,h:x.height,top:x.top,bottom:x.bottom}))};})()")"
  node -e 'const [slug,raw]=process.argv.slice(1);const value=JSON.parse(raw);if(!value.ok){console.error(slug,JSON.stringify(value));process.exit(1)}console.log(slug,JSON.stringify(value))' "$slug" "$result"
}

# Multiple frozen/live portrait specimens protect the compact Split family.
for slug in \
  "opera-i-dzhaz-znamensk-6876" \
  "myuzikl-alye-parusa-kaliningrad-4783"
do
  check_geometry "$slug" split
done

# Wide-photo Editorial pages retain the accepted tall card and bottom utility row.
for slug in \
  "kontsert-more-muzyki-svetlogorsk-6551" \
  "tribyut-linkin-park-ot-yalta-band-pos-romanovo-5374"
do
  check_geometry "$slug" editorial
done
