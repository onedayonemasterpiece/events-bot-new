#!/usr/bin/env bash
set -euo pipefail

: "${STATIC_SITE_REVIEW_BASE_URL:?Set STATIC_SITE_REVIEW_BASE_URL to the immutable preview/candidate base URL}"
command -v playwright-cli >/dev/null 2>&1 || {
  echo "playwright-cli is required for listing desktop geometry acceptance" >&2
  exit 2
}

base="${STATIC_SITE_REVIEW_BASE_URL%/}"
browser="${STATIC_SITE_PLAYWRIGHT_BROWSER:-chromium}"

check_route() {
  local route="$1" width="$2" height="$3"
  local session="listing-desktop-${route}-${width}-$$"
  cleanup() { playwright-cli -s="$session" close >/dev/null 2>&1 || true; }
  trap cleanup RETURN
  playwright-cli -s="$session" open --browser="$browser" >/dev/null
  playwright-cli -s="$session" resize "$width" "$height" >/dev/null
  playwright-cli -s="$session" goto "$base/$route/" >/dev/null
  focus_result="$(playwright-cli -s="$session" --raw eval "(()=>{const link=document.querySelector('.skip-link'),header=document.querySelector('.site-header');link?.focus();const lr=link?.getBoundingClientRect(),lc=link&&getComputedStyle(link),hc=header&&getComputedStyle(header);return {ok:document.activeElement===link&&(lr?.top||0)>=0&&Number(lc?.zIndex)>Number(hc?.zIndex),top:lr?.top,z:lc?.zIndex,headerZ:hc?.zIndex}})()")"
  node -e 'const value=JSON.parse(process.argv[1]);if(!value.ok){console.error(JSON.stringify(value));process.exit(1)}' "$focus_result"
  playwright-cli -s="$session" --raw eval "(()=>{document.documentElement.style.scrollBehavior='auto';window.scrollTo(0,Math.min(1100,document.documentElement.scrollHeight-innerHeight-10));return window.scrollY})()" >/dev/null
  result="$(playwright-cli -s="$session" --raw eval "(()=>{const q=s=>document.querySelector(s);const box=e=>e?.getBoundingClientRect();const css=e=>e&&getComputedStyle(e);const header=q('.site-header');const rail=q('.ke-listing-discovery-rail');const root=q('.ke-listing-page');const media=q('.ke-listing-card__media');const hb=box(header),rb=box(rail),mb=box(media),hc=css(header),rc=css(rail),pc=css(root),mc=css(media);const ok=hc?.position==='sticky'&&Math.abs(hb?.top||0)<=1&&Math.abs((hb?.height||0)-57)<=1&&(!rail||(rc?.position==='sticky'&&Math.abs((rb?.top||0)-(hb?.bottom||0))<=1&&Number(hc?.zIndex)>Number(rc?.zIndex)))&&pc?.position==='relative'&&(!media||(mc?.display==='block'&&mc?.position==='relative'&&(mb?.height||0)>=100&&(mb?.height||0)<=300&&(mb?.width||0)<innerWidth))&&document.documentElement.scrollWidth<=document.documentElement.clientWidth+1;return {ok,route:'$route',viewport:[innerWidth,innerHeight],scrollY,header:{position:hc?.position,top:hb?.top,height:hb?.height,z:hc?.zIndex},rail:rail?{position:rc?.position,top:rb?.top,z:rc?.zIndex}:null,rootPosition:pc?.position,media:media?{display:mc?.display,position:mc?.position,width:mb?.width,height:mb?.height}:null,overflow:document.documentElement.scrollWidth-document.documentElement.clientWidth};})()")"
  node -e 'const value=JSON.parse(process.argv[1]);if(!value.ok){console.error(JSON.stringify(value));process.exit(1)}console.log(JSON.stringify(value))' "$result"
  cleanup
  trap - RETURN
}

# 1536×864 equals FHD at 125%; adjacent widths protect adaptive packing.
for viewport in "1366 768" "1536 864" "1920 1080"; do
  read -r width height <<<"$viewport"
  for route in segodnya zavtra vyhodnye populyarnoe; do
    check_route "$route" "$width" "$height"
  done
done
