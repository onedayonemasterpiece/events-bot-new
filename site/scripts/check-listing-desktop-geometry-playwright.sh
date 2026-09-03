#!/usr/bin/env bash
set -euo pipefail

: "${STATIC_SITE_REVIEW_BASE_URL:?Set STATIC_SITE_REVIEW_BASE_URL to the immutable preview/candidate base URL}"
command -v playwright-cli >/dev/null 2>&1 || {
  echo "playwright-cli is required for listing desktop geometry acceptance" >&2
  exit 2
}

base="${STATIC_SITE_REVIEW_BASE_URL%/}"
browser="${STATIC_SITE_PLAYWRIGHT_BROWSER:-}"
config="${STATIC_SITE_PLAYWRIGHT_CONFIG:-}"

check_route() {
  local route="$1" width="$2" height="$3"
  local session="listing-desktop-${route}-${width}-$$"
  cleanup() { playwright-cli -s="$session" close >/dev/null 2>&1 || true; }
  trap cleanup RETURN
  local -a open_args=()
  [[ -n "$browser" ]] && open_args+=("--browser=$browser")
  [[ -n "$config" ]] && open_args+=("--config=$config")
  playwright-cli -s="$session" open "${open_args[@]}" >/dev/null
  playwright-cli -s="$session" resize "$width" "$height" >/dev/null
  playwright-cli -s="$session" goto "$base/$route/" >/dev/null
  focus_result="$(playwright-cli -s="$session" --raw eval "(()=>{const link=document.querySelector('.skip-link'),header=document.querySelector('.site-header');link?.focus();const lr=link?.getBoundingClientRect(),lc=link&&getComputedStyle(link),hc=header&&getComputedStyle(header);return {ok:document.activeElement===link&&(lr?.top||0)>=0&&Number(lc?.zIndex)>Number(hc?.zIndex),top:lr?.top,z:lc?.zIndex,headerZ:hc?.zIndex}})()")"
  node -e 'const value=JSON.parse(process.argv[1]);if(!value.ok){console.error(JSON.stringify(value));process.exit(1)}' "$focus_result"
  playwright-cli -s="$session" --raw eval "(()=>{document.documentElement.style.scrollBehavior='auto';const rail=document.querySelector('.ke-listing-discovery-rail'),header=document.querySelector('.site-header');const pinY=rail?rail.getBoundingClientRect().top+scrollY-(header?.getBoundingClientRect().height||0)+16:1100;window.scrollTo(0,Math.max(0,Math.min(1100,pinY,document.documentElement.scrollHeight-innerHeight-10)));return window.scrollY})()" >/dev/null
  result="$(playwright-cli -s="$session" --raw eval "(()=>{const q=s=>document.querySelector(s);const box=e=>e?.getBoundingClientRect();const css=e=>e&&getComputedStyle(e);const header=q('.site-header');const rail=q('.ke-listing-discovery-rail');const root=q('.ke-listing-page');const media=q('.ke-listing-card__media');const hb=box(header),rb=box(rail),mb=box(media),hc=css(header),rc=css(rail),pc=css(root),mc=css(media);const maxScroll=Math.max(0,document.documentElement.scrollHeight-innerHeight);const atShortPageProbeEnd=scrollY>=Math.max(0,maxScroll-11);const railPositionOk=!rail||(rc?.position==='sticky'&&Number(hc?.zIndex)>Number(rc?.zIndex)&&(Math.abs((rb?.top||0)-(hb?.bottom||0))<=1||(atShortPageProbeEnd&&(rb?.top||0)>=(hb?.bottom||0))));const popularRows=Array.from(document.querySelectorAll('.ke-popular-desktop .ke-popular-behavior__row')).filter(row=>{const rect=box(row),style=css(row);return rect&&rect.width>0&&rect.height>0&&style?.display!=='none'&&style?.visibility!=='hidden'}).map(row=>{const style=css(row);return {clientWidth:row.clientWidth,scrollWidth:row.scrollWidth,flexWrap:style?.flexWrap,overflowX:style?.overflowX,ok:style?.flexWrap==='nowrap'&&(row.scrollWidth<=row.clientWidth+1||style?.overflowX==='auto'||style?.overflowX==='scroll')}});const popularRowsOk=popularRows.every(row=>row.ok);const ok=hc?.position==='sticky'&&Math.abs(hb?.top||0)<=1&&Math.abs((hb?.height||0)-57)<=1&&railPositionOk&&pc?.position==='relative'&&(!media||(mc?.display==='block'&&mc?.position==='relative'&&(mb?.height||0)>=100&&(mb?.height||0)<=300&&(mb?.width||0)<innerWidth))&&popularRowsOk&&document.documentElement.scrollWidth<=document.documentElement.clientWidth+1;return {ok,route:'$route',viewport:[innerWidth,innerHeight],scrollY,maxScroll,header:{position:hc?.position,top:hb?.top,height:hb?.height,z:hc?.zIndex},rail:rail?{position:rc?.position,top:rb?.top,z:rc?.zIndex}:null,rootPosition:pc?.position,media:media?{display:mc?.display,position:mc?.position,width:mb?.width,height:mb?.height}:null,popularRows,overflow:document.documentElement.scrollWidth-document.documentElement.clientWidth};})()")"
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

# Current real data can exceed the desktop shell around both the 980px mobile
# boundary and the 1366px presentation contour. The row, not the document,
# owns that horizontal excess while preserving one row and intrinsic widths.
for width in 961 1022 1023 1024 1440; do
  check_route populyarnoe "$width" 900
done

# The desktop Weekend board begins immediately above the 720px mobile
# boundary. Its fixed time lane and two day summaries must shrink internally;
# long day/count labels may never widen the document at that seam.
for width in 721 800; do
  check_route vyhodnye "$width" 900
done
