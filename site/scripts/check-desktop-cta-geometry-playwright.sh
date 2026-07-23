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
  local label="$1"
  local route="$2"
  local expected_family="$3"
  local expected_primary_label="${4:-}"
  local expected_control_count="${5:-3}"
  local expected_mode="${6:-default}"
  playwright-cli -s="$session" goto "$base/$route/" >/dev/null
  result="$(playwright-cli -s="$session" --raw eval "(()=>{const expected='$expected_family';const expectedLabel='$expected_primary_label';const expectedControlCount=Number('$expected_control_count');const expectedMode='$expected_mode';const root=document.querySelector('[data-desktop-clean-event]');const p=document.querySelector('[data-desktop-action-panel]');const admission=p?.querySelector(':scope > p:first-child');const primary=p?.querySelector(':scope > .desktop-prototype__primary-action');const primaryLabel=primary?.querySelector('[data-desktop-primary-label],[data-calendar-label],[data-desktop-phone-label]');const r=p?.querySelector(':scope > [data-desktop-action-row=\"calendar-share-like\"]');const controls=r?[...r.children].filter(e=>e instanceof HTMLElement):[];if(!root||!p||!admission||!primary||!r||controls.length!==expectedControlCount)return {ok:false,reason:'missing root/panel/expected actions',expectedControlCount,actualControlCount:controls.length};const pr=p.getBoundingClientRect(),ar=admission.getBoundingClientRect(),xr=primary.getBoundingClientRect(),rr=r.getBoundingClientRect(),cr=controls.map(e=>e.getBoundingClientRect());const actualFamily=root.dataset.desktopFamily;const panelFamily=p.dataset.actionFamily;const layout=p.dataset.actionLayout;const controlsAligned=Math.max(...cr.map(x=>x.top))-Math.min(...cr.map(x=>x.top))<=1&&Math.max(...cr.map(x=>x.bottom))-Math.min(...cr.map(x=>x.bottom))<=1;const contained=[ar,xr,rr,...cr].every(x=>x.left>=pr.left-1&&x.right<=pr.right+1&&x.top>=pr.top-1&&x.bottom<=pr.bottom+1);const overflow=p.scrollWidth<=p.clientWidth+1&&r.scrollWidth<=r.clientWidth+1;const centers=[ar,xr,rr].map(x=>(x.top+x.bottom)/2);const oneRow=Math.max(...centers)-Math.min(...centers)<=2&&ar.right<=xr.left+1&&xr.right<=rr.left+1&&pr.height<=128;const threeRows=ar.bottom<=xr.top+1&&xr.bottom<=rr.top+1&&Math.abs(rr.bottom-(pr.bottom-parseFloat(getComputedStyle(p).paddingBottom)))<=2&&pr.height>=160;const geometry=expected==='split'?oneRow:threeRows;const primaryText=(primaryLabel?.textContent||primary.textContent||'').trim();const lr=primaryLabel?.getBoundingClientRect();const lineHeight=primaryLabel?(parseFloat(getComputedStyle(primaryLabel).lineHeight)||parseFloat(getComputedStyle(primaryLabel).fontSize)||16):0;const primaryLabelFits=!primaryLabel||(lr.left>=xr.left-1&&lr.right<=xr.right+1&&lr.height<=lineHeight*1.35&&primaryLabel.scrollWidth<=primaryLabel.clientWidth+1);const secondaryCalendar=r.querySelector('[data-calendar-action]');const secondaryCalendarLabel=secondaryCalendar?.querySelector('[data-calendar-label]');const secondaryCalendarLabelRect=secondaryCalendarLabel?.getBoundingClientRect();const iconsMode=expectedMode!=='icons'||(p.dataset.actionDensity==='compact'&&p.dataset.actionFit==='icons'&&secondaryCalendar&&secondaryCalendarLabelRect.width<=1);const calendarActions=[...p.querySelectorAll('[data-calendar-action]')];const calendarPrimaryMode=expectedMode!=='calendar-primary'||(primary.matches('[data-calendar-action]')&&calendarActions.length===1&&!secondaryCalendar);const labelMatches=!expectedLabel||primaryText===expectedLabel;return {ok:actualFamily===expected&&panelFamily===expected&&layout===(expected==='split'?'inline':'stacked')&&controlsAligned&&contained&&overflow&&geometry&&primaryLabelFits&&iconsMode&&calendarPrimaryMode&&labelMatches,expected,actualFamily,panelFamily,layout,density:p.dataset.actionDensity,fit:p.dataset.actionFit,controlsAligned,contained,overflow,oneRow,threeRows,primaryText,primaryLabelFits,iconsMode,calendarPrimaryMode,labelMatches,panel:{w:pr.width,h:pr.height},admission:{l:ar.left,r:ar.right,t:ar.top,b:ar.bottom},primary:{l:xr.left,r:xr.right,t:xr.top,b:xr.bottom},row:{l:rr.left,r:rr.right,t:rr.top,b:rr.bottom},controls:cr.map(x=>({w:x.width,h:x.height,top:x.top,bottom:x.bottom}))};})()")"
  node -e 'const [label,raw]=process.argv.slice(1);const value=JSON.parse(raw);if(!value.ok){console.error(label,JSON.stringify(value));process.exit(1)}console.log(label,JSON.stringify(value))' "$label" "$result"
}

# These two immutable candidate specimens are intentionally independent from
# current event eligibility. Event 6876/4783 may expire; that must not turn the
# live Playwright gate into a false 404 pass/failure or silently remove the
# compact Split contract.
check_geometry "split CTA fixture" "lab/event-desktop/examples/cta-phone-invariant" split
check_geometry "registration CTA fixture" "lab/event-desktop/examples/cta-registration-invariant" split "Зарегистрироваться" 3 icons
check_geometry "free calendar-primary CTA fixture" "lab/event-desktop/examples/cta-free-calendar-invariant" split "В календарь" 2 calendar-primary
check_geometry "editorial CTA fixture" "lab/event-desktop/examples/footer-service-v1" editorial
