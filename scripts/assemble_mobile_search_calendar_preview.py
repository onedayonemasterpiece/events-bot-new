#!/usr/bin/env python3
"""Compose the accepted v13 calendar shell with the real Astro Search build.

This is deliberately an assembly step, not a third renderer.  The Astro build
keeps the production Search runtime and canonical EventCard templates.  The
accepted v23 calendar HTML, rails, crop and gestures are copied from their
donor and receive the byte-for-byte v13 expanded-menu CSS/JS and markup.
"""

from __future__ import annotations

import argparse
import hashlib
import html as html_lib
import json
import random
import re
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import build_mobile_menu_reference4_leather_tag_lab as leather  # noqa: E402
import build_mobile_shell_unification_lab as shell  # noqa: E402


DEFAULT_DONOR = shell.DONOR_DEFAULT
ARTIFACT_SOURCE_PAGE = "date-2026-07-24"

ARTIFACT_CSS = r"""
/* Isolated amber-artifact placement research; rail geometry stays 112 CSS px. */
.amber-artifact {
  position: relative;
  isolation: isolate;
  flex: 0 0 94px;
  width: 94px;
  height: 112px;
  min-width: 94px;
  min-height: 112px;
  display: grid;
  place-items: center;
  overflow: hidden;
  padding: 0;
  border: 0;
  border-radius: 0;
  background: transparent;
  color: #221a14;
  cursor: pointer;
  touch-action: manipulation;
  -webkit-tap-highlight-color: transparent;
}
.amber-artifact::before {
  content: "";
  position: absolute;
  z-index: 0;
  left: 50%;
  bottom: 0;
  width: 94px;
  height: 68px;
  border-radius: 50%;
  background: radial-gradient(ellipse at 50% 76%, rgba(255,236,156,.58) 0 10%, rgba(255,190,43,.42) 28%, rgba(255,157,12,.18) 48%, rgba(255,153,0,0) 72%);
  opacity: .76;
  transform: translate3d(-50%,0,0) scale(.96);
  transform-origin: 50% 76%;
}
.amber-artifact::after {
  content: "";
  position: absolute;
  z-index: 1;
  left: 50%;
  bottom: 6px;
  box-sizing: border-box;
  width: 78px;
  height: 17px;
  border-radius: 50%;
  border: 1.5px solid rgba(255,238,174,.94);
  background: radial-gradient(ellipse, rgba(255,220,118,.28) 0 38%, rgba(255,180,35,.08) 58%, transparent 72%);
  box-shadow: 0 0 5px rgba(255,221,116,.88), 0 0 13px rgba(255,163,20,.52);
  opacity: .9;
  transform: translate3d(-50%,0,0) scale(.96);
  transform-origin: center;
}
.amber-artifact__rays {
  position: absolute;
  z-index: 0;
  left: 50%;
  bottom: 8px;
  width: 88px;
  height: 76px;
  pointer-events: none;
  background:
    linear-gradient(78deg, transparent 42%, rgba(255,205,92,.22) 49%, transparent 55%),
    linear-gradient(102deg, transparent 43%, rgba(255,222,130,.18) 50%, transparent 57%);
  -webkit-mask-image: linear-gradient(to top, #000 0 10%, rgba(0,0,0,.74) 40%, transparent 88%);
  mask-image: linear-gradient(to top, #000 0 10%, rgba(0,0,0,.74) 40%, transparent 88%);
  opacity: .54;
  transform: translate3d(-50%,0,0) scaleX(.92);
  transform-origin: 50% 100%;
}
.amber-artifact__visual {
  position: relative;
  z-index: 2;
  width: 74px;
  height: 96px;
  display: grid;
  place-items: center;
  transform-origin: 50% 72%;
}
.amber-artifact__visual img {
  grid-area: 1 / 1;
  width: 74px;
  height: 96px;
  display: block;
  object-fit: contain;
  pointer-events: none;
  filter: drop-shadow(0 5px 7px rgba(157,78,0,.26));
}
.amber-artifact__shine {
  grid-area: 1 / 1;
  width: 74px;
  height: 96px;
  opacity: 0;
  pointer-events: none;
  background: linear-gradient(108deg, transparent 22%, rgba(255,255,255,.82) 46%, rgba(255,226,142,.42) 56%, transparent 76%);
  -webkit-mask: url("./assets/gamification/amber-cosmonaut-3x.webp") center / contain no-repeat;
  mask: url("./assets/gamification/amber-cosmonaut-3x.webp") center / contain no-repeat;
  transform: translate3d(-72px,0,0);
}
.amber-artifact__found {
  position: absolute;
  z-index: 3;
  right: 2px;
  bottom: 5px;
  min-width: 52px;
  min-height: 24px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 3px;
  padding: 2px 5px;
  border: 1px solid rgba(121,48,20,.22);
  border-radius: 999px;
  background: rgba(255,253,248,.94);
  color: #793014;
  font-family: inherit;
  font-size: 9px;
  font-weight: 800;
  line-height: 1;
  opacity: 0;
  transform: translate3d(0,5px,0);
  pointer-events: none;
}
.amber-artifact__found svg { width: 12px; height: 12px; fill: none; stroke: currentColor; stroke-width: 2; }
.amber-artifact.is-awake:not(.is-collected) .amber-artifact__visual {
  animation: amber-arrive 420ms cubic-bezier(.175,.885,.32,1.275) both, amber-float 3000ms 440ms ease-in-out infinite alternate;
}
.amber-artifact.is-awake:not(.is-collected)::before {
  animation: amber-glow 3000ms 440ms ease-in-out infinite alternate;
}
.amber-artifact.is-awake:not(.is-collected)::after {
  animation: amber-ring 3000ms 440ms ease-in-out infinite alternate;
}
.amber-artifact.is-awake:not(.is-collected) .amber-artifact__rays {
  animation: amber-rays 3000ms 440ms ease-in-out infinite alternate;
}
.amber-artifact.is-awake:not(.is-collected) .amber-artifact__shine {
  animation: amber-shine-cycle 5200ms 760ms linear infinite both;
}
.amber-artifact.is-collecting .amber-artifact__visual { animation: amber-found 430ms cubic-bezier(.22,.86,.3,1) both; }
.amber-artifact.is-collected::before { opacity: .28; transform: translate3d(-50%,0,0) scale(.82); }
.amber-artifact.is-collected::after { opacity: .42; box-shadow:0 0 4px rgba(255,190,60,.38); transform:translate3d(-50%,0,0) scale(.86); }
.amber-artifact.is-collected .amber-artifact__rays { opacity: .16; }
.amber-artifact.is-collected .amber-artifact__visual { transform: translate3d(0,1px,0) scale(.94); filter: saturate(.82); }
.amber-artifact.is-collected .amber-artifact__found { opacity: 1; transform: translate3d(0,0,0); transition: opacity 180ms ease-out, transform 220ms ease-out; }
.amber-artifact:focus-visible { outline: 3px solid rgba(15,118,110,.66); outline-offset: -4px; }
@keyframes amber-arrive { from { opacity:0; transform:translate3d(16px,3px,0) scale(.84) rotate(1.5deg); } to { opacity:1; transform:translate3d(0,-1px,0) scale(1) rotate(0); } }
@keyframes amber-float { from { transform:translate3d(0,1.5px,0) rotate(-.65deg); } to { transform:translate3d(0,-2.5px,0) rotate(.65deg); } }
@keyframes amber-glow { from { opacity:.62; transform:translate3d(-50%,0,0) scale(.94); } to { opacity:.86; transform:translate3d(-50%,-1px,0) scale(1.05); } }
@keyframes amber-ring { from { opacity:.72; transform:translate3d(-50%,0,0) scale(.95); } to { opacity:1; transform:translate3d(-50%,-1px,0) scale(1.04); } }
@keyframes amber-rays { from { opacity:.36; transform:translate3d(-50%,1px,0) scaleX(.9); } to { opacity:.62; transform:translate3d(-50%,-1px,0) scaleX(1.02); } }
@keyframes amber-shine-cycle { 0%,100% { opacity:0; transform:translate3d(-72px,0,0); } 3% { opacity:0; } 6% { opacity:.58; } 12.5% { opacity:0; transform:translate3d(72px,0,0); } 12.6%,99% { opacity:0; transform:translate3d(72px,0,0); } }
@keyframes amber-found { 0% { transform:scale(1) rotate(0); filter:brightness(1); } 45% { transform:scale(1.09) rotate(-1.5deg); filter:brightness(1.28); } 100% { transform:scale(.94) rotate(0); filter:brightness(1); } }
@media (prefers-reduced-motion: reduce) {
  .amber-artifact,
  .amber-artifact::before,
  .amber-artifact::after,
  .amber-artifact__rays,
  .amber-artifact__visual,
  .amber-artifact__shine,
  .amber-artifact__found { animation: none !important; transition: none !important; transform: none !important; }
  .amber-artifact::before,
  .amber-artifact::after,
  .amber-artifact__rays { transform: translate3d(-50%,0,0) !important; }
  .amber-artifact__shine { display:none; }
}
"""

ARTIFACT_JS = r"""
(() => {
  const buttons = [...document.querySelectorAll('[data-amber-artifact]')];
  if (!buttons.length) return;
  const reduced = matchMedia('(prefers-reduced-motion: reduce)').matches;
  const live = document.querySelector('[data-amber-artifact-live]');
  const storageKey = (button) => `ke_amber_artifact_prototype_v1:${button.dataset.artifactPlacement || 'unknown'}`;
  const setCollected = (button, collected) => {
    button.classList.toggle('is-collected', collected);
    button.setAttribute('aria-pressed', String(collected));
    button.setAttribute('aria-label', collected
      ? 'Артефакт «Янтарный космонавт» найден'
      : 'Секретный артефакт «Янтарный космонавт». Нажмите, чтобы найти');
  };
  for (const button of buttons) {
    try { setCollected(button, localStorage.getItem(storageKey(button)) === 'found'); } catch (_) { setCollected(button, false); }
    button.addEventListener('click', (event) => {
      event.preventDefault();
      event.stopPropagation();
      if (button.classList.contains('is-collected')) return;
      button.classList.add('is-collecting');
      setCollected(button, true);
      try { localStorage.setItem(storageKey(button), 'found'); } catch (_) {}
      if (live) live.textContent = 'Найден артефакт «Янтарный космонавт»';
      window.dispatchEvent(new CustomEvent('kenigevents:artifact-collected', { detail: {
        artifactId: 'amber_cosmonaut',
        placement: button.dataset.artifactPlacement || 'unknown',
        eventId: button.closest('.event-row')?.dataset.event || '',
      }}));
      setTimeout(() => button.classList.remove('is-collecting'), reduced ? 0 : 460);
    });
  }
  if (reduced || !('IntersectionObserver' in window)) {
    buttons.forEach((button) => button.classList.add('is-awake'));
    return;
  }
  const observer = new IntersectionObserver((entries) => {
    for (const entry of entries) {
      if (!entry.isIntersecting || entry.intersectionRatio < .72) continue;
      entry.target.classList.add('is-awake');
      observer.unobserve(entry.target);
    }
  }, { threshold: [.72] });
  buttons.forEach((button) => observer.observe(button));
})();
"""


def unified_bottom_nav(base: str, current: str) -> str:
    routes = {
        "popular": ("populyarnoe", "Популярное"),
        "calendar": ("segodnya", "Даты"),
        "search": ("poisk", "Поиск"),
        "personal": ("dlya-menya", "Для меня"),
    }
    selected = "calendar" if current in {"calendar", "tomorrow", "weekend"} else current
    items = []
    for key, (slug, label) in routes.items():
        aria = ' aria-current="page"' if key == selected else ""
        items.append(
            f'<a href="{base}/{slug}/"{aria}>'
            f'<span class="nav-icon-shell">{shell.ICONS[key]}</span><span>{label}</span></a>'
        )
    return '<nav class="bottom-nav shell-bottom-nav" aria-label="Основная навигация">' + "".join(items) + "</nav>"


def render_donor_page(
    source: Path,
    *,
    build_id: str,
    current: str,
    title: str,
    meta: str,
) -> str:
    asset_root = f"/{build_id}"
    base = asset_root
    html = shell.normalize_donor_html(source.read_text(), asset_root)
    header = shell.shell_header("p", base, asset_root, current, title, meta)
    html = re.sub(r'<header class="site-header">.*?</header>', header, html, count=1, flags=re.S)
    html = re.sub(
        r'<nav class="bottom-nav".*?</nav>',
        unified_bottom_nav(base, current),
        html,
        count=1,
        flags=re.S,
    )
    html = html.replace(
        f"/{build_id}/izbrannoe/",
        f"/{build_id}/dlya-menya/#izbrannoe",
    )
    html = html.replace('<body class="', '<body class="shell-lab variant-reference4-leather-tag ', 1)
    if '<body class="' not in html:
        html = html.replace('<body', '<body class="shell-lab variant-reference4-leather-tag"', 1)
    return html


def write_page(output: Path, slug: str, html: str) -> None:
    destination = output / slug
    if destination.exists():
        shutil.rmtree(destination)
    destination.mkdir(parents=True)
    (destination / "index.html").write_text(html)


def artifact_markup(build_id: str, placement: str) -> str:
    asset = f"/{build_id}/assets/gamification/amber-cosmonaut"
    return (
        f'<button class="amber-artifact" type="button" data-amber-artifact '
        f'data-artifact-placement="{placement}" aria-pressed="false" '
        f'aria-label="Секретный артефакт «Янтарный космонавт». Нажмите, чтобы найти">'
        f'<span class="amber-artifact__rays" aria-hidden="true"></span>'
        f'<span class="amber-artifact__visual" aria-hidden="true">'
        f'<img src="{asset}-1x.webp" srcset="{asset}-1x.webp 1x, {asset}-2x.webp 2x, {asset}-3x.webp 3x" '
        f'width="74" height="96" alt="" decoding="async">'
        f'<span class="amber-artifact__shine"></span></span>'
        f'<span class="amber-artifact__found" aria-hidden="true">'
        f'<svg viewBox="0 0 20 20"><path d="m4 10 4 4 8-9"/></svg><span>Найден</span></span>'
        f'</button>'
    )


def event_row_records(page_html: str) -> list[dict[str, str]]:
    records = []
    for match in re.finditer(r'<article class="event-row" data-event="(?P<id>\d+)".*?</article>', page_html, flags=re.S):
        row = match.group(0)
        title_match = re.search(r'<span class="event-title">(.*?)</span>', row, flags=re.S)
        title = re.sub(r'<[^>]+>', '', title_match.group(1) if title_match else f'Событие {match.group("id")}')
        records.append({
            "id": match.group("id"),
            "title": html_lib.unescape(title).strip(),
            "row": row,
            "has_medallion": "event-medallion-slot" in row,
        })
    return records


def inject_artifact(page_html: str, *, build_id: str, event_id: str, placement: str) -> str:
    row_pattern = re.compile(rf'<article class="event-row" data-event="{re.escape(event_id)}".*?</article>', flags=re.S)
    match = row_pattern.search(page_html)
    if not match:
        raise RuntimeError(f"Artifact target event {event_id} is missing")
    row = match.group(0)
    artifact = artifact_markup(build_id, placement)
    if placement == "after-medallion":
        needle = '</a><button type="button" class="event-like-cta"'
        if "event-medallion-slot" not in row or needle not in row:
            raise RuntimeError(f"Event {event_id} does not support after-medallion placement")
        row = row.replace(needle, f'</a>{artifact}<button type="button" class="event-like-cta"', 1)
    elif placement == "tail":
        needle = '</button></span></div></article>'
        if not row.endswith(needle):
            raise RuntimeError(f"Event {event_id} rail tail contract changed")
        row = row[:-len(needle)] + f'</button>{artifact}</span></div></article>'
    else:
        raise ValueError(f"Unknown artifact placement: {placement}")
    page_html = page_html[:match.start()] + row + page_html[match.end():]
    page_html = page_html.replace('</head>', f'<link rel="stylesheet" href="/{build_id}/artifact.css"></head>', 1)
    page_html = page_html.replace('</body>', f'<p class="sr-only" data-amber-artifact-live aria-live="polite"></p><script src="/{build_id}/artifact.js"></script></body>', 1)
    return page_html


def build_artifact_prototypes(output: Path, build_id: str) -> list[dict[str, str]]:
    # The A/B pages must inherit the already assembled accepted v13 menu.  A
    # second render after the module-global donor was restored caused the old
    # tonal menu to leak into v27.
    source = output / ARTIFACT_SOURCE_PAGE / "index.html"
    if not source.exists():
        raise RuntimeError(f"Artifact source page is missing: {source}")
    rendered = source.read_text()
    if 'class="reference4-menu"' not in rendered or 'class="tone-service"' in rendered:
        raise RuntimeError("Artifact A/B must inherit the accepted Reference4 menu")
    records = event_row_records(rendered)
    candidates = [record for record in records if record["has_medallion"]][:12]
    if len(candidates) < 2:
        raise RuntimeError("Artifact A/B needs two rail rows with medallions")
    seed = int.from_bytes(hashlib.sha256(build_id.encode("utf-8")).digest()[:8], "big")
    rng = random.Random(seed)
    rng.shuffle(candidates)
    specs = [
        ("artifact-tail", "tail", candidates[0]),
        ("artifact-after-medallion", "after-medallion", candidates[1]),
    ]
    manifest = []
    for slug, placement, record in specs:
        variant_html = inject_artifact(
            rendered,
            build_id=build_id,
            event_id=record["id"],
            placement=placement,
        )
        write_page(output, slug, variant_html)
        manifest.append({
            "route": f"/{build_id}/{slug}/",
            "placement": placement,
            "sourcePage": f"/{build_id}/{ARTIFACT_SOURCE_PAGE}/",
            "eventId": record["id"],
            "eventTitle": record["title"],
        })
    (output / "artifact.css").write_text(ARTIFACT_CSS)
    (output / "artifact.js").write_text(ARTIFACT_JS)
    (output / "artifact-prototypes.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-id", required=True)
    parser.add_argument("--astro-output", type=Path, required=True)
    parser.add_argument("--donor", type=Path, default=DEFAULT_DONOR)
    args = parser.parse_args()

    output = args.astro_output.resolve()
    donor = args.donor.resolve()
    if not (output / "poisk" / "index.html").exists():
        raise SystemExit("Astro Search page is missing; run build:preview first")
    if not (donor / "segodnya" / "index.html").exists():
        raise SystemExit(f"Accepted calendar donor is missing: {donor}")

    # `.prerender` is an internal staging directory.  Published Astro labs are
    # retained so the standard generated-output gate can validate the assembly.
    for disposable in (output / ".prerender",):
        if disposable.exists():
            shutil.rmtree(disposable)

    temporary = output.parent / f".{args.build_id}-reference4"
    if temporary.exists():
        shutil.rmtree(temporary)

    previous_plane = shell.plane_content
    previous_variants = shell.VARIANTS
    previous_css = shell.LAB_CSS
    previous_js = shell.LAB_JS
    try:
        shell.plane_content = leather.plane_content
        shell.VARIANTS = leather.VARIANTS
        shell.LAB_CSS = previous_css + leather.REFERENCE4_CSS
        # Fullscreen v13 deliberately removes generic document-scroll close.
        scroll_close = """  document.addEventListener('scroll', () => {
    if (menu?.hasAttribute('open') && Math.abs((scrollY || 0) - openedAtY) > 24) closeMenu();
  }, { passive: true });
"""
        if scroll_close not in previous_js:
            raise RuntimeError("mobile shell close contract changed")
        shell.LAB_JS = previous_js.replace(scroll_close, "") + leather.REFERENCE4_JS

        # Copy only the shared accepted shell assets.  Calendar page bodies
        # below come directly from the v23 donor; Search remains Astro-owned.
        temporary.mkdir(parents=True)
        shutil.copytree(donor / "assets", temporary / "assets")
        for filename in ("styles.css", "app.js", "event.html"):
            shutil.copy2(donor / filename, temporary / filename)
        (temporary / "lab.css").write_text(shell.LAB_CSS)
        (temporary / "lab.js").write_text(shell.LAB_JS)
    finally:
        shell.plane_content = previous_plane
        shell.VARIANTS = previous_variants
        shell.LAB_CSS = previous_css
        shell.LAB_JS = previous_js

    shutil.copytree(temporary / "assets", output / "assets", dirs_exist_ok=True)
    for filename in ("styles.css", "app.js", "event.html", "lab.css", "lab.js"):
        shutil.copy2(temporary / filename, output / filename)

    # Assets required by the accepted expanded menu and service-share action.
    shutil.copytree(ROOT / "site/public/assets/icons/reference4-v8", output / "assets/icons/reference4-v8", dirs_exist_ok=True)
    shutil.copytree(ROOT / "site/public/service-share", output / "service-share", dirs_exist_ok=True)
    (output / "assets/service-share").mkdir(parents=True, exist_ok=True)
    shutil.copy2(ROOT / "site/src/lib/service-share/controller.js", output / "assets/service-share/controller.js")
    (output / "assets/ui").mkdir(parents=True, exist_ok=True)
    for filename in ("reference4-leather-close-v8.webp", "mobile-head-skinny-leather-3x.webp"):
        shutil.copy2(ROOT / f"site/public/assets/ui/{filename}", output / f"assets/ui/{filename}")

    # The module globals must point at v13 while pages are rendered.
    previous_plane = shell.plane_content
    try:
        shell.plane_content = leather.plane_content
        page_specs = {
            "segodnya": ("calendar", "Сегодня", "события дня · Вся область"),
            "zavtra": ("tomorrow", "Завтра", "события следующего дня"),
            "vyhodnye": ("weekend", "Выходные", "суббота и воскресенье"),
            "populyarnoe": ("popular", "Популярное", "по категориям · Вся область"),
            "dlya-menya": ("personal", "Для меня", "личная лента"),
        }
        for slug, (current, title, meta) in page_specs.items():
            source = donor / slug / "index.html"
            if source.exists():
                write_page(output, slug, render_donor_page(source, build_id=args.build_id, current=current, title=title, meta=meta))

        # Preserve next-date/weekend navigation instead of falling through to
        # stale Astro templates or 404s.
        for source in sorted(donor.glob("date-*/index.html")) + sorted(donor.glob("vyhodnye-*/index.html")):
            slug = source.parent.name
            title = "Выходные" if slug.startswith("vyhodnye-") else "Дата"
            write_page(output, slug, render_donor_page(source, build_id=args.build_id, current="calendar", title=title, meta="календарь событий"))
    finally:
        shell.plane_content = previous_plane

    artifact_manifest = build_artifact_prototypes(output, args.build_id)

    if temporary.exists():
        shutil.rmtree(temporary)

    marker = output / "unified-mobile-preview.json"
    marker.write_text(
        '{\n'
        f'  "buildId": "{args.build_id}",\n'
        '  "search": "Astro AuthorizedEventSearch + canonical EventCard",\n'
        '  "calendar": "accepted v23 donor",\n'
        '  "shell": "reference-4 leather tag v13",\n'
        '  "artifactLab": "two isolated amber-cosmonaut rail placements"\n'
        '}\n'
    )
    print(f"Unified mobile preview assembled at {output}")
    for item in artifact_manifest:
        print(f"Artifact {item['placement']}: {item['eventId']} · {item['eventTitle']} · {item['route']}")


if __name__ == "__main__":
    main()
