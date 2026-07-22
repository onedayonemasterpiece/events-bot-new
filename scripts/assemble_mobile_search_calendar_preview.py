#!/usr/bin/env python3
"""Compose the accepted v13 calendar shell with the real Astro Search build.

This is deliberately an assembly step, not a third renderer.  The Astro build
keeps the production Search runtime and canonical EventCard templates.  The
accepted v23 calendar HTML, rails, crop and gestures are copied from their
donor and receive the byte-for-byte v13 expanded-menu CSS/JS and markup.
"""

from __future__ import annotations

import argparse
import re
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import build_mobile_menu_reference4_leather_tag_lab as leather  # noqa: E402
import build_mobile_shell_unification_lab as shell  # noqa: E402


DEFAULT_DONOR = shell.DONOR_DEFAULT


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

    if temporary.exists():
        shutil.rmtree(temporary)

    marker = output / "unified-mobile-preview.json"
    marker.write_text(
        '{\n'
        f'  "buildId": "{args.build_id}",\n'
        '  "search": "Astro AuthorizedEventSearch + canonical EventCard",\n'
        '  "calendar": "accepted v23 donor",\n'
        '  "shell": "reference-4 leather tag v13"\n'
        '}\n'
    )
    print(f"Unified mobile preview assembled at {output}")


if __name__ == "__main__":
    main()
