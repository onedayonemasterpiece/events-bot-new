#!/usr/bin/env python3
"""Build factual G/H/I mobile navigation prototypes from repository routes.

Unlike the earlier conceptual labs, every live-looking label comes from the
current static-site drawer/footer or the explicit first-release contract.
Festivals are rendered only as a disabled roadmap slot because `/festivali/`
does not exist yet.
"""

from __future__ import annotations

import argparse
import html
import shutil
from pathlib import Path

import build_mobile_shell_unification_lab as core


BUILD_ID_DEFAULT = "preview-20260721-mobile-shell-factual-nav-lab-v4"

VARIANTS = {
    "g": {
        "name": "Текущий сайт",
        "subtitle": "фактический drawer сегодня, с Главной первым пунктом",
        "body_class": "variant-factual-current",
    },
    "h": {
        "name": "Первый релиз",
        "subtitle": "текущие маршруты плюс обязательные аккаунт и Моё избранное",
        "body_class": "variant-factual-release",
    },
    "i": {
        "name": "Релиз + фестивали",
        "subtitle": "релизная навигация с честным неактивным местом под фестивали",
        "body_class": "variant-factual-roadmap",
    },
}

PAGE_SLUGS = {
    "home": "",
    "calendar": "segodnya",
    "tomorrow": "zavtra",
    "weekend": "vyhodnye",
    "exhibitions": "vystavki",
    "popular": "populyarnoe",
    "clubs": "kluby-po-interesam",
    "search": "poisk",
    "personal": "dlya-menya",
    "favorites": "izbrannoe",
    "partners": "partners",
    "partnership": "partnerstvo",
}


def href(base: str, page: str) -> str:
    slug = PAGE_SLUGS[page]
    return f"{base}/{slug}/" if slug else f"{base}/"


def active(current: str, page: str) -> str:
    return ' aria-current="page"' if current == page else ""


def nav_link(base: str, current: str, page: str, label: str, class_name: str = "") -> str:
    class_attr = f' class="{class_name}"' if class_name else ""
    return f'<a{class_attr} href="{href(base, page)}"{active(current, page)}>{html.escape(label)}</a>'


def plane_content(variant: str, base: str, current: str) -> str:
    home = nav_link(base, current, "home", "Главная", "factual-home-link")
    dates = "".join(
        [
            nav_link(base, current, "calendar", "Сегодня"),
            nav_link(base, current, "tomorrow", "Завтра"),
            nav_link(base, current, "weekend", "Выходные"),
        ]
    )
    if variant == "g":
        return f"""
          <nav class="factual-row factual-row--home" aria-label="Главная">{home}</nav>
          <nav class="factual-row factual-row--three" aria-label="По датам">{dates}</nav>
          <nav class="factual-row factual-row--three" aria-label="Разделы афиши">
            {nav_link(base, current, 'exhibitions', 'Выставки')}{nav_link(base, current, 'popular', 'Популярное')}{nav_link(base, current, 'clubs', 'Клубы')}
          </nav>
          <nav class="factual-row factual-row--three factual-row--secondary" aria-label="Другие разделы">
            {nav_link(base, current, 'search', 'Поиск')}{nav_link(base, current, 'personal', 'Для меня')}{nav_link(base, current, 'partners', 'Инфопартнёры')}
          </nav>
        """

    release_service = """
      <div class="factual-service">
        <button type="button"><span>Аккаунт</span><b>Войти</b></button>
        <button type="button"><span>Сервис</span><b>Поделиться</b></button>
      </div>
    """
    release_main = f"""
      <nav class="factual-row factual-row--two factual-row--release-main" aria-label="Главная и сохранённые события">
        {home}{nav_link(base, current, 'favorites', 'Моё избранное')}
      </nav>
    """
    if variant == "h":
        return release_service + release_main + f"""
          <nav class="factual-row factual-row--three" aria-label="По датам">{dates}</nav>
          <nav class="factual-row factual-row--three" aria-label="Разделы афиши">
            {nav_link(base, current, 'exhibitions', 'Выставки')}{nav_link(base, current, 'popular', 'Популярное')}{nav_link(base, current, 'clubs', 'Клубы')}
          </nav>
          <nav class="factual-row factual-row--three factual-row--secondary" aria-label="Другие разделы">
            {nav_link(base, current, 'search', 'Поиск')}{nav_link(base, current, 'personal', 'Для меня')}{nav_link(base, current, 'partners', 'Инфопартнёры')}
          </nav>
        """

    return release_service + release_main + f"""
      <nav class="factual-row factual-row--three" aria-label="По датам">{dates}</nav>
      <nav class="factual-row factual-row--three factual-row--roadmap" aria-label="Тематические разделы">
        {nav_link(base, current, 'exhibitions', 'Выставки')}{nav_link(base, current, 'clubs', 'Клубы')}<span class="factual-disabled" aria-disabled="true"><b>Фестивали</b><small>позже</small></span>
      </nav>
      <nav class="factual-row factual-row--two factual-row--secondary factual-row--compact" aria-label="Другие разделы">
        {nav_link(base, current, 'popular', 'Популярное')}{nav_link(base, current, 'partners', 'Инфопартнёры')}
      </nav>
    """


def bottom_nav(base: str, current: str) -> str:
    section = {
        "popular": "afisha",
        "exhibitions": "afisha",
        "clubs": "afisha",
        "calendar": "dates",
        "tomorrow": "dates",
        "weekend": "dates",
        "search": "search",
        "personal": "personal",
        "favorites": "personal",
    }.get(current)
    items = [
        ("afisha", "Афиша", "popular"),
        ("dates", "Даты", "calendar"),
        ("search", "Поиск", "search"),
        ("personal", "Для меня", "personal"),
    ]
    rendered = []
    for key, label, page in items:
        aria = ' aria-current="page"' if section == key else ""
        rendered.append(
            f'<a href="{href(base, page)}"{aria}><span class="nav-icon-shell">{core.ICONS["popular" if key == "afisha" else key if key != "dates" else "calendar"]}</span><span>{label}</span></a>'
        )
    return '<nav class="bottom-nav shell-bottom-nav" aria-label="Основная навигация">' + "".join(rendered) + "</nav>"


def factual_footer(base: str) -> str:
    return f"""<footer class="mobile-micro-footer factual-footer" id="utility">
      <a href="{href(base, 'partners')}">Инфопартнёры</a>
      <a href="{href(base, 'partnership')}">Информационное партнёрство</a>
      <a href="mailto:info@kenigevents.ru?subject=Правообладателям">Правообладателям</a>
    </footer>"""


def simple_page(donor: Path, variant: str, base: str, asset_root: str, page: str, title: str, meta: str) -> str:
    info = VARIANTS[variant]
    if page == "home":
        intro = "Главная — самостоятельная точка входа сайта, а не синоним Популярного."
        section_title = "Сегодня в афише"
    elif page == "favorites":
        intro = "Release-target shell списка сохранённых событий; счётчик появляется только при N > 0."
        section_title = "Сохранённые события"
    else:
        intro = "Маршрут существует в фактическом проекте; в этом lab проверяется общая навигационная оболочка."
        section_title = "События и материалы раздела"
    main = f"""
      <main class="factual-specimen">
        <section class="page-head"><span class="variant-kicker">Вариант {variant.upper()} · {info['name']}</span><h1>{html.escape(title)}</h1><p>{html.escape(intro)}</p></section>
        <div class="feed-head"><div class="feed-head__copy"><strong>{html.escape(section_title)}</strong><span>{html.escape(meta)}</span></div></div>
        <section class="event-list">{core.sample_rows(donor, asset_root, 4)}</section>
        {factual_footer(base)}
      </main>
    """
    header = core.shell_header(variant, base, asset_root, page, title, meta)
    return f'''<!doctype html><html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><meta name="robots" content="noindex,nofollow,noarchive"><meta name="theme-color" content="#fbf7ef"><title>{html.escape(title)} · {html.escape(info["name"])}</title><link rel="stylesheet" href="{asset_root}/styles.css?v=23"><link rel="stylesheet" href="{asset_root}/lab.css?v=1"></head><body class="shell-lab {info["body_class"]}" data-page="{page}">{header}{main}{bottom_nav(base, page)}<div data-city-sheet hidden aria-hidden="true"></div><div class="confirm-sheet" hidden></div><div class="toast" role="status" aria-live="polite" hidden><span></span><button>Отменить</button></div><script src="{asset_root}/app.js?v=23"></script><script src="{asset_root}/lab.js?v=1"></script></body></html>'''


FACTUAL_CSS = r'''
/* v4 factual navigation: labels/routes derive from current code and release plan. */
.variant-factual-current .mobile-discovery-menu{--plane-h:168px}
.variant-factual-release .mobile-discovery-menu,.variant-factual-roadmap .mobile-discovery-menu{--plane-h:208px}
.factual-row{box-sizing:border-box;height:42px;display:grid;align-items:stretch;padding:0 14px;border-bottom:1px solid var(--hairline)}
.factual-row--home{grid-template-columns:1fr}.factual-row--two{grid-template-columns:repeat(2,minmax(0,1fr))}.factual-row--three{grid-template-columns:repeat(3,minmax(0,1fr))}
.factual-row>*{box-sizing:border-box;min-width:0;display:flex;align-items:center;border:0;border-radius:0;background:transparent;color:inherit;text-decoration:none;font-size:14px;line-height:16px;font-weight:740;font-family:inherit}
.factual-row>*+*{border-left:1px solid var(--hairline);padding-left:12px}
.factual-row--home .factual-home-link{font-size:18px;line-height:21px;font-weight:850;color:var(--accent)}
.factual-row--secondary>*{font-size:10.5px;line-height:13px;color:var(--text-sec)}
.factual-row--compact{height:40px;border-bottom:0}
.factual-row--release-main .factual-home-link{font-size:16px;font-weight:850;color:var(--accent)}
.factual-service{box-sizing:border-box;height:40px;display:grid;grid-template-columns:1fr 122px;padding:0 14px;background:var(--plane-alt);border-bottom:1px solid var(--hairline)}
.factual-service button{box-sizing:border-box;min-width:0;display:flex;flex-direction:column;align-items:flex-start;justify-content:center;gap:0;padding:0;border:0;border-radius:0;background:transparent;color:inherit;text-align:left;font-family:inherit}
.factual-service button+button{border-left:1px solid var(--hairline);padding-left:12px}
.factual-service span{font-size:8px;line-height:10px;font-weight:800;text-transform:uppercase;letter-spacing:.07em;color:var(--text-sec)}
.factual-service b{font-size:11px;line-height:14px;font-weight:750}
.factual-disabled{flex-direction:column;align-items:flex-start;justify-content:center;color:#9c9188!important;cursor:not-allowed}
.factual-disabled b{font-size:12px;line-height:14px}.factual-disabled small{font-size:8px;line-height:9px;font-weight:760;text-transform:uppercase;letter-spacing:.07em;color:var(--accent)}
.factual-row a[aria-current="page"]{position:relative;color:var(--accent)}
.factual-row a[aria-current="page"]::after{content:"";position:absolute;left:0;right:12px;bottom:0;height:2px;background:var(--accent)}
.factual-row a:focus-visible,.factual-service button:focus-visible{outline:2px solid var(--accent);outline-offset:-3px}
.factual-specimen{max-width:390px;margin:0 auto;padding-top:96px;padding-bottom:96px}.factual-specimen .page-head{padding-bottom:16px}.factual-specimen>.event-list{padding-top:0}.factual-specimen>.event-list .event-row{margin-top:8px}
.factual-footer{margin-bottom:22px}.factual-footer a{max-width:100%}
@media(max-width:350px){
  .factual-row{padding-inline:12px}.factual-row>*{font-size:12px}.factual-row>*+*{padding-left:8px}.factual-row--secondary>*{font-size:9px}.factual-row--home .factual-home-link{font-size:17px}.factual-row--release-main .factual-home-link{font-size:14px}.factual-row--release-main>*{font-size:11px}
  .factual-service{grid-template-columns:1fr 116px;padding-inline:12px}.factual-service button+button{padding-left:8px}.factual-service span{font-size:7px}.factual-service b{font-size:10px}
  .factual-disabled b{font-size:10px}.factual-disabled small{font-size:7px}
}
'''


def build(output: Path, donor: Path, build_id: str) -> None:
    if not donor.exists():
        raise SystemExit(f"Missing accepted v23 donor: {donor}")
    if output.exists():
        shutil.rmtree(output)
    output.mkdir(parents=True)
    shutil.copytree(donor / "assets", output / "assets")
    for filename in ("styles.css", "app.js", "event.html"):
        shutil.copy2(donor / filename, output / filename)

    core.VARIANTS = VARIANTS
    core.plane_content = plane_content
    core.bottom_nav = bottom_nav
    previous_css = core.LAB_CSS
    core.LAB_CSS = previous_css + FACTUAL_CSS
    try:
        (output / "lab.css").write_text(core.LAB_CSS)
        (output / "lab.js").write_text(core.LAB_JS)
        (output / "index.html").write_text(core.root_index(build_id))
        (output / "__preview").mkdir()
        (output / "__preview" / "index.html").write_text(core.root_index(build_id))
        asset_root = f"/{build_id}"
        for variant in VARIANTS:
            variant_dir = output / f"variant-{variant}"
            variant_dir.mkdir()
            base = f"{asset_root}/variant-{variant}"
            (variant_dir / "index.html").write_text(simple_page(donor, variant, base, asset_root, "home", "Главная", "вся афиша Калининграда и области"))

            pages = {
                "calendar": ("Сегодня", "6 событий · Вся область"),
                "tomorrow": ("Завтра", "события следующего дня"),
                "weekend": ("Выходные", "суббота и воскресенье"),
                "exhibitions": ("Выставки", "текущие и будущие выставки"),
                "popular": ("Популярное", "по категориям · Вся область"),
                "clubs": ("Клубы", "клубы по интересам"),
                "search": ("Поиск", "смысловой · по всей области"),
                "personal": ("Для меня", "личная лента"),
                "favorites": ("Моё избранное", "сохранённые события"),
                "partners": ("Инфопартнёры", "информационные партнёры проекта"),
                "partnership": ("Информационное партнёрство", "условия и контакты"),
            }
            for page, (title, meta) in pages.items():
                page_dir = variant_dir / PAGE_SLUGS[page]
                page_dir.mkdir()
                if page in {"calendar", "popular"}:
                    rendered = core.donor_page(donor, variant, base, asset_root, page)
                    rendered = rendered.replace(core.endcap(variant, base, page), factual_footer(base))
                elif page in {"search", "personal"}:
                    rendered = core.custom_page(donor, variant, base, asset_root, page)
                    old_endcap = core.endcap(variant, base, page)
                    rendered = rendered.replace(old_endcap, factual_footer(base))
                else:
                    rendered = simple_page(donor, variant, base, asset_root, page, title, meta)
                (page_dir / "index.html").write_text(rendered)
    finally:
        core.LAB_CSS = previous_css
    (output / "robots.txt").write_text("User-agent: *\nDisallow: /\n")
    print(f"Built {output}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--donor", type=Path, default=core.DONOR_DEFAULT)
    parser.add_argument("--build-id", default=BUILD_ID_DEFAULT)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    output = args.output or root / "site" / "dist" / args.build_id
    build(output, args.donor, args.build_id)


if __name__ == "__main__":
    main()
