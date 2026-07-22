#!/usr/bin/env python3
"""Build the exact expanded-menu adaptation of mobile menu reference (4).

Unlike the rejected v6, this builder does not replace or restyle the accepted
closed KenigEvents brand tag.  The supplied reference is used as the contract
for the expanded glass navigation layer only.
"""

from __future__ import annotations

import argparse
import html
import shutil
from pathlib import Path
from urllib.parse import quote_plus

import build_mobile_shell_factual_nav_lab as factual


BUILD_ID_DEFAULT = "preview-20260722-mobile-menu-reference4-exact-lab-v7"

VARIANTS = {
    "n": {
        "name": "Стеклянное меню reference 4",
        "subtitle": "точная адаптация раскрытого состояния; стандартная бирка не изменена",
        "body_class": "variant-reference4-exact",
    },
}

ICON_FILES = {
    "free": "euro-circle.svg",
    "free_slash": "slash.svg",
    "children": "teddy-bear.svg",
    "exhibitions": "palette.svg",
    "festivals": "party-horn.svg",
    "popular": "dazzling-star.svg",
    "partners": "handshake.svg",
    "search": "search.svg",
    "personal": "message-circle-heart.svg",
    "favorite": "heart.svg",
    "share": "share-nodes.svg",
}


def asset_root_from_base(base: str) -> str:
    return base.rsplit("/variant-", 1)[0]


def icon(asset_root: str, name: str, *, composite: bool = False) -> str:
    root = f"{asset_root}/assets/icons/reference4"
    if composite:
        return (
            '<span class="reference4-icon reference4-icon--composite" aria-hidden="true">'
            f'<span style="--reference4-icon:url(\'{root}/{ICON_FILES["free"]}\')"></span>'
            f'<span class="reference4-icon__slash" style="--reference4-icon:url(\'{root}/{ICON_FILES["free_slash"]}\')"></span>'
            "</span>"
        )
    return (
        '<span class="reference4-icon" aria-hidden="true">'
        f'<span style="--reference4-icon:url(\'{root}/{ICON_FILES[name]}\')"></span>'
        "</span>"
    )


def row(href: str, label: str, icon_html: str, current: bool = False) -> str:
    aria = ' aria-current="page"' if current else ""
    return (
        f'<a class="reference4-menu__row" href="{html.escape(href, quote=True)}"{aria}>'
        f"{icon_html}<span>{html.escape(label)}</span><i aria-hidden=\"true\"></i></a>"
    )


def plane_content(variant: str, base: str, current: str) -> str:
    asset_root = asset_root_from_base(base)
    wordmark = f"{asset_root}/assets/v2/brand/announcements-wordmark-ui.svg"
    leather = f"{asset_root}/assets/ui/reference4-leather-close.webp"
    search_base = factual.href(base, "search")

    def query(value: str) -> str:
        return f"{search_base}?q={quote_plus(value)}"

    dates = [
        ("calendar", "Сегодня"),
        ("tomorrow", "Завтра"),
        ("weekend", "Выходные"),
    ]
    date_links = "".join(
        f'<a href="{factual.href(base, page)}"{factual.active(current, page)}>{label}</a>'
        for page, label in dates
    )

    rows = "".join(
        [
            row(query("бесплатные события"), "Бесплатно", icon(asset_root, "free", composite=True)),
            row(query("события для детей"), "Детям", icon(asset_root, "children")),
            row(factual.href(base, "exhibitions"), "Выставки", icon(asset_root, "exhibitions"), current == "exhibitions"),
            row(query("фестивали"), "Фестивали", icon(asset_root, "festivals")),
            row(factual.href(base, "popular"), "Популярное", icon(asset_root, "popular"), current == "popular"),
            row(factual.href(base, "partners"), "Партнёры", icon(asset_root, "partners"), current == "partners"),
            row(factual.href(base, "search"), "Поиск", icon(asset_root, "search"), current == "search"),
            row(factual.href(base, "personal"), "Для меня", icon(asset_root, "personal"), current == "personal"),
        ]
    )

    return f"""
      <div class="reference4-menu" aria-label="Глобальная навигация">
        <button class="reference4-menu__close" type="button" aria-label="Закрыть навигацию" data-reference4-close>
          <img src="{leather}" width="333" height="376" alt="">
        </button>
        <a class="reference4-menu__brand" href="{factual.href(base, 'home')}" aria-label="Главная — Анонсы"{factual.active(current, 'home')}>
          <svg viewBox="0 0 7819 1514" aria-hidden="true" focusable="false"><use href="{wordmark}#announcements-wordmark-ui"></use></svg>
        </a>
        <nav class="reference4-menu__dates" aria-label="Быстрый выбор даты">{date_links}</nav>
        <nav class="reference4-menu__list" aria-label="Разделы афиши">{rows}</nav>
        <section class="reference4-menu__utility" aria-label="Аккаунт и сервис">
          <div class="reference4-menu__account-row">
            <button type="button" data-reference4-login>Войти</button>
            <a href="{factual.href(base, 'favorites')}"{factual.active(current, 'favorites')}>{icon(asset_root, 'favorite')}<span>Избранное</span></a>
          </div>
          <button class="reference4-menu__share" type="button" data-reference4-share><span>Поделиться</span>{icon(asset_root, 'share')}</button>
        </section>
      </div>
    """


REFERENCE4_CSS = r'''
/* v7: supplied reference governs expanded state only; closed summary is core. */
.variant-reference4-exact .mobile-discovery-menu{--plane-h:calc(100dvh - 76px - env(safe-area-inset-bottom));height:100dvh;transform:translate3d(0,calc(-100dvh + 76px + env(safe-area-inset-bottom) - env(safe-area-inset-top)),0)}
.variant-reference4-exact .mobile-discovery-menu[open]{transform:translate3d(0,0,0)}
.variant-reference4-exact .mobile-discovery-menu.is-closing{transform:translate3d(0,calc(-100dvh + 76px + env(safe-area-inset-bottom) - env(safe-area-inset-top)),0)}
.variant-reference4-exact .mobile-discovery-menu__panel{height:calc(100dvh - 76px - env(safe-area-inset-bottom));padding:0;overflow-x:hidden;overflow-y:auto;overscroll-behavior:contain;-webkit-overflow-scrolling:touch;border:0;border-radius:0 0 28px 28px;background:linear-gradient(180deg,rgba(76,35,21,.42) 0,rgba(244,235,226,.54) 148px,rgba(250,247,242,.70) 100%);box-shadow:0 20px 46px rgba(46,25,16,.28),inset 0 -1px 0 rgba(255,255,255,.76);backdrop-filter:blur(25px) saturate(.90);-webkit-backdrop-filter:blur(25px) saturate(.90)}
.variant-reference4-exact .mobile-discovery-menu[open]>.mobile-discovery-menu__summary,.variant-reference4-exact .mobile-discovery-menu.is-closing>.mobile-discovery-menu__summary{opacity:0;visibility:hidden;pointer-events:none}
.reference4-menu{box-sizing:border-box;position:relative;z-index:1;width:100%;min-height:100%;padding:0 14px 18px;font-family:inherit;color:#241b16}
.reference4-menu__close{position:sticky;z-index:5;top:0;margin-left:auto;margin-bottom:-126px;box-sizing:border-box;width:112px;height:126px;padding:0;display:block;overflow:hidden;border:0;border-radius:3px 3px 18px 18px;background:transparent;filter:drop-shadow(0 8px 9px rgba(42,22,15,.20)) drop-shadow(0 18px 28px rgba(42,22,15,.16));cursor:pointer;touch-action:manipulation}
.reference4-menu__close img{display:block;width:112px;height:126px;object-fit:fill}
.reference4-menu__close:focus-visible{outline:3px solid #fffaf2;outline-offset:3px}
.reference4-menu__brand{box-sizing:border-box;width:168px;height:calc(100px + env(safe-area-inset-top));display:flex;align-items:center;padding:calc(37px + env(safe-area-inset-top)) 0 9px;color:#fffaf2;filter:drop-shadow(0 2px 6px rgba(43,23,15,.34));text-decoration:none}
.reference4-menu__brand svg{display:block;width:164px;height:auto;fill:currentColor}
.reference4-menu__brand:focus-visible{outline:3px solid #fffaf2;outline-offset:3px;border-radius:8px}
.reference4-menu__dates{box-sizing:border-box;height:58px;display:grid;grid-template-columns:1fr 1fr 1.34fr;align-items:center;gap:8px;margin:10px 0 12px}
.reference4-menu__dates a{box-sizing:border-box;min-width:0;height:46px;padding:0 11px;display:flex;align-items:center;justify-content:center;border:1.5px solid rgba(255,255,255,.78);border-radius:24px;background:rgba(255,255,255,.13);box-shadow:inset 0 1px 0 rgba(255,255,255,.38);color:#251c17;text-decoration:none;font-size:14px;line-height:18px;font-weight:720;white-space:nowrap;backdrop-filter:blur(7px);-webkit-backdrop-filter:blur(7px)}
.reference4-menu__dates a[aria-current="page"]{border-color:rgba(255,220,207,.94);background:rgba(255,214,198,.84);color:#8d351c;box-shadow:0 6px 17px rgba(151,62,30,.22),inset 0 1px 0 rgba(255,255,255,.78)}
.reference4-menu__dates a:focus-visible{outline:3px solid #98401f;outline-offset:2px}
.reference4-menu__list{box-sizing:border-box;overflow:hidden;border:1px solid rgba(255,255,255,.68);border-radius:22px;background:rgba(255,253,249,.61);box-shadow:0 12px 30px rgba(57,36,25,.16),inset 0 1px 0 rgba(255,255,255,.82);backdrop-filter:blur(15px) saturate(.92);-webkit-backdrop-filter:blur(15px) saturate(.92)}
.reference4-menu__row{box-sizing:border-box;position:relative;width:100%;height:52px;display:grid;grid-template-columns:38px 1fr 18px;align-items:center;gap:9px;padding:0 16px;color:#261d18;text-decoration:none;font-size:19px;line-height:23px;font-weight:560;letter-spacing:-.015em}
.reference4-menu__row+.reference4-menu__row::before{content:"";position:absolute;left:63px;right:16px;top:0;height:1px;background:rgba(114,93,80,.18)}
.reference4-menu__row[aria-current="page"]{background:rgba(255,236,225,.52);color:#8f351c}
.reference4-menu__row>i{width:9px;height:9px;border-top:1.5px solid currentColor;border-right:1.5px solid currentColor;transform:rotate(45deg);justify-self:end}
.reference4-menu__row:focus-visible{outline:3px solid #98401f;outline-offset:-3px}
.reference4-icon{position:relative;width:29px;height:29px;display:block;flex:0 0 29px;color:currentColor}
.reference4-icon>span{position:absolute;inset:0;display:block;background:currentColor;-webkit-mask:var(--reference4-icon) center/contain no-repeat;mask:var(--reference4-icon) center/contain no-repeat}
.reference4-icon--composite .reference4-icon__slash{inset:-1px;background:#261d18;transform:scale(.82)}
.reference4-menu__utility{box-sizing:border-box;overflow:hidden;margin-top:14px;border:1px solid rgba(255,255,255,.68);border-radius:22px;background:rgba(255,253,249,.58);box-shadow:0 10px 28px rgba(57,36,25,.14),inset 0 1px 0 rgba(255,255,255,.8);backdrop-filter:blur(15px);-webkit-backdrop-filter:blur(15px)}
.reference4-menu__account-row{box-sizing:border-box;height:56px;display:grid;grid-template-columns:.8fr 1.2fr;align-items:center;padding:6px 9px;border-bottom:1px solid rgba(114,93,80,.18);gap:8px}
.reference4-menu__account-row>*{box-sizing:border-box;height:44px;min-width:0;display:flex;align-items:center;justify-content:center;gap:7px;padding:0 12px;border:0;border-radius:23px;background:rgba(255,253,249,.84);box-shadow:0 5px 15px rgba(64,40,28,.11);color:#2a211c;text-decoration:none;font-family:inherit;font-size:14px;line-height:17px;font-weight:720}
.reference4-menu__account-row>a[aria-current="page"]{background:#3b2720;color:#fffaf2}
.reference4-menu__account-row .reference4-icon{width:20px;height:20px;flex-basis:20px}
.reference4-menu__share{box-sizing:border-box;width:100%;height:50px;display:flex;align-items:center;justify-content:space-between;padding:0 20px;border:0;background:rgba(255,255,255,.34);color:#2a211c;font-family:inherit;font-size:16px;line-height:20px;font-weight:600;text-align:left}
.reference4-menu__share .reference4-icon{width:25px;height:25px;flex-basis:25px}
.reference4-menu__account-row>*:focus-visible,.reference4-menu__share:focus-visible{outline:3px solid #98401f;outline-offset:-3px}
.shell-menu-open .shell-bottom-nav{filter:brightness(.67) saturate(.58);pointer-events:none;transition:filter 220ms ease}
.variant-reference4-exact .factual-specimen{padding-top:96px}
@supports not ((backdrop-filter:blur(1px)) or (-webkit-backdrop-filter:blur(1px))){.variant-reference4-exact .mobile-discovery-menu__panel{background:rgba(239,228,217,.94)}.reference4-menu__list,.reference4-menu__utility{background:rgba(255,252,247,.95)}}
@media(max-width:350px){
  .reference4-menu{padding-inline:10px}.reference4-menu__close{width:104px;height:117px;margin-bottom:-117px}.reference4-menu__close img{width:104px;height:117px}.reference4-menu__brand{width:150px}.reference4-menu__brand svg{width:146px}.reference4-menu__dates{gap:6px}.reference4-menu__dates a{padding-inline:8px;font-size:12.5px}.reference4-menu__row{grid-template-columns:34px 1fr 17px;gap:8px;padding-inline:14px;font-size:18px}.reference4-menu__row+.reference4-menu__row::before{left:56px;right:14px}
}
@media(prefers-reduced-motion:reduce){.shell-menu-open .shell-bottom-nav{transition:none}}
'''


REFERENCE4_JS = r'''
(() => {
  document.addEventListener('DOMContentLoaded', () => {
    const menu = document.querySelector('[data-mobile-discovery-menu]');
    const summary = menu?.querySelector(':scope > summary');
    const close = menu?.querySelector('[data-reference4-close]');
    close?.addEventListener('click', () => {
      summary?.click();
      setTimeout(() => summary?.focus({ preventScroll: true }), matchMedia('(prefers-reduced-motion: reduce)').matches ? 0 : 360);
    });
    menu?.addEventListener('toggle', () => {
      if (menu.open) requestAnimationFrame(() => close?.focus({ preventScroll: true }));
    });
    menu?.querySelector('[data-reference4-share]')?.addEventListener('click', async () => {
      const payload = { title: 'Анонсы', text: 'Афиша Калининграда и области', url: location.origin + '/' };
      try {
        if (navigator.share) await navigator.share(payload);
        else await navigator.clipboard.writeText(payload.url);
      } catch (error) {
        if (error?.name !== 'AbortError') console.warn('Share unavailable', error);
      }
    });
  });
})();
'''


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--donor", type=Path, default=factual.core.DONOR_DEFAULT)
    parser.add_argument("--build-id", default=BUILD_ID_DEFAULT)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    output = args.output or root / "site" / "dist" / args.build_id

    previous_js = factual.core.LAB_JS
    factual.VARIANTS = VARIANTS
    factual.plane_content = plane_content
    factual.FACTUAL_CSS = REFERENCE4_CSS
    factual.BUILD_ID_DEFAULT = BUILD_ID_DEFAULT
    factual.core.LAB_JS = previous_js + REFERENCE4_JS
    try:
        factual.build(output, args.donor, args.build_id)
    finally:
        factual.core.LAB_JS = previous_js

    project_icons = root / "site" / "public" / "assets" / "icons" / "reference4"
    project_ui = root / "site" / "public" / "assets" / "ui"
    shutil.copytree(project_icons, output / "assets" / "icons" / "reference4", dirs_exist_ok=True)
    (output / "assets" / "ui").mkdir(parents=True, exist_ok=True)
    for filename in ("reference4-leather-close.webp", "reference4-leather-close.metadata.json"):
        shutil.copy2(project_ui / filename, output / "assets" / "ui" / filename)
    print(f"Added exact reference assets to {output}")


if __name__ == "__main__":
    main()
