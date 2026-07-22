#!/usr/bin/env python3
"""Build the full-viewport, high-transparency reference-4 menu prototype.

The accepted closed KenigEvents brand tag is deliberately inherited byte for
byte from the mobile-shell donor.  This builder changes the expanded plane
only: compact canonical lockup, a tighter source-faithful leather crop, a
single-compositor glass layer and a coherent Phosphor Thin navigation set.
"""

from __future__ import annotations

import argparse
import html
import shutil
from pathlib import Path
from urllib.parse import quote_plus

import build_mobile_shell_factual_nav_lab as factual


BUILD_ID_DEFAULT = "preview-20260722-mobile-menu-reference4-fullglass-lab-v8"

VARIANTS = {
    "p": {
        "name": "Полноэкранное стекло reference 4",
        "subtitle": "тонкие пиктограммы; нижняя навигация перекрыта; Share видна сразу",
        "body_class": "variant-reference4-fullglass",
    },
}

ICON_FILES = {
    "children": "baby-thin.svg",
    "exhibitions": "palette-thin.svg",
    "festivals": "buildings-thin.svg",
    "popular": "popular-trend-up-thin.svg",
    "partners": "handshake-thin.svg",
    "search": "search-thin.svg",
    "personal": "user-focus-thin.svg",
    "favorite": "heart-thin.svg",
    "share": "share-network-thin.svg",
}


def asset_root_from_base(base: str) -> str:
    return base.rsplit("/variant-", 1)[0]


def icon(asset_root: str, name: str) -> str:
    root = f"{asset_root}/assets/icons/reference4-v8"
    return (
        '<span class="reference4-icon" aria-hidden="true">'
        f'<span style="--reference4-icon:url(\'{root}/{ICON_FILES[name]}\')"></span>'
        "</span>"
    )


def free_icon() -> str:
    # A typographic zero-price sign is intentional: crossed ₽ may mean that
    # rubles/cash are not accepted rather than that admission costs nothing.
    return '<span class="reference4-icon reference4-icon--free" aria-hidden="true">0 ₽</span>'


def row(href: str, label: str, icon_html: str, current: bool = False) -> str:
    aria = ' aria-current="page"' if current else ""
    return (
        f'<a class="reference4-menu__row" href="{html.escape(href, quote=True)}"{aria}>'
        f'{icon_html}<span>{html.escape(label)}</span><i aria-hidden="true"></i></a>'
    )


def plane_content(variant: str, base: str, current: str) -> str:
    asset_root = asset_root_from_base(base)
    wordmark = f"{asset_root}/assets/v2/brand/announcements-wordmark-ui.svg"
    leather = f"{asset_root}/assets/ui/reference4-leather-close-v8.webp"
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
            row(query("бесплатные события"), "Бесплатно", free_icon()),
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
          <img src="{leather}" width="333" height="332" alt="">
        </button>
        <a class="reference4-menu__brand" href="{factual.href(base, 'home')}" aria-label="Главная — Полюбить Калининград, Анонсы"{factual.active(current, 'home')}>
          <span class="reference4-menu__endorsement">Полюбить Калининград</span>
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
/* v8: full-viewport expanded plane; the inherited closed summary stays exact. */
.variant-reference4-fullglass .mobile-discovery-menu{--plane-h:100dvh;height:calc(100dvh + var(--shell-tag-h));transform:translate3d(0,calc(-100dvh - env(safe-area-inset-top)),0)}
.variant-reference4-fullglass .mobile-discovery-menu[open]{transform:translate3d(0,0,0)}
.variant-reference4-fullglass .mobile-discovery-menu.is-closing{transform:translate3d(0,calc(-100dvh - env(safe-area-inset-top)),0)}
.shell-menu-open .variant-reference4-fullglass .mobile-discovery-menu,.shell-menu-open.variant-reference4-fullglass .mobile-discovery-menu{z-index:60}
.variant-reference4-fullglass .mobile-discovery-menu__panel{height:100dvh;padding:env(safe-area-inset-top) 0 0;overflow-x:hidden;overflow-y:auto;overscroll-behavior:contain;-webkit-overflow-scrolling:touch;border:0;border-radius:0;background:linear-gradient(180deg,transparent 0 72%,rgba(255,253,247,.70) 82%,rgba(255,253,247,.98) 90% 100%),radial-gradient(120% 44% at 50% 108%,rgba(255,250,237,.74) 0,rgba(255,247,235,.32) 42%,transparent 74%),radial-gradient(62% 30% at 18% 7%,rgba(255,232,216,.34),transparent 68%),linear-gradient(180deg,rgba(103,49,31,.27) 0,rgba(244,233,224,.25) 28%,rgba(249,245,238,.18) 100%);box-shadow:inset 0 1px 0 rgba(255,255,255,.76),inset 0 -72px 90px rgba(255,250,239,.34),0 20px 46px rgba(46,25,16,.20);backdrop-filter:blur(30px) saturate(1.12) brightness(1.08);-webkit-backdrop-filter:blur(30px) saturate(1.12) brightness(1.08)}
.variant-reference4-fullglass .mobile-discovery-menu[open]>.mobile-discovery-menu__summary,.variant-reference4-fullglass .mobile-discovery-menu.is-closing>.mobile-discovery-menu__summary{opacity:0;visibility:hidden;pointer-events:none}
.reference4-menu{box-sizing:border-box;position:relative;z-index:1;width:100%;min-height:100%;padding:0 14px 16px;font-family:inherit;color:#241b16}
.reference4-menu::after{content:"";position:fixed;z-index:-1;inset:auto 0 0;height:26%;pointer-events:none;background:linear-gradient(180deg,transparent,rgba(255,251,241,.32) 45%,rgba(255,253,247,.62));mix-blend-mode:screen}
.reference4-menu__close{position:absolute;z-index:5;top:0;right:14px;box-sizing:border-box;width:112px;height:112px;padding:0;display:block;overflow:hidden;border:0;border-radius:0 0 18px 18px;background:transparent;filter:drop-shadow(0 8px 9px rgba(42,22,15,.20)) drop-shadow(0 18px 28px rgba(42,22,15,.14));cursor:pointer;touch-action:manipulation}
.reference4-menu__close img{display:block;width:112px;height:112px;object-fit:fill}
.reference4-menu__close:focus-visible{outline:3px solid #fffaf2;outline-offset:3px}
.reference4-menu__brand{box-sizing:border-box;width:174px;height:100px;display:grid;grid-template-rows:10px auto;align-content:end;gap:4px;padding:32px 0 10px;color:#fffaf2;filter:drop-shadow(0 2px 6px rgba(43,23,15,.34));text-decoration:none}
.reference4-menu__endorsement{align-self:end;font-size:7.5px;line-height:9px;font-weight:780;letter-spacing:.075em;text-transform:uppercase;white-space:nowrap;opacity:.92}
.reference4-menu__brand svg{display:block;width:164px;height:auto;fill:currentColor}
.reference4-menu__brand:focus-visible{outline:3px solid #fffaf2;outline-offset:3px;border-radius:8px}
.reference4-menu__dates{box-sizing:border-box;height:58px;display:grid;grid-template-columns:1fr 1fr 1.34fr;align-items:center;gap:8px;margin:8px 0 10px}
.reference4-menu__dates a{box-sizing:border-box;min-width:0;height:46px;padding:0 11px;display:flex;align-items:center;justify-content:center;border:1.5px solid rgba(255,255,255,.86);border-radius:24px;background:rgba(255,255,255,.09);box-shadow:inset 0 1px 0 rgba(255,255,255,.50);color:#251c17;text-decoration:none;font-size:14px;line-height:18px;font-weight:720;white-space:nowrap}
.reference4-menu__dates a[aria-current="page"]{border-color:rgba(255,226,213,.96);background:rgba(255,218,203,.72);color:#8d351c;box-shadow:0 0 24px rgba(255,187,158,.54),0 9px 22px rgba(151,62,30,.18),inset 0 1px 0 #fff}
.reference4-menu__dates a:focus-visible{outline:3px solid #98401f;outline-offset:2px}
.reference4-menu__list{box-sizing:border-box;overflow:hidden;border:1px solid rgba(255,255,255,.78);border-radius:22px;background:linear-gradient(180deg,rgba(255,252,247,.46),rgba(255,252,247,.30));box-shadow:0 12px 32px rgba(57,36,25,.11),inset 0 1px 0 rgba(255,255,255,.86)}
.reference4-menu__row{box-sizing:border-box;position:relative;width:100%;height:52px;display:grid;grid-template-columns:38px 1fr 18px;align-items:center;gap:9px;padding:0 16px;color:#261d18;text-decoration:none;font-size:19px;line-height:23px;font-weight:560;letter-spacing:-.015em}
.reference4-menu__row+.reference4-menu__row::before{content:"";position:absolute;left:63px;right:16px;top:0;height:1px;background:rgba(114,93,80,.18)}
.reference4-menu__row[aria-current="page"]{background:rgba(255,236,225,.42);color:#8f351c}
.reference4-menu__row>i{width:9px;height:9px;border-top:1.25px solid currentColor;border-right:1.25px solid currentColor;transform:rotate(45deg);justify-self:end}
.reference4-menu__row:focus-visible{outline:3px solid #98401f;outline-offset:-3px}
.reference4-icon{position:relative;width:29px;height:29px;display:block;flex:0 0 29px;color:currentColor}
.reference4-icon>span{position:absolute;inset:0;display:block;background:currentColor;-webkit-mask:var(--reference4-icon) center/contain no-repeat;mask:var(--reference4-icon) center/contain no-repeat}
.reference4-icon--free{box-sizing:border-box;width:30px;height:30px;border:1.25px solid currentColor;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:8.5px;line-height:1;font-weight:760;letter-spacing:-.045em;white-space:nowrap}
.reference4-menu__utility{box-sizing:border-box;overflow:hidden;margin-top:12px;border:1px solid rgba(255,255,255,.78);border-radius:22px;background:linear-gradient(180deg,rgba(255,252,247,.43),rgba(255,252,247,.28));box-shadow:0 10px 28px rgba(57,36,25,.10),inset 0 1px 0 rgba(255,255,255,.84)}
.reference4-menu__account-row{box-sizing:border-box;height:56px;display:grid;grid-template-columns:.8fr 1.2fr;align-items:center;padding:6px 9px;border-bottom:1px solid rgba(114,93,80,.18);gap:8px}
.reference4-menu__account-row>*{box-sizing:border-box;height:44px;min-width:0;display:flex;align-items:center;justify-content:center;gap:7px;padding:0 12px;border:0;border-radius:23px;background:rgba(255,253,249,.66);box-shadow:0 5px 15px rgba(64,40,28,.09),inset 0 1px rgba(255,255,255,.72);color:#2a211c;text-decoration:none;font-family:inherit;font-size:14px;line-height:17px;font-weight:720}
.reference4-menu__account-row>a[aria-current="page"]{background:#3b2720;color:#fffaf2}
.reference4-menu__account-row .reference4-icon{width:20px;height:20px;flex-basis:20px}
.reference4-menu__share{box-sizing:border-box;width:100%;height:50px;display:flex;align-items:center;justify-content:space-between;padding:0 20px;border:0;background:rgba(255,255,255,.18);color:#2a211c;font-family:inherit;font-size:16px;line-height:20px;font-weight:600;text-align:left}
.reference4-menu__share .reference4-icon{width:25px;height:25px;flex-basis:25px}
.reference4-menu__account-row>*:focus-visible,.reference4-menu__share:focus-visible{outline:3px solid #98401f;outline-offset:-3px}
.shell-menu-open .shell-bottom-nav{opacity:0;visibility:hidden;pointer-events:none;transform:translate3d(0,100%,0);transition:opacity 140ms ease,transform 220ms cubic-bezier(.22,.82,.22,1),visibility 0s linear 220ms}
body.shell-menu-open{overflow:hidden;overscroll-behavior:none}
.variant-reference4-fullglass .factual-specimen{padding-top:96px}
@supports not ((backdrop-filter:blur(1px)) or (-webkit-backdrop-filter:blur(1px))){.variant-reference4-fullglass .mobile-discovery-menu__panel{background:rgba(239,228,217,.94)}.reference4-menu__list,.reference4-menu__utility{background:rgba(255,252,247,.90)}}
@media(max-width:350px){
  .reference4-menu{padding-inline:10px;padding-bottom:12px}.reference4-menu__close{right:10px;width:104px;height:104px}.reference4-menu__close img{width:104px;height:104px}.reference4-menu__brand{width:154px;height:100px;padding-top:32px}.reference4-menu__brand svg{width:146px}.reference4-menu__endorsement{font-size:6.8px;letter-spacing:.065em}.reference4-menu__dates{height:44px;margin:8px 0;gap:6px}.reference4-menu__dates a{height:44px;padding-inline:8px;font-size:12.5px}.reference4-menu__row{height:48px;grid-template-columns:34px 1fr 17px;gap:8px;padding-inline:14px;font-size:18px}.reference4-menu__row+.reference4-menu__row::before{left:56px;right:14px}.reference4-menu__utility{margin-top:10px}.reference4-menu__account-row{height:52px}.reference4-menu__share{height:44px}
}
@media(max-height:679px){.variant-reference4-fullglass .mobile-discovery-menu__panel{overflow-y:auto}.reference4-menu{padding-bottom:max(12px,env(safe-area-inset-bottom))}}
@media(prefers-reduced-motion:reduce){.variant-reference4-fullglass .mobile-discovery-menu,.shell-menu-open .shell-bottom-nav{transition:none}}
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

    project_icons = root / "site" / "public" / "assets" / "icons" / "reference4-v8"
    project_ui = root / "site" / "public" / "assets" / "ui"
    shutil.copytree(project_icons, output / "assets" / "icons" / "reference4-v8", dirs_exist_ok=True)
    (output / "assets" / "ui").mkdir(parents=True, exist_ok=True)
    for filename in ("reference4-leather-close-v8.webp", "reference4-leather-close-v8.metadata.json"):
        shutil.copy2(project_ui / filename, output / "assets" / "ui" / filename)
    print(f"Added full-glass reference assets to {output}")


if __name__ == "__main__":
    main()
