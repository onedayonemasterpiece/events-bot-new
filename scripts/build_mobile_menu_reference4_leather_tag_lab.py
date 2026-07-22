#!/usr/bin/env python3
"""Build the reference-4 menu with a moving leather brand tag.

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


BUILD_ID_DEFAULT = "preview-20260722-mobile-menu-reference4-leather-tag-lab-v13"

VARIANTS = {
    "p": {
        "name": "Кожаная выезжающая бирка · reference 4",
        "subtitle": "кожаная бирка едет с плоскостью; utility разделён; service-card Share",
        "body_class": "variant-reference4-leather-tag",
    },
}

ICON_FILES = {
    "children": "baby-thin.svg",
    "exhibitions": "palette-thin.svg",
    "festivals": "buildings-thin.svg",
    "popular": "popular-trend-up-thin.svg",
    "service": "handshake-thin.svg",
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
    return '<span class="reference4-icon reference4-icon--free" aria-hidden="true">0&#x200A;₽</span>'


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
    service_share_manifest = f"{asset_root}/service-share/current/manifest.json"
    service_share_controller = f"{asset_root}/assets/service-share/controller.js"
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
            (
                '<button class="reference4-menu__row reference4-menu__row--service" type="button" '
                'data-reference4-service-open aria-expanded="false">'
                f'{icon(asset_root, "service")}<span>О сервисе</span><i aria-hidden="true"></i></button>'
            ),
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
          <span class="reference4-menu__endorsement">Полюбить<br>Калининград</span>
          <svg viewBox="0 0 7819 1514" aria-hidden="true" focusable="false"><use href="{wordmark}#announcements-wordmark-ui"></use></svg>
        </a>
        <nav class="reference4-menu__dates" aria-label="Быстрый выбор даты">{date_links}</nav>
        <div class="reference4-menu__list" data-reference4-list>
          <nav class="reference4-menu__list-plane is-active" aria-label="Разделы афиши" data-reference4-main>{rows}</nav>
          <nav class="reference4-menu__list-plane" aria-label="О сервисе" data-reference4-service aria-hidden="true">
            <button class="reference4-menu__row reference4-menu__row--back" type="button" data-reference4-service-back>
              <span class="reference4-menu__back-icon" aria-hidden="true"></span><span>Назад к афише</span>
            </button>
            {row(factual.href(base, 'partners'), 'Инфопартнёры', icon(asset_root, 'service'), current == 'partners')}
            {row(factual.href(base, 'partnership'), 'Информационное партнёрство', icon(asset_root, 'service'), current == 'partnership')}
            <a class="reference4-menu__row" href="mailto:info@kenigevents.ru?subject=Правообладателям">
              {icon(asset_root, 'service')}<span>Правообладателям</span><i aria-hidden="true"></i>
            </a>
          </nav>
        </div>
        <section
          class="reference4-menu__utility"
          aria-label="Аккаунт и сервис"
          data-service-share-root
          data-service-share-surface="menu"
          data-service-share-desktop-mode="d0"
          data-service-share-manifest-url="{service_share_manifest}"
          data-service-share-canonical-url="https://kenigevents.ru/"
          data-nosnippet
        >
          <div class="reference4-menu__account-row">
            <button type="button" data-reference4-login>Войти</button>
            <a href="{factual.href(base, 'favorites')}"{factual.active(current, 'favorites')}>{icon(asset_root, 'favorite')}<span>Избранное</span></a>
          </div>
          <button
            class="reference4-menu__share"
            type="button"
            data-reference4-share
            data-service-share-button
            data-service-share-intent="mobile"
            aria-label="Поделиться анонсами KenigEvents"
            aria-describedby="reference4-service-share-status"
            aria-busy="false"
          ><span>Поделиться</span>{icon(asset_root, 'share')}</button>
          <span id="reference4-service-share-status" class="sr-only" data-service-share-status role="status" aria-live="polite" aria-atomic="true"></span>
          <a class="sr-only" data-service-share-fallback href="https://kenigevents.ru/" hidden>https://kenigevents.ru/</a>
        </section>
      </div>
      <script type="module" src="{service_share_controller}"></script>
    """


REFERENCE4_CSS = r'''
/* v13: stable viewport plane, parent-level sticky layering and settled clipping against Android viewport bounce. */
.variant-reference4-leather-tag .shell-header{z-index:39}
.variant-reference4-leather-tag .mobile-discovery-menu{--plane-h:100%;inset:0!important;z-index:41;width:100%;height:auto;overflow:visible;pointer-events:none;transform:translate3d(0,-100%,0);overscroll-behavior:none}
.variant-reference4-leather-tag .mobile-discovery-menu[open]{transform:translate3d(0,0,0);pointer-events:auto}
.variant-reference4-leather-tag .mobile-discovery-menu[open].is-open-settled{overflow:clip}
.variant-reference4-leather-tag .mobile-discovery-menu.is-closing{transform:translate3d(0,-100%,0)}
.shell-menu-open .variant-reference4-leather-tag .mobile-discovery-menu,.shell-menu-open.variant-reference4-leather-tag .mobile-discovery-menu{z-index:60}
body.shell-menu-open .shell-header{z-index:60!important}
.variant-reference4-leather-tag .mobile-discovery-menu__panel{inset:0!important;width:100%;height:100%!important;padding:env(safe-area-inset-top) 0 0;overflow-x:hidden;overflow-y:auto;overscroll-behavior:none;-webkit-overflow-scrolling:touch;touch-action:none;border:0;border-radius:0;background:radial-gradient(62% 30% at 18% 7%,rgba(160,78,45,.18),transparent 70%),linear-gradient(180deg,rgba(70,36,25,.15) 0,rgba(227,216,207,.14) 32%,rgba(241,235,228,.18) 100%);box-shadow:inset 0 1px 0 rgba(255,255,255,.56),0 20px 46px rgba(46,25,16,.24);backdrop-filter:blur(22px) saturate(.96) brightness(.84);-webkit-backdrop-filter:blur(22px) saturate(.96) brightness(.84)}
.variant-reference4-leather-tag .mobile-discovery-menu__panel.is-scrollable{touch-action:pan-y}
.variant-reference4-leather-tag .mobile-discovery-menu__summary{top:100%!important;width:var(--shell-tag-w)!important;height:var(--shell-tag-h)!important;z-index:3!important;display:flex!important;flex-direction:column;align-items:flex-start;justify-content:flex-end;gap:4px;padding:0 0 12px 14px!important;text-align:left;background:transparent url("./assets/ui/mobile-head-skinny-leather-3x.webp") center/100% 100% no-repeat!important;box-shadow:none!important;filter:drop-shadow(0 8px 8px rgba(52,25,16,.22)) drop-shadow(0 15px 20px rgba(52,25,16,.14));transition:box-shadow 220ms ease!important}
.variant-reference4-leather-tag .mobile-discovery-menu__summary .brand-tag__endorsement{align-self:flex-start;margin-left:-1px;color:#fffaf4;text-shadow:0 1px 1.5px rgba(60,20,10,.38),0 0 1px rgba(60,20,10,.22)}
.variant-reference4-leather-tag .mobile-discovery-menu__summary .brand-tag__wordmark{align-self:flex-start;width:96px;filter:brightness(0) invert(1) drop-shadow(0 1px 1px rgba(60,20,10,.34))}
.variant-reference4-leather-tag .mobile-discovery-menu__summary .brand-tag__chevron{display:none!important}
.reference4-menu{box-sizing:border-box;position:relative;z-index:1;width:100%;min-height:100%;padding:0 14px 16px;font-family:inherit;color:#241b16}
.reference4-menu__brand{box-sizing:border-box;width:150px;height:112px;display:flex;flex-direction:column;align-items:flex-start;justify-content:flex-end;gap:5px;padding:32px 0 12px;color:#2b211c;text-decoration:none;filter:drop-shadow(0 1px 1px rgba(255,255,255,.28))}
.reference4-menu__endorsement{font-size:9px;line-height:10px;font-weight:780;letter-spacing:.05em;text-transform:uppercase;white-space:normal;opacity:.92}
.reference4-menu__brand svg{display:block;width:104px;height:auto;fill:currentColor}
.reference4-menu__brand:focus-visible{outline:3px solid #98401f;outline-offset:3px;border-radius:8px}
.reference4-menu__close{position:absolute;z-index:5;top:0;right:14px;box-sizing:border-box;width:112px;height:112px;padding:0;display:block;overflow:hidden;border:0;border-radius:0 0 18px 18px;background:transparent;filter:drop-shadow(0 8px 9px rgba(42,22,15,.24)) drop-shadow(0 18px 28px rgba(42,22,15,.18));cursor:pointer;touch-action:manipulation}
.reference4-menu__close img{display:block;width:112px;height:112px;object-fit:fill}
.reference4-menu__close:focus-visible{outline:3px solid #fffaf2;outline-offset:3px}
.reference4-menu__dates{box-sizing:border-box;height:58px;display:grid;grid-template-columns:1fr 1fr 1.34fr;align-items:center;gap:8px;margin:6px 0 10px}
.reference4-menu__dates a{box-sizing:border-box;min-width:0;height:46px;padding:0 11px;display:flex;align-items:center;justify-content:center;border:1.25px solid rgba(255,255,255,.58);border-radius:24px;background:rgba(255,255,255,.11);box-shadow:inset 0 1px 0 rgba(255,255,255,.42);color:#251c17;text-decoration:none;font-size:14px;line-height:18px;font-weight:720;white-space:nowrap}
.reference4-menu__dates a[aria-current="page"]{border-color:rgba(255,226,213,.84);background:rgba(255,218,203,.64);color:#8d351c;box-shadow:0 0 18px rgba(255,187,158,.36),inset 0 1px 0 rgba(255,255,255,.78)}
.reference4-menu__dates a:focus-visible{outline:3px solid #98401f;outline-offset:2px}
.reference4-menu__list{box-sizing:border-box;position:relative;height:416px;overflow:hidden;border:1px solid rgba(255,255,255,.56);border-radius:22px;background:linear-gradient(180deg,rgba(255,252,247,.48),rgba(255,252,247,.32));box-shadow:0 13px 34px rgba(46,28,20,.14),inset 0 1px 0 rgba(255,255,255,.68)}
.reference4-menu__list-plane{position:absolute;inset:0;display:block;opacity:0;visibility:hidden;transform:translate3d(24px,0,0);transition:opacity 180ms ease,transform 240ms cubic-bezier(.22,.82,.22,1),visibility 0s linear 240ms}
.reference4-menu__list-plane.is-active{opacity:1;visibility:visible;transform:translate3d(0,0,0);transition-delay:0s}
.reference4-menu__list.is-service .reference4-menu__list-plane[data-reference4-main]{transform:translate3d(-24px,0,0)}
.reference4-menu__row{box-sizing:border-box;position:relative;width:100%;height:52px;display:grid;grid-template-columns:38px 1fr 18px;align-items:center;gap:9px;padding:0 16px;border:0;background:transparent;color:#261d18;text-decoration:none;text-align:left;font-family:inherit;font-size:19px;line-height:23px;font-weight:560;letter-spacing:-.015em;cursor:pointer}
.reference4-menu__row+.reference4-menu__row::before{content:"";position:absolute;left:63px;right:16px;top:0;height:1px;background:rgba(114,93,80,.18)}
.reference4-menu__row[aria-current="page"]{background:rgba(255,236,225,.42);color:#8f351c}
.reference4-menu__row>i{width:9px;height:9px;border-top:1.25px solid currentColor;border-right:1.25px solid currentColor;transform:rotate(45deg);justify-self:end}
.reference4-menu__row:focus-visible{outline:3px solid #98401f;outline-offset:-3px}
.reference4-menu__row--back{grid-template-columns:38px 1fr;padding-right:34px;font-weight:720;color:#793014}
.reference4-menu__back-icon{justify-self:center;width:14px;height:14px;border-left:1.5px solid currentColor;border-bottom:1.5px solid currentColor;transform:rotate(45deg)}
.reference4-icon{position:relative;width:29px;height:29px;display:block;flex:0 0 29px;color:currentColor}
.reference4-icon>span{position:absolute;inset:0;display:block;background:currentColor;-webkit-mask:var(--reference4-icon) center/contain no-repeat;mask:var(--reference4-icon) center/contain no-repeat}
.reference4-icon--free{box-sizing:border-box;width:30px;height:30px;border:1px solid currentColor;border-radius:50%;display:flex;align-items:center;justify-content:center;padding-top:.5px;font-size:14px;line-height:1;font-weight:400;letter-spacing:-.02em;white-space:nowrap;opacity:.78}
.reference4-menu__utility{box-sizing:border-box;overflow:hidden;margin-top:12px;border:1px solid rgba(255,255,255,.56);border-radius:22px;background:rgba(255,252,247,.18);box-shadow:0 12px 30px rgba(46,28,20,.13),inset 0 1px 0 rgba(255,255,255,.62)}
.reference4-menu__account-row{box-sizing:border-box;height:56px;display:grid;grid-template-columns:.8fr 1.2fr;align-items:center;padding:6px 9px;border-bottom:1px solid rgba(114,93,80,.18);gap:8px;background:rgba(48,31,23,.075)}
.reference4-menu__account-row>*{box-sizing:border-box;height:44px;min-width:0;display:flex;align-items:center;justify-content:center;gap:7px;padding:0 12px;border:1px solid rgba(255,255,255,.48);border-radius:23px;background:rgba(255,253,249,.58);box-shadow:0 5px 15px rgba(64,40,28,.08),inset 0 1px rgba(255,255,255,.64);color:#2a211c;text-decoration:none;font-family:inherit;font-size:14px;line-height:17px;font-weight:720}
.reference4-menu__account-row>a[aria-current="page"]{background:#3b2720;color:#fffaf2}
.reference4-menu__account-row .reference4-icon{width:20px;height:20px;flex-basis:20px}
.reference4-menu__share{box-sizing:border-box;width:100%;height:50px;display:flex;align-items:center;justify-content:space-between;padding:0 20px;border:0;background:rgba(255,252,247,.58);color:#2a211c;font-family:inherit;font-size:16px;line-height:20px;font-weight:650;text-align:left;cursor:pointer}
.reference4-menu__share .reference4-icon{width:25px;height:25px;flex-basis:25px}
.reference4-menu__share:disabled{cursor:wait;opacity:.72}
.reference4-menu__account-row>*:focus-visible,.reference4-menu__share:focus-visible{outline:3px solid #98401f;outline-offset:-3px}
.shell-menu-open .shell-bottom-nav{opacity:0;visibility:hidden;pointer-events:none;transform:translate3d(0,100%,0);transition:opacity 140ms ease,transform 220ms cubic-bezier(.22,.82,.22,1),visibility 0s linear 220ms}
body.shell-menu-open{overflow:hidden;overscroll-behavior:none}
html.shell-menu-open{overflow:hidden;overscroll-behavior:none}
.variant-reference4-leather-tag .factual-specimen{padding-top:96px}
@supports not ((backdrop-filter:blur(1px)) or (-webkit-backdrop-filter:blur(1px))){.variant-reference4-leather-tag .mobile-discovery-menu__panel{background:rgba(203,191,181,.94)}.reference4-menu__list{background:rgba(255,252,247,.90)}}
@media(max-width:350px){
  .reference4-menu{padding-inline:10px;padding-bottom:12px}.reference4-menu__close{right:10px;width:104px;height:104px}.reference4-menu__close img{width:104px;height:104px}.reference4-menu__brand{width:140px;height:104px;padding:30px 0 11px}.reference4-menu__endorsement{font-size:8px;line-height:9px}.reference4-menu__brand svg{width:96px}.reference4-menu__dates{height:44px;margin:6px 0 8px;gap:6px}.reference4-menu__dates a{height:44px;padding-inline:8px;font-size:12.5px}.reference4-menu__list{height:384px}.reference4-menu__row{height:48px;grid-template-columns:34px 1fr 17px;gap:8px;padding-inline:14px;font-size:18px}.reference4-menu__row+.reference4-menu__row::before{left:56px;right:14px}.reference4-menu__row--back{grid-template-columns:34px 1fr}.reference4-icon--free{font-size:13px}.reference4-menu__utility{margin-top:10px}.reference4-menu__account-row{height:52px}.reference4-menu__share{height:44px}
}
@media(max-height:679px){.variant-reference4-leather-tag .mobile-discovery-menu__panel{overflow-y:auto}.reference4-menu{padding-bottom:max(12px,env(safe-area-inset-bottom))}}
@media(prefers-reduced-motion:reduce){.variant-reference4-leather-tag .mobile-discovery-menu,.variant-reference4-leather-tag .mobile-discovery-menu__summary,.reference4-menu__list-plane,.shell-menu-open .shell-bottom-nav{transition:none!important}}
'''

REFERENCE4_JS = r'''
(() => {
  document.addEventListener('DOMContentLoaded', () => {
    const menu = document.querySelector('[data-mobile-discovery-menu]');
    const summary = menu?.querySelector(':scope > summary');
    const close = menu?.querySelector('[data-reference4-close]');
    const list = menu?.querySelector('[data-reference4-list]');
    const mainPlane = menu?.querySelector('[data-reference4-main]');
    const servicePlane = menu?.querySelector('[data-reference4-service]');
    const serviceOpen = menu?.querySelector('[data-reference4-service-open]');
    const serviceBack = menu?.querySelector('[data-reference4-service-back]');
    const panel = menu?.querySelector('.mobile-discovery-menu__panel');
    let settleTimer = 0;

    const clearSettled = () => {
      clearTimeout(settleTimer);
      menu?.classList.remove('is-open-settled');
    };
    const scheduleSettled = () => {
      clearSettled();
      const settle = () => {
        if (menu?.open && !menu.classList.contains('is-closing')) menu.classList.add('is-open-settled');
      };
      if (matchMedia('(prefers-reduced-motion: reduce)').matches) requestAnimationFrame(settle);
      else settleTimer = setTimeout(settle, 360);
    };

    const syncScrollSurface = () => {
      if (!panel) return;
      panel.classList.toggle('is-scrollable', panel.scrollHeight > panel.clientHeight + 1);
    };

    const showService = (show) => {
      list?.classList.toggle('is-service', show);
      mainPlane?.classList.toggle('is-active', !show);
      servicePlane?.classList.toggle('is-active', show);
      mainPlane?.setAttribute('aria-hidden', show ? 'true' : 'false');
      servicePlane?.setAttribute('aria-hidden', show ? 'false' : 'true');
      serviceOpen?.setAttribute('aria-expanded', show ? 'true' : 'false');
      requestAnimationFrame(() => (show ? serviceBack : serviceOpen)?.focus({ preventScroll: true }));
    };
    serviceOpen?.addEventListener('click', () => showService(true));
    serviceBack?.addEventListener('click', () => showService(false));

    close?.addEventListener('click', () => {
      clearSettled();
      showService(false);
      summary?.click();
      setTimeout(() => summary?.focus({ preventScroll: true }), matchMedia('(prefers-reduced-motion: reduce)').matches ? 0 : 360);
    });
    menu?.addEventListener('toggle', () => {
      summary?.setAttribute('aria-label', menu.open ? 'Закрыть навигацию афиши' : 'Открыть навигацию афиши');
      document.documentElement.classList.toggle('shell-menu-open', menu.open);
      if (menu.open) {
        panel.scrollTop = 0;
        scheduleSettled();
        requestAnimationFrame(() => {
          syncScrollSurface();
          close?.focus({ preventScroll: true });
        });
      } else {
        clearSettled();
        showService(false);
      }
    });
    new MutationObserver(() => {
      if (menu?.classList.contains('is-closing') && menu.classList.contains('is-open-settled')) clearSettled();
    }).observe(menu, { attributes: true, attributeFilter: ['class'] });
    window.addEventListener('resize', syncScrollSurface, { passive: true });
    window.visualViewport?.addEventListener('resize', syncScrollSurface, { passive: true });

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
    document_scroll_autoclose = """  document.addEventListener('scroll', () => {
    if (menu?.hasAttribute('open') && Math.abs((scrollY || 0) - openedAtY) > 24) closeMenu();
  }, { passive: true });
"""
    if document_scroll_autoclose not in previous_js:
        raise RuntimeError("mobile shell document-scroll autoclose contract changed")
    fullscreen_base_js = previous_js.replace(document_scroll_autoclose, "")
    factual.VARIANTS = VARIANTS
    factual.plane_content = plane_content
    factual.FACTUAL_CSS = REFERENCE4_CSS
    factual.BUILD_ID_DEFAULT = BUILD_ID_DEFAULT
    factual.core.LAB_JS = fullscreen_base_js + REFERENCE4_JS
    try:
        factual.build(output, args.donor, args.build_id)
    finally:
        factual.core.LAB_JS = previous_js

    project_icons = root / "site" / "public" / "assets" / "icons" / "reference4-v8"
    project_ui = root / "site" / "public" / "assets" / "ui"
    project_service_share = root / "site" / "public" / "service-share"
    service_share_controller = root / "site" / "src" / "lib" / "service-share" / "controller.js"
    shutil.copytree(project_icons, output / "assets" / "icons" / "reference4-v8", dirs_exist_ok=True)
    shutil.copytree(project_service_share, output / "service-share", dirs_exist_ok=True)
    (output / "assets" / "service-share").mkdir(parents=True, exist_ok=True)
    shutil.copy2(service_share_controller, output / "assets" / "service-share" / "controller.js")
    (output / "assets" / "ui").mkdir(parents=True, exist_ok=True)
    for filename in (
        "reference4-leather-close-v8.webp",
        "reference4-leather-close-v8.metadata.json",
        "mobile-head-skinny-leather-3x.webp",
        "mobile-head-skinny-leather-3x.metadata.json",
    ):
        shutil.copy2(project_ui / filename, output / "assets" / "ui" / filename)
    print(f"Added leather-tag reference assets to {output}")


if __name__ == "__main__":
    main()
