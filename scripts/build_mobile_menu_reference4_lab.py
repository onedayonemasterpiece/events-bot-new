#!/usr/bin/env python3
"""Build the M mobile-menu challenger adapted from reference (4).

The adaptation keeps the accepted whole-object vertical motion and v23 content,
uses the canonical wide-o wordmark, and turns the right-side leather tab into a
branded closed handle / close control without copying the reference's fake IA.
"""

from __future__ import annotations

import argparse
import html
from pathlib import Path

import build_mobile_shell_factual_nav_lab as factual


BUILD_ID_DEFAULT = "preview-20260722-mobile-menu-reference4-lab-v6"

VARIANTS = {
    "m": {
        "name": "Кожаная бирка справа",
        "subtitle": "challenger: reference-derived hierarchy with factual navigation",
        "body_class": "variant-reference-leather",
    },
}


def asset_root_from_base(base: str) -> str:
    return base.rsplit("/variant-", 1)[0]


def nav(base: str, current: str, page: str, label: str, class_name: str = "") -> str:
    return factual.nav_link(base, current, page, label, class_name)


def plane_content(variant: str, base: str, current: str) -> str:
    asset_root = asset_root_from_base(base)
    wordmark = f"{asset_root}/assets/v2/brand/announcements-wordmark-ui.svg"
    return f"""
      <div class="reference-menu" aria-label="Глобальная навигация">
        <a class="reference-brand" href="{factual.href(base, 'home')}"{factual.active(current, 'home')} aria-label="Главная — Полюбить Калининград Анонсы">
          <span class="reference-brand__endorsement"><span>Полюбить</span><span>Калининград</span></span>
          <svg class="reference-brand__wordmark" viewBox="0 0 7819 1514" aria-hidden="true" focusable="false"><use href="{wordmark}#announcements-wordmark-ui"></use></svg>
          <small>Главная</small>
        </a>
        <nav class="reference-dates" aria-label="Быстрый выбор даты">
          {nav(base,current,'calendar','Сегодня')}
          {nav(base,current,'tomorrow','Завтра')}
          {nav(base,current,'weekend','Выходные')}
        </nav>
        <nav class="reference-sections" aria-label="Разделы афиши">
          {nav(base,current,'exhibitions','Выставки')}
          {nav(base,current,'popular','Популярное')}
          {nav(base,current,'clubs','Клубы')}
          <span class="reference-section-disabled" aria-disabled="true"><b>Фестивали</b><small>позже</small></span>
        </nav>
        <div class="reference-utilities" aria-label="Аккаунт и сервис">
          <button type="button"><small>Аккаунт</small><b>Войти</b></button>
          {nav(base,current,'favorites','Моё избранное')}
          {nav(base,current,'partners','Инфопартнёры')}
          <button type="button"><small>Сервис</small><b>Поделиться</b></button>
        </div>
      </div>
    """


def shell_header(variant: str, base: str, asset_root: str, current: str, context_title: str, context_meta: str) -> str:
    content = plane_content(variant, base, current)
    wordmark = f"{asset_root}/assets/v2/brand/announcements-wordmark-ui.svg"
    return f"""
      <header class="site-header shell-header">
        <details class="mobile-discovery-menu reference-discovery-menu" data-mobile-discovery-menu>
          <button class="reference-menu-scrim" type="button" tabindex="-1" aria-label="Закрыть навигацию"></button>
          <summary class="brand-tag mobile-discovery-menu__summary reference-leather-tag" aria-label="Открыть навигацию">
            <span class="reference-leather-tag__closed" aria-hidden="true">
              <svg viewBox="0 0 7819 1514" aria-hidden="true" focusable="false"><use href="{wordmark}#announcements-wordmark-ui"></use></svg>
              <small>Меню</small>
            </span>
            <span class="reference-leather-tag__open" aria-hidden="true">
              <b>×</b><small>Закрыть</small>
            </span>
          </summary>
          <div class="mobile-discovery-menu__panel reference-discovery-panel" aria-label="Меню и действия афиши">{content}</div>
        </details>
        <div class="sticky-date shell-context"><div class="sticky-date__layout"><div class="sticky-date__row1"><strong>{html.escape(context_title)}</strong></div><span class="shell-context__meta">{html.escape(context_meta)}</span></div></div>
      </header>
    """


REFERENCE_CSS = r'''
/* v6 reference (4) adaptation. Leather is an explicit research-only deviation. */
.variant-reference-leather .mobile-discovery-menu{--plane-h:388px;--shell-tag-w:96px;--shell-tag-h:90px}
.variant-reference-leather .mobile-discovery-menu__panel{height:calc(var(--plane-h) + env(safe-area-inset-top));overflow:hidden;border:0;border-bottom:2px solid #7b2f18;background:#f1dfcc;box-shadow:0 17px 36px rgba(56,35,22,.22)}
.reference-menu-scrim{position:absolute;z-index:0;left:0;right:0;top:calc(var(--plane-h) + env(safe-area-inset-top));height:calc(100dvh - var(--plane-h));padding:0;border:0;background:rgba(45,30,21,.14);opacity:0;visibility:hidden;pointer-events:none;touch-action:pan-y;transition:opacity 180ms ease,visibility 0s linear 320ms}
.variant-reference-leather .mobile-discovery-menu[open] .reference-menu-scrim,.variant-reference-leather .mobile-discovery-menu.is-closing .reference-menu-scrim{opacity:1;visibility:visible;pointer-events:auto;transition-delay:0s}
.variant-reference-leather .mobile-discovery-menu.is-closing .reference-menu-scrim{pointer-events:none}
.variant-reference-leather .mobile-discovery-menu__summary{left:auto!important;right:max(14px,env(safe-area-inset-right))!important;top:calc(var(--plane-h) + env(safe-area-inset-top) - 8px)!important;z-index:3!important;width:var(--shell-tag-w)!important;height:calc(var(--shell-tag-h) + env(safe-area-inset-top))!important;padding:max(12px,calc(env(safe-area-inset-top) + 9px)) 10px 10px!important;display:flex!important;align-items:center!important;justify-content:center!important;border:0!important;border-radius:0 0 15px 15px!important;background-color:#964324!important;background-image:radial-gradient(circle at 18% 24%,rgba(255,244,232,.22) 0 .65px,transparent .8px),radial-gradient(circle at 72% 68%,rgba(69,22,8,.2) 0 .75px,transparent .9px),repeating-linear-gradient(29deg,rgba(255,255,255,.035) 0 1px,rgba(84,29,12,.035) 1px 3px)!important;background-size:9px 9px,11px 11px,7px 7px!important;color:#fffaf2!important;box-shadow:inset 0 2px 3px rgba(255,255,255,.16),inset 0 -5px 9px rgba(71,23,8,.24),0 13px 25px rgba(70,31,15,.28)!important}
.variant-reference-leather .mobile-discovery-menu__summary::before{content:"";position:absolute;inset:5px;border:1px dashed rgba(255,232,214,.48);border-radius:0 0 11px 11px;pointer-events:none}
.reference-leather-tag__closed,.reference-leather-tag__open{position:relative;z-index:1;width:100%;height:100%;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:5px}
.reference-leather-tag__closed svg{display:block;width:76px;height:auto;color:#fff;fill:currentColor}
.reference-leather-tag__closed small,.reference-leather-tag__open small{font-size:8px;line-height:10px;font-weight:800;letter-spacing:.09em;text-transform:uppercase;color:#f8e8dc}
.reference-leather-tag__open{display:none}.reference-leather-tag__open b{font-size:34px;line-height:30px;font-weight:350}
.variant-reference-leather .mobile-discovery-menu[open] .reference-leather-tag__closed{display:none}.variant-reference-leather .mobile-discovery-menu[open] .reference-leather-tag__open{display:flex}
.variant-reference-leather .shell-context{left:12px!important;right:122px!important;justify-content:flex-start!important;text-align:left!important}
.reference-menu{box-sizing:border-box;width:100%;height:388px;padding:0 14px;display:flex;flex-direction:column;color:var(--text-main);font-family:inherit}
.reference-brand{box-sizing:border-box;height:72px;min-height:72px;display:grid;grid-template-columns:132px 1fr;grid-template-rows:26px 28px;align-content:center;column-gap:10px;color:var(--accent);text-decoration:none}
.reference-brand__endorsement{align-self:end;display:flex;flex-direction:column;font-size:8px;line-height:9px;font-weight:700;letter-spacing:.09em;text-transform:uppercase;color:var(--accent)}
.reference-brand__wordmark{grid-row:2;display:block;width:132px;height:auto;color:var(--accent);fill:currentColor}
.reference-brand>small{grid-column:2;grid-row:1/3;align-self:center;font-size:10px;line-height:12px;font-weight:750;letter-spacing:.07em;text-transform:uppercase;color:var(--text-sec)}
.reference-brand[aria-current="page"]>small{color:var(--accent);text-decoration:underline;text-decoration-thickness:2px;text-underline-offset:4px}
.reference-dates{box-sizing:border-box;height:52px;min-height:52px;display:flex;align-items:center;justify-content:space-between;gap:8px}
.reference-dates a{box-sizing:border-box;min-width:0;height:44px;display:flex;align-items:center;justify-content:center;padding:0 13px;border:1px solid rgba(152,64,31,.3);border-radius:22px;background:#fffaf2;color:var(--text-main);text-decoration:none;font-size:13px;line-height:16px;font-weight:740;box-shadow:0 4px 10px rgba(72,45,25,.06)}
.reference-dates a[aria-current="page"]{border-color:var(--accent);background:#f5cdbb;color:var(--accent);box-shadow:0 5px 13px rgba(152,64,31,.16)}
.reference-sections{box-sizing:border-box;height:176px;min-height:176px;overflow:hidden;border:1px solid rgba(152,64,31,.16);border-radius:19px;background:#fffaf2;box-shadow:0 9px 20px rgba(72,45,25,.1)}
.reference-sections>a,.reference-section-disabled{box-sizing:border-box;width:100%;height:44px;min-height:44px;padding:0 17px;display:flex;align-items:center;color:var(--text-main);text-decoration:none;font-size:17px;line-height:20px;font-weight:720}
.reference-sections>*+*{border-top:1px solid rgba(152,64,31,.12)}
.reference-sections>a[aria-current="page"]{color:var(--accent);background:#f8eee3}
.reference-section-disabled{justify-content:space-between;color:#9a8d82}.reference-section-disabled small{font-size:8px;line-height:10px;font-weight:800;letter-spacing:.08em;text-transform:uppercase;color:var(--accent)}
.reference-utilities{box-sizing:border-box;height:88px;min-height:88px;display:grid;grid-template-columns:1fr 1.35fr;grid-template-rows:44px 44px;column-gap:8px}
.reference-utilities>a,.reference-utilities>button{box-sizing:border-box;min-width:0;min-height:44px;display:flex;align-items:center;justify-content:flex-start;padding:0 10px;border:0;border-radius:0;background:transparent;color:var(--text-main);text-decoration:none;text-align:left;font-family:inherit;font-size:11px;line-height:14px;font-weight:720}
.reference-utilities>button{flex-direction:column;align-items:flex-start;justify-content:center}.reference-utilities>button small{font-size:7px;line-height:8px;font-weight:800;letter-spacing:.08em;text-transform:uppercase;color:var(--text-sec)}.reference-utilities>button b{font-size:11px;line-height:14px}
.reference-utilities>*:nth-child(odd){border-right:1px solid rgba(152,64,31,.14)}.reference-utilities>*:nth-child(n+3){border-top:1px solid rgba(152,64,31,.14)}
.reference-utilities>a[aria-current="page"]{color:var(--accent);text-decoration:underline;text-decoration-thickness:2px;text-underline-offset:4px}
.variant-reference-leather .shell-bottom-nav{background:#fffdf8!important;border-top:1px solid var(--hairline)!important;box-shadow:0 -5px 18px rgba(72,45,25,.08)!important}
.factual-specimen{max-width:390px;margin:0 auto;padding-top:96px;padding-bottom:96px}.factual-specimen .page-head{padding-bottom:16px}.factual-specimen>.event-list{padding-top:0}.factual-specimen>.event-list .event-row{margin-top:8px}.factual-footer{margin-bottom:22px}.factual-footer a{max-width:100%}
@media(max-width:350px){
  .reference-menu{padding-inline:12px}.reference-brand{grid-template-columns:124px 1fr}.reference-brand__wordmark{width:124px}.reference-dates{gap:6px}.reference-dates a{padding-inline:10px;font-size:12px}.reference-sections>a,.reference-section-disabled{padding-inline:15px}.reference-utilities{grid-template-columns:.95fr 1.45fr;column-gap:5px}.reference-utilities>a,.reference-utilities>button{padding-inline:7px;font-size:10px}.variant-reference-leather .shell-context{right:118px!important}
}
'''

REFERENCE_JS = r'''
(() => {
  const syncReferenceMenuLabel = () => {
    const menu = document.querySelector('.reference-discovery-menu');
    const summary = menu?.querySelector('.reference-leather-tag');
    if (!menu || !summary) return;
    summary.setAttribute('aria-label', menu.open ? 'Закрыть навигацию' : 'Открыть навигацию');
  };
  document.addEventListener('DOMContentLoaded', () => {
    const menu = document.querySelector('.reference-discovery-menu');
    if (!menu) return;
    const summary = menu.querySelector('.reference-leather-tag');
    syncReferenceMenuLabel();
    menu.addEventListener('toggle', syncReferenceMenuLabel);
    menu.querySelector('.reference-menu-scrim')?.addEventListener('click', () => summary?.click());
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

    previous_header = factual.core.shell_header
    previous_js = factual.core.LAB_JS
    factual.VARIANTS = VARIANTS
    factual.plane_content = plane_content
    factual.FACTUAL_CSS = REFERENCE_CSS
    factual.BUILD_ID_DEFAULT = BUILD_ID_DEFAULT
    factual.core.shell_header = shell_header
    factual.core.LAB_JS = previous_js + REFERENCE_JS
    try:
        factual.build(output, args.donor, args.build_id)
    finally:
        factual.core.shell_header = previous_header
        factual.core.LAB_JS = previous_js


if __name__ == "__main__":
    main()
