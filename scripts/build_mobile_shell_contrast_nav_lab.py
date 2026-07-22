#!/usr/bin/env python3
"""Build J/K/L mobile header-weight prototypes after Telegram review.

The lab keeps the factual release IA and accepted whole-object brand-tag motion,
but deliberately removes the repeated bordered-cell composition from v4.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import build_mobile_shell_factual_nav_lab as factual


BUILD_ID_DEFAULT = "preview-20260722-mobile-shell-contrast-nav-lab-v5"

VARIANTS = {
    "j": {
        "name": "Типографическая плоскость",
        "subtitle": "тёплая цельная шапка без внутренних клеток",
        "body_class": "variant-contrast-type",
    },
    "k": {
        "name": "Асимметричный индекс",
        "subtitle": "терракотовый якорь и свободный редакционный список",
        "body_class": "variant-contrast-index",
    },
    "l": {
        "name": "Терракотовый монолит",
        "subtitle": "намеренно тяжёлая брендовая шапка для проверки предела",
        "body_class": "variant-contrast-heavy",
    },
}


def link(base: str, current: str, page: str, label: str, class_name: str = "") -> str:
    return factual.nav_link(base, current, page, label, class_name)


def festival(class_name: str = "") -> str:
    cls = f"contrast-festival {class_name}".strip()
    return f'<span class="{cls}" aria-disabled="true"><b>Фестивали</b><small>позже</small></span>'


def service_button(label: str, value: str) -> str:
    return f'<button type="button"><small>{label}</small><b>{value}</b></button>'


def plane_content(variant: str, base: str, current: str) -> str:
    if variant == "j":
        return f"""
          <div class="nav-type" aria-label="Глобальное меню">
            <div class="nav-type__service">{service_button('Аккаунт', 'Войти')}{service_button('Сервис', 'Поделиться')}</div>
            <nav class="nav-type__lead" aria-label="Главное">{link(base,current,'home','Главная')}{link(base,current,'favorites','Моё избранное')}</nav>
            <nav class="nav-type__dates" aria-label="По датам">{link(base,current,'calendar','Сегодня')}{link(base,current,'tomorrow','Завтра')}{link(base,current,'weekend','Выходные')}</nav>
            <nav class="nav-type__sections" aria-label="Разделы афиши">{link(base,current,'exhibitions','Выставки')}{link(base,current,'popular','Популярное')}{link(base,current,'clubs','Клубы')}{festival()}</nav>
            <nav class="nav-type__tail" aria-label="Другие разделы">{link(base,current,'search','Поиск')}{link(base,current,'personal','Для меня')}{link(base,current,'partners','Инфопартнёры')}</nav>
          </div>
        """

    if variant == "k":
        return f"""
          <div class="nav-index" aria-label="Глобальное меню">
            <section class="nav-index__anchor" aria-label="Главное и сервис">
              {link(base,current,'home','Главная','nav-index__home')}
              {link(base,current,'favorites','Моё избранное','nav-index__favorite')}
              {service_button('Аккаунт','Войти')}
              {service_button('Сервис','Поделиться')}
              {festival('nav-index__festival')}
            </section>
            <section class="nav-index__flow">
              <nav aria-label="По датам" class="nav-index__dates">{link(base,current,'calendar','Сегодня')}{link(base,current,'tomorrow','Завтра')}{link(base,current,'weekend','Выходные')}</nav>
              <nav aria-label="Разделы афиши" class="nav-index__sections">{link(base,current,'exhibitions','Выставки')}{link(base,current,'popular','Популярное')}{link(base,current,'clubs','Клубы')}</nav>
              <nav aria-label="Другие разделы" class="nav-index__actions">{link(base,current,'search','Поиск')}{link(base,current,'personal','Для меня')}</nav>
              {link(base,current,'partners','Инфопартнёры','nav-index__partners')}
            </section>
          </div>
        """

    return f"""
      <div class="nav-heavy" aria-label="Глобальное меню">
        <div class="nav-heavy__top">
          {link(base,current,'home','Главная','nav-heavy__home')}
          <div class="nav-heavy__service">{service_button('Аккаунт','Войти')}{service_button('Сервис','Поделиться')}</div>
        </div>
        <div class="nav-heavy__middle">
          {link(base,current,'favorites','Моё избранное','nav-heavy__favorite')}
          <nav aria-label="По датам" class="nav-heavy__dates">{link(base,current,'calendar','Сегодня')}{link(base,current,'tomorrow','Завтра')}{link(base,current,'weekend','Выходные')}</nav>
        </div>
        <nav aria-label="Разделы афиши" class="nav-heavy__sections">{link(base,current,'exhibitions','Выставки')}{link(base,current,'popular','Популярное')}{link(base,current,'clubs','Клубы')}{festival()}</nav>
        <nav aria-label="Другие разделы" class="nav-heavy__tail">{link(base,current,'search','Поиск')}{link(base,current,'personal','Для меня')}{link(base,current,'partners','Инфопартнёры')}</nav>
      </div>
    """


CONTRAST_CSS = r'''
/* v5: three different header weights; no repeated bordered-cell menu. */
.variant-contrast-type .mobile-discovery-menu{--plane-h:228px}
.variant-contrast-index .mobile-discovery-menu{--plane-h:236px}
.variant-contrast-heavy .mobile-discovery-menu{--plane-h:224px}
.variant-contrast-type .mobile-discovery-menu__panel,.variant-contrast-index .mobile-discovery-menu__panel,.variant-contrast-heavy .mobile-discovery-menu__panel{border:0;box-shadow:0 15px 32px rgba(56,35,22,.19)}
.variant-contrast-type .mobile-discovery-menu__panel{background:#f2e3d1;border-bottom:3px solid var(--accent)}
.variant-contrast-index .mobile-discovery-menu__panel{background:#fffaf2;border-bottom:3px solid var(--accent)}
.variant-contrast-heavy .mobile-discovery-menu__panel{background:#ad4926;border-bottom:3px solid #6f2a14;color:#fffaf2;box-shadow:0 17px 36px rgba(77,29,12,.28)}
.variant-contrast-type .mobile-discovery-menu__summary{box-shadow:0 12px 24px rgba(72,45,25,.22)!important}
.variant-contrast-index .mobile-discovery-menu__summary{background:#8b3318!important;border-right:1px solid rgba(255,250,242,.2)!important;box-shadow:0 13px 26px rgba(65,29,13,.28)!important}
.variant-contrast-heavy .mobile-discovery-menu__summary{background:#70250f!important;border-right:1px solid rgba(255,250,242,.24)!important;box-shadow:0 13px 26px rgba(70,24,9,.3)!important}
.nav-type,.nav-index,.nav-heavy{box-sizing:border-box;width:100%;height:100%;font-family:inherit}
.nav-type a,.nav-type button,.nav-index a,.nav-index button,.nav-heavy a,.nav-heavy button{box-sizing:border-box;min-width:0;min-height:44px;display:flex;align-items:center;border:0;background:transparent;color:inherit;text-decoration:none;font-family:inherit;cursor:pointer}
.nav-type button,.nav-index button,.nav-heavy button{flex-direction:column;align-items:flex-start;justify-content:center;padding:0;text-align:left}
.nav-type button small,.nav-index button small,.nav-heavy button small{font-size:7.5px;line-height:9px;font-weight:800;letter-spacing:.08em;text-transform:uppercase;opacity:.66}
.nav-type button b,.nav-index button b,.nav-heavy button b{font-size:11px;line-height:14px;font-weight:760}
.contrast-festival{box-sizing:border-box;min-width:0;min-height:44px;display:flex;flex-direction:column;align-items:flex-start;justify-content:center;color:#8e8176}
.contrast-festival b{font-size:inherit;line-height:inherit}.contrast-festival small{font-size:7px;line-height:8px;font-weight:800;letter-spacing:.07em;text-transform:uppercase;color:var(--accent)}
.nav-type a[aria-current="page"],.nav-index a[aria-current="page"]{color:var(--accent);text-decoration:underline;text-decoration-thickness:2px;text-underline-offset:5px}
.nav-heavy a[aria-current="page"]{color:#fff;text-decoration:underline;text-decoration-thickness:2px;text-underline-offset:5px}
.nav-type a:focus-visible,.nav-type button:focus-visible,.nav-index a:focus-visible,.nav-index button:focus-visible,.nav-heavy a:focus-visible,.nav-heavy button:focus-visible{outline:2px solid currentColor;outline-offset:-3px}
/* J — one warm surface; hierarchy comes from type, not cells. */
.nav-type{padding:0 16px;color:var(--text-main)}
.nav-type>*{box-sizing:border-box;display:flex;align-items:stretch;border:0}
.nav-type__service{height:44px;justify-content:space-between}.nav-type__service button{width:46%}.nav-type__service button:last-child{align-items:flex-end;text-align:right}
.nav-type__lead{height:48px;align-items:center;justify-content:space-between}.nav-type__lead a:first-child{font-size:22px;line-height:24px;font-weight:860}.nav-type__lead a:last-child{justify-content:flex-end;font-size:15px;line-height:18px;font-weight:720}
.nav-type__dates{height:44px;justify-content:space-between}.nav-type__dates a{font-size:15px;line-height:18px;font-weight:780;color:var(--accent)}
.nav-type__sections{height:46px;justify-content:space-between;gap:8px}.nav-type__sections a,.nav-type__sections .contrast-festival{font-size:11.5px;line-height:14px;font-weight:720}
.nav-type__tail{height:46px;justify-content:space-between}.nav-type__tail a{font-size:11px;line-height:14px;font-weight:680;color:var(--text-sec)}
/* K — one asymmetric split, not a matrix of cells. */
.nav-index{display:flex;color:var(--text-main)}
.nav-index__anchor{box-sizing:border-box;width:112px;height:236px;padding:8px 10px 8px 14px;display:flex;flex-direction:column;background:#b55a38;color:#fffaf2}
.nav-index__anchor>*{width:100%}.nav-index__anchor a[aria-current="page"]{color:#fff}
.nav-index__home{height:52px!important;font-size:19px;line-height:22px;font-weight:850}.nav-index__favorite{font-size:11.5px;line-height:14px;font-weight:700}.nav-index__anchor button{color:#fffaf2}.nav-index__anchor button small{opacity:.7}.nav-index__anchor .contrast-festival{margin-top:auto;color:#ead4c9;font-size:11px;line-height:12px}.nav-index__anchor .contrast-festival b{max-width:100%;white-space:normal}.nav-index__anchor .contrast-festival small{color:#fff2e9}
.nav-index__flow{box-sizing:border-box;flex:1;height:236px;padding:8px 15px 8px 16px;display:flex;flex-direction:column;justify-content:center}
.nav-index__flow nav{display:flex;align-items:stretch;justify-content:space-between}.nav-index__dates{height:50px}.nav-index__dates a{font-size:14px;line-height:17px;font-weight:780;color:var(--accent)}
.nav-index__sections{height:50px}.nav-index__sections a{font-size:13px;line-height:16px;font-weight:720}.nav-index__actions{height:50px}.nav-index__actions a{font-size:17px;line-height:20px;font-weight:820}.nav-index__partners{height:50px;font-size:11px;line-height:14px;font-weight:660;color:var(--text-sec)}
/* L — intentionally heavy brand plane; dock stays light to avoid a sandwich. */
.nav-heavy{padding:0 16px;color:#fffaf2}
.nav-heavy>*,.nav-heavy nav{box-sizing:border-box;display:flex;align-items:stretch;border:0}
.nav-heavy__top{height:60px;justify-content:space-between}.nav-heavy__home{font-size:25px;line-height:28px;font-weight:880}.nav-heavy__service{width:128px;display:flex;justify-content:space-between}.nav-heavy__service button{color:#fffaf2}.nav-heavy__service button:last-child{align-items:flex-end;text-align:right}
.nav-heavy__middle{height:54px;justify-content:space-between}.nav-heavy__favorite{font-size:14px;line-height:17px;font-weight:760}.nav-heavy__dates{width:166px;justify-content:flex-end;gap:8px}.nav-heavy__dates a{font-size:10.5px;line-height:14px;font-weight:760;color:#fff4ec}
.nav-heavy__sections{height:56px;justify-content:space-between;gap:8px}.nav-heavy__sections a,.nav-heavy__sections .contrast-festival{font-size:11.5px;line-height:14px;font-weight:720;color:#fffaf2}.nav-heavy__sections .contrast-festival{color:#efcbb9}.nav-heavy__sections .contrast-festival small{color:#fff4ec}
.nav-heavy__tail{height:54px;justify-content:space-between}.nav-heavy__tail a{font-size:12px;line-height:15px;font-weight:700;color:#f3d8ca}
/* Keep lower chrome deliberately light and stable in all comparisons. */
.variant-contrast-type .shell-bottom-nav,.variant-contrast-index .shell-bottom-nav,.variant-contrast-heavy .shell-bottom-nav{background:#fffdf8!important;border-top:1px solid var(--hairline)!important;box-shadow:0 -5px 18px rgba(72,45,25,.08)!important}
.variant-contrast-type .mobile-micro-footer,.variant-contrast-index .mobile-micro-footer,.variant-contrast-heavy .mobile-micro-footer{background:transparent;border-top:1px solid var(--hairline)}
.factual-specimen{max-width:390px;margin:0 auto;padding-top:96px;padding-bottom:96px}.factual-specimen .page-head{padding-bottom:16px}.factual-specimen>.event-list{padding-top:0}.factual-specimen>.event-list .event-row{margin-top:8px}.factual-footer{margin-bottom:22px}.factual-footer a{max-width:100%}
@media(max-width:350px){
  .nav-type,.nav-heavy{padding-inline:12px}.nav-type__lead a:first-child{font-size:21px}.nav-type__lead a:last-child{font-size:13px}.nav-type__sections{gap:5px}.nav-type__sections a,.nav-type__sections .contrast-festival{font-size:10.5px}.nav-type__tail a{font-size:10px}
  .nav-index__anchor{width:106px;padding-left:12px;padding-right:8px}.nav-index__flow{padding-left:12px;padding-right:12px}.nav-index__dates a{font-size:12px}.nav-index__sections a{font-size:11px}.nav-index__actions a{font-size:15px}.nav-index__partners{font-size:10px}
  .nav-heavy__home{font-size:23px}.nav-heavy__service{width:118px}.nav-heavy__middle .nav-heavy__dates{width:166px;gap:7px}.nav-heavy__sections{gap:5px}.nav-heavy__sections a,.nav-heavy__sections .contrast-festival{font-size:10.5px}.nav-heavy__tail a{font-size:10.5px}
}
'''


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--donor", type=Path, default=factual.core.DONOR_DEFAULT)
    parser.add_argument("--build-id", default=BUILD_ID_DEFAULT)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    output = args.output or root / "site" / "dist" / args.build_id

    factual.VARIANTS = VARIANTS
    factual.plane_content = plane_content
    factual.FACTUAL_CSS = CONTRAST_CSS
    factual.BUILD_ID_DEFAULT = BUILD_ID_DEFAULT
    factual.build(output, args.donor, args.build_id)


if __name__ == "__main__":
    main()
