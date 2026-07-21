#!/usr/bin/env python3
"""Build the D/E/F mobile-shell lab focused on global top navigation.

The accepted v23 Calendar/Popular output and the v2 whole-object drawer motion
remain the donors.  This file only supplies the global plane IA, footer policies
and a new isolated preview build id.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import build_mobile_shell_unification_lab as core


BUILD_ID_DEFAULT = "preview-20260721-mobile-shell-global-nav-lab-v3"

VARIANTS = {
    "d": {
        "name": "Глобальное меню",
        "subtitle": "только постоянная навигация и сервисы сайта",
        "body_class": "variant-global-menu",
    },
    "e": {
        "name": "Карта афиши",
        "subtitle": "устойчивые сущности сайта вместо локальных фильтров",
        "body_class": "variant-global-map",
    },
    "f": {
        "name": "Глобальное + контекст",
        "subtitle": "глобальная база и одна честно отделённая строка страницы",
        "body_class": "variant-global-hybrid",
    },
}


def service_row(height_class: str = "") -> str:
    return f"""
      <div class="global-service {height_class}">
        <button type="button"><span>Город</span><b>Калининград и область</b></button>
        <button type="button"><span>Аккаунт</span><b>Войти</b></button>
      </div>
    """


def plane_content(variant: str, base: str, current: str) -> str:
    del base
    if variant == "d":
        return service_row() + """
          <nav class="global-destinations" aria-label="Глобальные разделы афиши">
            <a href="#rubrics">Рубрики</a><a href="#venues">Площадки</a><a href="#collections">Подборки</a>
          </nav>
          <nav class="global-utility" aria-label="О сервисе">
            <a href="#about">О проекте</a><a href="#support">Поддержка</a><a href="#documents">Документы</a>
          </nav>
        """
    if variant == "e":
        return service_row("global-service--map") + """
          <div class="global-map-columns">
            <nav aria-label="Каталог"><span>Каталог</span><a href="#venues">Площадки</a><a href="#organizers">Организаторы</a><a href="#cities">Города</a></nav>
            <nav aria-label="Редакция"><span>Редакция</span><a href="#collections">Подборки</a><a href="#journal">Журнал</a><a href="#about">О проекте</a></nav>
          </div>
        """

    contextual = {
        "search": ("Недавние", "Сохранённые"),
        "calendar": ("Сегодня", "Завтра"),
        "popular": ("Растут", "Бесплатно"),
        "personal": ("Лайки", "Настройки"),
    }[current]
    return service_row("global-service--hybrid") + f"""
      <nav class="hybrid-global-links" aria-label="Глобальные разделы афиши">
        <a href="#rubrics">Рубрики</a><a href="#venues">Площадки</a><a href="#collections">Подборки</a>
      </nav>
      <nav class="hybrid-utility" aria-label="О сервисе"><a href="#about">О проекте</a><a href="#support">Поддержка</a></nav>
      <nav class="hybrid-context" aria-label="Действия текущей страницы">
        <span>На этой странице</span><a href="#canvas-action">{contextual[0]}</a><a href="#canvas-action">{contextual[1]}</a>
      </nav>
    """


def endcap(variant: str, base: str, current: str) -> str:
    del base, current
    if variant == "d":
        return ""
    return """<footer class="mobile-micro-footer global-footer" id="utility">
      <a href="#about">О проекте</a><a href="#contacts">Контакты</a><a href="#documents">Документы</a>
    </footer>"""


GLOBAL_NAV_CSS = r'''
/* v3 D/E/F: top plane is global navigation first. */
.variant-global-menu .mobile-discovery-menu{--plane-h:164px}
.variant-global-map .mobile-discovery-menu{--plane-h:180px}
.variant-global-hybrid .mobile-discovery-menu{--plane-h:186px}
.global-service{box-sizing:border-box;height:56px;display:grid;grid-template-columns:minmax(0,1fr) 112px;padding:0 14px;background:var(--plane-alt);border-bottom:1px solid var(--hairline)}
.global-service--map{height:50px}.global-service--hybrid{height:52px}
.global-service>button{box-sizing:border-box;min-width:0;display:flex;flex-direction:column;align-items:flex-start;justify-content:center;padding:0;border:0;border-radius:0;background:transparent;color:inherit;text-align:left;font-family:inherit}
.global-service>button+button{border-left:1px solid var(--hairline);padding-left:14px}
.global-service span{font-size:8.5px;line-height:10px;font-weight:800;letter-spacing:.08em;text-transform:uppercase;color:var(--text-sec)}
.global-service b{display:block;max-width:100%;font-size:12px;line-height:15px;font-weight:720;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.global-destinations{box-sizing:border-box;height:62px;display:grid;grid-template-columns:repeat(3,minmax(0,1fr));padding:0 14px;border-bottom:1px solid var(--hairline)}
.global-destinations a,.hybrid-global-links a{min-width:0;display:flex;align-items:center;color:inherit;text-decoration:none;font-size:15px;line-height:18px;font-weight:760}
.global-destinations a+a,.hybrid-global-links a+a{border-left:1px solid var(--hairline);padding-left:12px}
.global-utility{box-sizing:border-box;height:46px;display:grid;grid-template-columns:repeat(3,minmax(0,1fr));padding:0 14px}
.global-utility a,.hybrid-utility a{min-width:0;display:flex;align-items:center;color:var(--text-sec);text-decoration:none;font-size:10.5px;line-height:13px;font-weight:660}
.global-utility a+a,.hybrid-utility a+a{border-left:1px solid var(--hairline);padding-left:12px}
.global-map-columns{box-sizing:border-box;height:130px;display:grid;grid-template-columns:1fr 1fr;padding:11px 14px 9px}
.global-map-columns nav{box-sizing:border-box;min-width:0;display:grid;grid-template-rows:18px repeat(3,1fr);align-items:center}
.global-map-columns nav+nav{border-left:1px solid var(--hairline);padding-left:18px}
.global-map-columns span{font-size:9px;line-height:11px;font-weight:820;letter-spacing:.09em;text-transform:uppercase;color:var(--accent)}
.global-map-columns a{display:flex;align-items:center;min-width:0;border-bottom:1px solid var(--hairline);color:inherit;text-decoration:none;font-size:13px;line-height:16px;font-weight:680}
.global-map-columns a:last-child{border-bottom:0}
.hybrid-global-links{box-sizing:border-box;height:52px;display:grid;grid-template-columns:repeat(3,minmax(0,1fr));padding:0 14px;border-bottom:1px solid var(--hairline)}
.hybrid-utility{box-sizing:border-box;height:34px;display:grid;grid-template-columns:1fr 1fr;padding:0 14px;border-bottom:1px solid var(--hairline)}
.hybrid-context{box-sizing:border-box;height:48px;display:grid;grid-template-columns:104px repeat(2,minmax(0,1fr));align-items:stretch;padding:0 14px;background:var(--plane-alt)}
.hybrid-context>*{min-width:0;display:flex;align-items:center}
.hybrid-context span{font-size:8px;line-height:10px;font-weight:800;letter-spacing:.06em;text-transform:uppercase;color:var(--text-sec)}
.hybrid-context a{border-left:1px solid var(--hairline);padding-left:10px;color:inherit;text-decoration:none;font-size:11px;line-height:14px;font-weight:720;white-space:nowrap}
.mobile-discovery-menu__panel a:active,.mobile-discovery-menu__panel button:active{background:rgba(152,64,31,.06)}
.mobile-discovery-menu__panel a:focus-visible,.mobile-discovery-menu__panel button:focus-visible{outline:2px solid var(--accent);outline-offset:-3px}
.global-footer{margin-bottom:22px}
@media(max-width:350px){
  .global-service{grid-template-columns:minmax(0,1fr) 96px;padding-inline:12px}.global-service>button+button{padding-left:10px}.global-service b{font-size:11px}
  .global-destinations,.global-utility,.hybrid-global-links,.hybrid-utility{padding-inline:12px}
  .global-destinations a,.hybrid-global-links a{font-size:13px}.global-destinations a+a,.hybrid-global-links a+a,.global-utility a+a,.hybrid-utility a+a{padding-left:8px}
  .global-map-columns{padding-inline:12px}.global-map-columns nav+nav{padding-left:13px}.global-map-columns a{font-size:12px}
  .hybrid-context{grid-template-columns:88px repeat(2,minmax(0,1fr));padding-inline:12px}.hybrid-context span{font-size:7px;letter-spacing:.045em}.hybrid-context a{padding-left:7px;font-size:10px}
}
'''


def build(output: Path, donor: Path, build_id: str) -> None:
    core.VARIANTS = VARIANTS
    core.plane_content = plane_content
    core.endcap = endcap
    previous_css = core.LAB_CSS
    try:
        core.LAB_CSS = previous_css + GLOBAL_NAV_CSS
        core.build(output, donor, build_id)
    finally:
        core.LAB_CSS = previous_css


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
