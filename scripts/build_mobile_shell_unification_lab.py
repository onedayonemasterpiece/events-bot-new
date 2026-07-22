#!/usr/bin/env python3
"""Build a noindex mobile-only A/B/C shell research lab from the accepted v23 donor.

The event rails are copied from the accepted generated v23 HTML.  This script only
replaces the global shell, search/personal specimens and route wiring, so the lab
cannot silently become another approximate reimplementation of the rail design.
"""

from __future__ import annotations

import argparse
import html
import re
import shutil
from pathlib import Path


DONOR_DEFAULT = Path(
    "/home/dev/.codex/worktrees/events-bot-new/calendar-occurrences-v21-mobile/"
    "artifacts/codex/mobile-calendar-v23-research-20260721/public"
)
BUILD_ID_DEFAULT = "preview-20260721-mobile-shell-unification-lab-v2"

VARIANTS = {
    "a": {
        "name": "Строгие строки",
        "subtitle": "время и рубрики — плоскими типографическими рядами",
        "body_class": "variant-rows",
    },
    "b": {
        "name": "Индекс",
        "subtitle": "город и профиль слева, действия текущего раздела справа",
        "body_class": "variant-index",
    },
    "c": {
        "name": "Тональные зоны",
        "subtitle": "сервисная строка и спокойный рубрикатор без отдельного футера",
        "body_class": "variant-tonal",
    },
}

ROUTES = {
    "popular": ("populyarnoe", "Популярное"),
    "calendar": ("segodnya", "Сегодня"),
    "search": ("", "Поиск"),
    "personal": ("dlya-menya", "Для меня"),
}

ICONS = {
    "popular": '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4.75 6.25A2.25 2.25 0 0 1 7 4h10a2.25 2.25 0 0 1 2.25 2.25v2.06a2.3 2.3 0 0 0 0 4.38v2.06A2.25 2.25 0 0 1 17 17H7a2.25 2.25 0 0 1-2.25-2.25v-2.06a2.3 2.3 0 0 0 0-4.38V6.25Zm5 1a.75.75 0 0 0 0 1.5h4.5a.75.75 0 0 0 0-1.5h-4.5Zm0 4a.75.75 0 0 0 0 1.5h2.8a.75.75 0 0 0 0-1.5h-2.8Z"/></svg>',
    "calendar": '<svg viewBox="0 0 24 24" aria-hidden="true"><path fill-rule="evenodd" d="M7 2a1 1 0 0 1 1 1v1h8V3a1 1 0 1 1 2 0v1h.25A2.75 2.75 0 0 1 21 6.75v11.5A2.75 2.75 0 0 1 18.25 21H5.75A2.75 2.75 0 0 1 3 18.25V6.75A2.75 2.75 0 0 1 5.75 4H6V3a1 1 0 0 1 1-1ZM5 9v9.25c0 .41.34.75.75.75h12.5c.41 0 .75-.34.75-.75V9H5Z" clip-rule="evenodd"/></svg>',
    "search": '<svg viewBox="0 0 24 24" aria-hidden="true"><path fill-rule="evenodd" d="M10.75 3a7.75 7.75 0 1 0 4.82 13.82l3.8 3.8a.88.88 0 0 0 1.24-1.24l-3.8-3.8A7.75 7.75 0 0 0 10.75 3Zm-6 7.75a6 6 0 1 1 12 0 6 6 0 0 1-12 0Z" clip-rule="evenodd"/></svg>',
    "personal": '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 2.8c.36 0 .68.23.81.56l1.78 4.55 4.55 1.78a.87.87 0 0 1 0 1.62l-4.55 1.78-1.78 4.55a.87.87 0 0 1-1.62 0l-1.78-4.55-4.55-1.78a.87.87 0 0 1 0-1.62l4.55-1.78 1.78-4.55c.13-.33.45-.56.81-.56Zm6.05 12.45c.28 0 .52.17.63.43l.58 1.38 1.38.58a.68.68 0 0 1 0 1.26l-1.38.58-.58 1.38a.68.68 0 0 1-1.26 0l-.58-1.38-1.38-.58a.68.68 0 0 1 0-1.26l1.38-.58.58-1.38a.68.68 0 0 1 .63-.43Z"/></svg>',
}


def variant_href(base: str, key: str) -> str:
    slug = ROUTES[key][0]
    return f"{base}/{slug}/" if slug else f"{base}/"


def bottom_nav(base: str, current: str) -> str:
    items = []
    for key, (_, label) in ROUTES.items():
        aria = ' aria-current="page"' if key == current else ""
        items.append(
            f'<a href="{variant_href(base, key)}"{aria}>'
            f'<span class="nav-icon-shell">{ICONS[key]}</span><span>{label if key != "calendar" else "Даты"}</span></a>'
        )
    return '<nav class="bottom-nav shell-bottom-nav" aria-label="Основная навигация">' + "".join(items) + "</nav>"


def plane_content(variant: str, base: str, current: str) -> str:
    if variant == "a":
        return f"""
          <div class="plane-row plane-row--service"><button type="button"><span>Город</span><b>Калининград и область</b></button><button type="button"><span>Аккаунт</span><b>Войти</b></button></div>
          <nav class="plane-row plane-row--primary" aria-label="Быстрый выбор времени"><a href="{base}/segodnya/">Сегодня</a><a href="{base}/segodnya/">Завтра</a><a href="{base}/populyarnoe/">Выходные</a></nav>
          <nav class="plane-row plane-row--secondary" aria-label="Рубрики"><a href="{base}/populyarnoe/">Выставки</a><a href="{base}/">Клубы</a><a href="{base}/">Бесплатно</a></nav>
        """
    if variant == "b":
        contextual = {
            "calendar": ("Сегодня", "Завтра", "Выходные"),
            "popular": ("Быстро растут", "По темам", "На выходных"),
            "search": ("Недавние", "Сохранённые", "Бесплатно"),
            "personal": ("Интересы", "Лайки", "Подписки"),
        }[current]
        return f"""
          <section class="index-column index-column--global"><span>Мой контекст</span><button type="button"><b>Калининград</b><small>+ область</small></button><button type="button"><b>Войти</b><small>синхронизировать</small></button><a href="#utility">О проекте</a></section>
          <nav class="index-column index-column--context" aria-label="Действия текущего раздела"><span>{ROUTES[current][1]}</span><a href="#canvas-action">{contextual[0]}</a><a href="#canvas-action">{contextual[1]}</a><a href="#canvas-action">{contextual[2]}</a></nav>
        """
    return f"""
      <div class="tone-service"><button type="button"><span>Город</span><b>Калининград и область</b></button><button type="button"><span>Аккаунт</span><b>Войти</b></button><a href="#utility">О проекте</a></div>
      <nav class="tone-grid" aria-label="Быстрые разделы"><a href="{base}/segodnya/"><span>Когда</span><b>Завтра</b></a><a href="{base}/populyarnoe/"><span>Когда</span><b>Выходные</b></a><a href="{base}/populyarnoe/"><span>Что</span><b>Выставки</b></a><a href="{base}/"><span>Цена</span><b>Бесплатно</b></a></nav>
    """


def shell_header(variant: str, base: str, asset_root: str, current: str, context_title: str, context_meta: str) -> str:
    content = plane_content(variant, base, current)
    wordmark = f"{asset_root}/assets/v2/brand/announcements-wordmark-ui.svg"
    return f"""
      <header class="site-header shell-header">
        <details class="mobile-discovery-menu" data-mobile-discovery-menu>
          <summary class="brand-tag mobile-discovery-menu__summary" aria-label="Открыть меню афиши">
            <span class="brand-tag__endorsement">Полюбить<br>Калининград</span>
            <img class="brand-tag__wordmark" src="{wordmark}" width="96" alt="Анонсы">
            <span class="brand-tag__chevron" aria-hidden="true"></span>
          </summary>
          <div class="mobile-discovery-menu__panel" aria-label="Меню и действия афиши">{content}</div>
        </details>
        <div class="sticky-date shell-context"><div class="sticky-date__layout"><div class="sticky-date__row1"><strong>{html.escape(context_title)}</strong></div><span class="shell-context__meta">{html.escape(context_meta)}</span></div></div>
      </header>
    """


def endcap(variant: str, base: str, current: str) -> str:
    if variant == "a":
        if current == "calendar":
            return '<div class="no-footer-marker" id="utility"><span>Календарная лента продолжается</span></div>'
        return '<footer class="mobile-micro-footer" id="utility"><a href="#utility">О проекте</a><a href="#utility">Контакты</a><a href="#utility">Документы</a></footer>'
    if variant == "b":
        return f'''<section class="discovery-endcap" id="utility"><b>Куда дальше?</b><p>Один компактный terminal на всех разделах.</p><div><a href="{base}/segodnya/">К датам</a><a href="{base}/">К поиску</a><a href="#utility">О проекте</a></div></section>'''
    return '<div class="no-footer-marker" id="utility"><span>Служебные действия находятся в плоскости бирки</span></div>'


def normalize_donor_html(source: str, base: str) -> str:
    source = source.replace('../assets/', f'{base}/assets/')
    source = source.replace('../event.html', f'{base}/event.html')
    source = re.sub(r'<link rel="stylesheet" href="[^"]+">', f'<link rel="stylesheet" href="{base}/styles.css?v=23"><link rel="stylesheet" href="{base}/lab.css?v=1">', source, count=1)
    source = re.sub(r'<script src="[^"]+app\.js[^\"]*"></script>', f'<script src="{base}/app.js?v=23"></script><script src="{base}/lab.js?v=1"></script>', source, count=1)
    return source


def donor_page(donor: Path, variant: str, base: str, asset_root: str, page: str) -> str:
    filename = "segodnya" if page == "calendar" else "populyarnoe"
    source = normalize_donor_html((donor / filename / "index.html").read_text(), asset_root)
    context = ("Сегодня", "6 событий · Вся область") if page == "calendar" else ("Популярное", "по категориям · Вся область")
    source = re.sub(r'<header class="site-header">.*?</header>', shell_header(variant, base, asset_root, page, *context), source, count=1, flags=re.S)
    source = re.sub(r'<nav class="bottom-nav".*?</nav>', bottom_nav(base, page), source, count=1, flags=re.S)
    source = re.sub(r'<section class="terminal".*?</section>', endcap(variant, base, page), source, count=1, flags=re.S)
    source = source.replace('<body class="', f'<body class="shell-lab {VARIANTS[variant]["body_class"]} ')
    if '<body class="' not in source:
        source = source.replace('<body', f'<body class="shell-lab {VARIANTS[variant]["body_class"]}"', 1)
    source = source.replace('</title>', f' · {VARIANTS[variant]["name"]}</title>', 1)
    return source


def sample_rows(donor: Path, asset_root: str, limit: int = 3) -> str:
    source = normalize_donor_html((donor / "populyarnoe" / "index.html").read_text(), asset_root)
    rows = re.findall(r'<article class="event-row".*?</article>', source, flags=re.S)
    return "".join(rows[:limit])


def custom_page(donor: Path, variant: str, base: str, asset_root: str, page: str) -> str:
    info = VARIANTS[variant]
    if page == "search":
        title, meta = "Поиск", "смысловой · по всей области"
        main = f'''
          <main class="search-specimen">
            <section class="page-head"><span class="variant-kicker">Вариант {variant.upper()} · {info["name"]}</span><h1>Найти событие</h1><p>{info["subtitle"]}</p></section>
            <form class="search-form-lab" data-search-form>
              <label for="query-{variant}">Что хочется сделать?</label>
              <textarea id="query-{variant}" rows="3">Послушать хор</textarea>
              <button class="search-progress-cta" type="submit" data-search-cta style="--cta-progress:0%"><span data-search-label>Найти события</span></button>
              <div class="sr-only" role="progressbar" aria-label="Прогресс поиска" aria-valuemin="0" aria-valuemax="100" data-search-progress></div>
              <p class="search-status" aria-live="polite" data-search-status>Прогресс будет показан внутри кнопки, без отдельной полосы.</p>
            </form>
            <section class="saved-searches"><b>Сохранённые поиски</b><div><button>бесплатно сегодня</button><button>с детьми</button><button>послушать хор</button><button>театр на выходных</button></div></section>
            <section class="search-results" data-search-results hidden><div class="feed-head"><div class="feed-head__copy"><strong>Подходящие события</strong><span>демо выдачи</span></div></div><div class="event-list">{sample_rows(donor, asset_root)}</div></section>
            {endcap(variant, base, page)}
          </main>
        '''
    else:
        title, meta = "Для меня", "личная лента · 8 идей"
        main = f'''
          <main class="personal-specimen">
            <section class="page-head"><span class="variant-kicker">Вариант {variant.upper()} · {info["name"]}</span><h1>Для меня</h1><p>Личная лента в том же каркасе, без отдельной версии шапки.</p></section>
            <section class="personal-onboarding"><div><b>Лента уже работает</b><p>Лайки и отметки «неинтересно» меняют рекомендации. Вход нужен только для синхронизации.</p></div><button type="button">Настроить интересы</button></section>
            <div class="feed-head"><div class="feed-head__copy"><strong>На основе интересов</strong><span>крупные карточки появятся позже; сейчас проверяем каркас</span></div></div>
            <section class="event-list">{sample_rows(donor, asset_root, 4)}</section>
            {endcap(variant, base, page)}
          </main>
        '''
    header = shell_header(variant, base, asset_root, page, title, meta)
    return f'''<!doctype html><html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><meta name="robots" content="noindex,nofollow,noarchive"><meta name="theme-color" content="#fbf7ef"><title>{title} · {info["name"]}</title><link rel="stylesheet" href="{asset_root}/styles.css?v=23"><link rel="stylesheet" href="{asset_root}/lab.css?v=1"></head><body class="shell-lab {info["body_class"]}" data-page="{page}">{header}{main}{bottom_nav(base, page)}<div data-city-sheet hidden aria-hidden="true"><button type="button" data-city-close>Закрыть</button></div><div class="confirm-sheet" hidden><div class="confirm-panel" role="dialog" aria-modal="true"><h2></h2><p></p><div class="actions"><button data-confirm-cancel>Отмена</button><button data-confirm-negative>Пометить неинтересным</button></div></div></div><div class="toast" role="status" aria-live="polite" hidden><span></span><button>Отменить</button></div><script src="{asset_root}/app.js?v=23"></script><script src="{asset_root}/lab.js?v=1"></script></body></html>'''


LAB_CSS = r'''
/* v2 shell-only research overrides. Accepted v23 rails remain untouched. */
:root{--drawer-ease:cubic-bezier(.22,.86,.32,1);--shell-tag-w:120px;--shell-tag-h:84px;--shell-header-h:64px;--plane-bg:#fbf7ef;--plane-alt:#fffdf8;--hairline:#e7d8c8;--text-main:#221a14;--text-sec:#776b61;--accent:#98401f}
body.shell-lab{font-family:Inter,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:var(--plane-bg);color:var(--text-main)}
.shell-header{height:var(--shell-header-h);background:transparent;z-index:26;pointer-events:none}
.shell-context{z-index:1;display:flex;align-items:center;min-width:0;transition:opacity 120ms ease}.shell-context__meta{display:block;margin-top:3px;font-size:10.5px;line-height:12px;color:var(--text-sec);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.shell-menu-open .shell-context{opacity:0;visibility:hidden}
.mobile-discovery-menu{--plane-h:152px;position:fixed;inset:0 0 auto 0;z-index:28;display:block;width:100%;height:calc(var(--plane-h) + var(--shell-tag-h));color:var(--text-main);pointer-events:none;transform:translate3d(0,calc(-1 * var(--plane-h) - env(safe-area-inset-top)),0);transition:transform 320ms var(--drawer-ease);will-change:transform}
.variant-index .mobile-discovery-menu{--plane-h:160px}.variant-tonal .mobile-discovery-menu{--plane-h:184px}
.mobile-discovery-menu[open]{transform:translate3d(0,0,0)}.mobile-discovery-menu.is-closing{transform:translate3d(0,calc(-1 * var(--plane-h) - env(safe-area-inset-top)),0)}
.mobile-discovery-menu__summary{position:absolute!important;left:max(12px,env(safe-area-inset-left))!important;top:calc(var(--plane-h) + env(safe-area-inset-top))!important;z-index:2!important;box-sizing:border-box!important;width:var(--shell-tag-w)!important;height:calc(var(--shell-tag-h) + env(safe-area-inset-top))!important;min-height:0!important;display:grid!important;grid-template-rows:1fr auto;align-content:end;overflow:hidden;isolation:isolate;padding:max(18px,calc(env(safe-area-inset-top) + 13px)) 8px 9px!important;border:0!important;border-radius:0 0 11px 11px!important;background:var(--accent)!important;color:var(--plane-alt)!important;box-shadow:0 9px 20px rgba(72,45,25,.16)!important;list-style:none;cursor:pointer;pointer-events:auto;touch-action:manipulation}
.mobile-discovery-menu__summary::-webkit-details-marker{display:none}.mobile-discovery-menu__summary::marker{content:""}.mobile-discovery-menu__summary .brand-tag__endorsement{align-self:end;font-size:7.5px;line-height:8px;letter-spacing:.075em;font-weight:750;text-transform:uppercase}.mobile-discovery-menu__summary .brand-tag__wordmark{display:block;width:96px;height:auto;align-self:end}.brand-tag__chevron{position:absolute;right:8px;bottom:8px;width:6px;height:6px;border-right:1.5px solid currentColor;border-bottom:1.5px solid currentColor;transform:rotate(45deg);transition:transform 180ms ease}.mobile-discovery-menu[open] .brand-tag__chevron{transform:rotate(225deg) translate(-1px,-1px)}
.mobile-discovery-menu__panel{position:absolute;inset:0 0 auto 0;z-index:1;box-sizing:border-box;width:100%;height:calc(var(--plane-h) + env(safe-area-inset-top));padding-top:env(safe-area-inset-top);overflow:hidden;background:var(--plane-bg);border-bottom:1px solid var(--hairline);color:var(--text-main);visibility:hidden;pointer-events:none;transition:visibility 0s linear 320ms}
.mobile-discovery-menu[open] .mobile-discovery-menu__panel{visibility:visible;pointer-events:auto;transition-delay:0s}.mobile-discovery-menu.is-closing .mobile-discovery-menu__panel{visibility:visible;pointer-events:none}
/* A · strict rows */
.plane-row{box-sizing:border-box;display:grid;align-items:stretch;margin:0;border-bottom:1px solid var(--hairline)}.plane-row>*{min-width:0;display:flex;align-items:center;border:0;background:transparent;color:inherit;text-decoration:none;text-align:left;font-family:inherit}.plane-row--service{height:48px;grid-template-columns:minmax(0,1fr) 112px;padding:0 14px}.plane-row--service>*{display:flex;flex-direction:column;align-items:flex-start;justify-content:center}.plane-row--service>*:last-child{border-left:1px solid var(--hairline);padding-left:14px}.plane-row--service span{font-size:9px;line-height:10px;text-transform:uppercase;letter-spacing:.08em;color:var(--text-sec)}.plane-row--service b{display:block;max-width:100%;font-size:12px;line-height:15px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.plane-row--primary{height:52px;grid-template-columns:repeat(3,minmax(0,1fr));padding:0 14px}.plane-row--primary a{font-size:17px;line-height:19px;font-weight:780}.plane-row--primary a+a{border-left:1px solid var(--hairline);padding-left:12px}.plane-row--secondary{height:52px;grid-template-columns:repeat(3,minmax(0,1fr));padding:0 14px;border-bottom:0}.plane-row--secondary a{font-size:12px;line-height:15px;font-weight:680;color:#5f5147}.plane-row--secondary a+a{border-left:1px solid var(--hairline);padding-left:12px}
/* B · index */
.variant-index .mobile-discovery-menu__panel{display:grid;grid-template-columns:45% 55%;padding-inline:14px}.index-column{box-sizing:border-box;min-width:0;height:160px;padding:16px 12px 10px 2px;display:flex;flex-direction:column;align-items:flex-start;gap:0}.index-column+ .index-column{border-left:1px solid var(--hairline);padding-left:16px}.index-column>span{margin-bottom:9px;font-size:9px;line-height:11px;font-weight:800;text-transform:uppercase;letter-spacing:.09em;color:var(--accent)}.index-column a,.index-column button{box-sizing:border-box;width:100%;min-height:33px;display:flex;align-items:baseline;gap:4px;padding:0;border:0;border-bottom:1px solid var(--hairline);background:transparent;color:var(--text-main);text-decoration:none;text-align:left;font:680 13px/15px inherit}.index-column small{font-size:9px;color:var(--text-sec)}.index-column--global>a{font-size:10px;color:var(--text-sec);border-bottom:0}.index-column--context a{font-size:14px;line-height:16px}
/* C · tonal zones */
.variant-tonal .mobile-discovery-menu__panel{background:var(--plane-bg)}.tone-service{box-sizing:border-box;height:56px;padding:0 14px;display:grid;grid-template-columns:minmax(0,1fr) 78px 58px;align-items:stretch;background:var(--plane-alt);border-bottom:1px solid var(--hairline)}.tone-service>*{min-width:0;display:flex;flex-direction:column;justify-content:center;align-items:flex-start;border:0;background:transparent;color:inherit;text-decoration:none;font-family:inherit}.tone-service>*+*{border-left:1px solid var(--hairline);padding-left:10px}.tone-service span{font-size:8.5px;line-height:10px;text-transform:uppercase;letter-spacing:.07em;color:var(--text-sec)}.tone-service b{max-width:100%;font-size:11px;line-height:14px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.tone-service>a{font-size:9px;line-height:12px;color:var(--text-sec)}.tone-grid{height:128px;display:grid;grid-template-columns:1fr 1fr;grid-template-rows:1fr 1fr}.tone-grid a{box-sizing:border-box;min-width:0;padding:12px 14px;display:flex;flex-direction:column;justify-content:center;color:inherit;text-decoration:none}.tone-grid a:nth-child(even){border-left:1px solid var(--hairline)}.tone-grid a:nth-child(n+3){border-top:1px solid var(--hairline)}.tone-grid span{font-size:8.5px;line-height:10px;text-transform:uppercase;letter-spacing:.08em;color:var(--text-sec)}.tone-grid b{font-size:15px;line-height:18px;margin-top:2px}
.shell-bottom-nav{z-index:40!important}.shell-lab .date-accessory{z-index:39!important}.variant-kicker{display:block;margin-bottom:7px;font-size:9px;line-height:11px;font-weight:800;letter-spacing:.09em;text-transform:uppercase;color:var(--accent)}
.search-specimen,.personal-specimen{max-width:390px;margin:0 auto;padding-top:96px;padding-bottom:96px}.search-specimen .page-head,.personal-specimen .page-head{padding-bottom:16px}.search-form-lab{margin:0 14px 20px;padding:14px 0 18px;border-top:1px solid var(--hairline);border-bottom:1px solid var(--hairline)}.search-form-lab label{display:block;font-size:10px;line-height:12px;font-weight:800;text-transform:uppercase;letter-spacing:.08em;color:var(--accent);margin-bottom:8px}.search-form-lab textarea{box-sizing:border-box;width:100%;min-height:82px;resize:none;border:0;border-bottom:2px solid var(--text-main);border-radius:0;background:transparent;padding:4px 0 10px;font:650 21px/1.25 inherit;color:var(--text-main);outline-offset:4px}.search-progress-cta{--cta-progress:0%;position:relative;isolation:isolate;box-sizing:border-box;width:100%;height:50px;margin-top:12px;overflow:hidden;border:0;border-radius:8px;background:#221a14;color:#fffdf8;font:800 14px/1 inherit;letter-spacing:.01em;cursor:pointer}.search-progress-cta::before{content:"";position:absolute;z-index:-1;inset:0 auto 0 0;width:var(--cta-progress);background:var(--accent);transition:width 240ms var(--drawer-ease)}.search-progress-cta[data-state="requesting"]{background:#30241d}.search-progress-cta[data-state="done"]::before{background:var(--accent)}.search-progress-cta span{position:relative;z-index:2}.search-status{min-height:28px;margin:8px 0 0;font-size:10.5px;line-height:14px;color:var(--text-sec)}.sr-only{position:absolute!important;width:1px!important;height:1px!important;padding:0!important;margin:-1px!important;overflow:hidden!important;clip:rect(0,0,0,0)!important;white-space:nowrap!important;border:0!important}
.saved-searches{margin:0 14px 22px;border-top:1px solid var(--hairline)}.saved-searches>b{display:block;padding:11px 0 7px;font-size:10px;line-height:12px;text-transform:uppercase;letter-spacing:.08em;color:var(--text-sec)}.saved-searches>div{display:block;margin:0}.saved-searches button{box-sizing:border-box;width:100%;display:flex;align-items:center;min-height:38px;border:0;border-bottom:1px solid var(--hairline);border-radius:0;background:transparent;padding:0;color:var(--text-main);text-align:left;font:700 13px/16px inherit}.saved-searches button::after{content:"→";margin-left:auto;color:var(--accent);font-size:18px}.search-results .event-list,.personal-specimen>.event-list{padding-top:0}.search-results .event-row,.personal-specimen>.event-list .event-row{margin-top:8px}
.personal-onboarding{margin:0 14px 18px;padding:13px 0;display:flex;gap:12px;align-items:flex-end;border-top:1px solid var(--hairline);border-bottom:1px solid var(--hairline);background:transparent}.personal-onboarding div{min-width:0}.personal-onboarding b{font-size:14px}.personal-onboarding p{margin:5px 0 0;font-size:11px;line-height:15px;color:var(--text-sec)}.personal-onboarding button{flex:0 0 104px;border:0;border-bottom:2px solid var(--accent);background:transparent;color:var(--accent);padding:8px 0;font:780 11px/1.2 inherit}.personal-specimen>.feed-head{box-sizing:border-box;max-width:100%;overflow:hidden}.personal-specimen>.feed-head .feed-head__copy{min-width:0;max-width:100%}.personal-specimen>.feed-head .feed-head__copy>*{max-width:100%;overflow:hidden;text-overflow:ellipsis}
.mobile-micro-footer{margin:28px 14px 14px;padding:12px 0;border-top:1px solid var(--hairline);display:flex;gap:16px;flex-wrap:wrap}.mobile-micro-footer a{font-size:10px;line-height:13px;color:var(--text-sec);text-decoration:none}.discovery-endcap{margin:28px 14px 14px;padding:14px 0;border-top:1px solid var(--hairline);border-bottom:1px solid var(--hairline);background:transparent}.discovery-endcap>b{font-size:17px;line-height:20px}.discovery-endcap p{margin:4px 0 10px;font-size:11px;line-height:15px;color:var(--text-sec)}.discovery-endcap div{display:flex;gap:15px;flex-wrap:wrap}.discovery-endcap a{color:var(--accent);text-decoration:underline;text-underline-offset:3px;font:720 11px/1.3 inherit}.no-footer-marker{height:24px;margin:20px 14px 8px;text-align:center;color:#aa9e94;font-size:8.5px;line-height:24px}
@media(max-width:350px){.shell-context{left:145px!important}.plane-row--service{grid-template-columns:minmax(0,1fr) 94px;padding-inline:12px}.plane-row--primary,.plane-row--secondary{padding-inline:12px}.plane-row--primary a{font-size:15px}.plane-row--primary a+a,.plane-row--secondary a+a{padding-left:8px}.variant-index .mobile-discovery-menu__panel{grid-template-columns:44% 56%;padding-inline:12px}.index-column{padding-right:8px}.index-column+ .index-column{padding-left:12px}.index-column--context a{font-size:12px}.tone-service{grid-template-columns:minmax(0,1fr) 70px 52px;padding-inline:12px}.tone-grid a{padding-inline:12px}.personal-onboarding{display:block}.personal-onboarding button{margin-top:9px}}
@media(prefers-reduced-motion:reduce){.mobile-discovery-menu,.brand-tag__chevron,.shell-context,.search-progress-cta::before{transition:none!important}}
'''

LAB_JS = r'''
(() => {
  const menu = document.querySelector('[data-mobile-discovery-menu]');
  const summary = menu?.querySelector('summary');
  let drawerTimer = 0;
  let openedAtY = 0;
  const syncState = (open) => {
    document.body.classList.toggle('shell-menu-open', open);
    summary?.setAttribute('aria-expanded', String(open));
  };
  const closeMenu = ({ returnFocus = false } = {}) => {
    if (!menu?.hasAttribute('open')) return;
    clearTimeout(drawerTimer);
    menu.classList.remove('is-opening');
    menu.classList.add('is-closing');
    syncState(false);
    drawerTimer = setTimeout(() => {
      menu.removeAttribute('open');
      menu.classList.remove('is-closing');
      if (returnFocus) summary?.focus({ preventScroll: true });
    }, matchMedia('(prefers-reduced-motion: reduce)').matches ? 0 : 340);
  };
  const openMenu = () => {
    if (!menu) return;
    clearTimeout(drawerTimer);
    openedAtY = scrollY || 0;
    menu.setAttribute('open', '');
    menu.classList.remove('is-closing');
    syncState(true);
    requestAnimationFrame(() => menu.classList.add('is-opening'));
  };
  summary?.setAttribute('aria-expanded', 'false');
  summary?.addEventListener('click', (event) => {
    event.preventDefault();
    if (menu.hasAttribute('open') && !menu.classList.contains('is-closing')) closeMenu();
    else openMenu();
  });
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') closeMenu({ returnFocus: true });
  });
  document.addEventListener('click', (event) => {
    if (menu?.hasAttribute('open') && !menu.contains(event.target)) closeMenu();
  });
  document.addEventListener('scroll', () => {
    if (menu?.hasAttribute('open') && Math.abs((scrollY || 0) - openedAtY) > 24) closeMenu();
  }, { passive: true });
  menu?.querySelectorAll('a').forEach((link) => link.addEventListener('click', () => closeMenu()));

  const form = document.querySelector('[data-search-form]');
  const cta = document.querySelector('[data-search-cta]');
  const label = document.querySelector('[data-search-label]');
  const semantic = document.querySelector('[data-search-progress]');
  const status = document.querySelector('[data-search-status]');
  const results = document.querySelector('[data-search-results]');
  let epoch = 0;
  const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  form?.addEventListener('submit', async (event) => {
    event.preventDefault();
    const mine = ++epoch;
    cta.disabled = true;
    cta.dataset.state = 'requesting';
    cta.style.setProperty('--cta-progress', '0%');
    cta.setAttribute('aria-busy', 'true');
    label.textContent = 'Подключаю поиск…';
    status.textContent = 'Готовлю смысловой поиск';
    semantic.removeAttribute('aria-valuenow');
    semantic.removeAttribute('aria-valuetext');
    await wait(520);
    if (mine !== epoch) return;
    const frames = [
      [12, 'Ищу · 12%', 'Понимаю запрос'],
      [38, 'Ищу · 38%', 'Сопоставляю интересы'],
      [64, 'Ищу · 64%', 'Проверяю события'],
      [86, 'Ищу · 86%', 'Сортирую выдачу'],
      [100, 'Готово · 3 события', 'Результаты готовы'],
    ];
    cta.dataset.state = 'determinate';
    for (const [value, text, message] of frames) {
      if (mine !== epoch) return;
      cta.style.setProperty('--cta-progress', `${value}%`);
      label.textContent = text;
      status.textContent = message;
      semantic.setAttribute('aria-valuenow', String(value));
      semantic.setAttribute('aria-valuetext', message);
      await wait(value === 100 ? 780 : 430);
    }
    if (mine !== epoch) return;
    results.hidden = false;
    cta.dataset.state = 'idle';
    cta.style.setProperty('--cta-progress', '0%');
    label.textContent = 'Искать снова';
    cta.disabled = false;
    cta.removeAttribute('aria-busy');
    results.scrollIntoView({ behavior: matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth', block: 'start' });
  });
})();
'''


def root_index(build_id: str) -> str:
    links = "".join(
        f'<a href="/{build_id}/variant-{key}/"><b>{key.upper()} · {data["name"]}</b><span>{data["subtitle"]}</span></a>'
        for key, data in VARIANTS.items()
    )
    return f'''<!doctype html><html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex,nofollow"><title>Mobile shell lab</title><style>body{{margin:0;padding:28px 16px;background:#fbf7ef;color:#221a14;font-family:Inter,system-ui,sans-serif}}main{{max-width:390px;margin:auto}}h1{{font-size:32px;line-height:1;margin:0 0 12px}}p{{color:#776b61}}a{{display:block;margin:10px 0;padding:16px;background:#fffdf8;border-left:4px solid #a54821;color:inherit;text-decoration:none}}a b,a span{{display:block}}a span{{margin-top:5px;font-size:12px;color:#776b61}}</style></head><body><main><h1>Единый mobile shell</h1><p>Три отдельные концепции. Внутри каждой работают четыре одинаково оформленных раздела и поиск с прогрессом внутри кнопки.</p>{links}</main></body></html>'''


def build(output: Path, donor: Path, build_id: str) -> None:
    if not donor.exists():
        raise SystemExit(f"Missing accepted v23 donor: {donor}")
    if output.exists():
        shutil.rmtree(output)
    output.mkdir(parents=True)
    shutil.copytree(donor / "assets", output / "assets")
    shutil.copy2(donor / "styles.css", output / "styles.css")
    shutil.copy2(donor / "app.js", output / "app.js")
    shutil.copy2(donor / "event.html", output / "event.html")
    (output / "lab.css").write_text(LAB_CSS)
    (output / "lab.js").write_text(LAB_JS)
    (output / "index.html").write_text(root_index(build_id))
    (output / "__preview").mkdir()
    (output / "__preview" / "index.html").write_text(root_index(build_id))
    base_root = f"/{build_id}"
    for variant in VARIANTS:
        variant_dir = output / f"variant-{variant}"
        variant_dir.mkdir()
        base = f"{base_root}/variant-{variant}"
        (variant_dir / "index.html").write_text(custom_page(donor, variant, base, base_root, "search"))
        for page in ("calendar", "popular", "personal"):
            slug = ROUTES[page][0]
            page_dir = variant_dir / slug
            page_dir.mkdir()
            rendered = donor_page(donor, variant, base, base_root, page) if page in {"calendar", "popular"} else custom_page(donor, variant, base, base_root, page)
            (page_dir / "index.html").write_text(rendered)
    (output / "robots.txt").write_text("User-agent: *\nDisallow: /\n")
    print(f"Built {output}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--donor", type=Path, default=DONOR_DEFAULT)
    parser.add_argument("--build-id", default=BUILD_ID_DEFAULT)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    output = args.output or root / "site" / "dist" / args.build_id
    build(output, args.donor, args.build_id)


if __name__ == "__main__":
    main()
