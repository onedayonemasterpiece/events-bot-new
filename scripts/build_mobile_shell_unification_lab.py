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
BUILD_ID_DEFAULT = "preview-20260721-mobile-shell-unification-lab-v1"

VARIANTS = {
    "a": {
        "name": "Бирка-лента",
        "subtitle": "быстрое верхнее меню в две строки",
        "body_class": "variant-ribbon",
    },
    "b": {
        "name": "Бирка-панель",
        "subtitle": "единый центр города, аккаунта и навигации",
        "body_class": "variant-panel",
    },
    "c": {
        "name": "Бирка-минимум",
        "subtitle": "наверху только глобальные действия",
        "body_class": "variant-minimal",
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


def sheet_content(variant: str, base: str) -> str:
    if variant == "a":
        return f"""
          <div class="sheet-title"><b>Быстро выбрать</b><span>не повторяет разделы снизу</span></div>
          <nav class="sheet-primary" aria-label="Быстрый выбор">
            <a href="{base}/segodnya/">Завтра</a><a href="{base}/populyarnoe/">Выходные</a>
            <a href="{base}/populyarnoe/">Выставки</a><a href="{base}/">Бесплатно</a>
          </nav>
          <nav class="sheet-quick" aria-label="Идеи поиска">
            <a href="{base}/">Послушать хор</a><a href="{base}/">С детьми</a><a href="{base}/">Под открытым небом</a>
          </nav>
          <div class="sheet-utility"><button type="button">Калининград и область</button><button type="button">Войти</button><a href="#utility">О проекте</a></div>
        """
    if variant == "b":
        return f"""
          <div class="sheet-title"><b>Меню афиши</b><span>всё важное в одном месте</span></div>
          <div class="sheet-groups">
            <section><h2>Когда</h2><div><a href="{base}/segodnya/">Сегодня</a><a href="{base}/segodnya/">Завтра</a><a href="{base}/populyarnoe/">Выходные</a></div></section>
            <section><h2>Что</h2><div><a href="{base}/populyarnoe/">Выставки</a><a href="{base}/">Бесплатно</a><a href="{base}/">С детьми</a></div></section>
            <section><h2>Мой контекст</h2><div><button type="button">2 города выбрано</button><button type="button">Войти или подписаться</button></div></section>
          </div>
          <div class="sheet-utility"><a href="#utility">О проекте</a><a href="#utility">Контакты</a><a href="#utility">Документы</a></div>
        """
    return f"""
      <div class="sheet-title"><b>Настройки афиши</b><span>разделы всегда доступны снизу</span></div>
      <div class="minimal-actions"><button type="button"><b>Калининград и область</b><span>Сменить города</span></button><button type="button"><b>Войти</b><span>Лайки и подписка</span></button></div>
      <div class="sheet-utility"><a href="#utility">О проекте</a><a href="#utility">Контакты</a><a href="#utility">Документы</a></div>
    """


def shell_header(variant: str, base: str, asset_root: str, current: str, context_title: str, context_meta: str) -> str:
    content = sheet_content(variant, base)
    wordmark = f"{asset_root}/assets/v2/brand/announcements-wordmark-ui.svg"
    return f"""
      <div class="shell-scrim" data-shell-close hidden></div>
      <section class="top-sheet" id="top-sheet-{variant}" aria-label="Меню" aria-hidden="true">
        <button class="sheet-close" type="button" data-shell-close aria-label="Закрыть меню">×</button>
        {content}
      </section>
      <header class="site-header shell-header">
        <button class="brand-tag shell-trigger" type="button" data-shell-trigger aria-controls="top-sheet-{variant}" aria-expanded="false">
          <span class="brand-tag__endorsement">Полюбить<br>Калининград</span>
          <img class="brand-tag__wordmark" src="{wordmark}" width="96" alt="Анонсы">
          <span class="brand-tag__chevron" aria-hidden="true"></span>
        </button>
        <div class="sticky-date shell-context"><div class="sticky-date__layout"><div class="sticky-date__row1"><strong>{html.escape(context_title)}</strong></div><span class="shell-context__meta">{html.escape(context_meta)}</span></div></div>
      </header>
    """


def endcap(variant: str, base: str, current: str) -> str:
    if variant == "a":
        return '<div class="no-footer-marker" id="utility"><span>Служебные ссылки находятся в меню бирки</span></div>'
    if variant == "b":
        return f'''<section class="discovery-endcap" id="utility"><b>Куда дальше?</b><p>Компактное завершение одинаково на всех четырёх страницах.</p><div><a href="{base}/segodnya/">Сегодня</a><a href="{base}/">Поиск</a><button type="button">О проекте и документы</button></div></section>'''
    if current in {"popular", "personal"}:
        return f'''<section class="discovery-endcap contextual-endcap" id="utility"><b>{"Ещё идеи" if current == "popular" else "Подборка закончилась"}</b><p>{"Продолжить с календарём или поиском." if current == "popular" else "Уточнить интересы или посмотреть всю афишу."}</p><div><a href="{base}/segodnya/">К датам</a><a href="{base}/">К поиску</a></div></section>'''
    return '<div class="no-footer-marker" id="utility"><span>Без терминального блока на этой ленте</span></div>'


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
/* Shell-only research overrides. Accepted v23 rail selectors above remain untouched. */
:root{--shell-ease:cubic-bezier(.2,.8,.2,1);--shell-tag-w:120px;--shell-tag-h:84px;--shell-header-h:64px}
html.shell-open,html.shell-open body{overflow:hidden;overscroll-behavior:none}
body.shell-lab{font-family:Inter,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
.shell-lab .brand-tag{left:12px;width:120px;height:84px}
.shell-header{height:var(--shell-header-h);background:transparent;z-index:70;pointer-events:none}
.shell-trigger{pointer-events:auto;border:0;text-align:left;cursor:pointer;padding:0;overflow:visible}
.shell-trigger .brand-tag__chevron{position:absolute;right:8px;bottom:8px;width:7px;height:7px;border-right:1.5px solid #fff;border-bottom:1.5px solid #fff;transform:rotate(45deg);transition:transform .22s var(--shell-ease)}
.shell-trigger[aria-expanded="true"] .brand-tag__chevron{transform:rotate(225deg) translate(-2px,-2px)}
.shell-context{display:flex;align-items:center;min-width:0}.shell-context__meta{display:block;margin-top:3px;font-size:10.5px;line-height:12px;color:#776b61;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.shell-scrim{position:fixed;inset:0;background:rgba(34,26,20,.26);z-index:55;opacity:0;transition:opacity .22s var(--shell-ease)}
.shell-scrim.is-visible{opacity:1}.top-sheet{position:fixed;left:0;right:0;top:0;z-index:60;box-sizing:border-box;background:#fffdf8;color:#221a14;padding:18px 16px 18px 148px;border-bottom:1px solid rgba(121,48,20,.18);transform:translateY(-105%);visibility:hidden;transition:transform .24s var(--shell-ease),visibility 0s .24s;box-shadow:0 10px 30px rgba(65,43,27,.12)}
.top-sheet.is-open{transform:translateY(0);visibility:visible;transition-delay:0s}.sheet-close{position:absolute;right:12px;top:10px;border:0;background:transparent;font:400 28px/1 system-ui;color:#776b61;width:40px;height:40px}.sheet-title{min-height:48px;padding-right:38px;display:flex;flex-direction:column;justify-content:center}.sheet-title b{font-size:17px;line-height:20px}.sheet-title span{font-size:11px;line-height:14px;color:#776b61}.sheet-primary,.sheet-quick,.sheet-utility{display:flex;gap:7px;overflow-x:auto;scrollbar-width:none}.sheet-primary a,.sheet-quick a,.sheet-utility a,.sheet-utility button,.sheet-groups a,.sheet-groups button{font:650 12px/1.2 inherit;color:#34271f;text-decoration:none;background:#f4ede3;border:0;padding:10px 12px;white-space:nowrap}.sheet-primary{margin:8px 0}.sheet-primary a{background:#a54821;color:#fff}.sheet-quick a{border-bottom:1px solid rgba(121,48,20,.25);background:transparent;padding-left:2px;padding-right:14px}.sheet-utility{margin-top:12px;padding-top:10px;border-top:1px solid rgba(121,48,20,.12)}.sheet-utility a,.sheet-utility button{padding:7px 8px;background:transparent;color:#776b61}
.variant-panel .top-sheet{padding-left:16px;padding-top:88px;border-radius:0 0 16px 16px;box-shadow:0 14px 28px rgba(72,45,25,.13);transition-duration:.3s}.variant-panel .sheet-title{position:absolute;left:148px;top:16px}.sheet-groups{display:grid;grid-template-columns:1fr 1fr;gap:8px}.sheet-groups section{background:#f7f0e7;padding:10px;min-width:0}.sheet-groups section:last-child{grid-column:1/-1}.sheet-groups h2{margin:0 0 8px;font-size:10px;line-height:12px;text-transform:uppercase;letter-spacing:.08em;color:#8b7768}.sheet-groups section>div{display:flex;gap:5px;overflow-x:auto}.sheet-groups a,.sheet-groups button{padding:8px;background:#fffdf8}
.variant-minimal .top-sheet{padding-top:16px;padding-bottom:14px;background:#fbf7ef;box-shadow:none}.minimal-actions{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:9px}.minimal-actions button{border:1px solid rgba(121,48,20,.14);background:#fffdf8;text-align:left;padding:10px}.minimal-actions b,.minimal-actions span{display:block}.minimal-actions b{font-size:12px}.minimal-actions span{font-size:10px;color:#776b61;margin-top:3px}
.variant-minimal .sheet-utility{flex-wrap:wrap;overflow:visible;row-gap:2px}.variant-minimal .sheet-utility a{flex:0 0 auto}
.shell-bottom-nav{z-index:80}.variant-panel .shell-bottom-nav{border-radius:14px 14px 0 0;box-shadow:0 -8px 22px rgba(72,45,25,.08)}.variant-minimal .shell-bottom-nav{border-top-color:transparent;background:linear-gradient(to bottom,rgba(251,247,239,.88),#fbf7ef 25%)}
.shell-lab main{padding-bottom:calc(86px + env(safe-area-inset-bottom))}.shell-lab .date-accessory{z-index:78}.variant-kicker{display:block;margin-bottom:8px;font-size:10px;line-height:12px;font-weight:800;letter-spacing:.08em;text-transform:uppercase;color:#a54821}
.search-specimen,.personal-specimen{max-width:390px;margin:0 auto;padding-top:96px}.search-specimen .page-head,.personal-specimen .page-head{padding-bottom:18px}.search-form-lab{margin:0 14px 18px;padding:14px;background:#fffdf8;border-top:1px solid rgba(121,48,20,.16);border-bottom:1px solid rgba(121,48,20,.16)}.search-form-lab label{display:block;font-size:12px;font-weight:750;margin-bottom:7px}.search-form-lab textarea{box-sizing:border-box;width:100%;resize:none;border:1px solid #cfc1b5;border-radius:8px;background:#fff;padding:12px;font:600 16px/1.35 inherit;color:#221a14}.search-progress-cta{--cta-progress:0%;position:relative;isolation:isolate;box-sizing:border-box;width:100%;height:50px;margin-top:9px;overflow:hidden;border:0;border-radius:8px;background:#a54821;color:#fff;font:800 14px/1 inherit;cursor:pointer}.search-progress-cta::before{content:"";position:absolute;z-index:-1;inset:0 auto 0 0;width:var(--cta-progress);background:#793014;transition:width .24s var(--shell-ease)}.search-progress-cta[data-state="requesting"]::after{content:"";position:absolute;inset:0;background:linear-gradient(105deg,transparent 30%,rgba(255,255,255,.15) 48%,transparent 66%);animation:cta-shimmer 1.1s infinite}.search-progress-cta[data-state="done"]::before{background:#793014}.search-progress-cta span{position:relative;z-index:2}.search-status{min-height:28px;margin:8px 2px 0;font-size:11px;line-height:14px;color:#776b61}.sr-only{position:absolute!important;width:1px!important;height:1px!important;padding:0!important;margin:-1px!important;overflow:hidden!important;clip:rect(0,0,0,0)!important;white-space:nowrap!important;border:0!important}@keyframes cta-shimmer{from{transform:translateX(-90%)}to{transform:translateX(90%)}}
.saved-searches{margin:0 14px 20px}.saved-searches>b{font-size:12px}.saved-searches>div{display:flex;gap:6px;overflow-x:auto;margin-top:8px}.saved-searches button{border:1px solid rgba(121,48,20,.2);border-radius:999px;background:transparent;padding:8px 11px;white-space:nowrap;color:#4c3b30}.search-results .event-list,.personal-specimen>.event-list{padding-top:0}.search-results .event-row,.personal-specimen>.event-list .event-row{margin-top:8px}
.personal-onboarding{margin:0 14px 18px;padding:14px;display:flex;gap:12px;align-items:flex-end;background:#fffdf8;border:1px solid rgba(121,48,20,.14)}.personal-onboarding div{min-width:0}.personal-onboarding b{font-size:14px}.personal-onboarding p{margin:5px 0 0;font-size:11px;line-height:15px;color:#776b61}.personal-onboarding button{flex:0 0 104px;border:0;background:#a54821;color:#fff;padding:10px 8px;font:750 11px/1.2 inherit}
.personal-specimen>.feed-head{box-sizing:border-box;max-width:100%;overflow:hidden}.personal-specimen>.feed-head .feed-head__copy{min-width:0;max-width:100%}.personal-specimen>.feed-head .feed-head__copy>*{max-width:100%;overflow:hidden;text-overflow:ellipsis}
.discovery-endcap{margin:28px 14px 12px;padding:16px;border-top:1px solid rgba(121,48,20,.2);background:#fffdf8}.discovery-endcap>b{font-size:18px}.discovery-endcap p{margin:5px 0 12px;font-size:12px;color:#776b61}.discovery-endcap div{display:flex;gap:8px;flex-wrap:wrap}.discovery-endcap a,.discovery-endcap button{border:0;background:#eee2d5;color:#34271f;padding:9px 11px;text-decoration:none;font:700 11px/1 inherit}.contextual-endcap{border-left:3px solid #a54821}.no-footer-marker{height:30px;margin:22px 14px 8px;text-align:center;color:#9b8d82;font-size:9px;line-height:30px}
@media(max-width:350px){.top-sheet{padding-left:140px}.sheet-title b{font-size:15px}.sheet-primary a,.sheet-quick a{font-size:11px;padding-right:9px}.shell-context{left:145px!important}.personal-onboarding{display:block}.personal-onboarding button{margin-top:10px}.variant-panel .top-sheet{padding-left:12px}.variant-panel .sheet-title{left:145px}}
@media(prefers-reduced-motion:reduce){.top-sheet,.shell-scrim,.brand-tag__chevron,.search-progress-cta::before{transition:none!important}.search-progress-cta::after{animation:none!important;display:none}}
'''


LAB_JS = r'''
(() => {
  const root = document.documentElement;
  const trigger = document.querySelector('[data-shell-trigger]');
  const sheet = document.querySelector('.top-sheet');
  const scrim = document.querySelector('.shell-scrim');
  let returnFocus = null;
  function setOpen(open) {
    if (!trigger || !sheet || !scrim) return;
    if (open) returnFocus = document.activeElement;
    root.classList.toggle('shell-open', open);
    trigger.setAttribute('aria-expanded', String(open));
    sheet.setAttribute('aria-hidden', String(!open));
    sheet.classList.toggle('is-open', open);
    scrim.hidden = !open;
    requestAnimationFrame(() => scrim.classList.toggle('is-visible', open));
    if (open) sheet.querySelector('a,button:not([data-shell-close])')?.focus({preventScroll:true});
    else if (returnFocus && document.contains(returnFocus)) returnFocus.focus({preventScroll:true});
  }
  trigger?.addEventListener('click', () => setOpen(trigger.getAttribute('aria-expanded') !== 'true'));
  document.querySelectorAll('[data-shell-close]').forEach((node) => node.addEventListener('click', () => setOpen(false)));
  addEventListener('keydown', (event) => { if (event.key === 'Escape' && root.classList.contains('shell-open')) setOpen(false); });

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
    cta.disabled = true; cta.dataset.state = 'requesting'; cta.style.setProperty('--cta-progress', '0%');
    cta.setAttribute('aria-busy', 'true'); label.textContent = 'Подключаю поиск…'; status.textContent = 'Готовлю смысловой поиск';
    semantic.removeAttribute('aria-valuenow'); semantic.removeAttribute('aria-valuetext');
    await wait(520); if (mine !== epoch) return;
    const frames = [[12,'Ищу · 12%','Понимаю запрос'],[38,'Ищу · 38%','Сопоставляю интересы'],[64,'Ищу · 64%','Проверяю события'],[86,'Ищу · 86%','Сортирую выдачу'],[100,'Готово · 3 события','Результаты готовы']];
    cta.dataset.state = 'determinate';
    for (const [value, text, message] of frames) {
      if (mine !== epoch) return;
      cta.style.setProperty('--cta-progress', value + '%'); label.textContent = text; status.textContent = message;
      semantic.setAttribute('aria-valuenow', String(value)); semantic.setAttribute('aria-valuetext', message);
      await wait(value === 100 ? 780 : 430);
    }
    if (mine !== epoch) return;
    results.hidden = false; cta.dataset.state = 'done'; cta.disabled = false; cta.removeAttribute('aria-busy');
    results.scrollIntoView({behavior: matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth', block:'start'});
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
