"""Official TIC route descriptions, collected by monitoring and read offline by the writer."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from time import monotonic
from urllib.parse import urljoin, urlsplit
import xml.etree.ElementTree as ET
from zoneinfo import ZoneInfo

import aiohttp
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
ORIGIN = "https://visit-kaliningrad.ru"
# Pilot entry points are server-rendered. Query-string pagination is disallowed by the site.
INDEXES = (ORIGIN + "/excursions/", ORIGIN + "/excursions/search/kaliningrad/")
SITEMAP = ORIGIN + "/sitemap-excursions-search.xml"
_MAX_BYTES = 2_000_000
_MAX_AGE = timedelta(days=14)
_PROFILE_ALIASES = {
    # TIC uses this same profile URL for «Ноза Усманова» at
    # https://visit-kaliningrad.ru/news/4noyabryabesplatnayaekskursiyaestafeta80letedinstva/
    "/pro/gidy-i-ekskursovody/usmanova-shakhnoza-mukhitdinovna/": "Ноза Усманова",
}
_LOGISTICS = re.compile(
    r"\d|рубл|₽|евро|бесплат|стоимост|цен[аыуе]|оплат|билет|заказ|запис|заброни|бронир|"
    r"расписан|график|телефон|начало|мест[ао] (?:сбора|встречи)|осталось|свободн.{0,8}мест|"
    r"январ|феврал|март[ае]|апрел|ма[йяе]\b|июн|июл|август|сентябр|октябр|ноябр|декабр|"
    r"понедельник|вторник|сред[ау]\b|четверг|пятниц|суббот|воскресень|ежеднев|"
    r"утром|вечером|минут|\bчас(?:а|ов)?\b|сегодня|завтра|https?://|экскурсовод\s*:|гид\s*:",
    re.I,
)

_TABLE = """CREATE TABLE IF NOT EXISTS guide_web_route_evidence (
    url TEXT PRIMARY KEY, title TEXT NOT NULL, guides_json TEXT NOT NULL,
    excerpt TEXT NOT NULL, retrieved_at TEXT NOT NULL
)"""


def _key(value):
    return " ".join(re.findall(r"[a-zа-я0-9]+", (value or "").casefold().replace("ё", "е")))


def _person_key(value):
    words = _key(value).split()
    return " ".join(sorted(words)) if len(words) >= 2 else ""


def _source_url(href, pattern):
    url = urlsplit(urljoin(ORIGIN, href or ""))
    if (url.scheme == "https" and url.netloc == "visit-kaliningrad.ru"
            and not url.query and not url.fragment and re.fullmatch(pattern, url.path)):
        return ORIGIN + url.path
    return None


def _listing(html):
    soup = BeautifulSoup(html, "html.parser")
    cards = {}
    for link in soup.select("a.excursions-item__link[href]"):
        url = _source_url(link["href"], r"/excursions/[a-z0-9_-]+/")
        heading = link.find("h3")
        if url and heading:
            cards[url] = heading.get_text(" ", strip=True)
    return cards


def _detail(html):
    soup = BeautifulSoup(html, "html.parser")
    info = soup.select_one("#excursion-info")
    heading = info.select_one(".excursion-info__title") if info else None
    if not heading:
        return None
    fragments = []
    guides = set()
    for row in info.select(".content-poster__table-row"):
        label = row.select_one(".vi-table__title")
        body = row.select_one(".vi-table__description")
        if not label or not body or _key(label.get_text()) not in {"дополнительно", "гид", "экскурсовод"}:
            continue
        for link in body.select("a[href]"):
            url = _source_url(link["href"], r"/pro/gidy-i-ekskursovody/[a-z0-9_-]+/")
            # A profile mentioned in prose may be a guest or another author.
            prefix = " ".join(str(link.previous_sibling or "").split()).casefold()
            if url and (re.search(r"(?:экскурсовод|гид)\s*:\s*$", prefix)
                        or _key(label.get_text()) in {"гид", "экскурсовод"}):
                guides.add(_person_key(link.get_text(" ", strip=True)))
                if alias := _PROFILE_ALIASES.get(urlsplit(url).path):
                    guides.add(_person_key(alias))
        if _key(label.get_text()) != "дополнительно":
            continue
        for br in body.find_all("br"):
            br.replace_with("\n")
        for line in body.get_text(" ").splitlines():
            line = " ".join(line.split())
            if re.search(r"^(?:экскурсовод|гид|график|расписание|стоимость)\s*[: (]", line, re.I):
                break
            for sentence in re.split(r"(?<=[.!?])\s+", line):
                if len(sentence) >= 35 and not _LOGISTICS.search(sentence):
                    fragments.append(sentence)
    guides.discard("")
    excerpt = ""
    for sentence in fragments:
        if len(excerpt) + len(sentence) + 1 > 1200:
            break
        excerpt = f"{excerpt} {sentence}".strip()
    if guides and excerpt:
        return heading.get_text(" ", strip=True), sorted(guides), excerpt
    return None


def _route_keys(title, organizers):
    title = _key(title)
    if not title:
        return set()
    return {title, *(_key(f"{org} {title}") for org in organizers if org)}


def _names(raw):
    try:
        values = json.loads(raw or "[]")
    except (TypeError, ValueError):
        return []
    return [v for v in values if isinstance(v, str)] if isinstance(values, list) else []


def _source_roles(row):
    pack = row.get("fact_pack")
    if pack is None:
        try:
            pack = json.loads(row.get("fact_pack_json") or "{}")
        except (TypeError, ValueError):
            pack = {}
    pack = pack if isinstance(pack, dict) else {}
    roles = []
    for role in ("guide_names", "organizer_names"):
        values = pack.get(role, row.get(role))
        if values is None and role not in pack:
            values = _names(row.get(f"{role}_json"))
        roles.append([n for n in values if isinstance(n, str)] if isinstance(values, list) else [])
    return roles


def _rotate_daily(urls, today):
    ordered = sorted(urls)
    if not ordered:
        return []
    start = today.toordinal() % len(ordered)
    return ordered[start:] + ordered[:start]


async def _fetch(session, url):
    async with session.get(url, allow_redirects=False) as response:
        if response.status in (404, 410):
            return None
        if response.status in (429, 503):
            response.raise_for_status()
        if response.status != 200:
            raise ValueError(f"HTTP {response.status}")
        allowed = {"text/xml", "application/xml"} if url == SITEMAP else {"text/html"}
        if response.content_type not in allowed:
            raise ValueError(f"unexpected MIME {response.content_type}")
        data = bytearray()
        async for chunk in response.content.iter_chunked(65_536):
            data.extend(chunk)
            if len(data) > _MAX_BYTES:
                raise ValueError("TIC page exceeds size limit")
        return data.decode(response.charset or "utf-8")


async def refresh_tic_catalog(db, *, wanted_occurrences=None, fetch=None, now=None):
    """Gather outside SQLite transactions. A failed request cannot erase previous evidence."""
    now = now or datetime.now(timezone.utc)
    today = now.astimezone(ZoneInfo("Europe/Kaliningrad")).date()
    if wanted_occurrences is None:
        async with db.raw_conn() as conn:
            rows = await conn.execute_fetchall(
                "SELECT canonical_title, guide_names_json, organizer_names_json, fact_pack_json FROM guide_occurrence "
                "WHERE date BETWEEN ? AND ? AND digest_eligible = 1 AND COALESCE(status,'') != 'cancelled'",
                (today.isoformat(), (today + timedelta(days=120)).isoformat()),
            )
        wanted_occurrences = [dict(zip(("canonical_title", "guide_names_json", "organizer_names_json", "fact_pack_json"), r))
                              for r in rows]
    wanted = defaultdict(set)
    for occurrence in wanted_occurrences:
        row = dict(occurrence)
        guides, organizers = _source_roles(row)
        people = set(filter(None, map(_person_key, guides)))
        if people:
            for key in _route_keys(row["canonical_title"], organizers):
                wanted[key].update(people)
    stats = {"indexes_read": 0, "listed": 0, "title_candidates": 0, "confirmed": 0, "errors": 0}
    if not wanted:
        return stats
    # Reserve the entire site before network I/O, including concurrent imports and restarts.
    async with db.raw_conn() as conn:
        await conn.execute("CREATE TABLE IF NOT EXISTS guide_web_scan_state (origin TEXT PRIMARY KEY, next_at REAL NOT NULL)")
        claim = await conn.execute(
            "INSERT INTO guide_web_scan_state VALUES (?,?) ON CONFLICT(origin) DO UPDATE SET next_at=excluded.next_at "
            "WHERE guide_web_scan_state.next_at<=?", (ORIGIN, now.timestamp()+86400, now.timestamp()))
        await conn.commit()
        if not claim.rowcount:
            return {**stats, "cooldown": 1}
    cache_rows, withdrawals, listed = [], [], {}
    started = monotonic()
    slots = asyncio.Semaphore(1)
    last_request = 0.0
    timeout = aiohttp.ClientTimeout(total=8)
    async with aiohttp.ClientSession(timeout=timeout, trust_env=True,
            headers={"User-Agent": "EventsBot-ExcursionMonitor/1.0"}) as session:
        async def read(url):
            nonlocal last_request
            if stats.get("limited"):
                return False, None
            if not fetch and last_request:
                await asyncio.sleep(max(0.0, 1.0 - (monotonic() - last_request)))
            last_request = monotonic()
            try:
                return True, await (fetch(url) if fetch else _fetch(session, url))
            except (aiohttp.ClientError, TimeoutError, ValueError, OSError) as exc:
                stats["errors"] += 1
                if isinstance(exc, aiohttp.ClientResponseError) and exc.status in (429, 503):
                    from .route_readers import _retry_after
                    wait = _retry_after((exc.headers or {}).get("Retry-After"))
                    stats["limited"] = 1
                    async with db.raw_conn() as conn:
                        await conn.execute("UPDATE guide_web_scan_state SET next_at=max(next_at,?) WHERE origin=?",
                                           (datetime.now(timezone.utc).timestamp()+wait, ORIGIN))
                        await conn.commit()
                    return False, None
                logger.warning("guide_monitor.tic_request_failed url=%s error=%s", url, exc)
                return False, None
        indexes = set(INDEXES)
        ok, xml = await read(SITEMAP)
        if ok and xml:
            try:
                published = ET.fromstring(xml).findall(".//{*}loc")
                for node in published:
                    if url := _source_url(node.text, r"/excursions/search/[a-z0-9_-]+/"):
                        indexes.add(url)
            except ET.ParseError:
                stats["errors"] += 1
        elif ok:
            stats["errors"] += 1
        stats["indexes_discovered"] = len(indexes)

        async def index_cards(url):
            async with slots:
                if monotonic() - started > 35:
                    stats["indexes_deferred"] = stats.get("indexes_deferred", 0) + 1
                    return
                ok, html = await read(url)
            if ok and html and (cards := _listing(html)):
                stats["indexes_read"] += 1
                listed.update(cards)
            elif ok:
                stats["errors"] += 1

        await asyncio.gather(*(index_cards(url) for url in _rotate_daily(indexes, today)[:4]))
        stats["listed"] = len(listed)
        candidates = [url for url, title in listed.items() if _key(title) in wanted]
        stats["title_candidates"] = len(candidates)

        async def read_candidate(url):
            async with slots:
                if monotonic() - started > 56:
                    stats["details_deferred"] = stats.get("details_deferred", 0) + 1
                    return
                ok, html = await read(url)
            if not ok:
                return
            parsed = _detail(html) if html else None
            if parsed and _key(parsed[0]) in wanted:
                title, guides, excerpt = parsed
                cache_rows.append((url, title, json.dumps(guides, ensure_ascii=False), excerpt, now.isoformat()))
                if wanted[_key(title)].intersection(guides):
                    stats["confirmed"] += 1
            else:
                withdrawals.append((url, now.isoformat()))

        await asyncio.gather(*(read_candidate(url) for url in _rotate_daily(candidates, today)[:3]))
    async with db.raw_conn() as conn:
        await conn.execute(_TABLE)
        await conn.executemany("DELETE FROM guide_web_route_evidence WHERE url=? AND retrieved_at <= ?", withdrawals)
        await conn.executemany(
            """INSERT INTO guide_web_route_evidence VALUES (?,?,?,?,?)
               ON CONFLICT(url) DO UPDATE SET title=excluded.title, guides_json=excluded.guides_json,
               excerpt=excluded.excerpt, retrieved_at=excluded.retrieved_at
               WHERE excluded.retrieved_at >= guide_web_route_evidence.retrieved_at""", cache_rows,
        )
        await conn.execute("DELETE FROM guide_web_route_evidence WHERE retrieved_at < ?", ((now - _MAX_AGE).isoformat(),))
        await conn.commit()
    stats["cached"] = len(cache_rows)
    return stats


async def load_tic_background(conn, records):
    """Recheck current title, organizer and guide even if the original occurrence was edited."""
    exists = await conn.execute_fetchall("SELECT 1 FROM sqlite_master WHERE type='table' AND name='guide_web_route_evidence'")
    if not exists:
        return {}
    rows = await conn.execute_fetchall(
        "SELECT url, title, guides_json, excerpt, retrieved_at FROM guide_web_route_evidence "
        "WHERE retrieved_at >= ? ORDER BY url", ((datetime.now(timezone.utc) - _MAX_AGE).isoformat(),))
    by_title = defaultdict(list)
    for url, title, guides, text, retrieved_at in rows:
        by_title[_key(title)].append((set(_names(guides)), {
            "title": title, "url": url, "retrieved_at": retrieved_at, "text": text}))
    result = {}
    for record in records:
        row = dict(record)
        guide_names, organizers = _source_roles(row)
        guides = set(map(_person_key, guide_names))
        matches = [source for key in _route_keys(row["canonical_title"], organizers)
                   for people, source in by_title.get(key, []) if guides & people]
        if len(matches) == 1:
            result[row["id"]] = matches[0]
    return result


async def record_tic_scan(db, details, mode, *, wanted_occurrences=None):
    if mode != "full":
        return
    try:
        async with asyncio.timeout(70):
            details["tic_catalog"] = await refresh_tic_catalog(db, wanted_occurrences=wanted_occurrences)
    except Exception as exc:
        # This auxiliary source must not prevent already imported social posts from being published.
        logger.warning("guide_monitor.tic_scan_failed", exc_info=True)
        details["tic_catalog"] = {"errors": 1, "error": type(exc).__name__}
