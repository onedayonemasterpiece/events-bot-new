"""Source-backed route descriptions collected by monitoring and consumed offline."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging
import re
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import aiosqlite

from .parser import extract_date_iso, normalize_title_key, split_occurrence_blocks
from .tic_catalog import _LOGISTICS, _key, _source_roles, load_tic_background, refresh_tic_catalog

logger = logging.getLogger(__name__)

_STABLE = re.compile(
    r"маршрут|истори|архитектур|пройд[её]м|проходит|проходил|прогуля|увид|посет|познаком|"
    r"расскаж|рассказ|узна|осмотр|покаж|экспонат|экспозиц|сохранил|располож|посвящ|исследу", re.I)
_TEMPORARY = re.compile(
    r"перенос|перенес|перенёс|отмен|остал[а-яё]* мест|в этот раз|в этот день|впервые|"
    r"угощ|дегустац|подар[окк]|скидк|сезон|летн(?:ий|яя|ем)|зимн(?:ий|яя|ем)|погод|"
    r"регистрац|заявк|включен|включён|продаж|трансфер|сбор групп|"
    r"предоплат|организованных групп|только для|мест огранич|\+\s*7|\[id\d+|@\w|#\w", re.I)
_ONE_OFF = re.compile(r"\b(?:разов(?:ая|ый) (?:прогулка|экскурсия|маршрут)|только один раз|"
                      r"единственн(?:ый|ая) (?:показ|экскурсия|выезд)|без повторений)\b", re.I)
_RECURRING = re.compile(r"\b(?:кажд(?:ую|ый|ое) (?:неделю|субботу|воскресенье|месяц|год)|"
                        r"еженедельно|ежемесячно|ежегодно|несколько раз в год)\b", re.I)
_TABLE = """CREATE TABLE IF NOT EXISTS guide_route_lookup (
    lookup_key TEXT PRIMARY KEY, provider TEXT NOT NULL, next_at REAL NOT NULL,
    checked_at REAL NOT NULL, evidence_json TEXT NOT NULL DEFAULT '{}')"""
# Reserve before doing I/O. A stopped monitoring process cannot create a hot retry loop.
_PROVIDER_GAP = 60 * 60
_NEGATIVE_TTL = 30 * 86400
_POSITIVE_TTL = 90 * 86400
_TRUST_TTL = 365 * 86400
_MATERIAL = """CREATE TABLE IF NOT EXISTS guide_route_material (
    occurrence_id INTEGER PRIMARY KEY, identity TEXT NOT NULL, history_json TEXT NOT NULL)"""


def _roles(row):
    guides, organizers = _source_roles(row)
    return {_key(name) for name in guides if name}, {_key(name) for name in organizers if name}


def _same_route(current, old):
    if _key(current.get("canonical_title") or current["title_normalized"]) != _key(old.get("canonical_title") or old["title_normalized"]):
        return False
    guides, organizers = _roles(current)
    old_guides, old_organizers = _roles(old)
    if (guides and old_guides and guides != old_guides) or (organizers and old_organizers and not organizers & old_organizers):
        return False
    shared_guides = guides & old_guides
    if any(2 <= len(name.split()) <= 5 for name in shared_guides - organizers - old_organizers):
        return True
    return (bool(current.get("primary_source_id")) and current["primary_source_id"] == old.get("primary_source_id")
            and bool(organizers & old_organizers or shared_guides))


def _focus(post, title, *, linked):
    """Require a unique title-bearing block for an unlinked or multi-outing announcement."""
    text = (post.get("text") or "").strip()
    if not text:
        return ""
    try:
        posted = datetime.fromisoformat(str(post.get("post_date") or "").replace("Z", "+00:00"))
    except ValueError:
        return ""
    blocks = split_occurrence_blocks(text, post_date=posted, keep_lines=True)
    if linked and len(blocks) == 1 and post.get("post_kind") not in {"announce_multi", "schedule", "status_update"}:
        return blocks[0]
    title_key = normalize_title_key(title).replace("ё", "е")
    if not title_key:
        return ""
    hits = [b for b in blocks if f" {title_key} " in f" {_key(b)} "]
    if len(hits) != 1:
        return ""
    block = hits[0]
    # The parser preserves preambles in some schedules. A title mentioned only in a
    # timetable line does not own another introduction, or the rest of a list of titles.
    lines = block.splitlines()
    title_lines = [i for i, line in enumerate(lines) if f" {title_key} " in f" {_key(line)} "]
    if not title_lines:
        return ""
    remaining = lines[title_lines[0]:]
    result = [remaining[0]]
    for line in remaining[1:]:
        if re.search(r"^\s*[•\d].*(?:[«“]|\s[—–]\s)", line) or extract_date_iso(line, post_date=posted):
            break
        result.append(line)
    return "\n".join(result)


def _stable_excerpt(block, title):
    title_words = _key(title).split()
    selected = []
    for fragment in re.split(r"(?<=[.!?])\s+|\n+", block):
        sentence = fragment.strip(" \t•—–")
        if not (8 <= len(sentence) <= 450) or _LOGISTICS.search(sentence) or _TEMPORARY.search(sentence):
            continue
        if not _STABLE.search(sentence) or not set(_key(sentence).split()) - set(title_words):
            continue
        if len(" ".join([*selected, sentence])) > 850:
            break
        if sentence not in selected:
            selected.append(sentence)
    return " ".join(selected)


def _lookup_key(row):
    guides, organizers = _roles(row)
    identity = [row.get("primary_source_id"), _key(row.get("canonical_title")), sorted(guides), sorted(organizers)]
    if (row.get("route_history") or {}).get("kind") == "one_off":
        identity.append(row.get("date"))
    return "route:" + hashlib.sha256(json.dumps(identity, ensure_ascii=False).encode()).hexdigest()


def _sufficient(excerpt):
    # At least one developed first-party description, beyond a title or a timetable line.
    return len(re.findall(r"[А-Яа-яЁёA-Za-z]{2,}", excerpt)) >= 12


def needs_background(info):
    return not (info["ambiguous_title"] or info["description_sufficient"]
                or _sufficient(" ".join(post["text"] for post in info["earlier"])))


def _current_state(row, linked):
    title = row.get("canonical_title") or row["title_normalized"]
    blocks = [_focus(p, title, linked=True) for p in linked.get(row["id"], [])]
    text = " ".join(blocks)
    # A bare destination on a timetable is not the title of an identified programme.
    single = len(normalize_title_key(title).split()) == 1 and bool(row.get("canonical_title"))
    named = any(_key(quote) == _key(title) for quote in re.findall(r'[«“"]([^»”"\n]+)[»”"]', text))
    ambiguous = single and not named
    kind = "one_observed_date"
    if _ONE_OFF.search(text):
        kind = "one_off"
    elif _RECURRING.search(text):
        kind = "explicitly_recurring"
    return {"kind": kind, "dated_departures": 1, "ambiguous_title": ambiguous, "earlier": [],
            "description_sufficient": any(_sufficient(_stable_excerpt(b, title)) for b in blocks)}


def _material_identity(row):
    return f"{_lookup_key(dict(row,route_history=None))}:{row.get('date')}:{row.get('updated_at','')}"


async def load_materialized_history(conn, records, linked):
    rows = {r["id"]: dict(r) for r in records}
    if not rows:
        return {}
    stored = {}
    if await conn.execute_fetchall("SELECT 1 FROM sqlite_master WHERE type='table' AND name='guide_route_material'"):
        marks = ",".join("?" for _ in rows)
        for occurrence_id, identity, raw in await conn.execute_fetchall(
                f"SELECT occurrence_id,identity,history_json FROM guide_route_material WHERE occurrence_id IN ({marks})",
                tuple(rows)):
            if identity == _material_identity(rows[occurrence_id]):
                stored[occurrence_id] = json.loads(raw)
    for key, row in rows.items():
        if key not in stored:
            stored[key] = _current_state(row, linked)
    return stored


async def load_route_history(conn, records, linked):
    """Derive recurrence from distinct dated outings; carry only literal stable source excerpts."""
    rows = {int(r["id"]): dict(r) for r in records}
    if not rows:
        return {}
    params = ",".join("?" for _ in rows)
    cur = await conn.execute(f"""
        SELECT current.id AS wanted, old.*, p.id AS post_id, p.text AS source_text,
               p.post_date, p.source_url, p.post_kind
        FROM guide_occurrence current JOIN guide_occurrence old ON old.title_normalized=current.title_normalized
             AND old.date < current.date
        JOIN guide_occurrence_source link ON link.occurrence_id=old.id
        JOIN guide_monitor_post p ON p.id=link.post_id
        WHERE current.id IN ({params})
        ORDER BY current.id, p.post_date DESC, p.id DESC""", tuple(rows))
    histories = {identity: _current_state(row, linked) for identity, row in rows.items()}
    dates = defaultdict(set)
    for source in await cur.fetchall():
        old = dict(source)
        row = rows[old["wanted"]]
        info = histories[old["wanted"]]
        if info["ambiguous_title"] or info["kind"] == "one_off" or old.get("status") in {"cancelled", "rescheduled"} or not _same_route(row, old):
            continue
        title = row.get("canonical_title") or row["title_normalized"]
        post = dict(text=old["source_text"], post_kind=old["post_kind"], post_date=old["post_date"])
        block = _focus(post, title, linked=True)
        if not block or _ONE_OFF.search(block):
            continue
        dates[old["wanted"]].add(old["date"])
        if not old["source_url"] or any(p.get("text") == old["source_text"] for p in linked.get(old["wanted"], [])):
            continue
        excerpt = _stable_excerpt(block, title)
        if not excerpt:
            continue
        if len(info["earlier"]) < 2 and all(p["text"] != excerpt for p in info["earlier"]):
            info["earlier"].append({"url": old["source_url"], "post_date": old["post_date"], "text": excerpt})
    for key, info in histories.items():
        if dates[key] and info["kind"] == "one_observed_date":
            info["kind"] = "repeated_dates"
        info["dated_departures"] = len(dates[key]) + 1
    return histories


async def load_cached_background(conn, rows, *, fresh_only=False, now=None):
    now = (now or datetime.now(timezone.utc)).timestamp()
    if not await conn.execute_fetchall("SELECT 1 FROM sqlite_master WHERE type='table' AND name='guide_route_lookup'"):
        return {}
    keyed = {_lookup_key(row): row["id"] for row in rows}
    if not keyed:
        return {}
    params = ",".join("?" for _ in keyed)
    found = await conn.execute_fetchall(
        f"SELECT lookup_key,evidence_json,next_at FROM guide_route_lookup WHERE lookup_key IN ({params})",
        tuple(keyed))
    by_key = {}
    for key, raw, next_at in found:
        if not raw or raw == "{}" or (fresh_only and next_at <= now):
            continue
        data = json.loads(raw)
        if datetime.fromisoformat(data["retrieved_at"]).timestamp() + _TRUST_TTL > now:
            by_key[key] = data
    return {row["id"]: by_key[_lookup_key(row)] for row in rows if _lookup_key(row) in by_key}


def _first_party(row, source, post, *, provider_verified):
    """A source's own wall can advertise another operator. Check the title and the credited people."""
    url = urlsplit(post.get("url") or "")
    if url.scheme != "https" or not url.hostname:
        return ""
    platform = source["platform"]
    if platform == "telegram":
        if url.hostname not in {"t.me", "telegram.me"} or not re.fullmatch(
                rf"/{re.escape(source['username'].lstrip('@'))}/\d+", url.path, re.I):
            return ""
    elif platform == "vk":
        flags = json.loads(source.get("flags_json") or "{}")
        owner = abs(int(flags.get("vk_id") or 0)) * (1 if flags.get("vk_personal_page") else -1)
        match = re.fullmatch(r"/wall(-?\d+)_\d+", url.path)
        if (url.hostname not in {"vk.com", "vk.ru"} or not match or (owner and int(match[1]) != owner)
                or (not owner and not provider_verified)):
            return ""
    else:
        return ""
    title = row["canonical_title"]
    block = _focus(post, title, linked=False)
    one_off = (row.get("route_history") or {}).get("kind") == "one_off"
    if one_off:
        if not block or extract_date_iso(block, post_date=datetime.fromisoformat(post["post_date"].replace("Z","+00:00"))) != row.get("date"):
            return ""
    if not block or (_ONE_OFF.search(block) and not one_off):
        return ""
    guides, organizers = _roles(row)
    names = {_key(source.get(field)) for field in ("profile_name", "marketing_name") if source.get(field)}
    # A guide's own channel can also announce a guest's tour. Names outside a role
    # attribution (for example a booking contact) cannot establish who leads it.
    role_pattern = re.compile(r"(?:\b[Гг]ид|[Ээ]кскурсовод|[Вв]ед[её]т|[Пп]роводит|[Оо]рганизатор)\s*[:—–-]?\s*"
                              r"([А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+)")
    credited = {_key(name) for name in role_pattern.findall(block)}
    if credited and not credited <= (guides | organizers):
        return ""
    voice = bool(re.search(r"\b(?:приглашаю|приглашаем|моя экскурсия|мой маршрут|наша прогулка|наш маршрут|организуем)\b", block, re.I))
    own_guide = source["source_kind"] == "guide_personal" and bool(names & guides)
    own_operator = bool(names & organizers) and (not guides or guides <= organizers)
    if not (credited & guides or (not guides and credited & organizers) or (voice and (own_guide or own_operator))):
        return ""
    return _stable_excerpt(block, title)


async def _claim(db, key, provider, now):
    async with db.raw_conn() as conn:
        await conn.execute(_TABLE)
        await conn.execute("BEGIN IMMEDIATE")
        current = await conn.execute_fetchall(
            "SELECT 1 FROM guide_route_lookup WHERE lookup_key IN (?,?) AND next_at>? LIMIT 1",
            (key, "provider:" + provider, now))
        if current:
            await conn.rollback()
            return False
        await conn.executemany("""INSERT INTO guide_route_lookup(lookup_key,provider,next_at,checked_at) VALUES (?,?,?,?)
            ON CONFLICT(lookup_key) DO UPDATE SET next_at=excluded.next_at,checked_at=excluded.checked_at""",
            [(key, provider, now + 86400, now), ("provider:" + provider, provider, now + _PROVIDER_GAP, now)])
        await conn.commit()
        return True


async def _finish(db, key, provider, now, wait, evidence=None, provider_wait=0):
    async with db.raw_conn() as conn:
        await conn.execute("""UPDATE guide_route_lookup SET next_at=?,evidence_json=COALESCE(?,evidence_json)
            WHERE lookup_key=? AND checked_at=?""",
            (now + wait, json.dumps(evidence, ensure_ascii=False) if evidence else None, key, now))
        if provider_wait:
            await conn.execute("UPDATE guide_route_lookup SET next_at=max(next_at,?) WHERE lookup_key=?",
                               (now + provider_wait, "provider:" + provider))
        await conn.commit()


def _pick_evidence(row, source, posts, known_urls, now, *, provider_verified=False):
    for post in posts:
        if post.get("url") in known_urls:
            continue
        text = _first_party(row, source, post, provider_verified=provider_verified)
        if _sufficient(text):
            return {"url": post["url"], "post_date": post["post_date"], "title": row["canonical_title"],
                    "retrieved_at": now.isoformat(), "text": text}
    return None


async def _local_source_evidence(db, row, source, known_urls, now):
    keyword = max(normalize_title_key(row["canonical_title"]).split(), key=len, default="")
    if len(keyword) < 4:
        return None
    spellings = {form for token in (keyword, keyword.replace("ё", "е"))
                 for form in (token, token.capitalize(), token.upper())}
    match_title = " OR ".join("instr(text,?)>0" for _ in spellings)
    async with db.raw_conn() as conn:
        rows = await conn.execute_fetchall(f"""SELECT source_url,post_date,text,post_kind FROM guide_monitor_post
            WHERE source_id=? AND ({match_title}) ORDER BY post_date DESC""", (source["id"], *spellings))
    posts = (dict(url=url, post_date=stamp, text=text, post_kind=kind) for url, stamp, text, kind in rows)
    return _pick_evidence(row, source, posts, known_urls, now)


async def _save_material(db, rows, histories, websites):
    async with db.raw_conn() as conn:
        for identity, info in histories.items():
            if identity in websites and info["kind"] != "one_off" and not info["ambiguous_title"]:
                info["website"] = websites[identity]
        await conn.executemany("""INSERT INTO guide_route_material VALUES (?,?,?)
            ON CONFLICT(occurrence_id) DO UPDATE SET identity=excluded.identity,history_json=excluded.history_json
            WHERE identity!=excluded.identity OR history_json!=excluded.history_json""",
            [(row["id"], _material_identity(row), json.dumps(histories[row["id"]], ensure_ascii=False))
             for row in rows])
        await conn.commit()


async def refresh_route_history(db, *, search=None, now=None, external=True):
    from .route_readers import RouteReadLimited, search_route_posts

    now = now or datetime.now(timezone.utc)
    today = now.astimezone(ZoneInfo("Europe/Kaliningrad")).date()
    async with db.raw_conn() as conn:
        conn.row_factory = aiosqlite.Row
        records = [dict(r) for r in await conn.execute_fetchall("""
            SELECT o.* FROM guide_occurrence o JOIN guide_source s ON s.id=o.primary_source_id
            WHERE o.date BETWEEN ? AND ? AND o.digest_eligible=1 AND COALESCE(o.status,'') NOT IN ('cancelled','rescheduled')
              AND s.enabled=1 AND s.source_kind IN ('guide_personal','guide_project','organization_with_tours','excursion_operator')
            ORDER BY o.date,o.id""", (today.isoformat(), (today + timedelta(days=45)).isoformat()))]
        await conn.execute(_TABLE)
        await conn.execute(_MATERIAL)
        await conn.execute("""DELETE FROM guide_route_lookup WHERE lookup_key LIKE 'route:%' AND
             ((evidence_json='{}' AND next_at<=?) OR
              (evidence_json!='{}' AND unixepoch(json_extract(evidence_json,'$.retrieved_at'))+?<=?))""",
             (now.timestamp(),_TRUST_TTL,now.timestamp()))
        await conn.commit()
        if not records:
            return {"searched": 0, "cached": 0, "materialized": 0}
        ids = [r["id"] for r in records]
        joined = await conn.execute_fetchall(f"""SELECT link.occurrence_id,p.text,p.post_date,p.post_kind,p.source_url AS url
            FROM guide_occurrence_source link JOIN guide_monitor_post p ON p.id=link.post_id
            WHERE link.occurrence_id IN ({','.join('?' for _ in ids)})""", ids)
        linked = defaultdict(list)
        for post in joined:
            linked[post["occurrence_id"]].append(dict(post))
        histories = await load_route_history(conn, records, linked)
        websites = await load_tic_background(conn, records)
        sources = {s["id"]: dict(s) for s in await conn.execute_fetchall("""
            SELECT s.*,p.display_name AS profile_name,p.marketing_name FROM guide_source s
            LEFT JOIN guide_profile p ON p.id=s.primary_profile_id WHERE s.enabled=1""")}
        for row in records:
            row["route_history"] = histories[row["id"]]
        cached = await load_cached_background(conn, records, fresh_only=True, now=now)
    stats = {"searched": 0, "cached": 0, "local": 0, "limited": 0, "materialized": len(records)}
    attempted = set()
    pending = []
    for row in records:
        identity = row["id"]
        hist = histories[identity]
        if not needs_background(hist) or (identity in websites and hist["kind"] != "one_off") or identity in cached:
            continue
        source = sources.get(row["primary_source_id"])
        if not source or source["platform"] not in {"vk", "telegram"}:
            continue
        key = _lookup_key(row)
        provider = source["platform"]
        # Search all locally retained first-party posts before any lookup across the network.
        if key in attempted:
            continue
        attempted.add(key)
        known_urls = {post.get("url") for post in linked[identity]}
        local_match = await _local_source_evidence(db, row, source, known_urls, now)
        if local_match:
            async with db.raw_conn() as conn:
                await conn.execute(_TABLE)
                await conn.execute("""INSERT INTO guide_route_lookup VALUES (?,?,?,?,?) ON CONFLICT(lookup_key)
                    DO UPDATE SET next_at=excluded.next_at,checked_at=excluded.checked_at,evidence_json=excluded.evidence_json
                    WHERE excluded.checked_at >= guide_route_lookup.checked_at""",
                    (key,provider,now.timestamp()+_POSITIVE_TTL,now.timestamp(),json.dumps(local_match,ensure_ascii=False)))
                await conn.commit()
            stats["local"] += 1
            continue
        pending.append((row, source, known_urls, key))
    sparse = [row for row, _, _, _ in pending if histories[row["id"]]["kind"] != "one_off"]
    if external and sparse:
        try:
            stats["tic_catalog"] = await refresh_tic_catalog(db, wanted_occurrences=sparse, now=now)
            async with db.raw_conn() as conn:
                websites.update(await load_tic_background(conn, sparse))
        except Exception as exc:
            logger.warning("guide_monitor.tic_scan_failed error=%s", type(exc).__name__)
            stats["tic_catalog"] = {"error": type(exc).__name__}
    await _save_material(db, records, histories, websites)
    if not external:
        return stats
    for row, source, known_urls, key in pending:
        provider = source["platform"]
        if (row["id"] in websites and histories[row["id"]]["kind"] != "one_off") or not await _claim(db, key, provider, now.timestamp()):
            continue
        stats["searched"] += 1
        try:
            async with asyncio.timeout(15):
                hits = await (search or search_route_posts)(source, row["canonical_title"])
            hit = _pick_evidence(row, source, hits, known_urls, now, provider_verified=True)
            await _finish(db,key,provider,now.timestamp(),_POSITIVE_TTL if hit else _NEGATIVE_TTL,hit)
            stats["cached"] += int(bool(hit))
        except RouteReadLimited as exc:
            wait = max(_PROVIDER_GAP, int(exc.retry_after))
            await _finish(db,key,provider,now.timestamp(),wait,provider_wait=wait)
            stats["limited"] += 1
        except Exception as exc:
            logger.warning("guide_monitor.route_search_failed provider=%s error=%s",provider,type(exc).__name__)
            await _finish(db,key,provider,now.timestamp(),86400)
    return stats


async def record_route_scan(db, details, mode):
    try:
        stats = await refresh_route_history(db, external=mode == "full")
        if "tic_catalog" in stats:
            details["tic_catalog"] = stats.pop("tic_catalog")
        details["route_history"] = stats
    except Exception as exc:
        logger.warning("guide_monitor.route_history_failed",exc_info=True)
        details["route_history"] = {"error": type(exc).__name__}
