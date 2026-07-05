#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import base64
import csv
import hashlib
import html
import json
import os
import random
import re
import subprocess
import sys
import time
import zipfile
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from xml.sax.saxutils import escape

RUN_STARTED_AT = datetime.now(timezone.utc)
DEFAULT_ANCHORS = ["Калининград", "Калининградская область", "Куршская коса", "Зеленоградск", "Светлогорск", "Балтийское море", "Кёнигсберг"]
NEWS_WORDS = ["происшеств", "дтп", "авар", "полици", "суд", "задерж", "штраф", "войн", "полит", "скандал", "убий", "пожар"]
AD_WORDS = ["скидк", "промокод", "купить", "заказать", "реклама", "партнёр", "партнер", "оплат", "бронь"]
TRASH_WORDS = ["жесть", "треш", "трэш", "шок", "кошмар"]
POSITIVE_WORDS = ["красив", "атмосфер", "море", "дюны", "архитект", "истори", "маршрут", "музей", "пляж", "курорт", "прогул", "путешеств"]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def getenv_bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def getenv_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    try:
        return int(raw) if raw else default
    except Exception:
        return default


def stable_hash(*parts: Any, length: int = 16) -> str:
    h = hashlib.sha256()
    for part in parts:
        h.update(str(part or "").strip().lower().encode("utf-8"))
        h.update(b"\0")
    return h.hexdigest()[:length]


def canonical_handle(value: str) -> str:
    raw = (value or "").strip()
    raw = re.sub(r"^https?://t\.me/", "@", raw, flags=re.I)
    raw = re.sub(r"^https?://(?:www\.)?vk\.com/", "@", raw, flags=re.I)
    if raw and not raw.startswith("@") and re.fullmatch(r"[A-Za-z0-9_\.]+", raw):
        raw = "@" + raw
    return raw


def canonical_source_url(platform: str, handle: str, url: str) -> str:
    if url:
        return url.strip()
    h = canonical_handle(handle).lstrip("@")
    if not h:
        return ""
    if platform == "telegram":
        return f"https://t.me/{h}"
    if platform in {"vk", "vkvideo"}:
        return f"https://vk.com/{h}"
    return h


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not line.strip() or line.lstrip().startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


def load_split_runtime_from_kaggle_input() -> dict[str, Any]:
    roots = [Path("/kaggle/input"), Path.cwd()]
    config: dict[str, Any] = {}
    secret_files: list[Path] = []
    key_files: list[Path] = []
    for root in roots:
        if not root.exists():
            continue
        for p in root.rglob("region_talk_run_config.json"):
            try:
                config.update(json.loads(p.read_text(encoding="utf-8")))
            except Exception:
                pass
        secret_files.extend(root.rglob("region_talk_secrets.enc"))
        key_files.extend(root.rglob("region_talk_fernet.key"))
    if secret_files and key_files:
        try:
            from cryptography.fernet import Fernet
            secrets = json.loads(Fernet(key_files[0].read_bytes().strip()).decrypt(secret_files[0].read_bytes()).decode("utf-8"))
            for k, v in secrets.items():
                if v is not None and str(v).strip():
                    os.environ.setdefault(str(k), str(v))
        except Exception as exc:
            raise RuntimeError(f"failed to load encrypted Region Talk secrets: {type(exc).__name__}") from exc
    return config


class Status:
    def __init__(self) -> None:
        self.client = None
        try:
            from kaggle_status_client import KaggleStatusClient  # type: ignore
            self.client = KaggleStatusClient.discover()
        except Exception:
            self.client = None
        self.events: list[dict[str, Any]] = []

    def event(self, name: str, **payload: Any) -> None:
        clean = {k: v for k, v in payload.items() if v is not None}
        clean.setdefault("event_name", name)
        clean.setdefault("created_at", utc_now_iso())
        self.events.append(clean)
        print(f"[region-talk] {name} {json.dumps({k:v for k,v in clean.items() if k not in {'token'}}, ensure_ascii=False)[:800]}", flush=True)
        if self.client:
            try:
                self.client.event(name, phase=clean.get("phase"), status=clean.get("status"), progress_json=clean)
            except Exception:
                pass


@dataclass
class Seed:
    source_seed_id: str
    platform: str
    source_title: str
    handle: str
    url: str
    source_kind: str
    source_scope_guess: str
    priority: int
    discovered_from: str
    discovered_from_url: str
    why_seeded: str
    expected_value: str
    known_risks: str
    initial_status: str
    monitoring_enabled: bool
    rights_policy: str
    notes: str

    @property
    def source_id(self) -> str:
        return "src_" + stable_hash(self.platform, canonical_source_url(self.platform, self.handle, self.url))

    @property
    def canonical_url(self) -> str:
        return canonical_source_url(self.platform, self.handle, self.url)


def find_seed_file(config: dict[str, Any]) -> Path:
    candidates = []
    for raw in [os.getenv("REGION_TALK_SEED_FILE"), config.get("seed_file"), "seed-sources-v1.csv", "docs/features/region-talk-channel/seed-sources-v1.csv"]:
        if raw:
            candidates.append(Path(str(raw)))
    for root in [Path.cwd(), Path(__file__).resolve().parent, Path("/kaggle/input")]:
        if root.exists():
            candidates.extend(root.rglob("seed-sources-v1.csv"))
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError("seed-sources-v1.csv not found")


def load_seeds(path: Path) -> list[Seed]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    seeds: list[Seed] = []
    for row in rows:
        seeds.append(Seed(
            source_seed_id=str(row.get("source_seed_id") or "").strip(),
            platform=str(row.get("platform") or "").strip().lower(),
            source_title=str(row.get("source_title") or "").strip(),
            handle=canonical_handle(str(row.get("handle") or "").strip()),
            url=str(row.get("url") or "").strip(),
            source_kind=str(row.get("source_kind") or "").strip(),
            source_scope_guess=str(row.get("source_scope_guess") or "").strip(),
            priority=getenv_int("_NO_SUCH_ENV", int(str(row.get("priority") or "999").strip() or "999")),
            discovered_from=str(row.get("discovered_from") or "").strip(),
            discovered_from_url=str(row.get("discovered_from_url") or "").strip(),
            why_seeded=str(row.get("why_seeded") or "").strip(),
            expected_value=str(row.get("expected_value") or "").strip(),
            known_risks=str(row.get("known_risks") or "").strip(),
            initial_status=str(row.get("initial_status") or "").strip(),
            monitoring_enabled=str(row.get("monitoring_enabled") or "").strip().lower() in {"1","true","yes","on"},
            rights_policy=str(row.get("rights_policy") or "unknown").strip() or "unknown",
            notes=str(row.get("notes") or "").strip(),
        ))
    return seeds


def score_text(text: str) -> dict[str, Any]:
    low = (text or "").lower()
    anchor_hits = [a for a in DEFAULT_ANCHORS if a.lower() in low]
    positive_hits = [w for w in POSITIVE_WORDS if w in low]
    news_hits = [w for w in NEWS_WORDS if w in low]
    ad_hits = [w for w in AD_WORDS if w in low]
    trash_hits = [w for w in TRASH_WORDS if w in low]
    region = min(1.0, 0.25 * len(anchor_hits) + (0.25 if len(text) > 240 and anchor_hits else 0))
    news = min(1.0, 0.28 * len(news_hits))
    ad = min(1.0, 0.20 * len(ad_hits))
    trash = min(1.0, 0.35 * len(trash_hits))
    value = min(1.0, 0.25 + 0.08 * len(positive_hits) + min(0.35, len(text) / 1200)) if anchor_hits else 0.0
    tone = min(1.0, 0.35 + 0.12 * len(positive_hits)) if anchor_hits else 0.0
    return {"anchor_hits": anchor_hits, "positive_hits": positive_hits, "news_hits": news_hits, "ad_hits": ad_hits, "trash_hits": trash_hits, "region_relevance_score": round(region,3), "newsiness_score": round(news,3), "ad_score": round(ad,3), "trash_score": round(trash,3), "text_value_score": round(value,3), "positive_or_useful_tone_score": round(tone,3)}


def media_scores(has_media: bool, text_score: dict[str, Any]) -> dict[str, Any]:
    if not has_media:
        return {"technical_quality_score":0.0,"aesthetic_score":0.0,"postcardness_score":0.0,"region_visual_relevance_score":0.0,"publication_safety_score":1.0,"low_noise_score":0.0,"overall_media_score":0.0,"is_selected_for_publication":False,"recognized_visual_elements":"","model_short_explanation":"No media detected; strong-image gate failed.","failure_reason":"no_media","model_id":"cv_only_metadata_v1","model_version":"2026-07-05"}
    anchors = text_score.get("anchor_hits") or []
    positives = text_score.get("positive_hits") or []
    technical = 0.72
    aesthetic = min(0.92, 0.68 + 0.04*len(positives))
    postcard = min(0.94, 0.66 + 0.06*len(anchors) + 0.03*len(positives))
    region_visual = min(0.95, 0.58 + 0.08*len(anchors))
    safety = 0.98 if not text_score.get("news_hits") and not text_score.get("trash_hits") else 0.78
    low_noise = 0.82
    overall = round((technical+aesthetic+postcard+region_visual+safety+low_noise)/6,3)
    selected = technical>=0.65 and aesthetic>=0.70 and postcard>=0.72 and safety>=0.95 and low_noise>=0.80 and overall>=0.75
    elements = []
    low = " ".join(anchors + positives).lower()
    if "море" in low or "балтий" in low: elements.append("sea/coast")
    if "курш" in low or "дюн" in low: elements.append("dunes/nature")
    if "архитект" in low or "кёниг" in low or "калининград" in low: elements.append("city/architecture")
    if not elements: elements.append("travel visual candidate")
    return {"technical_quality_score":round(technical,3),"aesthetic_score":round(aesthetic,3),"postcardness_score":round(postcard,3),"region_visual_relevance_score":round(region_visual,3),"publication_safety_score":round(safety,3),"low_noise_score":round(low_noise,3),"overall_media_score":overall,"is_selected_for_publication":selected,"recognized_visual_elements":"; ".join(elements),"model_short_explanation":f"cv_only metadata heuristic: media present; region anchors={len(anchors)}, travel/visual cues={len(positives)}; safety={'ok' if safety>=0.95 else 'blocked by news/trash cues'}.","failure_reason":"" if selected else "below_strong_image_threshold","model_id":"cv_only_metadata_v1","model_version":"2026-07-05"}


def candidate_score(text_score: dict[str, Any], media: dict[str, Any], seed: Seed) -> float:
    source_quality = 0.72 if seed.priority == 1 else 0.60 if seed.priority == 2 else 0.50
    score = text_score["region_relevance_score"]*0.20 + source_quality*0.12 + 0.55*0.12 + text_score["text_value_score"]*0.16 + text_score["positive_or_useful_tone_score"]*0.12 + media["overall_media_score"]*0.20 + 0.2*0.04 + 0.2*0.04
    score -= text_score["newsiness_score"]*0.35 + text_score["trash_score"]*0.40 + text_score["ad_score"]*0.18
    if not media["is_selected_for_publication"]: score -= 0.25
    if seed.rights_policy == "unknown": score -= 0.03
    return round(max(0.0, min(1.0, score)), 3)


async def fetch_telegram_posts(seeds: list[Seed], status: Status, output_dir: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    max_sources = getenv_int("REGION_TALK_MAX_SOURCES", 5)
    max_posts = getenv_int("REGION_TALK_MAX_POSTS_PER_SOURCE", 20)
    fetch_enabled = getenv_bool("REGION_TALK_FETCH_TELEGRAM", True)
    monitored = [s for s in seeds if s.platform == "telegram" and s.monitoring_enabled]
    monitored = sorted(monitored, key=lambda s: (s.priority, int(s.source_seed_id or 999)))[:max_sources]
    source_rows: list[dict[str, Any]] = []
    posts: list[dict[str, Any]] = []
    if not fetch_enabled:
        for s in monitored:
            source_rows.append({**asdict(s), "source_id": s.source_id, "canonical_url": s.canonical_url, "fetch_status":"skipped_fetch_disabled"})
        return source_rows, posts
    try:
        from telethon import TelegramClient  # type: ignore
        from telethon.sessions import StringSession  # type: ignore
    except Exception:
        if getenv_bool("REGION_TALK_AUTO_INSTALL", True):
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "telethon"])
            from telethon import TelegramClient  # type: ignore
            from telethon.sessions import StringSession  # type: ignore
        else:
            for s in monitored:
                source_rows.append({**asdict(s), "source_id": s.source_id, "canonical_url": s.canonical_url, "fetch_status":"skipped_telethon_not_installed"})
            return source_rows, posts
    bundle_env = (os.getenv("REGION_TALK_AUTH_BUNDLE_ENV") or "TELEGRAM_AUTH_BUNDLE_DISCOVERY").strip()
    raw = (os.getenv(bundle_env) or os.getenv("TELEGRAM_AUTH_BUNDLE_S22") or "").strip()
    api_id = int((os.getenv("TG_API_ID") or os.getenv("TELEGRAM_API_ID") or "0").strip() or 0)
    api_hash = (os.getenv("TG_API_HASH") or os.getenv("TELEGRAM_API_HASH") or "").strip()
    bundle: dict[str, Any] = {}
    if raw:
        bundle = json.loads(base64.urlsafe_b64decode(raw.encode("ascii")).decode("utf-8"))
    session = str(bundle.get("session") or os.getenv("TG_SESSION") or os.getenv("TELEGRAM_SESSION") or "").strip()
    if not session or not api_id or not api_hash:
        for s in monitored:
            source_rows.append({**asdict(s), "source_id": s.source_id, "canonical_url": s.canonical_url, "fetch_status":"skipped_missing_telethon_credentials"})
        return source_rows, posts
    client = TelegramClient(StringSession(session), api_id, api_hash, device_model=str(bundle.get("device_model") or "Region Talk Discovery"), system_version=str(bundle.get("system_version") or "Linux"), app_version=str(bundle.get("app_version") or "1.0"), lang_code=str(bundle.get("lang_code") or "ru"), system_lang_code=str(bundle.get("system_lang_code") or "ru"))
    await client.connect()
    if not await client.is_user_authorized():
        await client.disconnect()
        raise RuntimeError("Telethon client is not authorized")
    try:
        for idx, seed in enumerate(monitored, start=1):
            status.event("alive", phase="fetch", progress_label=f"источники {idx}/{len(monitored)}", sources_done=idx-1, sources_total=len(monitored))
            handle = seed.handle.lstrip("@")
            src_row = {**asdict(seed), "source_id": seed.source_id, "canonical_url": seed.canonical_url, "fetch_status":"ok", "posts_scanned":0}
            try:
                entity = await client.get_entity(handle)
                title = str(getattr(entity, "title", None) or seed.source_title)
                src_row["resolved_title"] = title
                seen: set[int] = set()
                queries = [None] + DEFAULT_ANCHORS[:5]
                for q in queries:
                    per_query = max(3, max_posts // len(queries) + 1)
                    async for msg in client.iter_messages(entity, limit=per_query, search=q):
                        mid = int(getattr(msg, "id", 0) or 0)
                        if not mid or mid in seen:
                            continue
                        seen.add(mid)
                        text = str(getattr(msg, "message", None) or "").strip()
                        if not text:
                            continue
                        dt = getattr(msg, "date", None)
                        post_url = f"https://t.me/{handle}/{mid}"
                        has_media = bool(getattr(msg, "photo", None) or getattr(msg, "document", None) or getattr(msg, "media", None))
                        posts.append({"post_id":"post_"+stable_hash("telegram", handle, mid), "source_id": seed.source_id, "source_seed_id": seed.source_seed_id, "source_title": title, "platform":"telegram", "handle": seed.handle, "post_url": post_url, "platform_post_key": f"tg:{handle}:{mid}", "post_date": dt.isoformat() if dt else "", "text": text, "text_excerpt": re.sub(r"\s+", " ", text)[:500], "has_media": has_media, "media_count": 1 if has_media else 0, "rights_policy": seed.rights_policy, "source_kind": seed.source_kind, "source_type": seed.source_kind, "source_url": seed.canonical_url})
                        if len(seen) >= max_posts:
                            break
                    if len(seen) >= max_posts:
                        break
                src_row["posts_scanned"] = len(seen)
            except Exception as exc:
                src_row["fetch_status"] = "error"
                src_row["fetch_error_code"] = type(exc).__name__
                src_row["fetch_error_message"] = str(exc)[:180]
            source_rows.append(src_row)
    finally:
        await client.disconnect()
    return source_rows, posts


def build_report(seeds: list[Seed], source_rows: list[dict[str, Any]], posts: list[dict[str, Any]], run_id: str, output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    run_now = utc_now_iso()
    source_seed_rows = [{**asdict(s), "source_id": s.source_id, "canonical_url": s.canonical_url} for s in seeds]
    candidates: list[dict[str, Any]] = []
    media_rows: list[dict[str, Any]] = []
    dropped: list[dict[str, Any]] = []
    new_posts: list[dict[str, Any]] = []
    increment: list[dict[str, Any]] = []
    seed_by_id = {s.source_seed_id: s for s in seeds}
    for p in posts:
        seed_for_post = seed_by_id.get(str(p.get('source_seed_id') or '')) or (seeds[0] if seeds else None)
        ts = score_text(p.get("text") or "")
        ms = media_scores(bool(p.get("has_media")), ts)
        cid = "cand_" + stable_hash(p["post_id"], "region-talk-semantic-bank-v1", ms.get("overall_media_score"))
        mid = "media_" + stable_hash(p["post_id"], p.get("post_url"), "media0")
        score = candidate_score(ts, ms, seed_for_post) if seed_for_post else 0
        current_stage = "post_fetched"
        rejection = ""
        if ts["region_relevance_score"] < 0.35:
            current_stage, rejection = "dropped_not_region", "not_about_region"
        elif ts["newsiness_score"] >= 0.45:
            current_stage, rejection = "dropped_news", "newsiness"
        elif ts["trash_score"] >= 0.35:
            current_stage, rejection = "dropped_trash", "trash"
        elif not ms["is_selected_for_publication"]:
            current_stage, rejection = "good_text_weak_media" if ts["region_relevance_score"] >= 0.35 else "dropped_weak_media", ms["failure_reason"]
        elif score >= 0.45:
            current_stage = "favorite" if score >= 0.62 else "semantic_candidate"
        else:
            current_stage, rejection = "dropped_low_score", "candidate_score_low"
        media_rows.append({"media_id": mid, "candidate_id": cid, "post_url": p.get("post_url"), "image_url_or_local_path": (p.get("post_url") + "#media") if p.get("has_media") else "", "thumbnail":"", **ms})
        row = {**p, **ts, **ms, "candidate_id": cid, "candidate_score": score, "current_stage": current_stage, "rejection_reason": rejection, "short_summary": make_summary(p.get("text") or ""), "why_this_is_about_kaliningrad": ", ".join(ts.get("anchor_hits") or []) or "no strong anchor", "what_positive": ", ".join(ts.get("positive_hits") or []), "what_neutral_or_useful":"route/place/travel context" if ts["region_relevance_score"] else "", "what_concern":"news/ad/trash risk checked", "image_model_report_short": ms["model_short_explanation"], "risk_flags": "; ".join([rejection] if rejection else ([] if p.get("rights_policy") != "unknown" else ["rights_unknown"])), "suggested_action": "manual_review" if current_stage in {"favorite","semantic_candidate"} else "reject", "manual_decision":"", "reviewer_comment":""}
        new_posts.append(row)
        increment.append({"entity_type":"post", "entity_id":p["post_id"], "source_title":p.get("source_title"), "post_url":p.get("post_url"), "first_seen_run_id":run_id, "previous_run_id":"", "current_run_id":run_id, "first_seen_at":run_now, "last_seen_at":run_now, "seen_run_count":1, "previous_stage":"", "current_stage":current_stage, "stage_transition":"new→"+current_stage, "new_this_run":"yes", "changed_this_run":"yes", "change_reason":rejection or "first_seen", "candidate_score_previous":"", "candidate_score_current":score, "candidate_score_delta":"", "media_score_previous":"", "media_score_current":ms["overall_media_score"], "media_score_delta":"", "manual_review_status":"unreviewed", "next_action":row["suggested_action"]})
        if current_stage in {"favorite", "semantic_candidate"}:
            candidates.append(row)
        elif current_stage == "good_text_weak_media":
            dropped.append(row)
        else:
            dropped.append(row)
    review_queue = sorted(candidates, key=lambda r: float(r.get("candidate_score") or 0), reverse=True)
    for i, r in enumerate(review_queue, start=1):
        r["rank"] = i
        r["status_badge"] = "READY" if r["current_stage"] == "favorite" else "NEEDS_REVIEW"
        r["new_or_seen"] = "NEW"
    favorites = [r for r in review_queue if r.get("current_stage") == "favorite"]
    run_summary = [{"run_id":run_id,"started_at":RUN_STARTED_AT.isoformat(),"finished_at":run_now,"git_sha":os.getenv("GIT_SHA", ""),"branch":"","config_profile":"mvp1","dry_run":"1","ydb_namespace":os.getenv("REGION_TALK_YDB_NAMESPACE") or "dry-run-json","seed_file_version":"v1","source_count_seeded":len(seeds),"source_count_scanned":len(source_rows),"posts_fetched":len(posts),"posts_region_relevant":sum(1 for r in new_posts if r["region_relevance_score"]>=0.35),"posts_with_strong_media":sum(1 for r in new_posts if r["is_selected_for_publication"]),"candidates_created":len(candidates),"favorites_created":len(favorites),"dropped_news":sum(1 for r in dropped if r["rejection_reason"]=="newsiness"),"dropped_trash":sum(1 for r in dropped if r["rejection_reason"]=="trash"),"dropped_not_region":sum(1 for r in dropped if r["rejection_reason"]=="not_about_region"),"dropped_weak_media":sum(1 for r in dropped if "media" in str(r.get("rejection_reason"))),"dropped_duplicate":0,"dropped_rights":0,"llm_calls":0,"image_model_calls":len(media_rows),"errors_count":sum(1 for s in source_rows if s.get("fetch_status") == "error"),"artifact_paths":""}]
    funnel = []
    for stage in ["seed_source","source_monitored","post_fetched","region_relevant","strong_media","semantic_candidate","favorite"]:
        count = {"seed_source":len(seeds),"source_monitored":len(source_rows),"post_fetched":len(posts),"region_relevant":run_summary[0]["posts_region_relevant"],"strong_media":run_summary[0]["posts_with_strong_media"],"semantic_candidate":len([r for r in candidates if r["current_stage"]=="semantic_candidate"]),"favorite":len(favorites)}[stage]
        funnel.append({"stage":stage,"current_run_count":count,"previous_run_count":"","delta":"","total_cumulative":count,"top_rejection_reasons":"","notes":""})
    sheets = {
        "00_readme":[{"field":"what","value":"Region Talk MVP-1 Candidate Report Only; no Telegram/VK publishing."},{"field":"run_id","value":run_id},{"field":"generated_at","value":run_now}],
        "01_run_summary":run_summary,
        "02_increment":increment,
        "03_funnel":funnel,
        "04_review_queue":review_queue,
        "05_favorites":favorites,
        "06_candidates_all":candidates,
        "07_new_posts_this_run":new_posts,
        "08_dropped_posts":dropped,
        "09_image_quality":media_rows,
        "10_good_text_weak_media":[r for r in dropped if r.get("current_stage") == "good_text_weak_media"],
        "11_sources_seed":source_seed_rows,
        "12_sources_discovered":[],
        "13_sources_monitored":source_rows,
        "14_verifier_reports":[],
        "15_manual_decisions":[{"candidate_id":"","manual_decision":"favorite|reject|approve_for_preview|approve_for_queue|block_source","reviewer":"","reviewed_at":"","reviewer_comment":"","rights_override":"","source_status_override":""}],
        "16_publish_preview_future":[{"note":"Future only. REGION_TALK_DISABLE_PUBLISH=1; no real publishing in MVP-1."}],
    }
    xlsx = output_dir / f"region-talk-candidates-{run_id}.xlsx"
    write_xlsx(xlsx, sheets)
    write_csv(output_dir / f"region-talk-candidates-{run_id}.csv", review_queue)
    payload = {"ok": True, "run_id": run_id, "generated_at": run_now, "summary": run_summary[0], "sheets": sheets, "xlsx_path": str(xlsx)}
    (output_dir / f"region-talk-candidates-{run_id}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / f"region-talk-candidates-{run_id}.md").write_text(render_md(payload), encoding="utf-8")
    (output_dir / f"region-talk-candidates-{run_id}.html").write_text(render_html(payload), encoding="utf-8")
    # Kaggle output-root convenience copies
    for p in [xlsx, output_dir / f"region-talk-candidates-{run_id}.json", output_dir / f"region-talk-candidates-{run_id}.md", output_dir / f"region-talk-candidates-{run_id}.html", output_dir / f"region-talk-candidates-{run_id}.csv"]:
        target = Path.cwd() / p.name
        if target.resolve() != p.resolve():
            target.write_bytes(p.read_bytes())
    latest = Path.cwd() / "candidates-latest.xlsx"
    latest.write_bytes(xlsx.read_bytes())
    (Path.cwd() / "output.json").write_text(json.dumps({"ok": True, "run_id": run_id, "xlsx": str(latest), "summary": run_summary[0]}, ensure_ascii=False, indent=2), encoding="utf-8")
    payload["latest_xlsx"] = str(latest)
    return payload


def make_summary(text: str) -> str:
    clean = re.sub(r"\s+", " ", text or "").strip()
    if len(clean) <= 180:
        return clean
    return clean[:177].rsplit(" ",1)[0] + "…"


def rows_headers(rows: list[dict[str, Any]]) -> list[str]:
    headers: list[str] = []
    for r in rows:
        for k in r.keys():
            if k not in headers:
                headers.append(k)
    return headers or ["empty"]


def col_name(n: int) -> str:
    s = ""
    while n:
        n, rem = divmod(n-1, 26)
        s = chr(65+rem) + s
    return s


def cell_xml(value: Any, row: int, col: int) -> str:
    ref = f"{col_name(col)}{row}"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return f'<c r="{ref}"><v>{value}</v></c>'
    text = escape(str(value if value is not None else ""))
    return f'<c r="{ref}" t="inlineStr"><is><t>{text}</t></is></c>'


def sheet_xml(rows: list[dict[str, Any]]) -> str:
    headers = rows_headers(rows)
    all_rows = [dict(zip(headers, headers))] + rows
    body=[]
    for ri, r in enumerate(all_rows, start=1):
        cells = ''.join(cell_xml(r.get(h, ""), ri, ci) for ci, h in enumerate(headers, start=1))
        body.append(f'<row r="{ri}">{cells}</row>')
    last = f"{col_name(len(headers))}{max(1,len(all_rows))}"
    return f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetViews><sheetView workbookViewId="0"><pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/></sheetView></sheetViews><dimension ref="A1:{last}"/><sheetData>{''.join(body)}</sheetData><autoFilter ref="A1:{col_name(len(headers))}1"/></worksheet>'''


def write_xlsx(path: Path, sheets: dict[str, list[dict[str, Any]]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    names = list(sheets.keys())
    sheet_overrides = ''.join(
        f'<Override PartName="/xl/worksheets/sheet{i}.xml" '
        f'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        for i in range(1, len(names) + 1)
    )
    sheet_relationships = ''.join(
        f'<Relationship Id="rId{i}" '
        f'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
        f'Target="worksheets/sheet{i}.xml"/>'
        for i in range(1, len(names) + 1)
    )
    styles_rid = len(names) + 1
    created_at = escape(RUN_STARTED_AT.replace(microsecond=0).isoformat().replace('+00:00', 'Z'))
    with zipfile.ZipFile(path, 'w', compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr('[Content_Types].xml', f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/><Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/><Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>{sheet_overrides}</Types>''')
        z.writestr('_rels/.rels', '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/><Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/></Relationships>''')
        z.writestr('docProps/core.xml', f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dcmitype="http://purl.org/dc/dcmitype/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><dc:creator>events-bot-new</dc:creator><dc:title>Region Talk MVP-1 candidate report</dc:title><dcterms:created xsi:type="dcterms:W3CDTF">{created_at}</dcterms:created><dcterms:modified xsi:type="dcterms:W3CDTF">{created_at}</dcterms:modified></cp:coreProperties>''')
        z.writestr('docProps/app.xml', f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes"><Application>events-bot-new</Application><DocSecurity>0</DocSecurity><ScaleCrop>false</ScaleCrop><HeadingPairs><vt:vector size="2" baseType="variant"><vt:variant><vt:lpstr>Worksheets</vt:lpstr></vt:variant><vt:variant><vt:i4>{len(names)}</vt:i4></vt:variant></vt:vector></HeadingPairs><TitlesOfParts><vt:vector size="{len(names)}" baseType="lpstr">{''.join(f'<vt:lpstr>{escape(name[:31])}</vt:lpstr>' for name in names)}</vt:vector></TitlesOfParts></Properties>''')
        z.writestr('xl/styles.xml', '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><fonts count="1"><font><sz val="11"/><color theme="1"/><name val="Calibri"/><family val="2"/><scheme val="minor"/></font></fonts><fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills><borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders><cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs><cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs><cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles><dxfs count="0"/><tableStyles count="0" defaultTableStyle="TableStyleMedium2" defaultPivotStyle="PivotStyleLight16"/></styleSheet>''')
        z.writestr('xl/workbook.xml', '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets>' + ''.join(f'<sheet name="{escape(name[:31])}" sheetId="{i}" r:id="rId{i}"/>' for i,name in enumerate(names,1)) + '</sheets></workbook>')
        z.writestr('xl/_rels/workbook.xml.rels', f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">{sheet_relationships}<Relationship Id="rId{styles_rid}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/></Relationships>''')
        for i, name in enumerate(names, 1):
            z.writestr(f'xl/worksheets/sheet{i}.xml', sheet_xml(sheets[name]))


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    headers = rows_headers(rows)
    with path.open('w', encoding='utf-8', newline='') as f:
        w=csv.DictWriter(f, fieldnames=headers, extrasaction='ignore')
        w.writeheader(); w.writerows(rows)


def render_md(payload: dict[str, Any]) -> str:
    s = payload.get('summary', {})
    return f"# Region Talk Candidate Report\n\n- run_id: `{payload.get('run_id')}`\n- posts_fetched: **{s.get('posts_fetched',0)}**\n- candidates_created: **{s.get('candidates_created',0)}**\n- favorites_created: **{s.get('favorites_created',0)}**\n- xlsx: `{payload.get('latest_xlsx') or payload.get('xlsx_path')}`\n"


def render_html(payload: dict[str, Any]) -> str:
    return '<!doctype html><meta charset="utf-8"><pre>' + html.escape(render_md(payload)) + '</pre>'


async def amain() -> int:
    load_dotenv(Path('.env'))
    config = load_split_runtime_from_kaggle_input()
    for k, v in (config.get('env') or {}).items():
        if v is not None:
            os.environ.setdefault(str(k), str(v))
    run_id = os.getenv('REGION_TALK_RUN_ID') or str(config.get('run_id') or f"region-talk-{RUN_STARTED_AT.strftime('%Y%m%dT%H%M%SZ')}")
    if not getenv_bool('REGION_TALK_DRY_RUN', True):
        raise RuntimeError('REGION_TALK_DRY_RUN=1 is required for MVP-1')
    if not getenv_bool('REGION_TALK_DISABLE_PUBLISH', True):
        raise RuntimeError('REGION_TALK_DISABLE_PUBLISH=1 is required for MVP-1')
    status = Status()
    status.event('kernel_started', phase='start', status='running', run_id=run_id)
    seed_path = find_seed_file(config)
    seeds = load_seeds(seed_path)
    out_dir = Path(os.getenv('REGION_TALK_OUTPUT_DIR') or str(config.get('output_dir') or f"artifacts/region-talk/runs/{run_id}"))
    status.event('preflight_ok', phase='preflight', status='running', run_id=run_id, seeds=len(seeds), seed_file=str(seed_path), dry_run=True, disable_publish=True)
    source_rows, posts = await fetch_telegram_posts(seeds, status, out_dir)
    status.event('posts_fetched', phase='fetch', status='running', sources_scanned=len(source_rows), posts_fetched=len(posts))
    payload = build_report(seeds, source_rows, posts, run_id, out_dir)
    summary = payload.get('summary', {})
    status.event('report_written', phase='report', status='done', run_id=run_id, posts_fetched=summary.get('posts_fetched'), candidates_created=summary.get('candidates_created'), favorites_created=summary.get('favorites_created'), xlsx=str(payload.get('latest_xlsx')))
    return 0


if __name__ == '__main__':
    raise SystemExit(asyncio.run(amain()))
