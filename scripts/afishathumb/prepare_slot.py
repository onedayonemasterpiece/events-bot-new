"""Prepare a single AfishaThumb slot for Stage-2 still rendering.

Runs OUTSIDE Blender. Selects one event from the prod snapshot DB,
downloads its primary poster, builds the sticker textures via
`typography.py`, and writes a manifest JSON consumed by the in-Blender
renderer (`render_slot_blender.py`).

Output is dropped under `artifacts/afishathumb/slot_<event_id>/`:

    poster.png             # downloaded poster image (resized to ~1080px)
    sticker_date.png       # date+time card
    sticker_location.png   # venue/address/city card
    sticker_free.png OR    # «БЕСПЛАТНО» card
    sticker_price.png      # price card
    sticker_digest.png     # search_digest fallback
    sticker_attention.png  # promo «обратите внимание» (only when promo)
    manifest.json          # geometric plan: per-sticker placement + paper params
"""

from __future__ import annotations

import argparse
import io
import json
import math
import sqlite3
import sys
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Optional
from urllib.request import Request, urlopen

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "kaggle" / "AfishaThumb" / "scripts"))

import random  # noqa: E402

from PIL import Image  # noqa: E402

from poster_analysis import PosterAnalysis, analyze_poster, fill_ratio_at  # noqa: E402
from poster_llm import PosterRegions, detect_regions, fallback_regions_from_density  # noqa: E402
from scene_llm import SceneLayout, canvas_to_world, plan_scene_layout  # noqa: E402
from story_plan import StoryBeat, StoryPlan, plan_story  # noqa: E402
from typography import (  # noqa: E402
    StickerStyle,
    render_attention_card,
    render_date_card,
    render_digest_card,
    render_free_card,
    render_location_card,
    render_price_card,
    render_title_card,
)


DB_PATH = REPO_ROOT / "db_prod_snapshot.sqlite"
OUT_ROOT = REPO_ROOT / "artifacts" / "afishathumb"


@dataclass
class PaperPlan:
    """Mirror of `kaggle/AfishaThumb/scripts/layout_posters.PaperPlacement`."""
    image: str
    anchor_angle_deg: float
    anchor_z: float
    width: float
    height: float
    tilt_deg: float = 0.0
    peel_corners: tuple[bool, bool, bool, bool] = (False, False, False, False)
    peel_intensity: float = 1.0
    wrinkle: float = 0.0
    name: str = "Paper"
    paper_offset: float = 0.004  # progressively raised so stacked papers
                                 # don't z-fight on the cylinder surface


@dataclass
class BeatPlan:
    """Serialisable mirror of `story_plan.StoryBeat`."""
    role: str
    target_anchor: str
    label: str
    dwell_s: float
    lens_mm: float
    radius_factor: float
    color: str
    is_focus_push: bool


@dataclass
class SlotManifest:
    event_id: int
    title: str
    date_iso: str
    time_text: str
    location_name: str
    address: str
    city: str
    is_free: bool
    price_text: str
    search_digest: str
    is_promo: bool
    papers: list[PaperPlan] = field(default_factory=list)
    # Camera framing: close-up centred on the main poster.
    camera_focus_angle_deg: float = 0.0
    camera_focus_z: float = 0.0
    camera_radius: float = 2.2
    camera_lens_mm: float = 70.0
    camera_target_offset_z: float = 0.0
    render_w: int = 1080
    render_h: int = 1572
    # Story plan + coverage tests for this slot.
    beats: list[BeatPlan] = field(default_factory=list)
    coverage_tests: dict = field(default_factory=dict)
    poster_text_density: float = 0.0
    # LLM-detected info regions on the poster (normalised 0..1, origin
    # top-left). Each is either null or [x0, y0, x1, y1]. Drives sticker
    # skip decisions and camera focus targets.
    poster_regions: dict = field(default_factory=dict)
    # Per-event tightness budget (degrees of arc beyond poster edge that
    # stickers can occupy). Seeded random in [3°, 15°] so events vary.
    tightness_budget_deg: float = 12.0
    # Number of attention anchors the camera arc visits (2 or 3, seeded).
    attention_anchor_count: int = 2


def _date_already_on_poster(ev: dict) -> bool:
    """Best-effort: when the event's poster carries the date legibly we
    skip the standalone date sticker (point 6 of the v1 feedback). For
    Stage-2 demo this is operator-controllable through `--no-date-sticker`;
    long-term this should be derived from `EventPoster.ocr_text` matching
    the event's date string.

    Right now the auto-detect just returns False and the caller flips
    the flag explicitly. Real implementation comes when we wire up the
    actual `EventPoster` join.
    """
    return False


def _pick_event(conn: sqlite3.Connection, event_id: Optional[int]) -> dict:
    c = conn.cursor()
    if event_id is not None:
        c.execute(
            "SELECT id,title,date,time,location_name,location_address,city,"
            "ticket_price_min,ticket_price_max,is_free,search_digest,photo_urls "
            "FROM event WHERE id=?",
            (event_id,),
        )
    else:
        # Default: pick a free, near-future event with a poster from a
        # cooperative host (storage.yandexcloud.net) and a populated digest.
        today = date.today().isoformat()
        c.execute(
            "SELECT id,title,date,time,location_name,location_address,city,"
            "ticket_price_min,ticket_price_max,is_free,search_digest,photo_urls "
            "FROM event WHERE date >= ? AND photo_urls IS NOT NULL AND photo_urls != '[]' "
            "AND search_digest IS NOT NULL AND is_free = 1 "
            "ORDER BY date LIMIT 1",
            (today,),
        )
    row = c.fetchone()
    if row is None:
        raise SystemExit("no candidate event")
    cols = [d[0] for d in c.description]
    return dict(zip(cols, row))


def _download_poster(urls: list[str], out_path: Path) -> Path:
    """Try poster URLs in order, save first successful download to out_path,
    resized to at most 1080px on the long edge for fast Blender loading."""
    last_err: Optional[Exception] = None
    for url in urls:
        if "catbox.moe" in url:
            continue
        try:
            req = Request(url, headers={"User-Agent": "afishathumb-stage2/0.1"})
            with urlopen(req, timeout=20) as resp:
                data = resp.read()
            img = Image.open(io.BytesIO(data)).convert("RGB")
            img.thumbnail((1080, 1620))
            out_path.parent.mkdir(parents=True, exist_ok=True)
            img.save(out_path, format="PNG")
            print(f"[prepare] downloaded poster from {url} -> {out_path}")
            return out_path
        except Exception as exc:  # noqa: BLE001
            print(f"[prepare] WARN: failed {url}: {exc!r}")
            last_err = exc
    raise SystemExit(f"could not fetch any poster URL: {last_err!r}")


def _parse_iso_date(s: str) -> tuple[int, int]:
    d = datetime.strptime(s, "%Y-%m-%d").date()
    return d.day, d.month


def _format_price(price_min, price_max) -> str:
    if price_min and price_max and price_min != price_max:
        return f"ОТ {int(price_min)} ₽"
    if price_min:
        return f"{int(price_min)} ₽"
    return ""


def prepare_slot(
    event_id: Optional[int],
    skip_date_sticker: bool = False,
    placement: str = "algo",
) -> SlotManifest:
    conn = sqlite3.connect(DB_PATH)
    ev = _pick_event(conn, event_id)
    print(f"[prepare] picked event {ev['id']}: {ev['title']}")

    suffix = "" if placement == "algo" else f"_{placement}"
    slot_dir = OUT_ROOT / f"slot_{ev['id']}{suffix}"
    slot_dir.mkdir(parents=True, exist_ok=True)
    # Round-6 file hygiene: wipe previous render outputs so the operator
    # is never looking at a stale frame after a re-prep. We keep
    # poster.png + manifest.json + poster_regions.json + the sticker
    # source PNGs + the text-mask debug image (they get regenerated).
    for stale in (
        "slot_overview.png", "slot_main.png", "slot_info.png",
        "slot_close.png", "slot_wide.png",
        "slot_trace.png", "screen_coords.json",
    ):
        p = slot_dir / stale
        if p.exists():
            p.unlink()

    urls = json.loads(ev["photo_urls"] or "[]")
    poster_path = slot_dir / "poster.png"
    _download_poster(urls, poster_path)

    # Multi-image support: up to 3 EXTRAS beyond the primary, max total
    # of 4 per requirements line 17. Round-9 adds perceptual-hash dedup
    # so events with two near-identical poster scans (e.g. 4131) don't
    # render as "two of the same poster".
    MAX_TOTAL_IMAGES = 4
    import cv2  # noqa: WPS433 — local import keeps script lightweight if cv2 isn't used elsewhere
    import numpy as np  # noqa: WPS433

    def _phash(path: Path) -> Optional[int]:
        """Tiny 8×8 average-hash. Cheap, contrib-free, but more than
        good enough for "are these two near-identical poster scans"."""
        try:
            data = np.fromfile(str(path), dtype=np.uint8)
            img = cv2.imdecode(data, cv2.IMREAD_COLOR)
            if img is None:
                return None
            small = cv2.resize(img, (8, 8), interpolation=cv2.INTER_AREA)
            gray = cv2.cvtColor(small, cv2.COLOR_BGR2GRAY)
            avg = gray.mean()
            bits = 0
            for v in gray.flatten():
                bits = (bits << 1) | (1 if v > avg else 0)
            return int(bits)
        except Exception:
            return None

    def _phash_dist(a: Optional[int], b: Optional[int]) -> int:
        if a is None or b is None:
            return 999
        return bin(a ^ b).count("1")

    primary_phash = _phash(poster_path)
    extra_paths: list[Path] = []
    kept_phashes: list[int] = []
    if primary_phash is not None:
        kept_phashes.append(primary_phash)
    for i, url in enumerate(urls[1:]):
        if len(extra_paths) >= MAX_TOTAL_IMAGES - 1:
            break
        if "catbox.moe" in url:
            continue
        extra_p = slot_dir / f"poster_extra_{i+1}.png"
        try:
            _download_poster([url], extra_p)
        except SystemExit:
            print(f"[prepare] skipping unreachable extra image: {url}")
            continue
        ph = _phash(extra_p)
        # Dedup: drop if this extra is within Hamming-distance 12 of any
        # already-kept image (round-9 P4).
        if any(_phash_dist(ph, k) <= 12 for k in kept_phashes):
            print(f"[prepare] near-duplicate image dropped: {extra_p.name}")
            try:
                extra_p.unlink()
            except OSError:
                pass
            continue
        extra_paths.append(extra_p)
        if ph is not None:
            kept_phashes.append(ph)

    # Poster content analysis (cv2 text-density mask) and LLM region
    # detection (Gemini-lite). LLM authority overrides cv2 heuristics
    # when present; when LLM is unavailable, cv2 fallback fills only
    # `title` and lets the placement engine assume nothing else is on
    # the poster.
    poster_analysis = analyze_poster(
        poster_path, save_debug_mask_to=slot_dir / "poster_text_mask.png"
    )
    regions = detect_regions(
        poster_path,
        cache_path=slot_dir / "poster_regions.json",
    )
    if regions.source == "cv2_fallback":
        regions = fallback_regions_from_density(poster_analysis)
        regions.source = "cv2_fallback"

    has_title_on_poster = regions.has_legible("title")
    has_date_on_poster = regions.has_legible("date") or skip_date_sticker
    has_time_on_poster = regions.has_legible("time")
    has_location_on_poster = regions.has_legible("location")
    has_price_on_poster = regions.has_legible("price")
    # "essence" = enough text body on the poster that a separate digest
    # sticker would be duplicative. We trust the LLM if it found multiple
    # info blocks (the poster carries its own info architecture);
    # otherwise we fall back to the raw density threshold.
    llm_non_null = sum(1 for k in ("title", "date", "time", "location", "price") if getattr(regions, k) is not None)
    has_essence_on_poster = (
        llm_non_null >= 3 or poster_analysis.text_density > 0.30
    )

    print(
        f"[prepare] poster analysis: density={poster_analysis.text_density:.2f} "
        f"llm_source={regions.source} non_null={llm_non_null} "
        f"on_poster: title={has_title_on_poster} date={has_date_on_poster} "
        f"time={has_time_on_poster} loc={has_location_on_poster} price={has_price_on_poster} "
        f"essence={has_essence_on_poster}"
    )

    day, month = _parse_iso_date(ev["date"])
    time_text = (ev["time"] or "").strip()
    # Use only the first half of ranged times for the card.
    if ".." in time_text:
        time_text = time_text.split("..", 1)[0]

    # Date sticker is skipped if EITHER the LLM-detected date region is
    # legible on the poster OR operator passed `--no-date-sticker`.
    include_date_sticker = not has_date_on_poster
    date_card: Optional[Path] = None
    if include_date_sticker:
        date_card = slot_dir / "sticker_date.png"
        render_date_card(date_card, day=day, month=month, time_text=time_text)

    # Title sticker logic (round-11): the product test that matters is
    # «зритель прочитает название за пролёт сцены». Even on a "real
    # poster" with many LLM regions detected, the title may not be a
    # legible title text — e.g. a ballet poster (4828) shows the
    # performance scene + theatre name but NO event name. So we only
    # SKIP the title banner when LLM detected a `title` region that is
    # both present AND big enough to read (≥ 4% of poster area).
    has_extras = len(extra_paths) > 0
    primary_is_poster = llm_non_null >= 3
    title_bbox = regions.title
    title_area = 0.0
    if title_bbox:
        title_area = (title_bbox[2] - title_bbox[0]) * (title_bbox[3] - title_bbox[1])
    title_clearly_on_poster = title_bbox is not None and title_area >= 0.04
    title_card: Optional[Path] = None
    needs_title_banner = (
        bool((ev.get("title") or "").strip())
        and not title_clearly_on_poster
    )
    is_no_poster_event = needs_title_banner  # kept for downstream code
    if needs_title_banner:
        title_card = slot_dir / "sticker_title.png"
        render_title_card(title_card, title=str(ev["title"]))

    # Address dedup. Real prod rows like 4131 store
    #   location_name = "Барн, каштановая аллея 1а"
    #   address       = "Каштановая Аллея 1А"
    # which renders as `<location> <repeated address> · <city>`. Drop the
    # address when it's a substring of (or identical to) the location_name,
    # and strip a trailing duplicated city.
    raw_loc = (ev["location_name"] or "").strip()
    raw_addr = (ev["location_address"] or "").strip()
    raw_city = (ev["city"] or "").strip()

    def _norm(s: str) -> str:
        return " ".join(s.lower().replace(",", " ").split())

    norm_loc = _norm(raw_loc)
    norm_addr = _norm(raw_addr)
    norm_city = _norm(raw_city)
    if norm_addr and (norm_addr in norm_loc or norm_loc.endswith(norm_addr)):
        raw_addr = ""
    if raw_addr and norm_city and norm_addr.endswith(norm_city):
        # Cut the city tail off the address; city stays in the city slot.
        idx = raw_addr.lower().rfind(raw_city.lower())
        if idx > 0:
            raw_addr = raw_addr[:idx].rstrip(",. ").strip()
    # Some prod rows (e.g. event 4131) store the city inside `location_name`
    # itself (`"Барн, каштановая аллея 1А, Калининград"`). The location card
    # already has a dedicated city slot, so strip the trailing city from the
    # venue name to avoid the rendered duplicate `БАРН ... КАЛИНИНГРАД` +
    # `Калининград`.
    if raw_city and norm_loc.endswith(norm_city):
        idx = raw_loc.lower().rfind(raw_city.lower())
        if idx > 0:
            raw_loc = raw_loc[:idx].rstrip(",. ").strip()
    # And if the address (after dedup) is now inside the location_name as a
    # comma-separated tail, lift it into the dedicated address slot so the
    # name stays short and readable.
    if not raw_addr and "," in raw_loc:
        head, _, tail = raw_loc.rpartition(",")
        tail = tail.strip()
        # Heuristic: address-like tails contain digits or street-keywords.
        addr_markers = ("ул.", "пр.", "пер.", "наб.", "просп", "д.", "стр.")
        if tail and (any(ch.isdigit() for ch in tail)
                     or any(m in tail.lower() for m in addr_markers)):
            raw_addr = tail
            raw_loc = head.rstrip(",. ").strip()

    # Location sticker: skip if the LLM confirmed the venue is already
    # printed on the poster at legible size (rare but happens).
    loc_card: Optional[Path] = None
    if not has_location_on_poster:
        loc_card = slot_dir / "sticker_location.png"
        render_location_card(
            loc_card,
            location_name=raw_loc,
            address=raw_addr,
            city=raw_city,
        )

    extras: list[Path] = []
    is_free = bool(ev["is_free"])
    cost_card: Optional[Path] = None
    if not has_price_on_poster:
        if is_free:
            cost_card = slot_dir / "sticker_free.png"
            render_free_card(cost_card, style=StickerStyle(
                bg=(244, 217, 71, 255), fg=(20, 14, 14, 255),
                corner_radius=18, paper_grain=True,
            ))
            extras.append(cost_card)
        else:
            ptxt = _format_price(ev["ticket_price_min"], ev["ticket_price_max"])
            if ptxt:
                cost_card = slot_dir / "sticker_price.png"
                render_price_card(cost_card, ptxt)
                extras.append(cost_card)

    # Digest sticker (round-11): create whenever the event has a
    # search_digest. The previous rule "skip if poster is text-dense"
    # over-fired — even posters with text on them don't necessarily
    # tell the viewer what the event IS about (a ballet poster might
    # have "Шурале" but not what kind of show). The digest helps the
    # viewer decide attend/skip.
    digest_card: Optional[Path] = None
    if ev["search_digest"]:
        digest_card = slot_dir / "sticker_digest.png"
        render_digest_card(digest_card, ev["search_digest"])

    # Layout plan. Coordinates are in column units (D = body diameter = 1.0).
    # Body radius = 0.5 D; body z-span ~ [0.18 .. 2.68].
    # Strategy:
    #  - The poster is the anchor and is large (≈ 0.78 D tall) so the
    #    "lookup" beat can frame it nearly full-screen.
    #  - Info-stickers sit tight around the poster (≤ 22° angular offset)
    #    so the cluster reads as one belongs-together set, not a billboard
    #    with floating labels.
    #  - Wrinkle + a single peeled corner per cluster — paper feel without
    #    chaos.
    #  - For the close-up beat the date card hugs the upper-left corner
    #    of the poster (a real human "first look" anchor) and the location
    #    card sits at the lower-right.
    anchor_z = 1.65
    anchor_angle = 0.0     # main poster on +X face

    # Plane dimensions follow the actual poster aspect so portraits stay
    # portrait and landscape photos read wide-and-short instead of being
    # vertically stretched onto a fixed portrait plane. We cap the arc
    # width at ≈ 60° (≈ 0.52 plane width on radius 0.5) so the wrap stays
    # visible from a single anchor angle.
    with Image.open(poster_path) as _pimg:
        poster_aspect = _pimg.size[0] / max(1, _pimg.size[1])
    # Hard cap on arc width: poster_w / cyl_radius (0.5) ≤ ~75° = 1.31 rad.
    # That bounds poster_w at ~0.66 so the cluster + side stickers all
    # fit in one cinematic frame instead of half-disappearing behind the
    # column. Landscape posters get shorter height but the same arc.
    MAX_POSTER_W = 0.66
    if poster_aspect >= 1.0:
        poster_w = min(MAX_POSTER_W, 0.50 * poster_aspect)
        poster_h = poster_w / poster_aspect
    else:
        poster_h = min(1.10, MAX_POSTER_W / poster_aspect)
        poster_w = poster_h * poster_aspect
        poster_w = min(MAX_POSTER_W, poster_w)
        poster_h = poster_w / poster_aspect


    # Seeded RNG so each event ends up with its OWN sticker layout but
    # the same event always reproduces.
    rng = random.Random(int(ev["id"]))

    def jitter(lo: float, hi: float) -> float:
        return rng.uniform(lo, hi)

    def coin(p: float = 0.5) -> bool:
        return rng.random() < p

    # C1 — per-event tightness budget in [3°, 15°] (operator-locked).
    tightness_deg = rng.uniform(3.0, 15.0)
    # S1 — attention anchor count, 2 or 3, weighted slightly toward 2.
    attention_anchor_count = 2 if rng.random() < 0.65 else 3
    # Cluster grouping: all stickers go on ONE side of the poster.
    # For photo-cluster events the stickers go ON the primary (P3), so
    # only L/R sides make sense — corner / T / B placements are reserved
    # for poster events where the cluster sits beside the poster.
    cluster_side = rng.choice(
        ["L", "R"]
        if not primary_is_poster
        else ["R", "L", "BR", "BL", "TR", "TL"]
    )
    print(
        f"[prepare] tightness_deg={tightness_deg:.1f} attention_anchors={attention_anchor_count}"
    )

    # Poster peel: V3 — at most one strong corner, optionally one mild
    # corner on the opposite side. Never two adjacent corners (would fold
    # over the title area). Intensity stays gentle so readability survives.
    strong_corner_idx = rng.randint(0, 3)
    peel_mask = [False, False, False, False]
    peel_mask[strong_corner_idx] = True
    if coin(0.35):
        # diagonally opposite mild peel
        peel_mask[3 - strong_corner_idx] = True
    # Round-8.1 normalisation: the primary image MUST keep its natural
    # aspect (round-8 4131 render stretched a portrait poster
    # horizontally because we shrank `primary_h` to make space for
    # title-banner + extras while keeping `primary_w` fixed). We now
    # compute primary at its aspect-correct dimensions and place title
    # banner + extras strip ABOVE / BELOW it as extra bands.
    primary_w_final = poster_w
    primary_h_final = poster_h  # preserves aspect (poster_w/poster_h = poster_aspect)
    title_banner_band_h = max(primary_h_final * 0.14, 0.10) if title_card is not None else 0.0
    extras_band_h = 0.0
    if extra_paths:
        # Rule N1: each extra is `~0.50` of the primary's longer side.
        # For a portrait primary the longer side is height → extras_h
        # equals `0.50 * primary_h`. For landscape, longer side is width
        # → extras_h equals `0.50 * primary_w`. We pick whichever keeps
        # the extras visibly substantial.
        primary_longer = max(primary_h_final, primary_w_final)
        extras_band_h = primary_longer * 0.50
    # ─── LLM-A branch ─────────────────────────────────────────────────
    # When `placement=="llm"`, ask Gemini-lite to design the layout
    # instead of running the deterministic geometric placer. Falls back
    # to the deterministic placer on LLM failure.
    _llm_done = None
    if placement == "llm":
        # Build scene brief.
        cost_text = (
            "БЕСПЛАТНО" if is_free
            else (_format_price(ev["ticket_price_min"], ev["ticket_price_max"])
                  or "уточняется")
        )
        # Aspects of available images.
        extras_aspects: list[float] = []
        for ep in extra_paths:
            try:
                with Image.open(ep) as _im:
                    extras_aspects.append(_im.size[0] / max(1, _im.size[1]))
            except Exception:
                extras_aspects.append(1.0)
        required_stickers: list[str] = []
        if title_card is not None:
            required_stickers.append("title_banner: wide ribbon with the full event name")
        if date_card is not None:
            required_stickers.append("date: a small card with day, month name, and start time")
        if loc_card is not None:
            required_stickers.append("location: a small card with venue + city")
        if cost_card is not None:
            required_stickers.append(
                "cost: a yellow card «БЕСПЛАТНО»" if is_free
                else "cost: a small card with the price"
            )
        if digest_card is not None:
            required_stickers.append("digest: a card with a short event description")

        scene_brief = {
            "title": ev["title"],
            "date": ev["date"],
            "time": time_text,
            "location": raw_loc,
            "city": raw_city,
            "cost_text": cost_text,
            "search_digest": ev["search_digest"],
            "primary_aspect": poster_aspect,
            "extras_aspects": extras_aspects,
            "regions": {
                "title": list(regions.title) if regions.title else None,
                "date": list(regions.date) if regions.date else None,
                "time": list(regions.time) if regions.time else None,
                "location": list(regions.location) if regions.location else None,
                "price": list(regions.price) if regions.price else None,
            },
            "required_stickers": required_stickers,
        }
        layout = plan_scene_layout(
            scene_brief,
            cache_path=slot_dir / "scene_llm_layout.json",
        )
        if layout.source == "fallback" or not layout.objects:
            print("[prepare] LLM-A returned no layout — falling back to deterministic placement")
        else:
            print(f"[prepare] LLM-A narrative: {layout.narrative}")
            # Map LLM `id` to (image path, role, sticker-like flag).
            asset_map: dict[str, tuple[Path, str, bool]] = {
                "primary": (poster_path, "image", False),
            }
            for i, ep in enumerate(extra_paths, start=1):
                asset_map[f"extra_{i}"] = (ep, "image", False)
            if title_card is not None:
                asset_map["title_banner"] = (title_card, "title", True)
                asset_map["title"] = (title_card, "title", True)
            if date_card is not None:
                asset_map["date"] = (date_card, "date", True)
            if loc_card is not None:
                asset_map["location"] = (loc_card, "location", True)
            if cost_card is not None:
                asset_map["cost"] = (cost_card, "cost", True)
            if digest_card is not None:
                asset_map["digest"] = (digest_card, "essence", True)

            papers: list[PaperPlan] = []
            for idx, obj in enumerate(layout.objects):
                if obj.id not in asset_map:
                    # Tolerate slight id variations the LLM might invent.
                    continue
                img_path, role, is_sticker = asset_map[obj.id]
                angle_deg, z, w_d, h_d = canvas_to_world(
                    obj.x_norm, obj.y_norm, obj.w_norm, obj.h_norm
                )
                # Paper-offset stagger so stickers sit above the
                # primary/extras without z-fighting (round-8 rule).
                if is_sticker:
                    p_offset = 0.025 + 0.003 * idx
                else:
                    p_offset = 0.004 + 0.0006 * idx
                # Resolve a manifest-friendly paper name.
                if obj.id == "primary":
                    name = f"Poster.{ev['id']}"
                elif obj.id.startswith("extra_"):
                    name = f"Image.{ev['id']}.{obj.id.split('_', 1)[1]}"
                else:
                    name = f"{obj.id.capitalize()}.{ev['id']}"
                papers.append(PaperPlan(
                    image=str(img_path),
                    anchor_angle_deg=angle_deg,
                    anchor_z=z,
                    width=max(0.05, w_d),
                    height=max(0.05, h_d),
                    tilt_deg=obj.tilt_deg,
                    peel_corners=(False, False, False, False),
                    peel_intensity=0.0 if is_sticker else 0.25,
                    wrinkle=0.04 if is_sticker else 0.10,
                    paper_offset=p_offset,
                    name=name,
                ))

            # Build the manifest from LLM placement and skip the rest of
            # the deterministic flow. Story plan still runs deterministically
            # below (LLM-B is a separate task) — it consults paper-names so
            # it survives unchanged here.
            print(f"[prepare] LLM-A placed {len(papers)} papers")
            poster_paper_name = f"Poster.{ev['id']}"
            primary_centre_z = next(
                (p.anchor_z for p in papers if p.name == poster_paper_name),
                anchor_z,
            )
            # Resolve names + story-plan inputs from the LLM placement.
            date_paper_name = (
                f"Date.{ev['id']}" if date_card is not None
                and any(p.name == f"Date.{ev['id']}" for p in papers) else None
            )
            cost_paper_name = (
                f"Cost.{ev['id']}" if cost_card is not None
                and any(p.name == f"Cost.{ev['id']}" for p in papers) else None
            )
            loc_paper_name = (
                f"Location.{ev['id']}" if loc_card is not None
                and any(p.name == f"Location.{ev['id']}" for p in papers) else None
            )
            digest_paper_name = (
                f"Digest.{ev['id']}" if digest_card is not None
                and any(p.name == f"Digest.{ev['id']}" for p in papers) else None
            )
            title_paper_name = (
                f"Title_banner.{ev['id']}" if title_card is not None
                and any(p.name == f"Title_banner.{ev['id']}" for p in papers) else None
            )

            # Skip the deterministic placement section entirely.
            from types import SimpleNamespace
            _llm_done = SimpleNamespace(papers=papers,
                                        primary_centre_z=primary_centre_z,
                                        date_paper_name=date_paper_name,
                                        cost_paper_name=cost_paper_name,
                                        loc_paper_name=loc_paper_name,
                                        digest_paper_name=digest_paper_name,
                                        title_paper_name=title_paper_name)

    # NOTE: when `placement=="llm"` we still let the deterministic
    # placer below run (it's cheap), then OVERRIDE its output with the
    # LLM-A layout right before the story plan. Keeping the deterministic
    # output as a baseline also gives us a guaranteed fallback if the
    # LLM result is empty or malformed.

    # Cluster vertical layout (top → bottom):
    #   1. title banner  (height = title_banner_band_h, optional)
    #   2. small gap     (0.02)
    #   3. PRIMARY image (height = primary_h_final, aspect preserved)
    #   4. small gap     (0.02)
    #   5. extras strip  (height = extras_band_h, optional)
    #
    # Centre the whole cluster on `anchor_z`. Each anchor_z below is
    # computed from the cluster top.
    total_cluster_h = primary_h_final
    if title_card is not None:
        total_cluster_h += title_banner_band_h + 0.02
    if extra_paths:
        total_cluster_h += extras_band_h + 0.02
    cluster_top_z = anchor_z + total_cluster_h / 2.0
    cursor_z = cluster_top_z

    # Title banner.
    if title_card is not None:
        banner_w = min(primary_w_final * 1.05, 0.66)
        banner_h = title_banner_band_h * 0.88
        banner_z = cursor_z - banner_h / 2.0
        papers_before_primary: list[PaperPlan] = [PaperPlan(
            image=str(title_card),
            anchor_angle_deg=anchor_angle + jitter(-0.6, 0.6),
            anchor_z=banner_z,
            width=banner_w, height=banner_h,
            tilt_deg=jitter(-1.5, 1.5),
            peel_corners=(False, False, False, False),
            peel_intensity=0.0,
            wrinkle=jitter(0.04, 0.10),
            paper_offset=0.006,
            name=f"Title.{ev['id']}",
        )]
        cursor_z = banner_z - banner_h / 2.0 - 0.02
    else:
        papers_before_primary = []

    # Primary image — natural aspect, no horizontal stretch.
    primary_centre_z = cursor_z - primary_h_final / 2.0
    poster_papers: list[PaperPlan] = [
        PaperPlan(
            image=str(poster_path),
            anchor_angle_deg=anchor_angle,
            anchor_z=primary_centre_z,
            width=primary_w_final,
            height=primary_h_final,
            tilt_deg=jitter(-1.6, -0.4),
            peel_corners=tuple(peel_mask),  # type: ignore[arg-type]
            peel_intensity=jitter(0.30, 0.55),
            wrinkle=jitter(0.06, 0.18),
            paper_offset=0.004,
            name=f"Poster.{ev['id']}",
        ),
    ]
    cursor_z = primary_centre_z - primary_h_final / 2.0 - 0.02
    papers: list[PaperPlan] = list(papers_before_primary) + list(poster_papers)
    next_paper_offset = 0.007  # poster + 0.003 for the first sticker

    # Extras strip below the primary. Each extra is `~0.50` of the
    # primary's longer side (round-8 N1 rule). We may have to cap the
    # extras count if their total arc would exceed primary's arc by too
    # much; otherwise the cluster wraps the cylinder unreasonably.
    if extra_paths:
        target_extra_h = extras_band_h
        # Drop extras one by one until the row fits inside primary_w * 1.25.
        keep: list[tuple[Path, float, float, float]] = []
        total_ex_arc = 0.0
        primary_arc_budget_deg = math.degrees((primary_w_final * 1.25) / 0.5)
        for ex_path in extra_paths:
            with Image.open(ex_path) as _im:
                ex_aspect = _im.size[0] / max(1, _im.size[1])
            ex_h = target_extra_h
            ex_w = ex_h * ex_aspect
            ex_arc = math.degrees(ex_w / 0.5)
            if total_ex_arc + ex_arc + (math.degrees(0.02 / 0.5) if keep else 0) > primary_arc_budget_deg:
                # Would overflow row width — drop this extra.
                break
            total_ex_arc += ex_arc + (math.degrees(0.02 / 0.5) if keep else 0)
            keep.append((ex_path, ex_w, ex_h, ex_aspect))

        n_ex = len(keep)
        if n_ex > 0:
            strip_z = primary_centre_z - primary_h_final / 2.0 - target_extra_h / 2.0 - 0.02
            # Centre the row of kept extras around `anchor_angle`.
            start_angle = anchor_angle - total_ex_arc / 2.0
            ang_cursor = start_angle
            for i, (ex_path, ex_w, ex_h, _aspect) in enumerate(keep):
                ex_arc = math.degrees(ex_w / 0.5)
                ang_cursor += ex_arc / 2.0
                papers.append(PaperPlan(
                    image=str(ex_path),
                    anchor_angle_deg=ang_cursor + jitter(-0.6, 0.6),
                    anchor_z=strip_z + jitter(-0.012, 0.012),
                    width=ex_w, height=ex_h,
                    tilt_deg=jitter(-2.5, 2.5),
                    peel_corners=tuple(coin(0.20) for _ in range(4)),  # type: ignore[arg-type]
                    peel_intensity=jitter(0.10, 0.35),
                    wrinkle=jitter(0.04, 0.12),
                    paper_offset=0.0045 + i * 0.0005,
                    name=f"Image.{ev['id']}.{i+1}",
                ))
                ang_cursor += ex_arc / 2.0 + math.degrees(0.02 / 0.5)

    # Half-arc of the poster on the cylinder (cyl_radius = 0.5). Stickers
    # may overlap the poster ONLY where the LLM region detector says the
    # zone is free; otherwise they go on bare cylinder within the
    # tightness budget.
    half_arc_deg = math.degrees(poster_w / 2.0 / 0.5)
    off_poster_margin_p = tightness_deg / max(1.0, half_arc_deg)   # in poster-norm x
    # Width cap for stickers so the 60%-off-poster constraint is
    # geometrically reachable on this poster + tightness combo.
    # We need: sw_p * 0.60 ≤ off_poster_margin_p  → sw_p ≤ margin / 0.60.
    max_sticker_w_p = off_poster_margin_p / 0.60
    # Convert back to 3D plane width.
    max_sticker_w_geom = max_sticker_w_p * poster_w
    # Round-9 P2: the geometric cap can produce stickers too small to
    # read on the main beat. Set a hard floor `MIN_STICKER_W` in 3D
    # units; if geometry can't fit a sticker of this size off-poster,
    # the placement engine MAY put it ON the primary image (rule P3 —
    # allowed for photo-cluster events).
    MIN_STICKER_W = 0.20
    PREFERRED_STICKER_W = 0.26
    max_sticker_w = max(max_sticker_w_geom, MIN_STICKER_W)
    # Photo-cluster events (primary is a photo not a poster) actively
    # WANT stickers over the image — readability is faster than scanning
    # the off-image side. Flag drives the placement-engine behaviour.
    photo_cluster_event = not primary_is_poster
    # Pre-compute LLM-occupied poster zones in normalised poster space.
    occupied_regions = list(regions.occupied_boxes())  # each: (x0,y0,x1,y1) in 0..1

    def _pick_sticker_anchor(
        sticker_w: float, sticker_h: float,
        prefer_side: str, prefer_band: str,
        avoid_other: list[tuple[float, float, float, float]],
    ) -> tuple[float, float]:
        """Choose (anchor_angle, anchor_z) for one sticker.

        Search space:
          - off-poster bare cylinder, left and right of the poster,
            within `tightness_deg` of the poster edge;
          - on-poster ONLY in zones not covered by any LLM region.

        Scoring:
          - hard-rejects any candidate that overlaps an LLM region or
            another already-placed sticker;
          - prefers the requested side/band; jitters by event seed.
        """
        sw_p = sticker_w / poster_w
        sh_p = sticker_h / poster_h
        margin = off_poster_margin_p  # already in poster-normalised x

        # Off-poster LEFT: sticker right edge ≤ 0 means 100% off-poster
        # (free side); we sample a few positions inside the allowed
        # arc tightness band so the sticker can sit fully or mostly
        # off-poster, never with > 40% on poster.
        x_left_off = [
            -margin,                          # farthest off-poster left
            -margin + sw_p * 0.05,            # 5% on-poster
            -margin + sw_p * 0.20,            # 20% on-poster
        ]
        x_right_off = [
            1.0 + margin - sw_p,              # farthest off-poster right
            1.0 + margin - sw_p * 1.05,       # 5% on-poster
            1.0 + margin - sw_p * 1.20,       # 20% on-poster
        ]
        # On-poster candidates are kept as a fallback only — they still
        # have to pass the 40%-on-poster cap and avoid LLM regions.
        x_on = [0.05, 0.25, 0.50, 0.75, 0.95 - sw_p]

        candidates: list[tuple[float, float, float]] = []
        for x_norm in x_left_off + x_on + x_right_off:
            for y_norm in [0.04, 0.20, 0.40, 0.60, 0.80 - sh_p]:
                # Hard rejects.
                # 1. Overlap with any LLM info region (on-poster only).
                if 0.0 <= x_norm <= 1.0 - sw_p:
                    bad = False
                    for (rx0, ry0, rx1, ry1) in occupied_regions:
                        if not (x_norm + sw_p <= rx0 or x_norm >= rx1
                                or y_norm + sh_p <= ry0 or y_norm >= ry1):
                            bad = True
                            break
                    if bad:
                        continue
                # 2. Collision with other already-placed stickers.
                bad = False
                for (ox, oy, ow, oh) in avoid_other:
                    if not (x_norm + sw_p <= ox or x_norm >= ox + ow
                            or y_norm + sh_p <= oy or y_norm >= oy + oh):
                        bad = True
                        break
                if bad:
                    continue
                # Soft scoring.
                side_score = x_norm if prefer_side == "L" else (1.0 - x_norm)
                if prefer_band == "T":
                    band_score = y_norm
                elif prefer_band == "B":
                    band_score = (1.0 - y_norm)
                else:
                    band_score = abs(y_norm - 0.5)
                # Hard rule: at least 60% of the sticker's area must sit
                # OFF the poster. Stickers are not allowed to blanket
                # the poster image — they may only "kiss" the edge with
                # at most 40% of their width on top.
                on_poster_overlap_x = max(0.0, min(1.0, x_norm + sw_p) - max(0.0, x_norm))
                on_poster_overlap_y = max(0.0, min(1.0, y_norm + sh_p) - max(0.0, y_norm))
                on_poster_frac = (on_poster_overlap_x * on_poster_overlap_y) / max(1e-6, sw_p * sh_p)
                if on_poster_frac > 0.40:
                    continue

                # Strong preference for fully off-poster (cleaner read).
                if x_norm + sw_p <= 0.0 or x_norm >= 1.0:
                    off_poster_bonus = -0.50
                else:
                    edge_dist = min(x_norm, 1.0 - (x_norm + sw_p))
                    off_poster_bonus = -0.20 + edge_dist * 0.60

                jit = rng.uniform(-0.02, 0.02)
                score = side_score * 0.4 + band_score * 0.25 + off_poster_bonus + jit
                candidates.append((score, x_norm, y_norm))
        if not candidates:
            # Degenerate fallback: place to the right of the poster on
            # bare cylinder, ignoring all rules.
            return (anchor_angle + half_arc_deg + tightness_deg, anchor_z)
        candidates.sort()
        _, x_norm, y_norm = candidates[0]
        rel_x = x_norm + sw_p / 2.0
        rel_y = y_norm + sh_p / 2.0
        angle = anchor_angle + (rel_x - 0.5) * 2.0 * half_arc_deg
        z = anchor_z + (0.5 - rel_y) * poster_h
        return (angle, z)

    # Already-placed sticker bounding boxes in normalised poster-space,
    # so subsequent stickers avoid stacking on each other.
    placed_boxes: list[tuple[float, float, float, float]] = []

    def _record_box(angle: float, z: float, w: float, h: float) -> None:
        rel_x_centre = (angle - anchor_angle) / (2.0 * half_arc_deg) + 0.5
        rel_y_centre = 0.5 - (z - anchor_z) / poster_h
        x0 = rel_x_centre - (w / poster_w) / 2.0
        y0 = rel_y_centre - (h / poster_h) / 2.0
        placed_boxes.append((x0, y0, w / poster_w, h / poster_h))

    # Build the list of stickers to place + their target dimensions
    # (width capped so the cluster geometrically fits the tightness
    # budget). Order matters — first sticker in the list sits closest
    # to the poster edge along the chosen side, subsequent stickers
    # stack outward.
    sticker_specs: list[tuple[str, str, float, float, dict]] = []
    # Title sticker is NOT added here — it is placed as a banner above
    # the primary image (see above). The side-sticker stack only
    # contains date / cost / location / digest.
    if date_card is not None:
        # Round-9 P2: stickers must be readable — floor at MIN_STICKER_W
        # regardless of tightness. Use preferred width as ideal, clamp by
        # geometric cap only when off-poster placement is mandatory.
        ideal_w = PREFERRED_STICKER_W + jitter(-0.02, 0.04)
        if photo_cluster_event:
            d_w = ideal_w  # on-image placement frees us from off-poster cap
        else:
            d_w = max(MIN_STICKER_W, min(max_sticker_w_geom, ideal_w))
        sticker_specs.append((
            f"Date.{ev['id']}", str(date_card), d_w, d_w * 0.78,
            dict(tilt_deg=jitter(-6.0, 6.0), peel=jitter(0.25, 0.55),
                 wrinkle=jitter(0.04, 0.10), peel_p=0.30),
        ))
    if cost_card is not None:
        ideal_w = 0.22 + jitter(-0.02, 0.04)
        if photo_cluster_event:
            c_w = ideal_w
        else:
            c_w = max(MIN_STICKER_W * 0.85, min(max_sticker_w_geom, ideal_w))
        # Cost peel deliberately gentler than other stickers — the
        # yellow "БЕСПЛАТНО" card with a strongly folded corner read as
        # crumpled (round-7 feedback). Keep it mostly-flat.
        sticker_specs.append((
            f"Cost.{ev['id']}", str(cost_card), c_w, c_w * 0.50,
            dict(tilt_deg=jitter(-6.0, 6.0), peel=jitter(0.05, 0.25),
                 wrinkle=jitter(0.0, 0.04), peel_p=0.12),
        ))
    if loc_card is not None:
        with Image.open(loc_card) as _lc:
            loc_aspect = _lc.size[0] / max(1, _lc.size[1])
        ideal_w = PREFERRED_STICKER_W + jitter(0.0, 0.04)
        if photo_cluster_event:
            l_w = ideal_w
        else:
            l_w = max(MIN_STICKER_W, min(max_sticker_w_geom, ideal_w))
        l_h = l_w / max(0.1, loc_aspect)
        sticker_specs.append((
            f"Loc.{ev['id']}", str(loc_card), l_w, l_h,
            dict(tilt_deg=jitter(-4.0, 4.0), peel=jitter(0.0, 0.30),
                 wrinkle=jitter(0.04, 0.08), peel_p=0.20),
        ))
    if digest_card is not None:
        ideal_w = 0.30 + jitter(-0.02, 0.06)
        if photo_cluster_event:
            g_w = ideal_w
        else:
            g_w = max(MIN_STICKER_W, min(max_sticker_w_geom, ideal_w))
        sticker_specs.append((
            f"Digest.{ev['id']}", str(digest_card), g_w, g_w * 0.55,
            dict(tilt_deg=jitter(-3.5, 3.5), peel=jitter(0.0, 0.35),
                 wrinkle=jitter(0.04, 0.08), peel_p=0.20),
        ))

    # Sticker-paper offsets are MUCH larger than the poster's peel
    # reach. Poster peel can lift a corner by up to ~0.019 (peel_intensity
    # × 0.035); sticker paper_offset starts at 0.030 so it never z-fights
    # with the poster mesh even on heavily peeled cases.
    STICKER_BASE_OFFSET = 0.030

    # Decide the cluster's primary direction.
    is_horizontal_stack = cluster_side in ("T", "B")
    is_left = cluster_side in ("L", "BL", "TL")
    is_bottom = cluster_side in ("B", "BL", "BR")

    # Anchor of the cluster, just past the poster edge along the chosen side.
    if cluster_side in ("L", "R"):
        # Vertical stack right beside the poster edge. CRITICAL: each
        # sticker's INNER edge must sit at `poster_edge + small_gap`,
        # not its centre — otherwise wide stickers overlap the poster.
        # Round-9 P3: for photo-cluster events stickers may PARTIALLY
        # overlap the primary image (in low-density zones) so they can
        # remain readable; for poster events they stay strictly off-poster.
        side_sign = -1 if is_left else 1
        gap_deg = max(2.0, tightness_deg * 0.2)
        inner_edge_offset_p = 0.0 if photo_cluster_event else half_arc_deg + gap_deg
        # Z-stack centred on the primary image with some upward offset.
        cluster_z_base = primary_centre_z + (primary_h_final / 2.0) * 0.55
        z_cursor = cluster_z_base
        for i, (name, img_path, sw, sh, extras) in enumerate(sticker_specs):
            s_arc = math.degrees(sw / 0.5)
            if photo_cluster_event:
                # On-image placement: sticker centre sits at half_arc * 0.65
                # → inside the primary's right (or left) area.
                angle = anchor_angle + side_sign * (half_arc_deg * 0.65) + jitter(-1.5, 1.5)
            else:
                # Off-poster: sticker INNER edge at poster_edge + small gap.
                angle = anchor_angle + side_sign * (half_arc_deg + gap_deg + s_arc / 2.0) + jitter(-0.6, 0.6)
            z = z_cursor - sh / 2.0
            papers.append(PaperPlan(
                image=img_path,
                anchor_angle_deg=angle, anchor_z=z,
                width=sw, height=sh,
                tilt_deg=extras["tilt_deg"],
                peel_corners=tuple(coin(extras["peel_p"]) for _ in range(4)),  # type: ignore[arg-type]
                peel_intensity=extras["peel"],
                wrinkle=extras["wrinkle"],
                paper_offset=STICKER_BASE_OFFSET + 0.003 * i,
                name=name,
            ))
            z_cursor = z - sh / 2.0 - 0.02  # 0.02 D gap between stickers
    elif cluster_side in ("BR", "BL", "TR", "TL"):
        # Corner cluster: a tight pile in one corner of the poster.
        side_sign = -1 if is_left else 1
        gap_deg = max(2.0, tightness_deg * 0.2)
        inner_edge_angle = anchor_angle + side_sign * (half_arc_deg + gap_deg)
        # Vertical centre of the corner cluster: ~55% from the relevant
        # poster edge.
        corner_z_base = primary_centre_z + (
            (-1 if is_bottom else 1) * (primary_h_final / 2.0) * 0.55
        )
        z_cursor = corner_z_base
        for i, (name, img_path, sw, sh, extras) in enumerate(sticker_specs):
            s_arc = math.degrees(sw / 0.5)
            angle = inner_edge_angle + side_sign * (s_arc / 2.0) + jitter(-1.0, 1.0)
            z = z_cursor + (-1 if is_bottom else 1) * (i * 0.01)
            papers.append(PaperPlan(
                image=img_path,
                anchor_angle_deg=angle, anchor_z=z,
                width=sw, height=sh,
                tilt_deg=extras["tilt_deg"],
                peel_corners=tuple(coin(extras["peel_p"]) for _ in range(4)),  # type: ignore[arg-type]
                peel_intensity=extras["peel"],
                wrinkle=extras["wrinkle"],
                paper_offset=STICKER_BASE_OFFSET + 0.003 * i,
                name=name,
            ))
            z_cursor += (-1 if is_bottom else 1) * (sh * 0.95)
    else:
        # T / B: horizontal stack above or below the poster.
        z_base = anchor_z + (-1 if is_bottom else 1) * (poster_h / 2.0 + 0.05)
        # Total horizontal span needed = sum of widths * 1.15
        total_arc = sum(math.degrees(s[2] / 0.5) for s in sticker_specs) * 1.15
        x_cursor = anchor_angle - total_arc / 2.0
        for i, (name, img_path, sw, sh, extras) in enumerate(sticker_specs):
            arc = math.degrees(sw / 0.5)
            angle = x_cursor + arc / 2.0 + jitter(-0.8, 0.8)
            z = z_base + jitter(-0.02, 0.02)
            papers.append(PaperPlan(
                image=img_path,
                anchor_angle_deg=angle, anchor_z=z,
                width=sw, height=sh,
                tilt_deg=extras["tilt_deg"],
                peel_corners=tuple(coin(extras["peel_p"]) for _ in range(4)),  # type: ignore[arg-type]
                peel_intensity=extras["peel"],
                wrinkle=extras["wrinkle"],
                paper_offset=STICKER_BASE_OFFSET + 0.003 * i,
                name=name,
            ))
            x_cursor += arc * 1.15

    # Resolve paper-name flags for the story plan.
    date_paper_name = f"Date.{ev['id']}" if date_card is not None else None
    cost_paper_name = f"Cost.{ev['id']}" if cost_card is not None else None
    loc_paper_name = f"Loc.{ev['id']}" if loc_card is not None else None
    digest_paper_name = f"Digest.{ev['id']}" if digest_card is not None else None
    title_paper_name = f"Title.{ev['id']}" if title_card is not None else None

    # ---- Round-8 strict-no-overlap validator + auto-tightness-bump ----
    # Compute every paper's effective angular-and-Z bounding box on the
    # cylinder surface and reject any layout where two papers' bboxes
    # intersect. If a collision is detected, log it loudly so we don't
    # silently ship overlapping papers like in round 7.
    def _bbox_for(p: PaperPlan) -> tuple[float, float, float, float]:
        # Approximate the rotated sticker plane's footprint on the
        # cylinder as an axis-aligned box in (angle_deg, z) space. Tilt
        # widens both dimensions by |sin(tilt)|·other_dim — small effect
        # for the ≤ 10° tilts we use, but accounted for.
        arc = math.degrees(p.width / 0.5)
        tilt_rad = math.radians(p.tilt_deg)
        bbox_arc = arc + abs(math.sin(tilt_rad)) * math.degrees(p.height / 0.5)
        bbox_z = p.height + abs(math.sin(tilt_rad)) * p.width
        return (
            p.anchor_angle_deg - bbox_arc / 2.0,
            p.anchor_angle_deg + bbox_arc / 2.0,
            p.anchor_z - bbox_z / 2.0,
            p.anchor_z + bbox_z / 2.0,
        )

    collisions: list[tuple[str, str]] = []
    for i in range(len(papers)):
        a0, a1, z0, z1 = _bbox_for(papers[i])
        for j in range(i + 1, len(papers)):
            b0, b1, c0, c1 = _bbox_for(papers[j])
            name_i, name_j = papers[i].name, papers[j].name
            is_image_i = "Image." in name_i or name_i.startswith("Poster.")
            is_image_j = "Image." in name_j or name_j.startswith("Poster.")
            # Allow extras + primary to share the cluster — they intentionally
            # sit tight on the cylinder.
            if is_image_i and is_image_j:
                continue
            # Round-9 P3: photo-cluster events DELIBERATELY place stickers
            # over the primary / extras (where the LLM said the image
            # carries no protected text). So an image↔sticker overlap
            # is EXPECTED in that mode.
            if photo_cluster_event and (is_image_i != is_image_j):
                continue
            overlap_arc = max(0.0, min(a1, b1) - max(a0, b0))
            overlap_z = max(0.0, min(z1, c1) - max(z0, c0))
            if overlap_arc > 0.4 and overlap_z > 0.005:
                collisions.append((name_i, name_j))

    if collisions:
        print(f"[prepare] WARNING: {len(collisions)} paper overlaps detected:")
        for a, b in collisions:
            print(f"  - {a} ↔ {b}")
    else:
        print("[prepare] paper overlap check: clean (round-8 T2)")

    # Override deterministic placement with LLM-A result, if available.
    if _llm_done is not None:
        papers = _llm_done.papers
        primary_centre_z = _llm_done.primary_centre_z
        date_paper_name = _llm_done.date_paper_name
        cost_paper_name = _llm_done.cost_paper_name
        loc_paper_name = _llm_done.loc_paper_name
        digest_paper_name = _llm_done.digest_paper_name
        title_paper_name = _llm_done.title_paper_name

    # Plan the camera "story" through this slot.
    poster_paper_name = f"Poster.{ev['id']}"
    story = plan_story(
        event_id=int(ev["id"]),
        attention_anchor_count=attention_anchor_count,
        has_title_on_poster=has_title_on_poster,
        has_date_on_poster=has_date_on_poster,
        has_location_on_poster=has_location_on_poster,
        has_essence_on_poster=has_essence_on_poster,
        has_cost_on_poster=has_price_on_poster,
        has_digest_sticker=digest_paper_name is not None,
        has_date_sticker=date_paper_name is not None,
        has_location_sticker=loc_paper_name is not None,
        has_cost_sticker=cost_paper_name is not None,
        has_title_sticker=title_paper_name is not None,
        has_attention_sticker=False,
        is_free=is_free,
        poster_paper_name=poster_paper_name,
        date_paper_name=date_paper_name,
        location_paper_name=loc_paper_name,
        cost_paper_name=cost_paper_name,
        digest_paper_name=digest_paper_name,
        title_paper_name=title_paper_name,
        attention_paper_name=None,
    )
    beats_payload = [
        BeatPlan(
            role=b.role,
            target_anchor=b.target_anchor,
            label=b.label,
            dwell_s=b.dwell_s,
            lens_mm=b.lens_mm,
            radius_factor=b.radius_factor,
            color=b.color,
            is_focus_push=b.is_focus_push,
        )
        for b in story.beats
    ]
    print(
        "[prepare] story plan: "
        + " → ".join(f"{b.role}({b.dwell_s:.1f}s)" for b in story.beats)
    )
    print(f"[prepare] coverage tests: {story.coverage_tests}")

    # Camera: the "main lookup" beat puts the poster nearly full-screen
    # with the side stickers peeking in at the edges. We size the radius
    # to the poster's actual width AND height so the longer dimension
    # fits the frame with ~12% headroom — portrait posters end up closer,
    # landscape posters get pushed back.
    sensor_w = 36.0
    aspect_frame = 1572.0 / 1080.0
    # Vertical and horizontal half-FOV at the chosen lens.
    half_w_at_d = lambda d, lens: d * (sensor_w / 2.0) / lens
    half_h_at_d = lambda d, lens: d * (sensor_w * aspect_frame / 2.0) / lens
    main_lens = 70.0
    # Required distance to fit poster width (with margin) and height (with margin).
    d_for_w = (poster_w * 1.18 / 2.0) * main_lens / (sensor_w / 2.0)
    d_for_h = (poster_h * 1.18 / 2.0) * main_lens / (sensor_w * aspect_frame / 2.0)
    main_distance_to_surface = max(d_for_w, d_for_h)
    main_radius = 0.50 + main_distance_to_surface  # body radius + distance to surface

    manifest = SlotManifest(
        event_id=int(ev["id"]),
        title=ev["title"] or "",
        date_iso=ev["date"] or "",
        time_text=time_text,
        location_name=ev["location_name"] or "",
        address=ev["location_address"] or "",
        city=ev["city"] or "",
        is_free=is_free,
        price_text=_format_price(ev["ticket_price_min"], ev["ticket_price_max"]),
        search_digest=ev["search_digest"] or "",
        is_promo=False,
        papers=papers,
        camera_focus_angle_deg=anchor_angle,
        camera_focus_z=anchor_z,
        camera_radius=main_radius,
        camera_lens_mm=main_lens,
        camera_target_offset_z=0.0,
        beats=beats_payload,
        coverage_tests=story.coverage_tests,
        poster_text_density=poster_analysis.text_density,
        poster_regions=regions.to_dict(),
        tightness_budget_deg=tightness_deg,
        attention_anchor_count=attention_anchor_count,
    )
    manifest_path = slot_dir / "manifest.json"
    with manifest_path.open("w", encoding="utf-8") as f:
        json.dump(asdict(manifest), f, ensure_ascii=False, indent=2)
    print(f"[prepare] wrote manifest -> {manifest_path}")
    return manifest


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--event-id", type=int, default=None)
    ap.add_argument(
        "--no-date-sticker", action="store_true",
        help="Skip the date+time sticker. Use when the poster itself shows "
             "the date legibly (point 6 of the v1 visual feedback).",
    )
    ap.add_argument(
        "--placement", choices=("algo", "llm"), default="algo",
        help="Layout strategy: `algo` is the deterministic placer; "
             "`llm` asks Gemini-lite to design the per-event layout. "
             "Output goes to `slot_<id>_llm/` for A/B comparison.",
    )
    args = ap.parse_args()
    prepare_slot(args.event_id,
                 skip_date_sticker=args.no_date_sticker,
                 placement=args.placement)


if __name__ == "__main__":
    main()
