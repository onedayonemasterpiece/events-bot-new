from __future__ import annotations
import asyncio, base64, hashlib, html, io, json, os, random, re, subprocess, sys, time, urllib.parse
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from statistics import mean, median, pstdev
from urllib.request import Request, urlopen, urlretrieve

RUN_STARTED = time.monotonic()
RUN_ID = os.getenv("REGION_TALK_RUN_ID") or os.getenv("RT_IMAGE_DIAG_RUN_ID") or "region-talk-image-diagnostic"
OUT = Path(os.getenv("REGION_TALK_IMAGE_DIAG_OUTPUT_DIR") or f"/kaggle/working/{RUN_ID}")
MEDIA = OUT / "media"
THUMBS = OUT / "contact_sheet_assets"
IMAGE_TERMINAL_UNSUPPORTED_STATUS = "not_reviewable_unsupported_media"
IMAGE_TERMINAL_ELIGIBILITY_STATUS = "rejected_publication_eligibility"
IMAGE_TERMINAL_SKIP_STATUSES = {
    "not_reviewable_no_media",
    IMAGE_TERMINAL_UNSUPPORTED_STATUS,
    IMAGE_TERMINAL_ELIGIBILITY_STATUS,
    "rejected_text_gate",
    "broken_media",
}
PUBLICATION_ELIGIBILITY_ACCEPT = "accept"
PUBLICATION_ELIGIBILITY_SOFT_DECISIONS = {
    "needs_source_review",
    "needs_text_review",
    "needs_visual_review",
    "review",
    "defer",
    "deferred",
}
PUBLICATION_ELIGIBILITY_GATE_VERSION = "region_talk_publication_eligibility_v5"
IMAGE_DECISION_CONTRACT_VERSION = "region_talk_image_editorial_gallery_guard_v3"
IMAGE_ACQUISITION_VERSION = "region_talk_plural_media_v3"
IMAGE_AUTH_RETRY_RESET_VERSION = "vk_service_read_token_v1"
IMAGE_LEGACY_SCORER_VERSION = "region_talk_cv_clip_laion_nima_legacy_v1"
IMAGE_QUALITY_NEEDS_REVIEW = "needs_visual_review"
IMAGE_QUALITY_LEGACY_ACCEPT = "legacy_auto_accept"
IMAGE_QUALITY_VLM_ACCEPT = "vlm_visual_accept"
IMAGE_QUALITY_OPERATOR_ACCEPT = "operator_visual_accept"
IMAGE_QUALITY_SCORING_RETRY = "scoring_retry"
IMAGE_VLM_PROMPT_VERSION = "region_talk_visual_adjudicator_v2"
IMAGE_VLM_DECISION_VERSION = "region_talk_visual_decision_v2"
LEGACY_PUBLICATION_ELIGIBILITY_GATE_VERSIONS = {
    "region_talk_publication_eligibility_v2",
    "region_talk_publication_eligibility_v3",
    "region_talk_publication_eligibility_v4",
}
UNSUPPORTED_MEDIA_SUFFIXES = {".mp4", ".mov", ".m4v", ".webm", ".avi", ".mkv"}
SUPPORTED_IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".gif", ".tif", ".tiff"}
PROCESSED_IMAGE_KEYS: set[str] = set()
IMAGE_VLM_BACKLOG_KEYS: set[str] = set()
IMAGE_VLM_RUNTIME: dict = {}
IMAGE_VLM_STATS = {
    "backlog_seen": 0,
    "attempted": 0,
    "replayed": 0,
    "accepted": 0,
    "rejected": 0,
    "review": 0,
    "errors": 0,
    "budget_deferred": 0,
    "run_limit_deferred": 0,
}
HEARTBEAT_EVENTS = {
    "kernel_started", "image_queue_poll", "image_batch_started", "image_batch_done",
    "media_fetch_started", "media_fetch_done", "image_inference_current", "image_inference_result",
    "model_load_started", "model_load_done", "model_unavailable",
    "ydb_source_visual_rollup_written", "report_written", "image_queue_poll_finished_empty",
    "image_vlm_started", "image_vlm_done", "image_vlm_deferred",
}
IMAGE_DIAG_HEARTBEAT_FIELDS = (
    "run_id", "event_name", "created_at", "phase", "reason", "attempt",
    "total", "leased", "remaining_budget", "pending", "blocked",
    "publication_eligibility_pending_count", "publication_eligibility_blocked_count",
    "publication_eligibility_refresh_deferred_count", "publication_eligibility_soft_deferred_count",
    "batch_index", "rows", "actual_scored", "actual_images", "actual_posts", "actual_frames", "failures",
    "xlsx", "html", "summary", "source", "max_items_per_run", "batch_size",
    "poll_interval_seconds", "wait_initial_seconds", "wait_after_drain_seconds",
    # Long CPU stages must identify the exact component and row. An event name
    # such as model_load_started without the model/post/timing data is not an
    # actionable heartbeat.
    "model", "model_id", "model_origin", "model_reference", "device", "load_seconds", "elapsed_seconds", "error",
    "index", "post_url", "image_queue_id", "source_title", "media_fetch_status",
    "telegram", "vk", "actual_downloaded", "status", "final_visual_score",
    "cv_score", "clip_score", "laion_score", "nima_score", "download_seconds",
    "decode_seconds", "inference_seconds", "total_processing_seconds", "width", "height",
    "vlm_decision", "vlm_status", "vlm_model", "vlm_calls", "vlm_max_calls", "vlm_backlog",
)


def max_media_fetch_attempts() -> int:
    return max(1, int(os.getenv("REGION_TALK_IMAGE_MAX_MEDIA_FETCH_ATTEMPTS") or "3"))


def max_images_per_post() -> int:
    # Telegram albums normally contain at most 10 media items, while VK posts
    # may expose more photo attachments.  The safe guardrail is full bounded
    # acquisition, not a silent first-image sample; callers may lower this only
    # for an explicitly labelled canary/cost experiment.
    return max(1, min(20, int(os.getenv("REGION_TALK_IMAGE_MAX_IMAGES_PER_POST") or "20")))


def legacy_publication_media_threshold() -> float:
    return float(os.getenv("REGION_TALK_PUBLICATION_MIN_OVERALL_MEDIA_SCORE") or "0.66")


def image_vlm_enabled() -> bool:
    return str(os.getenv("REGION_TALK_IMAGE_VLM_ENABLED") or "1").strip().lower() in {"1", "true", "yes", "on"}


def image_vlm_max_calls_per_run() -> int:
    return max(0, min(10, int(os.getenv("REGION_TALK_IMAGE_VLM_MAX_CALLS_PER_RUN") or "2")))


def image_vlm_model() -> str:
    return str(os.getenv("REGION_TALK_IMAGE_VLM_MODEL") or os.getenv("REGION_TALK_LLM_MODEL") or "gemini-3.1-flash-lite").strip()


def _row_float(row: dict, *keys: str) -> float:
    for key in keys:
        try:
            value = row.get(key)
            if value not in (None, ""):
                return float(value)
        except (TypeError, ValueError):
            continue
    return 0.0


def is_external_publication_row(row: dict) -> bool:
    return str(row.get("content_origin_type") or "").strip() in {
        "editorial_publication", "academic_publication"
    }


def visual_content_track(row: dict) -> str:
    """Select a visual vocabulary from structured editorial metadata.

    The track is not an acceptance shortcut. It only prevents professional
    architecture/interior/editorial work from being compared exclusively with
    scenic travel-postcard prompts before the selective VLM review.
    """
    explicit = str(row.get("visual_content_track") or "").strip().lower()
    if explicit:
        return explicit
    structured = " ".join(str(row.get(key) or "") for key in (
        "publication_content_type", "source_topic_class", "vector_content_type",
        "content_type", "diversity_topics",
    )).lower()
    if any(token in structured for token in (
        "architect", "interior", "urban", "museum", "exhibition", "design",
        "архитект", "интерьер", "музе", "выстав", "дизайн", "урбан",
    )):
        return "architecture_interior_editorial"
    if is_external_publication_row(row):
        return "editorial_publication"
    return "scenic_travel"


def image_vlm_request_fingerprint(row: dict, *, model: str | None = None) -> str:
    payload = {
        "stage": "region_talk_complete_album_visual_adjudication",
        "post_url": str(row.get("post_url") or "").strip().lower().rstrip("/"),
        "media_manifest_hash": str(row.get("input_media_manifest_hash") or ""),
        "expected_image_count": int(row.get("expected_image_count") or 0),
        "fetched_image_count": int(row.get("fetched_image_count") or 0),
        "image_decision_contract_version": str(row.get("image_decision_contract_version") or ""),
        "prompt_version": IMAGE_VLM_PROMPT_VERSION,
        "model": model or image_vlm_model(),
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def image_vlm_current_verdict(row: dict) -> bool:
    decision = str(row.get("image_vlm_decision") or "").strip().lower()
    return bool(
        str(row.get("image_vlm_status") or "").strip().lower() == "completed"
        and decision in {"accept", "reject", "review", "needs_review"}
        and str(row.get("image_vlm_prompt_version") or "") == IMAGE_VLM_PROMPT_VERSION
        and str(row.get("image_vlm_model") or "") == image_vlm_model()
        and str(row.get("image_vlm_media_manifest_hash") or "") == str(row.get("input_media_manifest_hash") or "")
        and str(row.get("image_vlm_request_fingerprint") or "") == image_vlm_request_fingerprint(row)
    )


def image_row_needs_vlm_review(row: dict) -> bool:
    """Return true only for complete, strict-funnel rows with a useful visual signal."""

    if not image_vlm_enabled():
        return False
    if str(row.get("image_quality_decision") or "") != IMAGE_QUALITY_NEEDS_REVIEW:
        return False
    if str(row.get("image_quality_reason") or "") != "uncalibrated_legacy_low_score_requires_visual_review":
        return False
    if publication_eligibility_gate_reason(row):
        return False
    if str(row.get("vector_gate_status") or "") != "vector_accept_candidate":
        return False
    if str(row.get("text_vector_fusion_status") or "") != "fused_e5_bge_m3":
        return False
    if str(row.get("image_model_input_type") or "") != "actual_image":
        return False
    expected = int(row.get("expected_image_count") or 0)
    fetched = int(row.get("fetched_image_count") or 0)
    if expected <= 0 or expected != fetched:
        return False
    if str(row.get("image_acquisition_status") or "") != "complete":
        return False
    if str(row.get("image_component_bundle_complete") or "").strip().lower() != "true":
        return False
    if not str(row.get("input_media_manifest_hash") or "").strip():
        return False
    if image_vlm_current_verdict(row):
        return False
    overall = _row_float(row, "overall_media_score", "final_visual_score")
    postcard = _row_float(row, "postcardness_score", "clip_postcardness_score", "cv_postcardness_score")
    best = _row_float(row, "shadow_best_frame_score")
    # This lane recovers scorer false negatives, not every weak image.  It is
    # deliberately wider than the legacy 0.66 boundary while still requiring
    # a near-threshold anchor, strong postcard semantics, or one strong frame.
    editorial_gallery = (
        is_external_publication_row(row)
        and fetched >= 2
        and best >= 0.50
    )
    return overall >= 0.58 or postcard >= 0.85 or best >= 0.66 or editorial_gallery


def image_vlm_priority(row: dict) -> tuple[float, float, float, int]:
    return (
        _row_float(row, "shadow_best_frame_score"),
        _row_float(row, "postcardness_score", "clip_postcardness_score", "cv_postcardness_score"),
        _row_float(row, "overall_media_score", "final_visual_score"),
        -int(row.get("image_queue_order") or 10**9),
    )


def _media_ref_list(value) -> list[str]:
    """Normalize compact YDB/list media references without treating text as one URL."""
    if isinstance(value, (list, tuple)):
        raw = list(value)
    else:
        text = str(value or "").strip()
        if not text:
            return []
        if text.startswith("["):
            try:
                parsed = json.loads(text)
                raw = list(parsed) if isinstance(parsed, list) else [text]
            except Exception:
                raw = re.split(r"[|\n]", text)
        else:
            raw = re.split(r"[|\n]", text)
    out: list[str] = []
    seen: set[str] = set()
    for item in raw:
        ref = str(item or "").strip().strip("'\"")
        if ref and ref not in seen:
            seen.add(ref)
            out.append(ref)
    return out


def _actual_media_paths(row: dict) -> list[str]:
    paths = _media_ref_list(row.get("actual_media_paths"))
    single = str(row.get("actual_media_path") or "").strip()
    if single and single not in paths:
        paths.insert(0, single)
    return paths


def _set_actual_media_paths(row: dict, paths: list[str]) -> None:
    unique = _media_ref_list(paths)[:max_images_per_post()]
    row["actual_media_paths"] = unique
    row["actual_media_path"] = unique[0] if unique else ""
    row["fetched_image_count"] = len(unique)


def _media_manifest_hash(items: list[dict]) -> str:
    payload = [
        {
            "media_id": str(item.get("media_id") or ""),
            "ordinal": int(item.get("ordinal") or 0),
            "content_sha256": str(item.get("content_sha256") or ""),
        }
        for item in items
    ]
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _manifest_item(path: str, *, media_id: str, ordinal: int) -> dict:
    p = Path(path)
    try:
        digest = hashlib.sha256(p.read_bytes()).hexdigest()
    except Exception:
        digest = ""
    return {
        "media_id": media_id,
        "ordinal": int(ordinal),
        "content_sha256": digest,
        "suffix": p.suffix.lower(),
    }


def _apply_media_manifest(row: dict, items: list[dict], *, expected: int, status: str) -> None:
    row["image_decision_contract_version"] = IMAGE_DECISION_CONTRACT_VERSION
    row["image_acquisition_version"] = IMAGE_ACQUISITION_VERSION
    row["expected_image_count"] = max(0, int(expected))
    row["fetched_image_count"] = len(items)
    row["distinct_image_count"] = len({str(item.get("content_sha256") or item.get("media_id") or "") for item in items})
    row["image_acquisition_status"] = status
    row["input_media_manifest_hash"] = _media_manifest_hash(items) if items else ""
    row["media_manifest_items"] = items


def _expected_image_count(row: dict, fallback: int = 1) -> int:
    for key in ("expected_image_count", "image_count", "media_count", "album_size"):
        try:
            value = int(float(row.get(key) or 0))
        except (TypeError, ValueError):
            value = 0
        if value > 0:
            return value
    return max(0, int(fallback))


def _row_direct_image_refs(row: dict) -> list[str]:
    refs: list[str] = []
    for key in (
        "image_urls", "media_photo_urls", "vk_media_photo_urls", "actual_media_urls",
        "image_url_or_local_path", "primary_media_path",
    ):
        for value in _media_ref_list(row.get(key)):
            ref = direct_image_url(value)
            if ref and ref not in refs:
                refs.append(ref)
    return refs[:max_images_per_post()]


class _EditorialGalleryParser(HTMLParser):
    """Extract only author-declared article lightbox/gallery targets.

    Navigation thumbnails, recommendations and site chrome are deliberately
    ignored. ``data-fancybox``/``data-lightbox`` anchors are a generic signal
    that the publisher considers an image part of the article gallery.
    """

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.refs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        values = {str(key).lower(): str(value or "") for key, value in attrs}
        gallery_marker = values.get("data-fancybox") or values.get("data-lightbox")
        href = values.get("href", "").strip()
        if gallery_marker and href:
            self.refs.append(href)


def extract_editorial_gallery_image_urls(page_html: str, *, base_url: str) -> list[str]:
    parser = _EditorialGalleryParser()
    parser.feed(str(page_html or ""))
    out: list[str] = []
    for raw in parser.refs:
        absolute = urllib.parse.urljoin(base_url, html.unescape(raw))
        ref = direct_image_url(absolute)
        if ref and ref not in out:
            out.append(ref)
    return out[:max_images_per_post()]


def discover_external_publication_image_refs(row: dict) -> list[str]:
    """Discover a bounded article gallery, retaining direct research refs as fallback."""
    direct = _row_direct_image_refs(row)
    post_url = str(row.get("post_url") or "").strip()
    if not is_external_publication_row(row) or not post_url.startswith(("http://", "https://")):
        return direct
    timeout = max(5, min(60, int(os.getenv("REGION_TALK_EXTERNAL_PAGE_FETCH_TIMEOUT_SECONDS") or "25")))
    try:
        request = Request(post_url, headers={
            "User-Agent": "Mozilla/5.0 (compatible; RegionTalkEditorialImageReview/1.0)",
            "Accept": "text/html,application/xhtml+xml",
        })
        with urlopen(request, timeout=timeout) as response:  # nosec B310 - public URL admitted by research contract
            content_type = str(response.headers.get("Content-Type") or "").lower()
            if "html" not in content_type:
                raise ValueError(f"article response is not HTML: {content_type[:80]}")
            payload = response.read(4 * 1024 * 1024 + 1)
            if len(payload) > 4 * 1024 * 1024:
                raise ValueError("article HTML exceeds 4 MiB discovery limit")
            charset = response.headers.get_content_charset() or "utf-8"
        gallery = extract_editorial_gallery_image_urls(payload.decode(charset, errors="replace"), base_url=post_url)
        row["web_gallery_discovery_status"] = "gallery_found" if gallery else "no_gallery_fallback_direct"
        row["web_gallery_discovered_count"] = len(gallery)
        row["web_gallery_discovery_version"] = IMAGE_ACQUISITION_VERSION
        merged = gallery + [ref for ref in direct if ref not in gallery]
        used = merged[:max_images_per_post()]
        row["web_gallery_used_count"] = len(used)
        return used
    except Exception as exc:
        row["web_gallery_discovery_status"] = "page_fetch_failed_fallback_direct"
        row["web_gallery_discovery_error"] = f"{type(exc).__name__}: {str(exc)[:220]}"
        row["web_gallery_discovery_version"] = IMAGE_ACQUISITION_VERSION
        row["web_gallery_discovered_count"] = 0
        row["web_gallery_used_count"] = len(direct)
        return direct


def _apply_acquired_paths(
    row: dict,
    paths: list[str],
    *,
    media_ids: list[str] | None = None,
    expected: int,
    status: str,
) -> None:
    _set_actual_media_paths(row, paths)
    ids = list(media_ids or [])
    items = [
        _manifest_item(path, media_id=ids[index] if index < len(ids) else f"frame:{index + 1}", ordinal=index + 1)
        for index, path in enumerate(_actual_media_paths(row))
    ]
    _apply_media_manifest(row, items, expected=expected, status=status)
    if paths:
        row["media_fetch_status"] = "downloaded" if status == "complete" else "downloaded_partial_album"
        row["media_fetch_error"] = "" if status == "complete" else "album acquisition is incomplete"


def _download_direct_image_refs(row: dict, refs: list[str], *, name_prefix: str) -> tuple[list[str], list[str]]:
    paths: list[str] = []
    errors: list[str] = []
    for index, ref in enumerate(refs[:max_images_per_post()], 1):
        suffix = ".jpg"
        match = re.search(r"\.(jpg|jpeg|png|webp)(?:\?|$)", ref, re.I)
        if match:
            suffix = "." + match.group(1).lower().replace("jpeg", "jpg")
        try:
            paths.append(_download_http_image(ref, MEDIA / f"{row['image_queue_id']}_{name_prefix}_{index}{suffix}"))
        except Exception as exc:
            errors.append(f"{index}:{type(exc).__name__}: {str(exc)[:160]}")
    return paths, errors


def fetch_web_direct(row: dict) -> None:
    """Acquire externally researched publication images from direct URLs.

    Telegram and VK have platform-specific fallbacks. Editorial/academic web
    rows discover an intentional article lightbox/gallery first and retain the
    research-provided direct image as fallback. Rights remain score-only unless
    separately cleared.
    """
    started = time.monotonic()
    refs = discover_external_publication_image_refs(row)
    if not refs:
        row["media_fetch_status"] = "needs_actual_image_fetch"
        row["media_fetch_error"] = "external publication has no direct image URL"
        row["media_download_seconds"] = round(time.monotonic() - started, 3)
        return
    paths, errors = _download_direct_image_refs(row, refs, name_prefix="web_public_url")
    gallery_discovered = int(row.get("web_gallery_discovered_count") or 0) > 0
    expected = len(refs) if gallery_discovered else _expected_image_count(row, len(refs))
    complete = bool(paths) and len(paths) >= expected and not errors
    if paths:
        _apply_acquired_paths(
            row,
            paths,
            media_ids=[f"web_direct:{index}" for index in range(1, len(paths) + 1)],
            expected=expected,
            status="complete" if complete else "partial",
        )
        row["media_fetch_status"] = "downloaded_public_url" if complete else "downloaded_partial_album"
    else:
        row["media_fetch_status"] = "needs_actual_image_fetch"
    if errors:
        row["media_fetch_error"] = "; ".join(errors)[:300]
    row["media_download_seconds"] = round(time.monotonic() - started, 3)


def image_work_key(row: dict) -> str:
    return str(row.get("image_queue_id") or row.get("post_url") or row.get("_ydb_pk") or "").strip()

def refresh_run_paths() -> None:
    global RUN_ID, OUT, MEDIA, THUMBS
    RUN_ID = os.getenv("REGION_TALK_RUN_ID") or os.getenv("RT_IMAGE_DIAG_RUN_ID") or RUN_ID or "region-talk-image-diagnostic"
    OUT = Path(os.getenv("REGION_TALK_IMAGE_DIAG_OUTPUT_DIR") or f"/kaggle/working/{RUN_ID}")
    MEDIA = OUT / "media"
    THUMBS = OUT / "contact_sheet_assets"
    for path in (OUT, MEDIA, THUMBS):
        path.mkdir(parents=True, exist_ok=True)

refresh_run_paths()

def log_event(name: str, **payload):
    payload.setdefault("event_name", name)
    payload.setdefault("created_at", datetime.now(timezone.utc).isoformat())
    print("[region-talk-image-diagnostic] " + json.dumps(payload, ensure_ascii=False)[:1200], flush=True)
    if name in HEARTBEAT_EVENTS:
        hb = globals().get("write_region_talk_image_diag_heartbeat")
        if callable(hb):
            try:
                hb({**payload, "run_id": RUN_ID})
            except Exception as exc:
                print(f"[region-talk-image-diagnostic] business_heartbeat_ydb_failed {type(exc).__name__}: {str(exc)[:160]}", flush=True)

def ensure(import_name: str, pip_name: str | None = None) -> bool:
    try:
        __import__(import_name); return True
    except Exception:
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", pip_name or import_name])
            __import__(import_name); return True
        except Exception as exc:
            log_event("package_unavailable", package=import_name, error=type(exc).__name__ + ": " + str(exc)[:300])
            return False

for imp, pip in [("PIL", "pillow"), ("openpyxl", "openpyxl"), ("requests", "requests"), ("cryptography", "cryptography"), ("telethon", "telethon")]:
    ensure(imp, pip)

import requests
from PIL import Image, ImageStat, ImageFilter
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill
from openpyxl.utils import get_column_letter
from cryptography.fernet import Fernet


CLIP_MODEL_ID = "openai/clip-vit-base-patch32"


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _is_clip_model_dir(path: Path) -> bool:
    """Return whether *path* contains a complete local HF CLIP contract."""
    return bool(
        path.is_dir()
        and (path / "config.json").is_file()
        and (path / "preprocessor_config.json").is_file()
        and any((path / name).is_file() for name in ("model.safetensors", "pytorch_model.bin"))
        and (path / "tokenizer.json").is_file()
    )


def clip_model_reference() -> tuple[str, str]:
    """Resolve the pinned Kaggle CLIP input without a Hub metadata request.

    Kaggle internet is used for public media acquisition, not as the durable
    distribution channel for a 600 MB scoring model. Production runs attach a
    versioned Kaggle Model and must therefore load the local directory with
    ``local_files_only=True``.
    """
    explicit = str(os.getenv("REGION_TALK_CLIP_MODEL_LOCAL_PATH") or "").strip()
    input_root = Path(os.getenv("REGION_TALK_KAGGLE_INPUT_ROOT") or "/kaggle/input")
    candidates: list[Path] = []
    if explicit:
        candidates.append(Path(explicit).expanduser())
    model_root = input_root / "models" / "yujkaggle" / "openaiclip-vit-base-patch32"
    candidates.extend(
        [
            model_root / "PyTorch" / "default" / "1",
            model_root / "pytorch" / "default" / "1",
            input_root / "openaiclip-vit-base-patch32" / "PyTorch" / "default" / "1",
            input_root / "openaiclip-vit-base-patch32" / "pytorch" / "default" / "1",
            input_root / "openaiclip-vit-base-patch32",
        ]
    )
    if input_root.exists():
        try:
            for config in input_root.rglob("config.json"):
                lowered = config.parent.as_posix().lower().replace("_", "-")
                if "clip-vit-base-patch32" in lowered or "openaiclip-vit-base-patch32" in lowered:
                    candidates.append(config.parent)
        except Exception:
            pass
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        if _is_clip_model_dir(candidate):
            origin = "kaggle_model_input" if str(candidate).startswith("/kaggle/input/") else "local_model_path"
            return str(candidate), origin
    require_local = _env_bool("REGION_TALK_CLIP_REQUIRE_LOCAL_MODEL", Path("/kaggle/input").exists())
    if require_local:
        raise FileNotFoundError(
            "complete local CLIP model input is required; expected the pinned "
            "Kaggle model or REGION_TALK_CLIP_MODEL_LOCAL_PATH"
        )
    return CLIP_MODEL_ID, "huggingface_hub"


def find_input_file(name: str) -> Path | None:
    for p in Path("/kaggle/input").glob(f"**/{name}"):
        return p
    return None

def load_json_file(name: str) -> dict:
    p = find_input_file(name)
    if not p:
        raise FileNotFoundError(name)
    return json.loads(p.read_text(encoding="utf-8"))

def load_runtime_config(*, preferred_parent: Path | None = None, config_paths: list[Path] | None = None) -> dict:
    cfg = {}
    paths = list(config_paths) if config_paths is not None else list(Path("/kaggle/input").glob("**/region_talk_run_config.json"))
    if preferred_parent is not None:
        preferred = preferred_parent / "region_talk_run_config.json"
        # Kaggle mounts several run-config datasets. The image worker's own
        # config must win over the generic orchestration config regardless of
        # filesystem glob order, especially for zero-valued idle waits.
        paths = [p for p in paths if p != preferred] + [p for p in paths if p == preferred]
    for p in paths:
        try:
            cfg.update(json.loads(p.read_text(encoding="utf-8")))
        except Exception:
            pass
    for k, v in (cfg.get("env") or {}).items():
        if v is not None:
            os.environ[str(k)] = str(v)
    return cfg

def load_kaggle_user_secrets() -> dict:
    names = ["REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON", "REGION_TALK_YDB_IAM_TOKEN", "YDB_ACCESS_TOKEN"]
    extra = (os.getenv("REGION_TALK_KAGGLE_SECRET_NAMES") or "").strip()
    if extra:
        names.extend([x.strip() for x in re.split(r"[,;\\s]+", extra) if x.strip()])
    names = list(dict.fromkeys(names))
    try:
        from kaggle_secrets import UserSecretsClient  # type: ignore
    except Exception as exc:
        return {"ok": False, "source": "kaggle_user_secrets", "error": type(exc).__name__, "loaded": []}
    loaded = []; errors = []; client = UserSecretsClient()
    for name in names:
        if os.getenv(name):
            continue
        try:
            value = client.get_secret(name)
            if value is not None and str(value).strip():
                os.environ.setdefault(name, str(value)); loaded.append(name)
        except Exception as exc:
            errors.append(f"{name}:{type(exc).__name__}")
    return {"ok": bool(loaded), "source": "kaggle_user_secrets", "loaded": loaded, "errors": errors[:5]}

def load_secrets() -> dict:
    status = {"encrypted": {"ok": False}, "kaggle_user_secrets": {"ok": False}}
    pairs = []
    for enc in Path("/kaggle/input").glob("**/region_talk_secrets.enc"):
        key = enc.parent / "region_talk_fernet.key"
        if key.exists(): pairs.append((enc, key))
    for enc, key in pairs:
        try:
            data = json.loads(Fernet(key.read_bytes().strip()).decrypt(enc.read_bytes()).decode("utf-8"))
            for k, v in data.items():
                if v is not None and str(v).strip(): os.environ.setdefault(str(k), str(v))
            status["encrypted"] = {"ok": True, "keys": sorted(data.keys())}
            break
        except Exception as exc:
            last = type(exc).__name__ + ": " + str(exc)[:200]
    else:
        status["encrypted"] = {"ok": False, "error": locals().get("last", "no secrets pair")}
    status["kaggle_user_secrets"] = load_kaggle_user_secrets()
    status["ok"] = bool(status["encrypted"].get("ok") or status["kaggle_user_secrets"].get("ok"))
    return status

input_path = find_input_file("image_diag_input.json")
runtime_config = load_runtime_config(preferred_parent=input_path.parent if input_path else None)
refresh_run_paths()
if input_path:
    input_payload = json.loads(input_path.read_text(encoding="utf-8"))
else:
    if os.getenv("REGION_TALK_IMAGE_DIAG_ALLOW_MISSING_INPUT") == "1":
        input_payload = {}
    else:
        raise FileNotFoundError("image_diag_input.json")
secret_status = load_secrets()
rows = input_payload.get("rows") or []
limit = int(os.getenv("REGION_TALK_IMAGE_DIAG_TOP_N") or input_payload.get("top_n") or 50)


def expected_publication_eligibility_gate_version() -> str:
    """Return the producer/consumer contract version required by this run."""
    return str(
        os.getenv("REGION_TALK_IMAGE_DIAG_EXPECTED_ELIGIBILITY_GATE_VERSION")
        or os.getenv("REGION_TALK_PUBLICATION_ELIGIBILITY_GATE_VERSION")
        or input_payload.get("expected_publication_eligibility_gate_version")
        or input_payload.get("publication_eligibility_gate_version")
        or runtime_config.get("expected_publication_eligibility_gate_version")
        or runtime_config.get("publication_eligibility_gate_version")
        or PUBLICATION_ELIGIBILITY_GATE_VERSION
    ).strip()


def _local_source_eligibility_reason(row: dict) -> str:
    """Recognize durable local-source classifications without reclassifying text."""
    markers = {
        "source_scope": str(row.get("source_scope") or "").strip().lower(),
        "source_geo_class": str(row.get("source_geo_class") or "").strip().lower(),
        "source_quick_class": str(row.get("source_quick_class") or "").strip().lower(),
        "source_topic_class": str(row.get("source_topic_class") or "").strip().lower(),
        "source_queue_status": str(row.get("source_queue_status") or "").strip().lower(),
        "image_product_gate_reason": str(row.get("image_product_gate_reason") or "").strip().lower(),
    }
    local_values = {
        "local_region",
        "kaliningrad_local",
        "local_region_source",
        "local_region_source_surface",
        "rejected_local_region_source",
        "local_kaliningrad_source_for_separate_monitoring",
    }
    for field, value in markers.items():
        if value in local_values or value.startswith("local_kaliningrad_source"):
            return f"local_source_marker:{field}={value}"
    return ""


def publication_eligibility_gate_reason(row: dict) -> str:
    """Fail closed unless CandidateReport signed this exact gate contract."""
    local_reason = _local_source_eligibility_reason(row)
    if local_reason:
        return local_reason
    decision = str(row.get("publication_eligibility_decision") or "").strip().lower()
    if decision != PUBLICATION_ELIGIBILITY_ACCEPT:
        # Recover the exact v4→v5 migration cycle produced when a row was
        # safely admitted by v4, leased for album rescore, and then rechecked
        # after its status had changed to ``image_analysis_in_progress``.  A
        # later CandidateReport may consequently sign only the circular
        # image-status rejection.  This exception never bypasses a semantic,
        # source, compliance or text rejection; it only allows the original
        # accepted low single-anchor row to reach non-terminal album review.
        if image_row_needs_contract_rescore(row):
            return ""
        return "publication_eligibility_decision_missing" if not decision else f"publication_eligibility_decision_not_accept:{decision}"
    actual_version = str(row.get("publication_eligibility_gate_version") or "").strip()
    expected_version = expected_publication_eligibility_gate_version()
    if not actual_version:
        return "publication_eligibility_gate_version_missing"
    if actual_version != expected_version:
        # One bounded migration exception: rows already accepted by v4 and
        # terminalized only by the old single-image quality scorer may be
        # reacquired under the v5 album contract. Source/text/compliance
        # semantics are unchanged; all other stale producer versions remain
        # fail-closed.
        rescore_helper = globals().get("image_row_needs_contract_rescore")
        if (
            actual_version in LEGACY_PUBLICATION_ELIGIBILITY_GATE_VERSIONS
            and expected_version == "region_talk_publication_eligibility_v5"
            and callable(rescore_helper)
            and rescore_helper(row)
        ):
            return ""
        return f"publication_eligibility_gate_version_mismatch:expected={expected_version};actual={actual_version}"
    return ""


def apply_publication_eligibility_audit(row: dict, *, reason: str | None = None) -> dict:
    """Persist the observed attestation and the consumer's gate result."""
    gate_reason = publication_eligibility_gate_reason(row) if reason is None else reason
    observed_decision = str(row.get("publication_eligibility_decision") or "").strip().lower()
    observed_version = str(row.get("publication_eligibility_gate_version") or "").strip()
    row["image_eligibility_decision"] = observed_decision or "missing"
    row["image_eligibility_gate_version"] = observed_version or "missing"
    row["image_eligibility_expected_gate_version"] = expected_publication_eligibility_gate_version()
    row["image_eligibility_reason"] = gate_reason or str(row.get("publication_eligibility_reason") or "accepted")
    refresh_helper = globals().get("_publication_eligibility_refreshable")
    refreshable = bool(gate_reason and callable(refresh_helper) and refresh_helper(gate_reason))
    soft_helper = globals().get("_publication_eligibility_soft_deferred")
    soft_deferred = bool(gate_reason and callable(soft_helper) and soft_helper(gate_reason))
    row["image_eligibility_status"] = (
        "deferred_refresh"
        if refreshable
        else "deferred_soft_gate"
        if soft_deferred
        else "blocked"
        if gate_reason
        else "accepted"
    )
    row["image_eligibility_checked_at"] = datetime.now(timezone.utc).isoformat()
    if gate_reason and not str(row.get("publication_eligibility_reason") or "").strip():
        row["publication_eligibility_reason"] = gate_reason
    return row


def _publication_eligibility_refreshable(reason: str) -> bool:
    return reason in {
        "publication_eligibility_decision_missing",
        "publication_eligibility_gate_version_missing",
    } or reason.startswith("publication_eligibility_gate_version_mismatch:")


def _publication_eligibility_soft_deferred(reason: str) -> bool:
    prefix = "publication_eligibility_decision_not_accept:"
    if not str(reason or "").startswith(prefix):
        return False
    return str(reason)[len(prefix):].strip().lower() in PUBLICATION_ELIGIBILITY_SOFT_DECISIONS


def _row_has_actual_diagnostic_evidence(row: dict) -> bool:
    """Return true when ImageDiagnostic has already produced durable evidence.

    Publication eligibility controls whether a row may advance, not whether a
    previously computed album score exists.  The two facts intentionally live
    in separate fields so a later source/text review cannot erase expensive
    image evidence.
    """
    try:
        scored_count = int(row.get("images_scored_actual_count") or 0)
    except (TypeError, ValueError):
        scored_count = 0
    return bool(
        str(row.get("image_model_input_type") or "").strip() == "actual_image"
        or str(row.get("final_visual_status") or "").strip() == "scored_actual_image"
        or scored_count > 0
    )


def _row_has_unsupported_media_evidence(row: dict, previous_status: str) -> bool:
    return bool(
        previous_status in {"not_reviewable_no_media", IMAGE_TERMINAL_UNSUPPORTED_STATUS}
        or str(row.get("image_model_input_type") or "").strip() == "unsupported_media"
        or str(row.get("media_acquisition_status") or "").strip() == "unsupported_media_or_decode_failed"
    )


def _restore_hidden_diagnostic_evidence(row: dict) -> None:
    """Repair fields erased by the former non-accept→terminal transition.

    The buggy transition zeroed only the summary counter and replaced two
    status labels; album/frame evidence remained in adjacent durable fields.
    Reconstruct only facts supported by those retained fields.
    """
    if str(row.get("image_model_input_type") or "").strip() != "actual_image":
        return
    counts = []
    for field in ("images_scored_actual_count", "actual_image_count", "frame_scores_available_count"):
        try:
            counts.append(max(0, int(row.get(field) or 0)))
        except (TypeError, ValueError):
            continue
    scored_count = max(counts or [0])
    if scored_count > 0:
        row["images_scored_actual_count"] = scored_count
        if str(row.get("final_visual_status") or "") == "blocked_publication_eligibility":
            row["final_visual_status"] = "scored_actual_image"
    if str(row.get("media_acquisition_status") or "") == "blocked_publication_eligibility":
        acquisition = str(row.get("image_acquisition_status") or "").strip().lower()
        if acquisition == "complete":
            row["media_acquisition_status"] = "actual_album_downloaded_and_scored"
        elif acquisition:
            row["media_acquisition_status"] = "partial_album_requires_retry_or_review"


def _legacy_actual_image_accept_attestation(row: dict) -> bool:
    current_decision = str(row.get("publication_eligibility_decision") or "").strip().lower()
    current_gate = str(row.get("publication_eligibility_gate_version") or "").strip()
    audit_decision = str(row.get("image_eligibility_decision") or "").strip().lower()
    audit_gate = str(row.get("image_eligibility_gate_version") or "").strip()
    return str(row.get("image_model_input_type") or "") == "actual_image" and (
        (
            current_decision == PUBLICATION_ELIGIBILITY_ACCEPT
            and current_gate in LEGACY_PUBLICATION_ELIGIBILITY_GATE_VERSIONS
        )
        or (
            audit_decision == PUBLICATION_ELIGIBILITY_ACCEPT
            and audit_gate in LEGACY_PUBLICATION_ELIGIBILITY_GATE_VERSIONS
        )
    )


def mark_publication_eligibility_blocked(row: dict, reason: str) -> dict:
    previous_status = str(row.get("image_queue_status") or "")
    legacy_actual_accept = _legacy_actual_image_accept_attestation(row)
    _restore_hidden_diagnostic_evidence(row)
    apply_publication_eligibility_audit(row, reason=reason)
    if _publication_eligibility_refreshable(reason):
        # Missing/stale producer attestation is a CandidateReport refresh, not
        # evidence that the actual image or post is bad.  In particular, a gate
        # version bump must not destroy the old ``actual_scored`` state before
        # CandidateReport can sign the new contract.
        if legacy_actual_accept and previous_status == IMAGE_TERMINAL_ELIGIBILITY_STATUS:
            row["image_queue_status"] = "actual_scored"
            row["previous_image_queue_status"] = previous_status
            row["status_changed_this_run"] = "true"
            row["last_status_changed_at"] = datetime.now(timezone.utc).isoformat()
        row["image_eligibility_status"] = "deferred_refresh"
        row["next_action"] = "recompute_publication_eligibility_before_image_analysis"
        row.setdefault("images_scored_actual_count", int(row.get("actual_image_count") or 0))
        return row
    if _publication_eligibility_soft_deferred(reason):
        # ``needs_*`` is absence of final publication evidence, not a negative
        # verdict.  A previous implementation collapsed every non-accept value
        # into the terminal eligibility reject and, on a one-row queue poll,
        # rewrote the whole ledger.  Restore/preserve ImageDiagnostic-owned
        # evidence and only defer the missing source/text/publication decision.
        row["previous_image_queue_status"] = previous_status
        if _row_has_actual_diagnostic_evidence(row):
            restored_status = "actual_scored"
            next_action = (
                "visual_review_nonterminal"
                if reason.endswith(":needs_visual_review")
                else "wait_for_source_or_text_gate_without_rescoring_image"
            )
        elif _row_has_unsupported_media_evidence(row, previous_status):
            restored_status = (
                previous_status
                if previous_status in {"not_reviewable_no_media", IMAGE_TERMINAL_UNSUPPORTED_STATUS}
                else IMAGE_TERMINAL_UNSUPPORTED_STATUS
            )
            next_action = "skip_unsupported_media"
        else:
            restored_status = "deferred_text_gate"
            next_action = (
                "resolve_source_verdict"
                if reason.endswith(":needs_source_review")
                else "complete_dual_text_gate"
            )
        row["image_queue_status"] = restored_status
        row["next_action"] = next_action
        row["status_changed_this_run"] = str(previous_status != restored_status).lower()
        if previous_status != restored_status or not row.get("last_status_changed_at"):
            row["last_status_changed_at"] = datetime.now(timezone.utc).isoformat()
        return row
    row["previous_image_queue_status"] = previous_status
    row["image_queue_status"] = IMAGE_TERMINAL_ELIGIBILITY_STATUS
    # A hard local/spam/compliance/text rejection closes publication spend, but
    # it must not mutate acquisition/model facts or erase an existing score.
    # Keeping the evidence makes the ledger auditable and avoids paying for a
    # second score if an authoritative source verdict is corrected later.
    row["next_action"] = "skip_publication_eligibility_rejected"
    row["status_changed_this_run"] = str(previous_status != IMAGE_TERMINAL_ELIGIBILITY_STATUS).lower()
    if previous_status != IMAGE_TERMINAL_ELIGIBILITY_STATUS or not row.get("last_status_changed_at"):
        row["last_status_changed_at"] = datetime.now(timezone.utc).isoformat()
    return row


def partition_publication_eligible_rows(batch: list[dict]) -> tuple[list[dict], list[dict]]:
    eligible: list[dict] = []
    blocked: list[dict] = []
    for row in batch:
        material_before = _image_eligibility_material_snapshot(row)
        reason = publication_eligibility_gate_reason(row)
        if reason:
            audited = mark_publication_eligibility_blocked(row, reason)
            audited["_image_diag_material_change"] = str(
                material_before != _image_eligibility_material_snapshot(audited)
            ).lower()
            blocked.append(audited)
        else:
            eligible.append(apply_publication_eligibility_audit(row, reason=""))
    return eligible, blocked


_IMAGE_ELIGIBILITY_VOLATILE_FIELDS = {
    "_ydb_pk",
    "_image_diag_material_change",
    "image_eligibility_checked_at",
    "last_image_diag_run_id",
    "last_image_diag_stage",
    "last_image_diag_at",
    "queue_item_updated_at",
    "status_changed_this_run",
    "last_status_changed_at",
    "previous_image_queue_status",
}


def _image_eligibility_material_snapshot(row: dict) -> str:
    """Return stable eligibility state without audit/run timestamps.

    ImageDiagnostic reads the complete historical image ledger on every poll.
    Rechecking an already blocked row must not turn that read into a write only
    because ``checked_at`` or run lineage changed.  Semantic/status/evidence
    transitions remain part of this snapshot and are therefore persisted.
    """
    material = {
        str(key): value
        for key, value in row.items()
        if str(key) not in _IMAGE_ELIGIBILITY_VOLATILE_FIELDS
    }
    return json.dumps(material, ensure_ascii=False, sort_keys=True, default=str)


def expose_publication_eligibility_counters(
    *,
    pending: int,
    blocked: int,
    refresh_deferred: int = 0,
    soft_deferred: int = 0,
) -> None:
    input_payload["publication_eligibility_pending_count"] = max(0, int(pending))
    input_payload["publication_eligibility_blocked_count"] = max(0, int(blocked))
    input_payload["publication_eligibility_refresh_deferred_count"] = max(0, int(refresh_deferred))
    input_payload["publication_eligibility_soft_deferred_count"] = max(0, int(soft_deferred))

def ydb_table_name(suffix: str = "state_kv") -> str:
    ns = re.sub(r"[^A-Za-z0-9_]+", "_", (os.getenv("REGION_TALK_YDB_NAMESPACE") or "region_talk_compact").strip() or "region_talk_compact").strip("_") or "region_talk_compact"
    return f"{ns}_{suffix}"

def ydb_cfg():
    endpoint=(os.getenv("REGION_TALK_YDB_ENDPOINT") or "").strip(); database=(os.getenv("REGION_TALK_YDB_DATABASE") or "").strip()
    if "?database=" in endpoint:
        endpoint_part, database_part = endpoint.split("?database=", 1)
        endpoint = endpoint_part
        if not database: database = database_part
    endpoint = endpoint.rstrip("/")
    if not endpoint or not database: raise RuntimeError("missing REGION_TALK_YDB_ENDPOINT/REGION_TALK_YDB_DATABASE")
    return endpoint, database, database.rstrip("/") + "/" + ydb_table_name()

def ydb_credentials(ydb):
    token=(os.getenv("REGION_TALK_YDB_IAM_TOKEN") or os.getenv("YC_IAM_TOKEN") or os.getenv("YDB_ACCESS_TOKEN") or "").strip()
    if token: return ydb.AccessTokenCredentials(token)
    key_json=(os.getenv("REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON") or "").strip()
    if key_json:
        import tempfile
        import ydb.iam  # type: ignore
        fd, path = tempfile.mkstemp(prefix="region-talk-image-ydb-sa-", suffix=".json")
        os.close(fd)
        Path(path).write_text(key_json, encoding="utf-8")
        return ydb.iam.ServiceAccountCredentials.from_file(path)
    if os.getenv("YDB_USER"): return ydb.StaticCredentials.from_user_password(os.getenv("YDB_USER"), os.getenv("YDB_PASSWORD", ""))
    return None

def ydb_connect():
    ensure("ydb", "ydb[yc]")
    import ydb
    endpoint,database,table_path=ydb_cfg(); creds=ydb_credentials(ydb)
    driver=ydb.Driver(endpoint=endpoint, database=database, credentials=creds) if creds is not None else ydb.Driver(endpoint=endpoint, database=database)
    driver.wait(timeout=int(os.getenv("REGION_TALK_YDB_CONNECT_TIMEOUT_SECONDS") or "20"), fail_fast=True)
    return ydb, driver, table_path


def ensure_region_talk_llm_runtime_import_path() -> None:
    candidates = [Path.cwd(), Path(__file__).resolve().parents[2], Path("/kaggle/working")]
    input_root = Path("/kaggle/input")
    if input_root.exists():
        candidates.extend(path.parent for path in input_root.rglob("region_talk_llm_runtime.py"))
        candidates.extend(path.parent.parent for path in input_root.rglob("google_ai/__init__.py"))
    for parent in candidates:
        if str(parent) not in sys.path and (
            (parent / "region_talk_llm_runtime.py").exists()
            or (parent / "google_ai" / "__init__.py").exists()
        ):
            sys.path.insert(0, str(parent))


def get_image_vlm_runtime() -> dict:
    if IMAGE_VLM_RUNTIME:
        return IMAGE_VLM_RUNTIME
    ensure_region_talk_llm_runtime_import_path()
    try:
        from region_talk_llm_runtime import DurableGeminiBudget, build_google_ai_client
    except Exception as exc:
        raise RuntimeError(f"region_talk_llm_runtime_missing: {type(exc).__name__}: {str(exc)[:240]}") from exc
    try:
        from google import genai as _genai  # noqa: F401
    except Exception:
        if str(os.getenv("REGION_TALK_AUTO_INSTALL") or "1").lower() in {"1", "true", "yes", "on"}:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "google-genai"])
        else:
            raise
    os.environ.setdefault("GOOGLE_AI_MAX_RETRIES", "1")
    os.environ.setdefault("GOOGLE_AI_PROVIDER_TIMEOUT_SEC", os.getenv("REGION_TALK_IMAGE_VLM_TIMEOUT_SECONDS") or "45")
    ydb, driver, table_path = ydb_connect()
    try:
        pool = ydb.SessionPool(driver)
        default_env = str(os.getenv("REGION_TALK_IMAGE_VLM_DEFAULT_ENV_VAR_NAME") or os.getenv("REGION_TALK_LLM_DEFAULT_ENV_VAR_NAME") or "GOOGLE_API_KEY3").strip()
        client = build_google_ai_client(default_env_var_name=default_env, consumer="region_talk_image_visual_adjudicator")
        timeout = max(1.0, float(os.getenv("REGION_TALK_IMAGE_VLM_TIMEOUT_SECONDS") or "45"))
        if hasattr(client, "provider_timeout_seconds"):
            client.provider_timeout_seconds = timeout
        budget_id = str(os.getenv("REGION_TALK_LLM_BUDGET_ID") or datetime.now(timezone.utc).strftime("region-talk-debug-%Y%m%d"))
        budget = DurableGeminiBudget(
            pool,
            ydb,
            table_path,
            budget_id=budget_id,
            budget_max=min(100, max(0, int(os.getenv("REGION_TALK_LLM_BUDGET_MAX") or "100"))),
            owner_prefix="image-vlm",
        )
    except Exception:
        try:
            driver.stop(timeout=5)
        except Exception:
            pass
        raise
    IMAGE_VLM_RUNTIME.update({
        "ydb": ydb,
        "driver": driver,
        "pool": pool,
        "table": table_path,
        "client": client,
        "budget": budget,
        "default_env_var_name": default_env,
        "timeout_seconds": timeout,
    })
    return IMAGE_VLM_RUNTIME


def close_image_vlm_runtime() -> None:
    driver = IMAGE_VLM_RUNTIME.pop("driver", None)
    if driver is not None:
        try:
            driver.stop(timeout=5)
        except Exception:
            pass
    IMAGE_VLM_RUNTIME.clear()


def _vlm_json(raw: str) -> dict:
    text = str(raw or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.I)
        text = re.sub(r"\s*```$", "", text)
    parsed = json.loads(text)
    if not isinstance(parsed, dict):
        raise ValueError("visual adjudicator returned a non-object JSON value")
    return parsed


def _vlm_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _vlm_image_parts(media_paths: list[str]) -> list:
    from google.genai import types
    from PIL import Image

    parts = []
    max_side = max(512, min(1600, int(os.getenv("REGION_TALK_IMAGE_VLM_MAX_SIDE") or "1024")))
    for ordinal, media_path in enumerate(media_paths, 1):
        with Image.open(media_path) as source:
            image = source.convert("RGB")
            image.thumbnail((max_side, max_side))
            buffer = io.BytesIO()
            image.save(buffer, format="JPEG", quality=82, optimize=True)
        parts.append(types.Part.from_text(text=f"IMAGE {ordinal} OF {len(media_paths)}"))
        parts.append(types.Part.from_bytes(data=buffer.getvalue(), mime_type="image/jpeg"))
    return parts


def _visual_adjudication_prompt(row: dict, image_count: int) -> str:
    track = visual_content_track(row)
    return f"""Ты — строгий визуальный редактор Region Talk. Перед тобой полный набор из {image_count} изображений одной публикации.
Оцени только визуальную пригодность набора для короткого редакционного тизера о Калининградской области. Текст и географическая релевантность проверяются отдельно и не должны влиять на визуальный вердикт.

ACCEPT: в полном наборе есть хотя бы один действительно сильный, технически читаемый и привлекательный кадр, который годится как самостоятельная иллюстрация редакционного тизера.
REJECT: все кадры явно слабые, нечитаемые, бытовые без выразительного места, скриншоты/афиши/новостная графика, доминирующая реклама или водяные знаки мешают публикации.
REVIEW: изображений недостаточно для уверенного решения или сигналы противоречивы.

Критически важно:
- не требуй от каждого хорошего кадра быть туристической открыткой, пейзажем или видом снаружи;
- профессиональная архитектурная, интерьерная, музейная, выставочная, научная и документальная фотография может быть сильной редакционной иллюстрацией;
- для архитектуры и интерьеров оцени композицию, свет, пространство, материалы, детализацию и способность заинтересовать читателя;
- несколько сильных кадров в целостной фотосерии усиливают вердикт, даже если первый/OG-кадр не лучший;
- безопасность, навязчивая реклама и техническая непригодность остаются обязательными ограничениями.

Верни ТОЛЬКО JSON:
{{
  "decision": "accept|reject|review",
  "strong_publishable_image": true,
  "best_image_ordinal": 1,
  "postcardness_score": 0.0,
  "editorial_suitability_score": 0.0,
  "aesthetic_score": 0.0,
  "technical_quality_score": 0.0,
  "publication_safety_score": 0.0,
  "commercial_overlay": false,
  "screenshot_or_graphic": false,
  "reason": "краткое проверяемое объяснение"
}}

Правила согласованности:
- accept требует strong_publishable_image=true и best_image_ordinal от 1 до {image_count};
- оцени весь альбом, но выбери лучший кадр;
- не выдумывай содержание за пределами видимого;
- visual_track={track}; structured_content_type={str(row.get('publication_content_type') or row.get('content_type') or '')[:120]}; source={str(row.get('source_title') or '')[:120]}; post={str(row.get('post_url') or '')[:200]}; prompt_version={IMAGE_VLM_PROMPT_VERSION}.
"""


def apply_image_vlm_result(row: dict, result: dict, *, fingerprint: str) -> dict:
    status = str(result.get("vlm_gate_status") or result.get("llm_gate_status") or "error").strip().lower()
    decision = str(result.get("vlm_decision") or result.get("decision") or "review").strip().lower()
    if decision == "needs_review":
        decision = "review"
    image_count = max(0, int(row.get("fetched_image_count") or 0))
    try:
        best_ordinal = int(result.get("best_image_ordinal") or 0)
    except (TypeError, ValueError):
        best_ordinal = 0
    accept_consistent = bool(
        status == "ok"
        and decision == "accept"
        and _vlm_bool(result.get("strong_publishable_image"))
        and 1 <= best_ordinal <= image_count
    )
    if status == "ok" and decision == "accept" and not accept_consistent:
        decision = "review"
        result["reason"] = ("accept_consistency_guard; " + str(result.get("reason") or ""))[:500]
    row.update({
        "image_vlm_status": "completed" if status == "ok" else status,
        "image_vlm_decision": decision,
        "image_vlm_model": image_vlm_model(),
        "image_vlm_prompt_version": IMAGE_VLM_PROMPT_VERSION,
        "image_vlm_decision_version": IMAGE_VLM_DECISION_VERSION,
        "image_vlm_request_fingerprint": fingerprint,
        "image_vlm_media_manifest_hash": str(row.get("input_media_manifest_hash") or ""),
        "image_vlm_best_image_ordinal": best_ordinal,
        "image_vlm_strong_publishable_image": str(_vlm_bool(result.get("strong_publishable_image"))).lower(),
        "image_vlm_postcardness_score": result.get("postcardness_score", ""),
        "image_vlm_editorial_suitability_score": result.get("editorial_suitability_score", ""),
        "image_vlm_aesthetic_score": result.get("aesthetic_score", ""),
        "image_vlm_technical_quality_score": result.get("technical_quality_score", ""),
        "image_vlm_publication_safety_score": result.get("publication_safety_score", ""),
        "visual_content_track": visual_content_track(row),
        "image_vlm_commercial_overlay": str(_vlm_bool(result.get("commercial_overlay"))).lower(),
        "image_vlm_screenshot_or_graphic": str(_vlm_bool(result.get("screenshot_or_graphic"))).lower(),
        "image_vlm_reason": str(result.get("reason") or result.get("llm_reason") or "")[:500],
        "image_vlm_updated_at": datetime.now(timezone.utc).isoformat(),
    })
    if accept_consistent:
        row["image_quality_decision"] = IMAGE_QUALITY_VLM_ACCEPT
        row["image_quality_reason"] = "complete_album_accepted_by_multimodal_visual_adjudicator"
        row["image_quality_terminality"] = "contract_version"
        row["image_model_type"] = "versioned_album_legacy_diagnostics_plus_vlm"
        row["next_action"] = "publication_verification"
        IMAGE_VLM_STATS["accepted"] += 1
    else:
        row["image_quality_decision"] = IMAGE_QUALITY_NEEDS_REVIEW
        row["image_quality_terminality"] = "nonterminal"
        row["next_action"] = "visual_review_nonterminal"
        if status != "ok":
            IMAGE_VLM_STATS["errors"] += 1
        elif decision == "reject":
            IMAGE_VLM_STATS["rejected"] += 1
        else:
            IMAGE_VLM_STATS["review"] += 1
    return row


def maybe_adjudicate_image_with_vlm(row: dict, media_paths: list[str]) -> dict:
    if not image_row_needs_vlm_review(row):
        return row
    max_calls = image_vlm_max_calls_per_run()
    if int(IMAGE_VLM_STATS["attempted"]) >= max_calls:
        IMAGE_VLM_STATS["run_limit_deferred"] += 1
        row["image_vlm_status"] = "deferred_run_limit"
        row["image_vlm_prompt_version"] = IMAGE_VLM_PROMPT_VERSION
        row["next_action"] = "visual_review_wait_vlm_capacity"
        log_event("image_vlm_deferred", phase="vlm", post_url=row.get("post_url"), image_queue_id=row.get("image_queue_id"), vlm_status="run_limit", vlm_calls=IMAGE_VLM_STATS["attempted"], vlm_max_calls=max_calls)
        return row
    fingerprint = image_vlm_request_fingerprint(row)
    try:
        runtime = get_image_vlm_runtime()
        budget = runtime["budget"]
        reservation = budget.reserve(fingerprint)
        reservation_status = str(reservation.get("status") or "")
        if reservation_status == "replay":
            IMAGE_VLM_STATS["replayed"] += 1
            result = dict(reservation.get("result") or {})
            apply_image_vlm_result(row, result, fingerprint=fingerprint)
            return row
        if reservation_status in {"busy", "exhausted"}:
            IMAGE_VLM_STATS["budget_deferred"] += 1
            row["image_vlm_status"] = "budget_" + reservation_status
            row["image_vlm_prompt_version"] = IMAGE_VLM_PROMPT_VERSION
            row["image_vlm_request_fingerprint"] = fingerprint
            row["next_action"] = "visual_review_wait_shared_gemini_budget"
            log_event("image_vlm_deferred", phase="vlm", post_url=row.get("post_url"), image_queue_id=row.get("image_queue_id"), vlm_status=reservation_status, vlm_calls=IMAGE_VLM_STATS["attempted"], vlm_max_calls=max_calls)
            return row
        IMAGE_VLM_STATS["attempted"] += 1
        log_event("image_vlm_started", phase="vlm", post_url=row.get("post_url"), image_queue_id=row.get("image_queue_id"), vlm_model=image_vlm_model(), vlm_calls=IMAGE_VLM_STATS["attempted"], vlm_max_calls=max_calls)
        from google.genai import types
        prompt_parts = [types.Part.from_text(text=_visual_adjudication_prompt(row, len(media_paths)))]
        prompt_parts.extend(_vlm_image_parts(media_paths))

        async def _call():
            return await runtime["client"].generate_content_async(
                model=image_vlm_model(),
                prompt=prompt_parts,
                generation_config={"temperature": 0.0, "response_mime_type": "application/json"},
                max_output_tokens=500,
            )

        raw, usage = asyncio.run(_call())
        data = _vlm_json(raw)
        result = {
            "llm_gate_status": "ok",
            "vlm_gate_status": "ok",
            "vlm_decision": str(data.get("decision") or "review").strip().lower(),
            "strong_publishable_image": data.get("strong_publishable_image"),
            "best_image_ordinal": data.get("best_image_ordinal"),
            "postcardness_score": data.get("postcardness_score"),
            "editorial_suitability_score": data.get("editorial_suitability_score"),
            "aesthetic_score": data.get("aesthetic_score"),
            "technical_quality_score": data.get("technical_quality_score"),
            "publication_safety_score": data.get("publication_safety_score"),
            "commercial_overlay": data.get("commercial_overlay"),
            "screenshot_or_graphic": data.get("screenshot_or_graphic"),
            "reason": str(data.get("reason") or "")[:500],
            "llm_model": image_vlm_model(),
            "llm_usage_input_tokens": getattr(usage, "input_tokens", ""),
            "llm_usage_output_tokens": getattr(usage, "output_tokens", ""),
            "llm_usage_total_tokens": getattr(usage, "total_tokens", ""),
        }
        budget.complete(fingerprint, result)
        apply_image_vlm_result(row, result, fingerprint=fingerprint)
        log_event("image_vlm_done", phase="vlm", post_url=row.get("post_url"), image_queue_id=row.get("image_queue_id"), vlm_model=image_vlm_model(), vlm_status=row.get("image_vlm_status"), vlm_decision=row.get("image_vlm_decision"), vlm_calls=IMAGE_VLM_STATS["attempted"], vlm_max_calls=max_calls)
    except Exception as exc:
        result = {
            "llm_gate_status": "error",
            "vlm_gate_status": "error",
            "vlm_decision": "review",
            "reason": f"{type(exc).__name__}: {str(exc)[:400]}",
            "llm_model": image_vlm_model(),
        }
        try:
            runtime = IMAGE_VLM_RUNTIME
            if runtime.get("budget") and fingerprint:
                runtime["budget"].complete(fingerprint, result)
        except Exception:
            pass
        apply_image_vlm_result(row, result, fingerprint=fingerprint)
        log_event("image_vlm_done", phase="vlm", post_url=row.get("post_url"), image_queue_id=row.get("image_queue_id"), vlm_model=image_vlm_model(), vlm_status="error", vlm_decision="review", error=result["reason"], vlm_calls=IMAGE_VLM_STATS["attempted"], vlm_max_calls=max_calls)
    return row

def write_region_talk_image_diag_heartbeat(payload: dict):
    if (os.getenv("REGION_TALK_IMAGE_DIAG_HEARTBEAT_YDB") or "1").lower() in {"0","false","no"}: return
    if (os.getenv("REGION_TALK_STATE_BACKEND") or "").lower() != "ydb" and not os.getenv("REGION_TALK_YDB_ENDPOINT"): return
    ydb, driver, table_path = ydb_connect(); pool=ydb.SessionPool(driver); now=datetime.now(timezone.utc).isoformat()
    clean={k:payload.get(k) for k in IMAGE_DIAG_HEARTBEAT_FIELDS if payload.get(k) not in (None,"",[],{})}
    clean.setdefault("run_id", RUN_ID); clean.setdefault("updated_at", now); clean.setdefault("notebook", "RegionTalkImageDiagnostic")
    def op(session):
        query=session.prepare(f"""DECLARE $pk AS Utf8; DECLARE $kind AS Utf8; DECLARE $payload_json AS Json; DECLARE $updated_at AS Utf8;
UPSERT INTO `{table_path}` (pk, kind, payload_json, updated_at) VALUES ($pk, $kind, $payload_json, $updated_at);""")
        tx=session.transaction(ydb.SerializableReadWrite())
        for pk in ["latest_business_heartbeat:image_diagnostic", "business_heartbeat:image_diagnostic:"+RUN_ID]:
            tx.execute(query,{"$pk":pk,"$kind":"business_heartbeat_image_diagnostic","$payload_json":json.dumps(clean,ensure_ascii=False),"$updated_at":now},commit_tx=False)
        tx.commit()
    try: pool.retry_operation_sync(op)
    finally: driver.stop(timeout=5)

def ydb_upsert_cursor(name: str, payload: dict):
    if (os.getenv("REGION_TALK_IMAGE_DIAG_WRITE_YDB") or "1").lower() in {"0","false","no"}: return
    if (os.getenv("REGION_TALK_STATE_BACKEND") or "").lower() != "ydb" and not os.getenv("REGION_TALK_YDB_ENDPOINT"): return
    safe = re.sub(r"[^A-Za-z0-9_:-]+", "_", str(name or "image_diagnostic")).strip("_") or "image_diagnostic"
    driver = None
    try:
        ydb, driver, table_path = ydb_connect(); pool=ydb.SessionPool(driver); now=datetime.now(timezone.utc).isoformat()
        item = {k: v for k, v in {
            "run_id": RUN_ID,
            "updated_at": now,
            "queue_name": safe,
            "phase": payload.get("phase"),
            "status": payload.get("status"),
            "reason": payload.get("reason"),
            "attempt": payload.get("attempt"),
            "cursor_position": payload.get("cursor_position"),
            "cursor_key": payload.get("cursor_key"),
            "total": payload.get("total"),
            "leased": payload.get("leased"),
            "remaining_budget": payload.get("remaining_budget"),
            "pending": payload.get("pending"),
            "blocked": payload.get("blocked"),
            "publication_eligibility_pending_count": payload.get("publication_eligibility_pending_count"),
            "publication_eligibility_blocked_count": payload.get("publication_eligibility_blocked_count"),
            "publication_eligibility_refresh_deferred_count": payload.get("publication_eligibility_refresh_deferred_count"),
            "progress_label": payload.get("progress_label"),
        }.items() if v not in (None, "", [], {})}
        def op(session):
            query=session.prepare(f"""DECLARE $pk AS Utf8; DECLARE $payload_json AS Json; DECLARE $updated_at AS Utf8;
UPSERT INTO `{table_path}` (pk, kind, payload_json, updated_at) VALUES ($pk, 'queue_cursor', $payload_json, $updated_at);""")
            tx=session.transaction(ydb.SerializableReadWrite())
            for pk in ["queue_cursor:"+safe, "queue_cursor:"+safe+":"+RUN_ID]:
                tx.execute(query,{"$pk":pk,"$payload_json":json.dumps(item,ensure_ascii=False),"$updated_at":now},commit_tx=False)
            tx.commit()
        pool.retry_operation_sync(op)
    except Exception as exc: log_event("ydb_cursor_write_failed", phase="poll", error=type(exc).__name__ + ": " + str(exc)[:240])
    finally:
        if driver is not None:
            driver.stop(timeout=5)

def ydb_select_kind(kind: str, limit_n: int):
    ydb, driver, table_path = ydb_connect(); pool=ydb.SessionPool(driver)
    def op(session):
        max_items=max(1, int(limit_n)); page_size=max(1, min(max_items, int(os.getenv("REGION_TALK_YDB_SELECT_PAGE_SIZE") or "200")))
        out=[]; prefix=kind+":"; prefix_upper=kind+";"; after=prefix
        while len(out) < max_items:
            query=session.prepare(f"""DECLARE $prefix AS Utf8; DECLARE $prefix_upper AS Utf8; DECLARE $after AS Utf8;
SELECT pk, payload_json FROM `{table_path}` WHERE pk >= $prefix AND pk < $prefix_upper AND pk > $after ORDER BY pk LIMIT {min(page_size, max_items-len(out))};""")
            rs=session.transaction(ydb.StaleReadOnly()).execute(query,{"$prefix":prefix,"$prefix_upper":prefix_upper,"$after":after}, commit_tx=True)
            rows=rs[0].rows if rs else []
            if not rows: break
            for row in rows:
                after=str(row.pk)
                payload=row.payload_json; d=json.loads(payload) if isinstance(payload,str) else dict(payload or {})
                if isinstance(d,dict): d.setdefault("_ydb_pk", str(row.pk)); out.append(d)
            if len(rows) < page_size: break
        return out
    try: return pool.retry_operation_sync(op)
    finally: driver.stop(timeout=5)

def ydb_select_image_queue(limit_n: int):
    scan_limit = int(os.getenv("REGION_TALK_IMAGE_DIAG_QUEUE_SCAN_LIMIT") or "5000")
    return ydb_select_kind("image_queue_item", max(1, scan_limit, limit_n*5))

def ydb_select_source_queue(limit_n: int = 10000):
    by_key = {}
    for row in ydb_select_kind("source_queue_item", max(1, limit_n)):
        key = str(row.get("canonical_source_key") or row.get("source_queue_id") or row.get("source_url") or row.get("_ydb_pk") or "")
        if key:
            by_key[key] = {**by_key.get(key, {}), **row}
    # Live CandidateReport writes source/public status aliases immediately while
    # a run is still in progress. ImageDiagnostic must consume those too, or a
    # source selected/skipped/updated only through the live-status path will be
    # invisible to visual rollups until a later final snapshot rewrite.
    for row in ydb_select_kind("source_status_item", max(1, limit_n)):
        key = str(row.get("canonical_source_key") or row.get("source_queue_id") or row.get("source_url") or row.get("_ydb_pk") or "")
        if key:
            by_key[key] = {**by_key.get(key, {}), **row}
    return list(by_key.values())[: max(1, limit_n)]

def text_region_confirmed(r):
    if str(r.get("is_ad_or_promo") or "").lower() in {"true","1","yes"}: return False
    if str(r.get("vector_gate_status") or "").startswith("vector_reject"): return False
    if str(r.get("kaliningrad_oblast_only_scope") or "").lower() not in {"true","1","yes"}: return False
    if str(r.get("kaliningrad_mention_role") or "main_subject") not in {"","main_subject","unclear"}: return False
    if str(r.get("external_geo_mentions") or r.get("mentioned_external_regions") or "").strip(): return False
    return True

def stale_image_lease(r):
    if str(r.get("image_queue_status") or "") != "image_analysis_in_progress":
        return False
    lease_at = str(r.get("lease_at") or "")
    try:
        dt = datetime.fromisoformat(lease_at.replace("Z", "+00:00"))
        if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
    except Exception:
        return True
    ttl = int(os.getenv("REGION_TALK_IMAGE_DIAG_STALE_LEASE_SECONDS") or "1800")
    return (datetime.now(timezone.utc) - dt).total_seconds() >= max(0, ttl)


def image_row_needs_contract_rescore(row: dict) -> bool:
    if str(row.get("image_decision_contract_version") or "") == IMAGE_DECISION_CONTRACT_VERSION:
        return False
    if str(row.get("image_model_input_type") or "") != "actual_image":
        return False
    if (
        is_external_publication_row(row)
        and str(row.get("publication_eligibility_decision") or "").strip().lower()
        == PUBLICATION_ELIGIBILITY_ACCEPT
    ):
        # v3 adds bounded article-gallery discovery and genre-aware review.
        # Re-open external-publication rows even when their old single OG frame
        # had already been scored under the v2 album contract.
        return True
    current_decision = str(row.get("publication_eligibility_decision") or "").strip().lower()
    current_gate = str(row.get("publication_eligibility_gate_version") or "").strip()
    audit_decision = str(row.get("image_eligibility_decision") or "").strip().lower()
    audit_gate = str(row.get("image_eligibility_gate_version") or "").strip()
    audit_reason = str(row.get("image_eligibility_reason") or "").strip()
    accepted_legacy = (
        current_decision == PUBLICATION_ELIGIBILITY_ACCEPT
        and current_gate in LEGACY_PUBLICATION_ELIGIBILITY_GATE_VERSIONS
    )
    accepted_legacy_audit = (
        audit_decision == PUBLICATION_ELIGIBILITY_ACCEPT
        and audit_gate in LEGACY_PUBLICATION_ELIGIBILITY_GATE_VERSIONS
        and audit_reason == (
            "publication_eligibility_gate_version_mismatch:expected="
            f"region_talk_publication_eligibility_v5;actual={audit_gate}"
        )
    )
    if not accepted_legacy and not accepted_legacy_audit:
        return False
    if current_decision not in {"", PUBLICATION_ELIGIBILITY_ACCEPT}:
        circular_reason = str(
            row.get("publication_eligibility_reason")
            or row.get("publication_eligibility_primary_reason")
            or ""
        ).strip()
        if circular_reason not in {
            "image_queue_not_actual_scored",
            "actual_image_required",
            "image_quality_contract_decision_missing",
        }:
            return False
    try:
        score = float(row.get("overall_media_score") or row.get("final_visual_score") or 0)
    except (TypeError, ValueError):
        score = 0.0
    return score < legacy_publication_media_threshold()


def image_row_needs_acquisition_repair(row: dict) -> bool:
    """Return true when a review row is unresolved only because input is partial."""
    if str(row.get("image_queue_status") or "") not in {"actual_scored", "needs_visual_review"}:
        return False
    if str(row.get("image_quality_terminality") or "") != "nonterminal":
        return False
    acquisition = str(row.get("image_acquisition_status") or "").strip().lower()
    reason = str(row.get("image_quality_reason") or "").strip().lower()
    return acquisition in {"partial", "versioned_album_rescore_required"} or reason == "incomplete_album_never_terminal_quality_reject"


def image_row_needs_auth_strategy_retry_reset(row: dict) -> bool:
    """Allow one bounded retry after replacing an IP-bound VK token.

    A few albums exhausted all three attempts before the remote worker began
    preferring the service read token.  The ordinary retry cap must remain
    monotonic, but those historical failures deserve exactly one reset under
    the new acquisition strategy.  The durable version marker prevents an
    endless reset loop if the service token later fails for another reason.
    """
    if str(row.get("media_fetch_retry_reset_version") or "") == IMAGE_AUTH_RETRY_RESET_VERSION:
        return False
    error = str(row.get("media_fetch_error") or "").lower()
    if "error_subcode': 1130" not in error and 'error_subcode": 1130' not in error and "another ip address" not in error:
        return False
    _token, token_name = _vk_read_token()
    return token_name in {"VK_SERVICE_TOKEN", "VK_SERVICE_KEY"}

def image_rows_for_stage_persist(batch, *, stage: str) -> list[dict]:
    if stage == "blocked_publication_eligibility":
        # The queue reader audits the whole historical ledger.  Persist only a
        # real eligibility/status/evidence transition, never a fresh audit
        # timestamp or run id for an otherwise unchanged historical row.
        return [
            row for row in batch
            if str(row.get("_image_diag_material_change") or "").lower() == "true"
        ]
    return list(batch)


def ydb_upsert_image_rows(batch, *, stage: str):
    batch = image_rows_for_stage_persist(batch, stage=stage)
    if not batch or (os.getenv("REGION_TALK_IMAGE_DIAG_WRITE_YDB") or "1").lower() in {"0","false","no"}: return
    ydb, driver, table_path = ydb_connect(); pool=ydb.SessionPool(driver); now=datetime.now(timezone.utc).isoformat()
    def op(session):
        query=session.prepare(f"""DECLARE $pk AS Utf8; DECLARE $payload_json AS Json; DECLARE $updated_at AS Utf8;
UPSERT INTO `{table_path}` (pk, kind, payload_json, updated_at) VALUES ($pk, 'image_queue_item', $payload_json, $updated_at);""")
        tx=session.transaction(ydb.SerializableReadWrite())
        for r in batch:
            key=str(r.get("image_queue_id") or r.get("post_url") or r.get("_ydb_pk") or "")
            if not key: continue
            apply_publication_eligibility_audit(r)
            previous_status=str(r.get("image_queue_status") or "")
            r["last_image_diag_run_id"]=RUN_ID; r["last_image_diag_stage"]=stage; r["last_image_diag_at"]=now
            r["queue_item_updated_at"] = now
            if previous_status:
                r.setdefault("last_status_changed_at", now)
            payload = dict(r)
            # Kaggle-local paths are transient and unusable after the notebook
            # exits. Persist compact manifest hashes/scores, not filesystem
            # names or duplicated path lists.
            for transient_key in ("actual_media_path", "actual_media_paths", "thumbnail_path", "unsupported_media_path", "vlm_revisit_requested", "_image_diag_material_change"):
                payload.pop(transient_key, None)
            tx.execute(query,{"$pk":"image_queue_item:"+key.replace("image_queue_item:",""),"$payload_json":json.dumps(payload,ensure_ascii=False),"$updated_at":now},commit_tx=False)
        tx.commit()
    try: pool.retry_operation_sync(op)
    finally: driver.stop(timeout=5)


def ydb_upsert_frame_score_rows(batch):
    if not batch or (os.getenv("REGION_TALK_IMAGE_DIAG_WRITE_YDB") or "1").lower() in {"0", "false", "no"}:
        return
    ydb, driver, table_path = ydb_connect(); pool = ydb.SessionPool(driver); now = datetime.now(timezone.utc).isoformat()
    def op(session):
        query = session.prepare(f"""DECLARE $pk AS Utf8; DECLARE $payload_json AS Json; DECLARE $updated_at AS Utf8;
UPSERT INTO `{table_path}` (pk, kind, payload_json, updated_at) VALUES ($pk, 'image_frame_score_item', $payload_json, $updated_at);""")
        tx = session.transaction(ydb.SerializableReadWrite())
        for row in batch:
            frame_id = str(row.get("image_frame_score_id") or "")
            if not frame_id:
                continue
            payload = dict(row)
            payload["frame_score_updated_at"] = now
            payload["last_image_diag_run_id"] = RUN_ID
            tx.execute(query, {
                "$pk": "image_frame_score_item:" + frame_id.replace("image_frame_score_item:", ""),
                "$payload_json": json.dumps(payload, ensure_ascii=False),
                "$updated_at": now,
            }, commit_tx=False)
        tx.commit()
    try: pool.retry_operation_sync(op)
    finally: driver.stop(timeout=5)

def ydb_upsert_source_rows(batch, *, stage: str):
    if not batch or (os.getenv("REGION_TALK_IMAGE_DIAG_WRITE_YDB") or "1").lower() in {"0","false","no"}: return
    ydb, driver, table_path = ydb_connect(); pool=ydb.SessionPool(driver); now=datetime.now(timezone.utc).isoformat()
    def op(session):
        query=session.prepare(f"""DECLARE $pk AS Utf8; DECLARE $kind AS Utf8; DECLARE $payload_json AS Json; DECLARE $updated_at AS Utf8;
UPSERT INTO `{table_path}` (pk, kind, payload_json, updated_at) VALUES ($pk, $kind, $payload_json, $updated_at);""")
        tx=session.transaction(ydb.SerializableReadWrite())
        for r in batch:
            key=str(r.get("canonical_source_key") or r.get("source_queue_id") or r.get("source_url") or r.get("_ydb_pk") or "")
            if not key: continue
            r["source_visual_rollup_run_id"]=RUN_ID; r["source_visual_rollup_updated_at"]=now; r["queue_item_updated_at"]=now
            clean_key=key.replace("source_queue_item:","").replace("source_status_item:","").replace("online_source_item:","")
            for kind in ["source_queue_item", "source_status_item"]:
                tx.execute(query,{"$pk":kind+":"+clean_key,"$kind":kind,"$payload_json":json.dumps(r,ensure_ascii=False),"$updated_at":now},commit_tx=False)
        tx.commit()
    try: pool.retry_operation_sync(op)
    finally: driver.stop(timeout=5)

def ydb_update_source_visual_rollups():
    try:
        image_rows=ydb_select_kind("image_queue_item", int(os.getenv("REGION_TALK_YDB_MAX_CANDIDATE_ROWS") or "5000"))
        source_rows=ydb_select_source_queue(int(os.getenv("REGION_TALK_YDB_MAX_SOURCE_ROWS") or "5000"))
    except Exception as exc:
        log_event("ydb_source_rollup_read_failed", error=type(exc).__name__ + ": " + str(exc)[:300]); return
    if not source_rows:
        return
    try: min_n=int(os.getenv("REGION_TALK_SOURCE_IMAGE_MIN_ACTUAL_SCORED") or "3")
    except Exception: min_n=3
    try: min_score=float(os.getenv("REGION_TALK_SOURCE_IMAGE_MIN_AVG_SCORE") or "0.55")
    except Exception: min_score=0.55
    updated=[]
    for srow in source_rows:
        urls={str(srow.get("source_url") or "").rstrip('/'), str(srow.get("canonical_url") or "").rstrip('/')} - {""}
        sid=str(srow.get("source_id") or "")
        matches=[]
        for ir in image_rows:
            if publication_eligibility_gate_reason(ir): continue
            if not text_region_confirmed(ir): continue
            if sid and str(ir.get("source_id") or "") == sid:
                matches.append(ir); continue
            if str(ir.get("source_url") or "").rstrip('/') in urls:
                matches.append(ir)
        scores=[]
        for ir in matches:
            if str(ir.get("image_queue_status") or "") != "actual_scored" and str(ir.get("image_model_input_type") or "") != "actual_image": continue
            try: scores.append(float(ir.get("overall_media_score") or ir.get("final_visual_score") or 0))
            except Exception: pass
        if not matches and not scores:
            continue
        avg=round(sum(scores)/len(scores),3) if scores else ""
        low=sum(1 for x in scores if x < min_score)
        previous_status=str(srow.get("source_queue_status") or "")
        previous_reason=str(srow.get("monitoring_exclusion_reason") or "")
        legacy_quality_reason="kaliningrad_posts_found_but_actual_images_systematically_low_score"
        # Raw uncalibrated image scores are observation only. They must never
        # terminalize a source: one scorer error would otherwise amplify into
        # loss of every future post from that author. Repair only the exact old
        # image-quality exclusion; unrelated local/spam/compliance decisions
        # remain untouched.
        if previous_status == "processed_found_ko_low_image_quality" and previous_reason in {"", legacy_quality_reason}:
            qstatus="processed_found_ko_candidate"
        else:
            qstatus=previous_status or "processed_found_ko_candidate"
        if len(scores) >= min_n and avg != "" and float(avg) < min_score:
            img_status="unadjudicated_raw_score_low_observation"
        elif len(scores) > 0:
            img_status="unadjudicated_raw_score_observation"
        else:
            img_status="needs_more_actual_image_evidence"
        reason="" if previous_reason == legacy_quality_reason else previous_reason
        changed=bool(previous_status and previous_status != qstatus)
        srow.update({
            "source_queue_status": qstatus, "previous_source_queue_status": previous_status if changed else srow.get("previous_source_queue_status", ""),
            "status_changed_this_run": str(changed).lower(), "last_status_changed_at": datetime.now(timezone.utc).isoformat() if changed or not srow.get("last_status_changed_at") else srow.get("last_status_changed_at"),
            "ko_posts_found": max(int(srow.get("ko_posts_found") or 0), len(matches)), "candidate_posts_found": max(int(srow.get("candidate_posts_found") or 0), len(matches)),
            "actual_images_scored_count": len(scores), "avg_actual_image_score": avg, "low_actual_image_count": low,
            "source_image_quality_status": img_status, "source_image_quality_min_actual_scored": min_n, "source_image_quality_min_avg_score": min_score,
            "source_image_quality_decision_use": "diagnostic_only_not_source_exclusion",
            "source_image_quality_contract_version": IMAGE_DECISION_CONTRACT_VERSION,
            "monitoring_exclusion_reason": reason,
        })
        updated.append(srow)
    if updated:
        ydb_upsert_source_rows(updated, stage="visual_rollup_from_image_diagnostic")
        log_event("ydb_source_visual_rollup_written", sources=len(updated), image_rows=len(image_rows))

def ydb_rows_for_diagnostic(limit_n: int):
    raw=ydb_select_image_queue(limit_n)
    eligible_raw, blocked = partition_publication_eligible_rows(raw)
    if blocked:
        ydb_upsert_image_rows(blocked, stage="blocked_publication_eligibility")
    pending=[]
    retry_exhausted=[]
    for r in eligible_raw:
        if image_work_key(r) in PROCESSED_IMAGE_KEYS: continue
        if not text_region_confirmed(r): continue
        status=str(r.get("image_queue_status") or "")
        input_type=str(r.get("image_model_input_type") or "")
        lease=str(r.get("lease_run_id") or "")
        needs_contract_rescore = image_row_needs_contract_rescore(r)
        needs_acquisition_repair = image_row_needs_acquisition_repair(r)
        needs_vlm_review = image_row_needs_vlm_review(r)
        if needs_vlm_review:
            IMAGE_VLM_BACKLOG_KEYS.add(image_work_key(r))
            IMAGE_VLM_STATS["backlog_seen"] = len(IMAGE_VLM_BACKLOG_KEYS)
            r["previous_image_queue_status"] = status
            r["image_queue_status"] = "needs_actual_image_fetch"
            r["media_acquisition_status"] = "needs_actual_image_fetch"
            r["actual_image_retry_reason"] = "complete_album_vlm_visual_adjudication"
            r["vlm_revisit_requested"] = "true"
            status = "needs_actual_image_fetch"
        if status == "actual_scored" and input_type == "actual_image" and not needs_contract_rescore and not needs_acquisition_repair and not needs_vlm_review:
            continue
        if status in IMAGE_TERMINAL_SKIP_STATUSES and not (
            status == IMAGE_TERMINAL_ELIGIBILITY_STATUS and needs_contract_rescore
        ):
            continue
        if needs_acquisition_repair:
            r["previous_image_queue_status"] = status
            r["image_queue_status"] = "needs_actual_image_fetch"
            r["media_acquisition_status"] = "needs_actual_image_fetch"
            r["actual_image_retry_reason"] = "partial_album_requires_acquisition_repair"
            status = "needs_actual_image_fetch"
            if image_row_needs_auth_strategy_retry_reset(r):
                r["media_fetch_attempt_count_before_strategy_reset"] = int(r.get("media_fetch_attempt_count") or 0)
                r["media_fetch_attempt_count"] = 0
                r["media_fetch_retry_exhausted"] = "false"
                r["media_fetch_retry_reset_version"] = IMAGE_AUTH_RETRY_RESET_VERSION
                r["media_fetch_retry_reset_reason"] = "ip_bound_vk_token_replaced_by_service_read_token"
        elif status == "needs_visual_review" and str(r.get("image_quality_terminality") or "") == "nonterminal" and not needs_vlm_review and not needs_contract_rescore:
            continue
        if status == "needs_actual_image_fetch" and int(r.get("media_fetch_attempt_count") or 0) >= max_media_fetch_attempts():
            r["image_queue_status"] = "needs_visual_review"
            r["media_acquisition_status"] = "media_fetch_exhausted_requires_nonterminal_review"
            r["image_quality_decision"] = IMAGE_QUALITY_NEEDS_REVIEW
            r["image_quality_reason"] = "media_acquisition_exhausted_without_complete_album"
            r["image_quality_terminality"] = "nonterminal"
            r["next_action"] = "visual_review_or_acquisition_repair"
            r["media_fetch_retry_exhausted"] = "true"
            retry_exhausted.append(r)
            continue
        # A failed media acquisition remains retryable for a future notebook
        # run, but must not be leased repeatedly by this same run. Otherwise a
        # persistent Telegram/VK auth error creates an unbounded hot retry loop.
        if str(r.get("last_image_diag_run_id") or "") == RUN_ID and status != "image_analysis_in_progress":
            continue
        if status == "actual_scored" and input_type != "actual_image":
            r["previous_image_queue_status"] = status
            r["previous_image_model_input_type"] = input_type
            r["image_queue_status"] = "needs_actual_image_fetch"
            r["media_acquisition_status"] = "needs_actual_image_fetch"
            r["actual_image_retry_reason"] = "metadata_only_actual_scored_is_not_final_visual_evidence"
        elif needs_contract_rescore:
            r["previous_image_queue_status"] = status
            r["image_queue_status"] = "needs_actual_image_fetch"
            r["media_acquisition_status"] = "versioned_album_rescore_required"
            r["actual_image_retry_reason"] = "legacy_low_score_requires_album_complete_rescore_or_review"
        if status == "image_analysis_in_progress" and lease and lease != RUN_ID and not stale_image_lease(r): continue
        if status == "image_analysis_in_progress" and lease and lease != RUN_ID and stale_image_lease(r):
            r["previous_image_queue_status"] = status
            r["stale_lease_reclaimed_from_run_id"] = lease
            r["stale_lease_reclaimed_at"] = datetime.now(timezone.utc).isoformat()
        pending.append(r)
    if retry_exhausted:
        ydb_upsert_image_rows(retry_exhausted, stage="media_fetch_retry_exhausted")
        log_event("media_fetch_retry_exhausted", phase="poll", rows=len(retry_exhausted), max_attempts=max_media_fetch_attempts())
    refresh_deferred = sum(
        1 for row in blocked if str(row.get("image_eligibility_status") or "") == "deferred_refresh"
    )
    soft_deferred = sum(
        1 for row in blocked if str(row.get("image_eligibility_status") or "") == "deferred_soft_gate"
    )
    terminal_blocked = len(blocked) - refresh_deferred - soft_deferred
    expose_publication_eligibility_counters(
        pending=len(pending),
        blocked=terminal_blocked,
        refresh_deferred=refresh_deferred,
        soft_deferred=soft_deferred,
    )
    ordinary_pending = [row for row in pending if str(row.get("vlm_revisit_requested") or "").lower() != "true"]
    vlm_pending = [row for row in pending if str(row.get("vlm_revisit_requested") or "").lower() == "true"]
    ordinary_pending = sorted(
        ordinary_pending,
        key=lambda r: (
            0 if str(r.get("selected_for_next_image_batch") or "").lower() in {"1", "true", "yes"} else 1,
            int(r.get("image_queue_order") or 10**9),
            str(r.get("post_url") or ""),
        ),
    )
    vlm_slots = max(0, image_vlm_max_calls_per_run() - int(IMAGE_VLM_STATS["attempted"]))
    vlm_pending = sorted(vlm_pending, key=image_vlm_priority, reverse=True)[:vlm_slots]
    # Reserve at most the bounded VLM slots for historical re-downloads while
    # leaving the rest of the batch available for genuinely new image work.
    pending = vlm_pending[:limit_n]
    pending.extend(ordinary_pending[: max(0, limit_n - len(pending))])
    now=datetime.now(timezone.utc).isoformat()
    for r in pending:
        r["image_queue_status"]="image_analysis_in_progress"; r["lease_run_id"]=RUN_ID; r["lease_at"]=now
    ydb_upsert_image_rows(pending, stage="leased_for_image_analysis")
    return pending, len(raw)

def poll_ydb_image_queue(limit_n: int, *, wait_seconds: int, reason: str):
    deadline = time.monotonic() + max(0, wait_seconds)
    attempt = 0
    while True:
        attempt += 1
        try:
            batch, total = ydb_rows_for_diagnostic(limit_n)
            input_payload["queue_rows_total"] = total
            eligibility_pending = int(input_payload.get("publication_eligibility_pending_count") or 0)
            eligibility_blocked = int(input_payload.get("publication_eligibility_blocked_count") or 0)
            eligibility_refresh_deferred = int(input_payload.get("publication_eligibility_refresh_deferred_count") or 0)
            eligibility_soft_deferred = int(input_payload.get("publication_eligibility_soft_deferred_count") or 0)
            log_event("image_queue_poll", phase="poll", reason=reason, attempt=attempt, total=total, leased=len(batch), remaining_budget=limit_n, pending=eligibility_pending, blocked=eligibility_blocked, publication_eligibility_pending_count=eligibility_pending, publication_eligibility_blocked_count=eligibility_blocked, publication_eligibility_refresh_deferred_count=eligibility_refresh_deferred, publication_eligibility_soft_deferred_count=eligibility_soft_deferred)
            ydb_upsert_cursor("image_diagnostic", {
                "phase": "poll",
                "status": "running",
                "reason": reason,
                "attempt": attempt,
                "cursor_position": len(batch),
                "cursor_key": str(batch[-1].get("image_queue_id") or batch[-1].get("post_url") or "") if batch else "",
                "total": total,
                "leased": len(batch),
                "remaining_budget": limit_n,
                "pending": eligibility_pending,
                "blocked": eligibility_blocked,
                "publication_eligibility_pending_count": eligibility_pending,
                "publication_eligibility_blocked_count": eligibility_blocked,
                "publication_eligibility_refresh_deferred_count": eligibility_refresh_deferred,
                "publication_eligibility_soft_deferred_count": eligibility_soft_deferred,
                "progress_label": f"image diagnostic poll leased {len(batch)}/{total}; eligibility pending={eligibility_pending} blocked={eligibility_blocked} refresh_deferred={eligibility_refresh_deferred} soft_deferred={eligibility_soft_deferred}",
            })
            if batch:
                return batch, total
        except Exception as exc:
            log_event("ydb_image_queue_read_failed", phase="poll", reason=reason, attempt=attempt, error=type(exc).__name__ + ": " + str(exc)[:300])
            if reason == "initial" and attempt == 1 and wait_seconds <= 0:
                raise
        if time.monotonic() >= deadline:
            return [], int(input_payload.get("queue_rows_total") or 0)
        sleep_for = min(max(1, POLL_INTERVAL_SECONDS), max(1, int(deadline - time.monotonic())))
        time.sleep(sleep_for)

source_mode=(os.getenv("REGION_TALK_IMAGE_DIAG_SOURCE") or input_payload.get("source") or ("ydb" if (os.getenv("REGION_TALK_STATE_BACKEND") or "").lower()=="ydb" else "input")).lower()
MAX_ITEMS_PER_RUN = int(os.getenv("REGION_TALK_IMAGE_DIAG_MAX_ITEMS_PER_RUN") or input_payload.get("max_items_per_run") or limit)
BATCH_SIZE = max(1, min(MAX_ITEMS_PER_RUN, int(os.getenv("REGION_TALK_IMAGE_DIAG_BATCH_SIZE") or input_payload.get("batch_size") or 30)))
POLL_INTERVAL_SECONDS = int(os.getenv("REGION_TALK_IMAGE_DIAG_POLL_INTERVAL_SECONDS") or input_payload.get("poll_interval_seconds") or 60)
WAIT_INITIAL_SECONDS = int(os.getenv("REGION_TALK_IMAGE_DIAG_WAIT_INITIAL_SECONDS") or input_payload.get("wait_initial_seconds") or 600)
WAIT_AFTER_DRAIN_SECONDS = int(os.getenv("REGION_TALK_IMAGE_DIAG_WAIT_AFTER_DRAIN_SECONDS") or input_payload.get("wait_after_drain_seconds") or 600)
if source_mode != "ydb":
    rows = rows[:MAX_ITEMS_PER_RUN]
else:
    rows = []
    input_payload["source"] = "ydb"
log_event("kernel_started", run_id=RUN_ID, source=input_payload.get("source") or source_mode, max_items_per_run=MAX_ITEMS_PER_RUN, batch_size=BATCH_SIZE, poll_interval_seconds=POLL_INTERVAL_SECONDS, wait_initial_seconds=WAIT_INITIAL_SECONDS, wait_after_drain_seconds=WAIT_AFTER_DRAIN_SECONDS, input_rows=len(rows), publication_eligibility_gate_version=expected_publication_eligibility_gate_version(), secret_status={"ok": secret_status.get("ok"), "keys_count": len(secret_status.get("keys") or [])})

model_availability = {
    "cv_local_baseline": {"available": True, "detail": "PIL resolution/sharpness/brightness/contrast baseline"},
    "laion_aesthetic_predictor": {"available": False, "detail": "not loaded yet"},
    "nima_lightweight_quality": {"available": False, "detail": "not loaded yet"},
    "clip_iqa_postcardness_prompt_scorer": {"available": False, "detail": "not loaded yet"},
}
errors = []

def parse_tg(url: str):
    m = re.search(r"t\.me/(?:s/)?([^/?#]+)/([0-9]+)", url or "")
    return (m.group(1), int(m.group(2))) if m else (None, None)

def _download_http_image(url: str, path: Path, *, timeout: int = 30) -> str:
    if url.startswith("//"):
        url = "https:" + url
    req = Request(url, headers={"User-Agent":"Mozilla/5.0 RegionTalkImageDiagnostic/1.0","Accept":"image/avif,image/webp,image/apng,image/*,*/*;q=0.8"})
    with urlopen(req, timeout=timeout) as resp:  # nosec B310 - public image URL from public post HTML/YDB row
        content_type = str(resp.headers.get("Content-Type") or "").split(";", 1)[0].strip().lower()
        if content_type and not (content_type.startswith("image/") or content_type == "application/octet-stream"):
            raise ValueError(f"public media URL returned non-image content-type: {content_type}")
        data = resp.read(25_000_000)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return str(path)

def _public_tg_post_image_url(handle: str, mid: int, *, timeout: int = 20) -> str:
    if not handle or not mid:
        return ""
    page_url = f"https://t.me/s/{handle}/{mid}"
    req = Request(page_url, headers={"User-Agent":"Mozilla/5.0 RegionTalkImageDiagnostic/1.0","Accept":"text/html,application/xhtml+xml"})
    with urlopen(req, timeout=timeout) as resp:  # nosec B310 - public Telegram page
        page = resp.read(2_000_000).decode("utf-8", errors="replace")
    marker = f'data-post="{handle}/{mid}"'
    idx = page.find(marker)
    if idx < 0:
        return ""
    start = page.rfind('<div class="tgme_widget_message_wrap', 0, idx)
    end = page.find('<div class="tgme_widget_message_wrap', idx + 10)
    block = page[start if start >= 0 else idx : end if end > 0 else len(page)]
    urls = re.findall(r"background-image:url\(([^)]+)\)", block, flags=re.I)
    if not urls:
        urls = re.findall(r'<img[^>]+src="([^"]+)"', block, flags=re.I)
    for raw in urls:
        url = html.unescape(raw.strip().strip('"\''))
        if url.startswith("//"):
            url = "https:" + url
        if url.startswith("http") and re.search(r"\.(?:jpg|jpeg|png|webp)(?:\?|$)", url, re.I):
            return url
    return ""

def parse_vk(url: str):
    m = re.search(r"vk\.com/wall(-?\d+)_(\d+)", url or "")
    return (int(m.group(1)), int(m.group(2))) if m else (None, None)

def decode_bundle():
    b64 = (os.getenv(os.getenv("REGION_TALK_AUTH_BUNDLE_ENV", "TELEGRAM_AUTH_BUNDLE_DISCOVERY2")) or os.getenv("TELEGRAM_AUTH_BUNDLE_DISCOVERY2") or "").strip()
    if not b64: return None
    return json.loads(base64.urlsafe_b64decode(b64.encode("ascii")).decode("utf-8"))

def public_tg_html_fallback_enabled() -> bool:
    return str(os.getenv("REGION_TALK_IMAGE_DIAG_PUBLIC_TG_HTML_FALLBACK") or "0").strip().lower() in {"1", "true", "yes", "on"}

def direct_image_url(ref: str) -> str:
    """Return only a real HTTP image locator, never a post-level marker.

    CandidateReport uses ``https://t.me/<handle>/<id>#media`` to say that the
    Telegram post has media but its bytes still need to be fetched.  Treating
    that marker as an image URL downloads the Telegram HTML page into a ``.jpg``
    file and terminalizes a perfectly valid candidate as a decode failure.
    """
    value = str(ref or "").strip()
    if not (value.startswith("http") or value.startswith("//")):
        return ""
    parsed = urllib.parse.urlsplit("https:" + value if value.startswith("//") else value)
    if parsed.fragment.lower() == "media":
        return ""
    return value

async def telegram_media_humanlike_pause(index: int, total: int) -> None:
    if index >= total:
        return
    minimum = max(0.0, float(os.getenv("REGION_TALK_IMAGE_DIAG_TG_MIN_DELAY_SECONDS") or "2.0"))
    maximum = max(minimum, float(os.getenv("REGION_TALK_IMAGE_DIAG_TG_MAX_DELAY_SECONDS") or "5.0"))
    if maximum <= 0:
        return
    delay = random.uniform(minimum, maximum)
    log_event("telegram_media_humanlike_pause", phase="telegram_fetch", seconds=round(delay, 3), after_index=index, total=total)
    await asyncio.sleep(delay)


def _telegram_message_is_image(message) -> bool:
    if getattr(message, "photo", None) is not None:
        return True
    file_obj = getattr(message, "file", None)
    mime_type = str(getattr(file_obj, "mime_type", "") or "").lower()
    return mime_type.startswith("image/")


async def _telegram_album_messages(client, handle: str, anchor, mid: int) -> list:
    grouped_id = getattr(anchor, "grouped_id", None)
    if grouped_id is None:
        return [anchor]
    radius = max(12, max_images_per_post() + 2)
    ids = list(range(max(1, mid - radius), mid + radius + 1))
    nearby = await client.get_messages(handle, ids=ids)
    messages = [
        message for message in (nearby or [])
        if message is not None and getattr(message, "grouped_id", None) == grouped_id
    ]
    messages.sort(key=lambda message: int(getattr(message, "id", 0) or 0))
    return messages or [anchor]


async def fetch_telegram(batch):
    remaining = []
    for r in batch:
        direct_refs = _row_direct_image_refs(r)
        if direct_refs:
            t0 = time.monotonic()
            paths, direct_errors = _download_direct_image_refs(r, direct_refs, name_prefix="public_url")
            expected = _expected_image_count(r, len(direct_refs))
            status = "complete" if paths and len(paths) >= expected and not direct_errors else "partial"
            if paths:
                _apply_acquired_paths(
                    r,
                    paths,
                    media_ids=[f"direct:{index}" for index in range(1, len(paths) + 1)],
                    expected=expected,
                    status=status,
                )
                r["media_download_seconds"] = round(time.monotonic()-t0, 3)
                r["media_fetch_status"] = "downloaded_public_url" if status == "complete" else "downloaded_partial_album"
                if direct_errors:
                    r["media_fetch_error"] = "; ".join(direct_errors)[:300]
                log_event("image_fetch_result", phase="public_url_fetch", image_queue_id=r.get("image_queue_id"), post_url=r.get("post_url"), status=r.get("media_fetch_status"), actual=True, actual_images=len(paths), expected_images=expected, seconds=r.get("media_download_seconds"))
            if status == "complete":
                continue
        remaining.append(r)
    batch = remaining
    if not batch:
        return
    bundle = decode_bundle()
    api_id = os.getenv("TG_API_ID") or os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TG_API_HASH") or os.getenv("TELEGRAM_API_HASH")
    if not bundle or not api_id or not api_hash:
        for r in batch:
            r["media_fetch_status"] = "needs_actual_image_fetch"
            r["media_fetch_error"] = "telegram auth bundle/api id/hash unavailable"
        return
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    device = {k: bundle[k] for k in ["device_model", "system_version", "app_version", "lang_code", "system_lang_code"] if bundle.get(k)}
    client = TelegramClient(StringSession(bundle["session"]), int(api_id), api_hash, flood_sleep_threshold=30, **device)
    await client.connect()
    try:
        for idx, r in enumerate(batch, 1):
            log_event("image_fetch_current", phase="telegram_fetch", index=idx, total=len(batch), image_queue_id=r.get("image_queue_id"), post_url=r.get("post_url"), source_title=r.get("source_title"))
            t0 = time.monotonic(); handle, mid = parse_tg(r.get("post_url", ""))
            if not handle:
                r["media_fetch_status"]="needs_actual_image_fetch"; r["media_fetch_error"]="cannot parse telegram url"; continue
            try:
                msg = await client.get_messages(handle, ids=mid)
                if not msg or not getattr(msg, "media", None):
                    public_url = _public_tg_post_image_url(handle, mid) if public_tg_html_fallback_enabled() else ""
                    if public_url:
                        path = MEDIA / f"{r['image_queue_id']}_{handle}_{mid}_public.jpg"
                        downloaded = _download_http_image(public_url, path)
                        expected = _expected_image_count(r, 1)
                        _apply_acquired_paths(r, [downloaded], media_ids=[f"telegram:{mid}"], expected=expected, status="complete" if expected <= 1 else "partial")
                        r["media_fetch_status"]="downloaded_public_tg_html"
                        r["media_download_seconds"] = round(time.monotonic()-t0, 3)
                        log_event("image_fetch_result", phase="telegram_fetch", index=idx, total=len(batch), image_queue_id=r.get("image_queue_id"), post_url=r.get("post_url"), status=r.get("media_fetch_status"), actual=bool(r.get("actual_media_path")), seconds=r.get("media_download_seconds"), error=r.get("media_fetch_error"))
                        continue
                    else:
                        r["media_fetch_status"]="needs_actual_image_fetch"
                        r["media_fetch_error"]="telegram message has no direct media; public t.me/s fallback disabled"
                        continue
                album_messages = await _telegram_album_messages(client, handle, msg, mid)
                image_messages = [message for message in album_messages if _telegram_message_is_image(message)]
                unsupported_messages = [message for message in album_messages if getattr(message, "media", None) and not _telegram_message_is_image(message)]
                expected = len(image_messages)
                if not image_messages:
                    if unsupported_messages:
                        suffix = str(getattr(getattr(unsupported_messages[0], "file", None), "ext", "") or "unknown")
                        mark_unsupported_media(r, f"telegram media is not an image: {suffix}")
                    else:
                        r["media_fetch_status"] = "needs_actual_image_fetch"
                        r["media_fetch_error"] = "telegram post contains no image frames"
                    continue
                paths: list[str] = []
                media_ids: list[str] = []
                download_errors: list[str] = []
                for frame_index, message in enumerate(image_messages[:max_images_per_post()], 1):
                    message_id = int(getattr(message, "id", 0) or 0)
                    try:
                        path = await client.download_media(message, file=str(MEDIA / f"{r['image_queue_id']}_{handle}_{message_id}"))
                        if path and not _path_is_unsupported_media(path):
                            paths.append(str(path))
                            media_ids.append(f"telegram:{message_id}")
                        else:
                            download_errors.append(f"{message_id}:empty_or_unsupported")
                    except Exception as frame_exc:
                        download_errors.append(f"{message_id}:{type(frame_exc).__name__}: {str(frame_exc)[:100]}")
                r["media_download_seconds"] = round(time.monotonic()-t0, 3)
                acquisition_complete = bool(paths) and len(paths) == expected and not download_errors and expected <= max_images_per_post()
                if paths:
                    _apply_acquired_paths(r, paths, media_ids=media_ids, expected=expected, status="complete" if acquisition_complete else "partial")
                    if download_errors:
                        r["media_fetch_error"] = "; ".join(download_errors)[:300]
                else:
                    r["media_fetch_status"]="needs_actual_image_fetch"; r["media_fetch_error"]="download_media returned empty path"
            except Exception as exc:
                try:
                    public_url = _public_tg_post_image_url(handle, mid) if public_tg_html_fallback_enabled() else ""
                    if public_url:
                        path = MEDIA / f"{r['image_queue_id']}_{handle}_{mid}_public.jpg"
                        downloaded = _download_http_image(public_url, path)
                        expected = _expected_image_count(r, 1)
                        _apply_acquired_paths(r, [downloaded], media_ids=[f"telegram:{mid}"], expected=expected, status="complete" if expected <= 1 else "partial")
                        r["media_fetch_status"]="downloaded_public_tg_html"
                    else:
                        r["media_fetch_status"]="needs_actual_image_fetch"
                        r["media_fetch_error"] = type(exc).__name__ + ": " + str(exc)[:260] + "; public t.me/s fallback disabled"
                except Exception as web_exc:
                    r["media_fetch_status"]="needs_actual_image_fetch"; r["media_fetch_error"] = type(exc).__name__ + ": " + str(exc)[:160] + "; public_html=" + type(web_exc).__name__ + ": " + str(web_exc)[:120]
                r["media_download_seconds"] = round(time.monotonic()-t0, 3)
            log_event("image_fetch_result", phase="telegram_fetch", index=idx, total=len(batch), image_queue_id=r.get("image_queue_id"), post_url=r.get("post_url"), status=r.get("media_fetch_status"), actual=bool(r.get("actual_media_path")), actual_images=len(_actual_media_paths(r)), expected_images=r.get("expected_image_count"), acquisition_status=r.get("image_acquisition_status"), seconds=r.get("media_download_seconds"), error=r.get("media_fetch_error"))
            if idx % 10 == 0:
                log_event("media_fetch_progress", phase="media_fetch", done=idx, total=len(batch), actual=sum(1 for x in batch if x.get("actual_media_path")))
            await telegram_media_humanlike_pause(idx, len(batch))
    finally:
        await client.disconnect()

def _vk_read_token() -> tuple[str, str]:
    """Choose a VK token that is safe for remote read-only wall requests.

    User tokens can be bound to the IP where they were issued.  Kaggle must
    therefore prefer a service token/key for public ``wall.getById`` reads,
    matching CandidateReport's token-selection contract.  The selected name
    is returned for diagnostics without ever logging the token value.
    """
    if _env_bool("REGION_TALK_VK_READ_SERVICE_FIRST", True):
        names = (
            "VK_SERVICE_TOKEN",
            "VK_SERVICE_KEY",
            "VK_TOKEN",
            "VK_USER_TOKEN",
            "VK_ACCESS_TOKEN4",
            "VK_ACCESS_TOKEN5",
            "VK_ACCESS_TOKEN",
        )
    else:
        names = (
            "VK_USER_TOKEN",
            "VK_ACCESS_TOKEN4",
            "VK_ACCESS_TOKEN5",
            "VK_ACCESS_TOKEN",
            "VK_SERVICE_TOKEN",
            "VK_SERVICE_KEY",
            "VK_TOKEN",
        )
    for name in names:
        token = str(os.getenv(name) or "").strip()
        if token:
            return token, name
    return "", ""


def fetch_vk(r):
    t0 = time.monotonic()
    direct_refs = _row_direct_image_refs(r)
    if direct_refs:
        paths, errors = _download_direct_image_refs(r, direct_refs, name_prefix="vk_public_url")
        expected = _expected_image_count(r, len(direct_refs))
        status = "complete" if paths and len(paths) >= expected and not errors else "partial"
        if paths:
            _apply_acquired_paths(r, paths, media_ids=[f"vk_direct:{index}" for index in range(1, len(paths) + 1)], expected=expected, status=status)
            r["media_fetch_status"] = "downloaded_public_url" if status == "complete" else "downloaded_partial_album"
            if errors:
                r["media_fetch_error"] = "; ".join(errors)[:300]
            r["media_download_seconds"] = round(time.monotonic()-t0, 3)
        if status == "complete":
            return
    token, token_kind = _vk_read_token()
    if token_kind:
        r["vk_read_token_kind"] = token_kind
    owner, pid = parse_vk(r.get("post_url", ""))
    if not token or owner is None:
        prior_error = str(r.get("media_fetch_error") or "")
        r["media_fetch_status"]="needs_actual_image_fetch"; r["media_fetch_error"]=(prior_error + "; " if prior_error else "") + "VK token unavailable or url parse failed"; return
    try:
        resp = requests.get("https://api.vk.com/method/wall.getById", params={"posts": f"{owner}_{pid}", "access_token": token, "v": "5.199"}, timeout=25)
        data = resp.json(); items = data.get("response")
        if isinstance(items, dict): items = items.get("items") or []
        if not items: raise RuntimeError(str(data.get("error") or "empty VK response")[:300])
        photos=[]
        for a in items[0].get("attachments") or []:
            if a.get("type") == "photo" and a.get("photo"):
                sizes = a["photo"].get("sizes") or []
                if sizes:
                    best = max(sizes, key=lambda s: int(s.get("width") or 0)*int(s.get("height") or 0))
                    if best.get("url"): photos.append(best.get("url"))
        if not photos: raise RuntimeError("no VK photo attachment")
        paths=[]; media_ids=[]; download_errors=[]
        for frame_index, photo_url in enumerate(photos[:max_images_per_post()], 1):
            try:
                img = requests.get(photo_url, timeout=35); img.raise_for_status()
                path = MEDIA / f"{r['image_queue_id']}_vk_{owner}_{pid}_{frame_index}.jpg"; path.write_bytes(img.content)
                paths.append(str(path)); media_ids.append(f"vk:{owner}_{pid}:{frame_index}")
            except Exception as frame_exc:
                download_errors.append(f"{frame_index}:{type(frame_exc).__name__}: {str(frame_exc)[:120]}")
        if not paths:
            raise RuntimeError("all VK photo downloads failed: " + "; ".join(download_errors)[:200])
        complete = len(paths) == len(photos) and not download_errors and len(photos) <= max_images_per_post()
        _apply_acquired_paths(r, paths, media_ids=media_ids, expected=len(photos), status="complete" if complete else "partial")
        if download_errors:
            r["media_fetch_error"] = "; ".join(download_errors)[:300]
        r["media_download_seconds"] = round(time.monotonic()-t0, 3)
    except Exception as exc:
        r["media_fetch_status"]="needs_actual_image_fetch"; r["media_fetch_error"] = type(exc).__name__ + ": " + str(exc)[:300]; r["media_download_seconds"] = round(time.monotonic()-t0, 3)

def _path_is_unsupported_media(path: str | Path | None) -> bool:
    if not path:
        return False
    suffix = Path(str(path)).suffix.lower()
    return suffix in UNSUPPORTED_MEDIA_SUFFIXES

def _path_has_supported_image_suffix(path: str | Path | None) -> bool:
    if not path:
        return False
    suffix = Path(str(path)).suffix.lower()
    return not suffix or suffix in SUPPORTED_IMAGE_SUFFIXES

def mark_unsupported_media(r, reason: str, *, path: str | Path | None = None):
    if path:
        r["unsupported_media_path"] = str(path)
    r["actual_media_path"] = ""
    r["actual_image_count"] = 0
    r["media_fetch_status"] = "unsupported_media"
    r["media_fetch_error"] = reason[:300]
    r["final_visual_status"] = "unsupported_media"
    r["image_model_input_type"] = "unsupported_media"
    r["image_model_type"] = "unsupported_media"
    r["image_diagnostic_error"] = reason[:300]
    return r

def _row_has_terminal_media_failure(r) -> bool:
    return str(r.get("media_fetch_status") or "") in {"unsupported_media", "decode_failed"} or str(r.get("final_visual_status") or "") == "unsupported_media"

def validate_image(r):
    t=time.monotonic()
    p = r.get("actual_media_path")
    if not p: r["actual_image_count"] = 0; return None
    if _path_is_unsupported_media(p) or not _path_has_supported_image_suffix(p):
        r["image_decode_seconds"] = round(time.monotonic()-t, 3)
        mark_unsupported_media(r, f"downloaded media is not an image: {Path(str(p)).suffix.lower() or 'unknown'}", path=p)
        return None
    try:
        im = Image.open(p).convert("RGB")
        r["actual_image_count"] = 1; r["image_width"], r["image_height"] = im.size; r["image_file_bytes"] = Path(p).stat().st_size
        th = im.copy(); th.thumbnail((420,300)); tp = THUMBS / (Path(p).stem + ".jpg"); th.save(tp, quality=84); r["thumbnail_path"] = str(tp)
        r["image_decode_seconds"] = round(time.monotonic()-t, 3)
        return im
    except Exception as exc:
        r["image_decode_seconds"] = round(time.monotonic()-t, 3); r["actual_image_count"] = 0; r["media_fetch_status"] = "decode_failed"; r["media_fetch_error"] = type(exc).__name__ + ": " + str(exc)[:300]; return None

def score_cv(im, r):
    t=time.monotonic(); w,h=im.size; gray=im.convert("L"); st=ImageStat.Stat(gray)
    mean_b=st.mean[0]/255.0; std_b=st.stddev[0]/128.0; sharp=ImageStat.Stat(gray.filter(ImageFilter.FIND_EDGES)).mean[0]/64.0
    aspect=w/max(1,h); res=min(1.0,(w*h)/(1280*720)); aspect_score=max(0,1-min(abs(aspect-1.5),1.5)/1.5)
    brightness=max(0,1-abs(mean_b-0.52)/0.52); contrast=max(0,min(1,std_b)); sharp_score=max(0,min(1,sharp))
    technical=.35*res+.25*sharp_score+.2*brightness+.2*contrast; aesthetic=.35*contrast+.25*brightness+.2*aspect_score+.2*sharp_score
    low_noise=.5*sharp_score+.5*contrast; postcard=.45*aesthetic+.35*technical+.2*aspect_score; overall=.35*technical+.3*aesthetic+.25*postcard+.1*low_noise
    r.update({"cv_technical_quality_score":round(technical,3),"cv_aesthetic_score":round(aesthetic,3),"cv_postcardness_score":round(postcard,3),"cv_publication_safety_score":0.98,"cv_low_noise_score":round(low_noise,3),"cv_overall_media_score":round(overall,3),"cv_inference_seconds":round(time.monotonic()-t,3)})

CLIP={"loaded":False,"error":None}
def maybe_clip():
    if CLIP["loaded"]: return True
    if CLIP["error"]: return False
    try:
        t=time.monotonic(); model_reference, model_origin=clip_model_reference(); local_only=model_origin != "huggingface_hub"
        import torch
        from transformers import CLIPModel, CLIPProcessor
        device="cuda" if torch.cuda.is_available() else "cpu"
        log_event("model_load_started", phase="model_load", model="clip_iqa_postcardness_prompt_scorer", model_id=CLIP_MODEL_ID, model_origin=model_origin, model_reference=model_reference, device=device)
        # ``local_files_only`` is the CLIP network guard. Do not set the
        # process-wide HF_HUB_OFFLINE flag: huggingface_hub captures it at
        # import time and that silently disables the later, independently
        # required pyiqa/NIMA weight lookup in the same notebook.
        proc=CLIPProcessor.from_pretrained(model_reference, local_files_only=local_only); model=CLIPModel.from_pretrained(model_reference, local_files_only=local_only).to(device); model.eval()
        CLIP.update({"loaded":True,"torch":torch,"processor":proc,"model":model,"device":device})
        model_availability["clip_iqa_postcardness_prompt_scorer"]={"available":True,"detail":f"{CLIP_MODEL_ID} from {model_origin} on {device}, load_seconds={round(time.monotonic()-t,2)}"}
        log_event("model_load_done", phase="model_load", model="clip_iqa_postcardness_prompt_scorer", model_id=CLIP_MODEL_ID, model_origin=model_origin, model_reference=model_reference, device=device, load_seconds=round(time.monotonic()-t, 3))
        return True
    except Exception as exc:
        CLIP["error"] = type(exc).__name__ + ": " + str(exc)[:500]
        model_availability["clip_iqa_postcardness_prompt_scorer"]={"available":False,"detail":CLIP["error"]}
        log_event("model_unavailable", phase="model_load", model="clip_iqa_postcardness_prompt_scorer", error=CLIP["error"])
        return False

def clip_prompt_bank(track: str) -> tuple[list[str], list[str]]:
    negative = [
        "screenshot", "meme", "advertising banner", "news incident photo",
        "low quality blurry photo", "document scan", "crowded political event", "accident scene",
    ]
    if track == "architecture_interior_editorial":
        positive = [
            "professional architectural photography",
            "high quality editorial interior photography",
            "well composed museum interior",
            "beautiful exhibition design photography",
            "architectural photograph with expressive light materials and spatial depth",
            "professional cultural venue photography",
        ]
    elif track == "editorial_publication":
        positive = [
            "high quality editorial feature photography",
            "professional documentary photography of a place",
            "visually compelling cultural magazine photograph",
            "strong standalone image for an editorial article",
            "well composed travel and heritage photography",
        ]
    else:
        positive = [
            "beautiful postcard travel photo", "scenic Baltic sea travel photo",
            "beautiful old European city architecture", "Kaliningrad travel postcard photo",
            "atmospheric seaside resort town",
        ]
    return positive, negative


def score_clip(im, r):
    if not maybe_clip(): return
    track = visual_content_track(r)
    pos, neg = clip_prompt_bank(track)
    prompts=pos+neg; t=time.monotonic()
    try:
        torch=CLIP["torch"]; inputs=CLIP["processor"](text=prompts,images=im,return_tensors="pt",padding=True).to(CLIP["device"])
        with torch.no_grad(): probs=CLIP["model"](**inputs).logits_per_image.softmax(dim=1)[0].detach().cpu().tolist()
        ps=sum(probs[:len(pos)]); ns=sum(probs[len(pos):])
        fit = round(ps/(ps+ns+1e-9),3)
        r.update({"visual_content_track":track,"clip_visual_fit_score":fit,"clip_postcardness_score":fit,"clip_score_semantics":"genre_visual_fit_compatibility_alias","clip_positive_mass":round(ps,4),"clip_negative_mass":round(ns,4),"clip_top_prompt":prompts[max(range(len(prompts)), key=lambda i: probs[i])],"clip_inference_seconds":round(time.monotonic()-t,3)})
    except Exception as exc:
        r["clip_error"] = type(exc).__name__ + ": " + str(exc)[:300]

LAION={"loaded":False,"error":None}
def maybe_laion():
    if LAION["loaded"]: return True
    if LAION["error"]: return False
    try:
        t=time.monotonic()
        log_event("model_load_started", phase="model_load", model="laion_aesthetic_predictor")
        if not maybe_clip():
            raise RuntimeError("CLIP unavailable; LAION aesthetic v1 needs CLIP ViT-B/32 embeddings")
        torch=CLIP["torch"]; device=CLIP["device"]
        cache=Path.home()/".cache"/"region-talk-image-diagnostic"; cache.mkdir(parents=True, exist_ok=True)
        weights=cache/"sa_0_4_vit_b_32_linear.pth"
        if not weights.exists():
            urlretrieve("https://github.com/LAION-AI/aesthetic-predictor/raw/main/sa_0_4_vit_b_32_linear.pth", weights)
        model=torch.nn.Linear(512,1).to(device)
        try:
            state=torch.load(str(weights), map_location=device, weights_only=True)
        except TypeError:
            state=torch.load(str(weights), map_location=device)
        model.load_state_dict(state); model.eval()
        LAION.update({"loaded":True,"model":model})
        model_availability["laion_aesthetic_predictor"]={"available":True,"detail":f"LAION sa_0_4_vit_b_32_linear on CLIP ViT-B/32, load_seconds={round(time.monotonic()-t,2)}"}
        log_event("model_load_done", phase="model_load", model="laion_aesthetic_predictor", device=device, load_seconds=round(time.monotonic()-t, 3))
        return True
    except Exception as exc:
        LAION["error"]=type(exc).__name__ + ": " + str(exc)[:500]
        model_availability["laion_aesthetic_predictor"]={"available":False,"detail":LAION["error"]}
        log_event("model_unavailable", model="laion_aesthetic_predictor", error=LAION["error"])
        return False

def score_laion(im, r):
    if not maybe_laion(): return
    t=time.monotonic()
    try:
        torch=CLIP["torch"]
        inputs=CLIP["processor"](images=im, return_tensors="pt").to(CLIP["device"])
        with torch.no_grad():
            emb=CLIP["model"].get_image_features(**inputs)
            if not hasattr(emb, "norm"):
                if hasattr(emb, "pooler_output"):
                    pooled=emb.pooler_output
                    target_in=int(getattr(LAION["model"], "in_features", 512))
                    emb=pooled if int(pooled.shape[-1]) == target_in else CLIP["model"].visual_projection(pooled)
                elif isinstance(emb, (tuple, list)) and emb:
                    emb=emb[0]
            emb=emb / emb.norm(dim=-1, keepdim=True)
            raw=float(LAION["model"](emb).detach().cpu().flatten()[0])
        r.update({"laion_aesthetic_raw_score":round(raw,3),"laion_aesthetic_score":round(max(0.0,min(1.0,raw/10.0)),3),"laion_inference_seconds":round(time.monotonic()-t,3)})
    except Exception as exc:
        r["laion_error"] = type(exc).__name__ + ": " + str(exc)[:300]
        r["laion_inference_seconds"] = round(time.monotonic()-t,3)

NIMA={"loaded":False,"error":None}
def maybe_nima():
    if NIMA["loaded"]: return True
    if NIMA["error"]: return False
    try:
        t=time.monotonic()
        log_event("model_load_started", phase="model_load", model="nima_lightweight_quality")
        try:
            import pyiqa  # type: ignore
        except Exception:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "pyiqa"])
            import pyiqa  # type: ignore
        import torch
        device="cuda" if torch.cuda.is_available() else "cpu"
        last=None
        for metric_name in ("nima", "nima-vgg16-ava"):
            try:
                metric=pyiqa.create_metric(metric_name, device=device)
                NIMA.update({"loaded":True,"metric":metric,"device":device,"metric_name":metric_name})
                model_availability["nima_lightweight_quality"]={"available":True,"detail":f"pyiqa {metric_name} on {device}, load_seconds={round(time.monotonic()-t,2)}"}
                log_event("model_load_done", phase="model_load", model="nima_lightweight_quality", model_id=metric_name, device=device, load_seconds=round(time.monotonic()-t, 3))
                return True
            except Exception as exc:
                last=type(exc).__name__ + ": " + str(exc)[:500]
        raise RuntimeError(last or "pyiqa NIMA metric unavailable")
    except Exception as exc:
        NIMA["error"]=type(exc).__name__ + ": " + str(exc)[:500]
        model_availability["nima_lightweight_quality"]={"available":False,"detail":NIMA["error"]}
        log_event("model_unavailable", model="nima_lightweight_quality", error=NIMA["error"])
        return False

def score_nima(r):
    if not maybe_nima(): return
    t=time.monotonic()
    try:
        score=NIMA["metric"](r.get("actual_media_path"))
        if hasattr(score, "detach"):
            raw=float(score.detach().cpu().flatten()[0])
        else:
            raw=float(score)
        r.update({"nima_quality_raw_score":round(raw,3),"nima_quality_score":round(max(0.0,min(1.0,raw/10.0)),3),"nima_inference_seconds":round(time.monotonic()-t,3)})
    except Exception as exc:
        r["nima_error"] = type(exc).__name__ + ": " + str(exc)[:300]
        r["nima_inference_seconds"] = round(time.monotonic()-t,3)

def finalize(r):
    if r.get("actual_image_count") != 1:
        r["final_visual_status"] = "unsupported_media" if _row_has_terminal_media_failure(r) else "needs_actual_image_fetch"; return
    vals=[r.get("cv_overall_media_score")]
    if r.get("clip_postcardness_score") not in (None, ""): vals.append(r.get("clip_postcardness_score"))
    if r.get("laion_aesthetic_score") not in (None, ""): vals.append(r.get("laion_aesthetic_score"))
    if r.get("nima_quality_score") not in (None, ""): vals.append(r.get("nima_quality_score"))
    vals=[float(v) for v in vals if v not in (None, "")]
    r["final_visual_score"] = round(sum(vals)/len(vals),3) if vals else ""
    r["final_visual_status"] = "scored_actual_image"
    comps=[r.get("cv_postcardness_score"), r.get("cv_aesthetic_score"), r.get("clip_postcardness_score"), r.get("laion_aesthetic_score"), r.get("nima_quality_score")]
    comps=[float(v) for v in comps if v not in (None, "")]
    r["model_disagreement_score"] = round(pstdev(comps),3) if len(comps)>1 else 0
    r["total_inference_seconds"] = round(sum(float(r.get(k) or 0) for k in ("cv_inference_seconds","clip_inference_seconds","laion_inference_seconds","nima_inference_seconds")), 3)
    r["total_processing_seconds"] = round(sum(float(r.get(k) or 0) for k in ("media_download_seconds","image_decode_seconds","total_inference_seconds")), 3)


FRAME_SCORE_FIELDS = (
    "cv_technical_quality_score", "cv_aesthetic_score", "cv_postcardness_score", "cv_overall_media_score",
    "clip_postcardness_score", "clip_visual_fit_score", "clip_score_semantics", "visual_content_track", "clip_positive_mass", "clip_negative_mass", "clip_top_prompt",
    "laion_aesthetic_raw_score", "laion_aesthetic_score", "nima_quality_raw_score", "nima_quality_score",
    "final_visual_score", "model_disagreement_score", "image_width", "image_height", "image_file_bytes",
    "cv_inference_seconds", "clip_inference_seconds", "laion_inference_seconds", "nima_inference_seconds",
    "total_inference_seconds", "image_decode_seconds", "final_visual_status",
)


def _frame_component_bundle_complete(frame: dict) -> bool:
    return all(frame.get(key) not in (None, "") for key in (
        "cv_overall_media_score", "clip_postcardness_score", "laion_aesthetic_score", "nima_quality_score",
    ))


def _legacy_frame_passes(frame: dict) -> bool:
    try:
        overall = float(frame.get("final_visual_score") or 0)
        postcardness = float(frame.get("clip_postcardness_score") or frame.get("cv_postcardness_score") or 0)
        aesthetic = float(frame.get("laion_aesthetic_score") or frame.get("cv_aesthetic_score") or 0)
        technical = float(frame.get("cv_technical_quality_score") or 0)
    except (TypeError, ValueError):
        return False
    primary = overall >= legacy_publication_media_threshold() and postcardness >= float(
        os.getenv("REGION_TALK_PUBLICATION_MIN_POSTCARDNESS_SCORE") or "0.55"
    )
    narrow_override = (
        overall >= float(os.getenv("REGION_TALK_PUBLICATION_NEAR_MIN_OVERALL_MEDIA_SCORE") or "0.63")
        and postcardness >= float(os.getenv("REGION_TALK_PUBLICATION_NEAR_MIN_POSTCARDNESS_SCORE") or "0.85")
        and aesthetic + float(os.getenv("REGION_TALK_PUBLICATION_SCORE_QUANTIZATION_EPSILON") or "0.001")
        >= float(os.getenv("REGION_TALK_PUBLICATION_NEAR_MIN_AESTHETIC_SCORE") or "0.52")
        and technical >= float(os.getenv("REGION_TALK_PUBLICATION_NEAR_MIN_TECHNICAL_SCORE") or "0.68")
    )
    return primary or narrow_override


def _compact_frame_score(parent: dict, frame: dict, *, frame_index: int, media_item: dict) -> dict:
    parent_key = str(parent.get("image_queue_id") or parent.get("post_url") or "")
    media_id = str(media_item.get("media_id") or f"frame:{frame_index}")
    compact = {
        "image_frame_score_id": f"{parent_key}:{frame_index}",
        "image_queue_id": parent.get("image_queue_id") or "",
        "post_url": parent.get("post_url") or "",
        "canonical_source_key": parent.get("canonical_source_key") or parent.get("source_key") or "",
        "frame_index": frame_index,
        "media_id": media_id,
        "content_sha256": media_item.get("content_sha256") or "",
        "image_decision_contract_version": IMAGE_DECISION_CONTRACT_VERSION,
        "image_acquisition_version": IMAGE_ACQUISITION_VERSION,
        "image_scorer_version": IMAGE_LEGACY_SCORER_VERSION,
    }
    compact.update({key: frame.get(key) for key in FRAME_SCORE_FIELDS if frame.get(key) not in (None, "")})
    return compact


def apply_album_quality_decision(row: dict, frame_scores: list[dict]) -> dict:
    scored = [frame for frame in frame_scores if frame.get("final_visual_status") == "scored_actual_image"]
    row["image_decision_contract_version"] = IMAGE_DECISION_CONTRACT_VERSION
    row["image_scorer_version"] = IMAGE_LEGACY_SCORER_VERSION
    row["visual_content_track"] = visual_content_track(row)
    row["images_scored_actual_count"] = len(scored)
    row["actual_image_count"] = len(scored)
    row["frame_scores_available_count"] = len(scored)
    row["image_component_bundle_complete"] = str(bool(scored) and all(_frame_component_bundle_complete(frame) for frame in scored)).lower()
    if not scored:
        row["image_quality_decision"] = IMAGE_QUALITY_SCORING_RETRY
        row["image_quality_reason"] = "no_decodable_scored_image"
        return row

    ranked = sorted(scored, key=lambda frame: float(frame.get("final_visual_score") or -1), reverse=True)
    best = ranked[0]
    anchor = scored[0]
    for key in FRAME_SCORE_FIELDS:
        if anchor.get(key) not in (None, ""):
            row[key] = anchor.get(key)
    row["overall_media_score"] = anchor.get("final_visual_score")
    row["postcardness_score"] = anchor.get("clip_postcardness_score") or anchor.get("cv_postcardness_score")
    row["visual_fit_score"] = anchor.get("clip_visual_fit_score") or anchor.get("clip_postcardness_score") or anchor.get("cv_postcardness_score")
    row["aesthetic_score"] = anchor.get("laion_aesthetic_score") or anchor.get("cv_aesthetic_score")
    row["technical_quality_score"] = anchor.get("cv_technical_quality_score")
    row["shadow_best_frame_index"] = int(best.get("frame_index") or 1)
    row["shadow_best_frame_score"] = best.get("final_visual_score")
    row["shadow_best_frame_postcardness_score"] = best.get("clip_postcardness_score") or best.get("cv_postcardness_score")
    row["selected_media_ids"] = json.dumps(
        [str(frame.get("media_id") or "") for frame in ranked[: min(3, len(ranked))]],
        ensure_ascii=False,
        separators=(",", ":"),
    )

    expected = int(row.get("expected_image_count") or len(scored))
    fetched = int(row.get("fetched_image_count") or len(_actual_media_paths(row)))
    acquisition_complete = str(row.get("image_acquisition_status") or "") == "complete" and expected == fetched
    components_complete = all(_frame_component_bundle_complete(frame) for frame in scored)
    if not acquisition_complete:
        row["image_quality_decision"] = IMAGE_QUALITY_NEEDS_REVIEW
        row["image_quality_reason"] = "incomplete_album_never_terminal_quality_reject"
        row["image_quality_terminality"] = "nonterminal"
    elif not components_complete:
        row["image_quality_decision"] = IMAGE_QUALITY_SCORING_RETRY
        row["image_quality_reason"] = "required_legacy_component_unavailable"
        row["image_quality_terminality"] = "nonterminal"
    elif _legacy_frame_passes(anchor):
        # Preserve only the already-established legacy success path. Later
        # frames are shadow diagnostics until the labelled album calibration
        # required by the external audit is complete.
        row["image_quality_decision"] = IMAGE_QUALITY_LEGACY_ACCEPT
        row["image_quality_reason"] = "complete_album_anchor_passed_existing_quality_contract"
        row["image_quality_terminality"] = "contract_version"
    else:
        row["image_quality_decision"] = IMAGE_QUALITY_NEEDS_REVIEW
        row["image_quality_reason"] = "uncalibrated_legacy_low_score_requires_visual_review"
        row["image_quality_terminality"] = "nonterminal"
    return row

def apply_image_queue_status(r):
    previous_status = str(r.get("image_queue_status") or "")
    if r.get("actual_image_count"):
        quality_decision = str(r.get("image_quality_decision") or "")
        if quality_decision == IMAGE_QUALITY_SCORING_RETRY:
            r["image_queue_status"] = "scoring_retry"
        elif quality_decision == IMAGE_QUALITY_NEEDS_REVIEW:
            r["image_queue_status"] = "actual_scored"
        else:
            r["image_queue_status"] = "actual_scored"
        r["image_model_input_type"] = "actual_image"
        r["image_model_type"] = (
            "versioned_album_legacy_diagnostics_plus_vlm"
            if quality_decision == IMAGE_QUALITY_VLM_ACCEPT
            else "versioned_album_legacy_diagnostics"
        )
        r["media_acquisition_status"] = (
            "actual_album_downloaded_and_scored"
            if str(r.get("image_acquisition_status") or "") == "complete"
            else "partial_album_requires_retry_or_review"
        )
        if quality_decision in {IMAGE_QUALITY_LEGACY_ACCEPT, IMAGE_QUALITY_VLM_ACCEPT, IMAGE_QUALITY_OPERATOR_ACCEPT}:
            r["next_action"] = "publication_verification"
        elif quality_decision == IMAGE_QUALITY_SCORING_RETRY:
            r["next_action"] = "retry_required_image_components"
        else:
            r["next_action"] = "visual_review_nonterminal"
    else:
        if _row_has_terminal_media_failure(r):
            r["image_queue_status"] = IMAGE_TERMINAL_UNSUPPORTED_STATUS
            r["media_acquisition_status"] = "unsupported_media_or_decode_failed"
            r["image_model_input_type"] = r.get("image_model_input_type") or "unsupported_media"
            r["image_model_type"] = r.get("image_model_type") or "unsupported_media"
            r["images_scored_actual_count"] = 0
            r["next_action"] = "skip_unsupported_media"
        elif int(r.get("media_fetch_attempt_count") or 0) >= max_media_fetch_attempts():
            r["image_queue_status"] = "needs_visual_review"
            r["media_acquisition_status"] = "media_fetch_exhausted_requires_nonterminal_review"
            r["images_scored_actual_count"] = 0
            r["media_fetch_retry_exhausted"] = "true"
            r["image_quality_decision"] = IMAGE_QUALITY_NEEDS_REVIEW
            r["image_quality_reason"] = "media_acquisition_exhausted_without_complete_album"
            r["image_quality_terminality"] = "nonterminal"
            r["next_action"] = "visual_review_or_acquisition_repair"
        else:
            r["image_queue_status"] = "needs_actual_image_fetch"
            r["media_acquisition_status"] = "needs_actual_image_fetch"
    if previous_status and previous_status != str(r.get("image_queue_status") or ""):
        r["previous_image_queue_status"] = previous_status
        r["status_changed_this_run"] = "true"
        r["last_status_changed_at"] = datetime.now(timezone.utc).isoformat()
    else:
        r.setdefault("status_changed_this_run", "false")
    return r

def process_batch(batch_rows, batch_index: int):
    rows, eligibility_blocked_rows = partition_publication_eligible_rows(batch_rows)
    eligibility_refresh_deferred_count = sum(
        1 for row in eligibility_blocked_rows
        if str(row.get("image_eligibility_status") or "") == "deferred_refresh"
    )
    eligibility_soft_deferred_count = sum(
        1 for row in eligibility_blocked_rows
        if str(row.get("image_eligibility_status") or "") == "deferred_soft_gate"
    )
    eligibility_terminal_blocked_count = (
        len(eligibility_blocked_rows)
        - eligibility_refresh_deferred_count
        - eligibility_soft_deferred_count
    )
    if eligibility_blocked_rows:
        expose_publication_eligibility_counters(
            pending=len(rows),
            blocked=eligibility_terminal_blocked_count,
            refresh_deferred=eligibility_refresh_deferred_count,
            soft_deferred=eligibility_soft_deferred_count,
        )
        ydb_upsert_image_rows(eligibility_blocked_rows, stage="blocked_publication_eligibility")
    elif source_mode != "ydb":
        expose_publication_eligibility_counters(
            pending=len(rows), blocked=0, refresh_deferred=0, soft_deferred=0
        )
    attempt_at = datetime.now(timezone.utc).isoformat()
    for r in rows:
        r["media_fetch_attempt_count"] = int(r.get("media_fetch_attempt_count") or 0) + 1
        r["media_fetch_last_attempt_at"] = attempt_at
    eligibility_pending_count = int(input_payload.get("publication_eligibility_pending_count") or 0)
    eligibility_blocked_count = int(input_payload.get("publication_eligibility_blocked_count") or 0)
    eligibility_refresh_deferred_count = int(input_payload.get("publication_eligibility_refresh_deferred_count") or 0)
    eligibility_soft_deferred_count = int(input_payload.get("publication_eligibility_soft_deferred_count") or 0)
    log_event("image_batch_started", phase="batch", batch_index=batch_index, rows=len(rows), pending=eligibility_pending_count, blocked=eligibility_blocked_count, publication_eligibility_pending_count=eligibility_pending_count, publication_eligibility_blocked_count=eligibility_blocked_count, publication_eligibility_refresh_deferred_count=eligibility_refresh_deferred_count, publication_eligibility_soft_deferred_count=eligibility_soft_deferred_count)
    # Fetch media, no source/comment scanning.
    tg=[r for r in rows if "t.me/" in (r.get("post_url") or "")]
    vk=[r for r in rows if "vk.com/wall" in (r.get("post_url") or "")]
    web=[r for r in rows if r not in tg and r not in vk]
    log_event("media_fetch_started", phase="media_fetch", telegram=len(tg), vk=len(vk), web=len(web), total=len(rows))
    try:
        if tg:
            asyncio.run(fetch_telegram(tg))
        for r in tg:
            ydb_upsert_image_rows([r], stage="media_fetch_result")
    except Exception as exc:
        err=type(exc).__name__ + ": " + str(exc)[:500]
        errors.append({"stage":"telegram_fetch_batch","error":err})
        for r in tg:
            if not r.get("media_fetch_status"):
                r["media_fetch_status"]="needs_actual_image_fetch"; r["media_fetch_error"]=err
            ydb_upsert_image_rows([r], stage="media_fetch_result")
    for i, r in enumerate(vk, 1):
        log_event("image_fetch_current", phase="vk_fetch", index=i, total=len(vk), image_queue_id=r.get("image_queue_id"), post_url=r.get("post_url"), source_title=r.get("source_title"))
        fetch_vk(r)
        ydb_upsert_image_rows([r], stage="media_fetch_result")
        log_event("image_fetch_result", phase="vk_fetch", index=i, total=len(vk), image_queue_id=r.get("image_queue_id"), post_url=r.get("post_url"), status=r.get("media_fetch_status"), actual=bool(r.get("actual_media_path")), seconds=r.get("media_download_seconds"), error=r.get("media_fetch_error"))
        if i % 10 == 0: log_event("vk_fetch_progress", phase="media_fetch", done=i, total=len(vk), actual=sum(1 for x in vk if x.get("actual_media_path")))
    for i, r in enumerate(web, 1):
        log_event("image_fetch_current", phase="web_fetch", index=i, total=len(web), image_queue_id=r.get("image_queue_id"), post_url=r.get("post_url"), source_title=r.get("source_title"))
        fetch_web_direct(r)
        ydb_upsert_image_rows([r], stage="media_fetch_result")
        log_event("image_fetch_result", phase="web_fetch", index=i, total=len(web), image_queue_id=r.get("image_queue_id"), post_url=r.get("post_url"), status=r.get("media_fetch_status"), actual=bool(r.get("actual_media_path")), seconds=r.get("media_download_seconds"), error=r.get("media_fetch_error"))
    log_event("media_fetch_done", phase="media_fetch", actual_downloaded=sum(1 for r in rows if r.get("actual_media_path")), total=len(rows))

    for i, r in enumerate(rows, 1):
        media_paths = _actual_media_paths(r)
        if media_paths and not r.get("media_manifest_items"):
            expected = _expected_image_count(r, len(media_paths))
            manifest = [_manifest_item(path, media_id=f"frame:{idx}", ordinal=idx) for idx, path in enumerate(media_paths, 1)]
            _apply_media_manifest(r, manifest, expected=expected, status="complete" if len(media_paths) == expected else "partial")
        log_event("image_inference_current", phase="inference", index=i, total=len(rows), image_queue_id=r.get("image_queue_id"), post_url=r.get("post_url"), source_title=r.get("source_title"), media_fetch_status=r.get("media_fetch_status"), actual_images=len(media_paths), expected_images=r.get("expected_image_count"))
        frame_scores: list[dict] = []
        compact_frame_rows: list[dict] = []
        manifest_items = list(r.get("media_manifest_items") or [])
        for frame_index, media_path in enumerate(media_paths, 1):
            frame = {
                "actual_media_path": media_path,
                "frame_index": frame_index,
                "media_id": str((manifest_items[frame_index - 1] if frame_index <= len(manifest_items) else {}).get("media_id") or f"frame:{frame_index}"),
                "visual_content_track": visual_content_track(r),
            }
            im = validate_image(frame)
            if im is None:
                errors.append({"image_queue_id":r.get("image_queue_id"),"post_url":r.get("post_url"),"frame_index":frame_index,"stage":"media_acquisition_or_decode","error":frame.get("media_fetch_error") or "no image"})
                continue
            try:
                score_cv(im, frame); score_clip(im, frame); score_laion(im, frame); score_nima(frame); finalize(frame)
            finally:
                try: im.close()
                except Exception: pass
            frame_scores.append(frame)
            media_item = manifest_items[frame_index - 1] if frame_index <= len(manifest_items) else _manifest_item(media_path, media_id=f"frame:{frame_index}", ordinal=frame_index)
            compact_frame_rows.append(_compact_frame_score(r, frame, frame_index=frame_index, media_item=media_item))
        if compact_frame_rows:
            ydb_upsert_frame_score_rows(compact_frame_rows)
        if not frame_scores:
            errors.append({"image_queue_id":r.get("image_queue_id"),"post_url":r.get("post_url"),"stage":"media_acquisition_or_decode","error":r.get("media_fetch_error") or "no decodable image"})
            r["actual_image_count"] = 0
            finalize(r)
        else:
            apply_album_quality_decision(r, frame_scores)
            maybe_adjudicate_image_with_vlm(r, media_paths)
        apply_image_queue_status(r)
        ydb_upsert_image_rows([r], stage="scored_or_retry")
        log_event("image_inference_result", phase="inference", index=i, total=len(rows), image_queue_id=r.get("image_queue_id"), post_url=r.get("post_url"), status=r.get("final_visual_status"), image_quality_decision=r.get("image_quality_decision"), image_quality_reason=r.get("image_quality_reason"), actual_images=r.get("images_scored_actual_count"), expected_images=r.get("expected_image_count"), final_visual_score=r.get("final_visual_score"), shadow_best_frame_score=r.get("shadow_best_frame_score"), cv_score=r.get("cv_overall_media_score"), clip_score=r.get("clip_postcardness_score"), laion_score=r.get("laion_aesthetic_score"), nima_score=r.get("nima_quality_score"), download_seconds=r.get("media_download_seconds"), decode_seconds=r.get("image_decode_seconds"), inference_seconds=r.get("total_inference_seconds"), total_processing_seconds=r.get("total_processing_seconds"), width=r.get("image_width"), height=r.get("image_height"))
        if i % 10 == 0: log_event("inference_progress", phase="inference", done=i, total=len(rows), actual_scored=sum(1 for x in rows if x.get("final_visual_status")=="scored_actual_image"))

    for r in rows:
        apply_image_queue_status(r)
    ydb_upsert_image_rows(rows, stage="scored_or_retry")
    ydb_update_source_visual_rollups()
    result_rows = rows + eligibility_blocked_rows
    log_event("image_batch_done", phase="batch", batch_index=batch_index, rows=len(result_rows), actual_scored=sum(1 for r in rows if r.get("image_queue_status")=="actual_scored"), actual_posts=sum(1 for r in rows if r.get("images_scored_actual_count")), actual_frames=sum(int(r.get("images_scored_actual_count") or 0) for r in rows), pending=eligibility_pending_count, blocked=eligibility_blocked_count, publication_eligibility_pending_count=eligibility_pending_count, publication_eligibility_blocked_count=eligibility_blocked_count, publication_eligibility_refresh_deferred_count=eligibility_refresh_deferred_count, publication_eligibility_soft_deferred_count=eligibility_soft_deferred_count)
    return result_rows

all_processed_rows=[]
if source_mode == "ydb":
    remaining = MAX_ITEMS_PER_RUN
    batch_index = 0
    wait_seconds = WAIT_INITIAL_SECONDS
    while remaining > 0:
        batch, _total = poll_ydb_image_queue(min(BATCH_SIZE, remaining), wait_seconds=wait_seconds, reason="initial" if batch_index == 0 else "after_drain")
        if not batch:
            log_event("image_queue_poll_finished_empty", phase="poll", reason="initial" if batch_index == 0 else "after_drain", processed=len(all_processed_rows), max_items_per_run=MAX_ITEMS_PER_RUN)
            break
        batch_index += 1
        processed = process_batch(batch, batch_index)
        PROCESSED_IMAGE_KEYS.update(image_work_key(row) for row in processed if image_work_key(row))
        all_processed_rows.extend(processed)
        remaining -= len(processed)
        wait_seconds = 0
        while remaining > 0:
            batch, _total = poll_ydb_image_queue(min(BATCH_SIZE, remaining), wait_seconds=0, reason="drain_available")
            if not batch:
                wait_seconds = WAIT_AFTER_DRAIN_SECONDS
                break
            batch_index += 1
            processed = process_batch(batch, batch_index)
            PROCESSED_IMAGE_KEYS.update(image_work_key(row) for row in processed if image_work_key(row))
            all_processed_rows.extend(processed)
            remaining -= len(processed)
    deduped_rows = {}
    for row in all_processed_rows:
        key = image_work_key(row) or f"row:{len(deduped_rows)}"
        deduped_rows[key] = row
    rows = list(deduped_rows.values())
else:
    rows = process_batch(rows, 1) if rows else []

actual_rows=[r for r in rows if r.get("final_visual_status")=="scored_actual_image"]
actual_frame_count=sum(int(r.get("images_scored_actual_count") or 0) for r in actual_rows)
top=sorted(actual_rows,key=lambda r:float(r.get("final_visual_score") or -1),reverse=True)
low=sorted(actual_rows,key=lambda r:float(r.get("final_visual_score") or 999))
disagree=sorted(actual_rows,key=lambda r:float(r.get("model_disagreement_score") or 0),reverse=True)
def timing_stats(field, data):
    vals=[float(r.get(field)) for r in data if r.get(field) not in (None, "")]
    if not vals: return []
    vals_sorted=sorted(vals); p90=vals_sorted[int(0.9*(len(vals_sorted)-1))]
    return [
        {"metric":f"{field}_count","value":len(vals)},
        {"metric":f"{field}_sum","value":round(sum(vals),3)},
        {"metric":f"{field}_mean","value":round(mean(vals),3)},
        {"metric":f"{field}_median","value":round(median(vals),3)},
        {"metric":f"{field}_p90","value":round(p90,3)},
        {"metric":f"{field}_max","value":round(max(vals),3)},
    ]
summary=[
    {"metric":"run_id","value":RUN_ID}, {"metric":"input_rows","value":len(rows)},
    {"metric":"actual_posts_scored_count","value":len(actual_rows)},
    {"metric":"actual_frames_scored_count","value":actual_frame_count},
    # Backward-compatible alias: historically this counted posts, not images.
    {"metric":"actual_images_count_legacy_post_count","value":len(actual_rows)},
    {"metric":"metadata_only_count","value":len(rows)-len(actual_rows)}, {"metric":"failures_count","value":len(errors)},
    {"metric":"publication_eligibility_pending_count","value":int(input_payload.get("publication_eligibility_pending_count") or 0)},
    {"metric":"publication_eligibility_blocked_count","value":int(input_payload.get("publication_eligibility_blocked_count") or 0)},
    {"metric":"publication_eligibility_refresh_deferred_count","value":int(input_payload.get("publication_eligibility_refresh_deferred_count") or 0)},
    {"metric":"image_vlm_backlog_seen_count","value":int(IMAGE_VLM_STATS["backlog_seen"])},
    {"metric":"image_vlm_attempted_count","value":int(IMAGE_VLM_STATS["attempted"])},
    {"metric":"image_vlm_replayed_count","value":int(IMAGE_VLM_STATS["replayed"])},
    {"metric":"image_vlm_accepted_count","value":int(IMAGE_VLM_STATS["accepted"])},
    {"metric":"image_vlm_rejected_nonterminal_count","value":int(IMAGE_VLM_STATS["rejected"])},
    {"metric":"image_vlm_review_count","value":int(IMAGE_VLM_STATS["review"])},
    {"metric":"image_vlm_error_count","value":int(IMAGE_VLM_STATS["errors"])},
    {"metric":"image_vlm_budget_deferred_count","value":int(IMAGE_VLM_STATS["budget_deferred"])},
    {"metric":"image_vlm_run_limit_deferred_count","value":int(IMAGE_VLM_STATS["run_limit_deferred"])},
    {"metric":"elapsed_seconds","value":round(time.monotonic()-RUN_STARTED,3)}, {"metric":"generated_at","value":datetime.now(timezone.utc).isoformat()},
]
for field in ("media_download_seconds","image_decode_seconds","cv_inference_seconds","clip_inference_seconds","laion_inference_seconds","nima_inference_seconds","total_inference_seconds","total_processing_seconds"):
    summary.extend(timing_stats(field, actual_rows if field!="media_download_seconds" else rows))
if actual_rows:
    proc=[float(r.get("total_processing_seconds") or 0) for r in actual_rows if r.get("total_processing_seconds") not in (None, "")]
    infer=[float(r.get("total_inference_seconds") or 0) for r in actual_rows if r.get("total_inference_seconds") not in (None, "")]
    if proc: summary.append({"metric":"throughput_actual_images_per_min_processing_mean","value":round(60/mean(proc),3)})
    if infer: summary.append({"metric":"throughput_actual_images_per_min_inference_mean","value":round(60/mean(infer),3)})
for k,v in model_availability.items(): summary.append({"metric":f"model_{k}","value":json.dumps(v,ensure_ascii=False)})

def write_sheet(wb,name,data,keys=None):
    ws=wb.create_sheet(name)
    if not data:
        ws.append(["_sheet_note"]); ws.append(["no rows"]); return
    if not keys:
        keys=[]
        for r in data:
            for k in r:
                if k not in keys: keys.append(k)
    ws.append(keys)
    for c in ws[1]: c.font=Font(bold=True); c.fill=PatternFill("solid", fgColor="D9EAF7")
    for r in data: ws.append([json.dumps(r.get(k),ensure_ascii=False) if isinstance(r.get(k),(dict,list)) else r.get(k) for k in keys])
    for i,k in enumerate(keys,1): ws.column_dimensions[get_column_letter(i)].width=min(max(12,len(str(k))+2),45)
wb=Workbook(); wb.remove(wb.active)
write_sheet(wb,"00_summary",summary,["metric","value"]); write_sheet(wb,"01_image_queue_input",rows); write_sheet(wb,"02_scored_images",rows); write_sheet(wb,"03_top_high_score",top[:30]); write_sheet(wb,"04_low_score",low[:30]); write_sheet(wb,"05_model_disagreement",disagree[:30]); write_sheet(wb,"06_errors",errors)
xlsx=OUT/f"{RUN_ID}.xlsx"; wb.save(xlsx)

def rel(path):
    try: return Path(path).relative_to(OUT).as_posix()
    except Exception: return str(path or "")
def card(r,label):
    img=f"<img src='{html.escape(rel(r.get('thumbnail_path')))}'>" if r.get("thumbnail_path") else "<div class='noimg'>no image</div>"
    return f"<a class='card' href='{html.escape(r.get('post_url') or '')}' target='_blank'>{img}<div><b>{label}</b> score {html.escape(str(r.get('final_visual_score','')))} cv {html.escape(str(r.get('cv_overall_media_score','')))} clip {html.escape(str(r.get('clip_postcardness_score','')))} laion {html.escape(str(r.get('laion_aesthetic_score','')))} nima {html.escape(str(r.get('nima_quality_score','')))}<br>time total {html.escape(str(r.get('total_processing_seconds','')))}s infer {html.escape(str(r.get('total_inference_seconds','')))}s<br>{html.escape(r.get('source_title') or '')}<br>{html.escape(r.get('post_url') or '')}</div></a>"
html_doc="""<!doctype html><meta charset='utf-8'><style>body{font-family:Arial,sans-serif;background:#111;color:#eee;margin:24px}.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:16px}.card{display:block;background:#1e1e1e;border:1px solid #444;border-radius:12px;padding:10px;color:#eee;text-decoration:none}img{width:100%;height:210px;object-fit:cover;border-radius:8px}.noimg{height:210px;background:#333;display:flex;align-items:center;justify-content:center}</style>"""
html_doc += f"<h1>Region Talk image diagnostic - {html.escape(RUN_ID)}</h1><p>Posts with actual media: {len(actual_rows)}/{len(rows)}; distinct frames scored: {actual_frame_count}. Models: {html.escape(json.dumps(model_availability,ensure_ascii=False))}</p>"
html_doc += "<h2>Top high score</h2><div class='grid'>" + "".join(card(r,"HIGH") for r in top[:24]) + "</div>"
html_doc += "<h2>Low score</h2><div class='grid'>" + "".join(card(r,"LOW") for r in low[:24]) + "</div>"
html_path=OUT/"contact_sheet.html"; html_path.write_text(html_doc,encoding="utf-8")
summary_md=OUT/"summary.md"
available_names=[k for k,v in model_availability.items() if v.get("available")]
best="blend(" + ", ".join(available_names) + ")" if available_names else "no visual model available"
timing_lines="\n".join(f"- {x['metric']}: {x['value']}" for x in summary if any(s in str(x.get("metric")) for s in ("_mean","_median","_p90","throughput_actual_images_per_min")))
summary_md.write_text(f"""# Region Talk image diagnostic - {RUN_ID}\n\n- Input post rows: {len(rows)} from image_candidate_queue.\n- Posts with actual decoded media scored: {len(actual_rows)}.\n- Distinct actual frames scored across those posts: {actual_frame_count}.\n- Metadata-only/failed post rows: {len(rows)-len(actual_rows)}.\n- Elapsed seconds: {round(time.monotonic()-RUN_STARTED,3)}.\n\n## Models that worked\n\n```json\n{json.dumps(model_availability,ensure_ascii=False,indent=2)}\n```\n\n## Timing / throughput\n\n{timing_lines}\n\n## What worked\n\nThis run did not scan sources or comments; it only acquired media for queued image rows and scored actual decoded images.\n\n## What was weak\n\nRows without decoded actual images have no final visual score. Any unavailable model above is recorded with its exact loader error, not substituted with metadata heuristics.\n\n## Visually most convincing scoring\n\nCurrent recommendation: {best}. See `contact_sheet.html`.\n\n## Production recommendations\n\n1. Keep metadata-only rows out of visual ranking.\n2. Package LAION/NIMA weights as stable Kaggle model/input assets if live installs are too slow or flaky.\n3. Use CLIP prompt score as second opinion, not as sole score.\n4. Add screenshot/text/watermark/face/news detectors before publication readiness.\n""",encoding="utf-8")
(OUT/"scored_images.json").write_text(json.dumps({"run_id":RUN_ID,"summary":summary,"model_availability":model_availability,"rows":rows,"errors":errors},ensure_ascii=False,indent=2),encoding="utf-8")
close_image_vlm_runtime()
log_event("report_written", phase="report", status="done", run_id=RUN_ID, actual_images=len(actual_rows), actual_posts=len(actual_rows), actual_frames=actual_frame_count, rows=len(rows), xlsx=str(xlsx), html=str(html_path), summary=str(summary_md), failures=len(errors), ydb_write="attempted", vlm_calls=IMAGE_VLM_STATS["attempted"], vlm_max_calls=image_vlm_max_calls_per_run(), vlm_backlog=IMAGE_VLM_STATS["backlog_seen"])
