from __future__ import annotations

import asyncio
import base64
import hashlib
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

INPUT_ROOT = Path("/kaggle/input")
WORK_DIR = Path("/kaggle/working")
WORK_REPO = WORK_DIR / "repo_bundle"
RESULT_PATH = WORK_DIR / "guide_excursions_results.json"
MEDIA_OUTPUT_DIR = WORK_DIR / "guide_media"
REPO_BOOTSTRAP_WAIT_SECONDS = max(0, int(os.getenv("GUIDE_MONITORING_REPO_BOOTSTRAP_WAIT_SECONDS", "15") or 15))
GUIDE_MEDIA_OUTPUT_LIMIT_PER_POST = max(
    1,
    min(int(os.getenv("GUIDE_MEDIA_OUTPUT_LIMIT_PER_POST", "6") or 6), 10),
)
GUIDE_MEDIA_OUTPUT_MAX_MB = max(
    1,
    min(int(os.getenv("GUIDE_MEDIA_OUTPUT_MAX_MB", "20") or 20), 50),
)

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))


def ensure_libs() -> None:
    modules = [
        ("telethon", "telethon"),
        ("google.generativeai", "google-generativeai"),
        ("cryptography", "cryptography"),
        ("supabase", "supabase"),
        ("nest_asyncio", "nest_asyncio"),
    ]
    missing: list[str] = []
    for module_name, package_name in modules:
        try:
            __import__(module_name)
        except Exception:
            missing.append(package_name)
    if missing:
        print(f"Installing Python packages: {', '.join(missing)}", flush=True)
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", *missing])
    else:
        print("Python packages already available: telethon, google-generativeai, cryptography, supabase", flush=True)


ensure_libs()


def _bootstrap_repo_bundle() -> None:
    bundled_package = SCRIPT_DIR / "google_ai" / "__init__.py"
    if bundled_package.exists():
        print(f"Using bundled google_ai from {bundled_package.parent}", flush=True)
        return
    try:
        if importlib.util.find_spec("google_ai") is not None:
            print("Using google_ai already available on kernel path", flush=True)
            return
    except Exception:
        pass
    deadline = time.monotonic() + REPO_BOOTSTRAP_WAIT_SECONDS
    last_snapshot: list[str] = []
    while True:
        repo_zip_path: Path | None = None
        repo_tree_root: Path | None = None
        flat_repo_root: Path | None = None
        snapshot: list[str] = []
        for path in INPUT_ROOT.rglob("*"):
            if path.is_file():
                snapshot.append(str(path.relative_to(INPUT_ROOT)))
                if len(snapshot) >= 40:
                    break
        last_snapshot = snapshot
        for path in INPUT_ROOT.rglob("repo_bundle.zip"):
            if path.is_file():
                repo_zip_path = path
                break
        if repo_zip_path is None:
            for init_path in INPUT_ROOT.rglob("__init__.py"):
                if init_path.parent.name == "google_ai":
                    repo_tree_root = init_path.parent.parent
                    break
                parent = init_path.parent
                if all((parent / name).is_file() for name in ("__init__.py", "client.py", "exceptions.py", "secrets.py")):
                    flat_repo_root = parent
                    break
        if repo_zip_path is not None:
            if WORK_REPO.exists():
                shutil.rmtree(WORK_REPO)
            WORK_REPO.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(repo_zip_path) as zip_file:
                zip_file.extractall(WORK_REPO)
            sys.path.insert(0, str(WORK_REPO))
            print(f"Bootstrapped repo bundle from {repo_zip_path}", flush=True)
            return
        if repo_tree_root is not None:
            sys.path.insert(0, str(repo_tree_root))
            print(f"Bootstrapped repo bundle from {repo_tree_root}", flush=True)
            return
        if flat_repo_root is not None:
            if WORK_REPO.exists():
                shutil.rmtree(WORK_REPO)
            package_root = WORK_REPO / "google_ai"
            package_root.mkdir(parents=True, exist_ok=True)
            for name in ("__init__.py", "client.py", "exceptions.py", "secrets.py"):
                shutil.copy2(flat_repo_root / name, package_root / name)
            sys.path.insert(0, str(WORK_REPO))
            print(f"Bootstrapped flat repo bundle from {flat_repo_root}", flush=True)
            return
        if time.monotonic() >= deadline:
            print(
                (
                    "Repo bundle not found under /kaggle/input; relying on kernel sources only "
                    f"visible={last_snapshot} wait_s={REPO_BOOTSTRAP_WAIT_SECONDS}"
                ),
                flush=True,
            )
            return
        time.sleep(5)
from cryptography.fernet import Fernet  # noqa: E402
from telethon import TelegramClient, functions  # noqa: E402
from telethon.sessions import StringSession  # noqa: E402

DEFAULT_GUIDE_MONITORING_MODEL = "models/gemma-4-31b-it"
DEFAULT_GUIDE_MONITORING_SCREEN_MODEL = "models/gemma-4-31b-it"
MODEL = DEFAULT_GUIDE_MONITORING_MODEL
SCREEN_MODEL = DEFAULT_GUIDE_MONITORING_SCREEN_MODEL
EXTRACT_MODEL = DEFAULT_GUIDE_MONITORING_MODEL
GOOGLE_KEY_ENV = "GOOGLE_API_KEY2"
GOOGLE_FALLBACK_KEY_ENV = "GOOGLE_API_KEY"
GOOGLE_ACCOUNT_ENV = "GOOGLE_API_LOCALNAME2"
GOOGLE_ACCOUNT_FALLBACK_ENV = "GOOGLE_API_LOCALNAME"
LLM_TIMEOUT_SECONDS = 120
LLM_TIMEOUT_RETRY_ATTEMPTS = 1
LLM_PROVIDER_5XX_RETRY_ATTEMPTS = 1
GUIDE_OCR_ENABLED = True
GUIDE_OCR_IMAGE_LIMIT_PER_POST = 2
GUIDE_OCR_MAX_IMAGE_BYTES = 6 * 1024 * 1024
GUIDE_OCR_TEXT_LIMIT = 1200
_GEMMA_CLIENTS: dict[str, Any] = {}
_SUPABASE_CLIENT: Any | None = None
_LLM_GATEWAY_LOGGED = False

URL_RE = re.compile(r"https?://[^\s<>()]+", re.I)
USERNAME_RE = re.compile(r"(?<!\w)@([A-Za-z0-9_]{4,64})")
PHONE_RE = re.compile(r"(?:(?:\+7|8)[\s(.-]*)?(?:\d[\s().-]*){10,11}")
DATE_RE = re.compile(
    r"\b(?:\d{1,2}[./]\d{1,2}(?:[./]\d{2,4})?|\d{1,2}\s+(?:январ|феврал|март|апрел|мая|май|июн|июл|август|сентябр|октябр|ноябр|декабр)[а-я]*)\b",
    re.I,
)
TIME_RE = re.compile(r"\b([01]?\d|2[0-3]):([0-5]\d)\b")
KEYCAP_DIGIT_RE = re.compile(r"([0-9])\ufe0f?\u20e3")


def _normalize_model_name(model: str) -> str:
    raw = (model or "").strip()
    if raw.startswith("models/"):
        return raw
    return f"models/{raw}"


def _normalize_keycap_digit_dates(text: str | None) -> str:
    """Turn emoji keycap digits into normal digits for schedule anchoring only."""
    return KEYCAP_DIGIT_RE.sub(r"\1", str(text or ""))


def _retry_after_seconds(message: str) -> float | None:
    match = re.search(r"retry after\s+(\d+)\s*ms", message or "", flags=re.I)
    if not match:
        return None
    try:
        delay_ms = int(match.group(1))
    except Exception:
        return None
    if delay_ms <= 0:
        return None
    return max(0.5, min(delay_ms / 1000.0, 65.0))


def _provider_5xx_status(exc: Exception) -> int | None:
    status_code = int(getattr(exc, "status_code", 0) or 0)
    if status_code in {500, 502, 503, 504}:
        return status_code
    message = str(exc or "")
    if "Unknown field for Schema" in message or "anyOf" in message:
        return None
    for code in (500, 502, 503, 504):
        if str(code) in message:
            return code
    lowered = message.lower()
    if "internalservererror" in lowered or "internal error" in lowered or "unavailable" in lowered:
        return 500
    return None


def refresh_runtime_settings() -> None:
    global MODEL, SCREEN_MODEL, EXTRACT_MODEL, GOOGLE_KEY_ENV, GOOGLE_ACCOUNT_ENV, LLM_TIMEOUT_SECONDS
    global LLM_TIMEOUT_RETRY_ATTEMPTS, LLM_PROVIDER_5XX_RETRY_ATTEMPTS
    global GUIDE_OCR_ENABLED, GUIDE_OCR_IMAGE_LIMIT_PER_POST, GUIDE_OCR_MAX_IMAGE_BYTES, GUIDE_OCR_TEXT_LIMIT
    global _GEMMA_CLIENTS, _SUPABASE_CLIENT, _LLM_GATEWAY_LOGGED
    MODEL = (os.getenv("GUIDE_MONITORING_MODEL") or DEFAULT_GUIDE_MONITORING_MODEL).strip()
    SCREEN_MODEL = (os.getenv("GUIDE_MONITORING_SCREEN_MODEL") or DEFAULT_GUIDE_MONITORING_SCREEN_MODEL).strip()
    EXTRACT_MODEL = (os.getenv("GUIDE_MONITORING_EXTRACT_MODEL") or MODEL).strip()
    GOOGLE_KEY_ENV = (os.getenv("GUIDE_MONITORING_GOOGLE_KEY_ENV") or "GOOGLE_API_KEY2").strip() or "GOOGLE_API_KEY2"
    GOOGLE_ACCOUNT_ENV = (os.getenv("GUIDE_MONITORING_GOOGLE_ACCOUNT_ENV") or "GOOGLE_API_LOCALNAME2").strip() or "GOOGLE_API_LOCALNAME2"
    try:
        LLM_TIMEOUT_SECONDS = max(30, int(float((os.getenv("GUIDE_MONITORING_LLM_TIMEOUT_SEC") or "120").strip() or 120)))
    except Exception:
        LLM_TIMEOUT_SECONDS = 120
    try:
        LLM_TIMEOUT_RETRY_ATTEMPTS = max(
            0,
            min(int(float((os.getenv("GUIDE_MONITORING_LLM_TIMEOUT_RETRIES") or "1").strip() or 1)), 3),
        )
    except Exception:
        LLM_TIMEOUT_RETRY_ATTEMPTS = 1
    try:
        LLM_PROVIDER_5XX_RETRY_ATTEMPTS = max(
            0,
            min(int(float((os.getenv("GUIDE_MONITORING_LLM_PROVIDER_5XX_RETRIES") or "1").strip() or 1)), 3),
        )
    except Exception:
        LLM_PROVIDER_5XX_RETRY_ATTEMPTS = 1
    GUIDE_OCR_ENABLED = (os.getenv("GUIDE_MONITORING_OCR_ENABLED") or "1").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    try:
        GUIDE_OCR_IMAGE_LIMIT_PER_POST = max(
            1,
            min(int(float((os.getenv("GUIDE_MONITORING_OCR_IMAGE_LIMIT") or "2").strip() or 2)), 4),
        )
    except Exception:
        GUIDE_OCR_IMAGE_LIMIT_PER_POST = 2
    try:
        GUIDE_OCR_MAX_IMAGE_BYTES = max(
            512 * 1024,
            min(int(float((os.getenv("GUIDE_MONITORING_OCR_MAX_IMAGE_BYTES") or str(6 * 1024 * 1024)).strip() or (6 * 1024 * 1024))), 12 * 1024 * 1024),
        )
    except Exception:
        GUIDE_OCR_MAX_IMAGE_BYTES = 6 * 1024 * 1024
    try:
        GUIDE_OCR_TEXT_LIMIT = max(
            300,
            min(int(float((os.getenv("GUIDE_MONITORING_OCR_TEXT_LIMIT") or "1200").strip() or 1200)), 3000),
        )
    except Exception:
        GUIDE_OCR_TEXT_LIMIT = 1200
    _GEMMA_CLIENTS = {}
    _SUPABASE_CLIENT = None
    _LLM_GATEWAY_LOGGED = False


class _GuideSecretsProviderAdapter:
    def __init__(self, base: Any):
        self.base = base

    def get_secret(self, name: str) -> str | None:
        if name == "GOOGLE_API_KEY":
            return self.base.get_secret(GOOGLE_KEY_ENV) or self.base.get_secret(GOOGLE_FALLBACK_KEY_ENV)
        return self.base.get_secret(name)

    def get_secret_pool(self, prefix: str) -> list[str]:
        if prefix == "GOOGLE_API_KEY":
            keys: list[str] = []
            primary = self.get_secret("GOOGLE_API_KEY")
            if primary:
                keys.append(primary)
            return keys
        getter = getattr(self.base, "get_secret_pool", None)
        if callable(getter):
            return list(getter(prefix) or [])
        return []


def _build_supabase_client() -> Any | None:
    if (os.getenv("SUPABASE_DISABLED") or "").strip() == "1":
        return None
    base_url = (os.getenv("SUPABASE_URL") or "").strip().rstrip("/")
    key = (os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY") or "").strip()
    if not base_url or not key:
        return None
    from supabase import create_client
    from supabase.client import ClientOptions

    options = ClientOptions()
    options.schema = (os.getenv("SUPABASE_SCHEMA") or "public").strip() or "public"
    return create_client(base_url, key, options=options)


def _get_supabase_client() -> Any | None:
    global _SUPABASE_CLIENT
    if _SUPABASE_CLIENT is None:
        _SUPABASE_CLIENT = _build_supabase_client()
    return _SUPABASE_CLIENT


def _guide_account_name() -> str | None:
    return (os.getenv(GOOGLE_ACCOUNT_ENV) or os.getenv(GOOGLE_ACCOUNT_FALLBACK_ENV) or "").strip() or None


def _log_llm_gateway_once() -> None:
    global _LLM_GATEWAY_LOGGED
    if _LLM_GATEWAY_LOGGED:
        return
    print(
        (
            "Guide monitor llm_gateway="
            f"google_ai key_env={GOOGLE_KEY_ENV} "
            f"account_env={GOOGLE_ACCOUNT_ENV} "
            f"account_name={_guide_account_name() or '-'} "
            f"supabase={'yes' if _get_supabase_client() is not None else 'no'} "
            f"timeout={LLM_TIMEOUT_SECONDS}s "
            f"timeout_retries={LLM_TIMEOUT_RETRY_ATTEMPTS} "
            f"provider_5xx_retries={LLM_PROVIDER_5XX_RETRY_ATTEMPTS} "
            f"reserve_fallback={os.getenv('GOOGLE_AI_ALLOW_RESERVE_FALLBACK', '1')} "
            f"local_fallback={os.getenv('GOOGLE_AI_LOCAL_LIMITER_FALLBACK', '1')}"
        ),
        flush=True,
    )
    _LLM_GATEWAY_LOGGED = True


def _get_gemma_client(consumer: str) -> Any:
    client = _GEMMA_CLIENTS.get(consumer)
    if client is None:
        _bootstrap_repo_bundle()
        from google_ai import GoogleAIClient, SecretsProvider

        _log_llm_gateway_once()
        client = GoogleAIClient(
            supabase_client=_get_supabase_client(),
            secrets_provider=_GuideSecretsProviderAdapter(SecretsProvider()),
            consumer=consumer,
            account_name=_guide_account_name(),
        )
        _GEMMA_CLIENTS[consumer] = client
        print(
            (
                f"[gemma:client] consumer={consumer} "
                f"model_family=gemma-only account={_guide_account_name() or '-'} "
                f"key_env={GOOGLE_KEY_ENV}"
            ),
            flush=True,
        )
    return client


def _find_file(name: str) -> Path:
    for path in INPUT_ROOT.rglob(name):
        if path.is_file():
            return path
    raise RuntimeError(f"{name} not found under {INPUT_ROOT}")


def load_runtime_config() -> dict[str, Any]:
    config_path = _find_file("config.json")
    secrets_path = _find_file("secrets.enc")
    key_path = _find_file("fernet.key")
    config = json.loads(config_path.read_text(encoding="utf-8"))
    decrypted = Fernet(key_path.read_bytes()).decrypt(secrets_path.read_bytes())
    secrets = json.loads(decrypted.decode("utf-8"))
    if not isinstance(config, dict) or not isinstance(secrets, dict):
        raise RuntimeError("Invalid config/secrets payload")
    for key, value in secrets.items():
        if value is None:
            continue
        os.environ.setdefault(str(key), str(value))
    return config


def _resolve_auth_bundle() -> tuple[str | None, dict[str, Any] | None]:
    allow_non_s22 = (
        (os.getenv("GUIDE_MONITORING_ALLOW_NON_S22_AUTH") or "0").strip().lower()
        in {"1", "true", "yes", "on"}
    )
    bundle_env = (os.getenv("GUIDE_MONITORING_AUTH_BUNDLE_ENV") or "").strip()
    if bundle_env:
        if bundle_env != "TELEGRAM_AUTH_BUNDLE_S22" and not allow_non_s22:
            raise RuntimeError(
                "GUIDE_MONITORING_AUTH_BUNDLE_ENV may use only TELEGRAM_AUTH_BUNDLE_S22 for Kaggle "
                "unless GUIDE_MONITORING_ALLOW_NON_S22_AUTH=1 is explicitly set"
            )
        candidates = [bundle_env]
    else:
        candidates = ["TELEGRAM_AUTH_BUNDLE_S22"]
        if (os.getenv("TELEGRAM_AUTH_BUNDLE_S22") or "").strip() == "" and (
            os.getenv("TELEGRAM_AUTH_BUNDLE_E2E") or ""
        ).strip():
            if not allow_non_s22:
                raise RuntimeError(
                    "Guide Kaggle monitoring requires TELEGRAM_AUTH_BUNDLE_S22; refusing to fall back to "
                    "TELEGRAM_AUTH_BUNDLE_E2E without GUIDE_MONITORING_ALLOW_NON_S22_AUTH=1"
                )
            candidates.append("TELEGRAM_AUTH_BUNDLE_E2E")
    for key in candidates:
        if not key:
            continue
        raw = (os.getenv(key) or "").strip()
        if not raw:
            continue
        decoded = base64.urlsafe_b64decode(raw.encode("ascii")).decode("utf-8")
        payload = json.loads(decoded)
        if isinstance(payload, dict) and str(payload.get("session") or "").strip():
            return key, payload
    return None, None


async def create_client() -> TelegramClient:
    api_id = int((os.getenv("TG_API_ID") or os.getenv("TELEGRAM_API_ID") or "0").strip() or 0)
    api_hash = (os.getenv("TG_API_HASH") or os.getenv("TELEGRAM_API_HASH") or "").strip()
    if not api_id or not api_hash:
        raise RuntimeError("Missing TG_API_ID/TG_API_HASH")
    _bundle_env, bundle = _resolve_auth_bundle()
    session = str((bundle or {}).get("session") or os.getenv("TG_SESSION") or os.getenv("TELEGRAM_SESSION") or "").strip()
    if not session:
        raise RuntimeError("Missing TELEGRAM auth bundle or session")
    client = TelegramClient(
        StringSession(session),
        api_id,
        api_hash,
        device_model=str((bundle or {}).get("device_model") or "Kaggle Guide Monitor"),
        system_version=str((bundle or {}).get("system_version") or "Linux"),
        app_version=str((bundle or {}).get("app_version") or "1.0"),
        lang_code=str((bundle or {}).get("lang_code") or "ru"),
        system_lang_code=str((bundle or {}).get("system_lang_code") or "ru"),
    )
    await client.connect()
    if not await client.is_user_authorized():
        raise RuntimeError("Telethon client is not authorized")
    return client


async def ensure_client_connected(client: TelegramClient, *, force_reconnect: bool = False) -> None:
    connected = False
    try:
        connected = bool(client.is_connected())
    except Exception:
        connected = False
    if force_reconnect and connected:
        await client.disconnect()
        connected = False
    if not connected:
        await client.connect()
    if not await client.is_user_authorized():
        raise RuntimeError("Telethon client is not authorized")


def _message_text(message: Any) -> str:
    return str(getattr(message, "message", None) or getattr(message, "text", None) or "").strip()


def _message_media_kind(message: Any) -> str | None:
    if getattr(message, "photo", None):
        return "photo"
    if getattr(message, "video", None):
        return "video"
    doc = getattr(message, "document", None)
    if doc is None:
        return None
    mime = str(getattr(doc, "mime_type", None) or "").lower()
    if mime.startswith("video/"):
        return "video"
    if mime.startswith("image/"):
        return "photo"
    return None


def _reactions_payload(message: Any) -> tuple[int | None, dict[str, int] | None]:
    reactions = getattr(message, "reactions", None)
    if not reactions or not getattr(reactions, "results", None):
        return None, None
    out: dict[str, int] = {}
    total = 0
    for item in getattr(reactions, "results", []) or []:
        count = int(getattr(item, "count", 0) or 0)
        reaction = getattr(item, "reaction", None)
        emoji = str(getattr(reaction, "emoticon", None) or getattr(reaction, "document_id", None) or reaction or "")
        if not emoji:
            emoji = "reaction"
        out[emoji] = count
        total += count
    return total or None, out or None


@dataclass(slots=True)
class ScannedPost:
    message_id: int
    grouped_id: int | None
    post_date: datetime
    source_url: str
    text: str
    views: int | None
    forwards: int | None
    reactions_total: int | None
    reactions_json: dict[str, int] | None
    media_refs: list[dict[str, Any]]
    media_assets: list[dict[str, Any]]


def collapse_ws(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def normalize_title_key(value: str | None) -> str:
    text = collapse_ws(value).lower()
    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"[^a-zа-яё0-9]+", " ", text, flags=re.I)
    text = re.sub(r"\b(?:экскурсия|экскурсии|прогулка|прогулки|тур|маршрут|авторская|пешеходная|поездка|путешествие)\b", " ", text, flags=re.I)
    return collapse_ws(text)


def build_source_fingerprint(*, title_normalized: str, date_iso: str | None, time_text: str | None) -> str:
    import hashlib

    payload = "|".join([str(title_normalized or ""), str(date_iso or ""), str(time_text or "")])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


async def materialize_post_media_assets(
    client: TelegramClient,
    *,
    username: str,
    post: ScannedPost,
) -> list[dict[str, Any]]:
    if not post.media_refs:
        return []
    entity = await client.get_entity(username)
    MEDIA_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out: list[dict[str, Any]] = []
    for idx, media_ref in enumerate(post.media_refs[:GUIDE_MEDIA_OUTPUT_LIMIT_PER_POST]):
        message_id = int(media_ref.get("message_id") or 0)
        if message_id <= 0:
            continue
        try:
            message = await client.get_messages(entity, ids=message_id)
        except Exception:
            continue
        if not message:
            continue
        kind = _message_media_kind(message) or str(media_ref.get("kind") or "").strip().lower()
        if kind not in {"photo", "video"}:
            continue
        try:
            downloaded = await client.download_media(message, file=bytes)
        except Exception:
            continue
        if not downloaded:
            continue
        payload = bytes(downloaded)
        if not payload:
            continue
        if len(payload) > GUIDE_MEDIA_OUTPUT_MAX_MB * 1024 * 1024:
            continue
        ext = ".mp4" if kind == "video" else ".jpg"
        rel_path = f"guide_media/{username}_{post.message_id}_{message_id}_{idx}{ext}"
        asset_path = WORK_DIR / rel_path
        asset_path.parent.mkdir(parents=True, exist_ok=True)
        asset_path.write_bytes(payload)
        out.append(
            {
                "message_id": message_id,
                "kind": kind,
                "grouped_id": int(media_ref.get("grouped_id") or 0) or None,
                "relative_path": rel_path,
                "size_bytes": len(payload),
            }
        )
    return out


def _collapse_group(messages: list[Any], *, username: str) -> ScannedPost | None:
    ordered = sorted(messages, key=lambda item: int(getattr(item, "id", 0) or 0))
    anchor = next((msg for msg in ordered if _message_text(msg)), None) or ordered[-1]
    anchor_id = int(getattr(anchor, "id", 0) or 0)
    post_date = getattr(anchor, "date", None) or getattr(ordered[-1], "date", None) or datetime.now(timezone.utc)
    if post_date.tzinfo is None:
        post_date = post_date.replace(tzinfo=timezone.utc)
    text = "\n".join(_message_text(msg) for msg in ordered if _message_text(msg)).strip()
    views = max((getattr(msg, "views", None) or 0) for msg in ordered) or None
    forwards = max((getattr(msg, "forwards", None) or 0) for msg in ordered) or None
    reactions_total = None
    reactions_json = None
    for msg in ordered:
        total, payload = _reactions_payload(msg)
        if total is not None and ((reactions_total or -1) < total):
            reactions_total = total
            reactions_json = payload
    media_refs: list[dict[str, Any]] = []
    for msg in ordered:
        kind = _message_media_kind(msg)
        if not kind:
            continue
        media_refs.append(
            {
                "message_id": int(getattr(msg, "id", 0) or 0),
                "kind": kind,
                "grouped_id": int(getattr(msg, "grouped_id", 0) or 0) or None,
            }
        )
    if not text and not media_refs:
        return None
    return ScannedPost(
        message_id=anchor_id,
        grouped_id=int(getattr(anchor, "grouped_id", 0) or 0) or None,
        post_date=post_date.astimezone(timezone.utc),
        source_url=f"https://t.me/{username}/{anchor_id}",
        text=text,
        views=views,
        forwards=forwards,
        reactions_total=reactions_total,
        reactions_json=reactions_json,
        media_refs=media_refs,
        media_assets=[],
    )


async def scan_source_posts(client: TelegramClient, *, username: str, limit: int, days_back: int) -> tuple[dict[str, Any], list[ScannedPost]]:
    entity = await client.get_entity(username)
    source_title = str(getattr(entity, "title", None) or getattr(entity, "first_name", None) or "").strip() or None
    about_text = ""
    about_links: list[str] = []
    try:
        full = await client(functions.channels.GetFullChannelRequest(channel=entity))
        about_text = str(getattr(full.full_chat, "about", None) or "").strip()
    except Exception:
        try:
            full = await client(functions.users.GetFullUserRequest(id=entity))
            about_text = str(getattr(full.full_user, "about", None) or "").strip()
        except Exception:
            about_text = ""
    if about_text:
        seen: set[str] = set()
        for token in about_text.replace("\n", " ").split():
            raw = str(token or "").strip("()[]{}<>.,!?:;\"'")
            if raw.startswith("http://") or raw.startswith("https://"):
                if raw not in seen:
                    seen.add(raw)
                    about_links.append(raw)
    messages = await client.get_messages(entity, limit=max(1, int(limit)))
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, int(days_back)))
    singles: list[Any] = []
    grouped: dict[int, list[Any]] = {}
    for msg in messages:
        msg_date = getattr(msg, "date", None)
        if msg_date is None:
            continue
        if msg_date.tzinfo is None:
            msg_date = msg_date.replace(tzinfo=timezone.utc)
        if msg_date.astimezone(timezone.utc) < cutoff:
            continue
        if getattr(msg, "action", None):
            continue
        grouped_id = int(getattr(msg, "grouped_id", 0) or 0)
        if grouped_id:
            grouped.setdefault(grouped_id, []).append(msg)
        else:
            singles.append(msg)
    posts: list[ScannedPost] = []
    for msg in singles:
        collapsed = _collapse_group([msg], username=username)
        if collapsed:
            posts.append(collapsed)
    for group in grouped.values():
        collapsed = _collapse_group(group, username=username)
        if collapsed:
            posts.append(collapsed)
    posts.sort(key=lambda item: (item.post_date, item.message_id), reverse=True)
    return {"source_title": source_title, "about_text": about_text or None, "about_links": about_links}, posts


def _detect_image_mime(data: bytes) -> str:
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data.startswith(b"GIF87a") or data.startswith(b"GIF89a"):
        return "image/gif"
    if data.startswith(b"RIFF") and data[8:12] == b"WEBP":
        return "image/webp"
    return "image/jpeg"


def _ocr_chunk_text(chunk: Mapping[str, Any]) -> str:
    parts = [collapse_ws(chunk.get("title")), collapse_ws(chunk.get("text"))]
    return "\n".join(part for part in parts if part).strip()


def _post_has_photo_media(post: ScannedPost) -> bool:
    for item in post.media_refs or []:
        kind = collapse_ws(item.get("kind")).lower()
        if kind == "photo":
            return True
    return False


def _should_run_post_ocr(post: ScannedPost, source_kind: str, flags: dict[str, Any], *, base_pass: bool) -> bool:
    if not GUIDE_OCR_ENABLED or not _post_has_photo_media(post):
        return False
    if base_pass:
        return True
    text_len = len(collapse_ws(post.text))
    if text_len <= 220:
        return True
    if bool(flags.get("grouped_album_present")):
        return True
    if any(bool(flags.get(key)) for key in ("has_date_signal", "has_booking_signal", "has_status_signal")):
        return True
    return source_kind in {"guide_personal", "guide_project", "organization_with_tours", "excursion_operator"}


async def _collect_post_ocr_inputs(
    client: TelegramClient,
    *,
    username: str,
    post: ScannedPost,
) -> list[dict[str, Any]]:
    if not _post_has_photo_media(post):
        return []
    entity = await client.get_entity(username)
    out: list[dict[str, Any]] = []
    for media_ref in post.media_refs:
        if len(out) >= GUIDE_OCR_IMAGE_LIMIT_PER_POST:
            break
        if collapse_ws(media_ref.get("kind")).lower() != "photo":
            continue
        message_id = int(media_ref.get("message_id") or 0)
        if message_id <= 0:
            continue
        try:
            message = await client.get_messages(entity, ids=message_id)
        except Exception:
            continue
        if not message:
            continue
        try:
            downloaded = await client.download_media(message, file=bytes)
        except Exception:
            continue
        if not downloaded:
            continue
        payload = bytes(downloaded)
        if not payload or len(payload) > GUIDE_OCR_MAX_IMAGE_BYTES:
            continue
        out.append(
            {
                "message_id": message_id,
                "mime_type": _detect_image_mime(payload),
                "data": payload,
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        )
    return out


async def _ocr_post_image(
    image_payload: Mapping[str, Any],
    *,
    consumer: str,
    model: str,
    post: ScannedPost,
) -> dict[str, Any] | None:
    mime_type = collapse_ws(image_payload.get("mime_type")) or "image/jpeg"
    image_bytes = image_payload.get("data")
    if not isinstance(image_bytes, (bytes, bytearray)) or not image_bytes:
        return None
    schema = {
        "type": "object",
        "properties": {
            "ocr_text": {"type": "string"},
            "ocr_title": {"type": "string"},
            "excursion_signal": {"type": "boolean"},
            "schedule_signal": {"type": "boolean"},
            "booking_signal": {"type": "boolean"},
        },
        "required": [
            "ocr_text",
            "ocr_title",
            "excursion_signal",
            "schedule_signal",
            "booking_signal",
        ],
    }
    prompt = [
        {
            "text": (
                "You read one Telegram image related to guide excursions. Return only JSON.\n"
                "Fields:\n"
                "- ocr_text: all readable Russian/English text from the image, preserving dates, times, prices, handles, phones and links.\n"
                "- ocr_title: dominant route/excursion title from the image, or empty string if there is no reliable title.\n"
                "- excursion_signal: true only if the image itself shows a concrete excursion/walk/tour/route signal.\n"
                "- schedule_signal: true only if the image contains an explicit date/time or same-day relative schedule marker.\n"
                "- booking_signal: true only if the image contains explicit booking/contact/link/phone/DM signal.\n"
                "Do not invent unreadable text. Ignore decorative slogans unless they carry excursion facts.\n"
                f"Post context: url={post.source_url} post_date_utc={post.post_date.isoformat()}."
            )
        },
        {
            "inline_data": {
                "mime_type": mime_type,
                "data": bytes(image_bytes),
            }
        },
    ]
    data = await ask_gemma(
        model,
        prompt,
        consumer=consumer,
        max_output_tokens=420,
        response_schema=schema,
    )
    if not isinstance(data, dict):
        return None
    text = collapse_ws(data.get("ocr_text"))
    title = collapse_ws(data.get("ocr_title"))
    if not text and not title:
        return None
    return {
        "title": title or None,
        "text": text[:GUIDE_OCR_TEXT_LIMIT] if text else "",
        "excursion_signal": bool(data.get("excursion_signal")),
        "schedule_signal": bool(data.get("schedule_signal")),
        "booking_signal": bool(data.get("booking_signal")),
    }


async def collect_post_ocr_chunks(
    client: TelegramClient,
    *,
    username: str,
    post: ScannedPost,
    model: str,
) -> list[dict[str, Any]]:
    image_inputs = await _collect_post_ocr_inputs(client, username=username, post=post)
    if not image_inputs:
        return []
    chunks: list[dict[str, Any]] = []
    for idx, image_payload in enumerate(image_inputs, start=1):
        try:
            chunk = await _ocr_post_image(
                image_payload,
                consumer="guide_scout_ocr",
                model=model,
                post=post,
            )
        except Exception as exc:
            print(
                (
                    "[guide:ocr:error] "
                    f"message_id={post.message_id} image_message_id={int(image_payload.get('message_id') or 0)} "
                    f"error={type(exc).__name__}: {exc}"
                ),
                flush=True,
            )
            continue
        if not chunk:
            continue
        chunks.append(
            {
                "id": f"O{idx}",
                "message_id": int(image_payload.get("message_id") or 0) or None,
                "sha256": collapse_ws(image_payload.get("sha256")) or None,
                **chunk,
            }
        )
    return chunks


def prefilter_flags(post: ScannedPost, *, ocr_chunks: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    ocr_chunks = [item for item in (ocr_chunks or []) if isinstance(item, dict)]
    ocr_text = "\n".join(_ocr_chunk_text(item) for item in ocr_chunks if _ocr_chunk_text(item))
    text = collapse_ws("\n".join(part for part in (post.text, ocr_text) if collapse_ws(part))).lower()
    schedule_text = _normalize_keycap_digit_dates(text)
    has_ocr_excursion_signal = any(bool(item.get("excursion_signal")) for item in ocr_chunks)
    has_ocr_schedule_signal = any(bool(item.get("schedule_signal")) for item in ocr_chunks)
    has_ocr_booking_signal = any(bool(item.get("booking_signal")) for item in ocr_chunks)
    return {
        "has_date_signal": bool(DATE_RE.search(schedule_text) or "завтра" in text or "сегодня" in text or has_ocr_schedule_signal),
        "has_time_signal": bool(TIME_RE.search(text)),
        "has_price_signal": any(token in text for token in ("стоимость", "цена", "руб", "₽")),
        "has_booking_signal": bool(URL_RE.search(text) or USERNAME_RE.search(text) or PHONE_RE.search(text) or "запись" in text or "бронир" in text or has_ocr_booking_signal),
        "has_status_signal": any(token in text for token in ("мест нет", "лист ожидания", "sold out", "перенос", "отмена", "осталось", "последние места")),
        "has_group_signal": any(token in text for token in ("по запросу", "организованные группы", "для групп", "школьн", "семь")),
        "has_excursion_keywords": any(token in text for token in ("экскурс", "прогул", "маршрут", "путешеств", "тур ")) or has_ocr_excursion_signal,
        "has_ocr_excursion_signal": has_ocr_excursion_signal,
        "grouped_album_present": bool(post.grouped_id),
        "message_url": post.source_url,
    }


def prefilter_pass(post: ScannedPost, source_kind: str, flags: dict[str, Any]) -> bool:
    if not (flags["has_excursion_keywords"] or flags.get("has_ocr_excursion_signal")):
        return False
    if source_kind == "aggregator" and not (
        flags.get("has_ocr_excursion_signal")
        or any(token in collapse_ws(post.text).lower() for token in ("авторская", "приглашаем", "пешеходная"))
    ):
        return False
    return any(flags[key] for key in ("has_date_signal", "has_booking_signal", "has_status_signal", "grouped_album_present"))


def _line_cleanup(line: str) -> str:
    line = str(line or "").replace("\xa0", " ").strip()
    line = re.sub(r"^[•\-\u2022▪▫◾◽]+\s*", "", line)
    return line.strip()


def _looks_decorative_line(line: str) -> bool:
    scan_line = _normalize_keycap_digit_dates(line)
    return bool(scan_line) and not bool(re.search(r"[0-9A-Za-zА-Яа-яЁё]", scan_line))


def _looks_generic_preamble(line: str) -> bool:
    low = collapse_ws(line).lower()
    return any(
        token in low
        for token in (
            "экскурсии и путешествия на",
            "в марте у меня для вас насыщенная программа",
            "весна, идём гулять",
            "весна, идем гулять",
        )
    )


def _has_schedule_anchor(line: str) -> bool:
    scan_line = _normalize_keycap_digit_dates(line)
    low = collapse_ws(scan_line).lower()
    if any(token in low for token in ("мест нет", "лист ожидания", "уже набрана")):
        return False
    return bool(DATE_RE.search(scan_line) or "завтра" in low or "сегодня" in low)


def _is_section_break(line: str) -> bool:
    cleaned = collapse_ws(line)
    if not cleaned:
        return False
    if TIME_RE.search(cleaned):
        return False
    low = cleaned.lower()
    if low.startswith(("обзорные экскурсии", "апрельская премьера", "аудиоквест", "бесплатные лекции")):
        return True
    return cleaned.endswith(":") and len(cleaned.split()) <= 6 and not _has_schedule_anchor(cleaned)


def split_occurrence_blocks(text: str) -> list[str]:
    lines = [_line_cleanup(line) for line in str(text or "").splitlines()]
    lines = [line for line in lines if line and not _looks_decorative_line(line)]
    if not lines:
        return []

    blocks: list[list[str]] = []
    current: list[str] = []
    preamble: list[str] = []
    anchors = 0
    section_break_seen = False
    for line in lines:
        if anchors > 0 and current and _is_section_break(line):
            blocks.append(current)
            current = []
            preamble = [line]
            section_break_seen = True
            continue
        if _has_schedule_anchor(line) and current:
            blocks.append(current)
            current = [line]
            anchors += 1
            continue
        if _has_schedule_anchor(line) and not current:
            anchors += 1
            carry_preamble = [item for item in preamble if not _looks_generic_preamble(item)]
            current = [*carry_preamble, line] if carry_preamble else [line]
            preamble = []
            continue
        if anchors == 0:
            preamble.append(line)
            continue
        current.append(line)
    if current:
        blocks.append(current)

    if anchors <= 1 and not section_break_seen:
        payload = collapse_ws(text)
        return [payload] if payload else []
    return [collapse_ws("\n".join(block)) for block in blocks if collapse_ws("\n".join(block))]


def build_occurrence_blocks(text: str, *, limit: int = 8) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    for idx, block in enumerate(split_occurrence_blocks(text), start=1):
        cleaned = collapse_ws(block)
        if not cleaned:
            continue
        schedule_anchor_text = collapse_ws(_normalize_keycap_digit_dates(cleaned))
        low = cleaned.lower()
        block_payload = {
            "id": f"B{idx}",
            "text": cleaned[:1200],
            "has_schedule_anchor": _has_schedule_anchor(cleaned),
            "has_time_signal": bool(TIME_RE.search(cleaned)),
            "looks_detail_pending": any(
                token in low
                for token in (
                    "подробности позже",
                    "подробности будут позже",
                    "детали позже",
                    "детали будут позже",
                )
            ),
        }
        if schedule_anchor_text and schedule_anchor_text != cleaned:
            block_payload["schedule_anchor_text"] = schedule_anchor_text[:1200]
        blocks.append(block_payload)
        if len(blocks) >= limit:
            break
    return blocks


def split_text_chunks(text: str, *, limit: int = 8) -> list[dict[str, str]]:
    chunks: list[dict[str, str]] = []
    for idx, block in enumerate(re.split(r"\n{2,}", str(text or "").strip()), start=1):
        cleaned = collapse_ws(block)
        if not cleaned:
            continue
        chunks.append({"id": f"T{idx}", "text": cleaned[:700]})
        if len(chunks) >= limit:
            break
    if not chunks and collapse_ws(text):
        chunks.append({"id": "T1", "text": collapse_ws(text)[:700]})
    return chunks


def _compact_source_payload(source_payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "username": collapse_ws(source_payload.get("username")),
        "source_kind": collapse_ws(source_payload.get("source_kind")),
        "title": collapse_ws(source_payload.get("title")),
        "display_name": collapse_ws(source_payload.get("display_name")),
        "marketing_name": collapse_ws(source_payload.get("marketing_name")),
        "base_region": collapse_ws(source_payload.get("base_region")),
    }


def _compact_screen_payload(screen: dict[str, Any]) -> dict[str, Any]:
    return {
        "decision": collapse_ws(screen.get("decision")),
        "post_kind": collapse_ws(screen.get("post_kind")),
        "extract_mode": collapse_ws(screen.get("extract_mode")),
        "digest_eligible_default": collapse_ws(screen.get("digest_eligible_default")),
        "base_region_fit": collapse_ws(screen.get("base_region_fit")),
        "contains_future_public_signal": bool(screen.get("contains_future_public_signal")),
        "contains_past_report_signal": bool(screen.get("contains_past_report_signal")),
    }


def _compact_post_payload(
    post: ScannedPost,
    *,
    flags: dict[str, Any],
    for_extract: bool = False,
    ocr_chunks: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    excerpt_limit = 1500 if for_extract else 700
    chunk_limit = 3 if for_extract else 2
    chunk_text_limit = 420 if for_extract else 220
    chunks = []
    for item in split_text_chunks(post.text, limit=chunk_limit):
        chunks.append(
            {
                "id": item.get("id"),
                "text": collapse_ws(item.get("text"))[:chunk_text_limit],
            }
        )
    compact_flags = {
        key: bool(flags.get(key))
        for key in (
            "has_date_signal",
            "has_time_signal",
            "has_price_signal",
            "has_booking_signal",
            "has_status_signal",
            "has_group_signal",
            "has_excursion_keywords",
            "has_ocr_excursion_signal",
            "grouped_album_present",
        )
    }
    compact_ocr_chunks: list[dict[str, Any]] = []
    for item in ocr_chunks or []:
        text = collapse_ws(item.get("text"))[:chunk_text_limit]
        title = collapse_ws(item.get("title"))[:120]
        if not text and not title:
            continue
        compact_ocr_chunks.append(
            {
                "id": item.get("id"),
                "title": title or None,
                "text": text or "",
                "excursion_signal": bool(item.get("excursion_signal")),
                "schedule_signal": bool(item.get("schedule_signal")),
                "booking_signal": bool(item.get("booking_signal")),
            }
        )
        if len(compact_ocr_chunks) >= 3:
            break
    payload = {
        "message_id": post.message_id,
        "post_date_utc": post.post_date.isoformat(),
        "message_url": post.source_url,
        "text_excerpt": collapse_ws(post.text)[:excerpt_limit],
        "text_chunks": chunks,
        "ocr_chunks": compact_ocr_chunks,
        "media_hints": {
            "photo_count": sum(1 for item in post.media_refs if collapse_ws(item.get("kind")).lower() == "photo"),
            "video_count": sum(1 for item in post.media_refs if collapse_ws(item.get("kind")).lower() == "video"),
        },
        "prefilter_flags": compact_flags,
    }
    if for_extract:
        payload["schedule_blocks"] = _compact_occurrence_blocks(post.text, limit=8)
    return payload


def _compact_occurrence_blocks(text: str, *, limit: int = 5) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    for block in build_occurrence_blocks(text, limit=limit):
        payload = {
            "id": block.get("id"),
            "text": collapse_ws(block.get("text"))[:420],
            "has_schedule_anchor": bool(block.get("has_schedule_anchor")),
            "has_time_signal": bool(block.get("has_time_signal")),
            "looks_detail_pending": bool(block.get("looks_detail_pending")),
        }
        if collapse_ws(block.get("schedule_anchor_text")):
            payload["schedule_anchor_text"] = collapse_ws(block.get("schedule_anchor_text"))[:420]
        blocks.append(payload)
    return blocks


def _string_list_value(value: Any, *, limit: int = 8) -> list[str]:
    out: list[str] = []
    if isinstance(value, str):
        raw = collapse_ws(value)
        items = re.split(r"\s*[;,]\s*", raw) if raw else []
    elif isinstance(value, (list, tuple, set)):
        items = list(value)
    else:
        items = []
    for item in items:
        text = collapse_ws(item)
        if not text or text in out:
            continue
        out.append(text)
        if len(out) >= limit:
            break
    return out


def _extract_json(raw: str) -> Any | None:
    cleaned = str(raw or "").strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```[a-zA-Z0-9_-]*\n", "", cleaned)
        cleaned = cleaned.replace("```", "")
    try:
        data = json.loads(cleaned)
        return data if isinstance(data, (dict, list)) else None
    except Exception:
        pass
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start >= 0 and end > start:
        try:
            data = json.loads(cleaned[start : end + 1])
            return data if isinstance(data, (dict, list)) else None
        except Exception:
            return None
    start = cleaned.find("[")
    end = cleaned.rfind("]")
    if start >= 0 and end > start:
        try:
            data = json.loads(cleaned[start : end + 1])
            return data if isinstance(data, list) else None
        except Exception:
            return None
    return None


_OCCURRENCE_FIELD_TYPES: dict[str, dict[str, Any]] = {
    "source_block_id": {"type": "string"},
    "canonical_title": {"type": "string"},
    "title_normalized": {"type": "string"},
    "date": {"type": "string"},
    "time": {"type": "string"},
    "duration_text": {"type": "string"},
    "city": {"type": "string"},
    "meeting_point": {"type": "string"},
    "route_summary": {"type": "string"},
    "audience_fit": {"type": "array", "items": {"type": "string"}},
    "group_format": {"type": "string"},
    "price_text": {"type": "string"},
    "booking_text": {"type": "string"},
    "booking_url": {"type": "string"},
    "channel_url": {"type": "string"},
    "status": {"type": "string"},
    "seats_text": {"type": "string"},
    "summary_one_liner": {"type": "string"},
    "digest_blurb": {"type": "string"},
    "digest_eligible": {"type": "boolean"},
    "digest_eligibility_reason": {"type": "string"},
    "is_last_call": {"type": "boolean"},
    "post_kind": {"type": "string"},
    "availability_mode": {"type": "string"},
    "guide_names": {"type": "array", "items": {"type": "string"}},
    "organizer_names": {"type": "array", "items": {"type": "string"}},
    "base_region_fit": {"type": "string"},
    "fact_pack": {"type": "object"},
    "fact_claims": {"type": "array", "items": {"type": "object"}},
    "template_hint": {"type": "object"},
    "profile_hint": {"type": "object"},
}


def _occurrence_schema(*keys: str) -> dict[str, Any]:
    properties = {
        key: dict(_OCCURRENCE_FIELD_TYPES[key])
        for key in keys
        if key in _OCCURRENCE_FIELD_TYPES
    }
    return {
        "type": "object",
        "properties": properties,
    }


def _occurrences_wrapper_schema(*keys: str) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "occurrences": {
                "type": "array",
                "items": _occurrence_schema(*keys),
            }
        },
        "required": ["occurrences"],
    }


def _single_occurrence_wrapper_schema(*keys: str) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "occurrence": _occurrence_schema(*keys)
        },
        "required": ["occurrence"],
    }


async def ask_gemma(
    model: str,
    prompt: Any,
    *,
    consumer: str,
    max_output_tokens: int = 2200,
    response_schema: dict[str, Any] | None = None,
) -> Any | None:
    if not (os.getenv(GOOGLE_KEY_ENV) or os.getenv(GOOGLE_FALLBACK_KEY_ENV) or "").strip():
        raise RuntimeError(f"{GOOGLE_KEY_ENV} is missing in Kaggle runtime")
    client = _get_gemma_client(consumer)
    timeout_retries_used = 0
    provider_5xx_retries_used = 0
    for attempt in range(4):
        try:
            raw, _usage = await asyncio.wait_for(
                client.generate_content_async(
                    model=model,
                    prompt=prompt,
                    generation_config={
                        "temperature": 0,
                        **(
                            {
                                "response_mime_type": "application/json",
                                "response_schema": response_schema,
                            }
                            if response_schema is not None
                            else {}
                        ),
                    },
                    max_output_tokens=max_output_tokens,
                ),
                timeout=LLM_TIMEOUT_SECONDS,
            )
            return _extract_json(raw or "")
        except Exception as exc:
            retry_after = _retry_after_seconds(str(exc))
            if retry_after is not None and attempt < 3:
                await asyncio.sleep(min(90.0, retry_after + 1.0))
                continue
            if isinstance(exc, asyncio.TimeoutError) and attempt < 3 and timeout_retries_used < LLM_TIMEOUT_RETRY_ATTEMPTS:
                timeout_retries_used += 1
                delay = min(12.0, 2.0 * timeout_retries_used)
                print(
                    (
                        "[gemma:retry] "
                        f"consumer={consumer} model={model} reason=timeout "
                        f"attempt={attempt + 1} retry={timeout_retries_used}/{LLM_TIMEOUT_RETRY_ATTEMPTS} "
                        f"delay={delay:.1f}s"
                    ),
                    flush=True,
                )
                await asyncio.sleep(delay)
                continue
            status_5xx = _provider_5xx_status(exc)
            if status_5xx is not None and attempt < 3 and provider_5xx_retries_used < LLM_PROVIDER_5XX_RETRY_ATTEMPTS:
                provider_5xx_retries_used += 1
                delay = min(15.0, 3.0 * provider_5xx_retries_used)
                print(
                    (
                        "[gemma:retry] "
                        f"consumer={consumer} model={model} reason=provider_{status_5xx} "
                        f"attempt={attempt + 1} retry={provider_5xx_retries_used}/{LLM_PROVIDER_5XX_RETRY_ATTEMPTS} "
                        f"delay={delay:.1f}s"
                    ),
                    flush=True,
                )
                await asyncio.sleep(delay)
                continue
            raise
    return None


def _boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    raw = collapse_ws(value)
    if not raw:
        return False
    return raw.lower() in {"1", "true", "yes", "y", "on", "да"}


def _normalize_choice(value: Any, *, allowed: set[str], default: str, aliases: dict[str, str] | None = None) -> str:
    raw = collapse_ws(value).lower()
    if aliases:
        raw = aliases.get(raw, raw)
    return raw if raw in allowed else default


def _normalize_reasons(value: Any, *, limit: int = 3) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        text = collapse_ws(item)
        if not text or text in out:
            continue
        out.append(text[:220])
        if len(out) >= limit:
            break
    return out


def _normalize_digest_eligibility(
    *,
    date_iso: str | None,
    availability_mode: str | None,
    status: str | None,
    digest_eligible: bool,
    digest_reason: str | None,
) -> tuple[bool, str | None]:
    if not collapse_ws(date_iso):
        return False, collapse_ws(digest_reason) or "missing_date"
    if collapse_ws(availability_mode) and collapse_ws(availability_mode) != "scheduled_public":
        return False, collapse_ws(digest_reason) or "not_scheduled_public"
    if collapse_ws(status) == "cancelled":
        return False, collapse_ws(digest_reason) or "cancelled"
    return bool(digest_eligible), collapse_ws(digest_reason) or None


def _coerce_occurrence_items(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if not isinstance(data, dict):
        return []
    occurrences = data.get("occurrences")
    if isinstance(occurrences, list):
        return [item for item in occurrences if isinstance(item, dict)]
    occurrence = data.get("occurrence")
    if isinstance(occurrence, dict):
        return [occurrence]
    return []


def _has_material_value(value: Any) -> bool:
    if isinstance(value, str):
        return bool(collapse_ws(value))
    if isinstance(value, (list, tuple, set)):
        return any(_has_material_value(item) for item in value)
    if isinstance(value, dict):
        return any(_has_material_value(item) for item in value.values())
    return value is not None


def _seed_fact_pack_from_occurrence(item: dict[str, Any]) -> dict[str, Any]:
    pack = dict(item.get("fact_pack") or {}) if isinstance(item.get("fact_pack"), dict) else {}
    for key in (
        "canonical_title",
        "title_normalized",
        "date",
        "time",
        "duration_text",
        "city",
        "meeting_point",
        "route_summary",
        "audience_fit",
        "group_format",
        "price_text",
        "booking_text",
        "booking_url",
        "status",
        "seats_text",
        "summary_one_liner",
        "digest_blurb",
        "digest_eligible",
        "digest_eligibility_reason",
        "is_last_call",
        "post_kind",
        "availability_mode",
        "guide_names",
        "organizer_names",
        "base_region_fit",
    ):
        value = item.get(key)
        if _has_material_value(value) and key not in pack:
            pack[key] = value
    return pack


def _merge_string_lists(left: Any, right: Any, *, limit: int = 8) -> list[str]:
    return _string_list_value([*(_string_list_value(left, limit=limit)), *(_string_list_value(right, limit=limit))], limit=limit)


def _merge_occurrence_layers(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    protected_keys = {
        "canonical_title",
        "title_normalized",
        "date",
        "time",
        "source_block_id",
        "source_fingerprint",
        "channel_url",
    }
    list_keys = {"audience_fit", "guide_names", "organizer_names"}
    for key, value in patch.items():
        if key == "fact_pack":
            continue
        if key == "fact_claims":
            existing = merged.get("fact_claims") if isinstance(merged.get("fact_claims"), list) else []
            incoming = value if isinstance(value, list) else []
            merged["fact_claims"] = [*existing, *incoming]
            continue
        if key in list_keys:
            merged[key] = _merge_string_lists(merged.get(key), value, limit=8 if key == "audience_fit" else 4)
            continue
        if key in {"template_hint", "profile_hint"}:
            current = merged.get(key) if isinstance(merged.get(key), dict) else {}
            incoming = value if isinstance(value, dict) else {}
            merged[key] = {**current, **incoming}
            continue
        if key in protected_keys and _has_material_value(merged.get(key)):
            continue
        if _has_material_value(value):
            merged[key] = value
    merged["fact_pack"] = {
        **_seed_fact_pack_from_occurrence(base),
        **(patch.get("fact_pack") if isinstance(patch.get("fact_pack"), dict) else {}),
        **_seed_fact_pack_from_occurrence(merged),
    }
    return merged


def _semantic_focus_excerpt(post: ScannedPost, *, source_block_id: str | None = None) -> str:
    if source_block_id:
        for block in build_occurrence_blocks(post.text, limit=8):
            if collapse_ws(block.get("id")) == collapse_ws(source_block_id):
                return collapse_ws(block.get("text"))[:900]
    return collapse_ws(post.text)[:900]


async def screen_post(
    source_payload: dict[str, Any],
    post: ScannedPost,
    flags: dict[str, Any],
    *,
    ocr_chunks: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    compact_source = _compact_source_payload(source_payload)
    compact_post = _compact_post_payload(post, flags=flags, for_extract=False, ocr_chunks=ocr_chunks)
    schema = {
        "type": "object",
        "properties": {
            "decision": {"type": "string", "enum": ["ignore", "announce", "status_update", "template_only"]},
            "post_kind": {
                "type": "string",
                "enum": [
                    "announce_single",
                    "announce_multi",
                    "status_update",
                    "reportage",
                    "template_signal",
                    "on_demand_offer",
                    "mixed_or_non_target",
                ],
            },
            "extract_mode": {"type": "string", "enum": ["none", "announce", "status", "template"]},
            "digest_eligible_default": {"type": "string", "enum": ["yes", "no", "mixed"]},
            "contains_future_public_signal": {"type": "boolean"},
            "contains_past_report_signal": {"type": "boolean"},
            "base_region_fit": {"type": "string", "enum": ["inside", "outside", "ambiguous", "unknown"]},
            "reasons": {"type": "array", "items": {"type": "string"}},
            "confidence": {"type": "string", "enum": ["low", "medium", "high"]},
        },
        "required": [
            "decision",
            "post_kind",
            "extract_mode",
            "digest_eligible_default",
            "contains_future_public_signal",
            "contains_past_report_signal",
            "base_region_fit",
            "reasons",
            "confidence",
        ],
    }
    prompt = (
        "Classify one Telegram post from a guide/excursions source. Return only JSON.\n"
        "Allowed values:\n"
        "- decision: ignore | announce | status_update | template_only\n"
        "- post_kind: announce_single | announce_multi | status_update | reportage | template_signal | on_demand_offer | mixed_or_non_target\n"
        "- extract_mode: none | announce | status | template\n"
        "- digest_eligible_default: yes | no | mixed\n"
        "- base_region_fit: inside | outside | ambiguous | unknown\n"
        "- confidence: low | medium | high\n"
        "Rules:\n"
        "- announce/status_update only if the post text or OCR contains a real guided public excursion/walk/tour/route signal grounded in the input\n"
        "- do not treat a dated event as an excursion just because it is posted by a guide source; the public product must be a guided walk/excursion/tour/route/storytelling visit\n"
        "- volunteer cleanups, subbotniks, restoration work days, community service, lectures without a guided route, and generic meetups are mixed_or_non_target/ignore unless the post explicitly announces a guided excursion or walk as the primary public offer\n"
        "- if there is a concrete future walk/excursion with date/time/meeting point, prefer announce or status_update over reportage\n"
        "- if the body is mostly historical/reportage but ends with or inserts a concrete future excursion CTA — including relative date markers (this Sunday, tomorrow, next weekend) or a named guide — treat it as announce or status_update, not reportage; absence of exact time or meeting point is fine\n"
        "- on-demand offers or posts saying only that dates remain without naming the dates may be announce/template_only, but digest_eligible_default must be no or mixed until a concrete future date is grounded\n"
        "- generic calendars, inspiration, bloom/lifestyle, or travel-wishlist posts without a concrete excursion -> ignore\n"
        "- if one post clearly contains several different excursions led by the source's own guide, use announce_multi\n"
        "- a post that enumerates multiple festivals/events across different cities or regions as a round-up/travel calendar is template_only or ignore, even when one entry falls inside source.base_region; individual enumerated entries are not per-guide excursions and must not be materialized as announce\n"
        "- base_region_fit is your own semantic judgement about whether the concrete excursion(s) in the post take place inside source.base_region; it must not be derived from keyword matching alone\n"
        "- if the post is about places outside source.base_region (other regions/countries/cities) or is a multi-region travel calendar, set base_region_fit=outside or ambiguous and prefer decision=ignore or template_only\n"
        "- if source.base_region is empty or the post has no concrete place, use base_region_fit=unknown\n"
        "- do not invent facts\n"
        "- reasons must be 1-3 short grounded strings\n"
        "- no reasoning, analysis, or hidden thinking traces\n"
        f"Input:\n{json.dumps({'source': compact_source, 'post': compact_post}, ensure_ascii=False)}"
    )
    data = await ask_gemma(
        SCREEN_MODEL,
        prompt,
        consumer="guide_scout_screen",
        max_output_tokens=160,
        response_schema=schema,
    )
    if not isinstance(data, dict):
        return {
            "decision": "ignore",
            "post_kind": "mixed_or_non_target",
            "extract_mode": "none",
            "digest_eligible_default": "mixed",
            "contains_future_public_signal": False,
            "contains_past_report_signal": False,
            "base_region_fit": "unknown",
            "reasons": ["llm_parse_failed"],
            "confidence": "low",
        }
    decision = _normalize_choice(
        data.get("decision"),
        allowed={"ignore", "announce", "status_update", "template_only"},
        default="ignore",
    )
    post_kind = _normalize_choice(
        data.get("post_kind"),
        allowed={
            "announce_single",
            "announce_multi",
            "status_update",
            "reportage",
            "template_signal",
            "on_demand_offer",
            "mixed_or_non_target",
        },
        default="mixed_or_non_target",
    )
    extract_mode = _normalize_choice(
        data.get("extract_mode"),
        allowed={"none", "announce", "status", "template"},
        default="none",
    )
    digest_eligible_default = _normalize_choice(
        data.get("digest_eligible_default"),
        allowed={"yes", "no", "mixed"},
        aliases={"true": "yes", "false": "no"},
        default="mixed",
    )
    base_region_fit = _normalize_choice(
        data.get("base_region_fit"),
        allowed={"inside", "outside", "ambiguous", "unknown"},
        default="unknown",
    )
    confidence = _normalize_choice(
        data.get("confidence"),
        allowed={"low", "medium", "high"},
        default="low",
    )
    return {
        "decision": decision,
        "post_kind": post_kind,
        "extract_mode": extract_mode,
        "digest_eligible_default": digest_eligible_default,
        "contains_future_public_signal": _boolish(data.get("contains_future_public_signal")),
        "contains_past_report_signal": _boolish(data.get("contains_past_report_signal")),
        "base_region_fit": base_region_fit,
        "reasons": _normalize_reasons(data.get("reasons")),
        "confidence": confidence,
    }


async def _extract_announce_post_tier1(
    source_payload: dict[str, Any],
    *,
    post: ScannedPost,
    flags: dict[str, Any],
    screen: dict[str, Any],
    ocr_chunks: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    compact_source = _compact_source_payload(source_payload)
    compact_screen = _compact_screen_payload(screen)
    compact_post = _compact_post_payload(post, flags=flags, for_extract=True, ocr_chunks=ocr_chunks)
    schema = _occurrences_wrapper_schema(
        "source_block_id",
        "canonical_title",
        "title_normalized",
        "date",
        "time",
        "duration_text",
        "city",
        "meeting_point",
        "route_summary",
        "audience_fit",
        "group_format",
        "price_text",
        "booking_text",
        "booking_url",
        "channel_url",
        "status",
        "seats_text",
        "digest_eligible",
        "digest_eligibility_reason",
        "is_last_call",
        "post_kind",
        "availability_mode",
        "guide_names",
        "organizer_names",
        "base_region_fit",
        "fact_pack",
    )
    prompt = (
        "You are trail_scout.announce_extract_tier1.v1.\n"
        "Extract Tier-1 public occurrence facts from one Telegram post about guide excursions.\n"
        "Return only JSON with key occurrences.\n"
        "Rules:\n"
        "- extract only real excursion occurrences or direct public updates about a specific occurrence\n"
        "- ignore past occurrences for MVP\n"
        "- if the post contains several dated schedule lines, return one occurrence per dated line even when they share one booking/contact block\n"
        "- use post.schedule_blocks as the complete schedule index when text_excerpt is shortened; keep source_block_id equal to the block id when a block id is available\n"
        "- numeric emoji keycaps in dates are normal digits: 3️⃣ мая means 3 мая; 1️⃣3️⃣ мая means 13 мая\n"
        "- if a dated line has no explicit closed/sold-out/cancelled marker and the post has shared booking/contact/meeting facts, set status=available, availability_mode=scheduled_public, digest_eligible=true\n"
        "- if a dated line is explicitly tentative/preliminary/only hoped-for or says it is just a free date to move another walk into, do not mark it digest-ready; use digest_eligible=false with digest_eligibility_reason=tentative_or_free_date\n"
        "- when a dated line says places are gone/sold out/full/cancelled, set the matching unavailable status and digest_eligible=false\n"
        "- do not output an extra template/no-date occurrence for a route already covered by concrete dated occurrences in this same post\n"
        "- title_normalized must be a short stable route identity core; do not include guide names, organizer/source labels, parentheses, dates, times, marketing suffixes, or availability words there\n"
        "- volunteer cleanups, subbotniks, restoration work days, community service, lectures without a guided route, and generic meetups are not excursion occurrences unless the block explicitly makes a guided excursion/walk/tour the primary public offer\n"
        "- use OCR facts when the poster carries operational details missing from the text, but only if they are explicit on the poster\n"
        "- set base_region_fit per occurrence by your own judgement versus source.base_region (inside|outside|ambiguous|unknown); do not rely on keyword matching\n"
        "- if an occurrence clearly takes place outside source.base_region, set base_region_fit=outside; do not silently drop it, let the server filter\n"
        "- do not invent details; if a field is unclear, leave it empty\n"
        "- keep only Tier-1 public facts; no extra commentary or hidden thinking traces\n\n"
        f"Input:\n{json.dumps({'source': compact_source, 'screen': compact_screen, 'post': compact_post}, ensure_ascii=False)}"
    )
    data = await ask_gemma(
        EXTRACT_MODEL,
        prompt,
        consumer="guide_scout_announce_tier1_extract",
        max_output_tokens=520,
        response_schema=schema,
    )
    return _coerce_occurrence_items(data)


async def _extract_announce_post_tier1_failopen_for_block_rescue(
    source_payload: dict[str, Any],
    *,
    post: ScannedPost,
    flags: dict[str, Any],
    screen: dict[str, Any],
    ocr_chunks: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    try:
        return await _extract_announce_post_tier1(
            source_payload,
            post=post,
            flags=flags,
            screen=screen,
            ocr_chunks=ocr_chunks,
        )
    except Exception as exc:
        print(
            (
                "[guide:announce_extract:warning] "
                f"message_id={post.message_id} mode=block_rescue "
                f"error={type(exc).__name__}: {exc}"
            ),
            flush=True,
        )
        return []


async def _extract_status_post(
    source_payload: dict[str, Any],
    *,
    post: ScannedPost,
    flags: dict[str, Any],
    screen: dict[str, Any],
    ocr_chunks: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    compact_source = _compact_source_payload(source_payload)
    compact_screen = _compact_screen_payload(screen)
    compact_post = _compact_post_payload(post, flags=flags, for_extract=True, ocr_chunks=ocr_chunks)
    schema = _occurrences_wrapper_schema(
        "source_block_id",
        "canonical_title",
        "title_normalized",
        "date",
        "time",
        "city",
        "meeting_point",
        "route_summary",
        "price_text",
        "booking_text",
        "booking_url",
        "status",
        "seats_text",
        "digest_eligible",
        "digest_eligibility_reason",
        "is_last_call",
        "post_kind",
        "availability_mode",
        "guide_names",
        "organizer_names",
        "base_region_fit",
        "fact_claims",
        "fact_pack",
    )
    prompt = (
        "You are trail_scout.status_claim_extract.v1.\n"
        "Extract one occurrence or direct occurrence update from a Telegram post.\n"
        "Return only JSON with key occurrences.\n"
        "Rules:\n"
        "- focus on status deltas such as last_call, seats left, moved time, changed meeting point, cancellation, or clarified booking\n"
        "- use OCR only for explicit grounded deltas visible on the poster/image\n"
        "- do not invent missing fields\n"
        "- fact_claims should use only claim_role anchor or status_delta\n"
        "- no extra commentary or hidden thinking traces\n\n"
        f"Input:\n{json.dumps({'source': compact_source, 'screen': compact_screen, 'post': compact_post}, ensure_ascii=False)}"
    )
    data = await ask_gemma(
        EXTRACT_MODEL,
        prompt,
        consumer="guide_scout_status_claim_extract",
        max_output_tokens=380,
        response_schema=schema,
    )
    return _coerce_occurrence_items(data)


async def _extract_template_post(
    source_payload: dict[str, Any],
    *,
    post: ScannedPost,
    flags: dict[str, Any],
    screen: dict[str, Any],
    ocr_chunks: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    compact_source = _compact_source_payload(source_payload)
    compact_screen = _compact_screen_payload(screen)
    compact_post = _compact_post_payload(post, flags=flags, for_extract=True, ocr_chunks=ocr_chunks)
    schema = _occurrences_wrapper_schema(
        "source_block_id",
        "canonical_title",
        "title_normalized",
        "city",
        "route_summary",
        "audience_fit",
        "group_format",
        "booking_text",
        "booking_url",
        "post_kind",
        "availability_mode",
        "guide_names",
        "organizer_names",
        "base_region_fit",
        "digest_eligible",
        "digest_eligibility_reason",
        "template_hint",
        "profile_hint",
        "fact_claims",
        "fact_pack",
    )
    prompt = (
        "You are trail_scout.template_extract.v1.\n"
        "Extract template-level excursion information from a Telegram post that has no concrete future occurrence yet.\n"
        "Return only JSON with key occurrences.\n"
        "Rules:\n"
        "- no future date means digest_eligible must stay false\n"
        "- if the same post also contains concrete dated occurrence blocks for this route, do not create a duplicate template occurrence\n"
        "- volunteer cleanups, subbotniks, restoration work days, community service, lectures without a guided route, and generic meetups are not template excursions unless a guided route/walk/tour is the primary reusable offer\n"
        "- use template_hint for reusable route/topic information\n"
        "- OCR may contribute reusable route/topic facts only when they are explicit on the poster/image\n"
        "- do not invent schedule facts\n"
        "- no extra commentary or hidden thinking traces\n\n"
        f"Input:\n{json.dumps({'source': compact_source, 'screen': compact_screen, 'post': compact_post}, ensure_ascii=False)}"
    )
    data = await ask_gemma(
        EXTRACT_MODEL,
        prompt,
        consumer="guide_scout_template_extract",
        max_output_tokens=320,
        response_schema=schema,
    )
    return _coerce_occurrence_items(data)


def _clean_occurrence_payload(
    item: dict[str, Any],
    *,
    post: ScannedPost,
    source_payload: dict[str, Any],
    screen: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    title = collapse_ws(item.get("canonical_title"))
    if not title:
        return None
    title_normalized = collapse_ws(item.get("title_normalized")) or normalize_title_key(title)
    if not title_normalized:
        return None
    date_iso = collapse_ws(item.get("date") or item.get("date_iso")) or None
    time_text = collapse_ws(item.get("time") or item.get("time_text")) or None
    route_summary = collapse_ws(item.get("route_summary")) or None
    city = collapse_ws(item.get("city")) or None
    meeting_point = collapse_ws(item.get("meeting_point")) or None
    summary_one_liner = collapse_ws(item.get("summary_one_liner")) or None
    base_region_fit = (
        collapse_ws(item.get("base_region_fit"))
        or collapse_ws((screen or {}).get("base_region_fit"))
        or "unknown"
    )
    if base_region_fit == "outside":
        return None
    availability_mode = collapse_ws(item.get("availability_mode")) or "scheduled_public"
    status = collapse_ws(item.get("status")) or "scheduled"
    digest_eligible, digest_reason = _normalize_digest_eligibility(
        date_iso=date_iso,
        availability_mode=availability_mode,
        status=status,
        digest_eligible=_boolish(item.get("digest_eligible")),
        digest_reason=collapse_ws(item.get("digest_eligibility_reason")) or None,
    )
    out = {
        "canonical_title": title,
        "title_normalized": title_normalized,
        "date": date_iso,
        "time": time_text,
        "duration_text": collapse_ws(item.get("duration_text")) or None,
        "city": city,
        "meeting_point": meeting_point,
        "route_summary": route_summary,
        "audience_fit": _string_list_value(item.get("audience_fit"), limit=8),
        "group_format": collapse_ws(item.get("group_format")) or None,
        "price_text": collapse_ws(item.get("price_text")) or None,
        "booking_text": collapse_ws(item.get("booking_text")) or None,
        "booking_url": collapse_ws(item.get("booking_url")) or None,
        "channel_url": collapse_ws(item.get("channel_url")) or post.source_url,
        "status": status,
        "seats_text": collapse_ws(item.get("seats_text")) or None,
        "summary_one_liner": summary_one_liner,
        "digest_blurb": collapse_ws(item.get("digest_blurb")) or None,
        "digest_eligible": digest_eligible,
        "digest_eligibility_reason": digest_reason,
        "is_last_call": _boolish(item.get("is_last_call")),
        "post_kind": collapse_ws(item.get("post_kind")) or "announce_single",
        "availability_mode": availability_mode,
        "guide_names": _string_list_value(item.get("guide_names"), limit=4),
        "organizer_names": _string_list_value(item.get("organizer_names"), limit=4),
        "source_block_id": collapse_ws(item.get("source_block_id")) or None,
        "base_region_fit": base_region_fit,
        "source_fingerprint": collapse_ws(item.get("source_fingerprint")) or build_source_fingerprint(title_normalized=title_normalized, date_iso=date_iso, time_text=time_text),
        "fact_pack": item.get("fact_pack") if isinstance(item.get("fact_pack"), dict) else {},
        "fact_claims": item.get("fact_claims") if isinstance(item.get("fact_claims"), list) else [],
        "template_hint": item.get("template_hint") if isinstance(item.get("template_hint"), dict) else {},
        "profile_hint": item.get("profile_hint") if isinstance(item.get("profile_hint"), dict) else {},
    }
    if not out["fact_pack"]:
        out["fact_pack"] = {}
    out["fact_pack"]["base_region_fit"] = base_region_fit
    if route_summary and "route_summary" not in out["fact_pack"]:
        out["fact_pack"]["route_summary"] = route_summary
    if out["duration_text"] and "duration_text" not in out["fact_pack"]:
        out["fact_pack"]["duration_text"] = out["duration_text"]
    if out["group_format"] and "group_format" not in out["fact_pack"]:
        out["fact_pack"]["group_format"] = out["group_format"]
    return out


async def _extract_occurrence_block(
    source_payload: dict[str, Any],
    *,
    post: ScannedPost,
    flags: dict[str, Any],
    screen: dict[str, Any],
    block: dict[str, Any],
    ocr_chunks: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    compact_source = _compact_source_payload(source_payload)
    compact_screen = _compact_screen_payload(screen)
    compact_post = _compact_post_payload(post, flags=flags, for_extract=True, ocr_chunks=ocr_chunks)
    compact_post_for_block = {
        "message_id": compact_post.get("message_id"),
        "post_date_utc": compact_post.get("post_date_utc"),
        "message_url": compact_post.get("message_url"),
        "ocr_chunks": compact_post.get("ocr_chunks") or [],
        "media_hints": compact_post.get("media_hints") or {},
        "prefilter_flags": compact_post.get("prefilter_flags") or {},
    }
    schema = _single_occurrence_wrapper_schema(
        "source_block_id",
        "canonical_title",
        "title_normalized",
        "date",
        "time",
        "duration_text",
        "city",
        "meeting_point",
        "route_summary",
        "audience_fit",
        "group_format",
        "price_text",
        "booking_text",
        "booking_url",
        "channel_url",
        "status",
        "seats_text",
        "digest_eligible",
        "digest_eligibility_reason",
        "is_last_call",
        "post_kind",
        "availability_mode",
        "guide_names",
        "organizer_names",
        "base_region_fit",
        "fact_pack",
    )
    prompt = (
        "You are trail_scout.announce_extract_tier1.v1.\n"
        "Extract Tier 1 guide-excursion facts for exactly one candidate occurrence block from a multi-announcement Telegram post.\n"
        "Return only JSON with key occurrence.\n"
        "If the block is not a real public or template-like excursion signal inside the base region, return {\"occurrence\": {}}.\n"
        "Rules:\n"
        "- treat the block as one primary excursion candidate\n"
        "- materialize only if the block is primarily a guided public excursion/walk/tour/route or direct update about one\n"
        "- do not materialize volunteer cleanups, subbotniks, restoration work days, community service, lectures without a guided route, or generic meetups unless a guided excursion/walk/tour is the primary public offer\n"
        "- if title/date/route signal is present, materialize the occurrence even when some details are still pending\n"
        "- numeric emoji keycaps in dates are normal digits: 3️⃣ мая means 3 мая; 1️⃣3️⃣ мая means 13 мая; use occurrence_block.schedule_anchor_text only as a normalized reading aid\n"
        "- if the block has a future date/time and no closed/sold-out/cancelled marker, set status=available, availability_mode=scheduled_public, digest_eligible=true\n"
        "- if the block is explicitly tentative/preliminary/only hoped-for or says it is just a free date to move another walk into, do not mark it digest-ready; use digest_eligible=false with digest_eligibility_reason=tentative_or_free_date\n"
        "- if the block says places are gone/sold out/full/cancelled, set the matching unavailable status and digest_eligible=false\n"
        "- title_normalized must be a short stable route identity core; do not include guide names, organizer/source labels, parentheses, dates, times, marketing suffixes, or availability words there\n"
        "- OCR may rescue missing title/date/time facts only when they are explicit on the poster/image\n"
        "- ignore unrelated side notes inside the same block\n"
        "- set base_region_fit per occurrence by your own judgement versus source.base_region (inside|outside|ambiguous|unknown); do not rely on keyword matching\n"
        "- do not invent details; keep source_block_id equal to the input block id\n"
        "- no extra commentary or hidden thinking traces\n\n"
        f"Input:\n{json.dumps({'source': compact_source, 'screen': compact_screen, 'post': compact_post_for_block, 'occurrence_block': {'id': block.get('id'), 'text': collapse_ws(block.get('text'))[:900], 'schedule_anchor_text': collapse_ws(block.get('schedule_anchor_text'))[:900], 'has_schedule_anchor': bool(block.get('has_schedule_anchor')), 'has_time_signal': bool(block.get('has_time_signal')), 'looks_detail_pending': bool(block.get('looks_detail_pending'))}}, ensure_ascii=False)}"
    )
    data = await ask_gemma(
        EXTRACT_MODEL,
        prompt,
        consumer="guide_scout_tier1_extract_block",
        max_output_tokens=340,
        response_schema=schema,
    )
    items = _coerce_occurrence_items(data)
    item = items[0] if items else None
    if not isinstance(item, dict):
        return None
    if not collapse_ws(item.get("source_block_id")):
        item["source_block_id"] = block.get("id")
    semantic_patch = await _extract_occurrence_semantics_failopen(
        source_payload,
        post=post,
        flags=flags,
        screen=screen,
        occurrence_seed=item,
        focus_excerpt=_semantic_focus_excerpt(post, source_block_id=block.get("id")),
        ocr_chunks=ocr_chunks,
    )
    return _clean_occurrence_payload(
        _merge_occurrence_layers(item, semantic_patch),
        post=post,
        source_payload=source_payload,
        screen=screen,
    )


async def _extract_occurrence_block_failopen(
    source_payload: dict[str, Any],
    *,
    post: ScannedPost,
    flags: dict[str, Any],
    screen: dict[str, Any],
    block: dict[str, Any],
    ocr_chunks: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    try:
        return await _extract_occurrence_block(
            source_payload,
            post=post,
            flags=flags,
            screen=screen,
            block=block,
            ocr_chunks=ocr_chunks,
        )
    except Exception as exc:
        print(
            (
                "[guide:block_extract:warning] "
                f"message_id={post.message_id} block_id={collapse_ws(block.get('id')) or '-'} "
                f"error={type(exc).__name__}: {exc}"
            ),
            flush=True,
        )
        return None


async def _extract_occurrence_semantics(
    source_payload: dict[str, Any],
    *,
    post: ScannedPost,
    flags: dict[str, Any],
    screen: dict[str, Any],
    occurrence_seed: dict[str, Any],
    focus_excerpt: str,
    ocr_chunks: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    compact_source = _compact_source_payload(source_payload)
    compact_screen = _compact_screen_payload(screen)
    compact_post = {
        "message_id": post.message_id,
        "message_url": post.source_url,
        "post_date_utc": post.post_date.isoformat(),
        "focus_excerpt": collapse_ws(focus_excerpt)[:900],
        "ocr_chunks": [
            {
                "id": item.get("id"),
                "title": collapse_ws(item.get("title"))[:120] or None,
                "text": collapse_ws(item.get("text"))[:280],
                "excursion_signal": bool(item.get("excursion_signal")),
                "schedule_signal": bool(item.get("schedule_signal")),
                "booking_signal": bool(item.get("booking_signal")),
            }
            for item in (ocr_chunks or [])
            if collapse_ws(item.get("text")) or collapse_ws(item.get("title"))
        ][:3],
    }
    seed = {
        "source_block_id": collapse_ws(occurrence_seed.get("source_block_id")),
        "canonical_title": collapse_ws(occurrence_seed.get("canonical_title")),
        "date": collapse_ws(occurrence_seed.get("date")),
        "time": collapse_ws(occurrence_seed.get("time")),
        "city": collapse_ws(occurrence_seed.get("city")),
        "meeting_point": collapse_ws(occurrence_seed.get("meeting_point")),
        "route_summary": collapse_ws(occurrence_seed.get("route_summary")),
        "price_text": collapse_ws(occurrence_seed.get("price_text")),
        "booking_text": collapse_ws(occurrence_seed.get("booking_text")),
        "guide_names": _string_list_value(occurrence_seed.get("guide_names"), limit=4),
        "organizer_names": _string_list_value(occurrence_seed.get("organizer_names"), limit=4),
        "base_region_fit": collapse_ws(occurrence_seed.get("base_region_fit")),
        "post_kind": collapse_ws(occurrence_seed.get("post_kind")),
        "availability_mode": collapse_ws(occurrence_seed.get("availability_mode")),
    }
    schema = _single_occurrence_wrapper_schema(
        "audience_fit",
        "group_format",
        "duration_text",
        "route_summary",
        "city",
        "meeting_point",
        "price_text",
        "booking_text",
        "booking_url",
        "status",
        "seats_text",
        "summary_one_liner",
        "digest_blurb",
        "digest_eligible",
        "digest_eligibility_reason",
        "is_last_call",
        "guide_names",
        "organizer_names",
        "base_region_fit",
        "fact_claims",
        "template_hint",
        "profile_hint",
        "fact_pack",
    )
    prompt = (
        "You are route_weaver.enrich.v1 for guide excursions.\n"
        "Enrich one already extracted occurrence with missing semantic facts from the same Telegram post.\n"
        "Return only JSON with key occurrence.\n"
        "Rules:\n"
        "- do not rename or replace canonical_title/date/time/source_block_id from the seed\n"
        "- fill only facts supported by the focus excerpt or OCR chunks from the same post\n"
        "- preserve the dominant term family from the source: прогулка stays прогулка, экскурсия stays экскурсия\n"
        "- do not downgrade a seed with concrete future date/time/booking/meeting facts to status unknown or digest_eligible=false unless the focus excerpt explicitly says sold out/full/cancelled/past/private\n"
        "- for a concrete future public schedule with no disqualifying status, keep or set status=available, availability_mode=scheduled_public, digest_eligible=true\n"
        "- volunteer cleanups, subbotniks, restoration work days, community service, lectures without a guided route, and generic meetups stay digest_eligible=false unless the guided excursion/walk/tour is the primary public offer\n"
        "- base_region_fit is your own semantic judgement versus source.base_region; do not weaken seed.base_region_fit unless the focus excerpt explicitly contradicts it\n"
        "- summary_one_liner and digest_blurb are optional; leave them empty if evidence is weak\n"
        "- fact_claims may use only claim_role: anchor, support, status_delta, template_hint, guide_profile_hint\n"
        "- no extra commentary or hidden thinking traces\n\n"
        f"Input:\n{json.dumps({'source': compact_source, 'screen': compact_screen, 'post': compact_post, 'occurrence_seed': seed}, ensure_ascii=False)}"
    )
    data = await ask_gemma(
        EXTRACT_MODEL,
        prompt,
        consumer="route_weaver_enrich",
        max_output_tokens=380,
        response_schema=schema,
    )
    items = _coerce_occurrence_items(data)
    item = items[0] if items else None
    return item if isinstance(item, dict) else {}


async def _extract_occurrence_semantics_failopen(
    source_payload: dict[str, Any],
    *,
    post: ScannedPost,
    flags: dict[str, Any],
    screen: dict[str, Any],
    occurrence_seed: dict[str, Any],
    focus_excerpt: str,
    ocr_chunks: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    try:
        return await _extract_occurrence_semantics(
            source_payload,
            post=post,
            flags=flags,
            screen=screen,
            occurrence_seed=occurrence_seed,
            focus_excerpt=focus_excerpt,
            ocr_chunks=ocr_chunks,
        )
    except Exception as exc:
        print(
            (
                "[guide:enrich:warning] "
                f"message_id={post.message_id} block_id={collapse_ws(occurrence_seed.get('source_block_id')) or '-'} "
                f"error={type(exc).__name__}: {exc}"
            ),
            flush=True,
        )
        return {}


async def extract_post(
    source_payload: dict[str, Any],
    post: ScannedPost,
    flags: dict[str, Any],
    screen: dict[str, Any],
    *,
    ocr_chunks: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    occurrence_blocks = build_occurrence_blocks(post.text, limit=6)
    is_multi = str(screen.get("post_kind") or "") == "announce_multi"
    extract_mode = str(screen.get("extract_mode") or "none")
    if extract_mode == "status":
        items = await _extract_status_post(source_payload, post=post, flags=flags, screen=screen, ocr_chunks=ocr_chunks)
    elif extract_mode == "template":
        items = await _extract_template_post(source_payload, post=post, flags=flags, screen=screen, ocr_chunks=ocr_chunks)
    elif is_multi and occurrence_blocks:
        items = await _extract_announce_post_tier1_failopen_for_block_rescue(
            source_payload,
            post=post,
            flags=flags,
            screen=screen,
            ocr_chunks=ocr_chunks,
        )
    else:
        items = await _extract_announce_post_tier1(source_payload, post=post, flags=flags, screen=screen, ocr_chunks=ocr_chunks)
    cleaned: list[dict[str, Any]] = []
    seen_fingerprints: set[str] = set()
    if isinstance(items, list):
        for item in items:
            if not isinstance(item, dict):
                continue
            merged_item = item
            if extract_mode == "announce":
                semantic_patch = await _extract_occurrence_semantics_failopen(
                    source_payload,
                    post=post,
                    flags=flags,
                    screen=screen,
                    occurrence_seed=item,
                    focus_excerpt=_semantic_focus_excerpt(post, source_block_id=item.get("source_block_id")),
                    ocr_chunks=ocr_chunks,
                )
                merged_item = _merge_occurrence_layers(item, semantic_patch)
            occurrence = _clean_occurrence_payload(merged_item, post=post, source_payload=source_payload, screen=screen)
            if occurrence:
                fingerprint = str(occurrence.get("source_fingerprint") or "")
                if fingerprint and fingerprint in seen_fingerprints:
                    continue
                if fingerprint:
                    seen_fingerprints.add(fingerprint)
                cleaned.append(occurrence)
    if is_multi and occurrence_blocks:
        covered_block_ids = {
            collapse_ws(item.get("source_block_id"))
            for item in cleaned
            if collapse_ws(item.get("source_block_id"))
        }
        for block in occurrence_blocks:
            block_id = collapse_ws(block.get("id"))
            if not block_id or block_id in covered_block_ids or not bool(block.get("has_schedule_anchor")):
                continue
            rescued = await _extract_occurrence_block_failopen(
                source_payload,
                post=post,
                flags=flags,
                screen=screen,
                block=block,
                ocr_chunks=ocr_chunks,
            )
            if not rescued:
                continue
            fingerprint = str(rescued.get("source_fingerprint") or "")
            if fingerprint and fingerprint in seen_fingerprints:
                continue
            if fingerprint:
                seen_fingerprints.add(fingerprint)
            cleaned.append(rescued)
            covered_block_ids.add(block_id)
    return cleaned


def _llm_status_from_exception(exc: Exception) -> str:
    rate_limit_error = None
    provider_error = None
    try:
        from google_ai.exceptions import ProviderError, RateLimitError

        provider_error = ProviderError
        rate_limit_error = RateLimitError
    except Exception:
        provider_error = None
        rate_limit_error = None
    if rate_limit_error is not None and isinstance(exc, rate_limit_error):
        blocked = (getattr(exc, "blocked_reason", None) or "unknown").strip() or "unknown"
        return f"llm_deferred_rate_limit:{blocked}"
    if provider_error is not None and isinstance(exc, provider_error) and int(getattr(exc, "status_code", 0) or 0) == 429:
        return "llm_deferred_provider_429"
    if isinstance(exc, asyncio.TimeoutError):
        return "llm_deferred_timeout"
    return f"llm_error:{type(exc).__name__}"


def _summarize_source_stats(posts: list[dict[str, Any]]) -> dict[str, int]:
    llm_ok = 0
    llm_deferred = 0
    llm_error = 0
    for post in posts:
        status = str(post.get("llm_status") or "").strip()
        if status == "ok":
            llm_ok += 1
        elif status.startswith("llm_deferred"):
            llm_deferred += 1
        elif status not in {"", "skipped_prefilter"}:
            llm_error += 1
    return {
        "posts_total": len(posts),
        "prefilter_true": sum(1 for post in posts if bool(post.get("prefilter_passed"))),
        "llm_ok": llm_ok,
        "llm_deferred": llm_deferred,
        "llm_error": llm_error,
        "occurrences_total": sum(len(post.get("occurrences") or []) for post in posts),
    }


def _summarize_run_stats(sources_output: list[dict[str, Any]]) -> dict[str, int]:
    totals = {
        "sources_total": len(sources_output),
        "posts_total": 0,
        "prefilter_true": 0,
        "llm_ok": 0,
        "llm_deferred": 0,
        "llm_error": 0,
        "occurrences_total": 0,
    }
    for source_payload in sources_output:
        stats = source_payload.get("stats") if isinstance(source_payload.get("stats"), dict) else {}
        for key in ("posts_total", "prefilter_true", "llm_ok", "llm_deferred", "llm_error", "occurrences_total"):
            totals[key] += int(stats.get(key) or 0)
    return totals


async def process_source(client: TelegramClient, source_payload: dict[str, Any], *, limit: int, days_back: int) -> dict[str, Any]:
    username = str(source_payload.get("username") or "").strip()
    source_kind = str(source_payload.get("source_kind") or "").strip()
    print(f"[source:start] @{username} kind={source_kind or 'unknown'} limit={limit} days_back={days_back}", flush=True)
    try:
        await ensure_client_connected(client)
        meta, posts = await scan_source_posts(client, username=username, limit=limit, days_back=days_back)
    except Exception as exc:
        if "disconnected" not in str(exc).lower():
            raise
        await ensure_client_connected(client, force_reconnect=True)
        meta, posts = await scan_source_posts(client, username=username, limit=limit, days_back=days_back)
    out_posts: list[dict[str, Any]] = []
    errors: list[str] = []
    for post in posts:
        ocr_chunks: list[dict[str, Any]] = []
        ocr_status = "skipped"
        base_flags = prefilter_flags(post)
        base_pass = prefilter_pass(post, source_kind, base_flags)
        if _should_run_post_ocr(post, source_kind, base_flags, base_pass=base_pass):
            try:
                ocr_chunks = await collect_post_ocr_chunks(
                    client,
                    username=username,
                    post=post,
                    model=EXTRACT_MODEL,
                )
                ocr_status = "ok" if ocr_chunks else "empty"
            except Exception as exc:
                ocr_status = f"error:{type(exc).__name__}"
                print(
                    f"[guide:ocr:error] source=@{username} message_id={post.message_id} error={type(exc).__name__}: {exc}",
                    flush=True,
                )
        flags = prefilter_flags(post, ocr_chunks=ocr_chunks)
        passes = prefilter_pass(post, source_kind, flags)
        media_assets = await materialize_post_media_assets(client, username=username, post=post) if passes else []
        payload: dict[str, Any] = {
            "message_id": post.message_id,
            "grouped_id": post.grouped_id,
            "post_date": post.post_date.isoformat(),
            "source_url": post.source_url,
            "text": post.text,
            "views": post.views,
            "forwards": post.forwards,
            "reactions_total": post.reactions_total,
            "reactions_json": post.reactions_json,
            "media_refs": post.media_refs,
            "media_assets": media_assets,
            "ocr_chunks": ocr_chunks,
            "ocr_status": ocr_status,
            "prefilter_passed": passes,
            "prefilter_flags": flags,
            "llm_status": "skipped_prefilter",
            "screen": {
                "decision": "ignore",
                "post_kind": "mixed_or_non_target",
                "extract_mode": "none",
                "digest_eligible_default": "mixed",
                "contains_future_public_signal": False,
                "contains_past_report_signal": False,
                "reasons": ["prefilter_false"],
                "confidence": "low",
            },
            "occurrences": [],
        }
        if passes:
            try:
                screen = await screen_post(
                    {
                        "username": username,
                        "source_kind": source_kind,
                        "title": meta.get("source_title"),
                        "display_name": source_payload.get("display_name"),
                        "marketing_name": source_payload.get("marketing_name"),
                        "base_region": source_payload.get("base_region"),
                        "trust_level": source_payload.get("trust_level"),
                        "flags": source_payload.get("flags"),
                    },
                    post,
                    flags,
                    ocr_chunks=ocr_chunks,
                )
                payload["screen"] = screen
                if screen.get("extract_mode") != "none" and screen.get("decision") != "ignore":
                    payload["occurrences"] = await extract_post(
                        {
                            "username": username,
                            "source_kind": source_kind,
                            "title": meta.get("source_title"),
                            "display_name": source_payload.get("display_name"),
                            "marketing_name": source_payload.get("marketing_name"),
                            "base_region": source_payload.get("base_region"),
                            "trust_level": source_payload.get("trust_level"),
                            "flags": source_payload.get("flags"),
                        },
                        post,
                        flags,
                        screen,
                        ocr_chunks=ocr_chunks,
                    )
                payload["llm_status"] = "ok"
            except Exception as exc:
                error_kind = _llm_status_from_exception(exc)
                payload["llm_status"] = error_kind
                payload["screen"] = {
                    "decision": "ignore",
                    "post_kind": "mixed_or_non_target",
                    "extract_mode": "none",
                    "digest_eligible_default": "mixed",
                    "contains_future_public_signal": False,
                    "contains_past_report_signal": False,
                    "reasons": [error_kind],
                    "confidence": "low",
                }
                errors.append(f"message_id={post.message_id}: {error_kind}: {exc}")
        out_posts.append(payload)
    source_status = "partial" if errors else "ok"
    print(
        (
            f"[source:done] @{username} status={source_status} "
            f"posts={len(posts)} extracted={sum(len((item.get('occurrences') or [])) for item in out_posts)} "
            f"errors={len(errors)}"
        ),
        flush=True,
    )
    stats = _summarize_source_stats(out_posts)
    return {
        "username": username,
        "source_title": meta.get("source_title"),
        "source_kind": source_kind,
        "about_text": meta.get("about_text"),
        "about_links": meta.get("about_links"),
        "source_status": source_status,
        "posts_scanned": len(posts),
        "stats": stats,
        "errors": errors,
        "posts": out_posts,
    }


async def main() -> None:
    config = load_runtime_config()
    _bootstrap_repo_bundle()
    refresh_runtime_settings()
    run_id = str(config.get("run_id") or f"guide_kaggle_{int(datetime.now(timezone.utc).timestamp())}")
    started_at = datetime.now(timezone.utc).isoformat()
    sources = [item for item in (config.get("sources") or []) if isinstance(item, dict)]
    print(
        (
            f"Guide monitor run_id={run_id} mode={config.get('scan_mode') or 'full'} "
            f"sources={len(sources)} limit={int(config.get('limit_per_source') or 25)} "
            f"days_back={int(config.get('days_back') or 7)} "
            f"screen_model={SCREEN_MODEL} extract_model={EXTRACT_MODEL}"
        ),
        flush=True,
    )
    client = await create_client()
    partial = False
    sources_output: list[dict[str, Any]] = []
    try:
        limit = int(config.get("limit_per_source") or 25)
        days_back = int(config.get("days_back") or 7)
        for source in sources:
            if not isinstance(source, dict):
                continue
            try:
                await ensure_client_connected(client)
                result = await process_source(client, source, limit=limit, days_back=days_back)
            except Exception as exc:
                partial = True
                result = {
                    "username": str(source.get("username") or ""),
                    "source_title": str(source.get("title") or "") or None,
                    "source_kind": str(source.get("source_kind") or "") or None,
                    "source_status": "error",
                    "posts_scanned": 0,
                    "errors": [f"{type(exc).__name__}: {exc}"],
                    "posts": [],
                }
            if result.get("source_status") != "ok":
                partial = True
            sources_output.append(result)
    finally:
        await client.disconnect()
    stats = _summarize_run_stats(sources_output)
    payload = {
        "schema_version": 1,
        "run_id": run_id,
        "scan_mode": str(config.get("scan_mode") or "full"),
        "started_at": started_at,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "partial": partial,
        "stats": stats,
        "sources": sources_output,
    }
    RESULT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        (
            f"Guide monitor completed partial={partial} "
            f"sources={len(sources_output)} "
            f"posts={sum(int(item.get('posts_scanned') or 0) for item in sources_output)}"
        ),
        flush=True,
    )
    print(
        (
            "Guide monitor stats "
            f"posts_total={stats['posts_total']} "
            f"prefilter_true={stats['prefilter_true']} "
            f"llm_ok={stats['llm_ok']} "
            f"llm_deferred={stats['llm_deferred']} "
            f"llm_error={stats['llm_error']} "
            f"occurrences_total={stats['occurrences_total']}"
        ),
        flush=True,
    )
    print(f"Wrote {RESULT_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
