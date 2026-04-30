from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import importlib.util
import json
import logging
import os
import random
import re
import statistics
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta, date
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('telegram_monitor')

SCRIPT_DIR = Path(globals().get('__file__', Path.cwd() / 'telegram_monitor.py')).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))


def bootstrap_google_ai_bundle() -> None:
    try:
        if importlib.util.find_spec('google_ai') is not None:
            return
    except Exception:
        pass

    candidate_roots = [SCRIPT_DIR, Path.cwd(), Path('/kaggle/working')]
    seen: set[str] = set()
    for root in candidate_roots:
        root_str = str(root)
        if root_str in seen:
            continue
        seen.add(root_str)
        if (root / 'google_ai' / '__init__.py').exists():
            sys.path.insert(0, root_str)
            logger.info('tg_monitor.google_ai bootstrap root=%s', root_str)
            return

    kaggle_input = Path('/kaggle/input')
    if kaggle_input.exists():
        for init_path in kaggle_input.rglob('__init__.py'):
            if init_path.parent.name != 'google_ai':
                continue
            bundle_root = init_path.parent.parent
            bundle_root_str = str(bundle_root)
            if bundle_root_str not in sys.path:
                sys.path.insert(0, bundle_root_str)
            logger.info('tg_monitor.google_ai bootstrap input_root=%s', bundle_root_str)
            return


bootstrap_google_ai_bundle()


def ensure_libs() -> None:
    modules = [
        ("telethon", "telethon"),
        ("google.generativeai", "google-generativeai"),
        ("cryptography", "cryptography"),
        ("supabase", "supabase"),
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


ensure_libs()

from PIL import Image
import imagehash
from google_ai import GoogleAIClient, SecretsProvider
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import Channel, Chat, MessageEntityCustomEmoji, MessageEntityTextUrl, MessageEntityUrl, PeerChannel, PeerChat, PeerUser, User
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.messages import GetFullChatRequest
from telethon.helpers import add_surrogate, del_surrogate
from telethon.errors import FloodWaitError, AuthKeyDuplicatedError, SessionRevokedError
import telethon

logger.info('tg_monitor.telethon version=%s', getattr(telethon, '__version__', 'unknown'))


KAGGLE_INPUT = Path('/kaggle/input')

def _find_file(filename: str) -> Path | None:
    if not KAGGLE_INPUT.exists():
        return None
    for path in KAGGLE_INPUT.rglob(filename):
        if path.is_file():
            return path
    return None

def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding='utf-8'))

def load_config() -> dict:
    path = _find_file('config.json')
    if not path:
        raise RuntimeError('config.json not found in /kaggle/input')
    return _load_json(path)

def load_secrets() -> dict:
    enc_path = _find_file('secrets.enc')
    key_path = _find_file('fernet.key')
    if not enc_path or not key_path:
        raise RuntimeError('secrets.enc/fernet.key not found in /kaggle/input')
    from cryptography.fernet import Fernet
    fernet = Fernet(key_path.read_bytes().strip())
    decrypted = fernet.decrypt(enc_path.read_bytes())
    return json.loads(decrypted.decode('utf-8'))

config = load_config()
secrets = load_secrets()
logger.info('tg_monitor.secrets_keys=%s', sorted((secrets or {}).keys()))
if 'TELEGRAM_AUTH_BUNDLE_S22' in (secrets or {}):
    logger.info('tg_monitor.bundle_len=%s', len(str((secrets or {}).get('TELEGRAM_AUTH_BUNDLE_S22') or ''))) 
for k, v in (secrets or {}).items():
    if not k or v in (None, ""):
        continue
    os.environ[k] = str(v)

AUTH_BUNDLE_B64 = (os.getenv('TELEGRAM_AUTH_BUNDLE_S22') or '').strip()
ALLOW_TG_SESSION = os.getenv("TG_MONITORING_ALLOW_TG_SESSION", "0") == "1"
bundle = None
TG_SESSION = ''
DEVICE_CONFIG = {
    'device_model': 'Samsung S22 Ultra',
    'system_version': '13.0',
    'app_version': '9.6.6',
}

if AUTH_BUNDLE_B64:
    try:
        raw = base64.urlsafe_b64decode(AUTH_BUNDLE_B64.encode('ascii')).decode('utf-8')
        bundle = json.loads(raw)
    except Exception as exc:
        raise RuntimeError(f'Invalid TELEGRAM_AUTH_BUNDLE_S22: {exc}')
    required_keys = [
        'session',
        'device_model',
        'system_version',
        'app_version',
        'lang_code',
        'system_lang_code',
    ]
    missing = [key for key in required_keys if not bundle.get(key)]
    if missing:
        raise RuntimeError(f"TELEGRAM_AUTH_BUNDLE_S22 missing keys: {', '.join(missing)}")
    TG_SESSION = bundle['session']
    os.environ.pop('TG_SESSION', None)
    DEVICE_CONFIG = {
        'device_model': bundle['device_model'],
        'system_version': bundle['system_version'],
        'app_version': bundle['app_version'],
        'lang_code': bundle['lang_code'],
        'system_lang_code': bundle['system_lang_code'],
    }
else:
    if not ALLOW_TG_SESSION:
        raise RuntimeError('TELEGRAM_AUTH_BUNDLE_S22 is required for Kaggle monitoring. Set TG_MONITORING_ALLOW_TG_SESSION=1 to use TG_SESSION.')
    TG_SESSION = os.getenv('TG_SESSION', '')

TG_API_ID = os.getenv('TG_API_ID', '')
TG_API_HASH = os.getenv('TG_API_HASH', '')

DEFAULT_TG_MONITORING_TEXT_MODEL = 'models/gemma-4-31b-it'
DEFAULT_TG_MONITORING_VISION_MODEL = 'models/gemma-4-31b-it'
GOOGLE_KEY_ENV = (os.getenv('TG_MONITORING_GOOGLE_KEY_ENV') or 'GOOGLE_API_KEY3').strip() or 'GOOGLE_API_KEY3'
GOOGLE_FALLBACK_KEY_ENV = (os.getenv('TG_MONITORING_GOOGLE_FALLBACK_KEY_ENV') or GOOGLE_KEY_ENV).strip() or GOOGLE_KEY_ENV
GOOGLE_ACCOUNT_ENV = (os.getenv('TG_MONITORING_GOOGLE_ACCOUNT_ENV') or 'GOOGLE_API_LOCALNAME3').strip() or 'GOOGLE_API_LOCALNAME3'
GOOGLE_ACCOUNT_FALLBACK_ENV = (os.getenv('TG_MONITORING_GOOGLE_ACCOUNT_FALLBACK_ENV') or GOOGLE_ACCOUNT_ENV).strip() or GOOGLE_ACCOUNT_ENV
PRIMARY_GOOGLE_API_KEY = (os.getenv(GOOGLE_KEY_ENV) or '').strip()
FALLBACK_GOOGLE_API_KEY = (os.getenv(GOOGLE_FALLBACK_KEY_ENV) or '').strip()

if not TG_SESSION or not TG_API_ID or not TG_API_HASH:
    raise RuntimeError('Missing TG credentials after secrets load')
if not (PRIMARY_GOOGLE_API_KEY or FALLBACK_GOOGLE_API_KEY):
    raise RuntimeError(f'Missing {GOOGLE_KEY_ENV}/{GOOGLE_FALLBACK_KEY_ENV} after secrets load')

logger.info(
    'tg_monitor.secrets tg_session_len=%s tg_api_id_set=%s tg_api_hash_set=%s google_key_env=%s primary_key_set=%s fallback_key_set=%s google_account_env=%s bundle_set=%s',
    len(TG_SESSION) if TG_SESSION else 0,
    bool(TG_API_ID),
    bool(TG_API_HASH),
    GOOGLE_KEY_ENV,
    bool(PRIMARY_GOOGLE_API_KEY),
    bool(FALLBACK_GOOGLE_API_KEY),
    GOOGLE_ACCOUNT_ENV,
    bool(AUTH_BUNDLE_B64),
)

# Gemma models
TEXT_MODEL = (os.getenv('TG_MONITORING_TEXT_MODEL') or DEFAULT_TG_MONITORING_TEXT_MODEL).strip()
VISION_MODEL = (os.getenv('TG_MONITORING_VISION_MODEL') or os.getenv('TG_MONITORING_TEXT_MODEL') or DEFAULT_TG_MONITORING_VISION_MODEL).strip()
FALLBACK_TEXT_MODEL = (os.getenv('TG_MONITORING_TEXT_MODEL_FALLBACK') or '').strip()
FALLBACK_VISION_MODEL = (os.getenv('TG_MONITORING_VISION_MODEL_FALLBACK') or '').strip()
LLM_CALL_TIMEOUT_SECONDS = float(
    (os.getenv('TG_MONITORING_LLM_TIMEOUT_SECONDS') or os.getenv('GOOGLE_AI_PROVIDER_TIMEOUT_SEC') or '45').strip()
    or '45'
)
if LLM_CALL_TIMEOUT_SECONDS > 0:
    os.environ.setdefault('GOOGLE_AI_PROVIDER_TIMEOUT_SEC', str(LLM_CALL_TIMEOUT_SECONDS))

# Scan limits
MAX_MESSAGES_PER_SOURCE = int(os.getenv('TG_MONITORING_LIMIT', '50'))
MAX_DAYS_BACK = int(os.getenv('TG_MONITORING_DAYS_BACK', '3'))
MAX_IMAGES_PER_MESSAGE = int(os.getenv('TG_MONITORING_MAX_IMAGES', '4'))
MAX_EVENTS_PER_MESSAGE = int(os.getenv('TG_MONITORING_MAX_EVENTS_PER_MESSAGE', '8'))
ENABLE_OCR = os.getenv('TG_MONITORING_ENABLE_OCR', '1') == '1'

# Human-like delays
HUMAN_DELAY_MIN = float(os.getenv('TG_MONITORING_DELAY_MIN', '0.8'))
HUMAN_DELAY_MAX = float(os.getenv('TG_MONITORING_DELAY_MAX', '2.2'))
HUMAN_LONG_PAUSE_EVERY = int(os.getenv('TG_MONITORING_LONG_PAUSE_EVERY', '7'))
HUMAN_LONG_PAUSE_MIN = float(os.getenv('TG_MONITORING_LONG_PAUSE_MIN', '4'))
HUMAN_LONG_PAUSE_MAX = float(os.getenv('TG_MONITORING_LONG_PAUSE_MAX', '9'))
SOURCE_PAUSE_MIN = float(os.getenv('TG_MONITORING_SOURCE_PAUSE_MIN', '2'))
SOURCE_PAUSE_MAX = float(os.getenv('TG_MONITORING_SOURCE_PAUSE_MAX', '6'))
# Media download throttling (helps avoid Telethon FloodWait on busy channels)
MAX_MEDIA_PER_SOURCE = int(os.getenv('TG_MONITORING_MEDIA_MAX_PER_SOURCE', '12'))
HUMAN_MEDIA_DELAY_MIN = float(os.getenv('TG_MONITORING_MEDIA_DELAY_MIN', '1.2'))
HUMAN_MEDIA_DELAY_MAX = float(os.getenv('TG_MONITORING_MEDIA_DELAY_MAX', '3.0'))

# Telethon FloodWait handling
TG_FLOOD_SLEEP_THRESHOLD = int(os.getenv('TG_MONITORING_FLOOD_SLEEP_THRESHOLD', '600'))
TG_FLOOD_WAIT_MAX = int(os.getenv('TG_MONITORING_FLOOD_WAIT_MAX', '1800'))
TG_FLOOD_WAIT_JITTER_MIN = float(os.getenv('TG_MONITORING_FLOOD_WAIT_JITTER_MIN', '6'))
TG_FLOOD_WAIT_JITTER_MAX = float(os.getenv('TG_MONITORING_FLOOD_WAIT_JITTER_MAX', '18'))
TG_FLOOD_MAX_RETRIES = int(os.getenv('TG_MONITORING_FLOOD_MAX_RETRIES', '4'))

# Gemma rate limits (single limiter for all requests)
RATE_RPM = int(os.getenv('TG_GEMMA_RPM', '30'))
RATE_TPM = int(os.getenv('TG_GEMMA_TPM', '15000'))
RATE_RPD = int(os.getenv('TG_GEMMA_RPD', '14400'))
RATE_MINUTE_MARGIN = float(os.getenv('TG_GEMMA_MINUTE_MARGIN', '0.45'))
RATE_DAILY_MARGIN = float(os.getenv('TG_GEMMA_DAILY_MARGIN', '0.85'))

logger.info(
    'tg_monitor.config sources=%d run_id=%s',
    len(config.get('sources') or []),
    config.get('run_id') or 'auto',
)
logger.info(
    'tg_monitor.limits max_messages=%d max_days_back=%d max_images=%d ocr=%s',
    MAX_MESSAGES_PER_SOURCE,
    MAX_DAYS_BACK,
    MAX_IMAGES_PER_MESSAGE,
    ENABLE_OCR,
)
for src in config.get('sources') or []:
    logger.info(
        'tg_monitor.source_config username=%s last_id=%s default_location=%s trust_level=%s',
        src.get('username'),
        src.get('last_scanned_message_id'),
        src.get('default_location'),
        src.get('trust_level'),
    )


@dataclass
class RateLimitConfig:
    rpm: int = RATE_RPM
    tpm: int = RATE_TPM
    rpd: int = RATE_RPD
    minute_margin: float = RATE_MINUTE_MARGIN
    daily_margin: float = RATE_DAILY_MARGIN

    @property
    def effective_rpm(self) -> int:
        return int(self.rpm * (1 - self.minute_margin))

    @property
    def effective_tpm(self) -> int:
        return int(self.tpm * (1 - self.minute_margin))

    @property
    def effective_rpd(self) -> int:
        return int(self.rpd * (1 - self.daily_margin))


class TokenBucket:
    def __init__(self, capacity: int, refill_rate: float):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = capacity
        self.last_refill = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now

    def consume(self, tokens: int = 1) -> bool:
        self._refill()
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False

    def wait_time(self, tokens: int = 1) -> float:
        self._refill()
        if self.tokens >= tokens:
            return 0.0
        needed = tokens - self.tokens
        return needed / self.refill_rate


class GemmaRateLimiter:
    def __init__(self, config: RateLimitConfig | None = None):
        self.config = config or RateLimitConfig()
        self._rpm_bucket = TokenBucket(
            capacity=self.config.effective_rpm,
            refill_rate=self.config.effective_rpm / 60.0,
        )
        self._tpm_bucket = TokenBucket(
            capacity=self.config.effective_tpm,
            refill_rate=self.config.effective_tpm / 60.0,
        )
        self._daily_requests = 0
        self._last_reset_day: str | None = None

    def _check_daily_reset(self) -> None:
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        if self._last_reset_day != today:
            self._daily_requests = 0
            self._last_reset_day = today

    async def wait_if_needed(self, estimated_tokens: int) -> None:
        self._check_daily_reset()
        if self._daily_requests >= self.config.effective_rpd:
            logger.warning('Gemma daily request limit reached: %s', self.config.effective_rpd)
        while True:
            rpm_wait = self._rpm_bucket.wait_time(1)
            if rpm_wait <= 0:
                break
            await asyncio.sleep(min(rpm_wait, 5.0))
        while True:
            tpm_wait = self._tpm_bucket.wait_time(estimated_tokens)
            if tpm_wait <= 0:
                break
            await asyncio.sleep(min(tpm_wait, 5.0))
        self._rpm_bucket.consume(1)
        self._tpm_bucket.consume(estimated_tokens)
        self._daily_requests += 1

    def acquire(self, estimated_tokens: int = 500):
        return RateLimitContext(self, estimated_tokens)


class RateLimitContext:
    def __init__(self, limiter: GemmaRateLimiter, estimated_tokens: int):
        self._limiter = limiter
        self._estimated_tokens = estimated_tokens

    async def __aenter__(self):
        await self._limiter.wait_if_needed(self._estimated_tokens)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False


rate_limiter = GemmaRateLimiter()


async def human_sleep(min_s: float, max_s: float) -> None:
    delay = random.uniform(min_s, max_s)
    if random.random() < 0.12:
        delay += random.uniform(0.8, 2.5)
    await asyncio.sleep(delay)


async def _sleep_flood(wait_seconds: int, label: str, attempt: int) -> bool:
    wait_s = int(wait_seconds or 0)
    if wait_s <= 0:
        wait_s = 1
    if wait_s > TG_FLOOD_WAIT_MAX:
        logger.error(
            'tg_monitor.floodwait_abort label=%s wait=%ss max=%ss',
            label,
            wait_s,
            TG_FLOOD_WAIT_MAX,
        )
        return False
    jitter = random.uniform(TG_FLOOD_WAIT_JITTER_MIN, TG_FLOOD_WAIT_JITTER_MAX)
    total = wait_s + jitter
    logger.warning(
        'tg_monitor.floodwait label=%s wait=%ss total=%.1fs attempt=%d',
        label,
        wait_s,
        total,
        attempt,
    )
    await asyncio.sleep(total)
    return True


async def tg_call(label: str, func, *args, **kwargs):
    attempt = 0
    while True:
        attempt += 1
        try:
            return await func(*args, **kwargs)
        except FloodWaitError as e:
            ok = await _sleep_flood(getattr(e, 'seconds', 0), label, attempt)
            if not ok or attempt >= TG_FLOOD_MAX_RETRIES:
                raise
            continue
        except AuthKeyDuplicatedError:
            logger.error('tg_monitor.auth_key_duplicated label=%s', label)
            raise
        except SessionRevokedError:
            logger.error('tg_monitor.session_revoked label=%s', label)
            raise


def _estimate_tokens(text: str, has_images: bool = False) -> int:
    if not text:
        return 200
    base = max(200, len(text) // 4)
    if has_images:
        base += 800
    return base


def _safe_json(text: str):
    if not text:
        return None
    raw = text.strip()
    if raw.startswith('```'):
        raw = re.sub(r'^```[a-zA-Z]*\n?', '', raw).strip()
        if raw.endswith('```'):
            raw = raw[:-3].strip()
    start = min([i for i in [raw.find('{'), raw.find('[')] if i != -1] or [-1])
    end = max(raw.rfind('}'), raw.rfind(']'))
    if start != -1 and end != -1 and end > start:
        raw = raw[start:end+1]
    try:
        return json.loads(raw)
    except Exception:
        return None


def _is_not_found(exc: Exception) -> bool:
    msg = str(exc).lower()
    return 'not found' in msg or '404' in msg

_GIVEAWAY_RE = re.compile(r"\b(розыгрыш|разыгрыва\w*|розыгра\w*|выигра\w*|конкурс|giveaway)\b", re.IGNORECASE)
_TICKETS_RE = re.compile(r"\b(билет\w*|пригласительн\w*|абонемент\w*)\b", re.IGNORECASE)


def strip_custom_emoji_entities(text: str, entities) -> str:
    # Remove Telegram custom emoji ranges using UTF-16 offsets (Telethon API).
    if not text or not entities:
        return text or ''
    safe = add_surrogate(text)
    spans = []
    for ent in entities or []:
        if isinstance(ent, MessageEntityCustomEmoji):
            try:
                start = int(getattr(ent, 'offset', 0))
                length = int(getattr(ent, 'length', 0))
            except Exception:
                continue
            if length > 0:
                spans.append((start, start + length))
    if not spans:
        return text
    spans.sort()
    merged = []
    for start, end in spans:
        if not merged or start > merged[-1][1]:
            merged.append([start, end])
        else:
            merged[-1][1] = max(merged[-1][1], end)
    out = []
    last = 0
    for start, end in merged:
        if start > last:
            out.append(safe[last:start])
        # Keep length stable for other entity offsets: replace removed range with spaces.
        out.append(' ' * max(0, end - start))
        last = max(last, end)
    out.append(safe[last:])
    return del_surrogate(''.join(out))


def is_ticket_giveaway(text: str | None, ocr_text: str | None = None) -> bool:
    combined = ((text or '') + '\n' + (ocr_text or '')).strip()
    if not combined:
        return False
    return bool(_GIVEAWAY_RE.search(combined) and _TICKETS_RE.search(combined))

_GIVEAWAY_LINE_RE = re.compile(
    r"\b(розыгрыш|разыгрыва\w*|розыгра\w*|выигра\w*|конкурс|giveaway|"
    r"услови\w*|правил\w*|репост\w*|подпис\w*|отмет\w*|коммент\w*|лайк\w*|"
    r"итог\w*|победит\w*|случайн\w*)\b",
    re.IGNORECASE,
)

def _has_datetime_signals(text: str | None) -> bool:
    if not text:
        return False
    value = str(text).lower()
    if re.search(r"\b\d{1,2}[:.]\d{2}\b", value):
        return True
    if re.search(r"\b\d{1,2}[./]\d{1,2}\b", value):
        return True
    if re.search(r"\b(январ|феврал|март|апрел|ма[йя]|июн|июл|август|сентябр|октябр|ноябр|декабр)\w*\b", value):
        return True
    return False

def strip_giveaway_lines(text: str | None) -> str:
    # Keep event facts, drop giveaway mechanics.
    if not text:
        return ''
    kept = []
    for line in str(text).splitlines():
        if _GIVEAWAY_LINE_RE.search(line):
            if _has_datetime_signals(line):
                kept.append(line)
            continue
        kept.append(line)
    return '\n'.join(kept).strip()

_PROMO_STRIP_RE = re.compile(r"\b(акци(?:я|и|ю|ях)|скидк\w*|промокод\w*|спецпредложен\w*|бонус\w*|кэшбек\w*|кэшбэк\w*|кэшбэ\w*|подарок\w*|сертификат\w*)\b", re.IGNORECASE)
_CONGRATS_RE = re.compile(r"\b(поздравля\w*|с\s+дн[её]м\s+рождени\w*|юбиле\w*)\b", re.IGNORECASE)
_CONGRATS_CONTEXT_RE = re.compile(r"\b(ближайш\w*|спектакл\w*|концерт\w*|мероприят\w*|событи\w*)\b", re.IGNORECASE)

def is_promo_or_congrats(text: str | None, ocr_text: str | None = None) -> bool:
    combined = ((text or '') + '\n' + (ocr_text or '')).strip()
    if not combined:
        return False
    if _CONGRATS_RE.search(combined) and (_CONGRATS_CONTEXT_RE.search(combined) or '|' in combined):
        return True
    return False

def strip_promo_lines(text: str | None) -> str:
    if not text:
        return ''
    lines = []
    for line in str(text).splitlines():
        if _PROMO_STRIP_RE.search(line):
            continue
        lines.append(line)
    return '\n'.join(lines).strip()


MODEL_REGISTRY = {
    'text': {
        'name': TEXT_MODEL,
        'fallback': FALLBACK_TEXT_MODEL,
    },
    'vision': {
        'name': VISION_MODEL,
        'fallback': FALLBACK_VISION_MODEL,
    },
}

SUPABASE_URL = os.getenv('SUPABASE_URL', '').strip()
SUPABASE_SERVICE_KEY = os.getenv('SUPABASE_SERVICE_KEY', '').strip()
SUPABASE_KEY = (SUPABASE_SERVICE_KEY or os.getenv('SUPABASE_KEY', '')).strip()
SUPABASE_SCHEMA = (os.getenv('SUPABASE_SCHEMA', 'public') or 'public').strip()
SUPABASE_ENABLED = bool(SUPABASE_URL and SUPABASE_KEY)
SUPABASE_STORAGE_ENABLED = SUPABASE_ENABLED and os.getenv('SUPABASE_DISABLED', '').strip() != '1'
SUPABASE_BUCKET = (os.getenv('SUPABASE_BUCKET') or 'events-ics').strip() or 'events-ics'
SUPABASE_MEDIA_BUCKET = (os.getenv('SUPABASE_MEDIA_BUCKET') or SUPABASE_BUCKET).strip() or SUPABASE_BUCKET
SUPABASE_POSTERS_PREFIX = (os.getenv('TG_MONITORING_POSTERS_PREFIX') or 'p').strip() or 'p'
SUPABASE_POSTERS_MODE = (os.getenv('TG_MONITORING_POSTERS_SUPABASE_MODE') or 'always').strip().lower()
if SUPABASE_POSTERS_MODE not in {'off', 'fallback', 'always'}:
    SUPABASE_POSTERS_MODE = 'always'
SUPABASE_VIDEOS_MODE = (os.getenv('TG_MONITORING_VIDEOS_SUPABASE_MODE') or 'always').strip().lower()
if SUPABASE_VIDEOS_MODE not in {'off', 'always'}:
    SUPABASE_VIDEOS_MODE = 'always'
SUPABASE_VIDEOS_PREFIX = (os.getenv('TG_MONITORING_VIDEOS_PREFIX') or 'v').strip().strip('/') or 'v'

YC_STORAGE_ACCESS_KEY = (os.getenv('YC_SA_BOT_STORAGE') or os.getenv('YC_SA_ML_DEV') or '').strip()
YC_STORAGE_SECRET_KEY = (os.getenv('YC_SA_BOT_STORAGE_KEY') or os.getenv('YC_SA_ML_DEV_key') or os.getenv('YC_SA_ML_DEV_KEY') or '').strip()
YC_STORAGE_BUCKET = (os.getenv('YC_STORAGE_BUCKET') or 'kenigevents').strip() or 'kenigevents'
YC_STORAGE_ENDPOINT = (os.getenv('YC_STORAGE_ENDPOINT') or 'https://storage.yandexcloud.net').strip() or 'https://storage.yandexcloud.net'
YC_STORAGE_ENABLED = bool(YC_STORAGE_ACCESS_KEY and YC_STORAGE_SECRET_KEY and YC_STORAGE_BUCKET)
POSTER_STORAGE_ENABLED = bool(YC_STORAGE_ENABLED or SUPABASE_STORAGE_ENABLED)

def _env_float(name: str, default: float) -> float:
    try:
        return float((os.getenv(name) or '').strip() or str(default))
    except Exception:
        return float(default)


def _env_int(name: str, default: int) -> int:
    try:
        return int((os.getenv(name) or '').strip() or str(default))
    except Exception:
        return int(default)


TG_MONITORING_VIDEO_MAX_MB = _env_float('TG_MONITORING_VIDEO_MAX_MB', 10.0)
# Videos use a stricter safe bucket threshold than posters.
TG_MONITORING_VIDEO_BUCKET_SAFE_MB = _env_float('TG_MONITORING_VIDEO_BUCKET_SAFE_MB', 430.0)
SUPABASE_BUCKET_USAGE_GUARD_MAX_USED_MB = _env_float('SUPABASE_BUCKET_USAGE_GUARD_MAX_USED_MB', 490.0)
SUPABASE_BUCKET_USAGE_GUARD_CACHE_SEC = _env_int('SUPABASE_BUCKET_USAGE_GUARD_CACHE_SEC', 600)
SUPABASE_BUCKET_USAGE_GUARD_ON_ERROR = (os.getenv('SUPABASE_BUCKET_USAGE_GUARD_ON_ERROR') or 'deny').strip().lower()
if SUPABASE_BUCKET_USAGE_GUARD_ON_ERROR not in {'deny', 'allow'}:
    SUPABASE_BUCKET_USAGE_GUARD_ON_ERROR = 'deny'
_VIDEO_BUCKET_USAGE_CACHE = {'checked_at': 0.0, 'used_bytes': None}

logger.info(
    'tg_monitor.video_config mode=%s max_mb=%.1f safe_mb=%.1f bucket=%s',
    SUPABASE_VIDEOS_MODE,
    TG_MONITORING_VIDEO_MAX_MB,
    TG_MONITORING_VIDEO_BUCKET_SAFE_MB,
    SUPABASE_MEDIA_BUCKET,
)

def _short_id_from_digest(digest: str) -> str:
    # Stable, short key for URLs. Prefer digest-derived bytes; fallback to hashing the string.
    try:
        raw = bytes.fromhex(digest)
    except Exception:
        raw = hashlib.sha256(digest.encode('utf-8')).digest()
    # 9 bytes => 12 urlsafe base64 chars, no padding.
    return base64.urlsafe_b64encode(raw[:9]).decode('ascii').rstrip('=')


def _to_webp_bytes(image_bytes: bytes, *, quality: int = 82) -> bytes | None:
    # Store posters in WebP only (space efficient) to deduplicate across PROD/TEST.
    if not image_bytes:
        return None
    try:
        from PIL import Image, ImageOps
        from io import BytesIO
        with Image.open(BytesIO(image_bytes)) as im:
            im = ImageOps.exif_transpose(im)
            if im.mode in {'RGBA', 'LA'} or (im.mode == 'P' and 'transparency' in (im.info or {})):
                im = im.convert('RGBA')
            else:
                im = im.convert('RGB')
            out = BytesIO()
            im.save(out, format='WEBP', quality=int(quality), method=6)
            data = out.getvalue()
            return data if data else None
    except Exception:
        return None


def _detect_image_meta(image_bytes: bytes) -> tuple[str, str]:
    # Posters are stored in WebP only.
    return 'webp', 'image/webp'

_YANDEX_STORAGE_CLIENT = None


def _yandex_public_url(bucket: str, object_path: str) -> str:
    return f"https://storage.yandexcloud.net/{bucket}/{str(object_path or '').strip().lstrip('/')}"


def _get_yandex_storage_client():
    if not YC_STORAGE_ENABLED:
        return None
    global _YANDEX_STORAGE_CLIENT
    if _YANDEX_STORAGE_CLIENT is not None:
        return _YANDEX_STORAGE_CLIENT
    try:
        import boto3
        from botocore.config import Config
    except Exception as exc:
        logger.warning('yandex storage client unavailable: %s', exc)
        return None
    session = boto3.session.Session(
        aws_access_key_id=YC_STORAGE_ACCESS_KEY,
        aws_secret_access_key=YC_STORAGE_SECRET_KEY,
        region_name='ru-central1',
    )
    _YANDEX_STORAGE_CLIENT = session.client(
        's3',
        endpoint_url=YC_STORAGE_ENDPOINT.rstrip('/'),
        region_name='ru-central1',
        config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}),
    )
    return _YANDEX_STORAGE_CLIENT


def _yandex_storage_object_exists(*, bucket: str, object_path: str) -> bool | None:
    client = _get_yandex_storage_client()
    if client is None:
        return None
    try:
        client.head_object(Bucket=bucket, Key=object_path)
        return True
    except Exception as exc:
        code = str(getattr(exc, 'response', {}).get('Error', {}).get('Code') or '').strip()
        if code in {'404', 'NoSuchKey', 'NotFound'}:
            return False
        return None


def _upload_yandex_public_bytes(data: bytes, *, bucket: str, object_path: str, content_type: str) -> str | None:
    client = _get_yandex_storage_client()
    if client is None:
        return None
    try:
        client.put_object(
            Bucket=bucket,
            Key=object_path,
            Body=data,
            ContentType=content_type,
            CacheControl='public, max-age=31536000',
        )
    except Exception as exc:
        logger.warning('yandex poster upload failed: %s', exc)
        return None
    return _yandex_public_url(bucket, object_path)

def upload_to_supabase_storage(image_bytes: bytes, sha256_hex: str | None) -> tuple[str | None, str | None]:
    if not POSTER_STORAGE_ENABLED:
        return None, None
    if not image_bytes:
        return None, None

    webp_quality = _env_int('TG_MONITORING_POSTERS_WEBP_QUALITY', 82)
    stored_bytes = _to_webp_bytes(image_bytes, quality=webp_quality)
    if not stored_bytes:
        return None, None

    # Prefer perceptual hash for cross-resolution dedup.
    phash = _compute_phash(image_bytes)
    sha = (sha256_hex or '').strip()
    if phash:
        object_path = f"{SUPABASE_POSTERS_PREFIX}/dh16/{phash[:2]}/{phash}.webp"
    elif sha:
        object_path = f"{SUPABASE_POSTERS_PREFIX}/sha256/{sha[:2]}/{sha}.webp"
    else:
        rnd = uuid.uuid4().hex
        object_path = f"{SUPABASE_POSTERS_PREFIX}/rnd/{rnd[:2]}/{rnd}.webp"

    ext, content_type = _detect_image_meta(stored_bytes)

    if YC_STORAGE_ENABLED:
        public_url = _yandex_public_url(YC_STORAGE_BUCKET, object_path)
        exists = _yandex_storage_object_exists(bucket=YC_STORAGE_BUCKET, object_path=object_path)
        if exists is True:
            return public_url, object_path
        hosted = _upload_yandex_public_bytes(
            stored_bytes,
            bucket=YC_STORAGE_BUCKET,
            object_path=object_path,
            content_type=content_type,
        )
        if hosted:
            _VIDEO_OBJECT_EXISTS_CACHE[(YC_STORAGE_BUCKET, object_path)] = True
            return hosted, object_path
        return None, None

    if not SUPABASE_STORAGE_ENABLED:
        return None, None

    allowed, _deny_reason = _poster_bucket_guard_allows(bucket=SUPABASE_MEDIA_BUCKET, extra_bytes=len(stored_bytes))
    if not allowed:
        return None, None

    exists = _supabase_storage_object_exists(bucket=SUPABASE_MEDIA_BUCKET, object_path=object_path)
    public_url = SUPABASE_URL.rstrip('/') + f"/storage/v1/object/public/{SUPABASE_MEDIA_BUCKET}/{object_path}"
    if exists is True:
        return public_url, object_path

    upload_url = SUPABASE_URL.rstrip('/') + f"/storage/v1/object/{SUPABASE_MEDIA_BUCKET}/{object_path}"
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': content_type,
        'x-upsert': 'false',
        'cache-control': 'public, max-age=31536000',
    }
    resp = requests.post(upload_url, headers=headers, data=stored_bytes, timeout=45)
    if resp.status_code not in (200, 201, 409):
        logger.warning('supabase poster upload failed: %s %s', resp.status_code, resp.text[:200])
        return None, None
    _VIDEO_OBJECT_EXISTS_CACHE[(SUPABASE_MEDIA_BUCKET, object_path)] = True
    return public_url, object_path


def _bucket_item_size_bytes(item: dict) -> int:
    meta = item.get('metadata') if isinstance(item, dict) else None
    if not isinstance(meta, dict):
        return 0
    size = meta.get('size')
    if isinstance(size, int):
        return max(0, int(size))
    if isinstance(size, str):
        try:
            return max(0, int(size))
        except Exception:
            return 0
    return 0


def _list_bucket_used_bytes(bucket: str) -> int:
    if not SUPABASE_STORAGE_ENABLED:
        return 0
    url = SUPABASE_URL.rstrip('/') + f"/storage/v1/object/list/{bucket}"
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
    }
    total = 0
    prefixes = ['']
    seen = {''}
    while prefixes:
        prefix = prefixes.pop(0)
        offset = 0
        while True:
            payload = {
                'prefix': prefix,
                'limit': 1000,
                'offset': offset,
                'sortBy': {'column': 'name', 'order': 'asc'},
            }
            resp = requests.post(url, headers=headers, json=payload, timeout=30)
            if resp.status_code >= 400:
                raise RuntimeError(f'bucket list failed {resp.status_code}: {resp.text[:200]}')
            try:
                items = resp.json()
            except Exception as exc:
                raise RuntimeError(f'bucket list invalid json: {exc}') from exc
            if not isinstance(items, list) or not items:
                break
            for item in items:
                if not isinstance(item, dict):
                    continue
                total += _bucket_item_size_bytes(item)
                # Supabase list returns folders without metadata; recurse into them.
                if _bucket_item_size_bytes(item) > 0:
                    continue
                name = str(item.get('name') or '').strip().strip('/')
                if not name:
                    continue
                child = f"{prefix.rstrip('/') + '/' if prefix else ''}{name}"
                if child and child not in seen:
                    seen.add(child)
                    prefixes.append(child)
            if len(items) < 1000:
                break
            offset += len(items)
    return int(total)


def _get_bucket_used_bytes_cached(bucket: str) -> int:
    now_ts = time.time()
    checked_at = float(_VIDEO_BUCKET_USAGE_CACHE.get('checked_at') or 0.0)
    used_cached = _VIDEO_BUCKET_USAGE_CACHE.get('used_bytes')
    if isinstance(used_cached, int) and SUPABASE_BUCKET_USAGE_GUARD_CACHE_SEC > 0 and (now_ts - checked_at) <= SUPABASE_BUCKET_USAGE_GUARD_CACHE_SEC:
        return max(0, int(used_cached))
    used = _list_bucket_used_bytes(bucket)
    _VIDEO_BUCKET_USAGE_CACHE['checked_at'] = now_ts
    _VIDEO_BUCKET_USAGE_CACHE['used_bytes'] = int(used)
    return int(used)


def _video_bucket_guard_allows(*, bucket: str, extra_bytes: int) -> tuple[bool, str | None]:
    safe_limit_bytes = int(max(TG_MONITORING_VIDEO_BUCKET_SAFE_MB, 1.0) * 1024 * 1024)
    try:
        used = _get_bucket_used_bytes_cached(bucket)
    except Exception as exc:
        logger.warning('video bucket usage check failed: %s', exc)
        if SUPABASE_BUCKET_USAGE_GUARD_ON_ERROR == 'allow':
            return True, None
        return False, 'bucket_guard'
    projected = int(used) + max(0, int(extra_bytes))
    if projected > safe_limit_bytes:
        logger.info('video bucket guard deny used_mb=%.2f extra_mb=%.2f safe_mb=%.2f', used / (1024 * 1024), max(0, int(extra_bytes)) / (1024 * 1024), TG_MONITORING_VIDEO_BUCKET_SAFE_MB)
        return False, 'bucket_guard'
    return True, None


def _poster_bucket_guard_allows(*, bucket: str, extra_bytes: int) -> tuple[bool, str | None]:
    safe_limit_bytes = int(max(SUPABASE_BUCKET_USAGE_GUARD_MAX_USED_MB, 1.0) * 1024 * 1024)
    try:
        used = _get_bucket_used_bytes_cached(bucket)
    except Exception as exc:
        logger.warning('poster bucket usage check failed: %s', exc)
        if SUPABASE_BUCKET_USAGE_GUARD_ON_ERROR == 'allow':
            return True, None
        return False, 'bucket_guard'
    projected = int(used) + max(0, int(extra_bytes))
    if projected > safe_limit_bytes:
        logger.info('poster bucket guard deny used_mb=%.2f extra_mb=%.2f safe_mb=%.2f', used / (1024 * 1024), max(0, int(extra_bytes)) / (1024 * 1024), SUPABASE_BUCKET_USAGE_GUARD_MAX_USED_MB)
        return False, 'bucket_guard'
    return True, None


def _video_mime_ext_from_message(msg) -> tuple[str, str]:
    mime = 'video/mp4'
    doc = getattr(msg, 'document', None)
    mt = getattr(doc, 'mime_type', None) or getattr(getattr(msg, 'video', None), 'mime_type', None)
    if isinstance(mt, str) and mt.strip():
        mime = mt.strip().lower()
    ext = 'mp4'
    if 'webm' in mime:
        ext = 'webm'
    elif 'mp4' in mime:
        ext = 'mp4'
    else:
        mime = 'video/mp4'
        ext = 'mp4'
    return mime, ext


_VIDEO_OBJECT_EXISTS_CACHE: dict[tuple[str, str], bool] = {}

def _supabase_storage_object_exists(*, bucket: str, object_path: str) -> bool | None:
    """Best-effort existence check to avoid re-downloading/re-uploading identical Telegram videos.

    Returns:
    - True: object exists
    - False: object definitely missing (404)
    - None: unknown (network/auth/etc), caller should fall back to normal flow
    """
    if not SUPABASE_STORAGE_ENABLED or not SUPABASE_KEY or not SUPABASE_URL:
        return None
    b = (bucket or '').strip()
    p = (object_path or '').strip().lstrip('/')
    if not b or not p:
        return None
    cache_key = (b, p)
    if cache_key in _VIDEO_OBJECT_EXISTS_CACHE:
        return _VIDEO_OBJECT_EXISTS_CACHE[cache_key]
    url = SUPABASE_URL.rstrip('/') + f"/storage/v1/object/{b}/{p}"
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f"Bearer {SUPABASE_KEY}",
    }
    try:
        resp = requests.head(url, headers=headers, timeout=12, allow_redirects=True)
    except Exception:
        return None
    if resp.status_code in (200, 206):
        _VIDEO_OBJECT_EXISTS_CACHE[cache_key] = True
        return True
    if resp.status_code == 404:
        _VIDEO_OBJECT_EXISTS_CACHE[cache_key] = False
        return False
    # Some environments may not allow HEAD; try a tiny ranged GET.
    if resp.status_code in (400, 405):
        try:
            headers2 = dict(headers)
            headers2['Range'] = 'bytes=0-0'
            resp2 = requests.get(url, headers=headers2, timeout=12, allow_redirects=True)
            if resp2.status_code in (200, 206):
                _VIDEO_OBJECT_EXISTS_CACHE[cache_key] = True
                return True
            if resp2.status_code == 404:
                _VIDEO_OBJECT_EXISTS_CACHE[cache_key] = False
                return False
        except Exception:
            return None
    return None

def upload_video_to_supabase_storage(video_bytes: bytes, *, sha256_hex: str | None, mime_type: str, ext: str) -> tuple[str | None, str | None, str | None]:
    if SUPABASE_VIDEOS_MODE != 'always':
        return None, None, 'mode_off'
    if not SUPABASE_STORAGE_ENABLED:
        return None, None, 'storage_disabled'
    if not video_bytes:
        return None, None, 'empty'
    allowed, deny_reason = _video_bucket_guard_allows(bucket=SUPABASE_MEDIA_BUCKET, extra_bytes=len(video_bytes))
    if not allowed:
        return None, None, deny_reason or 'bucket_guard'
    sha = (sha256_hex or '').strip()
    if not sha:
        return None, None, 'missing_sha256'
    # Canonical, content-addressed key (cross-env + cross-post dedup).
    object_path = f"{SUPABASE_VIDEOS_PREFIX}/sha256/{sha[:2]}/{sha}.{ext}"

    # If the object is already in storage, skip upload to save traffic.
    exists = _supabase_storage_object_exists(bucket=SUPABASE_MEDIA_BUCKET, object_path=object_path)
    if exists is True:
        public_url = SUPABASE_URL.rstrip('/') + f"/storage/v1/object/public/{SUPABASE_MEDIA_BUCKET}/{object_path}"
        return public_url, object_path, 'supabase'

    upload_url = SUPABASE_URL.rstrip('/') + f"/storage/v1/object/{SUPABASE_MEDIA_BUCKET}/{object_path}"
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f"Bearer {SUPABASE_KEY}",
        'Content-Type': mime_type or 'video/mp4',
        'x-upsert': 'false',
        'cache-control': 'public, max-age=31536000',
    }
    resp = requests.post(upload_url, headers=headers, data=video_bytes, timeout=90)
    if resp.status_code not in (200, 201, 409):
        logger.warning('supabase video upload failed: %s %s', resp.status_code, resp.text[:200])
        return None, None, 'upload_failed'
    _VIDEO_OBJECT_EXISTS_CACHE[(SUPABASE_MEDIA_BUCKET, object_path)] = True
    public_url = SUPABASE_URL.rstrip('/') + f"/storage/v1/object/public/{SUPABASE_MEDIA_BUCKET}/{object_path}"
    return public_url, object_path, 'supabase'

SUPABASE_CONSUMER = (os.getenv('TG_MONITORING_CONSUMER') or 'kaggle').strip() or 'kaggle'
GEMMA_CLIENT_MAX_RETRIES = max(1, int(os.getenv('TG_GEMMA_RETRIES', '2') or 2))

os.environ.setdefault('GOOGLE_AI_LOCAL_RPM', str(max(1, RATE_RPM)))
os.environ.setdefault('GOOGLE_AI_LOCAL_TPM', str(max(1, RATE_TPM)))
os.environ.setdefault('GOOGLE_AI_LOCAL_RPD', str(max(1, RATE_RPD)))
os.environ.setdefault('GOOGLE_AI_MAX_RETRIES', str(GEMMA_CLIENT_MAX_RETRIES))

_GEMMA_CLIENT: GoogleAIClient | None = None
_CANDIDATE_KEY_IDS: list[str] | None = None
_SUPABASE_CLIENT = None


def _key_env_aliases(name: str | None) -> list[str]:
    raw = (name or '').strip()
    if not raw:
        return []
    names = [raw]
    match = re.match(r'^(GOOGLE_API_KEY)_?(\d+)$', raw)
    if match:
        prefix, suffix = match.groups()
        compact = f'{prefix}{suffix}'
        underscored = f'{prefix}_{suffix}'
        for alias in (compact, underscored):
            if alias not in names:
                names.append(alias)
    return names


def _build_supabase_client():
    if not SUPABASE_ENABLED:
        return None
    from supabase import create_client
    from supabase.client import ClientOptions

    options = ClientOptions()
    options.schema = SUPABASE_SCHEMA or 'public'
    return create_client(SUPABASE_URL, SUPABASE_KEY, options=options)


def _get_supabase_client():
    global _SUPABASE_CLIENT
    if _SUPABASE_CLIENT is None:
        _SUPABASE_CLIENT = _build_supabase_client()
    return _SUPABASE_CLIENT


def _tg_account_name() -> str | None:
    return (os.getenv(GOOGLE_ACCOUNT_ENV) or os.getenv(GOOGLE_ACCOUNT_FALLBACK_ENV) or '').strip() or None


class _TelegramSecretsProviderAdapter:
    def __init__(self, base: SecretsProvider):
        self.base = base

    def get_secret(self, name: str):
        if name in {'GOOGLE_API_KEY', GOOGLE_KEY_ENV}:
            return self.base.get_secret(GOOGLE_KEY_ENV) or self.base.get_secret(GOOGLE_FALLBACK_KEY_ENV)
        return self.base.get_secret(name)


def _resolve_candidate_key_ids() -> list[str] | None:
    global _CANDIDATE_KEY_IDS
    if _CANDIDATE_KEY_IDS is not None:
        return list(_CANDIDATE_KEY_IDS)
    supabase = _get_supabase_client()
    if supabase is None:
        _CANDIDATE_KEY_IDS = []
        return None

    primary_envs = _key_env_aliases(GOOGLE_KEY_ENV)
    fallback_envs = [name for name in _key_env_aliases(GOOGLE_FALLBACK_KEY_ENV) if name not in primary_envs]
    env_names = [*primary_envs, *fallback_envs]
    if not env_names:
        _CANDIDATE_KEY_IDS = []
        return None

    try:
        result = (
            supabase.table('google_ai_api_keys')
            .select('id, env_var_name, priority')
            .eq('is_active', True)
            .in_('env_var_name', env_names)
            .order('priority')
            .order('id')
            .execute()
        )
        rows = list(result.data or [])
    except Exception as exc:
        logger.warning('tg_monitor.key_candidates_failed consumer=%s env=%s err=%s', SUPABASE_CONSUMER, ','.join(env_names), exc)
        _CANDIDATE_KEY_IDS = []
        return None

    primary_ids = [
        str(row.get('id'))
        for row in rows
        if row.get('id') and str(row.get('env_var_name') or '') in primary_envs
    ]
    fallback_ids = [
        str(row.get('id'))
        for row in rows
        if row.get('id') and str(row.get('env_var_name') or '') in fallback_envs
    ]
    if primary_envs and not primary_ids:
        logger.warning(
            'tg_monitor.key_candidates_missing_primary consumer=%s env=%s fallback=%s action=local_primary_limiter',
            SUPABASE_CONSUMER,
            ','.join(primary_envs),
            bool(fallback_ids),
        )
        _CANDIDATE_KEY_IDS = []
        return None
    resolved = primary_ids
    _CANDIDATE_KEY_IDS = list(resolved)
    return list(resolved) if resolved else None


def _get_gemma_client() -> GoogleAIClient:
    global _GEMMA_CLIENT
    if _GEMMA_CLIENT is None:
        _GEMMA_CLIENT = GoogleAIClient(
            supabase_client=_get_supabase_client(),
            secrets_provider=_TelegramSecretsProviderAdapter(SecretsProvider()),
            consumer=SUPABASE_CONSUMER,
            account_name=_tg_account_name(),
            default_env_var_name=GOOGLE_KEY_ENV,
        )
        logger.info(
            'tg_monitor.llm_gateway key_env=%s fallback_key_env=%s account_env=%s account_name=%s text_model=%s vision_model=%s fallback_text=%s fallback_vision=%s supabase=%s',
            GOOGLE_KEY_ENV,
            GOOGLE_FALLBACK_KEY_ENV,
            GOOGLE_ACCOUNT_ENV,
            _tg_account_name() or '-',
            TEXT_MODEL,
            VISION_MODEL,
            FALLBACK_TEXT_MODEL or '-',
            FALLBACK_VISION_MODEL or '-',
            'yes' if _get_supabase_client() is not None else 'no',
        )
    return _GEMMA_CLIENT


def _string_schema(description: str | None = None) -> dict:
    schema: dict = {'type': 'string'}
    if description:
        schema['description'] = description
    return schema


EVENT_ARRAY_SCHEMA = {
    'type': 'array',
    'items': {
        'type': 'object',
        'properties': {
            'title': _string_schema(
                'Human-readable event name. Never include inline comments (//, #), '
                "meta-commentary, reasoning, or markdown markers (**, __, ```). "
                'Never include uncertainty markers like "or something similar", alternative title candidates, '
                'or instruction-like text. '
                'Prefer a concise canonical attendee-facing title over a subsection label or long paraphrase. '
                'When message text contains a named event and OCR contains poster headings like a date, weekday, '
                'time, "НАЧАЛО В ...", "БИЛЕТЫ", "РЕГИСТРАЦИЯ", or venue labels, keep the named event from '
                'message text as title and use OCR only for date/time/venue details. '
                'A title made only of schedule/service words (for example "НАЧАЛО В 19:00") is invalid when '
                'the caption contains a named event headline. '
                'When one lecture/talk is presented with both a cycle/series label and a concrete lecture title, '
                'return one attendee-facing lecture title, not two rows. '
                'If a post describes a section/part inside a larger exhibition '
                '(for example "в разделе X на выставке Y"), use the main exhibition title Y '
                'unless X is explicitly announced as its own separate attendable event. '
                'If a post announces the opening of an exhibition and the same exhibition run, '
                'prefer one canonical exhibition title unless the post clearly advertises two separate attendable events.'
            ),
            'date': _string_schema(
                'YYYY-MM-DD or empty string; never a placeholder literal. '
                'Message date is context for resolving explicit relative anchors, not a default event date.'
            ),
            'time': _string_schema('HH:MM (24h) or empty string; never a date.'),
            'end_date': _string_schema('YYYY-MM-DD or empty string; omit for single-date events.'),
            'location_name': _string_schema(
                'Venue name where the event takes place; empty string if unknown. '
                'Must be a venue/place name or a precise hall/room label. Never copy descriptive prose, '
                'speaker biographies, schedule commentary, film metadata, ticket instructions, or narrative sentences. '
                'If the text gives only a hall/room label like "Кинозал" or "Атриум" and source context names '
                'the host venue, use the host venue as location_name and keep the hall label out of location_name. '
                'Do not use generic placeholders like "музей", "галерея", "пространство", or "площадка" '
                'unless that exact full venue name is explicitly stated. '
                'Never the literal string "unknown".'
            ),
            'location_address': _string_schema(
                'Street address of the venue; empty string if unknown. '
                'Never the literal string "unknown".'
            ),
            'city': _string_schema(
                'City of the venue where attendees physically go; empty string if not grounded in the text/OCR. '
                'Never copy a city that appears only as (a) a parenthetical origin/collection note, or '
                '(b) a biographical/affiliation mention of a speaker/author/curator/institution '
                '(e.g. "лектор — X, сотрудник Российской национальной библиотеки" does not put the event in '
                'Saint Petersburg). If the venue/address string itself contains a city like Калининград, '
                'that grounded venue city wins over every other city mention. '
                'The venue address wins; if no venue city is supported, leave empty. '
                'Never the literal string "unknown".'
            ),
            'ticket_link': _string_schema('Registration or ticket URL; empty string if none.'),
            'ticket_price_min': {'type': 'number'},
            'ticket_price_max': {'type': 'number'},
            'ticket_status': _string_schema(),
            'raw_excerpt': _string_schema(
                'Short (1-3 sentences) excerpt from the message without adding new facts. '
                'Never include inline comments, instruction-like text, or markdown markers.'
            ),
            'event_type': _string_schema(
                'Single lowercase Russian noun (концерт, выставка, лекция, спектакль, встреча, '
                'ярмарка, фестиваль, мастер-класс, кинопоказ, стендап, экскурсия, ...); '
                'never English tokens like "exhibition" or "meetup"; empty string if unsure.'
            ),
            'emoji': _string_schema(),
            'is_free': {'type': 'boolean'},
            'pushkin_card': {'type': 'boolean'},
            'search_digest': _string_schema(),
            'festival': _string_schema(),
        },
        'required': [
            'title',
            'date',
            'time',
            'end_date',
            'location_name',
            'location_address',
            'city',
            'ticket_link',
            'ticket_status',
            'raw_excerpt',
            'event_type',
            'emoji',
            'search_digest',
            'festival',
        ],
    },
}

SOURCE_METADATA_SCHEMA = {
    'type': 'object',
    'properties': {
        'is_festival_channel': {'type': 'boolean'},
        'festival_series': _string_schema(),
        'website_url': _string_schema(),
        'aliases': {'type': 'array', 'items': _string_schema()},
        'confidence': {'type': 'number'},
        'rationale_short': _string_schema(),
    },
}

TITLE_REVIEW_SCHEMA = {
    'type': 'array',
    'items': {
        'type': 'object',
        'properties': {
            'title': _string_schema(
                'Replacement attendee-facing event title chosen from message caption/text. '
                'Never a poster service heading like "НАЧАЛО В 19:00", date, price, age limit, or venue label.'
            ),
            'event_type': _string_schema(
                'Optional lowercase Russian noun if obvious from caption/text; empty string otherwise.'
            ),
            'search_digest': _string_schema('Optional short search phrase; empty string if unsure.'),
        },
        'required': ['title', 'event_type', 'search_digest'],
    },
}

OCR_SCHEMA = {
    'type': 'object',
    'properties': {
        'text': _string_schema(),
        'title': _string_schema(),
    },
}


def _generation_config(response_schema: dict | None = None) -> dict:
    cfg = {
        'temperature': 0,
        'max_output_tokens': 800,
        'response_mime_type': 'application/json',
    }
    if response_schema is not None:
        cfg['response_schema'] = response_schema
    return cfg


async def _call_model(kind: str, prompt: str, images=None, *, response_schema: dict | None = None) -> str:
    model_state = MODEL_REGISTRY[kind]
    primary_model = (model_state.get('name') or '').strip()
    fallback_model = (model_state.get('fallback') or '').strip()
    models_to_try = [primary_model]
    if fallback_model and fallback_model != primary_model:
        models_to_try.append(fallback_model)

    payload = prompt if not images else [prompt, *images]
    client = _get_gemma_client()
    candidate_key_ids = _resolve_candidate_key_ids()
    last_error: Exception | None = None

    for idx, model_name in enumerate(models_to_try):
        try:
            text, _usage = await client.generate_content_async(
                model=model_name,
                prompt=payload,
                generation_config=_generation_config(response_schema=response_schema),
                max_output_tokens=800,
                candidate_key_ids=candidate_key_ids,
            )
            return text
        except Exception as exc:
            last_error = exc
            if idx < len(models_to_try) - 1 and _is_not_found(exc):
                logger.warning('tg_monitor.model_not_found fallback=%s failed=%s', models_to_try[idx + 1], model_name)
                continue
            raise

    raise last_error or RuntimeError(f'tg_monitor model call failed kind={kind}')


def _compute_hash(image_bytes: bytes) -> str:
    return hashlib.sha256(image_bytes).hexdigest()


def _compute_phash(image_bytes: bytes) -> str | None:
    try:
        from PIL import Image, ImageOps
        img = Image.open(io.BytesIO(image_bytes))
        img = ImageOps.exif_transpose(img)
        resampling = getattr(Image, 'Resampling', None)
        lanczos = resampling.LANCZOS if resampling else Image.LANCZOS
        hash_size = 16
        gray = img.convert('L')
        small = gray.resize((hash_size + 1, hash_size), lanczos)
        pixels = list(small.getdata())
        pixels = [p >> 3 for p in pixels]
        bits = []
        row_w = hash_size + 1
        for row in range(hash_size):
            off = row * row_w
            for col in range(hash_size):
                bits.append(1 if pixels[off + col] > pixels[off + col + 1] else 0)
        value = 0
        for b in bits:
            value = (value << 1) | b
        width = (hash_size * hash_size) // 4
        return f"{value:0{width}x}"
    except Exception:
        return None


def upload_to_catbox(image_bytes: bytes) -> str | None:
    try:
        resp = requests.post(
            'https://catbox.moe/user/api.php',
            data={'reqtype': 'fileupload'},
            files={'fileToUpload': ('image.jpg', image_bytes)},
            timeout=30,
        )
        if resp.status_code == 200:
            return resp.text.strip()
    except Exception as exc:
        logger.warning('catbox upload failed: %s', exc)
    return None


def _message_date_iso(msg):
    dt = msg.date
    if dt and dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat() if dt else None


def _message_likes(msg) -> int | None:
    reactions = getattr(msg, 'reactions', None)
    if not reactions or not getattr(reactions, 'results', None):
        return None
    return sum(r.count for r in reactions.results if getattr(r, 'count', None))


def _source_type(entity) -> str:
    if isinstance(entity, Channel):
        return 'channel' if getattr(entity, 'broadcast', False) else 'supergroup'
    if isinstance(entity, Chat):
        return 'group'
    return 'unknown'


def _post_author_meta(msg) -> dict | None:
    from_id = getattr(msg, 'from_id', None)
    author = {
        'user_id': None,
        'username': None,
        'display_name': None,
        'is_user': False,
        'is_channel': False,
        'is_chat': False,
    }

    if isinstance(from_id, PeerUser):
        author['is_user'] = True
        try:
            author['user_id'] = int(getattr(from_id, 'user_id', None) or getattr(msg, 'sender_id', None) or 0) or None
        except Exception:
            author['user_id'] = None
    elif isinstance(from_id, PeerChannel):
        author['is_channel'] = True
    elif isinstance(from_id, PeerChat):
        author['is_chat'] = True

    sender = getattr(msg, 'sender', None)
    if isinstance(sender, User):
        author['is_user'] = True
        try:
            author['user_id'] = int(getattr(sender, 'id', None) or author['user_id'] or 0) or None
        except Exception:
            pass
        author['username'] = (getattr(sender, 'username', None) or '').strip() or None
        first = str(getattr(sender, 'first_name', None) or '').strip()
        last = str(getattr(sender, 'last_name', None) or '').strip()
        display = ' '.join(part for part in (first, last) if part).strip()
        author['display_name'] = display or author['username']
    elif isinstance(sender, Channel):
        author['is_channel'] = True
        author['username'] = (getattr(sender, 'username', None) or '').strip() or None
        author['display_name'] = (getattr(sender, 'title', None) or '').strip() or author['username']
    elif isinstance(sender, Chat):
        author['is_chat'] = True
        author['display_name'] = (getattr(sender, 'title', None) or '').strip() or None

    if author['is_user'] and (author['user_id'] or author['username'] or author['display_name']):
        return author
    if author['is_channel'] or author['is_chat']:
        return author
    return None


_METADATA_LINK_RE = re.compile(r"https?://[^\s<>()\"']+", re.IGNORECASE)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def _normalize_meta_text(value: str | None) -> str:
    raw = str(value or '').strip().lower()
    return re.sub(r"\s+", ' ', raw).strip()


def _extract_about_links(about: str | None) -> list[str]:
    raw = str(about or '')
    if not raw:
        return []
    out = []
    seen = set()
    for m in _METADATA_LINK_RE.findall(raw):
        url = str(m or '').strip().rstrip('.,);]')
        if not url.lower().startswith(('http://', 'https://')):
            continue
        key = url.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(url)
        if len(out) >= 12:
            break
    return out


def _compute_source_meta_hash(title: str | None, about: str | None) -> str:
    normalized = f"{_normalize_meta_text(title)}\n{_normalize_meta_text(about)}"
    digest = hashlib.sha256(normalized.encode('utf-8')).hexdigest()
    return f"sha256:{digest}"


def _build_source_metadata_prompt(payload: dict) -> str:
    return (
        "Ты извлекаешь подсказки по метаданным Telegram-источника. "
        "Даны username, title, about и about_links. "
        "Верни только JSON без markdown. "
        "Правила: не выдумывай факты; если уверенности нет, оставь пустые строки и низкую confidence. "
        "website_url: только официальный standalone website фестиваля/проекта/источника. "
        "Никогда не возвращай как website_url ссылки на Telegram, Telegra.ph, Instagram, VK, YouTube, Linktree, Taplink, Boosty, Patreon и другие соцсети/линк-агрегаторы. "
        "aliases: только варианты, реально встречающиеся в title/about. "
        "Схема ответа: "
        "{\"is_festival_channel\": bool, \"festival_series\": str, \"website_url\": str, \"aliases\": [str], \"confidence\": number, \"rationale_short\": str}.\n"
        "Input JSON:\n" + json.dumps(payload, ensure_ascii=False)
    )


_SOURCE_WEBSITE_BLOCK_RE = re.compile(
    r"^https?://(?:"
    r"(?:www\.)?t\.me/"
    r"|(?:www\.)?telegra\.ph/"
    r"|(?:www\.)?instagram\.com/"
    r"|(?:www\.)?vk(?:video)?\.com/"
    r"|(?:www\.)?youtube\.com/"
    r"|youtu\.be/"
    r"|(?:www\.)?linktr\.ee/"
    r"|(?:www\.)?taplink\.cc/"
    r"|(?:www\.)?boosty\.to/"
    r"|(?:www\.)?patreon\.com/"
    r")",
    flags=re.IGNORECASE,
)


def _is_disallowed_source_website_url(value: str | None) -> bool:
    url = str(value or '').strip()
    if not url:
        return False
    return bool(_SOURCE_WEBSITE_BLOCK_RE.match(url))


def _sanitize_source_suggestions(data: dict | None) -> dict | None:
    if not isinstance(data, dict):
        return None
    is_festival = bool(data.get('is_festival_channel'))
    festival_series = str(data.get('festival_series') or '').strip()
    website_url = str(data.get('website_url') or '').strip()
    if website_url and (not website_url.lower().startswith(('http://', 'https://'))):
        website_url = ''
    if website_url and _is_disallowed_source_website_url(website_url):
        website_url = ''
    aliases = []
    seen_aliases = set()
    for alias in data.get('aliases') or []:
        item = str(alias or '').strip()
        if not item:
            continue
        key = item.casefold()
        if key in seen_aliases:
            continue
        seen_aliases.add(key)
        aliases.append(item)
        if len(aliases) >= 5:
            break
    confidence = 0.0
    try:
        confidence = float(data.get('confidence') or 0.0)
    except Exception:
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))
    rationale = str(data.get('rationale_short') or '').strip()

    if not is_festival and confidence < 0.35:
        festival_series = ''
        website_url = ''

    return {
        'is_festival_channel': is_festival,
        'festival_series': festival_series,
        'website_url': website_url,
        'aliases': aliases,
        'confidence': confidence,
        'rationale_short': rationale,
    }


async def _suggest_source_metadata(payload: dict) -> dict | None:
    prompt = _build_source_metadata_prompt(payload)
    try:
        text = await _call_model('text', prompt, response_schema=SOURCE_METADATA_SCHEMA)
    except Exception as exc:
        logger.warning('source_meta.suggest failed username=%s: %s', payload.get('username'), exc)
        return None

    data = _safe_json(text)
    if data is None:
        fix_prompt = (
            "Fix and return valid JSON only. "
            "Do not include any extra text, inline comments (//, #), meta-commentary, or markdown markers (**, __).\n"
            "Input:\n" + text
        )
        try:
            fixed_text = await _call_model('text', fix_prompt, response_schema=SOURCE_METADATA_SCHEMA)
            data = _safe_json(fixed_text)
        except Exception as exc:
            logger.warning('source_meta.suggest json_fix failed username=%s: %s', payload.get('username'), exc)
            return None

    return _sanitize_source_suggestions(data)


async def _fetch_source_about(client: TelegramClient, entity, source_type: str, username: str) -> str:
    if source_type in {'channel', 'supergroup'}:
        try:
            full = await tg_call(f'get_full_channel:{username}', client, GetFullChannelRequest(entity))
            return str(getattr(getattr(full, 'full_chat', None), 'about', None) or '').strip()
        except Exception as exc:
            logger.warning('source_meta.full_channel_failed %s: %s', username, exc)
            return ''
    if source_type == 'group':
        chat_id = getattr(entity, 'id', None)
        if chat_id is None:
            return ''
        try:
            full = await tg_call(f'get_full_chat:{username}', client, GetFullChatRequest(int(chat_id)))
            return str(getattr(getattr(full, 'full_chat', None), 'about', None) or '').strip()
        except Exception as exc:
            logger.warning('source_meta.full_chat_failed %s: %s', username, exc)
            return ''
    return ''


async def _build_source_meta(client: TelegramClient, username: str, entity, source_type: str) -> dict:
    clean_username = str(username or '').strip().lstrip('@').lower()
    title = str(getattr(entity, 'title', None) or '').strip()
    about = await _fetch_source_about(client, entity, source_type, clean_username)
    about_links = _extract_about_links(about)
    suggestions_payload = {
        'username': clean_username,
        'title': title,
        'about': about,
        'about_links': about_links,
    }
    suggestions = await _suggest_source_metadata(suggestions_payload)
    return {
        'username': clean_username,
        'source_type': source_type or 'unknown',
        'title': title,
        'about': about,
        'about_links': about_links,
        'fetched_at': _utc_now_iso(),
        'meta_hash': _compute_source_meta_hash(title, about),
        'suggestions': suggestions,
    }


MONTHS_MAP = {
    'января': 1,
    'февраля': 2,
    'марта': 3,
    'апреля': 4,
    'мая': 5,
    'июня': 6,
    'июля': 7,
    'августа': 8,
    'сентября': 9,
    'октября': 10,
    'ноября': 11,
    'декабря': 12,
}
DATE_TEXT_RE = re.compile(
    r"\b(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\b",
    re.IGNORECASE,
)
DATE_NUM_RE = re.compile(r"\b(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?\b")
TIME_RE = re.compile(r"\b([01]?\d|2[0-3])[:.](\d{2})\b")
TIME_RANGE_RE = re.compile(
    r"\b([01]?\d|2[0-3])[:.](\d{2})\s*(?:-|–|—|…|\.{2,}|до)\s*([01]?\d|2[0-3])[:.](\d{2})\b",
    re.IGNORECASE,
)
TIME_START_HINT_RE = re.compile(
    r"\b(начал[ао]|старт|сбор|вход)\D{0,20}([01]?\d|2[0-3])[:.](\d{2})\b",
    re.IGNORECASE,
)
BRIDGE_NOTICE_RE = re.compile(
    r"\b(?:развод(?:ка|ки|ке|ку)?\s+мост(?:ов|ы|а)?|разводк[аеуи]\s+мостов|"
    r"развест[и]\s+мосты|разведут\s+мосты|мосты\s+разведут)\b",
    re.IGNORECASE,
)
BRIDGE_NAME_RE = re.compile(r"[«\"“]([^»\"”]*(?:Юбилейн|Высок)[^»\"”]*)[»\"”]", re.IGNORECASE)
BRIDGE_NIGHT_ON_RE = re.compile(
    r"\bв\s+ночь\s+на\s+(\d{1,2})\s+"
    r"(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\b",
    re.IGNORECASE,
)
BRIDGE_NIGHT_RANGE_RE = re.compile(
    r"\bв\s+ночь\s+с\s+(\d{1,2})\s+на\s+(\d{1,2})\s+"
    r"(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\b",
    re.IGNORECASE,
)
BRIDGE_RELATIVE_TODAY_RE = re.compile(r"\bсегодня\s+в\s+ночь\b|\bразводк[аеуи]\s+мостов\s+сегодня\b", re.IGNORECASE)
BRIDGE_HOUR_RANGE_RE = re.compile(
    r"\b(?:с|с\s+)?(\d{1,2})(?::00)?\s*(?:час(?:ов|а)?\s*)?"
    r"(?:до|-|–|—)\s*(\d{1,2})(?::00)?\s*(?:час(?:ов|а)?|утра|вечера)?\b",
    re.IGNORECASE,
)
BRIDGE_EVENING_MORNING_RE = re.compile(
    r"\bс\s+(\d{1,2})\s+вечера\s+до\s+(\d{1,2})\s+утра\b",
    re.IGNORECASE,
)


def _parse_message_date(message_date: str | None):
    if not message_date:
        return None
    try:
        return datetime.fromisoformat(message_date).date()
    except Exception:
        try:
            return datetime.strptime(message_date, '%Y-%m-%d').date()
        except Exception:
            return None


def _infer_ocr_date(day: int, month: int, year: int | None, msg_date):
    try:
        if year:
            candidate = date(year, month, day)
        elif msg_date:
            candidate = date(msg_date.year, month, day)
            if candidate < msg_date:
                candidate = date(msg_date.year + 1, month, day)
        else:
            candidate = date(datetime.now(timezone.utc).year, month, day)
        return candidate
    except Exception:
        return None


def _extract_ocr_datetime(ocr_text: str | None, message_date: str | None = None):
    if not ocr_text:
        return None, None
    text = (ocr_text or '').lower().replace('ё', 'е')
    msg_date = _parse_message_date(message_date)

    date_vals = []
    for day_str, month_name in DATE_TEXT_RE.findall(text):
        month = MONTHS_MAP.get(month_name.lower())
        if not month:
            continue
        candidate = _infer_ocr_date(int(day_str), month, None, msg_date)
        if candidate:
            date_vals.append(candidate.isoformat())

    for day_str, month_str, year_str in DATE_NUM_RE.findall(text):
        try:
            day = int(day_str)
            month = int(month_str)
        except Exception:
            continue
        if month < 1 or month > 12:
            continue
        year = None
        if year_str:
            try:
                year = int(year_str)
                if year < 100:
                    year += 2000
            except Exception:
                year = None
        candidate = _infer_ocr_date(day, month, year, msg_date)
        if candidate:
            date_vals.append(candidate.isoformat())

    date_vals = sorted(set(date_vals))
    date_val = date_vals[0] if len(date_vals) == 1 else None

    # Prefer explicit time ranges (10:00-18:00, с 10:00 до 18:00).
    ranges = []
    for h1, m1, h2, m2 in TIME_RANGE_RE.findall(text):
        try:
            start_h = int(h1)
            end_h = int(h2)
        except Exception:
            continue
        start = f"{start_h:02d}:{m1}"
        end = f"{end_h:02d}:{m2}"
        if start != end:
            ranges.append(f"{start}-{end}")
    ranges = list(dict.fromkeys(ranges))
    if len(ranges) == 1:
        return date_val, ranges[0]

    times = []
    for h_str, m_str in TIME_RE.findall(text):
        try:
            h = int(h_str)
            m = int(m_str)
        except Exception:
            continue
        if 0 <= h <= 23 and 0 <= m <= 59:
            times.append(f"{h:02d}:{m:02d}")

    # Avoid treating date tokens like '05.02' as time '05:02'.
    if date_val:
        try:
            d = date.fromisoformat(date_val)
            banned = {f"{d.day:02d}:{d.month:02d}", f"{d.month:02d}:{d.day:02d}"}
            times = [t for t in times if t not in banned]
        except Exception:
            pass

    if not times:
        return date_val, None

    unique = sorted(set(times))
    if len(unique) == 1:
        return date_val, unique[0]

    hint = TIME_START_HINT_RE.search(text)
    if hint:
        try:
            h = int(hint.group(2))
        except Exception:
            h = None
        mm = hint.group(3)
        if h is not None:
            return date_val, f"{h:02d}:{mm}"

    # Fallback: most frequent (then earliest).
    from collections import Counter
    counts = Counter(times)
    best = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]
    return date_val, best


def _bridge_date_from_parts(day: int, month_name: str, msg_date) -> str | None:
    month = MONTHS_MAP.get((month_name or '').strip().lower())
    if not month:
        return None
    candidate = _infer_ocr_date(int(day), month, None, msg_date)
    return candidate.isoformat() if candidate else None


def _extract_bridge_time(text: str | None) -> str:
    raw = str(text or '')
    m = BRIDGE_EVENING_MORNING_RE.search(raw)
    if m:
        try:
            start = int(m.group(1))
            if start < 12:
                start += 12
            end = int(m.group(2))
            return f"{start:02d}:00-{end:02d}:00"
        except Exception:
            pass
    m = BRIDGE_HOUR_RANGE_RE.search(raw)
    if m:
        try:
            start = int(m.group(1))
            end = int(m.group(2))
            return f"{start:02d}:00-{end:02d}:00"
        except Exception:
            pass
    explicit = _extract_ocr_datetime(raw, None)[1]
    return str(explicit or '').strip()


def _extract_bridge_names(text: str | None) -> list[str]:
    raw = str(text or '')
    names: list[str] = []
    seen: set[str] = set()
    for match in BRIDGE_NAME_RE.findall(raw):
        name = re.sub(r'\s+', ' ', str(match or '').strip(' «»"“”'))
        if not name:
            continue
        key = name.casefold()
        if key in seen:
            continue
        seen.add(key)
        names.append(name)
    low = raw.casefold()
    for name in ('Юбилейный', 'Высокий'):
        if name.casefold() in low and name.casefold() not in seen:
            seen.add(name.casefold())
            names.append(name)
    return names


def _bridge_event_fallback(
    text: str | None,
    *,
    message_date: str | None,
    source_username: str | None = None,
) -> list[dict]:
    """Final structural guardrail for grounded official @klgdcity bridge notices."""
    username = (source_username or '').strip().lstrip('@').lower()
    if username != 'klgdcity':
        return []
    raw = str(text or '').strip()
    if not raw or not BRIDGE_NOTICE_RE.search(raw):
        return []

    msg_date = _parse_message_date(message_date)
    event_dates: list[str] = []

    for day, month_name in BRIDGE_NIGHT_ON_RE.findall(raw):
        iso = _bridge_date_from_parts(int(day), month_name, msg_date)
        if iso:
            event_dates.append(iso)

    for start_day, _end_day, month_name in BRIDGE_NIGHT_RANGE_RE.findall(raw):
        iso = _bridge_date_from_parts(int(start_day), month_name, msg_date)
        if iso:
            event_dates.append(iso)

    if BRIDGE_RELATIVE_TODAY_RE.search(raw) and msg_date:
        event_dates.append(msg_date.isoformat())

    event_dates = sorted(dict.fromkeys(event_dates))
    if not event_dates:
        return []

    bridge_names = _extract_bridge_names(raw)
    bridge_label = ' и '.join(bridge_names) if bridge_names else 'мостов'
    title = f"Развод мостов {bridge_label}" if bridge_names else "Развод мостов"
    time_val = _extract_bridge_time(raw)
    location = 'Остров Октябрьский, Калининград' if re.search(r'Октябрьск|Остров', raw, re.IGNORECASE) else 'Калининград'
    excerpt = raw[:500]

    events: list[dict] = []
    for event_date in event_dates:
        events.append({
            'title': title,
            'date': event_date,
            'time': time_val,
            'end_date': '',
            'location_name': location,
            'location_address': '',
            'city': 'Калининград',
            'ticket_link': '',
            'ticket_price_min': None,
            'ticket_price_max': None,
            'ticket_status': '',
            'raw_excerpt': excerpt,
            'source_text': raw[:2500],
            'event_type': 'городское событие',
            'emoji': '🌉',
            'is_free': True,
            'pushkin_card': None,
            'search_digest': 'Развод мостов в Калининграде',
            'festival': None,
        })
    return events[: max(1, int(MAX_EVENTS_PER_MESSAGE))]


def _bridge_llm_output_is_usable(events: list, *, expected_count: int) -> bool:
    if not isinstance(events, list) or len(events) < max(1, int(expected_count or 1)):
        return False
    time_re = re.compile(r"^\d{2}:\d{2}(?:-\d{2}:\d{2})?$")
    for item in events[:expected_count]:
        if not isinstance(item, dict):
            return False
        title = str(item.get('title') or '').casefold()
        if not ('развод' in title and 'мост' in title):
            return False
        try:
            date.fromisoformat(str(item.get('date') or '').strip())
        except Exception:
            return False
        time_value = str(item.get('time') or '').strip()
        if time_value and not time_re.match(time_value):
            return False
    return True


async def _extract_bridge_events_rescue(
    content: str,
    *,
    message_date: str | None,
    source_username: str | None,
    source_title: str | None,
) -> list[dict]:
    username = (source_username or '').strip().lstrip('@').lower()
    if username != 'klgdcity' or not BRIDGE_NOTICE_RE.search(content or ''):
        return []
    date_context = f"Message date (ISO, UTC): {message_date}" if message_date else 'Message date: unknown'
    source_context = f"Source username: @{username}"
    if source_title:
        source_context += f"\nSource title: {str(source_title).strip()[:120]}"
    prompt = (
        'You are a narrow rescue extractor for official @klgdcity bridge-lifting notices. '
        'Return strict JSON array of event objects only. '
        'Extract ONLY notices about развод/разводка/разведение мостов. '
        'These notices are public city events. If the text has no bridge-lifting notice, return []. '
        'Resolve "сегодня" from Message date. Resolve "в ночь на D month" to D month. '
        'Resolve "в ночь с A на B month" as the start date A month. '
        'If the notice names two nights, return two events. '
        'Use HH:MM-HH:MM for explicit ranges like "с 23 до 05" or "с 11 вечера до 5 утра"; '
        'use empty time if no explicit range. '
        'Use title "Развод мостов" plus bridge names if grounded. Do not invent bridge names. '
        'Use city "Калининград"; use location_name "Остров Октябрьский, Калининград" when the text mentions Остров/Октябрьский. '
        'Fields per event: title, date (YYYY-MM-DD), time (HH:MM-HH:MM or empty), '
        'end_date, location_name, location_address, city, ticket_link, ticket_price_min, '
        'ticket_price_max, ticket_status, raw_excerpt, event_type, emoji, is_free, '
        'pushkin_card, search_digest, festival. '
        f'{date_context}\n{source_context}\n'
        'Message text:\n' + content
    )
    try:
        text = await _call_model('text', prompt, response_schema=EVENT_ARRAY_SCHEMA)
        data = _safe_json(text)
    except Exception as exc:
        logger.warning('extract_events bridge_rescue failed: %s', exc)
        return []
    if isinstance(data, dict) and isinstance(data.get('events'), list):
        out = data['events']
    elif isinstance(data, list):
        out = data
    else:
        out = []
    return (out or [])[: max(1, int(MAX_EVENTS_PER_MESSAGE))]


_EVENT_STRING_FIELDS: tuple[str, ...] = (
    'title',
    'date',
    'time',
    'end_date',
    'location_name',
    'location_address',
    'city',
    'ticket_link',
    'ticket_status',
    'raw_excerpt',
    'event_type',
    'emoji',
    'search_digest',
    'festival',
)
_UNKNOWN_LITERALS: frozenset[str] = frozenset({'unknown', 'n/a', 'none', 'null', '-', 'title'})
_LEAKED_COMMENT_TAIL_RE = re.compile(
    r"(?:\s+[(\[{]?\s*(?://|#)\s.*$|[(\[{]\s*(?://|#)\s.*$)",
    re.DOTALL,
)
_MARKDOWN_STRIP_RE = re.compile(r"(?:\*\*|__|~~|```|`)+")
# Gemma 4 structured-output leakage: model occasionally emits HTML-like tags
# (``</strong>``, ``<br>``, ...) and trailing meta-commentary like
# ``own title:`` / ``own id:`` inside JSON string fields. These never belong
# in event strings — strip them deterministically.
_HTML_TAG_RE = re.compile(r"</?[a-zA-Z][^<>]*>")
_TRAILING_META_TAIL_RE = re.compile(
    r"\s+(?:own\s+(?:title|id|type|event|field)|own)\s*[:=]?\s*$",
    re.IGNORECASE,
)
_SERVICE_HEADING_TITLE_RE = re.compile(
    r"^\s*(?:"
    r"(?:\d{1,2}\s+(?:января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря))"
    r"|(?:\d{1,2}[./]\d{1,2}(?:[./]\d{2,4})?)"
    r"|(?:начало\s+в\s+\d{1,2}[:.]\d{2})"
    r"|(?:билеты|регистрация|стоимость|цена|вход|место|адрес)"
    r")\s*$",
    re.IGNORECASE | re.UNICODE,
)


def _clean_event_string_value(value) -> str:
    """Sanitize a free-form LLM string value.

    Drops inline code-style comments (`// ...`, `# ...`) that Gemma 4 occasionally leaks
    mid-value, strips markdown emphasis markers, strips HTML-style tags and trailing
    meta-commentary tails, and collapses the literals we never want to trust downstream
    ("unknown", "n/a", ...).
    """
    if value is None:
        return ''
    if not isinstance(value, str):
        return ''
    cleaned = _LEAKED_COMMENT_TAIL_RE.sub('', value)
    cleaned = _HTML_TAG_RE.sub('', cleaned)
    cleaned = _MARKDOWN_STRIP_RE.sub('', cleaned)
    cleaned = _TRAILING_META_TAIL_RE.sub('', cleaned)
    cleaned = cleaned.strip().strip('*_~`').strip()
    if cleaned.casefold() in _UNKNOWN_LITERALS:
        return ''
    return cleaned


def _sanitize_extracted_events(events) -> list[dict]:
    """Final safety-net over LLM-extracted events.

    This does not replace the LLM (extract_events continues to be LLM-first). It only
    cleans up well-known Gemma 4 failure modes that slip through the prompt contract:
      - inline `// ...` / `# ...` commentary leaked into JSON string values;
      - HTML-style tags (``</strong>``, ``<br>``, ...) leaking into event strings;
      - trailing meta-commentary tails like ``own title:`` / ``own id:``;
      - stray markdown markers (``**``, ``__``, ``` ``` ```) wrapping titles/excerpts;
      - literal placeholders like ``"unknown"`` / ``"n/a"`` where the prompt asks for "";
      - fully empty ghost rows (no title AND no date), which Gemma 4 emits once per
        venue mention in multi-event posts.
    """
    cleaned: list[dict] = []
    if not isinstance(events, list):
        return cleaned
    for evt in events:
        if not isinstance(evt, dict):
            continue
        for field in _EVENT_STRING_FIELDS:
            if field in evt:
                evt[field] = _clean_event_string_value(evt.get(field))
        title = str(evt.get('title') or '').strip()
        date_val = str(evt.get('date') or '').strip()
        if not title:
            continue
        if not title and not date_val:
            continue
        cleaned.append(evt)
    return cleaned


async def _repair_service_heading_titles(
    *,
    message_text: str,
    ocr_text: str | None,
    date_context: str,
    source_context_line: str,
    events: list,
) -> list:
    """Ask the LLM to repair title-only OCR service-heading regressions.

    The deterministic part only detects the syntactic failure shape. The event
    title choice remains LLM-owned and is reviewed against the original caption.
    """
    if not events or not isinstance(events, list):
        return events
    suspect = False
    for ev in events:
        if not isinstance(ev, dict):
            continue
        title = str(ev.get('title') or '').strip()
        if title and _SERVICE_HEADING_TITLE_RE.search(title):
            suspect = True
            break
    if not suspect:
        return events

    prompt = (
        'Review extracted Telegram events and choose replacement titles for suspicious poster-service-heading titles. '
        'Return strict JSON array with exactly one object per input event, same order. '
        'Each output object has title, event_type, search_digest only. '
        'Do not add events. Do not drop events. '
        'A title made only of date/time/service text such as "НАЧАЛО В 19:00", "24 АПРЕЛЯ", "БИЛЕТЫ", '
        '"РЕГИСТРАЦИЯ", price, age limit, or venue/address label is invalid if the message caption contains a named event. '
        'In that case, output the named attendee-facing event from the caption as title. '
        'Example: caption "Второй Большой киноквиз!" and OCR "24 АПРЕЛЯ / НАЧАЛО В 19:00" '
        'must output title "Второй Большой киноквиз". '
        'If an input title is already a real attendee-facing event name, repeat it unchanged. '
        'Never output service headings as titles. Never include comments, markdown, alternatives, or reasoning in JSON values. '
        + date_context + '\n'
        + (source_context_line + '\n' if source_context_line else '')
        + 'Message caption/text:\n' + (message_text or '')[:6000] + '\n\n'
        + ('OCR text:\n' + (ocr_text or '')[:3000] + '\n\n' if ocr_text else '')
        + 'Extracted events JSON:\n' + json.dumps(events, ensure_ascii=False)
    )
    try:
        repaired_text = await _call_model('text', prompt, response_schema=TITLE_REVIEW_SCHEMA)
        repaired = _safe_json(repaired_text)
    except Exception as exc:
        logger.warning('extract_events title_review failed: %s', exc)
        return events
    if isinstance(repaired, dict) and isinstance(repaired.get('titles'), list):
        repaired = repaired['titles']
    if isinstance(repaired, list) and len(repaired) == len(events):
        out = []
        for old, new in zip(events, repaired):
            if not isinstance(old, dict):
                out.append(old)
                continue
            merged = dict(old)
            if isinstance(new, dict):
                title = _clean_event_string_value(new.get('title'))
                if title and not _SERVICE_HEADING_TITLE_RE.search(title):
                    merged['title'] = title
                for field in ('event_type', 'search_digest'):
                    value = _clean_event_string_value(new.get(field))
                    if value:
                        merged[field] = value
            out.append(merged)
        return out
    return events


async def extract_events(
    text: str,
    ocr_text: str | None = None,
    message_date: str | None = None,
    source_username: str | None = None,
    source_title: str | None = None,
):
    content = (text or '').strip()
    if not content or len(content) < 10:
        return []

    # Fast path: schedule-style posts (e.g. '07.02 | Мёртвые души') should not waste LLM calls.
    # Also cap extracted events to keep downstream Smart Update / LLM usage bounded.
    msg_date = None
    if message_date:
        try:
            msg_date = datetime.fromisoformat(message_date.replace('Z', '+00:00')).date()
        except Exception:
            msg_date = None

    sched_events = []
    line_re = re.compile(r'^\s*(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?\s*(?:[|—–\-:]+\s*)?(.*\S)\s*$')
    for line in content.splitlines():
        m = line_re.search(line)
        if not m:
            continue
        try:
            day = int(m.group(1))
            month = int(m.group(2))
        except Exception:
            continue
        if not (1 <= day <= 31 and 1 <= month <= 12):
            continue
        year_raw = (m.group(3) or '').strip()
        year = None
        if year_raw:
            try:
                year = int(year_raw)
                if year < 100:
                    year += 2000
            except Exception:
                year = None
        if year is None and msg_date is not None:
            year = msg_date.year
        if year is None:
            continue
        try:
            ev_date = date(year, month, day)
            if msg_date is not None and ev_date < msg_date and msg_date.month == 12 and month == 1:
                ev_date = date(year + 1, month, day)
        except Exception:
            continue
        title = (m.group(4) or '').strip()
        title = re.sub(r'\[(.*?)\]\([^)]*\)', r'\1', title)
        title = title.strip('*_~` ')
        title = re.sub(r'^[-•·\*]+\s*', '', title)
        title = re.sub(r'\s+', ' ', title).strip()
        title = title.lstrip(',.;:!?—–- ').strip()
        t_cf = title.casefold().replace('ё', 'е')
        if t_cf in {'понедельник','вторник','среда','четверг','пятница','суббота','воскресенье'}:
            # Defensive: avoid extracting pseudo-titles like ", четверг".
            continue
        if not title or len(title) < 3:
            continue
        line_excerpt = f"{day:02d}.{month:02d} | {title}"
        sched_events.append({
            'title': title,
            'date': ev_date.isoformat(),
            'time': '',
            'end_date': None,
            'location_name': None,
            'location_address': None,
            'city': None,
            'ticket_link': None,
            'ticket_price_min': None,
            'ticket_price_max': None,
            'ticket_status': None,
            'raw_excerpt': line_excerpt,
            'source_text': line_excerpt,
            'event_type': None,
            'emoji': None,
            'is_free': None,
            'pushkin_card': None,
            'search_digest': None,
            'festival': None,
        })

    # Fast-path schedule extraction is intentionally conservative.
    # If we only matched a single line, prefer the LLM path to avoid false positives.
    if len(sched_events) >= 2:
        try:
            sched_events = sorted(sched_events, key=lambda e: e.get('date') or '')
        except Exception:
            pass
        return (sched_events or [])[: max(1, int(MAX_EVENTS_PER_MESSAGE))]

    # Fast path #2: Russian month-name schedules
    # Example: "7 февраля в 17:00 - «Мурильо: Путь художника», 12+"
    ru_months = {
        'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4, 'мая': 5, 'июня': 6,
        'июля': 7, 'августа': 8, 'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12,
    }
    ru_sched_events = []
    ru_line_re = re.compile(
        r'^\s*(?:[🎞🎬•·*-]\s*)?(\d{1,2})\s+([а-яё]+)\s+в\s+(\d{1,2}:\d{2})\s*(?:[—–\-:]+\s*)?(.*\S)\s*$',
        re.IGNORECASE,
    )
    for line in content.splitlines():
        m = ru_line_re.search(line)
        if not m:
            continue
        try:
            day = int(m.group(1))
        except Exception:
            continue
        month_name = (m.group(2) or '').strip().lower()
        month = ru_months.get(month_name)
        if not month:
            continue
        tm = (m.group(3) or '').strip()
        year = msg_date.year if msg_date is not None else None
        if year is None:
            continue
        try:
            ev_date = date(year, month, day)
            if msg_date is not None and ev_date < msg_date and msg_date.month == 12 and month == 1:
                ev_date = date(year + 1, month, day)
        except Exception:
            continue
        title = (m.group(4) or '').strip()
        title = re.sub(r'\[(.*?)\]\([^)]*\)', r'\1', title)
        title = re.sub(r'^[«"\']+', '', title)
        title = re.sub(r'[»"\']+(?:,\s*\d{1,2}\+)?\s*$', '', title)
        title = re.sub(r',\s*\d{1,2}\+.*$', '', title)
        title = title.strip('*_~` ')
        title = re.sub(r'\s+', ' ', title).strip()
        title = title.lstrip(',.;:!?—–- ').strip()
        t_cf = title.casefold().replace('ё', 'е')
        if t_cf in {'понедельник','вторник','среда','четверг','пятница','суббота','воскресенье'}:
            continue
        if not title or len(title) < 3:
            continue
        line_excerpt = f"{day:02d}.{month:02d} | {title}"
        ru_sched_events.append({
            'title': title,
            'date': ev_date.isoformat(),
            'time': tm,
            'end_date': None,
            'location_name': None,
            'location_address': None,
            'city': None,
            'ticket_link': None,
            'ticket_price_min': None,
            'ticket_price_max': None,
            'ticket_status': None,
            'raw_excerpt': line_excerpt,
            'source_text': line_excerpt,
            'event_type': None,
            'emoji': None,
            'is_free': None,
            'pushkin_card': None,
            'search_digest': None,
            'festival': None,
        })

    # Be conservative: a single matched line can be an intro like "19.02, четверг".
    if len(ru_sched_events) >= 2:
        try:
            ru_sched_events = sorted(ru_sched_events, key=lambda e: (e.get('date') or '', e.get('time') or ''))
        except Exception:
            pass
        return (ru_sched_events or [])[: max(1, int(MAX_EVENTS_PER_MESSAGE))]

    # LLM path
    message_text_only = content
    if ocr_text:
        content = (content + '\n\nOCR:\n' + ocr_text).strip()
    if not content or len(content) < 10:
        return []
    message_date_ymd = msg_date.isoformat() if msg_date else ''
    date_context = f"Message date (ISO, UTC): {message_date}" if message_date else 'Message date: unknown'
    if message_date_ymd:
        date_context += f"\nMessage date date part (YYYY-MM-DD): {message_date_ymd}"
    source_context_parts = []
    if source_username:
        source_context_parts.append(f"Source username: @{str(source_username).strip().lstrip('@')}")
    if source_title:
        source_context_parts.append(f"Source title: {str(source_title).strip()[:120]}")
    source_context = ("\n" + "\n".join(source_context_parts)) if source_context_parts else ""
    source_context_line = "Source context: " + "; ".join(source_context_parts) if source_context_parts else ""
    schedule_like = bool(
        re.search(
            r'\b\d{1,2}\s+(?:января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\b',
            content,
            re.IGNORECASE,
        )
        and len(re.findall(r'\b\d{1,2}[.:]\d{2}\b', content)) >= 2
    )
    prompt = (
        'You extract events from a Telegram message. A single message may contain MULTIPLE events, '
        'including repertoire/schedule lines like "DD.MM | Title". '
        'Return strict JSON array of event objects. '
        'If there are no events, return [] only. '
        'Fields per event: title, date (YYYY-MM-DD), time (HH:MM or empty), '
        'end_date (YYYY-MM-DD or empty string), location_name, location_address, city, '
        'ticket_link, ticket_price_min, ticket_price_max, ticket_status, raw_excerpt, '
        'event_type, emoji, is_free, pushkin_card, search_digest, festival. '
        'Use empty string for unknown text fields. Omit numeric and boolean fields when unknown. '
        'Never return whitespace-only strings. '
        'Never output the literal string "unknown" (or "n/a", "none") in any text field; use empty string instead. '
        'Never include inline comments ("//", "#", "TODO"), meta-commentary, reasoning, or markdown markers '
        '(**, __, ```, ~~) inside any field value; JSON values must be plain text only. '
        'Never include uncertainty markers like "or something similar", alternative title candidates, '
        'or instruction-like phrases such as "return one event object" or "second row" inside any field value. '
        'Choose the final title silently. '
        'Title must be the attendee-facing event name, not a poster service heading. '
        'If message text/caption contains a named event and OCR contains only schedule/service headings '
        'like a date, weekday, time, "НАЧАЛО В ...", "БИЛЕТЫ", "РЕГИСТРАЦИЯ", price, age limit, or venue label, '
        'keep the named event from message text as title and use OCR only to fill date/time/venue/ticket fields. '
        'Before returning, audit every title: if it is only a schedule/service heading and the caption has a named '
        'headline, replace the title with that named headline. Example: caption "Второй Большой киноквиз!" plus '
        'OCR "24 АПРЕЛЯ / НАЧАЛО В 19:00" must return title "Второй Большой киноквиз", date "2026-04-24", time "19:00". '
        'Do not emit placeholder events that have empty title and empty date; if you cannot anchor an event to '
        'at least a real title or a real date from the text/OCR, do not include it at all. '
        'Never emit empty JSON objects ({}) or venue-only rows as list items. '
        'Use evidence from both message text and OCR. If OCR contains venue, hall/floor, city, exact date, exact time, '
        'or better speaker/title spelling, merge those facts into the event object. '
        'Prefer filling location_name and location_address whenever the source or OCR gives enough evidence. '
        'location_name must be a venue/place name, not arbitrary nearby text: never copy a descriptive sentence, '
        'speaker biography, schedule commentary, film metadata, ticket instruction, or event description into location_name. '
        'If a schedule groups items under a hall/room label such as "Кинозал:" or "Атриум:" and source context names '
        'the museum/theatre/venue, use the host venue as location_name; do not return only the hall label as the venue. '
        'If the venue is not grounded, leave location_name empty rather than filling it with prose. '
        'Do not use generic placeholder venue names like "музей", "галерея", "пространство", or "площадка" '
        'unless that exact full venue name is explicitly stated in text or OCR. '
        'If a post clearly invites attendance to one lecture/talk/meetup/excursion/event and text or OCR gives an exact date/time, '
        'do NOT return [] only because some venue, city, or ticket fields remain unresolved; return one best-effort event object and leave unresolved text fields empty. '
        'For such a clearly invited single lecture/talk, prefer filling date/time from text or OCR rather than leaving them empty. '
        'If only the title is reliable at extraction time, still prefer one best-effort lecture row over [] so downstream OCR/date merge can complete it. '
        'For a clearly invited single lecture/talk with one supported start datetime, return exactly one event row '
        'and merge the concrete lecture title, OCR date/time, and any supported venue/hosting/ticket facts into that row. '
        'A museum-hosted lecture invitation remains an event even when the venue is only implicit from phrases like '
        '"билеты продаются на сайте и в кассе музея"; keep the lecture and leave unresolved venue fields empty if needed. '
        'Use source context only as weak hosting context for such museum/library/venue-hosted posts; '
        'do not copy source context verbatim into title, raw_excerpt, or venue fields. '
        'Do not split one real event into an extra title-only row just because OCR or a poster repeats a subtitle, slogan, series label, or heading. '
        'Every additional event must have its own distinct date/time or clearly separate attendance intent; otherwise omit the duplicate row. '
        'If one lecture/talk has both a cycle/series label and a concrete lecture title, return exactly one row for the attendable lecture; '
        'keep the cycle/series label in raw_excerpt/search_digest, not as a second event row. '
        'Do not create a second row that contains only venue/location fields for the same lecture. '
        'Title must be the event name (not just a date, weekday, or time). '
        'Prefer concise human event titles; for talks/lectures/meetups keep project or series context in raw_excerpt/search_digest, not inside an overlong title. '
        'If a post says "в разделе X на выставке Y", title should usually be the main exhibition Y, not the subsection X, '
        'unless X is explicitly announced as its own separate attendable event. '
        'If a post announces the opening of an exhibition and also says the same exhibition will run until a later end_date, '
        'usually return ONE event object for that exhibition: keep the opening datetime as date/time and the exhibition run in end_date, '
        'instead of splitting "opening" and "exhibition" into two separate events, unless the post clearly advertises two separately attendable events. '
        'If the message begins with a date marker like "19.02, четверг" treat it as a date, not a title. '
        'Title must not start with punctuation like commas. '
        'raw_excerpt should be a short (1-3 sentences) excerpt from the message without adding new facts. '
        'Open calls / конкурсный отбор / приём заявок / набор участников are NOT events to attend. Return [] for such posts. '
        'Institution work-hours notices are NOT events: if a post is only about "график работы", "режим работы", '
        '"часы работы", "санитарный день", or that a venue is closed/not working, return []. '
        'But do NOT classify a post as a work-hours notice merely because it mentions a museum/library venue, '
        'a street/address such as "Музейная аллея", weekdays, dates, or times. '
        'If it announces attendee-facing lectures, shows, talks, workshops, excursions, or festival program slots '
        'with concrete dates/times, extract those events even when they happen at a museum or library. '
        'Official city notices about развод мостов / разводка мостов ARE events: extract them as public city events, '
        'even when the purpose is mobility planning rather than entertainment. '
        'For @klgdcity bridge-lifting notices, use title like "Развод мостов" plus grounded bridge names if present; '
        'use relative words such as "сегодня" only against the message date, and split multiple nights into multiple events. '
        'Pure retrospective reports of completed events ("прошло мероприятие", "ленту развернули", "приняли участие") '
        'are NOT new events to attend unless the same post also explicitly invites attendance at a future dated event. '
        'Fundraising-only posts ("сбор средств", "помогите собрать"), standalone video/blog/content pieces without a real invite, '
        'and book reviews/sales are NOT events to attend. Return [] for such posts. '
        'Date is REQUIRED for dated events: never invent a date from the message date. '
        'For non-exhibition single events (lecture/talk/meetup/excursion/etc.), if neither message text nor OCR '
        'contains an explicit event date or a relative date anchor like "сегодня", "завтра", or "послезавтра", '
        'return [] rather than using message_date as the event date. '
        'Message date is only context for resolving explicit relative anchors; it is not the event date by default. '
        'For exhibitions/fairs: allow missing time, but require an explicit date range or an explicit end_date ("до ..." / "по ..."). '
        'For museum/exhibition posts about currently displayed works, artists, or sections, prefer one ongoing exhibition card over [] or {} '
        'when the post clearly refers to a real exhibition/display in the present tense, even if this particular post does not restate the full range. '
        'In that case you MAY use message_date as an "as-of" merge date and keep unresolved venue fields empty. '
        'For museum posts spotlighting one artist or one body of work currently shown in the museum, prefer one exhibition card with event_type="выставка" '
        'and the best attendee-facing title instead of returning [] or {}. '
        'If explicit start date is missing but end_date exists, you MAY set date to message date as an "as-of" date for merging. '
        'Do not invent end_date for single-date events. '
        'Do not include hashtags in title, raw_excerpt, or search_digest. '
        'If OCR contains an explicit date or time, prefer it over the message date. '
        'If a date is missing a year, infer it from the message date and choose '
        'the nearest upcoming date relative to that message date. '
        'city must be the city of the venue where attendees physically go, grounded in the venue address from text or OCR. '
        'Do NOT copy a city that appears only as (a) a parenthetical origin/collection note '
        '(e.g. "(Санкт-Петербург)" describing a museum collection origin), or '
        '(b) a biographical/affiliation mention of a speaker, author, curator, organizer, or institution '
        '(e.g. "лектор — Борис Мегорский, заведующий отделом Российской национальной библиотеки" does not put the event in Saint Petersburg). '
        'If the supported venue/address string itself already contains "Калининград" or another explicit city, that venue city wins over every other city mention. '
        'If the venue line ends with a city like "Калининград", use that city; if no venue city is supported, leave city empty rather than guess. '
        'event_type must be a single lowercase Russian noun: концерт, выставка, лекция, спектакль, встреча, '
        'ярмарка, фестиваль, мастер-класс, кинопоказ, стендап, экскурсия, акция, экспозиция. '
        'Never emit English event_type tokens like "exhibition", "meetup", "party", "stand-up"; '
        'use "" if unsure rather than guessing. '
        + date_context + source_context + '\n'
        'Message text:\n' + content
    )
    if schedule_like:
        text = '[]'
    else:
        try:
            text = await _call_model('text', prompt, response_schema=EVENT_ARRAY_SCHEMA)
        except Exception as exc:
            logger.warning('extract_events failed: %s', exc)
            text = '[]'
    data = _safe_json(text)
    if data is None:
        fix_prompt = (
            'Fix and return valid JSON only. '
            'Do not include any extra text, inline comments (//, #), meta-commentary, or markdown markers (**, __). '
            'Input:\n' + text
        )
        try:
            fixed_text = await _call_model('text', fix_prompt, response_schema=EVENT_ARRAY_SCHEMA)
            data = _safe_json(fixed_text)
        except Exception as exc:
            logger.warning('extract_events json_fix failed: %s', exc)
    if isinstance(data, dict) and isinstance(data.get('events'), list):
        out = data['events']
    elif isinstance(data, list):
        out = data
    else:
        out = []
    if not isinstance(out, list):
        out = []
    out = await _repair_service_heading_titles(
        message_text=message_text_only,
        ocr_text=ocr_text,
        date_context=date_context,
        source_context_line=source_context_line,
        events=out,
    )
    # Guardrails: prevent pseudo-events from open calls/applications, and avoid
    # inventing event start dates from message date unless there's an explicit anchor.
    try:
        msg_date_iso = msg_date.isoformat() if msg_date else None
    except Exception:
        msg_date_iso = None
    open_call_re = re.compile(
        r"\b(open\s*call|опен\s*колл|опенколл|конкурсн\w*\s+отбор|при[её]м\s+заявок|подать\s+заявк\w*|заявк\w*\s+принима\w*)\b",
        re.IGNORECASE | re.UNICODE,
    )
    anchor_re = re.compile(
        r"\b(сегодня|завтра|послезавтра)\b"
        r"|\b\d{1,2}[./]\d{1,2}(?:[./](?:19|20)\d{2})?\b"
        r"|\b\d{1,2}\s+(?:января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\b",
        re.IGNORECASE | re.UNICODE,
    )
    has_anchor = bool(anchor_re.search(content) or (ocr_text and anchor_re.search(ocr_text)))
    def _lacks_supported_non_exhibition_date(ev: dict) -> bool:
        if not msg_date_iso or has_anchor or not isinstance(ev, dict):
            return False
        if str(ev.get('end_date') or '').strip():
            return False
        event_type = str(ev.get('event_type') or '').strip().casefold()
        # Exhibition/display cards are the only prompt-approved "as-of"
        # message-date use. Single lectures/talks/excursions need a supported date.
        if event_type in {'выставка', 'экспозиция', 'ярмарка', 'фестиваль'}:
            return False
        date_value = str(ev.get('date') or '').strip()
        if not date_value:
            return True
        if date_value != msg_date_iso:
            return False
        return True

    if open_call_re.search(content) or (ocr_text and open_call_re.search(ocr_text)):
        return []
    if msg_date_iso and not has_anchor:
        out = [
            e
            for e in out
            if not _lacks_supported_non_exhibition_date(e)
        ]
    bridge_fallback = _bridge_event_fallback(
        content,
        message_date=message_date,
        source_username=source_username,
    )
    if bridge_fallback and not _bridge_llm_output_is_usable(out, expected_count=len(bridge_fallback)):
        bridge_rescue = await _extract_bridge_events_rescue(
            content,
            message_date=message_date,
            source_username=source_username,
            source_title=source_title,
        )
        if _bridge_llm_output_is_usable(bridge_rescue, expected_count=len(bridge_fallback)):
            out = bridge_rescue
        else:
            out = bridge_fallback

    if not out and schedule_like:
        shared_schedule_context = content[:1200]
        if len(content) > 1200:
            shared_schedule_context += "\n...\n" + content[-1200:]
        schedule_header_re = re.compile(
            r'(?im)^\s*\d{1,2}\s+(?:января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\b[^\n]*$'
        )
        headers = list(schedule_header_re.finditer(content))
        schedule_blocks: list[str] = []
        for idx, match in enumerate(headers):
            end = headers[idx + 1].start() if idx + 1 < len(headers) else len(content)
            block = content[match.start():end].strip()
            block = re.split(r'\n\s*(?:Рассказывать о событиях|#|📸|Присылайте\s+ваши\s+фото)', block, maxsplit=1)[0].strip()
            if len(re.findall(r'\b\d{1,2}[.:]\d{2}\b', block)) >= 1:
                block_lines = [line.strip() for line in block.splitlines() if line.strip()]
                header_line = block_lines[0] if block_lines else block[:120]
                timed_lines = [line for line in block_lines[1:] if re.search(r'\b\d{1,2}[.:]\d{2}\b', line)]
                if timed_lines:
                    for chunk_start in range(0, len(timed_lines), 3):
                        schedule_blocks.append((header_line + '\n' + '\n'.join(timed_lines[chunk_start:chunk_start + 3])).strip())
                else:
                    schedule_blocks.append(block[:1200])
        if not schedule_blocks:
            schedule_blocks = [content[:1800]]
        schedule_events: list[dict] = []
        for schedule_block in schedule_blocks[:8]:
            schedule_prompt = (
                'Extract attendable schedule items from one small Telegram timetable chunk as strict JSON array. '
                'The chunk starts with one date header like "18 АПРЕЛЯ" followed by up to three time lines. '
                'Each returned event must correspond to one real schedule line with its own time under this date header. '
                'Fields per event: title, date (YYYY-MM-DD), time (HH:MM or empty), '
                'end_date (YYYY-MM-DD or empty string), location_name, location_address, city, '
                'ticket_link, ticket_price_min, ticket_price_max, ticket_status, raw_excerpt, '
                'event_type, emoji, is_free, pushkin_card, search_digest, festival. '
                'Infer the year from message date and choose the nearest upcoming date for the day/month header. '
                'Keep excursions, feedings, public talks, and other visitor-facing timetable items. '
                'Ignore photo-rubric text, hashtags, channel promotion, and generic ticket-sales boilerplate. '
                'Never use placeholder literals like "title" as a title; copy the attendee-facing name from the time line. '
                'location_name must be the shared venue/place for the timetable, not descriptive prose from surrounding text. '
                'Use the full message context below to recover shared venue/address facts that are outside this small day-block '
                '(for example a trailing "📍Остров Канта" line applies to all schedule rows in the block). '
                'If the chunk only has a hall/room label and the full message/source context names the host venue, use the host venue; '
                'otherwise leave location_name empty. '
                'Never emit empty JSON objects ({}) or venue-only rows. '
                'Never include inline comments, instruction-like text, uncertainty markers, or markdown markers inside any field value. '
                'Use source context only as weak venue context; do not copy it verbatim into title or raw_excerpt. '
                + date_context + '\n'
                + (source_context_line + '\n' if source_context_line else '')
                + 'Full message context for shared venue/address facts:\n' + shared_schedule_context + '\n'
                + 'Schedule day-block:\n' + schedule_block
            )
            try:
                text_schedule = await _call_model('text', schedule_prompt, response_schema=EVENT_ARRAY_SCHEMA)
                data_schedule = _safe_json(text_schedule)
                if isinstance(data_schedule, dict) and isinstance(data_schedule.get('events'), list):
                    schedule_events.extend(data_schedule['events'])
                elif isinstance(data_schedule, list):
                    schedule_events.extend(data_schedule)
            except Exception as exc:
                logger.warning('extract_events schedule rescue failed: %s', exc)
        out = schedule_events
    # Narrow LLM rescue-pass for single invited lecture/talk posts where the
    # general extractor returned [] despite OCR carrying the date/time anchor.
    if not out and ocr_text and re.search(
        r'\b(приглашаем\s+на\s+(?:лекци|встреч|экскурс|показ)|лекци[яюи]|лектор[а-яё]*|встреч[ауе]|экскурси[яюи]|кинопоказ\w*)\b',
        content,
        re.IGNORECASE,
    ):
        lecture_prompt = (
            'Extract a single attendable lecture/talk/meetup/excursion event from Telegram text as strict JSON array. '
            'Return [] only if there is clearly no real attendable single event. '
            'Fields per event: title, date (YYYY-MM-DD), time (HH:MM or empty), '
            'end_date (YYYY-MM-DD or empty string), location_name, location_address, city, '
            'ticket_link, ticket_price_min, ticket_price_max, ticket_status, raw_excerpt, '
            'event_type, emoji, is_free, pushkin_card, search_digest, festival. '
            'Use empty string for unknown text fields. '
            'If text says "Приглашаем на лекцию/встречу/экскурсию/показ" and OCR gives one explicit date/time, '
            'that is enough to keep one best-effort event row even if venue fields stay empty. '
            'Prefer one row over [] for such a clearly invited single event. '
            'Merge OCR date/time into that row; infer the year from message date when needed. '
            'Do not use message_date itself as the event date unless the text/OCR contains an explicit relative date anchor '
            'such as "сегодня", "завтра", or "послезавтра". '
            'If text invites a lecture/talk but neither text nor OCR gives a date or relative date anchor, return [] '
            'rather than a row dated by message_date. '
            'Never emit empty JSON objects ({}) or venue-only rows as list items. '
            'Never include inline comments, instruction-like text, uncertainty markers, or markdown markers inside any field value. '
            'Use source context only as weak hosting context; do not copy it verbatim into title, raw_excerpt, or venue fields. '
            'event_type must be a single lowercase Russian noun like лекция, встреча, экскурсия, кинопоказ. '
            'If the post is not a single attendable lecture/talk/meetup/excursion event, return []. '
            + date_context + '\n'
            + (source_context_line + '\n' if source_context_line else '')
            + 'Message text:\n' + content
        )
        try:
            text_lecture = await _call_model('text', lecture_prompt, response_schema=EVENT_ARRAY_SCHEMA)
            data_lecture = _safe_json(text_lecture)
            if isinstance(data_lecture, dict) and isinstance(data_lecture.get('events'), list):
                out = data_lecture['events']
            elif isinstance(data_lecture, list):
                out = data_lecture
        except Exception as exc:
            logger.warning('extract_events lecture rescue failed: %s', exc)
    if not out and re.search(r'\b(?:на\s+выставке|выставк[аеуы]?)\s+[«"].+?[»"]', content, re.IGNORECASE | re.DOTALL):
        named_exhibition_prompt = (
            'Extract one named ongoing exhibition event from Telegram text as strict JSON array. '
            'Return [] only if the quoted name is clearly not an exhibition title. '
            'Fields per event: title, date (YYYY-MM-DD), time (HH:MM or empty), '
            'end_date (YYYY-MM-DD or empty string), location_name, location_address, city, '
            'ticket_link, ticket_price_min, ticket_price_max, ticket_status, raw_excerpt, '
            'event_type, emoji, is_free, pushkin_card, search_digest, festival. '
            'If the post says "в разделе X на выставке Y", title must be the main exhibition Y, not the section X. '
            'Do not require the post to restate the exhibition date range; this rescue path is specifically for current named exhibition posts without full dates. '
            'Phrases like "на выставке Y можно увидеть ..." are sufficient evidence of a current display. '
            'For a kept ongoing named exhibition, set date to the Message date date part as an as-of merge date '
            'and set event_type exactly to "выставка". '
            'Use empty string for unknown venue/address/city fields; never output "unknown". '
            'Never emit empty JSON objects ({}) or venue-only rows. '
            'Never include inline comments, instruction-like text, uncertainty markers, or markdown markers inside any field value. '
            + date_context + '\n'
            + (source_context_line + '\n' if source_context_line else '')
            + 'Message text:\n' + content
        )
        try:
            text_named_exhibition = await _call_model('text', named_exhibition_prompt, response_schema=EVENT_ARRAY_SCHEMA)
            data_named_exhibition = _safe_json(text_named_exhibition)
            if isinstance(data_named_exhibition, dict) and isinstance(data_named_exhibition.get('events'), list):
                out = data_named_exhibition['events']
            elif isinstance(data_named_exhibition, list):
                out = data_named_exhibition
        except Exception as exc:
            logger.warning('extract_events named exhibition rescue failed: %s', exc)

    # Fallback for ongoing exhibition posts where generic extraction may return []
    # due to missing explicit start date/time.
    if not out and re.search(r'\b(выставк\w*|экспозици\w*|ярмарк\w*)\b', content, re.IGNORECASE):
        exhibition_prompt = (
            'Extract exhibition/fair events from Telegram text as strict JSON array. '
            'Return [] only if there is clearly no exhibition/fair event. '
            'Fields per event: title, date (YYYY-MM-DD), time (HH:MM or empty), '
            'end_date (YYYY-MM-DD or empty string), location_name, location_address, city, '
            'ticket_link, ticket_price_min, ticket_price_max, ticket_status, raw_excerpt, '
            'event_type, emoji, is_free, pushkin_card, search_digest, festival. '
            'Use empty string for unknown text fields. Never output the literal "unknown" in any field. '
            'Never include inline comments ("//", "#"), meta-commentary, reasoning, or markdown markers '
            '(**, __, ```) inside any field value. '
            'Never include uncertainty markers like "or something similar", alternative title candidates, or instruction-like text inside any field value. '
            'Do not emit placeholder events with empty title and empty date. '
            'Never emit empty JSON objects ({}) or venue-only rows as list items. '
            'Do not use generic placeholder venue names like "музей", "галерея", "пространство", or "площадка" '
            'unless that exact full venue name is explicitly stated in text or OCR. '
            'Set event_type to "выставка" (or "ярмарка" where appropriate); never English tokens like "exhibition". '
            'Open calls / конкурсный отбор / приём заявок are NOT events to attend. Return [] for such posts. '
            'Require an explicit date range or an explicit end_date ("до ..." / "по ..."). '
            'Exception for ongoing named exhibitions: if the post clearly refers to a specific exhibition by its explicit title '
            '(for example "на выставке «Y»" or "выставка «Y»") but discusses a section/update inside that same exhibition, '
            'you MAY still return one exhibition object for Y with date=message_date as an "as-of" merge date, '
            'even when this post does not restate the full date range. '
            'More generally, for museum/exhibition posts about currently displayed works, artists, or sections, prefer one ongoing exhibition card over [] or {} '
            'when the post clearly refers to a real exhibition/display in the present tense, even if this particular post does not restate the full range. '
            'This includes museum artist/work spotlight posts even when the word "выставка" is not repeated in that specific post. '
            'For museum posts spotlighting one artist or one body of work currently shown in the museum, prefer one exhibition card with event_type="выставка" '
            'and the best attendee-facing title instead of returning [] or {}. '
            'Do not return [] solely because the post is written as a museum editorial spotlight instead of a formal exhibition announcement. '
            'Use source context only as weak museum-host context; do not copy it verbatim into title, raw_excerpt, or location fields. '
            'For such museum spotlight posts, leave location_name empty rather than inventing a generic placeholder like "музей" if the full venue name is not stated. '
            'If explicit start date is missing but end_date exists, you MAY set date to message date as an "as-of" date for merging. '
            'city must be the city of the exhibition venue, grounded in the venue address; '
            'ignore parenthetical origin/collection notes and biographical/affiliation mentions of curators, authors, '
            'or the institution that owns the collection (the venue address wins). '
            'If the supported venue/address string itself already contains "Калининград" or another explicit city, that venue city wins over every other city mention. '
            'If no venue city is supported, leave city empty rather than guess. '
            'Do not include hashtags in any text fields. '
            + date_context + '\n'
            + (source_context_line + '\n' if source_context_line else '')
            + 'Message text:\n' + content
        )
        try:
            text_exh = await _call_model('text', exhibition_prompt, response_schema=EVENT_ARRAY_SCHEMA)
            data_exh = _safe_json(text_exh)
            if isinstance(data_exh, dict) and isinstance(data_exh.get('events'), list):
                out = data_exh['events']
            elif isinstance(data_exh, list):
                out = data_exh
        except Exception as exc:
            logger.warning('extract_events exhibition fallback failed: %s', exc)
    if not out and re.search(r'\b(музе\w*|художник\w*|картин\w*|полотно|натюрморт|пейзаж\w*|архиве\s+музея|архив\s+музея)\b', content, re.IGNORECASE):
        museum_prompt = (
            'Extract a single ongoing museum exhibition/display card from Telegram text as strict JSON array. '
            'Return [] only if the post is clearly not about a current museum display/exhibition. '
            'Fields per event: title, date (YYYY-MM-DD), time (HH:MM or empty), '
            'end_date (YYYY-MM-DD or empty string), location_name, location_address, city, '
            'ticket_link, ticket_price_min, ticket_price_max, ticket_status, raw_excerpt, '
            'event_type, emoji, is_free, pushkin_card, search_digest, festival. '
            'Use empty string for unknown text fields. '
            'For museum spotlight posts about one artist, artwork, or body of work, prefer one attendee-facing exhibition card '
            'with event_type="выставка" and date=message_date as an "as-of" merge date rather than []. '
            'If you return an event in this rescue path, do not leave date or event_type empty: '
            'set date=message_date as the as-of merge date and set event_type="выставка". '
            'Use source context only as weak museum-host context; do not copy it verbatim into title, raw_excerpt, or location fields. '
            'If the full venue name is not stated, leave location_name empty rather than generic placeholders like "музей". '
            'Do not emit empty JSON objects ({}) or venue-only rows. '
            'Never include inline comments, instruction-like text, uncertainty markers, or markdown markers inside any field value. '
            'If the text is clearly only archival/background and does not describe a current museum display, return []. '
            + date_context + '\n'
            + (source_context_line + '\n' if source_context_line else '')
            + 'Message text:\n' + content
        )
        try:
            text_museum = await _call_model('text', museum_prompt, response_schema=EVENT_ARRAY_SCHEMA)
            data_museum = _safe_json(text_museum)
            if isinstance(data_museum, dict) and isinstance(data_museum.get('events'), list):
                out = data_museum['events']
            elif isinstance(data_museum, list):
                out = data_museum
        except Exception as exc:
            logger.warning('extract_events museum spotlight rescue failed: %s', exc)
        if out and any(
            isinstance(ev, dict)
            and str(ev.get('title') or '').strip()
            and not str(ev.get('date') or '').strip()
            and not str(ev.get('event_type') or '').strip()
            for ev in out
        ):
            museum_fix_prompt = (
                'Repair a museum spotlight extraction as strict JSON array. '
                'If the post supports a current museum display/exhibition, keep one attendee-facing exhibition card '
                'with date set exactly to the Message date date part (YYYY-MM-DD) as the as-of merge date '
                'and event_type set exactly to "выставка". '
                'If the post does not support a current display, return [] instead of partial placeholder rows. '
                'Do not leave date or event_type empty on a kept museum exhibition card. '
                'A kept card with an empty date or empty event_type is invalid JSON for this task; repair those fields or return []. '
                'Never emit empty JSON objects ({}) or venue-only rows. '
                + date_context + '\n'
                + (source_context_line + '\n' if source_context_line else '')
                + 'Original message text:\n' + content + '\n\n'
                + 'Current extracted JSON:\n' + json.dumps(out, ensure_ascii=False)
            )
            try:
                text_museum_fix = await _call_model('text', museum_fix_prompt, response_schema=EVENT_ARRAY_SCHEMA)
                data_museum_fix = _safe_json(text_museum_fix)
                if isinstance(data_museum_fix, dict) and isinstance(data_museum_fix.get('events'), list):
                    out = data_museum_fix['events']
                elif isinstance(data_museum_fix, list):
                    out = data_museum_fix
            except Exception as exc:
                logger.warning('extract_events museum spotlight repair failed: %s', exc)
    if msg_date_iso and not has_anchor:
        out = [e for e in out if not _lacks_supported_non_exhibition_date(e)]
    out = await _repair_service_heading_titles(
        message_text=message_text_only,
        ocr_text=ocr_text,
        date_context=date_context,
        source_context_line=source_context_line,
        events=out,
    )
    out = _sanitize_extracted_events(out)
    return (out or [])[: max(1, int(MAX_EVENTS_PER_MESSAGE))]


async def ocr_image(image_bytes: bytes, message_date: str | None = None):
    if not ENABLE_OCR:
        return None, None
    try:
        img = Image.open(io.BytesIO(image_bytes))
    except Exception:
        return None, None
    date_context = f"Message date (ISO, UTC): {message_date}" if message_date else 'Message date: unknown'
    prompt = (
        'Extract readable text from the image. '
        'Return JSON: {"text": "...", "title": "..."}. '
        'If no text, return {"text": ""}. '
        + date_context
    )
    try:
        text = await _call_model('vision', prompt, images=[img], response_schema=OCR_SCHEMA)
    except Exception as exc:
        logger.warning('ocr_image failed: %s', exc)
        return None, None
    data = _safe_json(text)
    if data is None:
        fix_prompt = (
            'Fix and return valid JSON only. '
            'Do not include any extra text, inline comments (//, #), meta-commentary, or markdown markers (**, __). '
            'Input:\n' + text
        )
        try:
            fixed_text = await _call_model('vision', fix_prompt, images=[img], response_schema=OCR_SCHEMA)
            data = _safe_json(fixed_text)
        except Exception as exc:
            logger.warning('ocr_image json_fix failed: %s', exc)
    if isinstance(data, dict):
        text = data.get('text') or ''
        title = data.get('title') or None
        if text and not title:
            title = text.split('\n', 1)[0].strip() if text else None
        return text or None, title
    return None, None


async def scan_source(client: TelegramClient, source: dict) -> dict:
    username = (source.get('username') or '').strip()
    if not username:
        logger.warning('source.skip reason=missing_username')
        return {'messages': [], 'source_meta': None}
    entity = await tg_call(f'get_entity:{username}', client.get_entity, username)
    s_type = _source_type(entity)
    source_meta = await _build_source_meta(client, username, entity, s_type)
    last_id = source.get('last_scanned_message_id') or 0
    force_ids = source.get('force_message_ids') or []
    try:
        force_ids = [int(x) for x in (force_ids or []) if str(x).strip().isdigit()]
    except Exception:
        force_ids = []
    force_ids = sorted(set(force_ids))
    default_location = source.get('default_location')
    default_ticket_link = source.get('default_ticket_link')

    cutoff = datetime.now(timezone.utc) - timedelta(days=MAX_DAYS_BACK)

    latest_id = None
    latest_date = None
    try:
        latest = await tg_call(f'get_latest:{username}', client.get_messages, entity, limit=1)
        if latest:
            latest_msg = latest[0]
            latest_id = latest_msg.id
            latest_date = _message_date_iso(latest_msg)
    except Exception as exc:
        logger.warning('source.latest_failed %s: %s', username, exc)

    logger.info(
        'source.start username=%s type=%s last_id=%s latest_id=%s latest_date=%s cutoff=%s force_ids=%s',
        username,
        s_type,
        last_id or 0,
        latest_id,
        latest_date,
        cutoff.isoformat(),
        len(force_ids),
    )
    if not force_ids and last_id and latest_id and latest_id <= last_id:
        logger.info(
            'source.skip reason=no_new_messages username=%s last_id=%s latest_id=%s',
            username,
            last_id,
            latest_id,
        )
        return {'messages': [], 'source_meta': source_meta}

    messages_out = []
    views_vals = []
    likes_vals = []
    processed = 0
    messages_with_events = 0
    events_total = 0
    first_id = None
    last_id_seen = None
    first_date = None
    last_date = None
    cutoff_hit = False

    resume_max_id = 0
    flood_attempts = 0
    done = False
    media_downloaded = 0
    media_cap = MAX_MEDIA_PER_SOURCE

    async def _process_one(msg) -> bool:
        nonlocal processed, messages_with_events, events_total, first_id, last_id_seen, first_date, last_date
        nonlocal resume_max_id, media_downloaded

        msg_date = _message_date_iso(msg)
        if first_id is None:
            first_id = msg.id
            first_date = msg_date
        last_id_seen = msg.id
        last_date = msg_date
        if msg.id:
            resume_max_id = max(0, msg.id - 1)

        text_raw = msg.message or ''
        entities = list(getattr(msg, 'entities', None) or [])
        text_for_links = strip_custom_emoji_entities(text_raw, entities)
        # Extract links using a stable-offset string (before any line dropping),
        # because Telegram text-url entities reference UTF-16 offsets.
        text = strip_promo_lines(text_for_links)

        # Extract links from message text/entities/buttons.
        # We keep Telegram post links separately for linked-source processing, but also emit
        # all http(s) links into the JSON payload for server-side best-effort ticket inference.
        linked_source_urls = []
        links_meta = []  # list[{url, text, source}]
        link_spans = []  # list[{url, text, offset}]; offset may be None for buttons

        def _is_tg_post_url(u: str) -> bool:
            return bool(re.search(r'(?i)t\.me/[^/\s]+/\d+', u or ''))

        def _add_link(url: str | None, text_label: str | None, offset: int | None, source: str) -> None:
            u = (url or '').strip()
            if not u:
                return
            if not u.startswith(('http://', 'https://')):
                # Telegram often stores urls without scheme.
                u = 'https://' + u
            if not re.match(r'^https?://', u, flags=re.I):
                return
            key = u.lower().rstrip('/')
            if key in {x['url'].lower().rstrip('/') for x in links_meta}:
                return
            links_meta.append({'url': u, 'text': (text_label or '').strip() or None, 'source': source})
            link_spans.append({'url': u, 'text': (text_label or '').strip() or None, 'offset': offset})
            if _is_tg_post_url(u):
                linked_source_urls.append(u)

        # Telegram post links in plain text (linked sources).
        for m in re.finditer(r'(https?://)?t\.me/[^/\s]+/\d+(?:\?single)?', text_for_links):
            raw = m.group(0)
            _add_link(raw, raw, m.start(), 'regex_tg')

        # Explicit http(s) links in plain text.
        for m in re.finditer(r'https?://\S+', text_for_links):
            raw = m.group(0)
            _add_link(raw, raw, m.start(), 'regex_http')

        if entities and text_for_links:
            for ent in entities:
                url = None
                label = None
                offset = int(getattr(ent, 'offset', 0) or 0)
                length = int(getattr(ent, 'length', 0) or 0)
                if isinstance(ent, MessageEntityTextUrl):
                    url = (getattr(ent, 'url', None) or '').strip()
                    if length > 0:
                        label = text_for_links[offset: offset + length].strip()
                elif isinstance(ent, MessageEntityUrl):
                    if length > 0:
                        url = text_for_links[offset: offset + length].strip()
                        label = url
                if url:
                    _add_link(url, label, offset, 'entity')

        # Inline buttons (e.g. "More info", "Билеты", "Регистрация").
        try:
            btn_rows = getattr(msg, 'buttons', None) or []
            for row in btn_rows:
                for btn in (row or []):
                    b_url = getattr(btn, 'url', None)
                    b_text = getattr(btn, 'text', None)
                    if b_url:
                        _add_link(str(b_url), str(b_text or ''), None, 'button')
        except Exception:
            pass

        linked_urls_clean = []
        seen_linked = set()
        self_url = f'https://t.me/{username}/{msg.id}'
        for raw in linked_source_urls:
            mm = re.search(r't\.me/([^/]+)/([0-9]+)', raw)
            if not mm:
                continue
            canonical = f'https://t.me/{mm.group(1)}/{int(mm.group(2))}'
            key = canonical.lower()
            if key == self_url.lower() or key in seen_linked:
                continue
            seen_linked.add(key)
            linked_urls_clean.append(canonical)
        msg_date_obj = msg.date
        msg_date_val = msg_date_obj.date() if msg_date_obj else None

        views = getattr(msg, 'views', None)
        likes = _message_likes(msg)
        if isinstance(views, int):
            views_vals.append(views)
        if isinstance(likes, int):
            likes_vals.append(likes)

        is_giveaway = is_ticket_giveaway(text)
        if is_giveaway:
            # Do not mutate message text deterministically; let LLM ignore giveaway mechanics.
            logger.info('message.flag reason=ticket_giveaway username=%s message_id=%s', username, msg.id)

        skip_promo = is_promo_or_congrats(text)
        if skip_promo:
            logger.info('message.skip reason=promo_or_congrats username=%s message_id=%s', username, msg.id)

        posters = []
        videos = []
        video_status = None
        ocr_text = None
        ocr_title = None
        image_bytes = None
        grouped_id = getattr(msg, 'grouped_id', None)
        has_video = False
        try:
            if getattr(msg, 'video', None) or getattr(msg, 'video_note', None):
                has_video = True
            else:
                doc = getattr(msg, 'document', None)
                mt = getattr(doc, 'mime_type', None) or ''
                if isinstance(mt, str) and mt.lower().startswith('video/'):
                    has_video = True
        except Exception:
            has_video = False
        if has_video and SUPABASE_VIDEOS_MODE == 'always':
            try:
                await human_sleep(HUMAN_MEDIA_DELAY_MIN, HUMAN_MEDIA_DELAY_MAX)
                mime_v, ext_v = _video_mime_ext_from_message(msg)
                doc = getattr(msg, 'video', None) or getattr(msg, 'video_note', None) or getattr(msg, 'document', None)
                doc_id = getattr(doc, 'id', None) if doc is not None else None
                size_hint = None
                try:
                    size_hint = int(getattr(doc, 'size', None) or 0) if doc is not None else None
                except Exception:
                    size_hint = None
                object_path_hint = None
                if doc_id:
                    object_path_hint = f"{SUPABASE_VIDEOS_PREFIX}/tg/{int(doc_id)}.{ext_v}"
        
                # Fast path: reuse the same Telegram file already present in Supabase Storage
                # (saves both download and upload traffic).
                if object_path_hint:
                    exists = _supabase_storage_object_exists(bucket=SUPABASE_MEDIA_BUCKET, object_path=object_path_hint)
                    if exists is True:
                        supa_v_path = object_path_hint
                        supa_v_url = SUPABASE_URL.rstrip('/') + f"/storage/v1/object/public/{SUPABASE_MEDIA_BUCKET}/{object_path_hint}"
                        videos.append({
                            'size_bytes': size_hint,
                            'mime_type': mime_v,
                            'supabase_url': supa_v_url,
                            'supabase_path': supa_v_path,
                            'status': 'supabase',
                        })
                        video_status = 'supabase'
        
                if not video_status:
                    max_bytes = int(max(1.0, float(TG_MONITORING_VIDEO_MAX_MB)) * 1024 * 1024)
                    if size_hint and size_hint > max_bytes:
                        logger.info(
                            'video.skip too_large_hint username=%s message_id=%s size_mb=%.2f max_mb=%.2f',
                            username,
                            msg.id,
                            size_hint / (1024 * 1024),
                            TG_MONITORING_VIDEO_MAX_MB,
                        )
                        video_status = 'skipped:too_large'
                    else:
                        video_bytes = await tg_call(
                            f'download_video:{username}:{msg.id}',
                            client.download_media,
                            msg,
                            bytes,
                        )
                        if not video_bytes:
                            video_status = 'skipped:download_failed'
                        else:
                            size_bytes = int(len(video_bytes))
                            if size_bytes > max_bytes:
                                logger.info(
                                    'video.skip too_large username=%s message_id=%s size_mb=%.2f max_mb=%.2f',
                                    username,
                                    msg.id,
                                    size_bytes / (1024 * 1024),
                                    TG_MONITORING_VIDEO_MAX_MB,
                                )
                                video_status = 'skipped:too_large'
                            else:
                                sha_v = hashlib.sha256(video_bytes).hexdigest()
                                supa_v_url, supa_v_path, upload_status = upload_video_to_supabase_storage(
                                    video_bytes,
                                    sha256_hex=sha_v,
                                    mime_type=mime_v,
                                    ext=ext_v,
                                )
                                if supa_v_url or supa_v_path:
                                    videos.append({
                                        'sha256': sha_v,
                                        'size_bytes': size_bytes,
                                        'mime_type': mime_v,
                                        'supabase_url': supa_v_url,
                                        'supabase_path': supa_v_path,
                                        'status': 'supabase',
                                    })
                                    video_status = 'supabase'
                                else:
                                    reason = upload_status or 'upload_failed'
                                    videos.append({
                                        'sha256': sha_v,
                                        'size_bytes': size_bytes,
                                        'mime_type': mime_v,
                                        'status': reason,
                                    })
                                    video_status = f'skipped:{reason}'
            except Exception as exc:
                logger.warning('video process failed for %s/%s: %s', username, msg.id, exc)
                video_status = 'skipped:download_failed'
        elif has_video and SUPABASE_VIDEOS_MODE != 'always':
            video_status = 'skipped:mode_off'
        media_obj = None
        if msg.photo:
            media_obj = msg
        else:
            # Some channels post as a link with a rich preview image (webpage.photo).
            wp = getattr(getattr(msg, 'media', None), 'webpage', None)
            if wp and getattr(wp, 'photo', None):
                media_obj = wp.photo
        if media_obj is not None and media_downloaded < media_cap:
            # Media downloads are the most expensive Telegram calls and often trigger FloodWait.
            # Throttle them and cap per-source to keep monitoring stable.
            await human_sleep(HUMAN_MEDIA_DELAY_MIN, HUMAN_MEDIA_DELAY_MAX)
            try:
                image_bytes = await tg_call(
                    f'download_media:{username}:{msg.id}',
                    client.download_media,
                    media_obj,
                    bytes,
                )
                if image_bytes:
                    media_downloaded += 1
            except Exception as exc:
                logger.warning('media download failed for %s/%s: %s', username, msg.id, exc)
                image_bytes = None
        if image_bytes:
            try:
                sha = _compute_hash(image_bytes)
                phash = _compute_phash(image_bytes)
                catbox_url = None
                if SUPABASE_POSTERS_MODE != 'always' or not POSTER_STORAGE_ENABLED:
                    try:
                        catbox_url = upload_to_catbox(image_bytes)
                    except Exception as exc:
                        logger.warning('catbox upload failed for %s/%s: %s', username, msg.id, exc)
                        catbox_url = None
                supabase_url, supabase_path = None, None
                if SUPABASE_POSTERS_MODE == 'always' or (SUPABASE_POSTERS_MODE == 'fallback' and not catbox_url):
                    supabase_url, supabase_path = upload_to_supabase_storage(image_bytes, sha)
                ocr_text, ocr_title = await ocr_image(image_bytes, message_date=msg_date)
                posters.append({
                    'catbox_url': catbox_url,
                    'supabase_url': supabase_url,
                    'supabase_path': supabase_path,
                    'sha256': sha,
                    'phash': phash,
                    'ocr_text': ocr_text,
                    'ocr_title': ocr_title,
                })
            except Exception as exc:
                logger.warning('media process failed for %s/%s: %s', username, msg.id, exc)

        if is_promo_or_congrats(text, ocr_text):
            logger.info('message.skip reason=promo_or_congrats_ocr username=%s message_id=%s', username, msg.id)
            skip_promo = True
            events = []
            ocr_date_hint, ocr_time_hint = None, None
        else:
            # If OCR reveals giveaway terms, keep text intact; LLM should ignore mechanics.
            if is_ticket_giveaway(text, ocr_text):
                logger.info('message.flag reason=ticket_giveaway_ocr username=%s message_id=%s', username, msg.id)
            linked_texts = []
            text_for_extract = text
            if linked_urls_clean:
                for url in linked_urls_clean[:2]:
                    try:
                        mm = re.search(r't\.me/([^/]+)/([0-9]+)', url)
                        if not mm:
                            continue
                        ln_user = mm.group(1)
                        ln_id = int(mm.group(2))
                        ent2 = await client.get_entity(ln_user)
                        linked_msg = await client.get_messages(ent2, ids=ln_id)
                        lt = (getattr(linked_msg, 'message', None) or '').strip()
                        lt = strip_custom_emoji_entities(lt, getattr(linked_msg, 'entities', None))
                        lt = strip_promo_lines(lt)
                        lt = lt.strip()
                        if lt:
                            lt = lt[:900]
                        if lt and lt not in linked_texts:
                            linked_texts.append(lt)
                    except Exception as exc:
                        logger.info('linked.skip url=%s username=%s message_id=%s: %s', url, username, msg.id, exc)
                        continue
                if linked_texts:
                    text_for_extract = (text + "\\n\\n" + "\\n\\n".join(linked_texts)).strip()
            events = await extract_events(
                text_for_extract,
                ocr_text,
                message_date=msg_date,
                source_username=username,
                source_title=(source_meta or {}).get('title') if isinstance(source_meta, dict) else None,
            )
            ocr_date_hint, ocr_time_hint = _extract_ocr_datetime(ocr_text, msg_date)

        cleaned_events = []
        for ev in events or []:
            if not isinstance(ev, dict):
                continue
            year_hint_source = (ev.get('raw_excerpt') or text or '')
            if msg_date_val and ev.get('date') and not re.search(r'\b20\d{2}\b', year_hint_source):
                try:
                    ev_date = datetime.fromisoformat(ev['date']).date()
                    candidate = date(msg_date_val.year, ev_date.month, ev_date.day)
                    if candidate < msg_date_val and msg_date_val.month == 12 and ev_date.month == 1:
                        candidate = date(msg_date_val.year + 1, ev_date.month, ev_date.day)
                    ev['date'] = candidate.isoformat()
                except Exception:
                    pass
            if len(events or []) == 1:
                if ocr_date_hint:
                    ev['date'] = ocr_date_hint
                if ocr_time_hint:
                    ev['time'] = ocr_time_hint
            if default_location and not ev.get('location_name'):
                ev['location_name'] = default_location
            if default_ticket_link and not ev.get('ticket_link'):
                ev['ticket_link'] = default_ticket_link
            if linked_texts and text_for_extract:
                existing_src = (ev.get('source_text') or '').strip() if isinstance(ev.get('source_text'), str) else ''
                if not existing_src or len(existing_src) < 80:
                    ev['source_text'] = text_for_extract[:2500]
            if linked_urls_clean:
                ev['linked_source_urls'] = linked_urls_clean[:5]
            cleaned_events.append(ev)

        if len(cleaned_events) > MAX_EVENTS_PER_MESSAGE:
            cleaned_events = cleaned_events[:MAX_EVENTS_PER_MESSAGE]
        # Best-effort: map message links to per-event ticket_link.
        # This helps when posts use hidden text-url entities or buttons ("More info", "билеты", "здесь").
        def _ticketish(label: str | None, url: str | None) -> bool:
            t = (label or '').strip().casefold()
            u = (url or '').strip().casefold()
            if any(k in t for k in ['билет', 'регист', 'запис', 'more info', 'подробнее', 'здесь', 'here', 'tickets']):
                return True
            if any(d in u for d in ['timepad.ru', 'kassir.ru', 'qtickets.ru', 'ticketland.ru', 'ticketscloud.com', 'intickets.ru']):
                return True
            return False

        def _ru_month(m: int) -> str:
            return {1:'января',2:'февраля',3:'марта',4:'апреля',5:'мая',6:'июня',7:'июля',8:'августа',9:'сентября',10:'октября',11:'ноября',12:'декабря'}.get(m, '')

        def _date_variants(iso: str) -> list[str]:
            try:
                dt = datetime.fromisoformat(str(iso)).date()
            except Exception:
                return []
            return [
                f"{dt.day} {_ru_month(dt.month)}",
                f"{dt.day:02d}.{dt.month:02d}",
                f"{dt.day}.{dt.month}",
                f"{dt.day:02d}/{dt.month:02d}",
                f"{dt.day}/{dt.month}",
            ]

        def _find_event_start(ev: dict) -> int | None:
            d = (ev.get('date') or '').strip()
            if not d:
                return None
            for v in _date_variants(d):
                if not v:
                    continue
                p = text_for_links.find(v)
                if p >= 0:
                    return p
            return None

        def _pick_link(cands: list[dict]) -> str | None:
            # Prefer ticketish labels/domains.
            for c in cands:
                if _ticketish(c.get('text'), c.get('url')):
                    return c.get('url')
            if len(cands) == 1:
                return cands[0].get('url')
            return None

        if cleaned_events and link_spans:
            # Only set when ticket_link is missing (monitor extraction is authoritative).
            if len(cleaned_events) == 1:
                ev = cleaned_events[0]
                if not (ev.get('ticket_link') or '').strip():
                    picked = _pick_link([c for c in link_spans if (c.get('url') or '').startswith(('http://', 'https://'))])
                    if picked:
                        ev['ticket_link'] = picked
            else:
                # Multi-event: associate links to event segments using date anchors in text.
                starts = []
                for idx_ev, ev in enumerate(cleaned_events):
                    p = _find_event_start(ev)
                    if p is None:
                        continue
                    starts.append((p, idx_ev))
                starts.sort()
                for j, (p, idx_ev) in enumerate(starts):
                    end = starts[j+1][0] if j+1 < len(starts) else len(text_for_links)
                    ev = cleaned_events[idx_ev]
                    if (ev.get('ticket_link') or '').strip():
                        continue
                    seg_links = [c for c in link_spans if isinstance(c.get('offset'), int) and p <= int(c['offset']) < end]
                    picked = _pick_link(seg_links)
                    if picked:
                        ev['ticket_link'] = picked

        if cleaned_events:
            messages_with_events += 1
            events_total += len(cleaned_events)

        messages_out.append({
            'source_username': username,
            'source_type': s_type,
            'source_chat_id': getattr(entity, 'id', None),
            'source_title': (getattr(entity, 'title', None) or '').strip() or None,
            'message_id': msg.id,
            'message_date': msg_date,
            'grouped_id': grouped_id,
            'has_video': bool(has_video),
            'video_status': video_status,
            'post_author': _post_author_meta(msg),
            'source_link': f'https://t.me/{username}/{msg.id}',
            'text': text,
            'ocr_text': ocr_text,
            'metrics': {
                'views': views,
                'likes': likes,
            },
            'links': links_meta,
            'posters': posters,
            'videos': videos,
            'events': cleaned_events,
        })

        processed += 1
        await human_sleep(HUMAN_DELAY_MIN, HUMAN_DELAY_MAX)
        if HUMAN_LONG_PAUSE_EVERY > 0 and processed % HUMAN_LONG_PAUSE_EVERY == 0:
            await human_sleep(HUMAN_LONG_PAUSE_MIN, HUMAN_LONG_PAUSE_MAX)

        return processed >= MAX_MESSAGES_PER_SOURCE

    if force_ids:
        logger.info('source.force username=%s ids=%s', username, force_ids[:10])
        forced_msgs = []
        try:
            forced = await tg_call(
                f'get_forced:{username}',
                client.get_messages,
                entity,
                ids=force_ids,
            )
            if forced:
                forced_msgs = list(forced) if isinstance(forced, (list, tuple)) else [forced]
        except Exception as exc:
            logger.warning('source.force_fetch_failed %s: %s', username, exc)
            forced_msgs = []

        # If the forced message is a media group (album), pull neighbor messages to capture
        # the whole group (photos often live in adjacent message_ids).
        expanded = {}
        for msg in forced_msgs or []:
            if not msg:
                continue
            mid = getattr(msg, 'id', None)
            if mid:
                expanded[int(mid)] = msg
            gid = getattr(msg, 'grouped_id', None)
            if not (gid and mid):
                continue
            try:
                async for m2 in client.iter_messages(
                    entity,
                    limit=80,
                    min_id=max(0, int(mid) - 30),
                    max_id=int(mid) + 30,
                ):
                    if getattr(m2, 'grouped_id', None) == gid and getattr(m2, 'id', None):
                        expanded[int(m2.id)] = m2
            except Exception as exc:
                logger.warning('source.force_group_fetch_failed %s/%s: %s', username, mid, exc)

        prev_media_cap = media_cap
        try:
            media_cap = max(int(media_cap), sum(1 for m in expanded.values() if getattr(m, 'photo', None)))
        except Exception:
            media_cap = prev_media_cap

        for mid in sorted(expanded.keys()):
            done = await _process_one(expanded[mid])
            if done:
                break
        media_cap = prev_media_cap

    while not done and processed < MAX_MESSAGES_PER_SOURCE:
        remaining = MAX_MESSAGES_PER_SOURCE - processed
        try:
            async for msg in client.iter_messages(
                entity,
                limit=remaining,
                min_id=last_id or 0,
                max_id=resume_max_id,
            ):
                if not last_id and msg.date and msg.date.replace(tzinfo=timezone.utc) < cutoff:
                    cutoff_hit = True
                    done = True
                    break
                done = await _process_one(msg)
                if done:
                    break
            else:
                done = True
        except FloodWaitError as exc:
            flood_attempts += 1
            ok = await _sleep_flood(getattr(exc, 'seconds', 0), f'iter_messages:{username}', flood_attempts)
            if not ok or flood_attempts >= TG_FLOOD_MAX_RETRIES:
                raise
            continue

    median_views = int(statistics.median(views_vals)) if views_vals else None
    median_likes = int(statistics.median(likes_vals)) if likes_vals else None
    for msg in messages_out:
        msg['metrics']['channel_median_views'] = median_views
        msg['metrics']['channel_median_likes'] = median_likes

    if not messages_out:
        logger.info(
            'source.empty username=%s last_id=%s latest_id=%s cutoff_hit=%s',
            username,
            last_id or 0,
            latest_id,
            cutoff_hit,
        )

    logger.info(
        'source.done username=%s messages=%d processed=%d messages_with_events=%d events=%d first_id=%s last_id=%s cutoff_hit=%s',
        username,
        len(messages_out),
        processed,
        messages_with_events,
        events_total,
        first_id,
        last_id_seen,
        cutoff_hit,
    )
    if first_date or last_date:
        logger.info(
            'source.dates username=%s first_date=%s last_date=%s',
            username,
            first_date,
            last_date,
        )
    messages_out = _merge_media_groups(messages_out)
    return {'messages': messages_out, 'source_meta': source_meta}


def _merge_media_groups(messages: list[dict]) -> list[dict]:
    # Merge Telegram media groups (albums) into a single logical post.
    by_gid: dict[int, dict] = {}
    passthrough: list[dict] = []

    def _poster_key(p: dict) -> str:
        return str(p.get('sha256') or p.get('catbox_url') or p.get('supabase_url') or '')

    for msg in messages or []:
        gid = msg.get('grouped_id')
        if not gid:
            passthrough.append(msg)
            continue
        try:
            gid_i = int(gid)
        except Exception:
            passthrough.append(msg)
            continue

        acc = by_gid.get(gid_i)
        if not acc:
            acc = {
                'source_username': msg.get('source_username'),
                'source_type': msg.get('source_type'),
                'source_chat_id': msg.get('source_chat_id'),
                'source_title': msg.get('source_title'),
                'message_id': msg.get('message_id'),
                'message_date': msg.get('message_date'),
                'post_author': msg.get('post_author'),
                'text': msg.get('text') or '',
                'ocr_text': msg.get('ocr_text'),
                'metrics': msg.get('metrics') or {},
                'posters': [],
                'events': [],
                'grouped_id': gid_i,
            }
            by_gid[gid_i] = acc

        # anchor id = smallest id
        try:
            acc_id = int(acc.get('message_id') or 0)
            msg_id = int(msg.get('message_id') or 0)
            if acc_id <= 0 or (msg_id and msg_id < acc_id):
                acc['message_id'] = msg_id
        except Exception:
            pass

        # prefer non-empty text (caption)
        if (msg.get('text') or '').strip() and len((msg.get('text') or '')) > len((acc.get('text') or '')):
            acc['text'] = msg.get('text') or ''

        if msg.get('post_author') and not acc.get('post_author'):
            acc['post_author'] = msg.get('post_author')

        # merge posters (unique)
        seen = {_poster_key(p) for p in (acc.get('posters') or []) if isinstance(p, dict)}
        for p in msg.get('posters') or []:
            if not isinstance(p, dict):
                continue
            k = _poster_key(p)
            if not k or k in seen:
                continue
            seen.add(k)
            acc['posters'].append(p)

        # merge events (keep first non-empty set)
        if msg.get('events') and not acc.get('events'):
            acc['events'] = msg.get('events')

    merged = list(by_gid.values())
    for m in merged:
        username = (m.get('source_username') or '').strip()
        mid = m.get('message_id')
        if username and mid:
            m['source_link'] = f'https://t.me/{username}/{mid}'
        _assign_posters_to_events(m)

    all_msgs = passthrough + merged

    def _sort_key(x: dict):
        try:
            return -int(x.get('message_id') or 0)
        except Exception:
            return 0

    return sorted(all_msgs, key=_sort_key)


def _assign_posters_to_events(message: dict) -> None:
    posters = [p for p in (message.get('posters') or []) if isinstance(p, dict)]
    events = [e for e in (message.get('events') or []) if isinstance(e, dict)]
    if not posters or not events:
        return

    def _norm(s: str) -> str:
        s = (s or '').lower().replace('ё', 'е')
        s = re.sub(r'[^0-9a-zа-я]+', ' ', s)
        return re.sub(r'\s+', ' ', s).strip()

    def _date_tokens(iso: str | None) -> list[str]:
        if not iso:
            return []
        try:
            d = date.fromisoformat(str(iso).split('..', 1)[0].strip())
        except Exception:
            return []
        return [f'{d.day:02d}.{d.month:02d}', f'{d.day}.{d.month}', f'{d.day:02d}/{d.month:02d}', f'{d.day}/{d.month}']

    poster_texts = []
    for idx, p in enumerate(posters):
        txt = ' '.join([str(p.get('ocr_title') or ''), str(p.get('ocr_text') or '')]).strip()
        poster_texts.append((_norm(txt), idx))

    assigned: dict[int, list[dict]] = {i: [] for i in range(len(events))}
    used_posters: set[int] = set()

    # First pass: date token match
    for ei, ev in enumerate(events):
        tokens = _date_tokens(ev.get('date'))
        if not tokens:
            continue
        for ptxt, pi in poster_texts:
            if pi in used_posters:
                continue
            if any(tok in ptxt for tok in tokens):
                assigned[ei].append(posters[pi])
                used_posters.add(pi)

    # Second pass: fuzzy title match for remaining posters
    for ptxt, pi in poster_texts:
        if pi in used_posters:
            continue
        best = None
        best_score = 0
        for ei, ev in enumerate(events):
            title = _norm(str(ev.get('title') or ''))
            if not title:
                continue
            words = [w for w in title.split() if len(w) >= 4]
            overlap = sum(1 for w in set(words) if w in ptxt)
            if overlap > best_score:
                best_score = overlap
                best = ei
        if best is not None and best_score >= 1:
            assigned[best].append(posters[pi])
            used_posters.add(pi)

    # Apply per-event posters + provide per-event source_text (caption + OCR)
    caption = (message.get('text') or '').strip()
    for ei, ev in enumerate(events):
        ev_posters = assigned.get(ei) or []
        if ev_posters:
            ev['posters'] = ev_posters
            ocr_bits = []
            for p in ev_posters:
                if p.get('ocr_text'):
                    ocr_bits.append(str(p.get('ocr_text')).strip())
            ocr_joined = '\n\n'.join(ocr_bits).strip()
            raw = (ev.get('raw_excerpt') or '').strip()
            parts = [p for p in [raw, caption, ocr_joined] if p]
            ev['source_text'] = ('\n\n'.join(parts))[:8000]

    message['events'] = events


async def main():
    sources = config.get('sources') or []
    run_id = config.get('run_id') or f'kaggle_{uuid.uuid4().hex[:8]}'
    all_messages = []
    all_sources_meta = []

    logger.info('tg_monitor.run start run_id=%s sources=%d', run_id, len(sources))
    if not sources:
        logger.warning('tg_monitor.run no sources configured')

    device_config = DEVICE_CONFIG

    async with TelegramClient(StringSession(TG_SESSION), int(TG_API_ID), TG_API_HASH, flood_sleep_threshold=TG_FLOOD_SLEEP_THRESHOLD, **device_config) as client:
        for source in sources:
            try:
                await human_sleep(SOURCE_PAUSE_MIN, SOURCE_PAUSE_MAX)
                scan_result = await scan_source(client, source)
                msgs = scan_result.get('messages') if isinstance(scan_result, dict) else []
                meta = scan_result.get('source_meta') if isinstance(scan_result, dict) else None
                if isinstance(meta, dict) and meta.get('username'):
                    all_sources_meta.append(meta)
                all_messages.extend(msgs)
                logger.info('scanned %s messages for %s', len(msgs), source.get('username'))
            except Exception as exc:
                logger.exception('scan failed for %s: %s', source.get('username'), exc)
            await human_sleep(SOURCE_PAUSE_MIN, SOURCE_PAUSE_MAX)

    # Keep one metadata object per source username.
    sources_meta_by_username = {}
    for item in all_sources_meta:
        uname = str(item.get('username') or '').strip().lower()
        if not uname:
            continue
        sources_meta_by_username[uname] = item
    sources_meta = list(sources_meta_by_username.values())

    messages_with_events = sum(1 for m in all_messages if m.get('events'))
    events_extracted = sum(len(m.get('events') or []) for m in all_messages)

    logger.info(
        'tg_monitor.run summary run_id=%s messages=%d messages_with_events=%d events=%d sources_meta=%d',
        run_id,
        len(all_messages),
        messages_with_events,
        events_extracted,
        len(sources_meta),
    )

    payload = {
        'schema_version': 2,
        'run_id': run_id,
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'sources_meta': sources_meta,
        'messages': all_messages,
        'stats': {
            'sources_total': len(sources),
            'messages_scanned': len(all_messages),
            'messages_with_events': messages_with_events,
            'events_extracted': events_extracted,
        },
    }

    out_path = Path('telegram_results.json')
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    logger.info('Saved telegram_results.json with %s messages and %s sources_meta', len(all_messages), len(sources_meta))

try:
    _loop = asyncio.get_running_loop()
except RuntimeError:
    asyncio.run(main())
else:
    raise RuntimeError('telegram_monitor.py should not be imported while an event loop is already running')
