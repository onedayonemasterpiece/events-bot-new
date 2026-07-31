#!/usr/bin/env python3
from __future__ import annotations

import atexit
import csv
import gc
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

MODEL_SPECS = {
    "0.6b": ("Qwen/Qwen3-Embedding-0.6B", "qwen3_embedding_0_6b", 1024),
    "06b": ("Qwen/Qwen3-Embedding-0.6B", "qwen3_embedding_0_6b", 1024),
    "4b": ("Qwen/Qwen3-Embedding-4B", "qwen3_embedding_4b", 2560),
    "8b": ("Qwen/Qwen3-Embedding-8B", "qwen3_embedding_8b", 4096),
    "embeddinggemma": ("google/embeddinggemma-300m", "embeddinggemma_300m", 768),
    "embeddinggemma300m": ("google/embeddinggemma-300m", "embeddinggemma_300m", 768),
    "gemma": ("google/embeddinggemma-300m", "embeddinggemma_300m", 768),
}
DEFAULT_MODEL_SIZE = "0.6b"
MODEL_ID = "Qwen/Qwen3-Embedding-0.6B"
MODEL_SHORT = "qwen3_embedding_0_6b"
ENCODER_CONTRACT = "qwen3_embedding_0_6b_sentence_transformers_dense_1024_v1"
ENRICHMENT_KIND = "qwen3_embedding_0_6b_enrichment_item"
RESULT_KIND = "qwen3_embedding_0_6b_enrichment_result"
HEARTBEAT_KIND = "business_heartbeat_qwen3_embedding_0_6b_enrichment"
RUN_STARTED_AT = datetime.now(timezone.utc)
RUN_STARTED_MONOTONIC = time.monotonic()


def apply_runtime_env_defaults() -> None:
    os.environ.setdefault("HF_HUB_DOWNLOAD_TIMEOUT", os.getenv("REGION_TALK_HF_HUB_DOWNLOAD_TIMEOUT", "60"))
    os.environ.setdefault("HF_HUB_ETAG_TIMEOUT", os.getenv("REGION_TALK_HF_HUB_ETAG_TIMEOUT", "20"))
    os.environ.setdefault("HF_HUB_DISABLE_XET", os.getenv("REGION_TALK_HF_HUB_DISABLE_XET", "1"))
    os.environ.setdefault("HF_HUB_DISABLE_TELEMETRY", "1")
    os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", os.getenv("REGION_TALK_HF_HUB_DISABLE_PROGRESS_BARS", "1"))
    os.environ.setdefault("TQDM_DISABLE", os.getenv("REGION_TALK_TQDM_DISABLE", "1"))
    os.environ.setdefault("TRANSFORMERS_VERBOSITY", os.getenv("REGION_TALK_TRANSFORMERS_VERBOSITY", "error"))


apply_runtime_env_defaults()


def _normalise_qwen_model_size(value: str) -> str:
    raw = (value or "").strip().lower().replace("_", "").replace("-", "")
    if raw in {"0.6b", "06b", "0.6", "06"}:
        return "0.6b"
    if raw in {"4b", "4"}:
        return "4b"
    if raw in {"8b", "8"}:
        return "8b"
    if raw in {"embeddinggemma", "embeddinggemma300m", "gemma", "gemma300m"}:
        return "embeddinggemma"
    return DEFAULT_MODEL_SIZE


def configure_model_from_env() -> None:
    global MODEL_ID, MODEL_SHORT, ENCODER_CONTRACT, ENRICHMENT_KIND, RESULT_KIND, HEARTBEAT_KIND
    requested_size = _normalise_qwen_model_size(os.getenv("REGION_TALK_QWEN3_MODEL_SIZE") or DEFAULT_MODEL_SIZE)
    default_model_id, default_short, default_dim = MODEL_SPECS.get(requested_size, MODEL_SPECS[DEFAULT_MODEL_SIZE])
    MODEL_ID = (os.getenv("REGION_TALK_QWEN3_MODEL_ID") or default_model_id).strip() or default_model_id
    MODEL_SHORT = (os.getenv("REGION_TALK_QWEN3_MODEL_SHORT") or default_short).strip() or default_short
    ENCODER_CONTRACT = (
        os.getenv("REGION_TALK_QWEN3_ENCODER_CONTRACT")
        or f"{MODEL_SHORT}_sentence_transformers_dense_{default_dim}_v1"
    ).strip()
    ENRICHMENT_KIND = f"{MODEL_SHORT}_enrichment_item"
    RESULT_KIND = f"{MODEL_SHORT}_enrichment_result"
    HEARTBEAT_KIND = f"business_heartbeat_{MODEL_SHORT}_enrichment"


configure_model_from_env()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def stable_hash(*parts: Any, length: int = 16) -> str:
    h = hashlib.sha256()
    for part in parts:
        h.update(str(part or "").strip().lower().encode("utf-8"))
        h.update(b"\0")
    return h.hexdigest()[:length]


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


def getenv_float(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    try:
        return float(raw) if raw else default
    except Exception:
        return default


def runtime_elapsed_seconds() -> float:
    return time.monotonic() - RUN_STARTED_MONOTONIC


def runtime_remaining_seconds() -> float:
    return float(getenv_int("REGION_TALK_QWEN3_MAX_RUNTIME_SECONDS", 25 * 60)) - runtime_elapsed_seconds()


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not raw.strip() or raw.lstrip().startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def load_kaggle_user_secrets() -> dict[str, Any]:
    names = [
        "REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON",
        "REGION_TALK_YDB_IAM_TOKEN",
        "YDB_ACCESS_TOKEN",
        "YC_IAM_TOKEN",
    ]
    extra = (os.getenv("REGION_TALK_KAGGLE_SECRET_NAMES") or "").strip()
    if extra:
        names.extend([x.strip() for x in re.split(r"[,;\s]+", extra) if x.strip()])
    names = list(dict.fromkeys(names))
    try:
        from kaggle_secrets import UserSecretsClient  # type: ignore
    except Exception as exc:
        return {"ok": False, "source": "kaggle_user_secrets", "error": type(exc).__name__, "loaded": []}
    client = UserSecretsClient()
    loaded: list[str] = []
    errors: list[str] = []
    for name in names:
        if os.getenv(name):
            continue
        try:
            value = client.get_secret(name)
            if value is not None and str(value).strip():
                os.environ.setdefault(name, str(value))
                loaded.append(name)
        except Exception as exc:
            errors.append(f"{name}:{type(exc).__name__}")
    return {"ok": bool(loaded), "source": "kaggle_user_secrets", "loaded": loaded, "errors": errors[:5]}


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
            failures: list[str] = []
            loaded = False
            for secret_file in secret_files:
                for key_file in key_files:
                    try:
                        secrets = json.loads(Fernet(key_file.read_bytes().strip()).decrypt(secret_file.read_bytes()).decode("utf-8"))
                        for key, value in secrets.items():
                            if value is not None and str(value).strip():
                                os.environ.setdefault(str(key), str(value))
                        loaded = True
                        break
                    except Exception as exc:
                        failures.append(f"{secret_file.parent.name}/{key_file.parent.name}:{type(exc).__name__}")
                if loaded:
                    break
            config["secret_load_status"] = "ok" if loaded else "failed"
            if failures and not loaded:
                config["secret_load_errors"] = failures[:5]
        except Exception as exc:
            config["secret_load_status"] = "failed"
            config["secret_load_error"] = type(exc).__name__
    kaggle_secrets = load_kaggle_user_secrets()
    if kaggle_secrets.get("loaded"):
        config["kaggle_user_secrets_loaded"] = kaggle_secrets.get("loaded")
    env = config.get("env")
    if isinstance(env, dict):
        for key, value in env.items():
            if value is not None and str(value) != "":
                os.environ[str(key)] = str(value)
    return config


def ydb_config_status() -> dict[str, str]:
    endpoint = (os.getenv("REGION_TALK_YDB_ENDPOINT") or "").strip()
    database = (os.getenv("REGION_TALK_YDB_DATABASE") or "").strip()
    if "?database=" in endpoint:
        endpoint_part, database_part = endpoint.split("?database=", 1)
        endpoint = endpoint_part
        if not database:
            database = database_part
    endpoint = endpoint.rstrip("/")
    namespace = (os.getenv("REGION_TALK_YDB_NAMESPACE") or "region_talk_compact").strip() or "region_talk_compact"
    missing = [k for k, v in {"REGION_TALK_YDB_ENDPOINT": endpoint, "REGION_TALK_YDB_DATABASE": database}.items() if not v]
    return {"endpoint": endpoint, "database": database, "namespace": namespace, "missing": ",".join(missing)}


def ydb_table_name(suffix: str = "state_kv") -> str:
    namespace = re.sub(r"[^A-Za-z0-9_]+", "_", (os.getenv("REGION_TALK_YDB_NAMESPACE") or "region_talk_compact").strip() or "region_talk_compact").strip("_") or "region_talk_compact"
    return f"{namespace}_{suffix}"


def ensure_ydb_module() -> Any:
    try:
        import ydb  # type: ignore
        return ydb
    except Exception:
        if getenv_bool("REGION_TALK_AUTO_INSTALL", True):
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "ydb[yc]"])
            import ydb  # type: ignore
            return ydb
        raise


def ydb_credentials(ydb: Any) -> Any:
    token = (os.getenv("REGION_TALK_YDB_IAM_TOKEN") or os.getenv("YC_IAM_TOKEN") or os.getenv("YDB_ACCESS_TOKEN") or "").strip()
    if token:
        return ydb.AccessTokenCredentials(token)
    key_json = (os.getenv("REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON") or "").strip()
    if key_json:
        import tempfile
        import ydb.iam  # type: ignore
        fd, path = tempfile.mkstemp(prefix="region-talk-qwen3-ydb-sa-", suffix=".json")
        os.close(fd)
        Path(path).write_text(key_json, encoding="utf-8")
        return ydb.iam.ServiceAccountCredentials.from_file(path)
    if os.getenv("YDB_USER"):
        return ydb.StaticCredentials.from_user_password(os.getenv("YDB_USER"), os.getenv("YDB_PASSWORD", ""))
    return ydb.credentials_from_env_variables()


def ydb_connect() -> tuple[Any, Any, dict[str, str]]:
    cfg = ydb_config_status()
    if cfg.get("missing"):
        raise RuntimeError("missing YDB config: " + cfg["missing"])
    ydb = ensure_ydb_module()
    driver = ydb.Driver(endpoint=cfg["endpoint"], database=cfg["database"], credentials=ydb_credentials(ydb))
    driver.wait(timeout=getenv_int("REGION_TALK_YDB_CONNECT_TIMEOUT_SECONDS", 20), fail_fast=True)
    return ydb, driver, cfg


def ydb_request_settings(ydb: Any, *, timeout_seconds: int | None = None) -> Any:
    timeout = max(1, int(timeout_seconds or getenv_int("REGION_TALK_YDB_REQUEST_TIMEOUT_SECONDS", 8)))
    settings = ydb.BaseRequestSettings()
    settings = settings.with_timeout(timeout)
    settings = settings.with_operation_timeout(timeout)
    return settings


def ydb_kv_table_path(cfg: dict[str, str]) -> str:
    return cfg["database"].rstrip("/") + "/" + ydb_table_name("state_kv")


def ensure_ydb_kv_table(ydb: Any, session: Any, table_path: str) -> None:
    try:
        session.describe_table(table_path)
        return
    except Exception:
        pass
    desc = (
        ydb.TableDescription()
        .with_column(ydb.Column("pk", ydb.OptionalType(ydb.PrimitiveType.Utf8)))
        .with_column(ydb.Column("kind", ydb.OptionalType(ydb.PrimitiveType.Utf8)))
        .with_column(ydb.Column("payload_json", ydb.OptionalType(ydb.PrimitiveType.Json)))
        .with_column(ydb.Column("updated_at", ydb.OptionalType(ydb.PrimitiveType.Utf8)))
        .with_primary_key("pk")
    )
    session.create_table(table_path, desc)


def ydb_upsert_json(session: Any, ydb: Any, table_path: str, pk: str, kind: str, payload: dict[str, Any], updated_at: str, *, timeout_seconds: int | None = None) -> None:
    settings = ydb_request_settings(ydb, timeout_seconds=timeout_seconds)
    query = session.prepare(f"""
DECLARE $pk AS Utf8;
DECLARE $kind AS Utf8;
DECLARE $payload_json AS Json;
DECLARE $updated_at AS Utf8;
UPSERT INTO `{table_path}` (pk, kind, payload_json, updated_at)
VALUES ($pk, $kind, $payload_json, $updated_at);
""", settings=settings)
    session.transaction(ydb.SerializableReadWrite()).execute(
        query,
        {"$pk": pk, "$kind": kind, "$payload_json": json.dumps(payload, ensure_ascii=False), "$updated_at": updated_at},
        commit_tx=True,
        settings=settings,
    )


def ydb_upsert_json_many(session: Any, ydb: Any, table_path: str, rows: list[tuple[str, str, dict[str, Any]]], updated_at: str, *, chunk_size: int = 50) -> int:
    if not rows:
        return 0
    settings = ydb_request_settings(ydb)
    query = session.prepare(f"""
DECLARE $pk AS Utf8;
DECLARE $kind AS Utf8;
DECLARE $payload_json AS Json;
DECLARE $updated_at AS Utf8;
UPSERT INTO `{table_path}` (pk, kind, payload_json, updated_at)
VALUES ($pk, $kind, $payload_json, $updated_at);
""", settings=settings)
    written = 0
    chunk_size = max(1, int(chunk_size or 50))
    for start in range(0, len(rows), chunk_size):
        tx = session.transaction(ydb.SerializableReadWrite())
        for pk, kind, payload in rows[start:start + chunk_size]:
            tx.execute(query, {"$pk": pk, "$kind": kind, "$payload_json": json.dumps(payload, ensure_ascii=False), "$updated_at": updated_at}, commit_tx=False, settings=settings)
            written += 1
        tx.commit(settings=settings)
    return written


def ydb_select_kind_items(session: Any, ydb: Any, table_path: str, kind: str, *, limit: int = 10000) -> dict[str, dict[str, Any]]:
    max_items = max(1, int(limit))
    page_size = max(1, min(max_items, getenv_int("REGION_TALK_YDB_SELECT_PAGE_SIZE", 200)))
    out: dict[str, dict[str, Any]] = {}
    prefix = kind + ":"
    prefix_upper = kind + ";"
    after = prefix
    settings = ydb_request_settings(ydb)
    while len(out) < max_items:
        query = session.prepare(f"""
DECLARE $prefix AS Utf8;
DECLARE $prefix_upper AS Utf8;
DECLARE $after AS Utf8;
SELECT pk, payload_json, updated_at FROM `{table_path}`
WHERE pk >= $prefix AND pk < $prefix_upper AND pk > $after
ORDER BY pk
LIMIT {min(page_size, max_items - len(out))};
""", settings=settings)
        result_sets = session.transaction(ydb.StaleReadOnly()).execute(
            query,
            {"$prefix": prefix, "$prefix_upper": prefix_upper, "$after": after},
            commit_tx=True,
            settings=settings,
        )
        rows = result_sets[0].rows if result_sets else []
        if not rows:
            break
        for row in rows:
            pk = str(row.pk)
            payload = row.payload_json
            data = json.loads(payload) if isinstance(payload, str) else dict(payload or {})
            if isinstance(data, dict):
                data.setdefault("_ydb_pk", pk)
                data.setdefault("_ydb_kind", kind)
                data.setdefault("_ydb_updated_at", str(getattr(row, "updated_at", "") or ""))
                out[pk] = data
            after = pk
        if len(rows) < page_size:
            break
    return out


_YDB_EVENT_CACHE: dict[str, Any] = {"driver": None}


def _close_ydb_driver(driver: Any) -> None:
    try:
        driver.stop(timeout=5)
    except Exception:
        pass


def close_cached_ydb() -> None:
    driver = _YDB_EVENT_CACHE.get("driver")
    if driver is not None:
        _close_ydb_driver(driver)
    _YDB_EVENT_CACHE["driver"] = None


atexit.register(close_cached_ydb)


def emit_event(name: str, **payload: Any) -> None:
    row = {"event_name": name, "created_at": utc_now_iso(), **payload}
    try:
        out = Path(os.getenv("REGION_TALK_QWEN3_EVENT_LOG_PATH") or "region_talk_qwen3_events.jsonl")
        with out.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception:
        pass
    if getenv_bool("REGION_TALK_QWEN3_STDOUT_EVENTS", True):
        printable = {k: v for k, v in row.items() if k not in {"embedding_vector"}}
        print("[region-talk-qwen3] " + json.dumps(printable, ensure_ascii=False, sort_keys=True), flush=True)
    if getenv_bool("REGION_TALK_QWEN3_WRITE_HEARTBEATS", True) and (os.getenv("REGION_TALK_STATE_BACKEND") or "").strip().lower() == "ydb":
        try:
            ydb, driver, cfg = ydb_connect()
            table_path = ydb_kv_table_path(cfg)
            pool = ydb.SessionPool(driver)
            run_id = str(payload.get("run_id") or os.getenv("REGION_TALK_RUN_ID") or "")
            compact = {k: v for k, v in row.items() if k in {
                "event_name", "created_at", "phase", "status", "progress_label", "run_id", "texts_loaded",
                "texts_done", "texts_total", "qwen3_batch_size", "qwen3_rows_written", "elapsed_seconds", "error",
            }}
            def op(session: Any) -> None:
                ensure_ydb_kv_table(ydb, session, table_path)
                ydb_upsert_json(session, ydb, table_path, f"latest_business_heartbeat:{MODEL_SHORT}_enrichment", HEARTBEAT_KIND, compact, row["created_at"], timeout_seconds=5)
                if run_id:
                    ydb_upsert_json(session, ydb, table_path, f"business_heartbeat:{MODEL_SHORT}_enrichment:{run_id}", HEARTBEAT_KIND, compact, row["created_at"], timeout_seconds=5)
            pool.retry_operation_sync(op)
            driver.stop(timeout=5)
        except Exception:
            pass


def semantic_bank_v1() -> dict[str, list[str]]:
    return {
        "ko_visit_impression": [
            "Личный рассказ о поездке в Калининградскую область: что увидели, что понравилось, какие места запомнились.",
            "Автор делится впечатлениями от Зеленоградска, Светлогорска, Куршской косы, Балтийска или других городов Калининградской области.",
            "Фотоотчет или заметка путешественника о посещении Калининградской области с эмоциями и наблюдениями.",
        ],
        "ko_route_useful": [
            "Полезный маршрут по Калининградской области: как добраться, что посмотреть, где гулять, советы для поездки.",
            "Содержательная карточка о достопримечательности Калининградской области, истории места, природе, море, дюнах или архитектуре.",
            "Пост о нескольких городах или местах внутри Калининградской области без других регионов России.",
        ],
        "ko_visual_place_card": [
            "Красивое место Калининградской области с описанием вида, атмосферы, моря, пляжа, кирхи, форта, музея или природной локации.",
            "Один конкретный объект или локация в Калининградской области: чем интересен и почему стоит посмотреть.",
        ],
        "other_region_travel": [
            "Пост о Москве, московских парках, пляжах, маршрутах и прогулках, не связанный с Калининградской областью.",
            "Путешествие по Хайнаню, Турции, Беларуси, Европе, Кавказу, Байкалу, Сочи, Петербургу или другому региону, где Калининградская область не является основной темой.",
            "Рассказ о другом городе или стране, случайно содержащий слово, похожее на калининградский топоним.",
        ],
        "multi_region_roundup": [
            "Подборка разных регионов России: Калининград, Байкал, Дагестан, Сочи, Карелия, Алтай и другие направления одним списком.",
            "Дайджест куда поехать летом по России, где Калининградская область только один пункт среди многих регионов.",
            "Сравнение направлений или список городов из разных регионов и стран.",
        ],
        "news_report": [
            "Новость, официальное сообщение, заявление властей, происшествие, политика, суд, полиция, транспортные планы или исследовательская новость.",
            "Информационная заметка СМИ о факте, находке, решении, субсидиях, запуске парома или событии без личного опыта посещения региона.",
        ],
        "event_announcement": [
            "Анонс мероприятия, афиша, выставка, концерт, регистрация, билеты, расписание, программа, приглашаем прийти.",
            "Пост приглашает на событие или публикует календарь мероприятий, а не рассказывает о впечатлениях от региона.",
        ],
        "ad_or_promo": [
            "Реклама, промокод, скидка, конкурс, розыгрыш, тур, экскурсия, бронирование, покупка билетов, коммерческая услуга.",
            "Промо туристического сервиса или платной поездки, где основной смысл — продать или зарегистрировать.",
        ],
        "low_substance": [
            "Короткий пост без содержания: только фото, эмодзи, хэштег, поздравление или слабая подпись без полезной информации.",
            "Служебное объявление, репост, навигация по каналу, техническая новость или пустой визуальный дамп.",
        ],
    }


def _find_input_file(name: str) -> Path | None:
    candidates = [Path(name), Path.cwd() / name]
    if Path("/kaggle/input").exists():
        candidates.extend(Path("/kaggle/input").rglob(name))
    for path in candidates:
        try:
            if path.exists() and path.is_file():
                return path
        except Exception:
            continue
    return None


def ko_geo_bank_v1() -> list[str]:
    out: list[str] = []
    p = _find_input_file(os.getenv("REGION_TALK_PLACE_LEXICON_FILE") or "kaliningrad-place-lexicon-v1.csv")
    if p:
        try:
            with p.open("r", encoding="utf-8-sig", newline="") as fh:
                for row in csv.DictReader(fh):
                    name = str(row.get("canonical_name") or row.get("name") or "").strip()
                    if name:
                        out.append(name)
        except Exception:
            pass
    out.extend([
        "Калининград", "Калининградская область", "Кёнигсберг", "Куршская коса", "Зеленоградск",
        "Светлогорск", "Янтарный", "Балтийск", "Балтийская коса", "Черняховск", "Гусев", "Советск",
        "Неман", "Правдинск", "Гвардейск", "Гурьевск", "Багратионовск", "Мамоново", "Нестеров",
        "Краснознаменск", "Полесск", "Славск", "Озёрск", "Виштынецкое озеро", "Роминтенская пуща",
        "Рыбная деревня", "Остров Канта", "Кафедральный собор", "Форт №5", "Фридландские ворота",
        "Танцующий лес", "Высота Эфа", "Балтийское море", "Куршский залив", "Вислинский залив",
    ])
    seen: set[str] = set()
    unique: list[str] = []
    for item in out:
        key = item.strip().lower()
        if key and key not in seen:
            seen.add(key)
            unique.append(item.strip())
    return unique


def external_ru_geo_bank_v1() -> list[str]:
    # Discriminative Russia/nearby travel geography bank. It is not a complete gazetteer yet;
    # it intentionally covers common travel/news false positives and Russian regions/cities.
    return [
        "Москва", "Московская область", "Санкт-Петербург", "Ленинградская область", "Сочи", "Краснодарский край",
        "Крым", "Севастополь", "Казань", "Татарстан", "Нижний Новгород", "Ярославль", "Владимир", "Суздаль",
        "Тверь", "Псков", "Великий Новгород", "Карелия", "Мурманск", "Кольский полуостров", "Архангельск",
        "Вологда", "Кострома", "Рязань", "Тула", "Калуга", "Смоленск", "Брянск", "Орёл", "Курск",
        "Воронеж", "Ростов-на-Дону", "Дон", "Волгоград", "Астрахань", "Кавказ", "Дагестан", "Чечня",
        "Ингушетия", "Кабардино-Балкария", "Карачаево-Черкесия", "Северная Осетия", "Ставропольский край",
        "Пятигорск", "Кисловодск", "Домбай", "Архыз", "Эльбрус", "Урал", "Екатеринбург", "Пермь",
        "Башкирия", "Уфа", "Челябинск", "Тюмень", "Сибирь", "Новосибирск", "Томск", "Омск", "Красноярск",
        "Алтай", "Республика Алтай", "Алтайский край", "Байкал", "Иркутск", "Бурятия", "Улан-Удэ", "Якутия",
        "Дальний Восток", "Владивосток", "Приморский край", "Хабаровск", "Камчатка", "Сахалин", "Курилы",
        "Белгород", "Липецк", "Тамбов", "Саратов", "Самара", "Пенза", "Ульяновск", "Мордовия", "Марий Эл",
        "Чувашия", "Удмуртия", "Киров", "Коми", "Ненецкий автономный округ", "Ямал", "Ханты-Мансийск",
    ]


def external_country_bank_v1() -> list[str]:
    return [
        "Беларусь", "Минск", "Литва", "Вильнюс", "Польша", "Гданьск", "Варшава", "Германия", "Берлин",
        "Финляндия", "Эстония", "Латвия", "Грузия", "Армения", "Азербайджан", "Казахстан", "Узбекистан",
        "Турция", "Стамбул", "Анталья", "Египет", "Таиланд", "Бали", "Индонезия", "Китай", "Хайнань",
        "Япония", "Корея", "Италия", "Испания", "Франция", "Португалия", "Греция", "ОАЭ", "Дубай",
    ]


def bank_version_and_hash(bank: Any, *, version: str) -> tuple[str, str]:
    payload = json.dumps(bank, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return version, hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _normalize_matrix(vectors: Any) -> Any:
    import numpy as np  # type: ignore
    arr = np.asarray(vectors, dtype="float32")
    if arr.ndim == 1:
        arr = arr.reshape(1, -1)
    denom = np.linalg.norm(arr, axis=1, keepdims=True)
    denom[denom == 0] = 1.0
    return arr / denom


class Qwen3EmbeddingEncoder:
    def __init__(self) -> None:
        self.backend = "sentence_transformers"
        self.model: Any = None
        self.device = "unknown"
        self.model_load_ref = MODEL_ID

    def _find_kaggle_model_path(self) -> str:
        explicit = (os.getenv("REGION_TALK_QWEN3_MODEL_LOCAL_PATH") or "").strip()
        if explicit and (Path(explicit) / "config.json").exists():
            return explicit
        source = (os.getenv("REGION_TALK_QWEN3_KAGGLE_MODEL_SOURCE") or "").strip()
        variation = (os.getenv("REGION_TALK_QWEN3_KAGGLE_MODEL_VARIATION") or "").strip().lower()
        source_parts: list[str] = []
        if source:
            source_parts = [p for p in source.strip("/").split("/") if p]
            if len(source_parts) >= 4:
                variation = variation or source_parts[-2].lower()
        roots = [Path("/kaggle/input"), Path.cwd()]
        direct_suffixes: list[Path] = []
        if variation:
            if len(source_parts) >= 4:
                model_slug = source_parts[1]
                framework = source_parts[2]
                version = source_parts[4] if len(source_parts) >= 5 else "1"
                direct_suffixes.extend([
                    Path(model_slug) / framework / variation / version,
                    Path(model_slug) / framework.lower() / variation / version,
                    Path(model_slug) / framework.capitalize() / variation / version,
                    Path(model_slug) / variation / version,
                ])
            direct_suffixes.extend([
                Path("qwen-3-embedding") / "transformers" / variation / "1",
                Path("qwen-3-embedding") / "Transformers" / variation / "1",
                Path("qwen3-embedding") / "transformers" / variation / "1",
                Path("qwen3-embedding") / "Transformers" / variation / "1",
                Path("qwen-3-embedding") / variation / "1",
                Path("embeddinggemma") / "transformers" / variation / "1",
                Path("embeddinggemma") / "Transformers" / variation / "1",
            ])
        for root in roots:
            if not root.exists():
                continue
            for suffix in direct_suffixes:
                candidate = root / suffix
                if (candidate / "config.json").exists():
                    return str(candidate)
            if variation:
                try:
                    for config in root.rglob("config.json"):
                        path_text = config.parent.as_posix().lower()
                        model_slug = source_parts[1].lower() if len(source_parts) >= 2 else ""
                        if variation in path_text and (not model_slug or model_slug in path_text or "qwen" in path_text or "gemma" in path_text):
                            return str(config.parent)
                except Exception:
                    pass
        if source and getenv_bool("REGION_TALK_QWEN3_USE_KAGGLEHUB_FALLBACK", True):
            try:
                import kagglehub  # type: ignore
            except Exception:
                if getenv_bool("REGION_TALK_AUTO_INSTALL", True):
                    try:
                        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "--upgrade", "kagglehub"])
                        import kagglehub  # type: ignore
                    except Exception:
                        return MODEL_ID
                else:
                    return MODEL_ID
            try:
                source_without_version = "/".join(source.strip("/").split("/")[:4])
                model_dir = kagglehub.model_download(source_without_version)
                if model_dir and (Path(model_dir) / "config.json").exists():
                    return str(model_dir)
            except Exception:
                pass
        return MODEL_ID

    def load(self) -> None:
        started = time.monotonic()
        emit_event("qwen3_model_load_started", phase="model_load", status="running", run_id=os.getenv("REGION_TALK_RUN_ID") or "", model_id=MODEL_ID, backend=self.backend)
        def _versions_need_upgrade() -> bool:
            try:
                from importlib.metadata import version  # type: ignore
            except Exception:
                try:
                    from importlib_metadata import version  # type: ignore
                except Exception:
                    return False

            def parse(raw: str) -> tuple[int, int, int]:
                nums = [int(x) for x in re.findall(r"\d+", raw)[:3]]
                return tuple((nums + [0, 0, 0])[:3])  # type: ignore[return-value]

            try:
                st_version = parse(version("sentence-transformers"))
                transformers_version = parse(version("transformers"))
            except Exception:
                return True
            return st_version < (2, 7, 0) or transformers_version < (4, 51, 0)

        st_package = os.getenv("REGION_TALK_QWEN3_SENTENCE_TRANSFORMERS_PACKAGE", "sentence-transformers>=2.7.0")
        transformers_package = os.getenv("REGION_TALK_QWEN3_TRANSFORMERS_PACKAGE", "transformers>=4.51.0")
        accel_package = os.getenv("REGION_TALK_QWEN3_ACCELERATE_PACKAGE", "accelerate")
        force_install = getenv_bool("REGION_TALK_QWEN3_FORCE_MODEL_PACKAGE_INSTALL", False)
        if getenv_bool("REGION_TALK_AUTO_INSTALL", True) and (force_install or _versions_need_upgrade()):
            subprocess.check_call([
                sys.executable, "-m", "pip", "install", "-q", "--upgrade",
                st_package, transformers_package, accel_package
            ])
        try:
            from sentence_transformers import SentenceTransformer  # type: ignore
        except Exception:
            if getenv_bool("REGION_TALK_AUTO_INSTALL", True):
                subprocess.check_call([
                    sys.executable, "-m", "pip", "install", "-q", "--upgrade",
                    st_package, transformers_package, accel_package
                ])
                from sentence_transformers import SentenceTransformer  # type: ignore
            else:
                raise
        try:
            import torch  # type: ignore
            self.device = "cuda" if getattr(torch, "cuda", None) and torch.cuda.is_available() else "cpu"
        except Exception:
            self.device = "cpu"
        load_ref = self._find_kaggle_model_path()
        self.model_load_ref = load_ref
        constructor_kwargs: dict[str, Any] = {"trust_remote_code": True}
        if self.device == "cuda" and getenv_bool("REGION_TALK_QWEN3_USE_GPU_MODEL_KWARGS", True):
            try:
                import torch  # type: ignore
                model_kwargs: dict[str, Any] = {"torch_dtype": torch.float16}
                device_map = (os.getenv("REGION_TALK_QWEN3_DEVICE_MAP") or "").strip()
                if device_map:
                    model_kwargs["device_map"] = device_map
                constructor_kwargs["model_kwargs"] = model_kwargs
                constructor_kwargs["tokenizer_kwargs"] = {"padding_side": "left"}
            except Exception:
                pass
        try:
            self.model = SentenceTransformer(load_ref, **constructor_kwargs)
        except TypeError:
            try:
                self.model = SentenceTransformer(load_ref, trust_remote_code=True)
            except TypeError:
                self.model = SentenceTransformer(load_ref)
        except Exception:
            if constructor_kwargs.get("model_kwargs"):
                try:
                    self.model = SentenceTransformer(load_ref, trust_remote_code=True)
                except TypeError:
                    self.model = SentenceTransformer(load_ref)
            else:
                raise
        try:
            self.model.max_seq_length = max(64, getenv_int("REGION_TALK_QWEN3_MAX_LENGTH", 2048))
        except Exception:
            pass
        emit_event("qwen3_model_load_done", phase="model_load", status="running", run_id=os.getenv("REGION_TALK_RUN_ID") or "", model_id=MODEL_ID, model_load_ref=load_ref if load_ref == MODEL_ID else "kaggle_input", backend=self.backend, device=self.device, elapsed_seconds=round(time.monotonic() - started, 3))

    def encode(self, texts: list[str], *, batch_size: int, max_length: int, query: bool = False) -> Any:
        if self.model is None:
            self.load()
        try:
            self.model.max_seq_length = max(64, max_length)
        except Exception:
            pass
        kwargs = {"batch_size": max(1, batch_size), "normalize_embeddings": True, "show_progress_bar": False}
        if query:
            try:
                return _normalize_matrix(self.model.encode(texts, prompt_name="query", **kwargs))
            except TypeError:
                return _normalize_matrix(self.model.encode(texts, **kwargs))
            except ValueError:
                return _normalize_matrix(self.model.encode(texts, **kwargs))
        return _normalize_matrix(self.model.encode(texts, **kwargs))

    def release(self) -> None:
        self.model = None
        gc.collect()
        try:
            import torch  # type: ignore
            if getattr(torch, "cuda", None) and torch.cuda.is_available():
                torch.cuda.empty_cache()
        except Exception:
            pass

def _score_against_bank(query_vecs: Any, prototype_vecs: Any, labels: list[str]) -> list[dict[str, float]]:
    sims_rows = (query_vecs @ prototype_vecs.T).tolist()
    out: list[dict[str, float]] = []
    for sims in sims_rows:
        scores: dict[str, float] = {}
        for label, sim in zip(labels, sims):
            scores[label] = max(scores.get(label, -1.0), float(sim))
        out.append(scores)
    return out


def _top_from_scores(scores: dict[str, float], labels: Iterable[str] | None = None) -> tuple[str, float]:
    subset = set(labels or scores.keys())
    items = [(k, v) for k, v in scores.items() if k in subset]
    if not items:
        return "", 0.0
    return max(items, key=lambda item: item[1])


def _round_scores(scores: dict[str, float]) -> dict[str, float]:
    return {k: round(float(v), 4) for k, v in sorted(scores.items())}


def text_hash(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()


def compact_text(value: Any, *, max_len: int = 4000) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()[:max_len]


TEXT_FIELDS = [
    "text", "full_text", "text_excerpt", "short_summary", "why_keep_in_memory", "why_this_is_about_kaliningrad",
    "what_positive", "what_neutral_or_useful", "llm_reason", "publication_story_reason", "model_short_explanation",
]


def text_from_row(row: dict[str, Any]) -> tuple[str, list[str]]:
    parts: list[str] = []
    used: list[str] = []
    for field in TEXT_FIELDS:
        raw = compact_text(row.get(field), max_len=1200)
        if raw and raw not in parts:
            parts.append(raw)
            used.append(field)
    return compact_text(". ".join(parts), max_len=getenv_int("REGION_TALK_QWEN3_TEXT_MAX_CHARS", 3000)), used


def enrichment_pk(post_id: str, post_url: str, text_sha: str, model_id: str = MODEL_ID) -> str:
    base = post_id or stable_hash(post_url, text_sha, length=16)
    model_key = MODEL_SHORT if model_id == MODEL_ID else stable_hash(model_id, length=8)
    return f"{ENRICHMENT_KIND}:{base}:{model_key}:{text_sha[:12]}"


def collect_text_rows(items_by_kind: dict[str, dict[str, dict[str, Any]]], *, existing_pks: set[str], limit: int, include_existing: bool = False) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen_text_or_url: set[str] = set()
    priority = {
        "publication_candidate_item": 0,
        "candidate_memory_item": 1,
        "image_queue_item": 2,
        "processed_post_item": 3,
        "post_live_item": 4,
    }
    candidates: list[tuple[int, str, dict[str, Any]]] = []
    for kind, items in items_by_kind.items():
        for _pk, row in items.items():
            if not isinstance(row, dict):
                continue
            text, used = text_from_row(row)
            if len(text) < getenv_int("REGION_TALK_QWEN3_MIN_TEXT_CHARS", 24):
                continue
            post_url = str(row.get("post_url") or "").strip()
            post_id = str(row.get("post_id") or row.get("candidate_memory_id") or row.get("publication_candidate_id") or "").strip()
            sha = text_hash(text)
            pk = enrichment_pk(post_id, post_url, sha)
            if not include_existing and pk in existing_pks:
                continue
            dedupe_key = post_url or sha
            if dedupe_key in seen_text_or_url:
                continue
            seen_text_or_url.add(dedupe_key)
            rr = dict(row)
            rr["_source_kind"] = kind
            rr["_embedding_text"] = text
            rr["_embedding_text_fields"] = used
            rr["_embedding_text_hash"] = sha
            rr["_enrichment_pk"] = pk
            candidates.append((priority.get(kind, 9), str(row.get("post_date") or row.get("published_at") or ""), rr))
    candidates.sort(key=lambda item: (item[0], item[1]), reverse=False)
    for _priority, _date, row in candidates[:max(1, limit)]:
        rows.append(row)
    return rows


def load_ydb_rows(limit: int, *, include_existing: bool = False) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    ydb, driver, cfg = ydb_connect()
    table_path = ydb_kv_table_path(cfg)
    pool = ydb.SessionPool(driver)
    max_scan = max(limit * 5, getenv_int("REGION_TALK_QWEN3_YDB_SCAN_LIMIT", 1000))
    kinds = [k.strip() for k in re.split(r"[,;+\s]+", os.getenv("REGION_TALK_QWEN3_INPUT_KINDS") or "publication_candidate_item,candidate_memory_item,image_queue_item,processed_post_item,post_live_item") if k.strip()]

    def op(session: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        ensure_ydb_kv_table(ydb, session, table_path)
        items_by_kind: dict[str, dict[str, dict[str, Any]]] = {}
        for kind in kinds:
            items_by_kind[kind] = ydb_select_kind_items(session, ydb, table_path, kind, limit=max_scan)
        existing = ydb_select_kind_items(session, ydb, table_path, ENRICHMENT_KIND, limit=max_scan)
        existing_pks = set(existing.keys())
        rows = collect_text_rows(items_by_kind, existing_pks=existing_pks, limit=limit, include_existing=include_existing)
        meta = {
            "table_path": table_path,
            "input_kinds": kinds,
            "loaded_by_kind": {kind: len(items_by_kind.get(kind) or {}) for kind in kinds},
            f"existing_{ENRICHMENT_KIND}s": len(existing_pks),
        }
        return rows, meta

    try:
        rows, meta = pool.retry_operation_sync(op)
    finally:
        driver.stop(timeout=5)
    return rows, meta


def make_fallback_rows(limit: int) -> list[dict[str, Any]]:
    texts = [
        "Личный отзыв о поездке в Калининградскую область: Куршская коса, Зеленоградск, море, дюны и впечатления от прогулки.",
        "Подборка разных регионов России: Байкал, Сочи, Алтай и Калининград одним списком направлений.",
        "Анонс мероприятия, регистрация, билеты и программа события в Калининграде.",
    ]
    out = []
    for idx, text in enumerate(texts[:max(1, limit)], start=1):
        sha = text_hash(text)
        out.append({
            "post_id": f"fallback_{idx}",
            "post_url": "",
            "source_title": "fallback",
            "_source_kind": "fallback",
            "_embedding_text": text,
            "_embedding_text_fields": ["fallback"],
            "_embedding_text_hash": sha,
            "_enrichment_pk": enrichment_pk(f"fallback_{idx}", "", sha),
        })
    return out


def build_enrichment_payload(
    row: dict[str, Any],
    semantic_scores: dict[str, float],
    geo_scores: dict[str, float],
    dense_vector: list[float] | None,
    *,
    run_id: str,
    semantic_bank_version: str,
    semantic_bank_hash: str,
    geo_bank_version: str,
    geo_bank_hash: str,
    embedding_dim: int,
    row_index: int,
) -> dict[str, Any]:
    positive_labels = {"ko_visit_impression", "ko_route_useful", "ko_visual_place_card"}
    negative_labels = set(semantic_scores) - positive_labels
    pos_class, pos_score = _top_from_scores(semantic_scores, positive_labels)
    neg_class, neg_score = _top_from_scores(semantic_scores, negative_labels)
    top_class, top_score = _top_from_scores(semantic_scores)
    ko_geo_class, ko_geo_score = _top_from_scores(geo_scores, [k for k in geo_scores if k.startswith("ko_geo:")])
    external_geo_class, external_geo_score = _top_from_scores(geo_scores, [k for k in geo_scores if k.startswith("external_ru_geo:") or k.startswith("external_country_geo:")])
    text = str(row.get("_embedding_text") or "")
    text_sha = str(row.get("_embedding_text_hash") or text_hash(text))
    post_id = str(row.get("post_id") or row.get("candidate_memory_id") or row.get("publication_candidate_id") or stable_hash(row.get("post_url"), text_sha, length=16))
    post_url = str(row.get("post_url") or "")
    payload: dict[str, Any] = {
        f"{MODEL_SHORT}_enrichment_id": str(row.get("_enrichment_pk") or enrichment_pk(post_id, post_url, text_sha)).replace(f"{ENRICHMENT_KIND}:", ""),
        "run_id": run_id,
        "created_at": utc_now_iso(),
        "source_kind": row.get("_source_kind") or "",
        "row_index": row_index,
        "post_id": post_id,
        "post_url": post_url,
        "source_id": row.get("source_id") or "",
        "source_title": row.get("source_title") or "",
        "source_url": row.get("source_url") or row.get("canonical_url") or "",
        "post_date": row.get("post_date") or row.get("published_at") or "",
        "text_hash": text_sha,
        "text_excerpt": text[:500],
        "text_source_fields": row.get("_embedding_text_fields") or [],
        "model_id": MODEL_ID,
        "model_short": MODEL_SHORT,
        "encoder_contract": ENCODER_CONTRACT,
        "embedding_kind": "dense",
        "embedding_dim": embedding_dim,
        "semantic_bank_version": semantic_bank_version,
        "semantic_bank_hash": semantic_bank_hash[:16],
        "geo_bank_version": geo_bank_version,
        "geo_bank_hash": geo_bank_hash[:16],
        "semantic_scores_by_class": _round_scores(semantic_scores),
        f"{MODEL_SHORT}_top_class": top_class,
        f"{MODEL_SHORT}_top_score": round(float(top_score), 4),
        f"{MODEL_SHORT}_positive_class": pos_class,
        f"{MODEL_SHORT}_positive_score": round(float(pos_score), 4),
        f"{MODEL_SHORT}_negative_class": neg_class,
        f"{MODEL_SHORT}_negative_score": round(float(neg_score), 4),
        f"{MODEL_SHORT}_margin_positive_vs_negative": round(float(pos_score - neg_score), 4),
        f"{MODEL_SHORT}_ko_geo_top": ko_geo_class.replace("ko_geo:", ""),
        f"{MODEL_SHORT}_ko_geo_score": round(float(ko_geo_score), 4),
        f"{MODEL_SHORT}_external_geo_top": re.sub(r"^external_(?:ru_|country_)?geo:", "", external_geo_class),
        f"{MODEL_SHORT}_external_geo_score": round(float(external_geo_score), 4),
        f"{MODEL_SHORT}_ko_vs_external_geo_margin": round(float(ko_geo_score - external_geo_score), 4),
        f"vector_gate_status_{MODEL_SHORT}": f"{MODEL_SHORT}_accept_candidate" if pos_score >= neg_score and (pos_score - neg_score) >= getenv_float("REGION_TALK_QWEN3_ACCEPT_MARGIN", 0.02) else f"{MODEL_SHORT}_review_or_reject",
        "dense_vector_stored": bool(dense_vector),
    }
    if dense_vector is not None:
        payload["embedding_vector"] = [round(float(x), 6) for x in dense_vector]
    return payload


def write_result_rows(rows: list[dict[str, Any]], summary: dict[str, Any]) -> int:
    ydb, driver, cfg = ydb_connect()
    table_path = ydb_kv_table_path(cfg)
    pool = ydb.SessionPool(driver)
    now = utc_now_iso()
    run_id = str(summary.get("run_id") or "")
    ydb_rows = [(str(row.get("_pk") or f"{ENRICHMENT_KIND}:" + row[f"{MODEL_SHORT}_enrichment_id"]), ENRICHMENT_KIND, {k: v for k, v in row.items() if k != "_pk"}) for row in rows]

    def op(session: Any) -> int:
        ensure_ydb_kv_table(ydb, session, table_path)
        written = ydb_upsert_json_many(session, ydb, table_path, ydb_rows, now, chunk_size=getenv_int("REGION_TALK_QWEN3_YDB_UPSERT_CHUNK_SIZE", 25))
        final_summary = {**summary, "rows_written": written, "ydb_write_status": "ok"}
        result_payload = {
            "summary": final_summary,
            "row_count": len(rows),
            "rows_without_vectors": [{k: v for k, v in row.items() if k != "embedding_vector"} for row in rows[:50]],
        }
        ydb_upsert_json(session, ydb, table_path, f"{RESULT_KIND}:{run_id}", RESULT_KIND, result_payload, now)
        ydb_upsert_json(session, ydb, table_path, f"{RESULT_KIND}:latest", RESULT_KIND, result_payload, now)
        return written

    try:
        return pool.retry_operation_sync(op)
    finally:
        driver.stop(timeout=5)


def run_qwen3_enrichment(run_id: str, output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    emit_event("qwen3_enrichment_started", phase="load_ydb", status="running", run_id=run_id, model_id=MODEL_ID)
    limit = max(1, getenv_int("REGION_TALK_QWEN3_BATCH_LIMIT", 12))
    include_existing = getenv_bool("REGION_TALK_QWEN3_REPROCESS_EXISTING", False)
    rows, ydb_meta = load_ydb_rows(limit, include_existing=include_existing)
    if not rows and getenv_bool("REGION_TALK_QWEN3_ALLOW_FALLBACK_TEXTS", False):
        rows = make_fallback_rows(limit)
        ydb_meta["fallback_texts_used"] = True
    emit_event("qwen3_text_rows_loaded", phase="load_ydb", status="running", run_id=run_id, texts_loaded=len(rows), texts_total=len(rows), progress_label=f"Qwen3 rows loaded {len(rows)}")
    if not rows:
        summary = {
            "ok": True,
            "status": "no_rows",
            "run_id": run_id,
            "model_id": MODEL_ID,
            "rows_loaded": 0,
            "rows_scored": 0,
            "rows_written": 0,
            "ydb_meta": ydb_meta,
            "elapsed_seconds": round(runtime_elapsed_seconds(), 3),
        }
        (output_dir / f"{MODEL_SHORT}_enrichment_result.json").write_text(json.dumps({"summary": summary, "rows": []}, ensure_ascii=False, indent=2), encoding="utf-8")
        (Path.cwd() / "output.json").write_text(json.dumps({"ok": True, "status": "no_rows", "run_id": run_id, "summary": summary}, ensure_ascii=False, indent=2), encoding="utf-8")
        return {"ok": True, "status": "no_rows", "summary": summary, "rows": []}

    encoder = Qwen3EmbeddingEncoder()
    batch_size = max(1, getenv_int("REGION_TALK_QWEN3_BATCH_SIZE", 4))
    max_length = max(64, getenv_int("REGION_TALK_QWEN3_MAX_LENGTH", 2048))
    semantic_bank = semantic_bank_v1()
    semantic_version, semantic_hash = bank_version_and_hash(semantic_bank, version="semantic_bank_v1")
    semantic_labels: list[str] = []
    semantic_texts: list[str] = []
    for label, examples in semantic_bank.items():
        for example in examples:
            semantic_labels.append(label)
            semantic_texts.append(example)
    geo_bank: dict[str, list[str]] = {
        "ko_geo": ko_geo_bank_v1(),
        "external_ru_geo": external_ru_geo_bank_v1(),
        "external_country_geo": external_country_bank_v1(),
    }
    geo_version, geo_hash = bank_version_and_hash(geo_bank, version="geo_discriminator_bank_v1")
    geo_labels: list[str] = []
    geo_texts: list[str] = []
    for group, names in geo_bank.items():
        for name in names:
            geo_labels.append(f"{group}:{name}")
            geo_texts.append(f"Пост о направлении: {name}")

    started = time.monotonic()
    semantic_proto = encoder.encode(semantic_texts, batch_size=batch_size, max_length=max_length)
    geo_proto = encoder.encode(geo_texts, batch_size=batch_size, max_length=max_length)
    emit_event("qwen3_prototype_vectors_ready", phase="vectorize", status="running", run_id=run_id, semantic_prototypes=len(semantic_texts), geo_prototypes=len(geo_texts), elapsed_seconds=round(time.monotonic() - started, 3))

    texts = [str(row.get("_embedding_text") or "") for row in rows]
    store_vectors = getenv_bool("REGION_TALK_QWEN3_STORE_DENSE_VECTORS", True)
    store_vector_max_rows = max(0, getenv_int("REGION_TALK_QWEN3_STORE_VECTOR_MAX_ROWS", 100))
    result_rows: list[dict[str, Any]] = []
    vectors_done = 0
    for start in range(0, len(texts), batch_size):
        if runtime_remaining_seconds() < getenv_int("REGION_TALK_QWEN3_RUNTIME_RESERVE_SECONDS", 90):
            emit_event("qwen3_runtime_reserve_reached", phase="vectorize", status="partial", run_id=run_id, texts_done=vectors_done, texts_total=len(texts), runtime_remaining_seconds=round(runtime_remaining_seconds(), 1))
            break
        batch_texts = texts[start:start + batch_size]
        query_vecs = encoder.encode(batch_texts, batch_size=batch_size, max_length=max_length, query=True)
        semantic_batch_scores = _score_against_bank(query_vecs, semantic_proto, semantic_labels)
        geo_batch_scores = _score_against_bank(query_vecs, geo_proto, geo_labels)
        for offset, row in enumerate(rows[start:start + batch_size]):
            idx = start + offset
            dense_vector = query_vecs[offset].tolist() if store_vectors and idx < store_vector_max_rows else None
            payload = build_enrichment_payload(
                row,
                semantic_batch_scores[offset],
                geo_batch_scores[offset],
                dense_vector,
                run_id=run_id,
                semantic_bank_version=semantic_version,
                semantic_bank_hash=semantic_hash,
                geo_bank_version=geo_version,
                geo_bank_hash=geo_hash,
                embedding_dim=int(query_vecs.shape[1]) if hasattr(query_vecs, "shape") and len(query_vecs.shape) > 1 else 0,
                row_index=idx + 1,
            )
            payload["_pk"] = str(row.get("_enrichment_pk") or f"{ENRICHMENT_KIND}:" + payload[f"{MODEL_SHORT}_enrichment_id"])
            result_rows.append(payload)
        vectors_done = len(result_rows)
        emit_event("qwen3_batch_done", phase="vectorize", status="running", run_id=run_id, texts_done=vectors_done, texts_total=len(texts), qwen3_batch_size=len(batch_texts), progress_label=f"Qwen3 {vectors_done}/{len(texts)}")
    encoder.release()

    summary = {
        "ok": True,
        "status": "done" if len(result_rows) == len(rows) else "partial",
        "run_id": run_id,
        "model_id": MODEL_ID,
        "model_short": MODEL_SHORT,
        "encoder_contract": ENCODER_CONTRACT,
        "backend": encoder.backend,
        "device": encoder.device,
        "rows_loaded": len(rows),
        "rows_scored": len(result_rows),
        "rows_written": 0,
        "batch_size": batch_size,
        "max_length": max_length,
        "dense_vectors_stored": sum(1 for row in result_rows if row.get("dense_vector_stored")),
        "semantic_bank_version": semantic_version,
        "semantic_bank_hash": semantic_hash[:16],
        "geo_bank_version": geo_version,
        "geo_bank_hash": geo_hash[:16],
        "ydb_meta": ydb_meta,
        "elapsed_seconds": round(runtime_elapsed_seconds(), 3),
    }
    try:
        written = write_result_rows(result_rows, summary)
        summary["rows_written"] = written
        summary["ydb_write_status"] = "ok"
    except Exception as exc:
        summary["ok"] = False
        summary["status"] = "error_write_ydb"
        summary["ydb_write_status"] = "error"
        summary["error"] = f"{type(exc).__name__}: {str(exc)[:500]}"
        emit_event("qwen3_ydb_write_failed", phase="write_ydb", status="error", run_id=run_id, error=summary["error"])

    public_rows = [{k: v for k, v in row.items() if k != "embedding_vector" and k != "_pk"} for row in result_rows]
    (output_dir / f"{MODEL_SHORT}_enrichment_result.json").write_text(json.dumps({"summary": summary, "rows": public_rows}, ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / f"{MODEL_SHORT}_enrichment_rows.jsonl").write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in public_rows) + ("\n" if public_rows else ""), encoding="utf-8")
    (output_dir / "stage_status.json").write_text(json.dumps({"run_id": run_id, "generated_at": utc_now_iso(), "status": summary["status"], "summary": summary}, ensure_ascii=False, indent=2), encoding="utf-8")
    for p in [output_dir / f"{MODEL_SHORT}_enrichment_result.json", output_dir / f"{MODEL_SHORT}_enrichment_rows.jsonl", output_dir / "stage_status.json"]:
        target = Path.cwd() / p.name
        if target.resolve() != p.resolve():
            target.write_bytes(p.read_bytes())
    (Path.cwd() / "output.json").write_text(json.dumps({"ok": summary.get("ok"), "status": summary["status"], "run_id": run_id, "summary": summary}, ensure_ascii=False, indent=2), encoding="utf-8")
    emit_event("qwen3_enrichment_done", phase="write_ydb", status=summary["status"], run_id=run_id, texts_done=summary["rows_scored"], texts_total=summary["rows_loaded"], qwen3_rows_written=summary["rows_written"], elapsed_seconds=summary["elapsed_seconds"])
    return {"ok": bool(summary.get("ok")), "status": summary["status"], "summary": summary, "rows": public_rows}


def main() -> int:
    load_dotenv(Path(".env"))
    config = load_split_runtime_from_kaggle_input()
    configure_model_from_env()
    run_id = os.getenv("REGION_TALK_RUN_ID") or f"region-talk-{MODEL_SHORT.replace('_', '-')}-{RUN_STARTED_AT.strftime('%Y%m%dT%H%M%SZ')}"
    os.environ.setdefault("REGION_TALK_RUN_ID", run_id)
    os.environ.setdefault("REGION_TALK_STATE_BACKEND", "ydb")
    os.environ.setdefault("REGION_TALK_REQUIRE_YDB_STATE", "1")
    os.environ.setdefault("REGION_TALK_DISABLE_PUBLISH", "1")
    os.environ.setdefault("REGION_TALK_DRY_RUN", "1")
    os.environ.setdefault("REGION_TALK_AUTH_BUNDLE_ENV", "REGION_TALK_NO_TELEGRAM_BUNDLE")
    if (os.getenv("REGION_TALK_STATE_BACKEND") or "").strip().lower() != "ydb" and getenv_bool("REGION_TALK_REQUIRE_YDB_STATE", True):
        raise RuntimeError("RegionTalkQwen3Embedding06BEnrichment requires REGION_TALK_STATE_BACKEND=ydb for live runs")
    out_dir = Path(os.getenv("REGION_TALK_OUTPUT_DIR") or f"artifacts/region-talk/runs/{run_id}")
    payload = run_qwen3_enrichment(run_id, out_dir)
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
