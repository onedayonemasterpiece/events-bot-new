#!/usr/bin/env python3
from __future__ import annotations

import atexit
import base64
import csv
import gc
import hashlib
import json
import os
import re
import subprocess
import struct
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

MODEL_ID = "BAAI/bge-m3"
MODEL_SHORT = "bge_m3"
ENCODER_CONTRACT = "bge_m3_flagembedding_dense_v1"
RUN_STARTED_AT = datetime.now(timezone.utc)
RUN_STARTED_MONOTONIC = time.monotonic()
LAST_COLLECT_STATS: dict[str, int] = {}


def _is_bge_model_dir(path: Path) -> bool:
    return bool(
        path.is_dir()
        and (path / "config.json").is_file()
        and any((path / name).is_file() for name in ("model.safetensors", "pytorch_model.bin"))
        and any((path / name).is_file() for name in ("tokenizer.json", "sentencepiece.bpe.model"))
    )


def bge_model_reference() -> tuple[str, str]:
    """Resolve the pinned complete BGE-M3 Kaggle model before HF Hub."""
    explicit = str(os.getenv("REGION_TALK_BGE_MODEL_LOCAL_PATH") or "").strip()
    input_root = Path(os.getenv("REGION_TALK_KAGGLE_INPUT_ROOT") or "/kaggle/input")
    candidates: list[Path] = []
    if explicit:
        candidates.append(Path(explicit).expanduser())
    candidates.extend(
        [
            input_root / "models" / "andreasbis" / "baai-bge-m3" / "transformers" / "default" / "1",
            input_root / "baai-bge-m3" / "transformers" / "default" / "1",
            input_root / "bge-m3" / "transformers" / "default" / "1",
        ]
    )
    if input_root.exists():
        try:
            for config in input_root.rglob("config.json"):
                lowered = config.parent.as_posix().lower().replace("_", "-")
                if "bge-m3" in lowered:
                    candidates.append(config.parent)
        except Exception:
            pass
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        if _is_bge_model_dir(candidate):
            origin = "kaggle_model_input" if str(candidate).startswith("/kaggle/input/") else "local_model_path"
            return str(candidate), origin
    if getenv_bool("REGION_TALK_BGE_USE_KAGGLEHUB_FALLBACK", True):
        source = str(os.getenv("REGION_TALK_BGE_KAGGLE_MODEL_SOURCE") or "").strip()
        if source:
            try:
                import kagglehub  # type: ignore

                source_without_version = "/".join(source.strip("/").split("/")[:4])
                downloaded = Path(kagglehub.model_download(source_without_version))
                if _is_bge_model_dir(downloaded):
                    return str(downloaded), "kagglehub_model_cache"
                for config in downloaded.rglob("config.json"):
                    if _is_bge_model_dir(config.parent):
                        return str(config.parent), "kagglehub_model_cache"
            except Exception:
                pass
    return MODEL_ID, "huggingface_hub"


def apply_runtime_env_defaults() -> None:
    os.environ.setdefault("HF_HUB_DOWNLOAD_TIMEOUT", os.getenv("REGION_TALK_HF_HUB_DOWNLOAD_TIMEOUT", "60"))
    os.environ.setdefault("HF_HUB_ETAG_TIMEOUT", os.getenv("REGION_TALK_HF_HUB_ETAG_TIMEOUT", "20"))
    os.environ.setdefault("HF_HUB_DISABLE_XET", os.getenv("REGION_TALK_HF_HUB_DISABLE_XET", "1"))
    os.environ.setdefault("HF_HUB_DISABLE_TELEMETRY", "1")
    os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", os.getenv("REGION_TALK_HF_HUB_DISABLE_PROGRESS_BARS", "1"))
    os.environ.setdefault("TQDM_DISABLE", os.getenv("REGION_TALK_TQDM_DISABLE", "1"))
    os.environ.setdefault("TRANSFORMERS_VERBOSITY", os.getenv("REGION_TALK_TRANSFORMERS_VERBOSITY", "error"))


apply_runtime_env_defaults()


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
    return float(getenv_int("REGION_TALK_BGE_MAX_RUNTIME_SECONDS", 25 * 60)) - runtime_elapsed_seconds()


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
        fd, path = tempfile.mkstemp(prefix="region-talk-bge-ydb-sa-", suffix=".json")
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
        out = Path(os.getenv("REGION_TALK_BGE_EVENT_LOG_PATH") or "region_talk_bge_events.jsonl")
        with out.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception:
        pass
    if getenv_bool("REGION_TALK_BGE_STDOUT_EVENTS", True):
        printable = {k: v for k, v in row.items() if k not in {"embedding_vector", "embedding_vector_f16_b64"}}
        print("[region-talk-bge] " + json.dumps(printable, ensure_ascii=False, sort_keys=True), flush=True)
    if getenv_bool("REGION_TALK_BGE_WRITE_HEARTBEATS", True) and (os.getenv("REGION_TALK_STATE_BACKEND") or "").strip().lower() == "ydb":
        try:
            ydb, driver, cfg = ydb_connect()
            table_path = ydb_kv_table_path(cfg)
            pool = ydb.SessionPool(driver)
            run_id = str(payload.get("run_id") or os.getenv("REGION_TALK_RUN_ID") or "")
            compact = {k: v for k, v in row.items() if k in {
                "event_name", "created_at", "phase", "status", "progress_label", "run_id", "texts_loaded",
                "texts_done", "texts_total", "bge_batch_size", "bge_rows_written", "elapsed_seconds", "error",
            }}
            def op(session: Any) -> None:
                ensure_ydb_kv_table(ydb, session, table_path)
                ydb_upsert_json(session, ydb, table_path, "latest_business_heartbeat:bge_m3_enrichment", "business_heartbeat_bge_m3_enrichment", compact, row["created_at"], timeout_seconds=5)
                if run_id:
                    ydb_upsert_json(session, ydb, table_path, f"business_heartbeat:bge_m3_enrichment:{run_id}", "business_heartbeat_bge_m3_enrichment", compact, row["created_at"], timeout_seconds=5)
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
        "ko_editorial_publication": [
            "Содержательная статья внешнего издания о культуре, архитектуре, природе, истории или людях Калининградской области с оригинальным анализом и полезными деталями.",
            "Профессиональный обзор из нерегионального журнала раскрывает значимый калининградский проект и помогает широкой аудитории понять, чем он интересен.",
            "Редакционный лонгрид формирует доказательный положительный или конструктивно-нейтральный образ Калининградской области без рекламы и новостной сенсационности.",
        ],
        "ko_academic_publication": [
            "Научная публикация исследует природу, экологию, общество или наследие Калининградской области и содержит понятное широкой аудитории познавательное зерно.",
            "Рецензируемое исследование использует Калининградскую область как центральный объект, объясняет методы, результаты и ограничения без преувеличений.",
        ],
        "other_region_travel": [
            "Пост о Москве, московских парках, пляжах, маршрутах и прогулках, не связанный с Калининградской областью.",
            "Путешествие по Хайнаню, Турции, Беларуси, Европе, Кавказу, Байкалу, Сочи, Петербургу или другому региону, где Калининградская область не является основной темой.",
            "Рассказ о другом городе или стране, случайно содержащий слово, похожее на калининградский топоним.",
            "Маршрут находится в другом регионе, а лес, дюны или побережье лишь похожи на Калининград; Калининград используется только для сравнения.",
        ],
        "multi_region_roundup": [
            "Подборка разных регионов России: Калининград, Байкал, Дагестан, Сочи, Карелия, Алтай и другие направления одним списком.",
            "Дайджест куда поехать летом по России, где Калининградская область только один пункт среди многих регионов.",
            "Сравнение направлений или список городов из разных регионов и стран.",
        ],
        "news_report": [
            "Новость, официальное сообщение, заявление властей, происшествие, политика, суд, полиция, транспортные планы или исследовательская новость.",
            "Информационная заметка СМИ о факте, находке, решении, субсидиях, запуске парома или событии без личного опыта посещения региона.",
            "Локальная городская новость или федеральная новость, где место лишь контекст события.",
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


class BgeEncoder:
    def __init__(self) -> None:
        self.backend = (os.getenv("REGION_TALK_BGE_BACKEND") or "flagembedding").strip().lower()
        self.model: Any = None
        self.device = "unknown"

    def load(self) -> None:
        started = time.monotonic()
        model_reference, model_origin = bge_model_reference()
        emit_event("bge_model_load_started", phase="model_load", status="running", run_id=os.getenv("REGION_TALK_RUN_ID") or "", model_id=MODEL_ID, model_origin=model_origin, model_reference=model_reference, backend=self.backend)
        if self.backend in {"flagembedding", "flag", "bge"}:
            try:
                from FlagEmbedding import BGEM3FlagModel  # type: ignore
            except Exception:
                if getenv_bool("REGION_TALK_AUTO_INSTALL", True):
                    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "FlagEmbedding"])
                    from FlagEmbedding import BGEM3FlagModel  # type: ignore
                else:
                    raise
            use_fp16 = False
            try:
                import torch  # type: ignore
                self.device = "cuda" if getattr(torch, "cuda", None) and torch.cuda.is_available() else "cpu"
                use_fp16 = bool(self.device == "cuda" and getenv_bool("REGION_TALK_BGE_USE_FP16", True))
            except Exception:
                self.device = "cpu"
            self.model = BGEM3FlagModel(model_reference, use_fp16=use_fp16)
            self.backend = "flagembedding"
            emit_event("bge_model_load_done", phase="model_load", status="running", run_id=os.getenv("REGION_TALK_RUN_ID") or "", model_id=MODEL_ID, model_origin=model_origin, model_reference=model_reference, backend=self.backend, device=self.device, use_fp16=use_fp16, elapsed_seconds=round(time.monotonic() - started, 3))
            return
        if self.backend in {"sentence-transformers", "sentence_transformers", "st"}:
            try:
                from sentence_transformers import SentenceTransformer  # type: ignore
            except Exception:
                if getenv_bool("REGION_TALK_AUTO_INSTALL", True):
                    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "sentence-transformers"])
                    from sentence_transformers import SentenceTransformer  # type: ignore
                else:
                    raise
            self.model = SentenceTransformer(model_reference)
            self.device = str(getattr(self.model, "device", "unknown"))
            self.backend = "sentence_transformers"
            emit_event("bge_model_load_done", phase="model_load", status="running", run_id=os.getenv("REGION_TALK_RUN_ID") or "", model_id=MODEL_ID, model_origin=model_origin, model_reference=model_reference, backend=self.backend, device=self.device, elapsed_seconds=round(time.monotonic() - started, 3))
            return
        raise RuntimeError(f"unsupported REGION_TALK_BGE_BACKEND={self.backend}")

    def encode(self, texts: list[str], *, batch_size: int, max_length: int) -> Any:
        if self.model is None:
            self.load()
        if self.backend == "flagembedding":
            output = self.model.encode(
                texts,
                batch_size=max(1, batch_size),
                max_length=max(32, max_length),
                return_dense=True,
                return_sparse=False,
                return_colbert_vecs=False,
            )
            dense = output["dense_vecs"] if isinstance(output, dict) else output
            return _normalize_matrix(dense)
        embeddings = self.model.encode(texts, batch_size=max(1, batch_size), normalize_embeddings=True, show_progress_bar=False)
        return _normalize_matrix(embeddings)

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


def encode_dense_vector_f16(vector: Iterable[float]) -> str:
    """Encode a normalized dense vector as compact portable float16 bytes."""
    values = [float(value) for value in vector]
    if not values:
        return ""
    raw = struct.pack("<" + ("e" * len(values)), *values)
    return base64.b64encode(raw).decode("ascii")


def decode_dense_vector_f16(value: str, embedding_dim: int) -> list[float]:
    """Decode the YDB f16 representation for future anti-vector scoring."""
    raw = base64.b64decode(str(value or ""))
    expected = max(0, int(embedding_dim)) * 2
    if len(raw) != expected:
        raise ValueError(f"invalid f16 vector bytes: actual={len(raw)} expected={expected}")
    return [float(item) for item in struct.unpack("<" + ("e" * int(embedding_dim)), raw)]


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
        raw = compact_text(row.get(field), max_len=getenv_int("REGION_TALK_BGE_FIELD_TEXT_MAX_CHARS", 3000))
        if raw and raw not in parts:
            parts.append(raw)
            used.append(field)
    return compact_text(". ".join(parts), max_len=getenv_int("REGION_TALK_BGE_TEXT_MAX_CHARS", 3000)), used


def enrichment_pk(post_id: str, post_url: str, text_sha: str, model_id: str = MODEL_ID) -> str:
    base = post_id or stable_hash(post_url, text_sha, length=16)
    model_key = "bge_m3" if model_id == MODEL_ID else stable_hash(model_id, length=8)
    return f"text_vector_enrichment_item:{base}:{model_key}:{text_sha[:12]}"


def _is_e5_text_vector_row(row: dict[str, Any]) -> bool:
    return str(row.get("model_short") or "") == "e5" or str(row.get("model_id") or "").startswith("intfloat/multilingual-e5")


def _is_bge_text_vector_row(row: dict[str, Any]) -> bool:
    return str(row.get("model_short") or "") == MODEL_SHORT or str(row.get("model_id") or "") == MODEL_ID


def _source_terminal_excluded(row: dict[str, Any]) -> bool:
    return bool(
        row.get("source_terminal_excluded") is True
        or str(row.get("source_terminal_excluded") or "").lower() in {"1", "true", "yes"}
        or str(row.get("source_queue_status") or row.get("fetch_status") or "")
        in {"rejected_local_region_source", "rejected_spam_source"}
        or str(row.get("source_scope") or "") in {"local_region", "spam"}
        or str(row.get("source_geo_class") or "") == "kaliningrad_local"
        or str(row.get("source_quick_class") or "") in {"local_region_source", "spam_source_reject"}
        or str(row.get("source_topic_class") or "")
        in {"local_region_source_surface", "spam_or_commercial_hashtag_source"}
    )


def _is_product_priority_row(row: dict[str, Any]) -> bool:
    method = str(row.get("discovery_method") or "").lower()
    reason = str(row.get("priority_reason") or "").lower()
    source_status = str(row.get("source_queue_status") or "").lower()
    source_topic = str(row.get("source_topic_class") or "").lower()
    try:
        exact_priority = int(row.get("post_link_priority")) == 0
    except (TypeError, ValueError):
        exact_priority = False
    return bool(
        exact_priority
        or method == "exact_post_link_queue"
        or row.get("keyword_hit_query")
        or row.get("keyword_hit_hashtag")
        or "keyword" in reason
        or "fast_check" in reason
        or source_status == "confirmed_external_publication_research"
        or source_topic in {"editorial_publication", "academic_publication"}
    )


def _existing_bge_row_is_current(row: dict[str, Any] | None, *, text_hash_value: str) -> bool:
    """Return whether an existing BGE PK satisfies the active scoring bank.

    A PK identifies model + post + text, but not the semantic prototype bank.
    Reusing a stale row after that bank changes leaves CandidateReport waiting
    forever: it correctly rejects the stale bank while the BGE worker skips the
    already-present PK.  A stale row must therefore be overwritten in place.
    """
    if not isinstance(row, dict) or not row:
        return False
    semantic_version, semantic_hash = bank_version_and_hash(semantic_bank_v1(), version="semantic_bank_v1")
    return bool(
        _is_bge_text_vector_row(row)
        and str(row.get("text_hash") or "") == text_hash_value
        and str(row.get("encoder_contract") or "") == ENCODER_CONTRACT
        and str(row.get("semantic_bank_version") or "") == semantic_version
        and str(row.get("semantic_bank_hash") or "") == semantic_hash[:16]
        and bool(row.get("semantic_scores_by_class"))
    )


def collect_text_rows(
    items_by_kind: dict[str, dict[str, dict[str, Any]]],
    *,
    existing_pks: set[str] | dict[str, dict[str, Any]],
    limit: int,
    include_existing: bool = False,
) -> list[dict[str, Any]]:
    global LAST_COLLECT_STATS
    rows: list[dict[str, Any]] = []
    stats = {
        "input_rows": 0,
        "source_terminal_skipped": 0,
        "non_e5_skipped": 0,
        "short_text_skipped": 0,
        "existing_skipped": 0,
        "missing_current_bge": 0,
        "existing_stale_rescore": 0,
        "selected_missing_current_bge": 0,
        "selected_stale_rescore": 0,
        "duplicate_skipped": 0,
        "eligible_rows": 0,
        "selected_rows": 0,
    }
    seen_text_or_url: set[str] = set()
    e5_only = getenv_bool("REGION_TALK_BGE_E5_ONLY", True)
    priority = {
        "text_vector_enrichment_item": 0,
        "publication_candidate_item": 0,
        "candidate_memory_item": 1,
        "image_queue_item": 2,
        "processed_post_item": 3,
        "post_live_item": 4,
    }
    # Tuple layout: (needs_stale_refresh, source_priority, post_date, row).
    # A missing BGE pair blocks the live E5 -> BGE -> fusion funnel, while a
    # stale semantic-bank row already has a usable historical pair and is
    # maintenance work.  Keep both populations, but never let a large bank
    # refresh consume a batch ahead of fresh missing pairs.
    candidates: list[tuple[bool, int, str, dict[str, Any]]] = []
    for kind, items in items_by_kind.items():
        for _pk, row in items.items():
            if not isinstance(row, dict):
                continue
            stats["input_rows"] += 1
            if kind == "text_vector_enrichment_item":
                # BGE is the external consumer of E5 rows.  Never re-process
                # BGE rows as input, otherwise the table accumulates BGE-on-BGE
                # duplicates and raw BGE totals become larger than E5 totals.
                if _is_bge_text_vector_row(row):
                    stats["non_e5_skipped"] += 1
                    continue
                if e5_only and not _is_e5_text_vector_row(row):
                    stats["non_e5_skipped"] += 1
                    continue
            elif e5_only:
                # Production dual-model mode is E5 -> BGE.  Historical direct
                # candidate/image/post rows can still be enabled explicitly for
                # research via REGION_TALK_BGE_E5_ONLY=0.
                stats["non_e5_skipped"] += 1
                continue
            if _source_terminal_excluded(row):
                stats["source_terminal_skipped"] += 1
                continue
            text, used = text_from_row(row)
            # Generic tiny snippets are usually low-value noise, but an exact
            # keyword/fast-check post is already scarce product evidence. It
            # must receive the second model even when the caption is short;
            # otherwise it remains forever in `dual_vector_pending`.
            if len(text) < getenv_int("REGION_TALK_BGE_MIN_TEXT_CHARS", 24) and not _is_product_priority_row(row):
                stats["short_text_skipped"] += 1
                continue
            post_url = str(row.get("post_url") or "").strip()
            post_id = str(row.get("post_id") or row.get("candidate_memory_id") or row.get("publication_candidate_id") or "").strip()
            source_text_hash = str(row.get("text_hash") or "").strip() if kind == "text_vector_enrichment_item" and _is_e5_text_vector_row(row) else ""
            sha = source_text_hash or text_hash(text)
            pk = enrichment_pk(post_id, post_url, sha)
            needs_stale_refresh = False
            if pk in existing_pks:
                # Legacy callers/tests may provide only a set. Preserve their
                # historical meaning (present == current). Live YDB loading
                # passes the payload mapping so model/bank drift is auditable.
                existing_row = existing_pks.get(pk) if isinstance(existing_pks, dict) else None
                if not include_existing and (
                    not isinstance(existing_pks, dict)
                    or _existing_bge_row_is_current(existing_row, text_hash_value=sha)
                ):
                    stats["existing_skipped"] += 1
                    continue
                # Explicit reprocessing and stale-bank repair are both
                # maintenance work, never a reason to delay a missing pair.
                stats["existing_stale_rescore"] += 1
                needs_stale_refresh = True
            else:
                stats["missing_current_bge"] += 1
            dedupe_key = post_url or sha
            if dedupe_key in seen_text_or_url:
                stats["duplicate_skipped"] += 1
                continue
            seen_text_or_url.add(dedupe_key)
            rr = dict(row)
            rr["_source_kind"] = kind
            rr["_embedding_text"] = text
            rr["_embedding_text_fields"] = used
            rr["_embedding_text_hash"] = sha
            if source_text_hash:
                rr["_paired_e5_text_hash"] = source_text_hash
                rr["_paired_e5_enrichment_id"] = row.get("text_vector_enrichment_id") or str(row.get("_ydb_pk") or "").replace("text_vector_enrichment_item:", "")
            rr["_enrichment_pk"] = pk
            candidates.append(
                (
                    needs_stale_refresh,
                    priority.get(kind, 9),
                    str(row.get("post_date") or row.get("published_at") or ""),
                    rr,
                )
            )
    stats["eligible_rows"] = len(candidates)

    def is_product_priority(item: tuple[bool, int, str, dict[str, Any]]) -> bool:
        return _is_product_priority_row(item[3])

    def post_timestamp(item: tuple[bool, int, str, dict[str, Any]]) -> float:
        raw = str(item[2] or "").strip()
        if not raw:
            return 0.0
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.timestamp()
        except (TypeError, ValueError):
            return 0.0

    target = max(1, limit)
    share = min(100, max(0, getenv_int("REGION_TALK_BGE_PRIORITY_SHARE_PERCENT", 80)))

    def select_lane_population(
        population: list[tuple[bool, int, str, dict[str, Any]]],
        slots: int,
    ) -> list[tuple[bool, int, str, dict[str, Any]]]:
        if slots <= 0 or not population:
            return []
        # High-confidence exact KO links are fresh-first; the ordinary reserve
        # is oldest-first. The reserve avoids starving generic discovery.
        product = sorted(
            (item for item in population if is_product_priority(item)),
            key=lambda item: (item[1], -post_timestamp(item)),
        )
        fifo = sorted(
            (item for item in population if not is_product_priority(item)),
            key=lambda item: (item[1], post_timestamp(item)),
        )
        product_cap = min(len(product), max(1, int(round(slots * share / 100.0)))) if product else 0
        fifo_cap = min(len(fifo), slots - product_cap)
        chosen = product[:product_cap] + fifo[:fifo_cap]
        if len(chosen) < slots:
            chosen.extend(product[product_cap:product_cap + (slots - len(chosen))])
        if len(chosen) < slots:
            chosen.extend(fifo[fifo_cap:fifo_cap + (slots - len(chosen))])
        return chosen

    # Missing pairs are live funnel work. Stale semantic-bank refresh is
    # maintenance and only fills capacity left after every selectable missing
    # pair. This is intentionally stronger than merely sorting the already
    # selected 80/20 population: a generic missing pair must beat an exact-link
    # stale refresh when the batch is full.
    missing_population = [item for item in candidates if not item[0]]
    stale_population = [item for item in candidates if item[0]]
    selected = select_lane_population(missing_population, target)
    if len(selected) < target:
        selected.extend(select_lane_population(stale_population, target - len(selected)))
    for needs_stale_refresh, _priority, _date, row in selected:
        rows.append(row)
        if needs_stale_refresh:
            stats["selected_stale_rescore"] += 1
        else:
            stats["selected_missing_current_bge"] += 1
    stats["selected_rows"] = len(rows)
    LAST_COLLECT_STATS = stats
    return rows


def load_ydb_rows(limit: int, *, include_existing: bool = False) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    max_scan = max(limit * 5, getenv_int("REGION_TALK_BGE_YDB_SCAN_LIMIT", 1000))
    kinds = [k.strip() for k in re.split(r"[,;+\s]+", os.getenv("REGION_TALK_BGE_INPUT_KINDS") or "text_vector_enrichment_item,publication_candidate_item,candidate_memory_item,image_queue_item,processed_post_item,post_live_item") if k.strip()]
    attempts = max(1, getenv_int("REGION_TALK_BGE_YDB_LOAD_ATTEMPTS", 3))
    backoff = max(0.0, getenv_float("REGION_TALK_BGE_YDB_LOAD_RETRY_BASE_SECONDS", 3.0))
    last_error: Exception | None = None

    # A main CandidateReport run may briefly contend for the same serverless
    # YDB capacity. Recreate the driver/session on each bounded outer retry:
    # SessionPool's internal retry can still exhaust on DEADLINE_EXCEEDED or
    # RESOURCE_EXHAUSTED while the concurrent writer is active.
    for attempt in range(1, attempts + 1):
        ydb, driver, cfg = ydb_connect()
        table_path = ydb_kv_table_path(cfg)
        pool = ydb.SessionPool(driver)

        def op(session: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
            ensure_ydb_kv_table(ydb, session, table_path)
            items_by_kind: dict[str, dict[str, dict[str, Any]]] = {}
            for kind in kinds:
                items_by_kind[kind] = ydb_select_kind_items(session, ydb, table_path, kind, limit=max_scan)
            # In the production E5-only lane the input kind is already the
            # complete vector ledger. Reuse that read instead of scanning the
            # same 4k+ rows twice before every small BGE batch.
            existing = items_by_kind.get("text_vector_enrichment_item")
            if existing is None:
                existing = ydb_select_kind_items(session, ydb, table_path, "text_vector_enrichment_item", limit=max_scan)
            existing_pks = set(existing.keys())
            rows = collect_text_rows(items_by_kind, existing_pks=existing, limit=limit, include_existing=include_existing)
            meta = {
                "table_path": table_path,
                "input_kinds": kinds,
                "loaded_by_kind": {kind: len(items_by_kind.get(kind) or {}) for kind in kinds},
                "existing_text_vector_enrichment_items": len(existing_pks),
                "e5_only": getenv_bool("REGION_TALK_BGE_E5_ONLY", True),
                "collect_stats": dict(LAST_COLLECT_STATS),
                "ydb_load_attempt": attempt,
            }
            return rows, meta

        try:
            return pool.retry_operation_sync(op)
        except Exception as exc:
            last_error = exc
            if attempt >= attempts:
                raise
            delay = backoff * attempt
            emit_event(
                "bge_ydb_load_retry",
                phase="load_ydb",
                status="retrying",
                run_id=str(os.getenv("REGION_TALK_RUN_ID") or ""),
                attempt=attempt,
                attempts_total=attempts,
                retry_delay_seconds=round(delay, 3),
                error_type=type(exc).__name__,
                error=str(exc)[:500],
            )
            if delay > 0:
                time.sleep(delay)
        finally:
            driver.stop(timeout=5)
    assert last_error is not None
    raise last_error


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
    positive_labels = {
        "ko_visit_impression", "ko_route_useful", "ko_visual_place_card",
        "ko_editorial_publication", "ko_academic_publication",
    }
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
        "text_vector_enrichment_id": str(row.get("_enrichment_pk") or enrichment_pk(post_id, post_url, text_sha)).replace("text_vector_enrichment_item:", ""),
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
        "text_source_fields": row.get("_embedding_text_fields") or [],
        "paired_e5_text_hash": row.get("_paired_e5_text_hash") or "",
        "paired_e5_enrichment_id": row.get("_paired_e5_enrichment_id") or "",
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
        "bge_m3_top_class": top_class,
        "bge_m3_top_score": round(float(top_score), 4),
        "bge_m3_positive_class": pos_class,
        "bge_m3_positive_score": round(float(pos_score), 4),
        "bge_m3_negative_class": neg_class,
        "bge_m3_negative_score": round(float(neg_score), 4),
        "bge_m3_margin_positive_vs_negative": round(float(pos_score - neg_score), 4),
        "bge_m3_ko_geo_top": ko_geo_class.replace("ko_geo:", ""),
        "bge_m3_ko_geo_score": round(float(ko_geo_score), 4),
        "bge_m3_external_geo_top": re.sub(r"^external_(?:ru_|country_)?geo:", "", external_geo_class),
        "bge_m3_external_geo_score": round(float(external_geo_score), 4),
        "bge_m3_ko_vs_external_geo_margin": round(float(ko_geo_score - external_geo_score), 4),
        "vector_gate_status_bge_m3": "bge_m3_accept_candidate" if pos_score >= neg_score and (pos_score - neg_score) >= getenv_float("REGION_TALK_BGE_ACCEPT_MARGIN", 0.02) else "bge_m3_review_or_reject",
        "dense_vector_stored": bool(dense_vector),
    }
    if dense_vector is not None:
        payload["embedding_vector_f16_b64"] = encode_dense_vector_f16(dense_vector)
        payload["embedding_vector_encoding"] = "f16_le_base64"
    return payload


def compact_paired_e5_payload(row: dict[str, Any], *, pruned_at: str) -> dict[str, Any]:
    """Drop the transient BGE input text after the matching BGE row exists.

    The E5 row remains the durable owner of semantic scores and pairing metadata;
    only the text payload that BGE has already consumed is removed. This keeps
    dual-vector scoring intact without retaining thousands of 3k-character
    excerpts indefinitely.
    """
    out = {k: v for k, v in row.items() if not str(k).startswith("_")}
    for field in ("text", "full_text", "text_excerpt", "raw", "why_keep_in_memory", "why_this_is_about_kaliningrad"):
        out.pop(field, None)
    out["text_payload_pruned_after_bge"] = True
    out["text_payload_pruned_at"] = pruned_at
    return out


def write_result_rows(rows: list[dict[str, Any]], summary: dict[str, Any]) -> int:
    ydb, driver, cfg = ydb_connect()
    table_path = ydb_kv_table_path(cfg)
    pool = ydb.SessionPool(driver)
    now = utc_now_iso()
    run_id = str(summary.get("run_id") or "")
    ydb_rows = [
        (
            str(row.get("_pk") or "text_vector_enrichment_item:" + row["text_vector_enrichment_id"]),
            "text_vector_enrichment_item",
            {k: v for k, v in row.items() if not str(k).startswith("_")},
        )
        for row in rows
    ]
    # The original post text remains transiently available after pairing so a
    # later CandidateReport/Image/Gemini pass can judge the complete post
    # without refetching Telegram. Terminal-state maintenance removes it after
    # accept/reject/sent. The old immediate-prune mode remains opt-in for
    # research probes that do not run the publication tail.
    paired_e5_rows = [
        (str(row["_paired_e5_pk"]), "text_vector_enrichment_item", dict(row["_paired_e5_payload"]))
        for row in rows
        if getenv_bool("REGION_TALK_BGE_PRUNE_E5_TEXT_AFTER_PAIR", False)
        and row.get("_paired_e5_pk") and isinstance(row.get("_paired_e5_payload"), dict)
    ]

    def op(session: Any) -> int:
        ensure_ydb_kv_table(ydb, session, table_path)
        written = ydb_upsert_json_many(session, ydb, table_path, ydb_rows, now, chunk_size=getenv_int("REGION_TALK_BGE_YDB_UPSERT_CHUNK_SIZE", 25))
        pruned = ydb_upsert_json_many(
            session,
            ydb,
            table_path,
            paired_e5_rows,
            now,
            chunk_size=getenv_int("REGION_TALK_BGE_YDB_UPSERT_CHUNK_SIZE", 25),
        )
        summary["e5_text_payloads_pruned"] = pruned
        final_summary = {**summary, "rows_written": written, "ydb_write_status": "ok"}
        result_payload = {
            "summary": final_summary,
            "row_count": len(rows),
            # Result rows are observability, not a second copy of enrichment
            # data. Keep only a handful of compact references; full scores and
            # the vector live in ``text_vector_enrichment_item``.
            "sample_refs": [
                {
                    "post_id": row.get("post_id"),
                    "post_url": row.get("post_url"),
                    "text_hash": row.get("text_hash"),
                    "vector_gate_status_bge_m3": row.get("vector_gate_status_bge_m3"),
                    "bge_m3_top_class": row.get("bge_m3_top_class"),
                    "bge_m3_top_score": row.get("bge_m3_top_score"),
                }
                for row in rows[:5]
            ],
        }
        ydb_upsert_json(session, ydb, table_path, f"bge_m3_enrichment_result:{run_id}", "bge_m3_enrichment_result", result_payload, now)
        ydb_upsert_json(session, ydb, table_path, "bge_m3_enrichment_result:latest", "bge_m3_enrichment_result", result_payload, now)
        return written

    try:
        return pool.retry_operation_sync(op)
    finally:
        driver.stop(timeout=5)


def run_bge_enrichment(run_id: str, output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    emit_event("bge_enrichment_started", phase="load_ydb", status="running", run_id=run_id, model_id=MODEL_ID)
    limit = max(1, getenv_int("REGION_TALK_BGE_BATCH_LIMIT", 12))
    include_existing = getenv_bool("REGION_TALK_BGE_REPROCESS_EXISTING", False)
    rows, ydb_meta = load_ydb_rows(limit, include_existing=include_existing)
    if not rows and getenv_bool("REGION_TALK_BGE_ALLOW_FALLBACK_TEXTS", False):
        rows = make_fallback_rows(limit)
        ydb_meta["fallback_texts_used"] = True
    emit_event("bge_text_rows_loaded", phase="load_ydb", status="running", run_id=run_id, texts_loaded=len(rows), texts_total=len(rows), source_terminal_skipped=int((ydb_meta.get("collect_stats") or {}).get("source_terminal_skipped") or 0), progress_label=f"BGE rows loaded {len(rows)}")
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
        (output_dir / "bge_m3_enrichment_result.json").write_text(json.dumps({"summary": summary, "rows": []}, ensure_ascii=False, indent=2), encoding="utf-8")
        (Path.cwd() / "output.json").write_text(json.dumps({"ok": True, "status": "no_rows", "run_id": run_id, "summary": summary}, ensure_ascii=False, indent=2), encoding="utf-8")
        emit_event(
            "bge_enrichment_done",
            phase="write_ydb",
            status="no_rows",
            run_id=run_id,
            texts_done=0,
            texts_total=0,
            bge_rows_written=0,
            elapsed_seconds=summary["elapsed_seconds"],
        )
        return {"ok": True, "status": "no_rows", "summary": summary, "rows": []}

    encoder = BgeEncoder()
    batch_size = max(1, getenv_int("REGION_TALK_BGE_BATCH_SIZE", 4))
    max_length = max(64, getenv_int("REGION_TALK_BGE_MAX_LENGTH", 2048))
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
    emit_event("bge_prototype_vectors_ready", phase="vectorize", status="running", run_id=run_id, semantic_prototypes=len(semantic_texts), geo_prototypes=len(geo_texts), elapsed_seconds=round(time.monotonic() - started, 3))

    texts = [str(row.get("_embedding_text") or "") for row in rows]
    store_vectors = getenv_bool("REGION_TALK_BGE_STORE_DENSE_VECTORS", True)
    store_vector_max_rows = max(0, getenv_int("REGION_TALK_BGE_STORE_VECTOR_MAX_ROWS", 100))
    result_rows: list[dict[str, Any]] = []
    vectors_done = 0
    for start in range(0, len(texts), batch_size):
        if runtime_remaining_seconds() < getenv_int("REGION_TALK_BGE_RUNTIME_RESERVE_SECONDS", 90):
            emit_event("bge_runtime_reserve_reached", phase="vectorize", status="partial", run_id=run_id, texts_done=vectors_done, texts_total=len(texts), runtime_remaining_seconds=round(runtime_remaining_seconds(), 1))
            break
        batch_texts = texts[start:start + batch_size]
        query_vecs = encoder.encode(batch_texts, batch_size=batch_size, max_length=max_length)
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
            payload["_pk"] = str(row.get("_enrichment_pk") or "text_vector_enrichment_item:" + payload["text_vector_enrichment_id"])
            paired_e5_pk = str(row.get("_ydb_pk") or "").strip()
            if paired_e5_pk and row.get("_source_kind") == "text_vector_enrichment_item" and _is_e5_text_vector_row(row):
                payload["_paired_e5_pk"] = paired_e5_pk
                payload["_paired_e5_payload"] = compact_paired_e5_payload(row, pruned_at=utc_now_iso())
            result_rows.append(payload)
        vectors_done = len(result_rows)
        emit_event("bge_batch_done", phase="vectorize", status="running", run_id=run_id, texts_done=vectors_done, texts_total=len(texts), bge_batch_size=len(batch_texts), progress_label=f"BGE {vectors_done}/{len(texts)}")
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
        emit_event("bge_ydb_write_failed", phase="write_ydb", status="error", run_id=run_id, error=summary["error"])

    public_rows = [
        {
            k: v
            for k, v in row.items()
            if not str(k).startswith("_") and k not in {"embedding_vector", "embedding_vector_f16_b64"}
        }
        for row in result_rows
    ]
    (output_dir / "bge_m3_enrichment_result.json").write_text(json.dumps({"summary": summary, "rows": public_rows}, ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / "bge_m3_enrichment_rows.jsonl").write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in public_rows) + ("\n" if public_rows else ""), encoding="utf-8")
    (output_dir / "stage_status.json").write_text(json.dumps({"run_id": run_id, "generated_at": utc_now_iso(), "status": summary["status"], "summary": summary}, ensure_ascii=False, indent=2), encoding="utf-8")
    for p in [output_dir / "bge_m3_enrichment_result.json", output_dir / "bge_m3_enrichment_rows.jsonl", output_dir / "stage_status.json"]:
        target = Path.cwd() / p.name
        if target.resolve() != p.resolve():
            target.write_bytes(p.read_bytes())
    (Path.cwd() / "output.json").write_text(json.dumps({"ok": summary.get("ok"), "status": summary["status"], "run_id": run_id, "summary": summary}, ensure_ascii=False, indent=2), encoding="utf-8")
    emit_event("bge_enrichment_done", phase="write_ydb", status=summary["status"], run_id=run_id, texts_done=summary["rows_scored"], texts_total=summary["rows_loaded"], bge_rows_written=summary["rows_written"], elapsed_seconds=summary["elapsed_seconds"])
    return {"ok": bool(summary.get("ok")), "status": summary["status"], "summary": summary, "rows": public_rows}


def main() -> int:
    load_dotenv(Path(".env"))
    config = load_split_runtime_from_kaggle_input()
    run_id = os.getenv("REGION_TALK_RUN_ID") or f"region-talk-bge-m3-{RUN_STARTED_AT.strftime('%Y%m%dT%H%M%SZ')}"
    os.environ.setdefault("REGION_TALK_RUN_ID", run_id)
    os.environ.setdefault("REGION_TALK_STATE_BACKEND", "ydb")
    os.environ.setdefault("REGION_TALK_REQUIRE_YDB_STATE", "1")
    os.environ.setdefault("REGION_TALK_DISABLE_PUBLISH", "1")
    os.environ.setdefault("REGION_TALK_DRY_RUN", "1")
    os.environ.setdefault("REGION_TALK_AUTH_BUNDLE_ENV", "REGION_TALK_NO_TELEGRAM_BUNDLE")
    if (os.getenv("REGION_TALK_STATE_BACKEND") or "").strip().lower() != "ydb" and getenv_bool("REGION_TALK_REQUIRE_YDB_STATE", True):
        raise RuntimeError("RegionTalkBgeM3Enrichment requires REGION_TALK_STATE_BACKEND=ydb for live runs")
    out_dir = Path(os.getenv("REGION_TALK_OUTPUT_DIR") or f"artifacts/region-talk/runs/{run_id}")
    payload = run_bge_enrichment(run_id, out_dir)
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
