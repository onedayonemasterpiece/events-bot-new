from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
import shutil
import tempfile
import time
from contextlib import asynccontextmanager

import aiosqlite
from sqlalchemy.ext.asyncio import AsyncConnection

_KNOWN_DATABASES: set["Database"] = set()

_VALID_JOURNAL_MODES = {"WAL", "DELETE", "TRUNCATE", "PERSIST", "MEMORY", "OFF"}


def _managed_vk_publication_globs() -> tuple[str, ...]:
    """Return exact-owner SQLite GLOBs for our managed VK projections."""

    group_ids = {
        str(os.getenv(name) or "").strip().lstrip("-")
        for name in ("VK_EVENTS_GROUP_ID", "VK_AFISHA_GROUP_ID")
    }
    group_ids.discard("")
    # Production default used by the publisher when the env alias is absent.
    group_ids.add("231920894")
    return tuple(f"*vk.com/wall-{group_id}_[0-9]*" for group_id in sorted(group_ids))


def _exclude_managed_vk_sql(column: str, globs: tuple[str, ...]) -> tuple[str, tuple[str, ...]]:
    if not globs:
        return "", ()
    predicates = " OR ".join(f"LOWER({column}) GLOB ?" for _ in globs)
    return f" AND NOT ({predicates})", globs


async def _add_column(conn, table: str, col_def: str) -> None:
    try:
        await conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_def}")
    except Exception as e:
        msg = str(e).lower()
        if "duplicate column name" in msg:
            return
        # SQLite restriction: ALTER TABLE ... ADD COLUMN only supports constant defaults.
        # Some prod snapshots may have older schema, and migrations here may attempt to add
        # columns like "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP", which would crash
        # startup with "Cannot add a column with non-constant default".
        if "non-constant default" in msg:
            sanitized = re.sub(
                r"\s+default\s+\(?current_timestamp\)?\b",
                "",
                col_def,
                flags=re.IGNORECASE,
            ).strip()
            if sanitized and sanitized != col_def:
                await conn.execute(f"ALTER TABLE {table} ADD COLUMN {sanitized}")
                return
        raise


class Database:
    def __init__(self, path: str):
        self.path = path
        # Ensure the directory exists for file-backed sqlite DBs.
        # This avoids failures in local/test environments when DB_PATH points to /data/db.sqlite.
        if path and not path.startswith((":memory:", "file:")):
            parent = os.path.dirname(path)
            if parent and parent not in (".", ""):
                try:
                    os.makedirs(parent, exist_ok=True)
                except PermissionError:
                    fallback = os.path.join(tempfile.gettempdir(), os.path.basename(path))
                    logging.warning(
                        "Database directory is not writable: %s. Falling back to %s",
                        parent,
                        fallback,
                    )
                    self.path = fallback
        self._conn: aiosqlite.Connection | None = None
        self._orm_engine = None
        self._sessionmaker = None
        _KNOWN_DATABASES.add(self)

    @staticmethod
    def _read_float_env(name: str, default: float) -> float:
        raw = (os.getenv(name) or "").strip()
        if not raw:
            return default
        try:
            return float(raw)
        except Exception:
            return default

    @classmethod
    def _sqlite_timeout_sec(cls) -> float:
        # sqlite3 "timeout" is busy_timeout (seconds). Keep reasonably high
        # to avoid flaky "database is locked" under concurrent async workers.
        return max(0.1, min(cls._read_float_env("DB_TIMEOUT_SEC", 30.0), 120.0))

    @classmethod
    def _sqlite_busy_timeout_ms(cls) -> int:
        raw = (os.getenv("DB_BUSY_TIMEOUT_MS") or "").strip()
        if raw:
            try:
                return int(raw)
            except Exception:
                pass
        return int(cls._sqlite_timeout_sec() * 1000)

    @staticmethod
    def _sqlite_journal_mode() -> str:
        journal_mode = (os.getenv("DB_JOURNAL_MODE") or "WAL").strip().upper()
        if journal_mode not in _VALID_JOURNAL_MODES:
            journal_mode = "WAL"
        return journal_mode

    @staticmethod
    def _sqlite_journal_size_limit_bytes() -> int:
        """Return the bounded retained-WAL target applied to every connection."""

        raw = (os.getenv("DB_WAL_JOURNAL_SIZE_LIMIT_MB") or "64").strip()
        try:
            size_mb = int(raw)
        except (TypeError, ValueError):
            size_mb = 64
        return max(4, min(size_mb, 256)) * 1024 * 1024

    async def _apply_sqlite_pragmas(self, conn: aiosqlite.Connection) -> None:
        journal_mode = self._sqlite_journal_mode()
        await conn.execute(f"PRAGMA journal_mode={journal_mode}")
        await conn.execute("PRAGMA synchronous=NORMAL")
        await conn.execute("PRAGMA foreign_keys=ON")
        await conn.execute("PRAGMA temp_store=MEMORY")
        await conn.execute("PRAGMA cache_size=-40000")
        await conn.execute(f"PRAGMA busy_timeout={self._sqlite_busy_timeout_ms()}")
        await conn.execute("PRAGMA mmap_size=134217728")
        await conn.execute(
            f"PRAGMA journal_size_limit={self._sqlite_journal_size_limit_bytes()}"
        )

    def _create_orm_engine(self):
        from sqlalchemy import event
        from sqlalchemy.ext.asyncio import create_async_engine
        from sqlalchemy.pool import NullPool

        engine = create_async_engine(
            f"sqlite+aiosqlite:///{self.path}",
            future=True,
            poolclass=NullPool,
            connect_args={"timeout": self._sqlite_timeout_sec()},
        )

        journal_mode = self._sqlite_journal_mode()
        busy_timeout_ms = self._sqlite_busy_timeout_ms()
        journal_size_limit_bytes = self._sqlite_journal_size_limit_bytes()

        @event.listens_for(engine.sync_engine, "connect")
        def _set_sqlite_pragmas(dbapi_connection, _connection_record) -> None:
            cursor = None
            try:
                cursor = dbapi_connection.cursor()
                cursor.execute(f"PRAGMA journal_mode={journal_mode}")
                cursor.execute("PRAGMA synchronous=NORMAL")
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.execute("PRAGMA temp_store=MEMORY")
                cursor.execute("PRAGMA cache_size=-40000")
                cursor.execute(f"PRAGMA busy_timeout={int(busy_timeout_ms)}")
                cursor.execute("PRAGMA mmap_size=134217728")
                cursor.execute(f"PRAGMA journal_size_limit={journal_size_limit_bytes}")
            except Exception:
                logging.debug("Failed to apply sqlite PRAGMAs on ORM connection", exc_info=True)
            finally:
                try:
                    if cursor is not None:
                        cursor.close()
                except Exception:
                    pass

        return engine

    async def close(self) -> None:
        if self._sessionmaker is not None:
            self._sessionmaker = None
        if self._orm_engine is not None:
            await self._orm_engine.dispose()
            self._orm_engine = None
        if self._conn is not None:
            await self._conn.close()
            self._conn = None
        _KNOWN_DATABASES.discard(self)

    async def init(self) -> None:
        async with aiosqlite.connect(self.path) as conn:
            debug = (os.getenv("DB_INIT_DEBUG") or "").strip().lower() in {"1", "true", "yes"}
            minimal_mode = (os.getenv("DB_INIT_MINIMAL") or "").strip().lower() in {"1", "true", "yes"}
            skip_posterocr_migration = minimal_mode or (
                (os.getenv("DB_INIT_SKIP_POSTER_OCR_MIGRATION") or "").strip().lower() in {"1", "true", "yes"}
            )

            def dbg(msg: str) -> None:
                if debug:
                    logging.info("db.init %s", msg)

            dbg(f"start path={self.path}")
            # WAL is fast but can be problematic on some filesystems (e.g. network/virtual mounts).
            # Allow overriding for local dev snapshots.
            journal_mode = (os.getenv("DB_JOURNAL_MODE") or "WAL").strip().upper()
            if journal_mode not in {"WAL", "DELETE", "TRUNCATE", "PERSIST", "MEMORY", "OFF"}:
                journal_mode = "WAL"
            if journal_mode != "WAL" and self.path and not self.path.startswith((":memory:", "file:")):
                # Best-effort cleanup of leftover WAL artifacts from previous runs.
                for suffix in ("-wal", "-shm"):
                    try:
                        os.remove(self.path + suffix)
                    except FileNotFoundError:
                        pass
                    except Exception:
                        logging.debug("Failed to remove sqlite artifact %s", self.path + suffix)
            await conn.execute(f"PRAGMA journal_mode={journal_mode}")
            await conn.execute("PRAGMA synchronous=NORMAL")
            await conn.execute("PRAGMA foreign_keys=ON")
            await conn.execute("PRAGMA temp_store=MEMORY")
            await conn.execute("PRAGMA cache_size=-40000")
            await conn.execute(f"PRAGMA busy_timeout={self._sqlite_busy_timeout_ms()}")
            await conn.execute("PRAGMA mmap_size=134217728")
            await conn.execute(
                f"PRAGMA journal_size_limit={self._sqlite_journal_size_limit_bytes()}"
            )
            dbg(f"pragmas journal_mode={journal_mode}")

            pragma_cursor = await conn.execute("PRAGMA table_info('posterocrcache')")
            poster_ocr_columns = await pragma_cursor.fetchall()
            await pragma_cursor.close()
            dbg(f"posterocrcache columns={len(poster_ocr_columns)}")

            detail_exists = any(col[1] == "detail" for col in poster_ocr_columns)
            model_exists = any(col[1] == "model" for col in poster_ocr_columns)
            created_at_exists = any(col[1] == "created_at" for col in poster_ocr_columns)
            pk_columns: list[str] = []
            if poster_ocr_columns:
                pk_info = sorted(
                    ((col[5], col[1]) for col in poster_ocr_columns if col[5]),
                    key=lambda item: item[0],
                )
                pk_columns = [name for _, name in pk_info]

            expected_pk = ["hash", "detail", "model"]
            needs_posterocr_migration = False
            if poster_ocr_columns:
                if not detail_exists or not model_exists:
                    needs_posterocr_migration = True
                elif pk_columns != expected_pk:
                    needs_posterocr_migration = True

            if needs_posterocr_migration and not skip_posterocr_migration:
                await conn.execute("DROP TABLE IF EXISTS posterocrcache_new")
                await conn.execute(
                    """
                    CREATE TABLE posterocrcache_new(
                        hash TEXT NOT NULL,
                        detail TEXT NOT NULL,
                        model TEXT NOT NULL,
                        text TEXT NOT NULL,
                        title TEXT,
                        prompt_tokens INTEGER NOT NULL DEFAULT 0,
                        completion_tokens INTEGER NOT NULL DEFAULT 0,
                        total_tokens INTEGER NOT NULL DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (hash, detail, model)
                    )
                    """
                )

                detail_default = "auto"
                model_default = os.getenv("POSTER_OCR_MODEL", "gpt-4o-mini")

                detail_expr = "COALESCE(detail, ?)" if detail_exists else "?"
                model_expr = "COALESCE(model, ?)" if model_exists else "?"
                created_at_expr = "created_at" if created_at_exists else "CURRENT_TIMESTAMP"

                # Check if 'title' column exists in the old table to copy it
                title_exists = any(col[1] == "title" for col in poster_ocr_columns)
                title_expr = "title" if title_exists else "NULL"

                insert_sql = f"""
                    INSERT INTO posterocrcache_new (
                        hash, detail, model, text, title,
                        prompt_tokens, completion_tokens, total_tokens, created_at
                    )
                    SELECT
                        hash,
                        {detail_expr},
                        {model_expr},
                        text,
                        {title_expr},
                        prompt_tokens,
                        completion_tokens,
                        total_tokens,
                        {created_at_expr}
                    FROM posterocrcache
                """

                params: list[str] = []
                params.append(detail_default)
                params.append(model_default)

                await conn.execute(insert_sql, params)
                await conn.execute("DROP TABLE posterocrcache")
                await conn.execute("ALTER TABLE posterocrcache_new RENAME TO posterocrcache")

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS user(
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    is_superadmin BOOLEAN DEFAULT 0,
                    is_partner BOOLEAN DEFAULT 0,
                    organization TEXT,
                    location TEXT,
                    blocked BOOLEAN DEFAULT 0,
                    last_partner_reminder TIMESTAMP WITH TIME ZONE
                        -- Existing deployments should backfill naive values as UTC.
                )
                """
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pendinguser(
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    requested_at TIMESTAMP
                )
                """
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS rejecteduser(
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    rejected_at TIMESTAMP
                )
                """
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS channel(
                    channel_id INTEGER PRIMARY KEY,
                    title TEXT,
                    username TEXT,
                    is_admin BOOLEAN DEFAULT 0,
                    is_registered BOOLEAN DEFAULT 0,
                    is_asset BOOLEAN DEFAULT 0,
                    daily_time TEXT,
                    last_daily TEXT
                )
                """
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS setting(
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS supabase_delete_queue(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bucket TEXT NOT NULL,
                    path TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    last_attempt_at TIMESTAMP,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    UNIQUE(bucket, path)
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_supabase_delete_queue_created_at ON supabase_delete_queue(created_at)"
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS event(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    festival TEXT,
                    date TEXT NOT NULL,
                    time TEXT NOT NULL,
                    time_is_default BOOLEAN NOT NULL DEFAULT 0,
                    location_name TEXT NOT NULL,
                    location_address TEXT,
                    city TEXT,
                    ticket_price_min INTEGER,
                    ticket_price_max INTEGER,
                    ticket_link TEXT,
                    event_type TEXT,
                    emoji TEXT,
                    end_date TEXT,
                    end_date_is_inferred BOOLEAN NOT NULL DEFAULT 0,
                    duration_forecast_minutes INTEGER,
                    identity_status TEXT NOT NULL DEFAULT 'canonical',
                    merged_into_event_id INTEGER,
                    date_is_inferred BOOLEAN NOT NULL DEFAULT 0,
                    date_provenance TEXT,
                    date_confidence REAL,
                    end_date_provenance TEXT,
                    end_date_confidence REAL,
                    is_free BOOLEAN DEFAULT 0,
                    pushkin_card BOOLEAN DEFAULT 0,
                    silent BOOLEAN DEFAULT 0,
                    lifecycle_status TEXT NOT NULL DEFAULT 'active',
                    telegraph_path TEXT,
                    source_text TEXT NOT NULL,
                    source_texts JSON,
                    collection_decisions JSON,
                    organizer_names JSON,
                    telegraph_url TEXT,
                    ics_url TEXT,
                    source_post_url TEXT,
                    source_vk_post_url TEXT,
                    vk_repost_url TEXT,
                    tg_event_post_url TEXT,
                    tg_event_post_id INTEGER,
                    tg_event_post_mode TEXT,
                    tg_event_source_hash TEXT,
                    ics_hash TEXT,
                    ics_file_id TEXT,
                    ics_post_hash TEXT,
                    ics_updated_at TIMESTAMP,
                    ics_post_url TEXT,
                    ics_post_id INTEGER,
                    source_chat_id INTEGER,
                    source_message_id INTEGER,
                    creator_id INTEGER,
                    photo_urls JSON,
                    photo_count INTEGER DEFAULT 0,
                    video_include_count INTEGER NOT NULL DEFAULT 0,
                    topics TEXT DEFAULT '[]',
                    topics_manual BOOLEAN DEFAULT 0,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    content_hash TEXT,
                    age_restriction TEXT,
                    age_restriction_status TEXT NOT NULL DEFAULT 'unknown',
                    age_restriction_provenance TEXT,
                    age_restriction_source_url TEXT,
                    age_restriction_confidence REAL,
                    age_restriction_evidence JSON,
                    age_restriction_decision_version TEXT,
                    age_restriction_input_hash TEXT,
                    age_restriction_updated_at TIMESTAMP,
                    age_assessment TEXT,
                    age_assessment_status TEXT NOT NULL DEFAULT 'not_scheduled',
                    age_assessment_provenance TEXT,
                    age_assessment_confidence REAL,
                    age_assessment_evidence JSON,
                    age_assessment_decision_version TEXT,
                    age_assessment_input_hash TEXT,
                    age_assessment_engine TEXT,
                    age_assessment_run_id TEXT,
                    age_assessment_updated_at TIMESTAMP,
                    FOREIGN KEY(merged_into_event_id) REFERENCES event(id) ON DELETE SET NULL
                )
                """
            )
            dbg("event core columns")
            await _add_column(conn, "event", "photo_urls JSON")
            await _add_column(conn, "event", "source_texts JSON")
            await _add_column(conn, "event", "collection_decisions JSON")
            await _add_column(conn, "event", "organizer_names JSON")
            await _add_column(conn, "event", "tg_source_author TEXT")
            await _add_column(conn, "event", "ics_hash TEXT")
            await _add_column(conn, "event", "ics_file_id TEXT")
            await _add_column(conn, "event", "ics_post_hash TEXT")
            await _add_column(conn, "event", "ics_updated_at TIMESTAMP")
            await _add_column(conn, "event", "ics_post_url TEXT")
            await _add_column(conn, "event", "ics_post_id INTEGER")
            await _add_column(conn, "event", "vk_repost_url TEXT")
            await _add_column(conn, "event", "vk_source_hash TEXT")
            await _add_column(conn, "event", "tg_event_post_url TEXT")
            await _add_column(conn, "event", "tg_event_post_id INTEGER")
            await _add_column(conn, "event", "tg_event_post_mode TEXT")
            await _add_column(conn, "event", "tg_event_source_hash TEXT")
            await _add_column(conn, "event", "vk_ticket_short_url TEXT")
            await _add_column(conn, "event", "vk_ticket_short_key TEXT")
            await _add_column(conn, "event", "vk_ics_short_url TEXT")
            await _add_column(conn, "event", "vk_ics_short_key TEXT")
            await _add_column(conn, "event", "topics TEXT DEFAULT '[]'")
            await _add_column(conn, "event", "topics_manual BOOLEAN DEFAULT 0")
            await _add_column(conn, "event", "tourist_label SMALLINT")
            await _add_column(conn, "event", "tourist_factors TEXT")
            await _add_column(conn, "event", "tourist_note TEXT")
            await _add_column(conn, "event", "tourist_label_by INTEGER")
            await _add_column(conn, "event", "tourist_label_at TIMESTAMP")
            await _add_column(conn, "event", "tourist_label_source TEXT")
            await _add_column(
                conn, "event", "video_include_count INTEGER NOT NULL DEFAULT 0"
            )
            await _add_column(
                conn, "event", "lifecycle_status TEXT NOT NULL DEFAULT 'active'"
            )
            await _add_column(conn, "event", "short_description TEXT")
            await _add_column(conn, "event", "search_digest TEXT")
            await _add_column(conn, "event", "ticket_status TEXT")
            await _add_column(conn, "event", "ticket_trust_level TEXT")
            await _add_column(conn, "event", "linked_event_ids TEXT")
            await _add_column(conn, "event", "preview_3d_url TEXT")
            await _add_column(conn, "event", "time_is_default BOOLEAN NOT NULL DEFAULT 0")
            await _add_column(conn, "event", "end_date_is_inferred BOOLEAN NOT NULL DEFAULT 0")
            await _add_column(conn, "event", "duration_forecast_minutes INTEGER")
            await _add_column(
                conn, "event", "identity_status TEXT NOT NULL DEFAULT 'canonical'"
            )
            await _add_column(conn, "event", "merged_into_event_id INTEGER")
            await _add_column(conn, "event", "date_is_inferred BOOLEAN NOT NULL DEFAULT 0")
            await _add_column(conn, "event", "date_provenance TEXT")
            await _add_column(conn, "event", "date_confidence REAL")
            await _add_column(conn, "event", "end_date_provenance TEXT")
            await _add_column(conn, "event", "end_date_confidence REAL")
            await _add_column(conn, "event", "age_restriction TEXT")
            await _add_column(
                conn, "event", "age_restriction_status TEXT NOT NULL DEFAULT 'unknown'"
            )
            await _add_column(conn, "event", "age_restriction_provenance TEXT")
            await _add_column(conn, "event", "age_restriction_source_url TEXT")
            await _add_column(conn, "event", "age_restriction_confidence REAL")
            await _add_column(conn, "event", "age_restriction_evidence JSON")
            await _add_column(conn, "event", "age_restriction_decision_version TEXT")
            await _add_column(conn, "event", "age_restriction_input_hash TEXT")
            await _add_column(conn, "event", "age_restriction_updated_at TIMESTAMP")
            await _add_column(conn, "event", "age_assessment TEXT")
            await _add_column(
                conn,
                "event",
                "age_assessment_status TEXT NOT NULL DEFAULT 'not_scheduled'",
            )
            await _add_column(conn, "event", "age_assessment_provenance TEXT")
            await _add_column(conn, "event", "age_assessment_confidence REAL")
            await _add_column(conn, "event", "age_assessment_evidence JSON")
            await _add_column(conn, "event", "age_assessment_decision_version TEXT")
            await _add_column(conn, "event", "age_assessment_input_hash TEXT")
            await _add_column(conn, "event", "age_assessment_engine TEXT")
            await _add_column(conn, "event", "age_assessment_run_id TEXT")
            await _add_column(conn, "event", "age_assessment_updated_at TIMESTAMP")
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_event_tourist_label ON event(tourist_label)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_identity_status ON event(identity_status)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_merged_into_event ON event(merged_into_event_id)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_date_inferred ON event(date_is_inferred, date)"
            )
            dbg("eventposter")

            eventposter_columns_before = await (
                await conn.execute("PRAGMA table_info('eventposter')")
            ).fetchall()
            eventposter_had_review_status = any(
                str(col[1]) == "review_status" for col in eventposter_columns_before
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS eventposter(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER NOT NULL,
                    catbox_url TEXT,
                    supabase_url TEXT,
                    supabase_path TEXT,
                    poster_hash TEXT NOT NULL,
                    phash TEXT,
                    raw_sha256 TEXT,
                    pixel_sha256 TEXT,
                    perceptual_hash TEXT,
                    width INTEGER,
                    height INTEGER,
                    mime_type TEXT,
                    review_status TEXT NOT NULL DEFAULT 'pending_review',
                    duplicate_of_id INTEGER,
                    review_reason TEXT,
                    reviewed_at TIMESTAMP,
                    display_order INTEGER NOT NULL DEFAULT 0,
                    ocr_text TEXT,
                    ocr_title TEXT,
                    image_text_mode TEXT,
                    media_role TEXT,
                    media_role_confidence REAL,
                    media_semantic_status TEXT NOT NULL DEFAULT 'pending',
                    media_semantic_reason_code TEXT,
                    media_semantic_evidence_json JSON,
                    media_semantic_model TEXT,
                    media_semantic_prompt_version TEXT,
                    media_semantic_context_hash TEXT,
                    media_semantic_classified_at TIMESTAMP,
                    focal_x REAL,
                    focal_y REAL,
                    safe_crop BOOLEAN,
                    image_geometry_id INTEGER,
                    thumbnail_256_url TEXT,
                    thumbnail_256_path TEXT,
                    thumbnail_256_width INTEGER,
                    thumbnail_256_height INTEGER,
                    thumbnail_512_url TEXT,
                    thumbnail_512_path TEXT,
                    thumbnail_512_width INTEGER,
                    thumbnail_512_height INTEGER,
                    prompt_tokens INTEGER NOT NULL DEFAULT 0,
                    completion_tokens INTEGER NOT NULL DEFAULT 0,
                    total_tokens INTEGER NOT NULL DEFAULT 0,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(event_id) REFERENCES event(id) ON DELETE CASCADE,
                    FOREIGN KEY(duplicate_of_id) REFERENCES eventposter(id) ON DELETE SET NULL,
                    FOREIGN KEY(image_geometry_id) REFERENCES event_image_geometry(id) ON DELETE SET NULL,
                    UNIQUE(event_id, poster_hash)
                )
                """
            )
            await _add_column(conn, "eventposter", "ocr_title TEXT")
            await _add_column(conn, "eventposter", "phash TEXT")
            await _add_column(conn, "eventposter", "supabase_url TEXT")
            await _add_column(conn, "eventposter", "supabase_path TEXT")
            await _add_column(conn, "eventposter", "raw_sha256 TEXT")
            await _add_column(conn, "eventposter", "pixel_sha256 TEXT")
            await _add_column(conn, "eventposter", "perceptual_hash TEXT")
            await _add_column(conn, "eventposter", "width INTEGER")
            await _add_column(conn, "eventposter", "height INTEGER")
            await _add_column(conn, "eventposter", "mime_type TEXT")
            await _add_column(
                conn,
                "eventposter",
                "review_status TEXT NOT NULL DEFAULT 'pending_review'",
            )
            await _add_column(conn, "eventposter", "duplicate_of_id INTEGER")
            await _add_column(conn, "eventposter", "review_reason TEXT")
            await _add_column(conn, "eventposter", "reviewed_at TIMESTAMP")
            await _add_column(
                conn,
                "eventposter",
                "display_order INTEGER NOT NULL DEFAULT 0",
            )
            await _add_column(conn, "eventposter", "image_text_mode TEXT")
            await _add_column(conn, "eventposter", "media_role TEXT")
            await _add_column(conn, "eventposter", "media_role_confidence REAL")
            await _add_column(conn, "eventposter", "media_semantic_status TEXT NOT NULL DEFAULT 'pending'")
            await _add_column(conn, "eventposter", "media_semantic_reason_code TEXT")
            await _add_column(conn, "eventposter", "media_semantic_evidence_json JSON")
            await _add_column(conn, "eventposter", "media_semantic_model TEXT")
            await _add_column(conn, "eventposter", "media_semantic_prompt_version TEXT")
            await _add_column(conn, "eventposter", "media_semantic_context_hash TEXT")
            await _add_column(conn, "eventposter", "media_semantic_classified_at TIMESTAMP")
            await _add_column(conn, "eventposter", "focal_x REAL")
            await _add_column(conn, "eventposter", "focal_y REAL")
            await _add_column(conn, "eventposter", "safe_crop BOOLEAN")
            await _add_column(conn, "eventposter", "image_geometry_id INTEGER")
            await _add_column(conn, "eventposter", "thumbnail_256_url TEXT")
            await _add_column(conn, "eventposter", "thumbnail_256_path TEXT")
            await _add_column(conn, "eventposter", "thumbnail_256_width INTEGER")
            await _add_column(conn, "eventposter", "thumbnail_256_height INTEGER")
            await _add_column(conn, "eventposter", "thumbnail_512_url TEXT")
            await _add_column(conn, "eventposter", "thumbnail_512_path TEXT")
            await _add_column(conn, "eventposter", "thumbnail_512_width INTEGER")
            await _add_column(conn, "eventposter", "thumbnail_512_height INTEGER")
            if not eventposter_had_review_status:
                await conn.execute(
                    "UPDATE eventposter SET review_status='approved', reviewed_at=COALESCE(reviewed_at, updated_at)"
                )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_eventposter_event ON eventposter(event_id)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_eventposter_phash ON eventposter(phash)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_eventposter_review_status ON eventposter(event_id, review_status)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_eventposter_raw_sha256 ON eventposter(event_id, raw_sha256)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_eventposter_pixel_sha256 ON eventposter(event_id, pixel_sha256)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_eventposter_pixel_sha256_global ON eventposter(pixel_sha256)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_eventposter_image_geometry ON eventposter(image_geometry_id)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_eventposter_media_semantic ON eventposter(event_id, media_semantic_status, media_role)"
            )
            await conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_eventposter_event_raw_sha256 ON eventposter(event_id, raw_sha256) WHERE raw_sha256 IS NOT NULL AND TRIM(raw_sha256) != ''"
            )

            dbg("event_image_geometry")
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS event_image_geometry(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pixel_sha256 TEXT NOT NULL,
                    model TEXT NOT NULL,
                    prompt_version TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'classified',
                    source_width INTEGER,
                    source_height INTEGER,
                    face_boxes_yxyx_json JSON,
                    valuable_region_yxyx_json JSON,
                    valuable_region_confidence REAL,
                    reason_code TEXT,
                    prompt_tokens INTEGER NOT NULL DEFAULT 0,
                    completion_tokens INTEGER NOT NULL DEFAULT 0,
                    total_tokens INTEGER NOT NULL DEFAULT 0,
                    analyzed_at TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(pixel_sha256, model, prompt_version)
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_image_geometry_status ON event_image_geometry(status, updated_at)"
            )

            dbg("event_media_review")
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS event_media_pair_review(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER NOT NULL,
                    left_poster_id INTEGER NOT NULL,
                    right_poster_id INTEGER NOT NULL,
                    context_hash TEXT NOT NULL,
                    pair_input_hash TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL DEFAULT 'pending',
                    decision TEXT,
                    duplicate_kind TEXT,
                    confidence REAL,
                    semantic_conflict INTEGER NOT NULL DEFAULT 0,
                    canonical_poster_id INTEGER,
                    reason_code TEXT,
                    primary_model TEXT,
                    escalation_model TEXT,
                    response_json JSON,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    provider_calls INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    next_run_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    resolved_at TIMESTAMP,
                    FOREIGN KEY(event_id) REFERENCES event(id) ON DELETE CASCADE,
                    FOREIGN KEY(left_poster_id) REFERENCES eventposter(id) ON DELETE CASCADE,
                    FOREIGN KEY(right_poster_id) REFERENCES eventposter(id) ON DELETE CASCADE,
                    FOREIGN KEY(canonical_poster_id) REFERENCES eventposter(id) ON DELETE SET NULL
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_media_pair_review_event_status ON event_media_pair_review(event_id, status, next_run_at)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_media_pair_review_left ON event_media_pair_review(left_poster_id)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_media_pair_review_right ON event_media_pair_review(right_poster_id)"
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS event_media_review_usage(
                    day TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    calls INTEGER NOT NULL DEFAULT 0,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(day, stage)
                )
                """
            )

            dbg("event_media_asset")
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS event_media_asset(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER NOT NULL,
                    kind TEXT NOT NULL DEFAULT 'video',
                    supabase_url TEXT,
                    supabase_path TEXT,
                    sha256 TEXT,
                    size_bytes INTEGER,
                    mime_type TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(event_id) REFERENCES event(id) ON DELETE CASCADE
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_media_asset_event ON event_media_asset(event_id)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_media_asset_kind ON event_media_asset(kind)"
            )

            dbg("video_asset")
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS video_asset(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sha256 TEXT NOT NULL UNIQUE,
                    analysis_status TEXT NOT NULL DEFAULT 'accepted',
                    cdn_url TEXT,
                    cdn_path TEXT,
                    cdn_bucket TEXT,
                    size_bytes INTEGER,
                    mime_type TEXT,
                    width INTEGER,
                    height INTEGER,
                    duration_seconds REAL,
                    aesthetic_score REAL,
                    technical_score REAL,
                    showcase_score REAL,
                    description TEXT,
                    search_text TEXT,
                    analysis_model TEXT,
                    analysis_version TEXT,
                    analysis_json JSON NOT NULL DEFAULT '{}',
                    analyzed_at TIMESTAMP,
                    orphaned_at TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    CHECK(analysis_status IN ('accepted', 'rejected', 'error', 'pending'))
                )
                """
            )
            await _add_column(conn, "video_asset", "orphaned_at TIMESTAMP")
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_video_asset_status_showcase "
                "ON video_asset(analysis_status, showcase_score)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_video_asset_cdn_path "
                "ON video_asset(cdn_bucket, cdn_path)"
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS event_video_link(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER NOT NULL,
                    video_asset_id INTEGER NOT NULL,
                    event_relevance_score REAL,
                    ranking_score REAL,
                    match_reason TEXT,
                    relation_confidence REAL,
                    source_url TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(event_id, video_asset_id),
                    FOREIGN KEY(event_id) REFERENCES event(id) ON DELETE CASCADE,
                    FOREIGN KEY(video_asset_id) REFERENCES video_asset(id) ON DELETE CASCADE
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_video_link_event_rank "
                "ON event_video_link(event_id, ranking_score)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_video_link_asset "
                "ON event_video_link(video_asset_id)"
            )

            dbg("poll_repost_run")
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS poll_repost_run(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_key TEXT NOT NULL,
                    run_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    target_event_date TEXT NOT NULL,
                    poll_chat_id TEXT,
                    poll_message_id INTEGER,
                    poll_id TEXT,
                    question_text TEXT,
                    options_json TEXT NOT NULL DEFAULT '[]',
                    result_json TEXT NOT NULL DEFAULT '{}',
                    winner_option_id TEXT,
                    winner_text TEXT,
                    chosen_event_id INTEGER,
                    kldevents_chat_id TEXT,
                    kldevents_message_id INTEGER,
                    kldevents_post_url TEXT,
                    reply_message_id INTEGER,
                    forwarded_message_id INTEGER,
                    resolve_after TIMESTAMP,
                    error_json TEXT NOT NULL DEFAULT '{}',
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(profile_key, run_key),
                    FOREIGN KEY(chosen_event_id) REFERENCES event(id)
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_poll_repost_run_status_resolve ON poll_repost_run(profile_key, status, resolve_after)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_poll_repost_run_event_updated ON poll_repost_run(chosen_event_id, updated_at)"
            )

            dbg("event_source")
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS smart_update_candidate_state(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    candidate_key TEXT NOT NULL UNIQUE,
                    occurrence_key TEXT NOT NULL,
                    canonical_source_url TEXT,
                    source_type TEXT NOT NULL,
                    intent TEXT NOT NULL CHECK(intent IN ('UPSERT_EVENT','ATTACH_CONTEXT')),
                    source_fingerprint TEXT NOT NULL,
                    candidate_payload JSON NOT NULL DEFAULT '{}',
                    current_outcome TEXT NOT NULL DEFAULT 'RETRY_SCHEDULED'
                        CHECK(current_outcome IN ('CREATED','MERGED','NOOP_EXACT_REPLAY','REJECTED_PRODUCT_POLICY','RETRY_SCHEDULED')),
                    accepted_event_id INTEGER,
                    diagnostic_event_id INTEGER,
                    reason TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    retry_attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 3,
                    retry_exhausted INTEGER NOT NULL DEFAULT 0,
                    next_retry_at TIMESTAMP,
                    claimed_by TEXT,
                    claim_expires_at TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP,
                    FOREIGN KEY(accepted_event_id) REFERENCES event(id) ON DELETE SET NULL,
                    FOREIGN KEY(diagnostic_event_id) REFERENCES event(id) ON DELETE SET NULL
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_smart_update_candidate_due "
                "ON smart_update_candidate_state(current_outcome, next_retry_at, claim_expires_at)"
            )
            await _add_column(
                conn,
                "smart_update_candidate_state",
                "retry_attempts INTEGER NOT NULL DEFAULT 0",
            )
            await conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_smart_update_candidate_source_occurrence "
                "ON smart_update_candidate_state(canonical_source_url, occurrence_key) "
                "WHERE canonical_source_url IS NOT NULL AND canonical_source_url<>''"
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS smart_update_attempt(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    candidate_state_id INTEGER NOT NULL,
                    attempt_no INTEGER NOT NULL,
                    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    finished_at TIMESTAMP,
                    terminal_outcome TEXT NOT NULL DEFAULT 'RETRY_SCHEDULED'
                        CHECK(terminal_outcome IN ('CREATED','MERGED','NOOP_EXACT_REPLAY','REJECTED_PRODUCT_POLICY','RETRY_SCHEDULED')),
                    accepted_event_id INTEGER,
                    diagnostic_event_id INTEGER,
                    reason TEXT,
                    UNIQUE(candidate_state_id, attempt_no),
                    FOREIGN KEY(candidate_state_id) REFERENCES smart_update_candidate_state(id) ON DELETE CASCADE,
                    FOREIGN KEY(accepted_event_id) REFERENCES event(id) ON DELETE SET NULL,
                    FOREIGN KEY(diagnostic_event_id) REFERENCES event(id) ON DELETE SET NULL
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_smart_update_attempt_terminal "
                "ON smart_update_attempt(terminal_outcome, finished_at)"
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS source_parser_recovery_request(
                    source_type TEXT PRIMARY KEY,
                    requested_since TIMESTAMP NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending'
                        CHECK(status IN ('pending','running','done','error')),
                    attempts INTEGER NOT NULL DEFAULT 0,
                    next_run_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    last_error TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_source_parser_recovery_due "
                "ON source_parser_recovery_request(status,next_run_at)"
            )
            accepted_terminals_sql = "'CREATED','MERGED','NOOP_EXACT_REPLAY'"
            await conn.execute(
                "CREATE TRIGGER IF NOT EXISTS trg_smart_update_candidate_terminal_insert "
                "BEFORE INSERT ON smart_update_candidate_state FOR EACH ROW WHEN "
                f"(NEW.current_outcome IN ({accepted_terminals_sql}) AND NEW.accepted_event_id IS NULL) OR "
                f"(NEW.current_outcome NOT IN ({accepted_terminals_sql}) AND NEW.accepted_event_id IS NOT NULL) "
                "BEGIN SELECT RAISE(ABORT,'smart_update_candidate_terminal_contract'); END"
            )
            await conn.execute(
                "CREATE TRIGGER IF NOT EXISTS trg_smart_update_candidate_terminal_update "
                "BEFORE UPDATE OF current_outcome,accepted_event_id ON smart_update_candidate_state "
                "FOR EACH ROW WHEN "
                f"(NEW.current_outcome IN ({accepted_terminals_sql}) AND NEW.accepted_event_id IS NULL) OR "
                f"(NEW.current_outcome NOT IN ({accepted_terminals_sql}) AND NEW.accepted_event_id IS NOT NULL) "
                "BEGIN SELECT RAISE(ABORT,'smart_update_candidate_terminal_contract'); END"
            )
            await conn.execute(
                "CREATE TRIGGER IF NOT EXISTS trg_smart_update_attempt_terminal_insert "
                "BEFORE INSERT ON smart_update_attempt FOR EACH ROW WHEN "
                f"(NEW.terminal_outcome IN ({accepted_terminals_sql}) AND NEW.accepted_event_id IS NULL) OR "
                f"(NEW.terminal_outcome NOT IN ({accepted_terminals_sql}) AND NEW.accepted_event_id IS NOT NULL) "
                "BEGIN SELECT RAISE(ABORT,'smart_update_attempt_terminal_contract'); END"
            )
            await conn.execute(
                "CREATE TRIGGER IF NOT EXISTS trg_smart_update_attempt_terminal_update "
                "BEFORE UPDATE OF terminal_outcome,accepted_event_id ON smart_update_attempt "
                "FOR EACH ROW WHEN "
                f"(NEW.terminal_outcome IN ({accepted_terminals_sql}) AND NEW.accepted_event_id IS NULL) OR "
                f"(NEW.terminal_outcome NOT IN ({accepted_terminals_sql}) AND NEW.accepted_event_id IS NOT NULL) "
                "BEGIN SELECT RAISE(ABORT,'smart_update_attempt_terminal_contract'); END"
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS event_source(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER NOT NULL,
                    source_type TEXT NOT NULL,
                    source_url TEXT NOT NULL,
                    canonical_source_url TEXT,
                    source_role TEXT,
                    source_fingerprint TEXT,
                    candidate_key TEXT,
                    occurrence_key TEXT,
                    smart_update_candidate_id INTEGER,
                    source_chat_username TEXT,
                    source_chat_id INTEGER,
                    source_message_id INTEGER,
                    source_text TEXT,
                    imported_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    trust_level TEXT,
                    FOREIGN KEY(event_id) REFERENCES event(id) ON DELETE CASCADE,
                    FOREIGN KEY(smart_update_candidate_id) REFERENCES smart_update_candidate_state(id) ON DELETE SET NULL,
                    UNIQUE(event_id, source_url)
                )
                """
            )
            await _add_column(conn, "event_source", "source_text TEXT")
            # These columns are intentionally nullable. Classifying all legacy
            # rows as identity sources would turn historical context/program
            # links into unsafe merge anchors.
            await _add_column(conn, "event_source", "canonical_source_url TEXT")
            await _add_column(conn, "event_source", "source_role TEXT")
            await _add_column(conn, "event_source", "source_fingerprint TEXT")
            await _add_column(conn, "event_source", "candidate_key TEXT")
            await _add_column(conn, "event_source", "occurrence_key TEXT")
            await _add_column(conn, "event_source", "smart_update_candidate_id INTEGER")
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_source_event ON event_source(event_id)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_source_type_url ON event_source(source_type, source_url)"
            )
            # Smart Update часто проверяет идемпотентность по `source_url` без знания `event_id`.
            # Индексы (event_id, source_url) и (source_type, source_url) не ускоряют такой lookup,
            # поэтому держим отдельный индекс по source_url.
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_source_url ON event_source(source_url)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_source_canonical_role "
                "ON event_source(canonical_source_url, source_role)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_source_fingerprint "
                "ON event_source(source_fingerprint)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_source_candidate "
                "ON event_source(candidate_key)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_source_occurrence "
                "ON event_source(canonical_source_url, occurrence_key)"
            )
            # Classified source rows must be complete and role-valid. Legacy
            # rows may remain NULL until an evidence-backed intake/repair
            # classifies them; arbitrary role strings or blank canonical
            # identities may not bypass the partial unique indexes.
            await conn.execute(
                "CREATE TRIGGER IF NOT EXISTS trg_event_source_identity_insert "
                "BEFORE INSERT ON event_source FOR EACH ROW WHEN "
                "(NEW.source_role IS NOT NULL AND NEW.source_role NOT IN ('identity_bearing','context_only')) "
                "OR (NEW.source_role IN ('identity_bearing','context_only') "
                "AND TRIM(COALESCE(NEW.canonical_source_url,''))='') "
                "BEGIN SELECT RAISE(ABORT,'event_source_identity_contract'); END"
            )
            await conn.execute(
                "CREATE TRIGGER IF NOT EXISTS trg_event_source_identity_update "
                "BEFORE UPDATE OF event_id,canonical_source_url,source_role ON event_source "
                "FOR EACH ROW WHEN "
                "(NEW.source_role IS NOT NULL AND NEW.source_role NOT IN ('identity_bearing','context_only')) "
                "OR (NEW.source_role IN ('identity_bearing','context_only') "
                "AND TRIM(COALESCE(NEW.canonical_source_url,''))='') "
                "BEGIN SELECT RAISE(ABORT,'event_source_identity_contract'); END"
            )
            await conn.execute(
                "CREATE TRIGGER IF NOT EXISTS trg_event_source_candidate_insert "
                "BEFORE INSERT ON event_source FOR EACH ROW WHEN "
                "((NEW.candidate_key IS NULL) != (NEW.occurrence_key IS NULL)) OR "
                "(NEW.smart_update_candidate_id IS NOT NULL AND "
                "(TRIM(COALESCE(NEW.candidate_key,''))='' OR TRIM(COALESCE(NEW.occurrence_key,''))='')) "
                "BEGIN SELECT RAISE(ABORT,'event_source_candidate_contract'); END"
            )
            await conn.execute(
                "CREATE TRIGGER IF NOT EXISTS trg_event_source_candidate_update "
                "BEFORE UPDATE OF candidate_key,occurrence_key,smart_update_candidate_id ON event_source "
                "FOR EACH ROW WHEN "
                "((NEW.candidate_key IS NULL) != (NEW.occurrence_key IS NULL)) OR "
                "(NEW.smart_update_candidate_id IS NOT NULL AND "
                "(TRIM(COALESCE(NEW.candidate_key,''))='' OR TRIM(COALESCE(NEW.occurrence_key,''))='')) "
                "BEGIN SELECT RAISE(ABORT,'event_source_candidate_contract'); END"
            )
            # A carrier URL may contain several independent occurrences.  The
            # previous global URL indexes encoded the false premise "one URL =
            # one Event"; replace them without rewriting/classifying legacy rows.
            await conn.execute("DROP INDEX IF EXISTS ux_event_source_event_canonical")
            await conn.execute("DROP INDEX IF EXISTS ux_event_source_identity_canonical")
            occurrence_conflict_cursor = await conn.execute(
                "SELECT canonical_source_url, occurrence_key FROM event_source "
                "WHERE source_role='identity_bearing' AND canonical_source_url IS NOT NULL "
                "AND canonical_source_url<>'' AND occurrence_key IS NOT NULL AND occurrence_key<>'' "
                "GROUP BY canonical_source_url, occurrence_key HAVING COUNT(*) > 1 LIMIT 1"
            )
            occurrence_conflict = await occurrence_conflict_cursor.fetchone()
            await occurrence_conflict_cursor.close()
            if occurrence_conflict is None:
                await conn.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS ux_event_source_identity_occurrence "
                    "ON event_source(canonical_source_url, occurrence_key) "
                    "WHERE source_role='identity_bearing' "
                    "AND canonical_source_url IS NOT NULL AND canonical_source_url<>'' "
                    "AND occurrence_key IS NOT NULL AND occurrence_key<>''"
                )
            else:
                raise RuntimeError(
                    "event_source_identity_occurrence_conflict:"
                    + hashlib.sha256(str(occurrence_conflict[0]).encode("utf-8")).hexdigest()[:12]
                    + ":"
                    + hashlib.sha256(str(occurrence_conflict[1]).encode("utf-8")).hexdigest()[:12]
                )
            legacy_conflict_cursor = await conn.execute(
                "SELECT canonical_source_url FROM event_source "
                "WHERE source_role='identity_bearing' AND canonical_source_url IS NOT NULL "
                "AND canonical_source_url<>'' AND occurrence_key IS NULL "
                "GROUP BY canonical_source_url HAVING COUNT(*) > 1 LIMIT 1"
            )
            legacy_conflict = await legacy_conflict_cursor.fetchone()
            await legacy_conflict_cursor.close()
            if legacy_conflict is not None:
                raise RuntimeError(
                    "event_source_legacy_identity_conflict:"
                    + hashlib.sha256(str(legacy_conflict[0]).encode("utf-8")).hexdigest()[:12]
                )
            await conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_event_source_identity_canonical_legacy "
                "ON event_source(canonical_source_url) "
                "WHERE source_role='identity_bearing' "
                "AND canonical_source_url IS NOT NULL AND canonical_source_url<>'' "
                "AND occurrence_key IS NULL"
            )
            await conn.execute("DROP INDEX IF EXISTS ux_event_source_smart_candidate")
            await conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_event_source_smart_candidate "
                "ON event_source(canonical_source_url,candidate_key) "
                "WHERE canonical_source_url IS NOT NULL AND canonical_source_url<>'' "
                "AND candidate_key IS NOT NULL AND candidate_key<>''"
            )

            dbg("event_identity")
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS event_identity_decision_log(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER,
                    candidate_event_id INTEGER,
                    source_id INTEGER,
                    source_type TEXT,
                    source_url TEXT,
                    decision TEXT NOT NULL,
                    decision_reason TEXT,
                    confidence REAL,
                    decided_by TEXT,
                    decision_payload JSON,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(event_id) REFERENCES event(id) ON DELETE SET NULL,
                    FOREIGN KEY(candidate_event_id) REFERENCES event(id) ON DELETE SET NULL,
                    FOREIGN KEY(source_id) REFERENCES event_source(id) ON DELETE SET NULL
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_identity_decision_log_event ON event_identity_decision_log(event_id)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_identity_decision_log_candidate ON event_identity_decision_log(candidate_event_id)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_identity_decision_log_source ON event_identity_decision_log(source_type, source_url)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_identity_decision_log_created ON event_identity_decision_log(created_at)"
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS event_identity_lock(
                    event_id INTEGER PRIMARY KEY,
                    lock_status TEXT NOT NULL DEFAULT 'active',
                    lock_reason TEXT,
                    locked_by TEXT,
                    locked_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP,
                    details JSON,
                    FOREIGN KEY(event_id) REFERENCES event(id) ON DELETE CASCADE
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_identity_lock_status ON event_identity_lock(lock_status, expires_at)"
            )

            dbg("event_source_fact")
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS event_source_fact(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER NOT NULL,
                    source_id INTEGER NOT NULL,
                    fact TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'added',
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(event_id) REFERENCES event(id) ON DELETE CASCADE,
                    FOREIGN KEY(source_id) REFERENCES event_source(id) ON DELETE CASCADE
                )
                """
            )
            # Schema evolution (older snapshots may lack the status column).
            await _add_column(conn, "event_source_fact", "status TEXT NOT NULL DEFAULT 'added'")
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_source_fact_event ON event_source_fact(event_id)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_source_fact_source ON event_source_fact(source_id)"
            )

            # Backfill `event_source` for legacy events created before Smart Update started
            # recording sources (idempotent). This improves Smart Update idempotency and reduces
            # duplicate event creation when the same post is reprocessed.
            skip_event_source_backfill = (
                (os.getenv("DB_INIT_SKIP_EVENT_SOURCE_BACKFILL") or "").strip().lower()
                in {"1", "true", "yes"}
            )
            if not skip_event_source_backfill:
                dbg("seed event_source backfill")
                try:
                    managed_vk_globs = _managed_vk_publication_globs()
                    source_post_filter, source_post_args = _exclude_managed_vk_sql(
                        "e.source_post_url", managed_vk_globs
                    )
                    source_vk_filter, source_vk_args = _exclude_managed_vk_sql(
                        "e.source_vk_post_url", managed_vk_globs
                    )
                    await conn.execute(
                        f"""
                        INSERT OR IGNORE INTO event_source(
                            event_id,
                            source_type,
                            source_url,
                            source_chat_id,
                            source_message_id
                        )
                        SELECT
                            e.id,
                            CASE
                                WHEN e.source_post_url LIKE '%t.me/%'
                                  OR e.source_post_url LIKE '%telegram.me/%' THEN 'telegram'
                                WHEN e.source_post_url LIKE '%vk.com/%' THEN 'vk'
                                ELSE 'legacy'
                            END,
                            e.source_post_url,
                            e.source_chat_id,
                            e.source_message_id
                        FROM event e
                        WHERE e.source_post_url IS NOT NULL AND TRIM(e.source_post_url) != ''
                        {source_post_filter}
                        """,
                        source_post_args,
                    )
                    await conn.execute(
                        f"""
                        INSERT OR IGNORE INTO event_source(
                            event_id,
                            source_type,
                            source_url,
                            source_chat_id,
                            source_message_id
                        )
                        SELECT
                            e.id,
                            'vk',
                            e.source_vk_post_url,
                            e.source_chat_id,
                            e.source_message_id
                        FROM event e
                        WHERE e.source_vk_post_url IS NOT NULL AND TRIM(e.source_vk_post_url) != ''
                        {source_vk_filter}
                        """,
                        source_vk_args,
                    )
                    if managed_vk_globs:
                        predicates = " OR ".join(
                            "LOWER(source_url) GLOB ?" for _ in managed_vk_globs
                        )
                        await conn.execute(
                            f"DELETE FROM event_source WHERE {predicates}",
                            managed_vk_globs,
                        )
                except Exception:
                    logging.warning(
                        "db.init: event_source backfill failed (non-fatal)",
                        exc_info=True,
                    )

            dbg("telegram_source")
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS telegram_source(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL UNIQUE,
                    title TEXT,
                    enabled BOOLEAN NOT NULL DEFAULT 1,
                    default_location TEXT,
                    default_ticket_link TEXT,
                    trust_level TEXT,
                    filters_json TEXT,
                    festival_source BOOLEAN DEFAULT 0,
                    festival_series TEXT,
                    about TEXT,
                    about_links_json JSON,
                    meta_hash TEXT,
                    meta_fetched_at TIMESTAMP,
                    suggested_festival_series TEXT,
                    suggested_website_url TEXT,
                    suggestion_confidence REAL,
                    suggestion_rationale TEXT,
                    last_scanned_message_id INTEGER,
                    last_scan_at TIMESTAMP
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_telegram_source_enabled ON telegram_source(enabled)"
            )
            await _add_column(conn, "telegram_source", "festival_source BOOLEAN DEFAULT 0")
            await _add_column(conn, "telegram_source", "festival_series TEXT")
            await _add_column(conn, "telegram_source", "filters_json TEXT")
            await _add_column(conn, "telegram_source", "title TEXT")
            await _add_column(conn, "telegram_source", "about TEXT")
            await _add_column(conn, "telegram_source", "about_links_json JSON")
            await _add_column(conn, "telegram_source", "meta_hash TEXT")
            await _add_column(conn, "telegram_source", "meta_fetched_at TIMESTAMP")
            await _add_column(conn, "telegram_source", "suggested_festival_series TEXT")
            await _add_column(conn, "telegram_source", "suggested_website_url TEXT")
            await _add_column(conn, "telegram_source", "suggestion_confidence REAL")
            await _add_column(conn, "telegram_source", "suggestion_rationale TEXT")

            dbg("telegram_scanned_message")
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS telegram_scanned_message(
                    source_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    message_date TIMESTAMP,
                    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    status TEXT NOT NULL,
                    events_extracted INTEGER NOT NULL DEFAULT 0,
                    events_imported INTEGER NOT NULL DEFAULT 0,
                    error TEXT,
                    PRIMARY KEY (source_id, message_id),
                    FOREIGN KEY(source_id) REFERENCES telegram_source(id) ON DELETE CASCADE
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_tg_scanned_source ON telegram_scanned_message(source_id)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_tg_scanned_processed_at ON telegram_scanned_message(processed_at)"
            )

            dbg("telegram_source_force_message")
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS telegram_source_force_message(
                    source_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (source_id, message_id),
                    FOREIGN KEY(source_id) REFERENCES telegram_source(id) ON DELETE CASCADE
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_tg_force_source ON telegram_source_force_message(source_id)"
            )

            dbg("telegram_monitoring_on_demand_queue")
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS telegram_monitoring_on_demand_queue(
                    source_username TEXT PRIMARY KEY,
                    source_id INTEGER NOT NULL,
                    chat_id INTEGER,
                    latest_message_id INTEGER,
                    latest_message_date TIMESTAMP,
                    first_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    next_run_at TIMESTAMP NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'pending',
                    last_run_at TIMESTAMP,
                    last_error TEXT,
                    FOREIGN KEY(source_id) REFERENCES telegram_source(id) ON DELETE CASCADE
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_tg_on_demand_status_next_run ON telegram_monitoring_on_demand_queue(status, next_run_at)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_tg_on_demand_source ON telegram_monitoring_on_demand_queue(source_id)"
            )

            dbg("telegram_post_metric")
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS telegram_post_metric(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    age_day INTEGER NOT NULL,
                    source_url TEXT,
                    message_ts INTEGER,
                    collected_ts INTEGER NOT NULL,
                    views INTEGER,
                    likes INTEGER,
                    comments INTEGER,
                    forwards INTEGER,
                    reactions_json JSON,
                    UNIQUE(source_id, message_id, age_day),
                    FOREIGN KEY(source_id) REFERENCES telegram_source(id) ON DELETE CASCADE
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_tg_metric_source_age ON telegram_post_metric(source_id, age_day)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_tg_metric_source_message ON telegram_post_metric(source_id, message_id)"
            )
            await _add_column(conn, "telegram_post_metric", "comments INTEGER")
            await _add_column(conn, "telegram_post_metric", "forwards INTEGER")
            await _add_column(conn, "telegram_post_metric", "reactions_json JSON")

            dbg("social_metric_snapshot")
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS social_metric_snapshot(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform TEXT NOT NULL,
                    publisher_id TEXT NOT NULL,
                    post_id INTEGER NOT NULL,
                    age_bucket TEXT NOT NULL,
                    publication_kind TEXT NOT NULL DEFAULT 'external_event_source',
                    source_url TEXT,
                    post_ts INTEGER,
                    collected_ts INTEGER NOT NULL,
                    views INTEGER,
                    likes INTEGER,
                    comments INTEGER,
                    shares INTEGER,
                    reactions_json JSON,
                    status TEXT NOT NULL DEFAULT 'collected',
                    error_code TEXT,
                    UNIQUE(platform, publisher_id, post_id, age_bucket)
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_social_metric_due ON social_metric_snapshot(platform, publisher_id, post_id, age_bucket, status)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_social_metric_url ON social_metric_snapshot(source_url, collected_ts)"
            )
            await _add_column(
                conn,
                "social_metric_snapshot",
                "publication_kind TEXT NOT NULL DEFAULT 'external_event_source'",
            )

            dbg("guide_profile")
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS guide_profile(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    slug TEXT NOT NULL UNIQUE,
                    profile_kind TEXT NOT NULL,
                    display_name TEXT NOT NULL,
                    marketing_name TEXT,
                    source_links_json JSON,
                    base_region TEXT,
                    audience_strengths_json JSON,
                    summary_short TEXT,
                    facts_rollup_json JSON,
                    first_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    last_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_guide_profile_kind ON guide_profile(profile_kind)"
            )

            dbg("guide_source")
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS guide_source(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform TEXT NOT NULL DEFAULT 'telegram',
                    username TEXT NOT NULL,
                    title TEXT,
                    about_text TEXT,
                    about_links_json JSON,
                    primary_profile_id INTEGER,
                    source_kind TEXT NOT NULL,
                    trust_level TEXT,
                    priority_weight REAL NOT NULL DEFAULT 1.0,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    flags_json JSON,
                    base_region TEXT,
                    added_via TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    last_scanned_message_id INTEGER,
                    last_scan_at TIMESTAMP,
                    UNIQUE(platform, username),
                    FOREIGN KEY(primary_profile_id) REFERENCES guide_profile(id) ON DELETE SET NULL
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_guide_source_enabled ON guide_source(enabled)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_guide_source_kind ON guide_source(source_kind)"
            )
            await _add_column(conn, "guide_source", "about_text TEXT")
            await _add_column(conn, "guide_source", "about_links_json JSON")

            dbg("guide_monitor_post")
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS guide_monitor_post(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    grouped_id INTEGER,
                    post_date TIMESTAMP,
                    source_url TEXT,
                    text TEXT,
                    views INTEGER,
                    forwards INTEGER,
                    reactions_total INTEGER,
                    reactions_json JSON,
                    content_hash TEXT,
                    media_refs_json JSON,
                    media_assets_json JSON,
                    post_kind TEXT,
                    prefilter_passed INTEGER NOT NULL DEFAULT 0,
                    llm_status TEXT,
                    title_hint TEXT,
                    raw_facts_json JSON,
                    first_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    last_scanned_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(source_id, message_id),
                    FOREIGN KEY(source_id) REFERENCES guide_source(id) ON DELETE CASCADE
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_guide_monitor_post_source_date ON guide_monitor_post(source_id, post_date)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_guide_monitor_post_kind ON guide_monitor_post(post_kind)"
            )
            await _add_column(conn, "guide_monitor_post", "media_assets_json JSON")

            dbg("guide_template")
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS guide_template(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_id INTEGER,
                    canonical_title TEXT NOT NULL,
                    title_normalized TEXT NOT NULL,
                    aliases_json JSON,
                    base_city TEXT,
                    availability_mode TEXT,
                    audience_fit_json JSON,
                    participant_profiles_json JSON,
                    summary_short TEXT,
                    facts_rollup_json JSON,
                    first_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    last_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(profile_id, title_normalized),
                    FOREIGN KEY(profile_id) REFERENCES guide_profile(id) ON DELETE SET NULL
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_guide_template_title_norm ON guide_template(title_normalized)"
            )

            dbg("guide_occurrence")
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS guide_occurrence(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    template_id INTEGER,
                    primary_source_id INTEGER,
                    primary_message_id INTEGER,
                    source_fingerprint TEXT NOT NULL UNIQUE,
                    canonical_title TEXT NOT NULL,
                    title_normalized TEXT NOT NULL,
                    participant_profiles_json JSON,
                    guide_names_json JSON,
                    organizer_names_json JSON,
                    digest_eligible INTEGER NOT NULL DEFAULT 1,
                    digest_eligibility_reason TEXT,
                    is_last_call INTEGER NOT NULL DEFAULT 0,
                    aggregator_only INTEGER NOT NULL DEFAULT 0,
                    rescheduled_from_id INTEGER,
                    date TEXT,
                    time TEXT,
                    duration_text TEXT,
                    city TEXT,
                    meeting_point TEXT,
                    audience_fit_json JSON,
                    price_text TEXT,
                    booking_text TEXT,
                    booking_url TEXT,
                    channel_url TEXT,
                    status TEXT NOT NULL DEFAULT 'scheduled',
                    seats_text TEXT,
                    summary_one_liner TEXT,
                    digest_blurb TEXT,
                    fact_pack_json JSON,
                    views INTEGER,
                    likes INTEGER,
                    published_new_digest_issue_id INTEGER,
                    published_last_call_digest_issue_id INTEGER,
                    published_visual_digest_issue_id INTEGER,
                    first_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    last_seen_post_at TIMESTAMP,
                    FOREIGN KEY(template_id) REFERENCES guide_template(id) ON DELETE SET NULL,
                    FOREIGN KEY(primary_source_id) REFERENCES guide_source(id) ON DELETE SET NULL,
                    FOREIGN KEY(rescheduled_from_id) REFERENCES guide_occurrence(id) ON DELETE SET NULL
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_guide_occurrence_date ON guide_occurrence(date)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_guide_occurrence_digest ON guide_occurrence(digest_eligible, published_new_digest_issue_id)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_guide_occurrence_last_call ON guide_occurrence(is_last_call, published_last_call_digest_issue_id)"
            )
            await _add_column(conn, "guide_occurrence", "fact_pack_json JSON")
            await _add_column(conn, "guide_occurrence", "published_visual_digest_issue_id INTEGER")
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_guide_occurrence_visual_digest "
                "ON guide_occurrence(digest_eligible, published_visual_digest_issue_id, updated_at)"
            )

            dbg("guide_occurrence_source")
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS guide_occurrence_source(
                    occurrence_id INTEGER NOT NULL,
                    post_id INTEGER NOT NULL,
                    role TEXT NOT NULL DEFAULT 'primary',
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (occurrence_id, post_id),
                    FOREIGN KEY(occurrence_id) REFERENCES guide_occurrence(id) ON DELETE CASCADE,
                    FOREIGN KEY(post_id) REFERENCES guide_monitor_post(id) ON DELETE CASCADE
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_guide_occurrence_source_role ON guide_occurrence_source(role)"
            )

            dbg("guide_fact_claim")
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS guide_fact_claim(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_kind TEXT NOT NULL,
                    entity_id INTEGER NOT NULL,
                    fact_key TEXT NOT NULL,
                    fact_value TEXT,
                    confidence REAL,
                    source_post_id INTEGER,
                    claim_role TEXT,
                    provenance_json JSON,
                    observed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    last_confirmed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(source_post_id) REFERENCES guide_monitor_post(id) ON DELETE SET NULL
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_guide_fact_claim_entity ON guide_fact_claim(entity_kind, entity_id)"
            )
            await _add_column(conn, "guide_fact_claim", "claim_role TEXT")
            await _add_column(conn, "guide_fact_claim", "provenance_json JSON")
            await _add_column(conn, "guide_fact_claim", "observed_at TIMESTAMP")
            await _add_column(conn, "guide_fact_claim", "last_confirmed_at TIMESTAMP")

            dbg("guide_digest_issue")
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS guide_digest_issue(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    family TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'preview',
                    target_chat TEXT,
                    title TEXT,
                    text TEXT,
                    items_json JSON,
                    media_items_json JSON,
                    run_id INTEGER,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    published_at TIMESTAMP,
                    published_message_ids_json JSON,
                    published_targets_json JSON,
                    FOREIGN KEY(run_id) REFERENCES ops_run(id) ON DELETE SET NULL
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_guide_digest_issue_family_status ON guide_digest_issue(family, status, created_at)"
            )
            await _add_column(conn, "guide_digest_issue", "published_targets_json JSON")

            # Canonical Telegram sources (safe seed).
            skip_tg_seed = (os.getenv("DB_INIT_SKIP_TG_SOURCES_SEED") or "").strip().lower() in {
                "1",
                "true",
                "yes",
            }
            if not skip_tg_seed:
                try:
                    from telegram_sources_seed import seed_telegram_sources

                    await seed_telegram_sources(conn)
                except Exception:
                    logging.exception("telegram_source seed failed (non-fatal)")

            skip_guide_seed = (os.getenv("DB_INIT_SKIP_GUIDE_SOURCES_SEED") or "").strip().lower() in {
                "1",
                "true",
                "yes",
            }
            if not skip_guide_seed:
                try:
                    from guide_excursions.seed import seed_guide_sources

                    await seed_guide_sources(conn)
                except Exception:
                    logging.exception("guide_source seed failed (non-fatal)")

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ops_run(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    kind TEXT NOT NULL,
                    trigger TEXT NOT NULL DEFAULT 'manual',
                    chat_id INTEGER,
                    operator_id INTEGER,
                    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    finished_at TIMESTAMP,
                    status TEXT NOT NULL DEFAULT 'running',
                    metrics_json JSON NOT NULL DEFAULT '{}',
                    details_json JSON NOT NULL DEFAULT '{}'
                )
                """
            )
            await _add_column(conn, "ops_run", "trigger TEXT NOT NULL DEFAULT 'manual'")
            await _add_column(conn, "ops_run", "chat_id INTEGER")
            await _add_column(conn, "ops_run", "operator_id INTEGER")
            await _add_column(conn, "ops_run", "finished_at TIMESTAMP")
            await _add_column(conn, "ops_run", "status TEXT NOT NULL DEFAULT 'running'")
            await _add_column(conn, "ops_run", "metrics_json JSON NOT NULL DEFAULT '{}'")
            await _add_column(conn, "ops_run", "details_json JSON NOT NULL DEFAULT '{}'")
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_ops_run_kind_started_at ON ops_run(kind, started_at)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_ops_run_status_started_at ON ops_run(status, started_at)"
            )

            # Telegram web preview (Instant View) probe results for Telegraph pages.
            # Used by the Telegraph cache sanitizer to track pages missing `cached_page`/photo
            # (often leads to “black screen” in Telegram clients).
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS telegraph_preview_probe(
                    url TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    ref_id INTEGER,
                    ref_key TEXT,
                    last_checked_at TIMESTAMP,
                    last_ok INTEGER NOT NULL DEFAULT 0,
                    last_has_cached_page INTEGER NOT NULL DEFAULT 0,
                    last_has_photo INTEGER NOT NULL DEFAULT 0,
                    last_title TEXT,
                    last_site_name TEXT,
                    last_error TEXT,
                    total_checks INTEGER NOT NULL DEFAULT 0,
                    total_ok INTEGER NOT NULL DEFAULT 0,
                    consecutive_failures INTEGER NOT NULL DEFAULT 0,
                    last_ok_at TIMESTAMP,
                    last_fail_at TIMESTAMP
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_telegraph_preview_probe_kind ON telegraph_preview_probe(kind)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_telegraph_preview_probe_last_checked ON telegraph_preview_probe(last_checked_at)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_telegraph_preview_probe_failures ON telegraph_preview_probe(consecutive_failures)"
            )

            # For local/offline regression runs we sometimes only need the core tables
            # (event + Smart Update + Telegram monitoring metadata). Building the full
            # schema and optional indexes on a prod snapshot can be slow.
            if (os.getenv("DB_INIT_MINIMAL") or "").strip().lower() in {"1", "true", "yes"}:
                dbg("minimal mode: returning after core tables")
                await conn.commit()
                return

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS monthpage(
                    month TEXT PRIMARY KEY,
                    url TEXT NOT NULL,
                    path TEXT NOT NULL,
                    url2 TEXT,
                    path2 TEXT,
                    content_hash TEXT,
                    content_hash2 TEXT
                )
                """
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS monthpagepart(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    month TEXT NOT NULL,
                    part_number INTEGER NOT NULL,
                    url TEXT NOT NULL,
                    path TEXT NOT NULL,
                    content_hash TEXT,
                    first_date TEXT,
                    last_date TEXT,
                    UNIQUE(month, part_number)
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_monthpagepart_month ON monthpagepart(month)"
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS monthexhibitionspage(
                    month TEXT PRIMARY KEY,
                    url TEXT NOT NULL,
                    path TEXT NOT NULL,
                    content_hash TEXT
                )
                """
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS weekendpage(
                    start TEXT PRIMARY KEY,
                    url TEXT NOT NULL,
                    path TEXT NOT NULL,
                    vk_post_url TEXT,
                    content_hash TEXT
                )
                """
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tomorrowpage(
                    date TEXT PRIMARY KEY,
                    url TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS weekpage(
                    start TEXT PRIMARY KEY,
                    vk_post_url TEXT,
                    content_hash TEXT
                )
                """
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS festival(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    full_name TEXT,
                    description TEXT,
                    start_date TEXT,
                    end_date TEXT,
                    telegraph_url TEXT,
                    telegraph_path TEXT,
                    vk_post_url TEXT,
                    vk_poll_url TEXT,
                    photo_url TEXT,
                    photo_urls JSON,
                    aliases JSON,
                    website_url TEXT,
                    program_url TEXT,
                    vk_url TEXT,
                    tg_url TEXT,
                    ticket_url TEXT,
                    location_name TEXT,
                    location_address TEXT,
                    city TEXT,
                    source_text TEXT,
                    source_post_url TEXT,
                    source_chat_id INTEGER,
                    source_message_id INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await _add_column(conn, "festival", "location_name TEXT")
            await _add_column(conn, "festival", "location_address TEXT")
            await _add_column(conn, "festival", "city TEXT")
            await _add_column(conn, "festival", "program_url TEXT")
            await _add_column(conn, "festival", "ticket_url TEXT")
            await _add_column(conn, "festival", "nav_hash TEXT")
            await _add_column(conn, "festival", "photo_urls JSON")
            await _add_column(conn, "festival", "aliases JSON")
            await _add_column(conn, "festival", "source_post_url TEXT")
            await _add_column(conn, "festival", "source_chat_id INTEGER")
            await _add_column(conn, "festival", "source_message_id INTEGER")
            await _add_column(conn, "festival", "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            # Parser-related fields (Universal Festival Parser)
            await _add_column(conn, "festival", "source_url TEXT")
            await _add_column(conn, "festival", "source_type TEXT")
            await _add_column(conn, "festival", "parser_run_id TEXT")
            await _add_column(conn, "festival", "parser_version TEXT")
            await _add_column(conn, "festival", "last_parsed_at TIMESTAMP")
            await _add_column(conn, "festival", "uds_storage_path TEXT")
            await _add_column(conn, "festival", "contacts_phone TEXT")
            await _add_column(conn, "festival", "contacts_email TEXT")
            await _add_column(conn, "festival", "is_annual BOOLEAN")
            await _add_column(conn, "festival", "audience TEXT")
            await _add_column(
                conn,
                "festival",
                "activities_json JSON NOT NULL DEFAULT '[]'",
            )
            await conn.execute(
                "UPDATE festival SET activities_json = '[]' WHERE activities_json IS NULL"
            )

            festival_cursor = await conn.execute("PRAGMA table_info('festival')")
            festival_columns = await festival_cursor.fetchall()
            await festival_cursor.close()
            festival_column_names = {column[1] for column in festival_columns}
            if "created_at" not in festival_column_names:
                await conn.execute("ALTER TABLE festival ADD COLUMN created_at TIMESTAMP")
                await conn.execute(
                    "UPDATE festival SET created_at = CURRENT_TIMESTAMP WHERE created_at IS NULL"
                )
            if "updated_at" not in festival_column_names:
                await conn.execute("ALTER TABLE festival ADD COLUMN updated_at TIMESTAMP")
            await conn.execute(
                "UPDATE festival SET updated_at = COALESCE(updated_at, created_at, CURRENT_TIMESTAMP) "
                "WHERE updated_at IS NULL"
            )

            # Public year-scoped calendar editions are separate from the
            # legacy festival-series rows.  Several parser paths still assume
            # ``Festival.name`` resolves to at most one record, so inserting a
            # second yearly row into ``festival`` would be unsafe.
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS festival_calendar_item(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    calendar_year INTEGER NOT NULL,
                    slug TEXT NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    start_date TEXT,
                    end_date TEXT,
                    date_precision TEXT NOT NULL DEFAULT 'exact',
                    date_label TEXT NOT NULL,
                    sort_date TEXT NOT NULL,
                    month_key TEXT NOT NULL,
                    display_order INTEGER NOT NULL,
                    place_label TEXT NOT NULL,
                    category TEXT NOT NULL,
                    status TEXT NOT NULL,
                    status_label TEXT NOT NULL,
                    source_url TEXT NOT NULL,
                    source_label TEXT NOT NULL,
                    internal_event_id INTEGER,
                    festival_id INTEGER,
                    cover_key TEXT NOT NULL,
                    image_width INTEGER NOT NULL,
                    image_height INTEGER NOT NULL,
                    media_mode TEXT NOT NULL DEFAULT 'visual',
                    object_position TEXT,
                    catalog_version TEXT NOT NULL,
                    is_public BOOLEAN NOT NULL DEFAULT 1,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(internal_event_id) REFERENCES event(id) ON DELETE SET NULL,
                    FOREIGN KEY(festival_id) REFERENCES festival(id) ON DELETE SET NULL,
                    UNIQUE(calendar_year, slug),
                    UNIQUE(calendar_year, display_order)
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_festival_calendar_item_public_month "
                "ON festival_calendar_item(calendar_year,is_public,month_key,display_order)"
            )

            await conn.execute(
                """
                CREATE TRIGGER IF NOT EXISTS festival_set_created_at
                AFTER INSERT ON festival
                FOR EACH ROW
                WHEN NEW.created_at IS NULL
                BEGIN
                    UPDATE festival
                    SET created_at = CURRENT_TIMESTAMP
                    WHERE id = NEW.id;
                END;
                """
            )
            await conn.execute(
                """
                CREATE TRIGGER IF NOT EXISTS festival_set_updated_at
                AFTER UPDATE ON festival
                FOR EACH ROW
                WHEN NEW.updated_at IS NULL OR NEW.updated_at = OLD.updated_at
                BEGIN
                    UPDATE festival
                    SET updated_at = CURRENT_TIMESTAMP
                    WHERE id = NEW.id;
                END;
                """
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS festival_queue(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    source_kind TEXT NOT NULL,
                    source_url TEXT NOT NULL,
                    source_text TEXT,
                    source_chat_username TEXT,
                    source_chat_id INTEGER,
                    source_message_id INTEGER,
                    source_group_id INTEGER,
                    source_post_id INTEGER,
                    festival_context TEXT,
                    festival_name TEXT,
                    festival_full TEXT,
                    festival_series TEXT,
                    dedup_links_json JSON NOT NULL DEFAULT '[]',
                    signals_json JSON NOT NULL DEFAULT '{}',
                    result_json JSON NOT NULL DEFAULT '{}',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    next_run_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await _add_column(conn, "festival_queue", "status TEXT NOT NULL DEFAULT 'pending'")
            await _add_column(conn, "festival_queue", "source_kind TEXT")
            await _add_column(conn, "festival_queue", "source_url TEXT")
            await _add_column(conn, "festival_queue", "source_text TEXT")
            await _add_column(conn, "festival_queue", "source_chat_username TEXT")
            await _add_column(conn, "festival_queue", "source_chat_id INTEGER")
            await _add_column(conn, "festival_queue", "source_message_id INTEGER")
            await _add_column(conn, "festival_queue", "source_group_id INTEGER")
            await _add_column(conn, "festival_queue", "source_post_id INTEGER")
            await _add_column(conn, "festival_queue", "festival_context TEXT")
            await _add_column(conn, "festival_queue", "festival_name TEXT")
            await _add_column(conn, "festival_queue", "festival_full TEXT")
            await _add_column(conn, "festival_queue", "festival_series TEXT")
            await _add_column(conn, "festival_queue", "dedup_links_json JSON NOT NULL DEFAULT '[]'")
            await _add_column(conn, "festival_queue", "signals_json JSON NOT NULL DEFAULT '{}'")
            await _add_column(conn, "festival_queue", "result_json JSON NOT NULL DEFAULT '{}'")
            await _add_column(conn, "festival_queue", "attempts INTEGER NOT NULL DEFAULT 0")
            await _add_column(conn, "festival_queue", "last_error TEXT")
            await _add_column(conn, "festival_queue", "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            await _add_column(conn, "festival_queue", "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            await _add_column(conn, "festival_queue", "next_run_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_festival_queue_status_next_run ON festival_queue(status, next_run_at)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_festival_queue_source_kind ON festival_queue(source_kind)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_festival_queue_source_url ON festival_queue(source_url)"
            )

            # Provider-neutral operational ledger for the collect-only
            # festival web research contour.  Provider execution and semantic
            # acceptance are deliberately represented by separate lane states.
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS festival_web_research_run(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_uid TEXT NOT NULL,
                    target_key TEXT NOT NULL,
                    series_candidate TEXT,
                    edition_candidate TEXT,
                    state TEXT NOT NULL DEFAULT 'pending',
                    mode TEXT NOT NULL DEFAULT 'collect_only',
                    review_status TEXT NOT NULL DEFAULT 'pending',
                    input_fingerprint TEXT NOT NULL,
                    orchestration_version TEXT NOT NULL,
                    contract_version TEXT NOT NULL,
                    taxonomy_version TEXT NOT NULL,
                    taxonomy_sha256 TEXT NOT NULL,
                    primary_queue_item_id INTEGER,
                    candidate_sha256 TEXT,
                    candidate_json JSON NOT NULL DEFAULT '{}',
                    quality_json JSON NOT NULL DEFAULT '{}',
                    artifact_manifest_json JSON NOT NULL DEFAULT '{}',
                    lease_owner TEXT,
                    lease_expires_at TIMESTAMP,
                    reviewed_by TEXT,
                    reviewed_at TIMESTAMP,
                    review_reason TEXT,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT ux_festival_web_research_run_uid UNIQUE(run_uid),
                    CONSTRAINT ux_festival_web_research_run_input_fingerprint UNIQUE(input_fingerprint),
                    FOREIGN KEY(primary_queue_item_id) REFERENCES festival_queue(id)
                        ON DELETE SET NULL
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_festival_web_research_run_state_updated "
                "ON festival_web_research_run(state,updated_at)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_festival_web_research_run_target_created "
                "ON festival_web_research_run(target_key,created_at)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_festival_web_research_run_review_updated "
                "ON festival_web_research_run(review_status,updated_at)"
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS festival_web_research_lane_run(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    lane TEXT NOT NULL DEFAULT 'antigravity',
                    attempt_no INTEGER NOT NULL DEFAULT 1,
                    request_uid TEXT NOT NULL,
                    provider_state TEXT NOT NULL DEFAULT 'pending',
                    semantic_state TEXT NOT NULL DEFAULT 'pending',
                    interaction_ids_json JSON NOT NULL DEFAULT '[]',
                    model_id TEXT,
                    prompt_version TEXT NOT NULL,
                    contract_version TEXT NOT NULL,
                    taxonomy_version TEXT NOT NULL,
                    taxonomy_sha256 TEXT NOT NULL,
                    input_fingerprint TEXT NOT NULL,
                    artifact_manifest_json JSON NOT NULL DEFAULT '{}',
                    usage_json JSON NOT NULL DEFAULT '{}',
                    validation_json JSON NOT NULL DEFAULT '{}',
                    candidate_sha256 TEXT,
                    candidate_json JSON NOT NULL DEFAULT '{}',
                    provider_error_code TEXT,
                    semantic_error_code TEXT,
                    last_error TEXT,
                    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT ux_festival_web_research_lane_attempt UNIQUE(run_id,lane,attempt_no),
                    CONSTRAINT ux_festival_web_research_lane_request_uid UNIQUE(request_uid),
                    FOREIGN KEY(run_id) REFERENCES festival_web_research_run(id)
                        ON DELETE CASCADE
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_festival_web_research_lane_provider_updated "
                "ON festival_web_research_lane_run(provider_state,updated_at)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_festival_web_research_lane_semantic_updated "
                "ON festival_web_research_lane_run(semantic_state,updated_at)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_festival_web_research_lane_input_fingerprint "
                "ON festival_web_research_lane_run(input_fingerprint)"
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS festival_web_research_item(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    queue_item_id INTEGER NOT NULL,
                    original_status TEXT NOT NULL,
                    source_role TEXT NOT NULL,
                    decision TEXT NOT NULL DEFAULT 'pending',
                    decision_reason TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT ux_festival_web_research_item_run_queue UNIQUE(run_id,queue_item_id),
                    FOREIGN KEY(run_id) REFERENCES festival_web_research_run(id)
                        ON DELETE CASCADE,
                    FOREIGN KEY(queue_item_id) REFERENCES festival_queue(id)
                        ON DELETE RESTRICT
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_festival_web_research_item_queue "
                "ON festival_web_research_item(queue_item_id,created_at)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_festival_web_research_item_decision "
                "ON festival_web_research_item(decision,updated_at)"
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS festival_web_research_source(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lane_run_id INTEGER NOT NULL,
                    source_id TEXT NOT NULL,
                    requested_url TEXT NOT NULL,
                    resolved_url TEXT,
                    canonical_url TEXT,
                    source_role TEXT NOT NULL,
                    edition_status TEXT NOT NULL DEFAULT 'unknown',
                    content_sha256 TEXT,
                    snapshot_ref TEXT,
                    normalizer_version TEXT,
                    quote_index_ref TEXT,
                    fetched_at TIMESTAMP,
                    decision TEXT NOT NULL DEFAULT 'pending',
                    exclusion_reason TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT ux_festival_web_research_source_lane_source UNIQUE(lane_run_id,source_id),
                    FOREIGN KEY(lane_run_id)
                        REFERENCES festival_web_research_lane_run(id)
                        ON DELETE CASCADE
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_festival_web_research_source_canonical_url "
                "ON festival_web_research_source(canonical_url)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_festival_web_research_source_content_hash "
                "ON festival_web_research_source(content_sha256)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_festival_web_research_source_lane_decision "
                "ON festival_web_research_source(lane_run_id,decision)"
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ticket_site_queue(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    status TEXT NOT NULL DEFAULT 'active',
                    site_kind TEXT NOT NULL,
                    url TEXT NOT NULL,
                    event_id INTEGER,
                    source_post_url TEXT,
                    source_chat_username TEXT,
                    source_chat_id INTEGER,
                    source_message_id INTEGER,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    last_result_json JSON NOT NULL DEFAULT '{}',
                    last_run_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    next_run_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await _add_column(conn, "ticket_site_queue", "status TEXT NOT NULL DEFAULT 'active'")
            await _add_column(conn, "ticket_site_queue", "site_kind TEXT")
            await _add_column(conn, "ticket_site_queue", "url TEXT")
            await _add_column(conn, "ticket_site_queue", "event_id INTEGER")
            await _add_column(conn, "ticket_site_queue", "source_post_url TEXT")
            await _add_column(conn, "ticket_site_queue", "source_chat_username TEXT")
            await _add_column(conn, "ticket_site_queue", "source_chat_id INTEGER")
            await _add_column(conn, "ticket_site_queue", "source_message_id INTEGER")
            await _add_column(conn, "ticket_site_queue", "attempts INTEGER NOT NULL DEFAULT 0")
            await _add_column(conn, "ticket_site_queue", "last_error TEXT")
            await _add_column(conn, "ticket_site_queue", "last_result_json JSON NOT NULL DEFAULT '{}'")
            await _add_column(conn, "ticket_site_queue", "last_run_at TIMESTAMP")
            await _add_column(conn, "ticket_site_queue", "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            await _add_column(conn, "ticket_site_queue", "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            await _add_column(conn, "ticket_site_queue", "next_run_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_ticket_site_queue_status_next_run ON ticket_site_queue(status, next_run_at)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_ticket_site_queue_site_kind ON ticket_site_queue(site_kind)"
            )
            await conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_ticket_site_queue_url ON ticket_site_queue(url)"
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS joboutbox(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER NOT NULL,
                    task TEXT NOT NULL,
                    payload TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempts INTEGER DEFAULT 0,
                    last_error TEXT,
                    last_result TEXT,
                    coalesce_key TEXT,
                    depends_on TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    next_run_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            await _add_column(conn, "joboutbox", "last_result TEXT")
            await _add_column(conn, "joboutbox", "coalesce_key TEXT")
            await _add_column(conn, "joboutbox", "depends_on TEXT")

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS vk_source(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_id INTEGER NOT NULL,
                    screen_name TEXT,
                    name TEXT,
                    location TEXT,
                    default_time TEXT,
                    default_ticket_link TEXT,
                    festival_source BOOLEAN DEFAULT 0,
                    festival_series TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_vk_source_group ON vk_source(group_id)"
            )

            await _add_column(conn, "vk_source", "default_ticket_link TEXT")
            await _add_column(conn, "vk_source", "festival_source BOOLEAN DEFAULT 0")
            await _add_column(conn, "vk_source", "festival_series TEXT")
            # owner_type distinguishes communities ("group") from personal
            # pages ("user"). Existing rows default to "group" which matches
            # the historical contract where group_id was always negated to
            # form owner_id in wall.get and post URLs.
            await _add_column(
                conn, "vk_source", "owner_type TEXT NOT NULL DEFAULT 'group'"
            )

            # Seed well-known VK sources with stable defaults so live E2E / fresh prod
            # snapshots don't lose operator UX improvements after DB refresh.
            try:
                await conn.execute(
                    """
                    UPDATE vk_source
                    SET location = ?
                    WHERE group_id = ?
                      AND (
                        location IS NULL
                        OR TRIM(location) = ''
                        OR location IN (
                            'Гаражка, Калининград',
                            'Гаражка Калининград',
                            'Garazhka Kaliningrad'
                        )
                      )
                    """,
                    ("Понарт, Судостроительная 6/2, Калининград", 226847232),
                )
                await conn.execute(
                    """
                    UPDATE vk_source
                    SET location = ?
                    WHERE group_id = ?
                      AND (
                        location IS NULL
                        OR TRIM(location) = ''
                        OR location IN (
                            'Калининград Сити Джаз Клуб',
                            'Калининград Сити Джаз Клуб, Мира 33-35, Калининград'
                        )
                      )
                    """,
                    ("Стендап клуб Локация, Юбилейная 18, Калининград", 214027639),
                )
                await conn.execute(
                    """
                    UPDATE vk_source
                    SET location = ?
                    WHERE group_id = ?
                      AND (
                        location IS NULL
                        OR TRIM(location) = ''
                        OR location IN (
                            'Калининград Сити Джаз Клуб',
                            'Калининград Сити Джаз Клуб, Мира 33-35, Калининград'
                        )
                      )
                    """,
                    ("Бар Бастион, Судостроительная 6/1, Калининград", 149955604),
                )
                await conn.execute(
                    """
                    UPDATE vk_source
                    SET location = ?
                    WHERE group_id = ?
                      AND (
                        location IS NULL
                        OR TRIM(location) = ''
                        OR UPPER(TRIM(location)) IN (
                            'ЛЕКЦИОННЫЙ ЗАЛ',
                            '4 ЭТАЖ ЛЕКЦИОННЫЙ ЗАЛ',
                            'ЛЕКЦИОННЫЙ ЗАЛ, 4 ЭТАЖ'
                        )
                      )
                    """,
                    ("Научная библиотека, Мира 9, Калининград", 30777579),
                )
                await conn.execute(
                    """
                    UPDATE vk_source
                    SET location = ?
                    WHERE group_id = ?
                      AND (
                        location IS NULL
                        OR TRIM(location) = ''
                        OR location IN (
                            'Дворец спорта «Юность»',
                            'Дворец спорта «Юность», Маршала Баграмяна 2, Калининград'
                        )
                      )
                    """,
                    ("Дворец спорта «Янтарный», Согласия 39, Калининград", 179910542),
                )
                await conn.execute(
                    """
                    UPDATE vk_source
                    SET location = ?
                    WHERE group_id = ?
                      AND (
                        location IS NULL
                        OR TRIM(location) = ''
                        OR location IN (
                            'Калининград Сити Джаз Клуб',
                            'Калининград Сити Джаз Клуб, Мира 33-35, Калининград'
                        )
                      )
                    """,
                    ("Центр «Мой бизнес», Уральская 18, Калининград", 39437155),
                )
                skip_vk_source_seed = (
                    (os.getenv("DB_INIT_SKIP_VK_SOURCES_SEED") or "").strip().lower()
                    in {"1", "true", "yes", "on"}
                )
                if not skip_vk_source_seed:
                    # Seed VK monitoring sources that are tied to specific operator
                    # requests rather than ad-hoc UI additions. INSERT OR IGNORE so
                    # the row only appears the first time and never overwrites
                    # operator edits later. For users the ``group_id`` column
                    # holds the positive user_id; ``owner_type='user'`` flips
                    # the owner_id sign in vk_wall_since and the URL builder.
                    await conn.execute(
                        """
                        INSERT OR IGNORE INTO vk_source(group_id, screen_name, name, owner_type)
                        VALUES (?, ?, ?, ?)
                        """,
                        (194393485, "club194393485", "Географическая школа", "group"),
                    )
                    await conn.execute(
                        """
                        INSERT OR IGNORE INTO vk_source(group_id, screen_name, name, owner_type)
                        VALUES (?, ?, ?, ?)
                        """,
                        (61694047, "ivsguide", "Игорь Селин", "user"),
                    )
                    await conn.execute(
                        """
                        INSERT OR IGNORE INTO vk_source(group_id, screen_name, name, owner_type)
                        VALUES (?, ?, ?, ?)
                        """,
                        (290624941, "natakkaz", "Наталья Казакова", "user"),
                    )
            except Exception:
                logging.warning("db.init: failed to seed vk_source defaults", exc_info=True)

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS vk_tmp_post(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    group_id INTEGER NOT NULL,
                    post_id INTEGER NOT NULL,
                    date INTEGER NOT NULL,
                    text TEXT,
                    photos JSON,
                    url TEXT
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_vk_tmp_post_batch ON vk_tmp_post(batch, id)"
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS vk_post_metric(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_id INTEGER NOT NULL,
                    post_id INTEGER NOT NULL,
                    age_day INTEGER NOT NULL,
                    source_url TEXT,
                    post_ts INTEGER,
                    collected_ts INTEGER NOT NULL,
                    views INTEGER,
                    likes INTEGER,
                    comments INTEGER,
                    reposts INTEGER,
                    UNIQUE(group_id, post_id, age_day)
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_vk_metric_group_age ON vk_post_metric(group_id, age_day)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_vk_metric_group_post ON vk_post_metric(group_id, post_id)"
            )
            await _add_column(conn, "vk_post_metric", "comments INTEGER")
            await _add_column(conn, "vk_post_metric", "reposts INTEGER")

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS event_publication(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER NOT NULL,
                    platform TEXT NOT NULL,
                    target TEXT NOT NULL,
                    stored_url TEXT,
                    live_url TEXT,
                    stored_post_id INTEGER,
                    live_post_id INTEGER,
                    match_method TEXT,
                    match_confidence REAL,
                    status TEXT NOT NULL DEFAULT 'unknown',
                    resolved_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(event_id, platform, target),
                    FOREIGN KEY(event_id) REFERENCES event(id) ON DELETE CASCADE
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_publication_target_status ON event_publication(target, status)"
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS page_section_cache(
                    page_key TEXT NOT NULL,
                    section_key TEXT NOT NULL,
                    hash TEXT NOT NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(page_key, section_key)
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_psc_page ON page_section_cache(page_key)"
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS vk_crawl_cursor (
                    group_id     INTEGER PRIMARY KEY,
                    last_seen_ts INTEGER DEFAULT 0,
                    last_post_id INTEGER DEFAULT 0,
                    updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    checked_at   INTEGER
                )
                """
            )

            await _add_column(conn, "vk_crawl_cursor", "checked_at INTEGER")
            # See vk_source.owner_type — default "group" preserves the
            # legacy contract for existing cursors.
            await _add_column(
                conn, "vk_crawl_cursor", "owner_type TEXT NOT NULL DEFAULT 'group'"
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS vk_inbox (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_id     INTEGER NOT NULL,
                    post_id      INTEGER NOT NULL,
                    date         INTEGER NOT NULL,
                    text         TEXT NOT NULL,
                    matched_kw   TEXT,
                    has_date     INTEGER NOT NULL,
                    event_ts_hint INTEGER,
                    status       TEXT NOT NULL DEFAULT 'pending',
                    locked_by    INTEGER,
                    locked_at    TIMESTAMP,
                    imported_event_id INTEGER,
                    review_batch TEXT,
                    attempts     INTEGER NOT NULL DEFAULT 0,
                    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_vk_inbox_unique ON vk_inbox(group_id, post_id)"
            )

            await _add_column(conn, "vk_inbox", "event_ts_hint INTEGER")
            await _add_column(conn, "vk_inbox", "attempts INTEGER NOT NULL DEFAULT 0")
            # See vk_source.owner_type — default "group" preserves the
            # legacy contract for existing inbox rows.
            await _add_column(
                conn, "vk_inbox", "owner_type TEXT NOT NULL DEFAULT 'group'"
            )
            await _add_column(conn, "vk_inbox", "source_packet_id INTEGER")
            await _add_column(conn, "vk_inbox", "next_attempt_at TIMESTAMP")
            await _add_column(conn, "vk_inbox", "last_typed_reason TEXT")
            await _add_column(conn, "vk_inbox", "quota_scope TEXT")
            await _add_column(conn, "vk_inbox", "provider_retry_after INTEGER")

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS vk_auto_import_state (
                    id INTEGER PRIMARY KEY CHECK(id=1),
                    fresh_since_history INTEGER NOT NULL DEFAULT 0,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await conn.execute(
                "INSERT OR IGNORE INTO vk_auto_import_state(id) VALUES(1)"
            )

            # Raw-first VK ingestion ledger.  A row is an immutable fetched
            # revision; semantic state lives beside it but the source payload is
            # never overwritten.  The inbox points at the newest revision.
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS vk_source_packet (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_type TEXT NOT NULL DEFAULT 'vk',
                    owner_id INTEGER NOT NULL,
                    owner_type TEXT NOT NULL DEFAULT 'group',
                    post_id INTEGER NOT NULL,
                    revision INTEGER NOT NULL,
                    source_url TEXT NOT NULL,
                    published_at INTEGER NOT NULL,
                    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    raw_text TEXT NOT NULL,
                    raw_payload_json TEXT NOT NULL,
                    attachment_metadata_json TEXT NOT NULL DEFAULT '[]',
                    envelope_version INTEGER,
                    capture_complete INTEGER NOT NULL DEFAULT 0,
                    evidence_replayability TEXT NOT NULL DEFAULT 'replayable_legacy_incomplete',
                    payload_hash TEXT NOT NULL,
                    source_revision_hash TEXT NOT NULL,
                    discovery_keyword_hints_json TEXT NOT NULL DEFAULT '[]',
                    discovered_date_hints_json TEXT NOT NULL DEFAULT '[]',
                    event_ts_hint INTEGER,
                    ocr_status TEXT NOT NULL DEFAULT 'pending',
                    llm_status TEXT NOT NULL DEFAULT 'pending',
                    evidence_manifest_json TEXT,
                    parse_result_json TEXT,
                    successful_parse_key TEXT,
                    prompt_version TEXT,
                    model TEXT,
                    quota_scope TEXT,
                    provider_retry_after INTEGER,
                    status TEXT NOT NULL DEFAULT 'pending',
                    next_attempt_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    lease_owner TEXT,
                    lease_expires_at TIMESTAMP,
                    last_typed_reason TEXT,
                    terminal_carrier_outcome TEXT,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(source_type, owner_id, post_id, revision),
                    UNIQUE(source_type, owner_id, post_id, source_revision_hash)
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_vk_source_packet_due ON vk_source_packet(status,next_attempt_at,published_at)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_vk_source_packet_post ON vk_source_packet(source_type,owner_id,post_id,revision)"
            )
            await _add_column(conn, "vk_source_packet", "provider_retry_after INTEGER")
            await _add_column(conn, "vk_source_packet", "envelope_version INTEGER")
            await _add_column(
                conn, "vk_source_packet", "capture_complete INTEGER NOT NULL DEFAULT 0"
            )
            await _add_column(
                conn,
                "vk_source_packet",
                "evidence_replayability TEXT NOT NULL DEFAULT 'replayable_legacy_incomplete'",
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS vk_source_packet_attempt (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_packet_id INTEGER NOT NULL,
                    attempt_no INTEGER NOT NULL,
                    attempt_kind TEXT NOT NULL DEFAULT 'primary',
                    parse_key TEXT,
                    payload_hash TEXT NOT NULL,
                    source_type TEXT NOT NULL DEFAULT 'vk',
                    source_url TEXT NOT NULL,
                    source_revision_hash TEXT NOT NULL,
                    discovery_hints_json TEXT NOT NULL DEFAULT '{}',
                    evidence_manifest_json TEXT,
                    llm_started INTEGER NOT NULL DEFAULT 0,
                    llm_completed INTEGER NOT NULL DEFAULT 0,
                    structured_response_valid INTEGER NOT NULL DEFAULT 0,
                    model TEXT,
                    quota_scope TEXT,
                    request_id TEXT,
                    response_id TEXT,
                    finish_reason TEXT,
                    provider_retry_after INTEGER,
                    input_tokens INTEGER,
                    output_tokens INTEGER,
                    thought_tokens INTEGER,
                    reserved_tokens INTEGER,
                    primary_disposition TEXT,
                    no_event_reason TEXT,
                    verification_triggered INTEGER NOT NULL DEFAULT 0,
                    verification_reason TEXT,
                    verification_disposition TEXT,
                    event_child_count INTEGER NOT NULL DEFAULT 0,
                    lifecycle_action_count INTEGER NOT NULL DEFAULT 0,
                    smart_update_child_outcomes_json TEXT NOT NULL DEFAULT '[]',
                    terminal_carrier_outcome TEXT,
                    next_attempt_at TIMESTAMP,
                    typed_error_reason TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP,
                    FOREIGN KEY(source_packet_id) REFERENCES vk_source_packet(id) ON DELETE CASCADE
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_vk_packet_attempt_packet ON vk_source_packet_attempt(source_packet_id,attempt_no)"
            )
            await _add_column(conn, "vk_source_packet_attempt", "payload_hash TEXT")
            await _add_column(conn, "vk_source_packet_attempt", "response_id TEXT")
            await _add_column(conn, "vk_source_packet_attempt", "finish_reason TEXT")
            await _add_column(conn, "vk_source_packet_attempt", "provider_retry_after INTEGER")
            await _add_column(conn, "vk_source_packet_attempt", "no_event_reason TEXT")
            await conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ux_vk_packet_success_parse
                ON vk_source_packet_attempt(parse_key)
                WHERE parse_key IS NOT NULL
                  AND llm_completed=1
                  AND structured_response_valid=1
                """
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS vk_crawl_continuation (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_type TEXT NOT NULL DEFAULT 'vk',
                    owner_id INTEGER NOT NULL,
                    owner_type TEXT NOT NULL DEFAULT 'group',
                    continuation_key TEXT,
                    scan_mode TEXT NOT NULL DEFAULT 'incremental',
                    page_size INTEGER NOT NULL DEFAULT 30,
                    since_ts INTEGER NOT NULL,
                    offset INTEGER NOT NULL,
                    horizon_ts INTEGER NOT NULL,
                    original_cursor_ts INTEGER NOT NULL DEFAULT 0,
                    original_cursor_post_id INTEGER NOT NULL DEFAULT 0,
                    reason TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    lease_owner TEXT,
                    locked_by TEXT,
                    lease_expires_at TIMESTAMP,
                    locked_at TIMESTAMP,
                    run_id TEXT,
                    last_page_fingerprint TEXT,
                    deepest_page_ts INTEGER,
                    deepest_page_post_id INTEGER,
                    last_typed_reason TEXT,
                    completed_at TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(source_type, owner_id, since_ts, offset, horizon_ts)
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_vk_crawl_continuation_due ON vk_crawl_continuation(status,next_attempt_at)"
            )
            await _add_column(
                conn,
                "vk_crawl_continuation",
                "scan_mode TEXT NOT NULL DEFAULT 'incremental'",
            )
            await _add_column(conn, "vk_crawl_continuation", "continuation_key TEXT")
            await _add_column(
                conn,
                "vk_crawl_continuation",
                "page_size INTEGER NOT NULL DEFAULT 30",
            )
            await _add_column(
                conn,
                "vk_crawl_continuation",
                "original_cursor_ts INTEGER NOT NULL DEFAULT 0",
            )
            await _add_column(
                conn,
                "vk_crawl_continuation",
                "original_cursor_post_id INTEGER NOT NULL DEFAULT 0",
            )
            await _add_column(conn, "vk_crawl_continuation", "locked_at TIMESTAMP")
            await _add_column(conn, "vk_crawl_continuation", "locked_by TEXT")
            await _add_column(conn, "vk_crawl_continuation", "run_id TEXT")
            await _add_column(
                conn, "vk_crawl_continuation", "last_page_fingerprint TEXT"
            )
            await _add_column(conn, "vk_crawl_continuation", "deepest_page_ts INTEGER")
            await _add_column(
                conn, "vk_crawl_continuation", "deepest_page_post_id INTEGER"
            )
            await _add_column(conn, "vk_crawl_continuation", "completed_at TIMESTAMP")
            # Rows may have been queued by an older producer before the
            # continuation consumer existed. Recover the immutable boundary
            # from the canonical cursor where possible, without advancing it.
            await conn.execute(
                """
                UPDATE vk_crawl_continuation
                SET scan_mode='backfill', page_size=50
                WHERE since_ts=0 AND horizon_ts>0 AND scan_mode='incremental'
                """
            )
            await conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ux_vk_crawl_continuation_key
                ON vk_crawl_continuation(continuation_key)
                WHERE continuation_key IS NOT NULL
                """
            )
            await conn.execute(
                """
                UPDATE vk_crawl_continuation
                SET original_cursor_ts=COALESCE(
                        (SELECT last_seen_ts FROM vk_crawl_cursor
                         WHERE group_id=vk_crawl_continuation.owner_id),
                        since_ts
                    ),
                    original_cursor_post_id=COALESCE(
                        (SELECT last_post_id FROM vk_crawl_cursor
                         WHERE group_id=vk_crawl_continuation.owner_id),
                        0
                    )
                WHERE original_cursor_ts=0 AND scan_mode='incremental'
                """
            )
            # The first continuation implementation incorrectly treated a
            # repeated full page as proof of completion. Head insertions can
            # shift an absolute VK offset by exactly one page and produce that
            # shape while an older tail still exists. Reopen only that poisoned
            # terminal state; real empty/short/horizon/cursor terminals remain
            # immutable. This update is intentionally idempotent across init×2.
            await conn.execute(
                """
                UPDATE vk_crawl_continuation
                SET status='retry', next_attempt_at=CURRENT_TIMESTAMP,
                    lease_owner=NULL, locked_by=NULL, lease_expires_at=NULL,
                    locked_at=NULL, run_id=NULL, completed_at=NULL,
                    last_typed_reason='LEGACY_EXACT_PAGE_REPLAY_REOPENED',
                    updated_at=CURRENT_TIMESTAMP
                WHERE status='done' AND last_typed_reason='EXACT_PAGE_REPLAY'
                """
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS vk_review_batch (
                    batch_id     TEXT PRIMARY KEY,
                    operator_id  INTEGER NOT NULL,
                    months_csv   TEXT NOT NULL,
                    started_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    finished_at  TIMESTAMP
                )
                """
            )

            # VK inbox -> imported events mapping (VK posts may yield multiple events).
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS vk_inbox_import_event (
                    inbox_id   INTEGER NOT NULL,
                    event_id   INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (inbox_id, event_id)
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_vk_inbox_import_event_event ON vk_inbox_import_event(event_id)"
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS posterocrcache(
                    hash TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    model TEXT NOT NULL,
                    text TEXT NOT NULL,
                    title TEXT,
                    prompt_tokens INTEGER NOT NULL DEFAULT 0,
                    completion_tokens INTEGER NOT NULL DEFAULT 0,
                    total_tokens INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (hash, detail, model)
                )
                """
            )
            await _add_column(conn, "posterocrcache", "title TEXT")

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ocrusage(
                    date TEXT PRIMARY KEY,
                    spent_tokens INTEGER NOT NULL DEFAULT 0
                )
                """
            )

            # Cache: resolve city/settlement -> (is in Kaliningrad oblast?) via Wikidata/LLM.
            # Used to deterministically filter out-of-region events without repeatedly
            # querying external sources for the same city names.
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS geo_city_region_cache(
                    city_norm TEXT PRIMARY KEY,
                    is_kaliningrad_oblast BOOLEAN,
                    region_code TEXT,
                    region_name TEXT,
                    source TEXT,
                    wikidata_qid TEXT,
                    details JSON,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await _add_column(
                conn,
                "geo_city_region_cache",
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            )
            await conn.execute(
                "UPDATE geo_city_region_cache "
                "SET created_at = COALESCE(created_at, updated_at, CURRENT_TIMESTAMP) "
                "WHERE created_at IS NULL"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_geo_city_region_cache_created_at "
                "ON geo_city_region_cache(created_at)"
            )

            # Cache for optional VK wall.post location markers. The publisher is
            # fail-open: only confident Kaliningrad Oblast marker payloads are
            # reused; negative/ambiguous rows prevent repeated resolution work
            # for a bounded TTL in vk_location_marker.py.
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS vk_location_marker_cache(
                    query_norm TEXT PRIMARY KEY,
                    query_display TEXT,
                    display_title TEXT,
                    city TEXT,
                    is_kaliningrad_oblast BOOLEAN,
                    lat REAL,
                    long REAL,
                    place_id TEXT,
                    confidence REAL,
                    provenance TEXT,
                    status TEXT,
                    details JSON,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            for column_def in (
                "query_display TEXT",
                "display_title TEXT",
                "city TEXT",
                "is_kaliningrad_oblast BOOLEAN",
                "lat REAL",
                "long REAL",
                "place_id TEXT",
                "confidence REAL",
                "provenance TEXT",
                "status TEXT",
                "details JSON",
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            ):
                await _add_column(conn, "vk_location_marker_cache", column_def)
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_vk_location_marker_cache_updated_at "
                "ON vk_location_marker_cache(updated_at)"
            )

            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_festival_name ON festival(name)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_event_date ON event(date)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_event_end_date ON event(end_date)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_event_city ON event(city)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_event_type ON event(event_type)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_event_is_free ON event(is_free)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_date_city ON event(date, city)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_date_festival ON event(date, festival)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_content_hash ON event(content_hash)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_identity_status ON event(identity_status)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_merged_into_event ON event(merged_into_event_id)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_event_date_inferred ON event(date_is_inferred, date)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_event_date_time ON event(date, time)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_event_festival_date_time ON event(festival, date, time)"
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS videoannounce_session(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    status TEXT NOT NULL DEFAULT 'CREATED',
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    started_at TIMESTAMP,
                    published_at TIMESTAMP,
                    finished_at TIMESTAMP,
                    kaggle_dataset TEXT,
                    kaggle_kernel_ref TEXT,
                    error TEXT,
                    video_url TEXT
                )
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS promo_campaign(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'draft',
                    goal_comment TEXT,
                    starts_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    ends_at TIMESTAMP,
                    total_exposure_goal INTEGER,
                    daily_exposure_cap INTEGER,
                    priority INTEGER NOT NULL DEFAULT 2,
                    sponsorship_disclosure TEXT,
                    created_by BIGINT,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    archived_at TIMESTAMP
                )
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS promo_target(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    campaign_id INTEGER NOT NULL,
                    target_type TEXT NOT NULL,
                    event_id INTEGER,
                    festival_name TEXT,
                    query_text TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(campaign_id) REFERENCES promo_campaign(id) ON DELETE CASCADE,
                    FOREIGN KEY(event_id) REFERENCES event(id) ON DELETE CASCADE
                )
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS promo_activity(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    campaign_id INTEGER NOT NULL,
                    surface TEXT NOT NULL,
                    profile_key TEXT,
                    slot INTEGER,
                    max_per_publish INTEGER NOT NULL DEFAULT 1,
                    target_exposure_goal INTEGER,
                    daily_cap INTEGER,
                    selection_policy TEXT NOT NULL DEFAULT 'diverse_shuffle',
                    enabled BOOLEAN NOT NULL DEFAULT 1,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(campaign_id) REFERENCES promo_campaign(id) ON DELETE CASCADE
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_promo_campaign_status_dates ON promo_campaign(status, starts_at, ends_at)"
            )
            await _add_column(
                conn, "promo_campaign", "priority INTEGER NOT NULL DEFAULT 2"
            )
            await _add_column(
                conn, "promo_activity", "config_json JSON NOT NULL DEFAULT '{}'"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_promo_target_campaign ON promo_target(campaign_id)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_promo_target_event ON promo_target(event_id)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_promo_target_festival ON promo_target(festival_name)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_promo_activity_campaign ON promo_activity(campaign_id)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_promo_activity_surface_profile ON promo_activity(surface, profile_key, enabled)"
            )
            await _add_column(conn, "videoannounce_session", "profile_key TEXT")
            await _add_column(conn, "videoannounce_session", "selection_params JSON")
            await _add_column(conn, "videoannounce_session", "test_chat_id BIGINT")
            await _add_column(conn, "videoannounce_session", "main_chat_id BIGINT")
            await _add_column(conn, "videoannounce_session", "published_at TIMESTAMP")
            await _add_column(conn, "videoannounce_session", "kaggle_dataset TEXT")
            await _add_column(conn, "videoannounce_session", "kaggle_kernel_ref TEXT")
            await _add_column(conn, "videoannounce_session", "partner_track_id TEXT")
            await _add_column(conn, "videoannounce_session", "partner_story_id TEXT")
            await _add_column(
                conn, "videoannounce_session", "partner_story_connection_hash TEXT"
            )
            await _add_column(
                conn, "videoannounce_session", "partner_story_deleted_at TIMESTAMP"
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS videoannounce_item(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER NOT NULL,
                    event_id INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'PENDING',
                    position INTEGER NOT NULL DEFAULT 0,
                    error TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(session_id) REFERENCES videoannounce_session(id) ON DELETE CASCADE,
                    FOREIGN KEY(event_id) REFERENCES event(id) ON DELETE CASCADE,
                    UNIQUE(session_id, event_id)
                )
                """
            )
            await _add_column(conn, "videoannounce_item", "final_title TEXT")
            await _add_column(conn, "videoannounce_item", "final_about TEXT")
            await _add_column(conn, "videoannounce_item", "final_description TEXT")
            await _add_column(conn, "videoannounce_item", "poster_text TEXT")
            await _add_column(conn, "videoannounce_item", "poster_source TEXT")
            await _add_column(
                conn, "videoannounce_item", "use_ocr INTEGER NOT NULL DEFAULT 0"
            )
            await _add_column(conn, "videoannounce_item", "llm_score REAL")
            await _add_column(conn, "videoannounce_item", "llm_reason TEXT")
            await _add_column(
                conn,
                "videoannounce_item",
                "is_mandatory BOOLEAN NOT NULL DEFAULT 0",
            )
            await _add_column(
                conn, "videoannounce_item", "include_count INTEGER NOT NULL DEFAULT 0"
            )
            await _add_column(conn, "videoannounce_item", "promo_campaign_id INTEGER")
            await _add_column(conn, "videoannounce_item", "promo_activity_id INTEGER")
            await _add_column(conn, "videoannounce_item", "promo_placement_kind TEXT")
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS videoannounce_eventhit(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER NOT NULL,
                    event_id INTEGER NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(session_id) REFERENCES videoannounce_session(id) ON DELETE CASCADE,
                    FOREIGN KEY(event_id) REFERENCES event(id) ON DELETE CASCADE,
                    UNIQUE(session_id, event_id)
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_videoannounce_session_status_created_at ON videoannounce_session(status, created_at)"
            )
            await conn.execute(
                "DROP INDEX IF EXISTS ux_videoannounce_session_rendering"
            )
            await conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ux_videoannounce_session_rendering_profile
                ON videoannounce_session(COALESCE(profile_key, 'default'))
                WHERE status = 'RENDERING'
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_videoannounce_item_session ON videoannounce_item(session_id)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_videoannounce_item_event ON videoannounce_item(event_id)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_videoannounce_item_status ON videoannounce_item(status)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_videoannounce_eventhit_event ON videoannounce_eventhit(event_id)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_videoannounce_eventhit_session ON videoannounce_eventhit(session_id)"
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS videoannounce_llm_trace(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER,
                    stage TEXT NOT NULL,
                    model TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    response_json TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(session_id) REFERENCES videoannounce_session(id) ON DELETE SET NULL
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_videoannounce_llm_trace_session ON videoannounce_llm_trace(session_id)"
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS promo_exposure(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    campaign_id INTEGER NOT NULL,
                    activity_id INTEGER,
                    event_id INTEGER NOT NULL,
                    surface TEXT NOT NULL,
                    placement_kind TEXT NOT NULL,
                    video_session_id INTEGER,
                    video_item_id INTEGER,
                    position INTEGER,
                    publish_status TEXT NOT NULL,
                    public_target_count INTEGER NOT NULL DEFAULT 0,
                    public_targets_json JSON NOT NULL DEFAULT '[]',
                    period_start TIMESTAMP,
                    period_end TIMESTAMP,
                    published_at TIMESTAMP,
                    details_json JSON NOT NULL DEFAULT '{}',
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(campaign_id) REFERENCES promo_campaign(id),
                    FOREIGN KEY(activity_id) REFERENCES promo_activity(id),
                    FOREIGN KEY(event_id) REFERENCES event(id),
                    FOREIGN KEY(video_session_id) REFERENCES videoannounce_session(id),
                    FOREIGN KEY(video_item_id) REFERENCES videoannounce_item(id)
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_promo_exposure_campaign_published ON promo_exposure(campaign_id, published_at)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_promo_exposure_event_surface ON promo_exposure(event_id, surface, published_at)"
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS organization(
                    name TEXT PRIMARY KEY,
                    vk_source_group_ids JSON NOT NULL DEFAULT '[]',
                    video_profile_key TEXT,
                    sponsorship_default TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            # Idempotent seed for partner organizations referenced by the
            # partner-promo feature. The name must match what is stored in
            # ``user.organization`` for partner accounts — for КОНБ that is
            # "Научная библиотека" in production. Existing rows are not
            # overwritten.
            await conn.execute(
                """
                INSERT OR IGNORE INTO organization
                    (name, vk_source_group_ids, video_profile_key, sponsorship_default)
                VALUES
                    ('Научная библиотека', '[30777579]', 'konb',
                     'Партнёрский материал · Научная библиотека')
                """
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS promo_vk_repost_job(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    campaign_id INTEGER NOT NULL,
                    activity_id INTEGER NOT NULL,
                    event_id INTEGER NOT NULL,
                    scheduled_at TIMESTAMP NOT NULL,
                    source_owner_id INTEGER NOT NULL,
                    source_post_id INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    executed_at TIMESTAMP,
                    vk_post_id INTEGER,
                    error_json JSON NOT NULL DEFAULT '{}',
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(campaign_id) REFERENCES promo_campaign(id) ON DELETE CASCADE,
                    FOREIGN KEY(activity_id) REFERENCES promo_activity(id) ON DELETE CASCADE,
                    FOREIGN KEY(event_id) REFERENCES event(id) ON DELETE CASCADE
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_promo_vk_repost_job_pending ON promo_vk_repost_job(status, scheduled_at)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_promo_vk_repost_job_source ON promo_vk_repost_job(source_owner_id, source_post_id, executed_at)"
            )

            dbg("kaggle_status")
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS kaggle_run_ledger(
                    run_id TEXT PRIMARY KEY,
                    session_id INTEGER,
                    kind TEXT,
                    notebook TEXT,
                    kernel_ref TEXT,
                    dataset_ref TEXT,
                    status TEXT NOT NULL DEFAULT 'created',
                    phase TEXT,
                    token_hash TEXT NOT NULL,
                    progress_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    last_heartbeat_at TEXT,
                    terminal_at TEXT,
                    error TEXT
                )
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS kaggle_run_event(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    event_name TEXT NOT NULL,
                    phase TEXT,
                    status TEXT,
                    event_uid TEXT,
                    progress_json TEXT NOT NULL DEFAULT '{}',
                    message TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(run_id, seq)
                )
                """
            )
            await _add_column(conn, "kaggle_run_event", "event_uid TEXT")
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_kaggle_run_event_run_id ON kaggle_run_event(run_id, seq)"
            )
            await conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_kaggle_run_event_uid ON kaggle_run_event(run_id, event_uid) WHERE event_uid IS NOT NULL"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_kaggle_run_event_created_at ON kaggle_run_event(created_at)"
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS kaggle_resource_lease(
                    resource_key TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    holder_kind TEXT,
                    status TEXT NOT NULL DEFAULT 'active',
                    acquired_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    expires_at TEXT NOT NULL,
                    released_at TEXT,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_kaggle_resource_lease_status ON kaggle_resource_lease(status, expires_at)"
            )

            # Static site effect ledger.  The singleton state row is the
            # cross-process no-op/single-flight authority; history is append-only
            # operator evidence.  Both tables are additive for existing SQLite
            # production databases.
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS static_site_build_state(
                    release_channel TEXT PRIMARY KEY,
                    schema_version TEXT NOT NULL,
                    last_success_fingerprint TEXT,
                    last_success_run_id TEXT,
                    last_success_at TEXT,
                    last_success_receipt_json TEXT NOT NULL DEFAULT '{}',
                    active_claim_token TEXT,
                    active_job_id INTEGER,
                    active_run_id TEXT,
                    active_fingerprint TEXT,
                    active_effective_date TEXT,
                    active_claimed_at TEXT,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS static_site_build_history(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    release_channel TEXT NOT NULL,
                    job_id INTEGER,
                    request_watermark TEXT,
                    input_fingerprint TEXT NOT NULL,
                    effective_date TEXT NOT NULL,
                    force_rebuild INTEGER NOT NULL DEFAULT 0,
                    outcome TEXT NOT NULL,
                    run_id TEXT,
                    evidence_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_static_site_build_history_fingerprint "
                "ON static_site_build_history(input_fingerprint, outcome, created_at)"
            )

            dbg("llm_daily_request_budget")
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS llm_daily_request_budget(
                    budget_key TEXT NOT NULL,
                    day TEXT NOT NULL,
                    used INTEGER NOT NULL DEFAULT 0,
                    limit_value INTEGER NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(budget_key, day),
                    CHECK(used >= 0),
                    CHECK(limit_value >= 0)
                )
                """
            )

            # Interest-club identities are owner-curated. Schema bootstrap is
            # additive and deliberately does not seed or approve any identity.
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS interest_club(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    slug TEXT NOT NULL UNIQUE,
                    canonical_name TEXT NOT NULL,
                    topic TEXT,
                    description TEXT,
                    city TEXT,
                    typical_place TEXT,
                    public_status TEXT NOT NULL DEFAULT 'shadow',
                    identity_version INTEGER NOT NULL DEFAULT 1,
                    policy_version TEXT NOT NULL DEFAULT 'interest-club-relation-v1',
                    aliases_json JSON NOT NULL DEFAULT '[]',
                    source_anchors_json JSON NOT NULL DEFAULT '[]',
                    provenance_json JSON NOT NULL DEFAULT '{}',
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    CHECK(public_status IN ('shadow','approved','archived','merged'))
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_interest_club_public_status ON interest_club(public_status)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_interest_club_updated_at ON interest_club(updated_at)"
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS interest_club_event(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    club_id INTEGER NOT NULL,
                    event_id INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'active',
                    decision_lane TEXT NOT NULL,
                    evidence_quote TEXT,
                    evidence_json JSON NOT NULL DEFAULT '{}',
                    model TEXT,
                    policy_version TEXT NOT NULL DEFAULT 'interest-club-relation-v1',
                    input_hash TEXT NOT NULL,
                    evaluated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(club_id, event_id),
                    FOREIGN KEY(club_id) REFERENCES interest_club(id) ON DELETE CASCADE,
                    FOREIGN KEY(event_id) REFERENCES event(id) ON DELETE CASCADE,
                    CHECK(status IN ('active','deferred','review'))
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_interest_club_event_event_status ON interest_club_event(event_id,status)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_interest_club_event_club_status ON interest_club_event(club_id,status)"
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS interest_club_evaluation(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    club_id INTEGER NOT NULL,
                    event_id INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    verdict TEXT NOT NULL,
                    decision_lane TEXT NOT NULL,
                    evidence_quote TEXT,
                    evidence_json JSON NOT NULL DEFAULT '{}',
                    model TEXT,
                    policy_version TEXT NOT NULL DEFAULT 'interest-club-relation-v1',
                    input_hash TEXT NOT NULL,
                    error_code TEXT,
                    attempts INTEGER NOT NULL DEFAULT 1,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(club_id, event_id, policy_version, input_hash),
                    FOREIGN KEY(club_id) REFERENCES interest_club(id) ON DELETE CASCADE,
                    FOREIGN KEY(event_id) REFERENCES event(id) ON DELETE CASCADE,
                    CHECK(status IN ('accepted','no_match','review','deferred','ineligible'))
                )
                """
            )
            # Production bootstraps schema through Database.init(), not an
            # Alembic command.  Upgrade the original pair-unique evaluation
            # table in place so a changed source hash can retain decision
            # history instead of failing on UNIQUE(club_id,event_id).
            index_cursor = await conn.execute(
                "PRAGMA index_list('interest_club_evaluation')"
            )
            index_rows = await index_cursor.fetchall()
            await index_cursor.close()
            unique_column_sets: set[tuple[str, ...]] = set()
            for index_row in index_rows:
                if not bool(index_row[2]):
                    continue
                index_name = str(index_row[1]).replace("'", "''")
                info_cursor = await conn.execute(
                    f"PRAGMA index_info('{index_name}')"
                )
                info_rows = await info_cursor.fetchall()
                await info_cursor.close()
                unique_column_sets.add(
                    tuple(str(row[2]) for row in sorted(info_rows, key=lambda row: row[0]))
                )
            legacy_evaluation_unique = ("club_id", "event_id")
            history_evaluation_unique = (
                "club_id",
                "event_id",
                "policy_version",
                "input_hash",
            )
            if (
                legacy_evaluation_unique in unique_column_sets
                and history_evaluation_unique not in unique_column_sets
            ):
                before_cursor = await conn.execute(
                    "SELECT COUNT(*) FROM interest_club_evaluation"
                )
                before_count = int((await before_cursor.fetchone())[0])
                await before_cursor.close()
                await conn.execute("DROP TABLE IF EXISTS interest_club_evaluation_new")
                await conn.execute(
                    """
                    CREATE TABLE interest_club_evaluation_new(
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        club_id INTEGER NOT NULL,
                        event_id INTEGER NOT NULL,
                        status TEXT NOT NULL,
                        verdict TEXT NOT NULL,
                        decision_lane TEXT NOT NULL,
                        evidence_quote TEXT,
                        evidence_json JSON NOT NULL DEFAULT '{}',
                        model TEXT,
                        policy_version TEXT NOT NULL DEFAULT 'interest-club-relation-v1',
                        input_hash TEXT NOT NULL,
                        error_code TEXT,
                        attempts INTEGER NOT NULL DEFAULT 1,
                        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(club_id, event_id, policy_version, input_hash),
                        FOREIGN KEY(club_id) REFERENCES interest_club(id) ON DELETE CASCADE,
                        FOREIGN KEY(event_id) REFERENCES event(id) ON DELETE CASCADE,
                        CHECK(status IN ('accepted','no_match','review','deferred','ineligible'))
                    )
                    """
                )
                await conn.execute(
                    """
                    INSERT INTO interest_club_evaluation_new(
                        id, club_id, event_id, status, verdict, decision_lane,
                        evidence_quote, evidence_json, model, policy_version,
                        input_hash, error_code, attempts, created_at, updated_at
                    )
                    SELECT
                        id, club_id, event_id, status, verdict, decision_lane,
                        evidence_quote, evidence_json, model, policy_version,
                        input_hash, error_code, attempts, created_at, updated_at
                    FROM interest_club_evaluation
                    """
                )
                after_cursor = await conn.execute(
                    "SELECT COUNT(*) FROM interest_club_evaluation_new"
                )
                after_count = int((await after_cursor.fetchone())[0])
                await after_cursor.close()
                if before_count != after_count:
                    raise RuntimeError(
                        "interest_club_evaluation migration row-count mismatch: "
                        f"{before_count}!={after_count}"
                    )
                await conn.execute("DROP TABLE interest_club_evaluation")
                await conn.execute(
                    "ALTER TABLE interest_club_evaluation_new "
                    "RENAME TO interest_club_evaluation"
                )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_interest_club_evaluation_status ON interest_club_evaluation(status,updated_at)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_interest_club_evaluation_event ON interest_club_evaluation(event_id)"
            )

            await conn.commit()

    async def _ensure_conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            self._conn = await aiosqlite.connect(self.path, timeout=self._sqlite_timeout_sec())
            await self._apply_sqlite_pragmas(self._conn)
        return self._conn

    @asynccontextmanager
    async def raw_conn(self):
        conn = await self._ensure_conn()
        yield conn

    @asynccontextmanager
    async def get_session(self):
        from sqlalchemy.ext.asyncio import AsyncSession
        from sqlalchemy.orm import sessionmaker

        if self._orm_engine is None:
            self._orm_engine = self._create_orm_engine()
        if self._sessionmaker is None:
            self._sessionmaker = sessionmaker(
                self._orm_engine, expire_on_commit=False, class_=AsyncSession
            )
        async with self._sessionmaker() as session:
            yield session

    @property
    def engine(self):
        if self._orm_engine is None:
            self._orm_engine = self._create_orm_engine()
        return self._orm_engine

    async def exec_driver_sql(
        self, sql: str, params: tuple | dict | None = None
    ):
        async with self.engine.begin() as conn:  # type: AsyncConnection
            result = await conn.exec_driver_sql(sql, params or ())
            try:
                return result.fetchall()
            except Exception:
                return []


async def close_known_databases() -> None:
    for db in list(_KNOWN_DATABASES):
        try:
            await db.close()
        except Exception:
            logging.exception("db.close failed for %s", getattr(db, "path", None))
    _KNOWN_DATABASES.clear()


async def wal_checkpoint_truncate(engine):
    async with engine.begin() as conn:
        result = await conn.exec_driver_sql("PRAGMA wal_checkpoint(TRUNCATE)")
        rows = result.fetchall()
    logging.info("db_checkpoint result=%s", rows)
    return rows


async def optimize(engine):
    async with engine.begin() as conn:
        await conn.exec_driver_sql("PRAGMA optimize")


async def vacuum(engine):
    async with engine.begin() as conn:
        await conn.exec_driver_sql("VACUUM")


def _checkpoint_result(rows) -> dict[str, int | bool | list]:
    normalized = [tuple(int(value) for value in row) for row in rows]
    first = normalized[0] if normalized else None
    return {
        "rows": normalized,
        "busy": int(first[0]) if first and len(first) >= 1 else -1,
        "log_frames": int(first[1]) if first and len(first) >= 2 else -1,
        "checkpointed_frames": int(first[2]) if first and len(first) >= 3 else -1,
        "ok": bool(first and len(first) >= 3 and first[0] == 0),
    }


def _db_file_sizes(db_path: str) -> dict[str, int]:
    def _size(path: str) -> int:
        try:
            return int(os.path.getsize(path))
        except OSError:
            return 0

    return {
        "db_bytes": _size(db_path),
        "wal_bytes": _size(f"{db_path}-wal"),
        "shm_bytes": _size(f"{db_path}-shm"),
    }


async def full_vacuum_with_safety(
    engine,
    db_path: str,
    *,
    min_free_bytes: int,
) -> dict[str, object]:
    """Run a full VACUUM only with capacity and checkpoint safety receipts."""

    started = time.perf_counter()
    before = _db_file_sizes(db_path)
    db_dir = os.path.dirname(os.path.abspath(db_path)) or "."
    free_before = int(shutil.disk_usage(db_dir).free)
    required_free = 2 * int(before["db_bytes"]) + max(0, int(min_free_bytes))
    receipt: dict[str, object] = {
        "operation": "full_vacuum",
        "status": "pending",
        "before": before,
        "free_before_bytes": free_before,
        "required_free_bytes": required_free,
    }
    if free_before < required_free:
        receipt.update(status="skipped", reason="insufficient_capacity")
        logging.warning("db_full_vacuum result=%s", receipt)
        return receipt

    pre = _checkpoint_result(await wal_checkpoint_truncate(engine))
    receipt["checkpoint_pre"] = pre
    if not pre["ok"]:
        receipt.update(status="skipped", reason="checkpoint_pre_busy")
        logging.warning("db_full_vacuum result=%s", receipt)
        return receipt

    free_after_pre = int(shutil.disk_usage(db_dir).free)
    receipt["free_after_checkpoint_bytes"] = free_after_pre
    if free_after_pre < required_free:
        receipt.update(status="skipped", reason="capacity_changed_after_checkpoint")
        logging.warning("db_full_vacuum result=%s", receipt)
        return receipt

    try:
        await vacuum(engine)
        receipt["status"] = "success"
    except BaseException as exc:
        receipt.update(
            status="error",
            reason="vacuum_failed",
            error_type=type(exc).__name__,
        )
        raise
    finally:
        try:
            post_receipt = _checkpoint_result(
                await wal_checkpoint_truncate(engine)
            )
            receipt["checkpoint_post"] = post_receipt
            if receipt.get("status") == "success" and not post_receipt["ok"]:
                receipt.update(status="error", reason="checkpoint_post_busy")
        except BaseException as checkpoint_exc:
            receipt["checkpoint_post"] = {
                "ok": False,
                "error_type": type(checkpoint_exc).__name__,
            }
            if receipt.get("status") == "success":
                receipt.update(status="error", reason="checkpoint_post_failed")
                raise
        finally:
            receipt["after"] = _db_file_sizes(db_path)
            receipt["free_after_bytes"] = int(shutil.disk_usage(db_dir).free)
            receipt["duration_ms"] = round((time.perf_counter() - started) * 1000)
            log = (
                logging.info
                if receipt.get("status") == "success"
                else logging.warning
            )
            log("db_full_vacuum result=%s", receipt)

    post = receipt.get("checkpoint_post")
    if isinstance(post, dict) and not post.get("ok"):
        raise RuntimeError(f"post-VACUUM WAL checkpoint did not complete: {post}")
    return receipt
