from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional
from dataclasses import dataclass

from sqlmodel import Field, SQLModel
from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Index,
    JSON,
    Boolean,
    Integer,
    SmallInteger,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.types import Enum as SAEnum


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


TOPIC_LABELS: dict[str, str] = {
    "STANDUP": "Стендап и комедия",
    "QUIZ_GAMES": "Квизы и игры",
    "OPEN_AIR": "Фестивали и open-air",
    "PARTIES": "Вечеринки",
    "CONCERTS": "Концерты",
    "MOVIES": "Кино",
    "EXHIBITIONS": "Выставки и арт",
    "THEATRE": "Театр",
    "THEATRE_CLASSIC": "Классический театр и драма",
    "THEATRE_MODERN": "Современный и экспериментальный театр",
    "LECTURES": "Лекции и встречи",
    "MASTERCLASS": "Мастер-классы",
    "PSYCHOLOGY": "Психология",
    "SCIENCE_POP": "Научпоп",
    "HANDMADE": "Хендмейд/маркеты/ярмарки/МК",
    "FASHION": "Мода и стиль",
    "NETWORKING": "Нетворкинг и карьера",
    "ACTIVE": "Активный отдых и спорт",
    "PERSONALITIES": "Личности и встречи",
    "HISTORICAL_IMMERSION": "Исторические реконструкции и погружение",
    "KIDS_SCHOOL": "Дети и школа",
    "FAMILY": "Семейные события",
    "URBANISM": "Урбанистика",
    "KRAEVEDENIE_KALININGRAD_OBLAST": "Краеведение Калининградской области",
}

TOPIC_IDENTIFIERS: set[str] = set(TOPIC_LABELS.keys())

_TOPIC_LEGACY_ALIASES: dict[str, str] = {
    "art": "EXHIBITIONS",
    "искусство": "EXHIBITIONS",
    "культура": "EXHIBITIONS",
    "выставка": "EXHIBITIONS",
    "выставки": "EXHIBITIONS",
    "gallery": "EXHIBITIONS",
    "галерея": "EXHIBITIONS",
    "ART": "EXHIBITIONS",
    "history_ru": "LECTURES",
    "HISTORY_RU": "LECTURES",
    "history": "LECTURES",
    "история": "LECTURES",
    "история россии": "LECTURES",
    "лекция": "LECTURES",
    "лекции": "LECTURES",
    "встреча": "LECTURES",
    "встречи": "LECTURES",
    "дискуссия": "LECTURES",
    "BUSINESS": "LECTURES",
    "business": "LECTURES",
    "предпринимательство": "LECTURES",
    "urbanism": "URBANISM",
    "урбанистика": "URBANISM",
    "урбанистический": "URBANISM",
    "краеведение": "KRAEVEDENIE_KALININGRAD_OBLAST",
    "краевед": "KRAEVEDENIE_KALININGRAD_OBLAST",
    "краеведческий": "KRAEVEDENIE_KALININGRAD_OBLAST",
    "краеведческие": "KRAEVEDENIE_KALININGRAD_OBLAST",
    "калининград": "KRAEVEDENIE_KALININGRAD_OBLAST",
    "kaliningrad": "KRAEVEDENIE_KALININGRAD_OBLAST",
    "калининградская область": "KRAEVEDENIE_KALININGRAD_OBLAST",
    "калининградской области": "KRAEVEDENIE_KALININGRAD_OBLAST",
    "кёнигсберг": "KRAEVEDENIE_KALININGRAD_OBLAST",
    "кенигсберг": "KRAEVEDENIE_KALININGRAD_OBLAST",
    "kenigsberg": "KRAEVEDENIE_KALININGRAD_OBLAST",
    "koenigsberg": "KRAEVEDENIE_KALININGRAD_OBLAST",
    "konigsberg": "KRAEVEDENIE_KALININGRAD_OBLAST",
    "königsberg": "KRAEVEDENIE_KALININGRAD_OBLAST",
    "kenig": "KRAEVEDENIE_KALININGRAD_OBLAST",
    "янтарный край": "KRAEVEDENIE_KALININGRAD_OBLAST",
    "янтарного края": "KRAEVEDENIE_KALININGRAD_OBLAST",
    "39 регион": "KRAEVEDENIE_KALININGRAD_OBLAST",
    "39-й регион": "KRAEVEDENIE_KALININGRAD_OBLAST",
    "39й регион": "KRAEVEDENIE_KALININGRAD_OBLAST",
    "39йрегион": "KRAEVEDENIE_KALININGRAD_OBLAST",
    "LITERATURE": "LECTURES",
    "literature": "LECTURES",
    "книги": "LECTURES",
    "TECH": "SCIENCE_POP",
    "tech": "SCIENCE_POP",
    "технологии": "SCIENCE_POP",
    "ит": "SCIENCE_POP",
    "психология": "PSYCHOLOGY",
    "psychology": "PSYCHOLOGY",
    "mental health": "PSYCHOLOGY",
    "science": "SCIENCE_POP",
    "science_pop": "SCIENCE_POP",
    "научпоп": "SCIENCE_POP",
    "CINEMA": "MOVIES",
    "cinema": "MOVIES",
    "кино": "MOVIES",
    "фильм": "MOVIES",
    "фильмы": "MOVIES",
    "movie": "MOVIES",
    "movies": "MOVIES",
    "MUSIC": "CONCERTS",
    "music": "CONCERTS",
    "музыка": "CONCERTS",
    "концерт": "CONCERTS",
    "концерты": "CONCERTS",
    "PARTY": "PARTIES",
    "party": "PARTIES",
    "вечеринка": "PARTIES",
    "вечер": "PARTIES",
    "вечеринки": "PARTIES",
    "STANDUP": "STANDUP",
    "standup": "STANDUP",
    "стендап": "STANDUP",
    "стендапы": "STANDUP",
    "комедия": "STANDUP",
    "quiz": "QUIZ_GAMES",
    "quizzes": "QUIZ_GAMES",
    "квиз": "QUIZ_GAMES",
    "квизы": "QUIZ_GAMES",
    "игры": "QUIZ_GAMES",
    "настолки": "QUIZ_GAMES",
    "настольные игры": "QUIZ_GAMES",
    "open_air": "OPEN_AIR",
    "open air": "OPEN_AIR",
    "open-air": "OPEN_AIR",
    "openair": "OPEN_AIR",
    "фестиваль": "OPEN_AIR",
    "фестивали": "OPEN_AIR",
    "мастер-класс": "MASTERCLASS",
    "мастер класс": "MASTERCLASS",
    "мастер-классы": "MASTERCLASS",
    "воркшоп": "MASTERCLASS",
    "workshop": "MASTERCLASS",
    "workshops": "MASTERCLASS",
    "театр": "THEATRE",
    "спектакль": "THEATRE",
    "спектакли": "THEATRE",
    "performance": "THEATRE",
    "performances": "THEATRE",
    "классический спектакль": "THEATRE_CLASSIC",
    "классический театр": "THEATRE_CLASSIC",
    "classic theatre": "THEATRE_CLASSIC",
    "драма": "THEATRE_CLASSIC",
    "драмы": "THEATRE_CLASSIC",
    "драматический театр": "THEATRE_CLASSIC",
    "dramatic theatre": "THEATRE_CLASSIC",
    "классика": "THEATRE_CLASSIC",
    "современный театр": "THEATRE_MODERN",
    "современные спектакли": "THEATRE_MODERN",
    "модерн": "THEATRE_MODERN",
    "экспериментальный театр": "THEATRE_MODERN",
    "experimental theatre": "THEATRE_MODERN",
    "modern theatre": "THEATRE_MODERN",
    "HANDMADE": "HANDMADE",
    "handmade": "HANDMADE",
    "hand-made": "HANDMADE",
    "маркет": "HANDMADE",
    "маркеты": "HANDMADE",
    "маркет-плейс": "HANDMADE",
    "маркетплейс": "HANDMADE",
    "маркетплейсы": "HANDMADE",
    "ярмарка": "HANDMADE",
    "ярмарки": "HANDMADE",
    "ярмарка выходного дня": "HANDMADE",
    "хендмейд": "HANDMADE",
    "HAND-MADE": "HANDMADE",
    "FASHION": "FASHION",
    "fashion": "FASHION",
    "fashion week": "FASHION",
    "показ мод": "FASHION",
    "показы мод": "FASHION",
    "fashion show": "FASHION",
    "fashion shows": "FASHION",
    "styling": "FASHION",
    "stylist": "FASHION",
    "style": "FASHION",
    "стиль": "FASHION",
    "стилист": "FASHION",
    "стилисты": "FASHION",
    "стилизация": "FASHION",
    "мода": "FASHION",
    "модный показ": "FASHION",
    "модные показы": "FASHION",
    "модный дом": "FASHION",
    "NETWORKING": "NETWORKING",
    "networking": "NETWORKING",
    "network": "NETWORKING",
    "нетворкинг": "NETWORKING",
    "нетворк": "NETWORKING",
    "знакомства": "NETWORKING",
    "карьера": "NETWORKING",
    "деловые встречи": "NETWORKING",
    "бизнес-завтрак": "NETWORKING",
    "бизнес завтрак": "NETWORKING",
    "business breakfast": "NETWORKING",
    "карьерный вечер": "NETWORKING",
    "ACTIVE": "ACTIVE",
    "active": "ACTIVE",
    "sport": "ACTIVE",
    "sports": "ACTIVE",
    "спорт": "ACTIVE",
    "спортивные": "ACTIVE",
    "спортзал": "ACTIVE",
    "активности": "ACTIVE",
    "активность": "ACTIVE",
    "активный отдых": "ACTIVE",
    "фитнес": "ACTIVE",
    "йога": "ACTIVE",
    "yoga": "ACTIVE",
    "пробежка": "ACTIVE",
    "PERSONALITIES": "PERSONALITIES",
    "personalities": "PERSONALITIES",
    "personality": "PERSONALITIES",
    "персоны": "PERSONALITIES",
    "личности": "PERSONALITIES",
    "встреча с автором": "PERSONALITIES",
    "встреча с героем": "PERSONALITIES",
    "встреча с артистом": "PERSONALITIES",
    "встреча с персонами": "PERSONALITIES",
    "книжный клуб": "PERSONALITIES",
    "книжные клубы": "PERSONALITIES",
    "book club": "PERSONALITIES",
    "реконструкция": "HISTORICAL_IMMERSION",
    "реконструкции": "HISTORICAL_IMMERSION",
    "историческое погружение": "HISTORICAL_IMMERSION",
    "исторические костюмы": "HISTORICAL_IMMERSION",
    "викинги": "HISTORICAL_IMMERSION",
    "средневековье": "HISTORICAL_IMMERSION",
    "KIDS_SCHOOL": "KIDS_SCHOOL",
    "kids_school": "KIDS_SCHOOL",
    "kids": "KIDS_SCHOOL",
    "дети": "KIDS_SCHOOL",
    "детям": "KIDS_SCHOOL",
    "детский": "KIDS_SCHOOL",
    "детские": "KIDS_SCHOOL",
    "школа": "KIDS_SCHOOL",
    "школьники": "KIDS_SCHOOL",
    "образование": "KIDS_SCHOOL",
    "FAMILY": "FAMILY",
    "family": "FAMILY",
    "семья": "FAMILY",
    "семейные": "FAMILY",
    "семейный": "FAMILY",
    "для всей семьи": "FAMILY",
}

TOPIC_IDENTIFIERS_BY_CASEFOLD: dict[str, str] = {
    key.casefold(): key for key in TOPIC_IDENTIFIERS
}
TOPIC_IDENTIFIERS_BY_CASEFOLD.update(
    {alias.casefold(): canonical for alias, canonical in _TOPIC_LEGACY_ALIASES.items()}
)


def normalize_topic_identifier(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    if not candidate:
        return None
    if candidate in TOPIC_IDENTIFIERS:
        return candidate
    return TOPIC_IDENTIFIERS_BY_CASEFOLD.get(candidate.casefold())


class User(SQLModel, table=True):
    user_id: int = Field(primary_key=True)
    username: Optional[str] = None
    is_superadmin: bool = False
    is_partner: bool = False
    organization: Optional[str] = None
    location: Optional[str] = None
    blocked: bool = False
    last_partner_reminder: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )


class Organization(SQLModel, table=True):
    __tablename__ = "organization"

    name: str = Field(primary_key=True)
    vk_source_group_ids: list[int] = Field(
        default_factory=list, sa_column=Column(JSON)
    )
    video_profile_key: Optional[str] = None
    sponsorship_default: Optional[str] = None
    created_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    updated_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )


class PendingUser(SQLModel, table=True):
    user_id: int = Field(primary_key=True)
    username: Optional[str] = None
    requested_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )


class RejectedUser(SQLModel, table=True):
    user_id: int = Field(primary_key=True)
    username: Optional[str] = None
    rejected_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )


class Channel(SQLModel, table=True):
    channel_id: int = Field(primary_key=True)
    title: Optional[str] = None
    username: Optional[str] = None
    is_admin: bool = False
    is_registered: bool = False
    is_asset: bool = False
    daily_time: Optional[str] = None
    last_daily: Optional[str] = None


class Setting(SQLModel, table=True):
    key: str = Field(primary_key=True)
    value: str


class SupabaseDeleteQueue(SQLModel, table=True):
    __tablename__ = "supabase_delete_queue"
    __table_args__ = (
        Index("ix_supabase_delete_queue_created_at", "created_at"),
        UniqueConstraint("bucket", "path", name="ux_supabase_delete_queue_bucket_path"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    bucket: str
    path: str
    created_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    last_attempt_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    attempts: int = 0
    last_error: Optional[str] = None


class Event(SQLModel, table=True):
    __table_args__ = (
        Index("idx_event_date", "date"),
        Index("idx_event_end_date", "end_date"),
        Index("idx_event_city", "city"),
        Index("idx_event_type", "event_type"),
        Index("idx_event_is_free", "is_free"),
        Index("ix_event_date_city", "date", "city"),
        Index("ix_event_date_festival", "date", "festival"),
        Index("ix_event_content_hash", "content_hash"),
        Index("ix_event_identity_status", "identity_status"),
        Index("ix_event_merged_into_event", "merged_into_event_id"),
        Index("ix_event_date_inferred", "date_is_inferred", "date"),
        Index(
            "ix_event_telegraph_not_null",
            "date",
            sqlite_where=text("telegraph_url IS NOT NULL"),
        ),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    title: str
    description: str
    short_description: Optional[str] = None
    search_digest: Optional[str] = None
    festival: Optional[str] = None
    date: str
    time: str
    time_is_default: bool = False
    location_name: str
    location_address: Optional[str] = None
    city: Optional[str] = None
    ticket_price_min: Optional[int] = None
    ticket_price_max: Optional[int] = None
    ticket_link: Optional[str] = None
    vk_ticket_short_url: Optional[str] = None
    vk_ticket_short_key: Optional[str] = None
    vk_ics_short_url: Optional[str] = None
    vk_ics_short_key: Optional[str] = None
    ticket_trust_level: Optional[str] = None
    event_type: Optional[str] = None
    emoji: Optional[str] = None
    end_date: Optional[str] = None
    end_date_is_inferred: bool = False
    # LLM-estimated duration used only for transport planning when no explicit
    # source duration/end was extracted. Public event timing remains unchanged.
    duration_forecast_minutes: Optional[int] = None
    identity_status: str = "canonical"
    merged_into_event_id: Optional[int] = Field(default=None, foreign_key="event.id")
    date_is_inferred: bool = False
    date_provenance: Optional[str] = None
    date_confidence: Optional[float] = None
    end_date_provenance: Optional[str] = None
    end_date_confidence: Optional[float] = None
    is_free: bool = False
    pushkin_card: bool = False
    silent: bool = False
    lifecycle_status: str = "active"  # active|cancelled|postponed
    telegraph_path: Optional[str] = None
    source_text: str
    source_texts: list[str] = Field(default_factory=list, sa_column=Column(JSON))
    # Versioned, source-bound factual decisions used by exact static collections.
    # Keep this nullable for old rows/snapshots; callers must reassign the whole
    # mapping after a deep merge so SQLAlchemy observes JSON changes reliably.
    collection_decisions: Optional[dict[str, Any]] = Field(
        default=None, sa_column=Column(JSON, nullable=True)
    )
    # Source-grounded organizations responsible for this concrete event.
    # A publisher is not an organizer without an explicit curated source
    # binding or quoted event-local LLM evidence.
    organizer_names: list[str] = Field(default_factory=list, sa_column=Column(JSON))
    telegraph_url: Optional[str] = None
    ics_url: Optional[str] = None
    source_post_url: Optional[str] = None
    source_vk_post_url: Optional[str] = None
    # Hash of the latest VK source-post payload (used to avoid redundant wall edits).
    # Kept separate from `content_hash` (Telegraph HTML hash).
    vk_source_hash: Optional[str] = None
    vk_repost_url: Optional[str] = None
    tg_event_post_url: Optional[str] = None
    tg_event_post_id: Optional[int] = None
    tg_event_post_mode: Optional[str] = None
    tg_event_source_hash: Optional[str] = None
    ics_hash: Optional[str] = None
    ics_file_id: Optional[str] = None
    ics_post_hash: Optional[str] = None
    ics_updated_at: Optional[datetime] = None
    ics_post_url: Optional[str] = None
    ics_post_id: Optional[int] = None
    source_chat_id: Optional[int] = None
    source_message_id: Optional[int] = None
    # Telegram chat post author username (lowercased, no @). Set only for
    # group/supergroup sources where the post was sent by a user — used by the
    # author-in-chat promo trigger. None for channels and non-Telegram sources.
    tg_source_author: Optional[str] = None
    creator_id: Optional[int] = None
    tourist_label: Optional[int] = Field(
        default=None, sa_column=Column(SmallInteger)
    )
    tourist_factors: list[str] = Field(
        default_factory=list, sa_column=Column(JSON)
    )
    tourist_note: Optional[str] = None
    tourist_label_by: Optional[int] = None
    tourist_label_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    tourist_label_source: Optional[str] = None
    photo_urls: list[str] = Field(default_factory=list, sa_column=Column(JSON))
    photo_count: int = 0
    video_include_count: int = 0
    topics: list[str] = Field(default_factory=list, sa_column=Column(JSON))
    topics_manual: bool = False
    added_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    content_hash: Optional[str] = None
    ticket_status: Optional[str] = None  # 'available', 'sold_out', or None/unknown
    # Official source-declared restriction. Automatic product assessment is kept
    # in separate fields and is not public by default.
    age_restriction: Optional[str] = None
    age_restriction_status: str = "unknown"
    age_restriction_provenance: Optional[str] = None
    age_restriction_source_url: Optional[str] = None
    age_restriction_confidence: Optional[float] = None
    age_restriction_evidence: dict = Field(default_factory=dict, sa_column=Column(JSON))
    age_restriction_decision_version: Optional[str] = None
    age_restriction_input_hash: Optional[str] = None
    age_restriction_updated_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    age_assessment: Optional[str] = None
    age_assessment_status: str = "not_scheduled"
    age_assessment_provenance: Optional[str] = None
    age_assessment_confidence: Optional[float] = None
    age_assessment_evidence: dict = Field(default_factory=dict, sa_column=Column(JSON))
    age_assessment_decision_version: Optional[str] = None
    age_assessment_input_hash: Optional[str] = None
    age_assessment_engine: Optional[str] = None
    age_assessment_run_id: Optional[str] = None
    age_assessment_updated_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    linked_event_ids: list[int] = Field(default_factory=list, sa_column=Column(JSON))
    preview_3d_url: Optional[str] = None  # 3D preview generated by Blender on Kaggle


class PromoCampaign(SQLModel, table=True):
    __tablename__ = "promo_campaign"
    __table_args__ = (
        Index("ix_promo_campaign_status_dates", "status", "starts_at", "ends_at"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    title: str
    status: str = "draft"
    goal_comment: Optional[str] = None
    starts_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    ends_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    total_exposure_goal: Optional[int] = None
    daily_exposure_cap: Optional[int] = None
    priority: int = Field(default=2, sa_column=Column(SmallInteger, default=2))
    sponsorship_disclosure: Optional[str] = None
    created_by: Optional[int] = None
    created_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    updated_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    archived_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )


class PromoTarget(SQLModel, table=True):
    __tablename__ = "promo_target"
    __table_args__ = (
        Index("ix_promo_target_campaign", "campaign_id"),
        Index("ix_promo_target_event", "event_id"),
        Index("ix_promo_target_festival", "festival_name"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    campaign_id: int = Field(foreign_key="promo_campaign.id")
    target_type: str
    event_id: Optional[int] = Field(default=None, foreign_key="event.id")
    festival_name: Optional[str] = None
    query_text: Optional[str] = None
    created_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )


class PromoActivity(SQLModel, table=True):
    __tablename__ = "promo_activity"
    __table_args__ = (
        Index("ix_promo_activity_campaign", "campaign_id"),
        Index("ix_promo_activity_surface_profile", "surface", "profile_key", "enabled"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    campaign_id: int = Field(foreign_key="promo_campaign.id")
    surface: str
    profile_key: Optional[str] = None
    slot: Optional[int] = None
    max_per_publish: int = 1
    target_exposure_goal: Optional[int] = None
    daily_cap: Optional[int] = None
    selection_policy: str = "diverse_shuffle"
    config_json: dict = Field(default_factory=dict, sa_column=Column(JSON))
    enabled: bool = Field(default=True, sa_column=Column(Boolean, default=True))
    created_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )


class PromoExposure(SQLModel, table=True):
    __tablename__ = "promo_exposure"
    __table_args__ = (
        Index("ix_promo_exposure_campaign_published", "campaign_id", "published_at"),
        Index("ix_promo_exposure_event_surface", "event_id", "surface", "published_at"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    campaign_id: int = Field(foreign_key="promo_campaign.id")
    activity_id: Optional[int] = Field(default=None, foreign_key="promo_activity.id")
    event_id: int = Field(foreign_key="event.id")
    surface: str
    placement_kind: str
    video_session_id: Optional[int] = Field(default=None, foreign_key="videoannounce_session.id")
    video_item_id: Optional[int] = Field(default=None, foreign_key="videoannounce_item.id")
    position: Optional[int] = None
    publish_status: str
    public_target_count: int = 0
    public_targets_json: list[dict] = Field(default_factory=list, sa_column=Column(JSON))
    period_start: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    period_end: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    published_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    details_json: dict = Field(default_factory=dict, sa_column=Column(JSON))
    created_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )


class PromoVkRepostJob(SQLModel, table=True):
    __tablename__ = "promo_vk_repost_job"
    __table_args__ = (
        Index(
            "ix_promo_vk_repost_job_pending",
            "status",
            "scheduled_at",
        ),
        Index(
            "ix_promo_vk_repost_job_source",
            "source_owner_id",
            "source_post_id",
            "executed_at",
        ),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    campaign_id: int = Field(foreign_key="promo_campaign.id")
    activity_id: int = Field(foreign_key="promo_activity.id")
    event_id: int = Field(foreign_key="event.id")
    scheduled_at: datetime = Field(sa_column=Column(DateTime(timezone=True)))
    source_owner_id: int
    source_post_id: int
    status: str = "pending"
    attempts: int = 0
    executed_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    vk_post_id: Optional[int] = None
    error_json: dict = Field(default_factory=dict, sa_column=Column(JSON))
    created_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    updated_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )


class VideoAnnounceSessionStatus(str, Enum):
    CREATED = "CREATED"
    SELECTED = "SELECTED"
    RENDERING = "RENDERING"
    DONE = "DONE"
    FAILED = "FAILED"
    PUBLISH_BLOCKED = "PUBLISH_BLOCKED"
    PUBLISHED_TEST = "PUBLISHED_TEST"
    PUBLISHED_MAIN = "PUBLISHED_MAIN"


class VideoAnnounceSession(SQLModel, table=True):
    __tablename__ = "videoannounce_session"
    __table_args__ = (
        Index("ix_videoannounce_session_status_created_at", "status", "created_at"),
        Index(
            "ux_videoannounce_session_rendering_profile",
            text("COALESCE(profile_key, 'default')"),
            unique=True,
            sqlite_where=text("status = 'RENDERING'"),
        ),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    status: VideoAnnounceSessionStatus = Field(
        default=VideoAnnounceSessionStatus.CREATED,
        sa_column=Column(SAEnum(VideoAnnounceSessionStatus)),
    )
    profile_key: Optional[str] = None
    selection_params: dict | None = Field(default=None, sa_column=Column(JSON))
    created_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    started_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    finished_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    published_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    test_chat_id: Optional[int] = None
    main_chat_id: Optional[int] = None
    kaggle_dataset: Optional[str] = None
    kaggle_kernel_ref: Optional[str] = None
    error: Optional[str] = None
    video_url: Optional[str] = None
    partner_track_id: Optional[str] = None
    partner_story_id: Optional[str] = None
    partner_story_connection_hash: Optional[str] = None
    partner_story_deleted_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )


class VideoAnnounceItemStatus(str, Enum):
    PENDING = "PENDING"
    READY = "READY"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class VideoAnnounceItem(SQLModel, table=True):
    __tablename__ = "videoannounce_item"
    __table_args__ = (
        Index("ix_videoannounce_item_session", "session_id"),
        Index("ix_videoannounce_item_event", "event_id"),
        Index("ix_videoannounce_item_status", "status"),
        Index(
            "ux_videoannounce_item_session_event",
            "session_id",
            "event_id",
            unique=True,
        ),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    session_id: int = Field(foreign_key="videoannounce_session.id")
    event_id: int = Field(foreign_key="event.id")
    status: VideoAnnounceItemStatus = Field(
        default=VideoAnnounceItemStatus.PENDING,
        sa_column=Column(SAEnum(VideoAnnounceItemStatus)),
    )
    position: int = 0
    final_title: Optional[str] = None
    final_about: Optional[str] = None
    final_description: Optional[str] = None
    poster_text: Optional[str] = None
    poster_source: Optional[str] = None
    use_ocr: bool = False
    llm_score: Optional[float] = None
    llm_reason: Optional[str] = None
    is_mandatory: bool = Field(default=False, sa_column=Column(Boolean, default=False))
    include_count: int = Field(default=0, sa_column=Column(Integer, default=0))
    promo_campaign_id: Optional[int] = Field(default=None, foreign_key="promo_campaign.id")
    promo_activity_id: Optional[int] = Field(default=None, foreign_key="promo_activity.id")
    promo_placement_kind: Optional[str] = None
    error: Optional[str] = None
    created_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )


class VideoAnnounceEventHit(SQLModel, table=True):
    __tablename__ = "videoannounce_eventhit"
    __table_args__ = (
        Index("ix_videoannounce_eventhit_event", "event_id"),
        Index("ix_videoannounce_eventhit_session", "session_id"),
        Index(
            "ux_videoannounce_eventhit_session_event",
            "session_id",
            "event_id",
            unique=True,
        ),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    session_id: int = Field(foreign_key="videoannounce_session.id")
    event_id: int = Field(foreign_key="event.id")
    created_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )


class VideoAnnounceLLMTrace(SQLModel, table=True):
    __tablename__ = "videoannounce_llm_trace"
    id: Optional[int] = Field(default=None, primary_key=True)
    session_id: Optional[int] = Field(default=None, foreign_key="videoannounce_session.id")
    stage: str
    model: str
    request_json: str
    response_json: str
    created_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )


class EventPoster(SQLModel, table=True):
    __table_args__ = (
        Index("ix_eventposter_event", "event_id"),
        Index("ix_eventposter_phash", "phash"),
        Index("ix_eventposter_review_status", "event_id", "review_status"),
        Index("ix_eventposter_raw_sha256", "event_id", "raw_sha256"),
        Index("ix_eventposter_pixel_sha256", "event_id", "pixel_sha256"),
        Index("ix_eventposter_pixel_sha256_global", "pixel_sha256"),
        Index("ix_eventposter_image_geometry", "image_geometry_id"),
        Index("ix_eventposter_media_semantic", "event_id", "media_semantic_status", "media_role"),
        UniqueConstraint("event_id", "poster_hash", name="ux_eventposter_event_hash"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    event_id: int = Field(foreign_key="event.id")
    catbox_url: Optional[str] = None
    # Optional fallback storage for posters to make Telegraph previews more reliable
    # and to survive Catbox outages/TLS issues.
    supabase_url: Optional[str] = None
    supabase_path: Optional[str] = None
    poster_hash: str
    # ``phash`` is the historical repository-compatible dHash16 value.  Keep it
    # for storage-path compatibility; new code stores the DCT hash separately.
    phash: Optional[str] = None
    raw_sha256: Optional[str] = None
    pixel_sha256: Optional[str] = None
    perceptual_hash: Optional[str] = None
    width: Optional[int] = None
    height: Optional[int] = None
    mime_type: Optional[str] = None
    review_status: str = "pending_review"
    duplicate_of_id: Optional[int] = Field(default=None, foreign_key="eventposter.id")
    review_reason: Optional[str] = None
    reviewed_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    display_order: int = 0
    ocr_text: Optional[str] = None
    ocr_title: Optional[str] = None
    # Event-relative multimodal classification. OCR presence alone is never a
    # poster decision; unknown/pending rows fail closed in public UI.
    image_text_mode: Optional[str] = None
    media_role: Optional[str] = None
    media_role_confidence: Optional[float] = None
    media_semantic_status: str = "pending"
    media_semantic_reason_code: Optional[str] = None
    media_semantic_evidence_json: Optional[dict[str, Any]] = Field(
        default=None, sa_column=Column(JSON)
    )
    media_semantic_model: Optional[str] = None
    media_semantic_prompt_version: Optional[str] = None
    media_semantic_context_hash: Optional[str] = None
    media_semantic_classified_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    focal_x: Optional[float] = None
    focal_y: Optional[float] = None
    safe_crop: Optional[bool] = None
    image_geometry_id: Optional[int] = Field(
        default=None, foreign_key="event_image_geometry.id"
    )
    thumbnail_256_url: Optional[str] = None
    thumbnail_256_path: Optional[str] = None
    thumbnail_256_width: Optional[int] = None
    thumbnail_256_height: Optional[int] = None
    thumbnail_512_url: Optional[str] = None
    thumbnail_512_path: Optional[str] = None
    thumbnail_512_width: Optional[int] = None
    thumbnail_512_height: Optional[int] = None
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    updated_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )


class EventImageGeometry(SQLModel, table=True):
    """Versioned, content-addressed crop-safety geometry for one physical image."""

    __tablename__ = "event_image_geometry"
    __table_args__ = (
        UniqueConstraint(
            "pixel_sha256",
            "model",
            "prompt_version",
            name="ux_event_image_geometry_version",
        ),
        Index("ix_event_image_geometry_status", "status", "updated_at"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    pixel_sha256: str
    model: str
    prompt_version: str
    status: str = "classified"
    source_width: Optional[int] = None
    source_height: Optional[int] = None
    # Compact normalized [ymin, xmin, ymax, xmax] arrays in the 0..1 range.
    # [] means analyzed and no faces; NULL means no valid analysis.
    face_boxes_yxyx_json: Optional[list[list[float]]] = Field(
        default=None, sa_column=Column(JSON)
    )
    valuable_region_yxyx_json: Optional[list[float]] = Field(
        default=None, sa_column=Column(JSON)
    )
    valuable_region_confidence: Optional[float] = None
    reason_code: Optional[str] = None
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    analyzed_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    created_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    updated_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )


class EventMediaPairReview(SQLModel, table=True):
    __tablename__ = "event_media_pair_review"
    __table_args__ = (
        Index("ix_event_media_pair_review_event_status", "event_id", "status", "next_run_at"),
        Index("ix_event_media_pair_review_left", "left_poster_id"),
        Index("ix_event_media_pair_review_right", "right_poster_id"),
        UniqueConstraint("pair_input_hash", name="ux_event_media_pair_review_input"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    event_id: int = Field(foreign_key="event.id")
    left_poster_id: int = Field(foreign_key="eventposter.id")
    right_poster_id: int = Field(foreign_key="eventposter.id")
    context_hash: str
    pair_input_hash: str
    status: str = "pending"
    decision: Optional[str] = None
    duplicate_kind: Optional[str] = None
    confidence: Optional[float] = None
    semantic_conflict: bool = False
    canonical_poster_id: Optional[int] = Field(default=None, foreign_key="eventposter.id")
    reason_code: Optional[str] = None
    primary_model: Optional[str] = None
    escalation_model: Optional[str] = None
    response_json: Optional[dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    attempts: int = 0
    provider_calls: int = 0
    last_error: Optional[str] = None
    next_run_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    created_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    updated_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    resolved_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )


class EventMediaReviewUsage(SQLModel, table=True):
    __tablename__ = "event_media_review_usage"
    day: str = Field(primary_key=True)
    stage: str = Field(primary_key=True)
    calls: int = 0
    updated_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )


class EventMediaAsset(SQLModel, table=True):
    __tablename__ = "event_media_asset"
    __table_args__ = (
        Index("ix_event_media_asset_event", "event_id"),
        Index("ix_event_media_asset_kind", "kind"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    event_id: int = Field(foreign_key="event.id")
    kind: str = Field(default="video")
    supabase_url: Optional[str] = None
    supabase_path: Optional[str] = None
    sha256: Optional[str] = None
    size_bytes: Optional[int] = None
    mime_type: Optional[str] = None
    created_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )


class VideoAsset(SQLModel, table=True):
    """Content-addressed video analysis and managed-CDN state.

    The row is deliberately global rather than event-owned.  Event links may
    disappear as events age out, while the terminal analysis remains useful as
    an exact-SHA cache and prevents paying to review the same bytes again.
    """

    __tablename__ = "video_asset"
    __table_args__ = (
        UniqueConstraint("sha256", name="ux_video_asset_sha256"),
        Index("ix_video_asset_status_showcase", "analysis_status", "showcase_score"),
        Index("ix_video_asset_cdn_path", "cdn_bucket", "cdn_path"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    sha256: str
    analysis_status: str = "accepted"
    cdn_url: Optional[str] = None
    cdn_path: Optional[str] = None
    cdn_bucket: Optional[str] = None
    size_bytes: Optional[int] = None
    mime_type: Optional[str] = None
    width: Optional[int] = None
    height: Optional[int] = None
    duration_seconds: Optional[float] = None
    aesthetic_score: Optional[float] = None
    technical_score: Optional[float] = None
    showcase_score: Optional[float] = None
    description: Optional[str] = None
    search_text: Optional[str] = None
    analysis_model: Optional[str] = None
    analysis_version: Optional[str] = None
    analysis_json: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    analyzed_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    orphaned_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    created_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    updated_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )


class EventVideoLink(SQLModel, table=True):
    """Many-to-many link between an event and a globally analyzed video."""

    __tablename__ = "event_video_link"
    __table_args__ = (
        UniqueConstraint(
            "event_id", "video_asset_id", name="ux_event_video_link_event_asset"
        ),
        Index("ix_event_video_link_event_rank", "event_id", "ranking_score"),
        Index("ix_event_video_link_asset", "video_asset_id"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    event_id: int = Field(foreign_key="event.id")
    video_asset_id: int = Field(foreign_key="video_asset.id")
    event_relevance_score: Optional[float] = None
    ranking_score: Optional[float] = None
    match_reason: Optional[str] = None
    relation_confidence: Optional[float] = None
    source_url: Optional[str] = None
    created_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )


class EventSource(SQLModel, table=True):
    __tablename__ = "event_source"
    __table_args__ = (
        Index("ix_event_source_event", "event_id"),
        Index("ix_event_source_type_url", "source_type", "source_url"),
        Index("ix_event_source_canonical_role", "canonical_source_url", "source_role"),
        Index("ix_event_source_fingerprint", "source_fingerprint"),
        Index("ix_event_source_candidate", "candidate_key"),
        Index("ix_event_source_occurrence", "canonical_source_url", "occurrence_key"),
        UniqueConstraint("event_id", "source_url", name="ux_event_source_event_url"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    event_id: int = Field(foreign_key="event.id")
    source_type: str
    source_url: str
    # Additive identity metadata. Legacy rows deliberately remain NULL until an
    # intake boundary can classify them from explicit provenance.
    canonical_source_url: Optional[str] = None
    source_role: Optional[str] = None
    source_fingerprint: Optional[str] = None
    # Candidate identity is additive: legacy source rows remain NULL until they
    # pass through an intake boundary that can supply an explicit occurrence.
    candidate_key: Optional[str] = None
    occurrence_key: Optional[str] = None
    smart_update_candidate_id: Optional[int] = Field(
        default=None, foreign_key="smart_update_candidate_state.id"
    )
    source_chat_username: Optional[str] = None
    source_chat_id: Optional[int] = None
    source_message_id: Optional[int] = None
    source_text: Optional[str] = None
    imported_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    trust_level: Optional[str] = None


class SmartUpdateCandidateState(SQLModel, table=True):
    __tablename__ = "smart_update_candidate_state"
    __table_args__ = (
        UniqueConstraint("candidate_key", name="ux_smart_update_candidate_key"),
        Index("ix_smart_update_candidate_due", "current_outcome", "next_retry_at"),
        Index(
            "ux_smart_update_candidate_source_occurrence",
            "canonical_source_url",
            "occurrence_key",
            unique=True,
            sqlite_where=text(
                "canonical_source_url IS NOT NULL AND canonical_source_url<>''"
            ),
        ),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    candidate_key: str
    occurrence_key: str
    canonical_source_url: Optional[str] = None
    source_type: str
    intent: str
    source_fingerprint: str
    candidate_payload: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    current_outcome: str = Field(default="RETRY_SCHEDULED")
    accepted_event_id: Optional[int] = Field(default=None, foreign_key="event.id")
    diagnostic_event_id: Optional[int] = Field(default=None, foreign_key="event.id")
    reason: Optional[str] = None
    attempts: int = Field(default=0)
    max_attempts: int = Field(default=3)
    retry_exhausted: bool = Field(default=False)
    next_retry_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    claimed_by: Optional[str] = None
    claim_expires_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    created_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    updated_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    completed_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )


class SmartUpdateAttempt(SQLModel, table=True):
    __tablename__ = "smart_update_attempt"
    __table_args__ = (
        UniqueConstraint(
            "candidate_state_id", "attempt_no", name="ux_smart_update_attempt_no"
        ),
        Index("ix_smart_update_attempt_terminal", "terminal_outcome", "finished_at"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    candidate_state_id: int = Field(foreign_key="smart_update_candidate_state.id")
    attempt_no: int
    started_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    finished_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    terminal_outcome: str = Field(default="RETRY_SCHEDULED")
    accepted_event_id: Optional[int] = Field(default=None, foreign_key="event.id")
    diagnostic_event_id: Optional[int] = Field(default=None, foreign_key="event.id")
    reason: Optional[str] = None


class EventIdentityDecisionLog(SQLModel, table=True):
    __tablename__ = "event_identity_decision_log"
    __table_args__ = (
        Index("ix_event_identity_decision_log_event", "event_id"),
        Index("ix_event_identity_decision_log_candidate", "candidate_event_id"),
        Index("ix_event_identity_decision_log_source", "source_type", "source_url"),
        Index("ix_event_identity_decision_log_created", "created_at"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    event_id: Optional[int] = Field(default=None, foreign_key="event.id")
    candidate_event_id: Optional[int] = Field(default=None, foreign_key="event.id")
    source_id: Optional[int] = Field(default=None, foreign_key="event_source.id")
    source_type: Optional[str] = None
    source_url: Optional[str] = None
    decision: str
    decision_reason: Optional[str] = None
    confidence: Optional[float] = None
    decided_by: Optional[str] = None
    decision_payload: dict[str, Any] = Field(
        default_factory=dict, sa_column=Column(JSON)
    )
    created_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )


class EventIdentityLock(SQLModel, table=True):
    __tablename__ = "event_identity_lock"
    __table_args__ = (
        Index("ix_event_identity_lock_status", "lock_status", "expires_at"),
    )

    event_id: int = Field(primary_key=True, foreign_key="event.id")
    lock_status: str = Field(default="active")
    lock_reason: Optional[str] = None
    locked_by: Optional[str] = None
    locked_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    expires_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    details: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))


class EventSourceFact(SQLModel, table=True):
    __tablename__ = "event_source_fact"
    __table_args__ = (
        Index("ix_event_source_fact_event", "event_id"),
        Index("ix_event_source_fact_source", "source_id"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    event_id: int = Field(foreign_key="event.id")
    source_id: int = Field(foreign_key="event_source.id")
    fact: str
    # Status of this fact in this source-iteration.
    # - added: applied to event (and should generally reflect in Telegraph content)
    # - duplicate: observed in source but already present -> not applied
    # - conflict: anchor conflict / ignored change
    # - note: technical/service note (filters, snippets, poster actions)
    status: str = Field(default="added")
    created_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )


class InterestClub(SQLModel, table=True):
    """Owner-curated club identity; never inferred from event recurrence alone."""

    __tablename__ = "interest_club"
    __table_args__ = (
        Index("ix_interest_club_public_status", "public_status"),
        Index("ix_interest_club_updated_at", "updated_at"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    slug: str = Field(sa_column=Column(String, nullable=False, unique=True))
    canonical_name: str
    topic: Optional[str] = None
    description: Optional[str] = None
    city: Optional[str] = None
    typical_place: Optional[str] = None
    public_status: str = "shadow"  # shadow|approved|archived|merged
    identity_version: int = 1
    policy_version: str = "interest-club-relation-v1"
    aliases_json: list[str] = Field(default_factory=list, sa_column=Column(JSON))
    source_anchors_json: list[str] = Field(default_factory=list, sa_column=Column(JSON))
    provenance_json: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    created_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    updated_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )


class InterestClubEvent(SQLModel, table=True):
    """Versioned projection relation; only ``active`` is publishable."""

    __tablename__ = "interest_club_event"
    __table_args__ = (
        UniqueConstraint("club_id", "event_id", name="ux_interest_club_event_pair"),
        Index("ix_interest_club_event_event_status", "event_id", "status"),
        Index("ix_interest_club_event_club_status", "club_id", "status"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    club_id: int = Field(foreign_key="interest_club.id")
    event_id: int = Field(foreign_key="event.id")
    status: str = "active"  # active|deferred|review
    decision_lane: str
    evidence_quote: Optional[str] = None
    evidence_json: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    model: Optional[str] = None
    policy_version: str = "interest-club-relation-v1"
    input_hash: str
    evaluated_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    updated_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )


class InterestClubEvaluation(SQLModel, table=True):
    """Hash-versioned decision history, including fail-closed non-relations."""

    __tablename__ = "interest_club_evaluation"
    __table_args__ = (
        UniqueConstraint(
            "club_id",
            "event_id",
            "policy_version",
            "input_hash",
            name="ux_interest_club_evaluation_history",
        ),
        Index("ix_interest_club_evaluation_status", "status", "updated_at"),
        Index("ix_interest_club_evaluation_event", "event_id"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    club_id: int = Field(foreign_key="interest_club.id")
    event_id: int = Field(foreign_key="event.id")
    status: str  # accepted|no_match|review|deferred|ineligible
    verdict: str  # yes|no|unclear|provider_error|ineligible
    decision_lane: str
    evidence_quote: Optional[str] = None
    evidence_json: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    model: Optional[str] = None
    policy_version: str = "interest-club-relation-v1"
    input_hash: str
    error_code: Optional[str] = None
    attempts: int = 1
    created_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    updated_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )


class TelegramSource(SQLModel, table=True):
    __tablename__ = "telegram_source"
    __table_args__ = (
        Index("ix_telegram_source_username", "username", unique=True),
        Index("ix_telegram_source_enabled", "enabled"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    username: str
    title: Optional[str] = None
    enabled: bool = Field(default=True, sa_column=Column(Boolean, default=True))
    default_location: Optional[str] = None
    default_ticket_link: Optional[str] = None
    trust_level: Optional[str] = None
    filters_json: Optional[dict] = Field(default=None, sa_column=Column(JSON, nullable=True))
    festival_source: Optional[bool] = Field(
        default=False, sa_column=Column(Boolean, default=False)
    )
    festival_series: Optional[str] = None
    about: Optional[str] = None
    about_links_json: Optional[list[str]] = Field(
        default=None, sa_column=Column(JSON, nullable=True)
    )
    meta_hash: Optional[str] = None
    meta_fetched_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    suggested_festival_series: Optional[str] = None
    suggested_website_url: Optional[str] = None
    suggestion_confidence: Optional[float] = None
    suggestion_rationale: Optional[str] = None
    last_scanned_message_id: Optional[int] = None
    last_scan_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )


class TelegramScannedMessage(SQLModel, table=True):
    __tablename__ = "telegram_scanned_message"
    __table_args__ = (
        Index("ix_tg_scanned_source", "source_id"),
        Index("ix_tg_scanned_processed_at", "processed_at"),
    )

    source_id: int = Field(foreign_key="telegram_source.id", primary_key=True)
    message_id: int = Field(primary_key=True)
    message_date: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    processed_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    status: str = "done"
    events_extracted: int = 0
    events_imported: int = 0
    error: Optional[str] = None


class TelegramSourceForceMessage(SQLModel, table=True):
    __tablename__ = "telegram_source_force_message"
    __table_args__ = (Index("ix_tg_force_source", "source_id"),)

    source_id: int = Field(foreign_key="telegram_source.id", primary_key=True)
    message_id: int = Field(primary_key=True)
    created_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )


class TelegramMonitoringOnDemandQueue(SQLModel, table=True):
    __tablename__ = "telegram_monitoring_on_demand_queue"
    __table_args__ = (
        Index("ix_tg_on_demand_status_next_run", "status", "next_run_at"),
        Index("ix_tg_on_demand_source", "source_id"),
    )

    source_username: str = Field(primary_key=True)
    source_id: int = Field(foreign_key="telegram_source.id")
    chat_id: Optional[int] = None
    latest_message_id: Optional[int] = None
    latest_message_date: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    first_seen_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    updated_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    next_run_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    attempts: int = 0
    status: str = "pending"
    last_run_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    last_error: Optional[str] = None


class TelegramPostMetric(SQLModel, table=True):
    __tablename__ = "telegram_post_metric"
    __table_args__ = (
        Index("ix_tg_metric_source_age", "source_id", "age_day"),
        Index("ix_tg_metric_source_message", "source_id", "message_id"),
        UniqueConstraint("source_id", "message_id", "age_day", name="ux_tg_metric_source_message_age"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    source_id: int = Field(foreign_key="telegram_source.id")
    message_id: int
    age_day: int
    source_url: Optional[str] = None
    message_ts: Optional[int] = None
    collected_ts: int = Field(default_factory=lambda: int(utc_now().timestamp()))
    views: Optional[int] = None
    likes: Optional[int] = None
    comments: Optional[int] = None
    forwards: Optional[int] = None
    reactions_json: Optional[dict] = Field(default=None, sa_column=Column(JSON, nullable=True))


class VkPostMetric(SQLModel, table=True):
    __tablename__ = "vk_post_metric"
    __table_args__ = (
        Index("ix_vk_metric_group_age", "group_id", "age_day"),
        Index("ix_vk_metric_group_post", "group_id", "post_id"),
        UniqueConstraint("group_id", "post_id", "age_day", name="ux_vk_metric_group_post_age"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    group_id: int
    post_id: int
    age_day: int
    source_url: Optional[str] = None
    post_ts: Optional[int] = None
    collected_ts: int = Field(default_factory=lambda: int(utc_now().timestamp()))
    views: Optional[int] = None
    likes: Optional[int] = None
    comments: Optional[int] = None
    reposts: Optional[int] = None


class SocialMetricSnapshot(SQLModel, table=True):
    __tablename__ = "social_metric_snapshot"
    __table_args__ = (
        Index(
            "ix_social_metric_due",
            "platform",
            "publisher_id",
            "post_id",
            "age_bucket",
            "status",
        ),
        Index("ix_social_metric_url", "source_url", "collected_ts"),
        UniqueConstraint(
            "platform",
            "publisher_id",
            "post_id",
            "age_bucket",
            name="ux_social_metric_post_bucket",
        ),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    platform: str
    publisher_id: str
    post_id: int
    age_bucket: str
    publication_kind: str = "external_event_source"
    source_url: Optional[str] = None
    post_ts: Optional[int] = None
    collected_ts: int = Field(default_factory=lambda: int(utc_now().timestamp()))
    views: Optional[int] = None
    likes: Optional[int] = None
    comments: Optional[int] = None
    shares: Optional[int] = None
    reactions_json: Optional[dict] = Field(default=None, sa_column=Column(JSON, nullable=True))
    status: str = "collected"
    error_code: Optional[str] = None


class TomorrowPage(SQLModel, table=True):
    date: str = Field(primary_key=True)  # YYYY-MM-DD
    url: str
    created_at: datetime = Field(default_factory=utc_now)


class MonthPage(SQLModel, table=True):
    __table_args__ = {"extend_existing": True}
    month: str = Field(primary_key=True)
    url: str
    path: str
    url2: Optional[str] = None  # Deprecated: use MonthPagePart
    path2: Optional[str] = None  # Deprecated: use MonthPagePart
    content_hash: Optional[str] = None
    content_hash2: Optional[str] = None  # Deprecated: use MonthPagePart


class MonthPagePart(SQLModel, table=True):
    """Stores individual parts of a month page when split into multiple pages."""
    __table_args__ = (
        Index("ix_monthpagepart_month", "month"),
        UniqueConstraint("month", "part_number", name="ux_monthpagepart_month_part"),
        {"extend_existing": True},
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    month: str  # e.g., "2025-01"
    part_number: int  # 1, 2, 3, ...
    url: str
    path: str
    content_hash: Optional[str] = None
    first_date: Optional[str] = None  # First event date on this page (YYYY-MM-DD)
    last_date: Optional[str] = None   # Last event date on this page (YYYY-MM-DD)


class MonthExhibitionsPage(SQLModel, table=True):
    __table_args__ = {"extend_existing": True}
    month: str = Field(primary_key=True)
    url: str
    path: str
    content_hash: Optional[str] = None


class WeekendPage(SQLModel, table=True):
    __table_args__ = {"extend_existing": True}
    start: str = Field(primary_key=True)
    url: str
    path: str
    vk_post_url: Optional[str] = None
    content_hash: Optional[str] = None


class WeekPage(SQLModel, table=True):
    __table_args__ = {"extend_existing": True}
    start: str = Field(primary_key=True)
    vk_post_url: Optional[str] = None
    content_hash: Optional[str] = None


class Festival(SQLModel, table=True):
    __table_args__ = (Index("idx_festival_name", "name"), {"extend_existing": True})
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    full_name: Optional[str] = None
    description: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    telegraph_url: Optional[str] = None
    telegraph_path: Optional[str] = None
    vk_post_url: Optional[str] = None
    vk_poll_url: Optional[str] = None
    photo_url: Optional[str] = None
    photo_urls: list[str] = Field(default_factory=list, sa_column=Column(JSON))
    aliases: list[str] = Field(default_factory=list, sa_column=Column(JSON))
    website_url: Optional[str] = None
    program_url: Optional[str] = None
    vk_url: Optional[str] = None
    tg_url: Optional[str] = None
    ticket_url: Optional[str] = None
    location_name: Optional[str] = None
    location_address: Optional[str] = None
    city: Optional[str] = None
    activities_json: list[dict] = Field(
        default_factory=list,
        sa_column=Column(
            JSON().with_variant(JSONB, "postgresql"),
            nullable=False,
            server_default=text("'[]'"),
        ),
    )
    source_text: Optional[str] = None
    source_post_url: Optional[str] = None
    source_chat_id: Optional[int] = None
    source_message_id: Optional[int] = None
    nav_hash: Optional[str] = None
    # Parser-related fields (Universal Festival Parser)
    source_url: Optional[str] = None  # Original URL of the festival site
    source_type: Optional[str] = None  # "canonical" | "official" | "external"
    parser_run_id: Optional[str] = None  # Last parser run ID
    parser_version: Optional[str] = None  # Parser version used
    last_parsed_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    uds_storage_path: Optional[str] = None  # Path in Supabase Storage to UDS JSON
    contacts_phone: Optional[str] = None  # Phone contact
    contacts_email: Optional[str] = None  # Email contact
    is_annual: Optional[bool] = None  # Is this an annual festival?
    audience: Optional[str] = None  # Target audience description
    created_at: datetime = Field(
        default_factory=utc_now,
        sa_column=Column(
            DateTime(timezone=True),
            nullable=False,
            server_default=text("CURRENT_TIMESTAMP"),
        ),
    )
    updated_at: datetime = Field(
        default_factory=utc_now,
        sa_column=Column(
            DateTime(timezone=True),
            nullable=False,
            server_default=text("CURRENT_TIMESTAMP"),
        ),
    )


class FestivalCalendarItem(SQLModel, table=True):
    """A public, year-scoped festival-calendar edition.

    The legacy ``festival`` table is keyed by a non-unique series name and is
    still consumed by parser/Telegraph paths that expect one matching row.
    Calendar editions therefore live in their own table instead of creating
    duplicate yearly ``festival.name`` rows.
    """

    __tablename__ = "festival_calendar_item"
    __table_args__ = (
        UniqueConstraint(
            "calendar_year",
            "slug",
            name="ux_festival_calendar_item_year_slug",
        ),
        UniqueConstraint(
            "calendar_year",
            "display_order",
            name="ux_festival_calendar_item_year_order",
        ),
        Index(
            "ix_festival_calendar_item_public_month",
            "calendar_year",
            "is_public",
            "month_key",
            "display_order",
        ),
        {"extend_existing": True},
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    calendar_year: int
    slug: str
    title: str
    description: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    date_precision: str = "exact"
    date_label: str
    sort_date: str
    month_key: str
    display_order: int
    place_label: str
    category: str
    status: str
    status_label: str
    source_url: str
    source_label: str
    internal_event_id: Optional[int] = Field(default=None, foreign_key="event.id")
    festival_id: Optional[int] = Field(default=None, foreign_key="festival.id")
    cover_key: str
    image_width: int
    image_height: int
    media_mode: str = "visual"
    object_position: Optional[str] = None
    catalog_version: str
    is_public: bool = True
    created_at: datetime = Field(
        default_factory=utc_now,
        sa_column=Column(
            DateTime(timezone=True),
            nullable=False,
            server_default=text("CURRENT_TIMESTAMP"),
        ),
    )
    updated_at: datetime = Field(
        default_factory=utc_now,
        sa_column=Column(
            DateTime(timezone=True),
            nullable=False,
            server_default=text("CURRENT_TIMESTAMP"),
        ),
    )



class FestivalQueueItem(SQLModel, table=True):
    __tablename__ = "festival_queue"
    __table_args__ = (
        Index("ix_festival_queue_status_next_run", "status", "next_run_at"),
        Index("ix_festival_queue_source_kind", "source_kind"),
        Index("ix_festival_queue_source_url", "source_url"),
        {"extend_existing": True},
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    status: str = Field(default="pending")
    source_kind: str  # vk | tg | url
    source_url: str
    source_text: Optional[str] = None
    source_chat_username: Optional[str] = None
    source_chat_id: Optional[int] = None
    source_message_id: Optional[int] = None
    source_group_id: Optional[int] = None
    source_post_id: Optional[int] = None
    festival_context: Optional[str] = None
    festival_name: Optional[str] = None
    festival_full: Optional[str] = None
    festival_series: Optional[str] = None
    dedup_links_json: list[str] = Field(default_factory=list, sa_column=Column(JSON))
    signals_json: dict = Field(default_factory=dict, sa_column=Column(JSON))
    result_json: dict = Field(default_factory=dict, sa_column=Column(JSON))
    attempts: int = 0
    last_error: Optional[str] = None
    created_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    updated_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    next_run_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )


class FestivalWebResearchRun(SQLModel, table=True):
    """Provider-neutral collect/review envelope for one festival edition target."""

    __tablename__ = "festival_web_research_run"
    __table_args__ = (
        UniqueConstraint("run_uid", name="ux_festival_web_research_run_uid"),
        UniqueConstraint(
            "input_fingerprint",
            name="ux_festival_web_research_run_input_fingerprint",
        ),
        Index(
            "ix_festival_web_research_run_state_updated",
            "state",
            "updated_at",
        ),
        Index(
            "ix_festival_web_research_run_target_created",
            "target_key",
            "created_at",
        ),
        Index(
            "ix_festival_web_research_run_review_updated",
            "review_status",
            "updated_at",
        ),
        {"extend_existing": True},
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    run_uid: str
    target_key: str
    series_candidate: Optional[str] = None
    edition_candidate: Optional[str] = None
    state: str = Field(
        default="pending",
        sa_column=Column(String, nullable=False, server_default=text("'pending'")),
    )
    mode: str = Field(
        default="collect_only",
        sa_column=Column(
            String, nullable=False, server_default=text("'collect_only'")
        ),
    )
    review_status: str = Field(
        default="pending",
        sa_column=Column(String, nullable=False, server_default=text("'pending'")),
    )
    input_fingerprint: str
    orchestration_version: str
    contract_version: str
    taxonomy_version: str
    taxonomy_sha256: str
    primary_queue_item_id: Optional[int] = Field(
        default=None,
        sa_column=Column(
            Integer,
            ForeignKey("festival_queue.id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    candidate_sha256: Optional[str] = None
    candidate_json: dict = Field(
        default_factory=dict,
        sa_column=Column(JSON, nullable=False, server_default=text("'{}'")),
    )
    quality_json: dict = Field(
        default_factory=dict,
        sa_column=Column(JSON, nullable=False, server_default=text("'{}'")),
    )
    artifact_manifest_json: dict = Field(
        default_factory=dict,
        sa_column=Column(JSON, nullable=False, server_default=text("'{}'")),
    )
    lease_owner: Optional[str] = None
    lease_expires_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    reviewed_by: Optional[str] = None
    reviewed_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    review_reason: Optional[str] = None
    started_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    completed_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    created_at: datetime = Field(
        default_factory=utc_now,
        sa_column=Column(
            DateTime(timezone=True),
            nullable=False,
            server_default=text("CURRENT_TIMESTAMP"),
        ),
    )
    updated_at: datetime = Field(
        default_factory=utc_now,
        sa_column=Column(
            DateTime(timezone=True),
            nullable=False,
            server_default=text("CURRENT_TIMESTAMP"),
        ),
    )


class FestivalWebResearchLaneRun(SQLModel, table=True):
    """One bounded collector attempt with distinct provider and semantic states."""

    __tablename__ = "festival_web_research_lane_run"
    __table_args__ = (
        UniqueConstraint(
            "run_id",
            "lane",
            "attempt_no",
            name="ux_festival_web_research_lane_attempt",
        ),
        UniqueConstraint(
            "request_uid",
            name="ux_festival_web_research_lane_request_uid",
        ),
        Index(
            "ix_festival_web_research_lane_provider_updated",
            "provider_state",
            "updated_at",
        ),
        Index(
            "ix_festival_web_research_lane_semantic_updated",
            "semantic_state",
            "updated_at",
        ),
        Index(
            "ix_festival_web_research_lane_input_fingerprint",
            "input_fingerprint",
        ),
        {"extend_existing": True},
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    run_id: int = Field(
        sa_column=Column(
            Integer,
            ForeignKey("festival_web_research_run.id", ondelete="CASCADE"),
            nullable=False,
        )
    )
    lane: str = Field(
        default="antigravity",
        sa_column=Column(
            String, nullable=False, server_default=text("'antigravity'")
        ),
    )
    attempt_no: int = Field(
        default=1,
        sa_column=Column(Integer, nullable=False, server_default=text("1")),
    )
    request_uid: str
    provider_state: str = Field(
        default="pending",
        sa_column=Column(String, nullable=False, server_default=text("'pending'")),
    )
    semantic_state: str = Field(
        default="pending",
        sa_column=Column(String, nullable=False, server_default=text("'pending'")),
    )
    interaction_ids_json: list[dict] = Field(
        default_factory=list,
        sa_column=Column(JSON, nullable=False, server_default=text("'[]'")),
    )
    model_id: Optional[str] = None
    prompt_version: str
    contract_version: str
    taxonomy_version: str
    taxonomy_sha256: str
    input_fingerprint: str
    artifact_manifest_json: dict = Field(
        default_factory=dict,
        sa_column=Column(JSON, nullable=False, server_default=text("'{}'")),
    )
    usage_json: dict = Field(
        default_factory=dict,
        sa_column=Column(JSON, nullable=False, server_default=text("'{}'")),
    )
    validation_json: dict = Field(
        default_factory=dict,
        sa_column=Column(JSON, nullable=False, server_default=text("'{}'")),
    )
    candidate_sha256: Optional[str] = None
    candidate_json: dict = Field(
        default_factory=dict,
        sa_column=Column(JSON, nullable=False, server_default=text("'{}'")),
    )
    provider_error_code: Optional[str] = None
    semantic_error_code: Optional[str] = None
    last_error: Optional[str] = None
    started_at: datetime = Field(
        default_factory=utc_now,
        sa_column=Column(
            DateTime(timezone=True),
            nullable=False,
            server_default=text("CURRENT_TIMESTAMP"),
        ),
    )
    completed_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    created_at: datetime = Field(
        default_factory=utc_now,
        sa_column=Column(
            DateTime(timezone=True),
            nullable=False,
            server_default=text("CURRENT_TIMESTAMP"),
        ),
    )
    updated_at: datetime = Field(
        default_factory=utc_now,
        sa_column=Column(
            DateTime(timezone=True),
            nullable=False,
            server_default=text("CURRENT_TIMESTAMP"),
        ),
    )


class FestivalWebResearchItem(SQLModel, table=True):
    """Immutable membership and eventual disposition of a grouped queue item."""

    __tablename__ = "festival_web_research_item"
    __table_args__ = (
        UniqueConstraint(
            "run_id",
            "queue_item_id",
            name="ux_festival_web_research_item_run_queue",
        ),
        Index(
            "ix_festival_web_research_item_queue",
            "queue_item_id",
            "created_at",
        ),
        Index(
            "ix_festival_web_research_item_decision",
            "decision",
            "updated_at",
        ),
        {"extend_existing": True},
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    run_id: int = Field(
        sa_column=Column(
            Integer,
            ForeignKey("festival_web_research_run.id", ondelete="CASCADE"),
            nullable=False,
        )
    )
    queue_item_id: int = Field(
        sa_column=Column(
            Integer,
            ForeignKey("festival_queue.id", ondelete="RESTRICT"),
            nullable=False,
        )
    )
    original_status: str
    source_role: str
    decision: str = Field(
        default="pending",
        sa_column=Column(String, nullable=False, server_default=text("'pending'")),
    )
    decision_reason: Optional[str] = None
    created_at: datetime = Field(
        default_factory=utc_now,
        sa_column=Column(
            DateTime(timezone=True),
            nullable=False,
            server_default=text("CURRENT_TIMESTAMP"),
        ),
    )
    updated_at: datetime = Field(
        default_factory=utc_now,
        sa_column=Column(
            DateTime(timezone=True),
            nullable=False,
            server_default=text("CURRENT_TIMESTAMP"),
        ),
    )


class FestivalWebResearchSource(SQLModel, table=True):
    """Source snapshot ledger for evidence and exclusion auditability."""

    __tablename__ = "festival_web_research_source"
    __table_args__ = (
        UniqueConstraint(
            "lane_run_id",
            "source_id",
            name="ux_festival_web_research_source_lane_source",
        ),
        Index(
            "ix_festival_web_research_source_canonical_url",
            "canonical_url",
        ),
        Index(
            "ix_festival_web_research_source_content_hash",
            "content_sha256",
        ),
        Index(
            "ix_festival_web_research_source_lane_decision",
            "lane_run_id",
            "decision",
        ),
        {"extend_existing": True},
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    lane_run_id: int = Field(
        sa_column=Column(
            Integer,
            ForeignKey("festival_web_research_lane_run.id", ondelete="CASCADE"),
            nullable=False,
        )
    )
    source_id: str
    requested_url: str
    resolved_url: Optional[str] = None
    canonical_url: Optional[str] = None
    source_role: str
    edition_status: str = Field(
        default="unknown",
        sa_column=Column(String, nullable=False, server_default=text("'unknown'")),
    )
    content_sha256: Optional[str] = None
    snapshot_ref: Optional[str] = None
    normalizer_version: Optional[str] = None
    quote_index_ref: Optional[str] = None
    fetched_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    decision: str = Field(
        default="pending",
        sa_column=Column(String, nullable=False, server_default=text("'pending'")),
    )
    exclusion_reason: Optional[str] = None
    created_at: datetime = Field(
        default_factory=utc_now,
        sa_column=Column(
            DateTime(timezone=True),
            nullable=False,
            server_default=text("CURRENT_TIMESTAMP"),
        ),
    )
    updated_at: datetime = Field(
        default_factory=utc_now,
        sa_column=Column(
            DateTime(timezone=True),
            nullable=False,
            server_default=text("CURRENT_TIMESTAMP"),
        ),
    )


class TicketSiteQueueItem(SQLModel, table=True):
    __tablename__ = "ticket_site_queue"
    __table_args__ = (
        Index("ix_ticket_site_queue_status_next_run", "status", "next_run_at"),
        Index("ix_ticket_site_queue_site_kind", "site_kind"),
        Index("ux_ticket_site_queue_url", "url", unique=True),
        {"extend_existing": True},
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    status: str = Field(default="active")  # active|running|error|disabled
    site_kind: str  # pyramida|dom_iskusstv|qtickets
    url: str
    event_id: Optional[int] = None
    source_post_url: Optional[str] = None
    source_chat_username: Optional[str] = None
    source_chat_id: Optional[int] = None
    source_message_id: Optional[int] = None
    attempts: int = 0
    last_error: Optional[str] = None
    last_result_json: dict = Field(default_factory=dict, sa_column=Column(JSON))
    last_run_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    created_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    updated_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    next_run_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )


class OpsRun(SQLModel, table=True):
    __tablename__ = "ops_run"
    __table_args__ = (
        Index("ix_ops_run_kind_started_at", "kind", "started_at"),
        Index("ix_ops_run_status_started_at", "status", "started_at"),
        {"extend_existing": True},
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    kind: str
    trigger: str = "manual"
    chat_id: Optional[int] = None
    operator_id: Optional[int] = None
    started_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    finished_at: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime(timezone=True))
    )
    status: str = "running"
    metrics_json: dict = Field(default_factory=dict, sa_column=Column(JSON))
    details_json: dict = Field(default_factory=dict, sa_column=Column(JSON))


class JobTask(str, Enum):
    event_media_review = "event_media_review"
    telegraph_build = "telegraph_build"
    vk_sync = "vk_sync"
    tg_event_publish = "tg_event_publish"
    tg_premium_emoji_edit = "tg_premium_emoji_edit"
    ics_publish = "ics_publish"
    tg_ics_post = "tg_ics_post"
    month_pages = "month_pages"
    weekend_pages = "weekend_pages"
    week_pages = "week_pages"
    festival_pages = "festival_pages"
    static_site_build = "static_site_build"
    event_vector_sync = "event_vector_sync"
    event_age_bge_assessment = "event_age_bge_assessment"
    interest_club_relation = "interest_club_relation"
    fest_nav_update_all = "fest_nav:update_all"


class JobStatus(str, Enum):
    pending = "pending"
    running = "running"
    done = "done"
    error = "error"
    paused = "paused"


class JobOutbox(SQLModel, table=True):
    __table_args__ = (
        Index("ix_job_outbox_event_task", "event_id", "task"),
        Index("ix_job_outbox_status_next_run_at", "status", "next_run_at"),
        {"extend_existing": True},
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    event_id: int
    task: JobTask = Field(sa_column=Column(SAEnum(JobTask)))
    payload: dict | None = Field(default=None, sa_column=Column(JSON))
    status: JobStatus = Field(
        default=JobStatus.pending, sa_column=Column(SAEnum(JobStatus))
    )
    attempts: int = 0
    last_error: Optional[str] = None
    last_result: Optional[str] = None
    updated_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    next_run_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )
    coalesce_key: Optional[str] = None
    depends_on: Optional[str] = None


class StaticSiteBuildState(SQLModel, table=True):
    __tablename__ = "static_site_build_state"
    __table_args__ = ({"extend_existing": True},)

    release_channel: str = Field(primary_key=True)
    schema_version: str
    last_success_fingerprint: Optional[str] = None
    last_success_run_id: Optional[str] = None
    last_success_at: Optional[str] = None
    last_success_receipt_json: str = "{}"
    active_claim_token: Optional[str] = None
    active_job_id: Optional[int] = None
    active_run_id: Optional[str] = None
    active_fingerprint: Optional[str] = None
    active_effective_date: Optional[str] = None
    active_claimed_at: Optional[str] = None
    updated_at: str = Field(default_factory=lambda: utc_now().isoformat())


class StaticSiteBuildHistory(SQLModel, table=True):
    __tablename__ = "static_site_build_history"
    __table_args__ = (
        Index(
            "ix_static_site_build_history_fingerprint",
            "input_fingerprint",
            "outcome",
            "created_at",
        ),
        {"extend_existing": True},
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    release_channel: str
    job_id: Optional[int] = None
    request_watermark: Optional[str] = None
    input_fingerprint: str
    effective_date: str
    force_rebuild: bool = False
    outcome: str
    run_id: Optional[str] = None
    evidence_json: str = "{}"
    created_at: str = Field(default_factory=lambda: utc_now().isoformat())


class PosterOcrCache(SQLModel, table=True):
    hash: str = Field(primary_key=True)
    detail: str = Field(primary_key=True)
    model: str = Field(primary_key=True)
    text: str
    title: Optional[str] = None
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    created_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )


class OcrUsage(SQLModel, table=True):
    date: str = Field(primary_key=True)
    spent_tokens: int = 0


def create_all(engine) -> None:
    SQLModel.metadata.create_all(engine)


class VKInbox(SQLModel, table=True):
    __tablename__ = "vk_inbox"
    id: Optional[int] = Field(default=None, primary_key=True)
    group_id: int
    post_id: int
    date: int
    text: str
    matched_kw: Optional[str] = None
    has_date: int
    event_ts_hint: Optional[int] = None
    status: str = Field(default="pending")
    locked_by: Optional[int] = None
    locked_at: Optional[datetime] = Field(default=None, sa_column=Column(DateTime(timezone=True)))
    imported_event_id: Optional[int] = None
    review_batch: Optional[str] = None
    attempts: int = 0
    created_at: datetime = Field(
        default_factory=utc_now, sa_column=Column(DateTime(timezone=True))
    )

@dataclass
class VkMissRecord:
    id: str
    url: str
    reason: str | None
    matched_kw: str | None
    timestamp: datetime

@dataclass
class VkMissReviewSession:
    queue: list[VkMissRecord]
    index: int = 0
    last_text: str | None = None
    last_published_at: datetime | None = None
