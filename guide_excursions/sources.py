from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from telegram_sources import normalize_tg_username


@dataclass(frozen=True, slots=True)
class GuideSourceSpec:
    username: str
    profile_slug: str
    profile_kind: str
    display_name: str
    marketing_name: str | None
    source_kind: str
    trust_level: str
    base_region: str = "Калининградская область"
    priority_weight: float = 1.0
    flags: dict[str, Any] | None = None
    notes: str | None = None
    platform: str = "telegram"
    source_url: str | None = None


def _normalize_vk_username(value: str) -> str:
    raw = str(value or "").strip().removeprefix("@")
    for prefix in ("https://vk.com/", "http://vk.com/", "https://vk.ru/", "http://vk.ru/"):
        if raw.lower().startswith(prefix):
            raw = raw[len(prefix) :]
            break
    raw = raw.split("?", 1)[0].split("#", 1)[0].strip("/")
    return raw.lower()


def canonical_guide_sources() -> tuple[GuideSourceSpec, ...]:
    raw: list[GuideSourceSpec] = [
        GuideSourceSpec(
            username="tanja_from_koenigsberg",
            profile_slug="tatyana-udovenko",
            profile_kind="person",
            display_name="Татьяна Удовенко",
            marketing_name="Татьяна Удовенко",
            source_kind="guide_personal",
            trust_level="high",
            priority_weight=1.15,
            flags={"collaboration_heavy": True},
        ),
        GuideSourceSpec(
            username="gid_zelenogradsk",
            profile_slug="gid-zelenogradsk",
            profile_kind="person",
            display_name="Гид Зеленоградск",
            marketing_name="Гид Зеленоградск",
            source_kind="guide_personal",
            trust_level="high",
            flags={},
        ),
        GuideSourceSpec(
            username="katimartihobby",
            profile_slug="kati-marti",
            profile_kind="person",
            display_name="Катя Марти",
            marketing_name="Шаги Кати Марти",
            source_kind="guide_personal",
            trust_level="high",
            flags={"caption_heavy": True},
        ),
        GuideSourceSpec(
            username="amber_fringilla",
            profile_slug="amber-fringilla",
            profile_kind="person",
            display_name="Amber Fringilla",
            marketing_name="Amber Fringilla",
            source_kind="guide_personal",
            trust_level="high",
            priority_weight=1.05,
            flags={"collaboration_heavy": True},
        ),
        GuideSourceSpec(
            username="art_from_the_Baltic",
            profile_slug="art-from-the-baltic",
            profile_kind="project",
            display_name="Art from the Baltic",
            marketing_name="Art from the Baltic",
            source_kind="guide_project",
            trust_level="medium",
            priority_weight=1.0,
            flags={"mixed_topic": True},
            notes="Added from operator request on 2026-03-16; casebook expansion pending.",
        ),
        GuideSourceSpec(
            username="alev701",
            profile_slug="alev701",
            profile_kind="person",
            display_name="Алексей А.",
            marketing_name="alev701",
            source_kind="guide_personal",
            trust_level="medium",
            flags={},
        ),
        GuideSourceSpec(
            username="murnikovaT",
            profile_slug="tatyana-murnikova",
            profile_kind="person",
            display_name="Татьяна Мурникова",
            marketing_name="Татьяна Мурникова",
            source_kind="guide_personal",
            trust_level="medium",
            flags={},
            notes="Added from operator request on 2026-04-28; guide channel for individual Kaliningrad excursions.",
        ),
        GuideSourceSpec(
            username="twometerguide",
            profile_slug="twometerguide",
            profile_kind="project",
            display_name="Двухметровый гид",
            marketing_name="Двухметровый гид",
            source_kind="guide_project",
            trust_level="medium",
            flags={"mixed_region": True},
        ),
        GuideSourceSpec(
            username="valeravezet",
            profile_slug="valera-vezet",
            profile_kind="project",
            display_name="Автобус Валера",
            marketing_name="Автобус Валера",
            source_kind="guide_project",
            trust_level="medium",
            flags={"promo_noise": True},
        ),
        GuideSourceSpec(
            username="ruin_keepers",
            profile_slug="ruin-keepers",
            profile_kind="organization",
            display_name="Хранители руин",
            marketing_name="Хранители руин",
            source_kind="organization_with_tours",
            trust_level="medium",
            priority_weight=1.05,
            flags={"organization": True},
        ),
        GuideSourceSpec(
            username="jeeptours39",
            profile_slug="vyvozim-v-les",
            profile_kind="organization",
            display_name="Вывозим в лес",
            marketing_name="Вывозим в лес",
            source_kind="organization_with_tours",
            trust_level="medium",
            priority_weight=1.0,
            flags={"organization": True, "offroad_tours": True},
            notes=(
                "Added from operator request on 2026-04-07; off-road / jeep-tour "
                "terminology must stay explicit and not be relabeled as generic excursion."
            ),
        ),
        GuideSourceSpec(
            username="kaliningradlibrary",
            profile_slug="kaliningrad-library",
            profile_kind="organization",
            display_name="Калининградская областная научная библиотека",
            marketing_name="Калининградская областная научная библиотека",
            source_kind="organization_with_tours",
            trust_level="medium",
            flags={"organization": True, "mixed_topic": True, "library": True},
            notes=(
                "Added from operator request on 2026-05-16; regional library channel with "
                "books/history/events mix, monitored for guided tours and excursion-like walks."
            ),
        ),
        GuideSourceSpec(
            username="konb39",
            profile_slug="kaliningrad-library",
            profile_kind="organization",
            display_name="Калининградская областная научная библиотека",
            marketing_name="Калининградская областная научная библиотека",
            source_kind="organization_with_tours",
            trust_level="medium",
            flags={"organization": True, "mixed_topic": True, "library": True, "vk_public": True},
            notes=(
                "Added from operator request on 2026-05-18; VK public for the regional library, "
                "monitored for guided tours and excursion-like walks alongside Telegram."
            ),
            platform="vk",
            source_url="https://vk.com/konb39",
        ),
        GuideSourceSpec(
            username="ruin.keepers",
            profile_slug="ruin-keepers",
            profile_kind="organization",
            display_name="Хранители руин",
            marketing_name="Хранители руин",
            source_kind="organization_with_tours",
            trust_level="medium",
            priority_weight=1.05,
            flags={"organization": True, "vk_public": True},
            notes=(
                "Added from operator request on 2026-05-19; VK public mirrors/extends "
                "the Ruin Keepers guide source and must dedup against Telegram ruin_keepers."
            ),
            platform="vk",
            source_url="https://vk.com/ruin.keepers",
        ),
        GuideSourceSpec(
            username="narodexcursovod",
            profile_slug="narodny-excursovod",
            profile_kind="organization",
            display_name="Народный экскурсовод",
            marketing_name="Народный экскурсовод",
            source_kind="organization_with_tours",
            trust_level="medium",
            flags={"organization": True, "vk_public": True, "mixed_topic": True},
            notes=(
                "Added from operator request on 2026-05-19; VK public monitored for "
                "Kaliningrad-region guided walks and excursion announcements."
            ),
            platform="vk",
            source_url="https://vk.com/narodexcursovod",
        ),
        GuideSourceSpec(
            username="balticsyndicate",
            profile_slug="baltic-syndicate",
            profile_kind="project",
            display_name="Baltic Syndicate",
            marketing_name="Baltic Syndicate",
            source_kind="guide_project",
            trust_level="medium",
            flags={"vk_public": True, "mixed_topic": True},
            notes="Added from operator request on 2026-05-18; VK public monitored for excursion announcements.",
            platform="vk",
            source_url="https://vk.ru/balticsyndicate",
        ),
        GuideSourceSpec(
            username="ivsguide",
            profile_slug="ivsguide",
            profile_kind="guide",
            display_name="Игорь Селин",
            marketing_name="Игорь Селин",
            source_kind="guide_personal",
            trust_level="medium",
            flags={
                "vk_personal_page": True,
                "mixed_topic": True,
                "guide": True,
            },
            notes=(
                "Added from operator request on 2026-05-18; VK personal page of "
                "a Kaliningrad guide (Игорь Селин), monitored for guided "
                "walks/excursions alongside other guide sources."
            ),
            platform="vk",
            source_url="https://vk.com/ivsguide",
        ),
        GuideSourceSpec(
            username="natakkaz",
            profile_slug="natakkaz",
            profile_kind="guide",
            display_name="Наталья Казакова",
            marketing_name="Наталья Казакова",
            source_kind="guide_personal",
            trust_level="medium",
            flags={
                "vk_personal_page": True,
                "mixed_topic": True,
                "guide": True,
            },
            notes=(
                "Added from operator request on 2026-05-18; VK personal page of "
                "a Kaliningrad guide (Наталья Казакова), monitored for guided "
                "walks/excursions alongside other guide sources."
            ),
            platform="vk",
            source_url="https://vk.ru/natakkaz",
        ),
        GuideSourceSpec(
            username="excursions_profitour",
            profile_slug="profitur",
            profile_kind="operator",
            display_name="Профи-тур",
            marketing_name="Профи-тур",
            source_kind="excursion_operator",
            trust_level="medium",
            flags={"operator": True, "on_request_heavy": True},
        ),
        GuideSourceSpec(
            username="vkaliningrade",
            profile_slug="v-kaliningrade",
            profile_kind="project",
            display_name="В Калининграде",
            marketing_name="В Калининграде",
            source_kind="aggregator",
            trust_level="medium",
            priority_weight=0.9,
            flags={"aggregator": True},
        ),
    ]
    out: list[GuideSourceSpec] = []
    for item in raw:
        platform = str(item.platform or "telegram").strip().lower() or "telegram"
        if platform == "telegram":
            username = normalize_tg_username(item.username)
        elif platform == "vk":
            username = _normalize_vk_username(item.username)
        else:
            continue
        if not username:
            continue
        out.append(
            GuideSourceSpec(
                username=username,
                profile_slug=item.profile_slug,
                profile_kind=item.profile_kind,
                display_name=item.display_name,
                marketing_name=item.marketing_name,
                source_kind=item.source_kind,
                trust_level=item.trust_level,
                base_region=item.base_region,
                priority_weight=float(item.priority_weight or 1.0),
                flags=dict(item.flags or {}),
                notes=item.notes,
                platform=platform,
                source_url=item.source_url,
            )
        )
    out.sort(key=lambda item: (item.platform, item.username))
    return tuple(out)
