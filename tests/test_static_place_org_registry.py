import copy
import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "site" / "scripts" / "static_place_org_registry.py"
SPEC = importlib.util.spec_from_file_location("static_place_org_registry", SCRIPT)
assert SPEC and SPEC.loader
REGISTRY_MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REGISTRY_MODULE)


@pytest.fixture(scope="module")
def registry():
    return REGISTRY_MODULE.load_registry()


def _ids(resolution, key):
    return [item["entity_id"] for item in resolution[key]]


def _membership(resolution, key, entity_id):
    return next(item for item in resolution[key] if item["entity_id"] == entity_id)


def _reason_codes(membership):
    return [reason["code"] for reason in membership["reasons"]]


def test_v1_registry_has_exact_entities_flags_kinds_and_reviewed_exclusions(registry):
    entities = registry["entities"]
    assert len(entities) == 11
    assert sum(item["kind"] == "mixed" for item in entities) == 7
    assert sum(item["kind"] == "organization" for item in entities) == 1
    assert sum(item["kind"] == "place" for item in entities) == 3
    assert sum(item["flags"]["official_theatre"] for item in entities) == 8
    assert sum(item["flags"]["venue_page_candidate"] for item in entities) == 6
    assert {
        item["id"] for item in entities if item["flags"]["official_theatre"]
    } == {
        "dramteatr39",
        "muzteatr39",
        "kaliningrad-puppet-theatre",
        "dom-iskusstv",
        "third-floor-theatre",
        "my-theatre-kaliningrad",
        "city-theatre-zheleznodorozhny",
        "act-opus",
    }
    assert {
        item["id"] for item in entities if item["flags"]["venue_page_candidate"]
    } == {
        "dramteatr39",
        "muzteatr39",
        "dom-iskusstv",
        "yantar-hall",
        "tretyakovka-kaliningrad",
        "kaliningrad-philharmonic",
    }
    assert next(item for item in entities if item["id"] == "act-opus")["canonicalVenue"] is None
    assert {
        item["id"] for item in registry["reviewedExclusions"]
    } >= {
        "yantar-hall-theatre-organization",
        "solyonaya-vorona",
        "cinema-lexical-match",
        "amphitheatre-lexical-match",
        "festival-name-theatre-match",
    }


def test_registry_schema_unique_ids_slugs_and_medallion_references_validate(registry):
    REGISTRY_MODULE.validate_registry(registry)
    ids = [item["id"] for item in registry["entities"]]
    slugs = [item["slug"] for item in registry["entities"]]
    assert len(ids) == len(set(ids))
    assert len(slugs) == len(set(slugs))

    broken = copy.deepcopy(registry)
    broken["entities"][1]["slug"] = broken["entities"][0]["slug"]
    with pytest.raises(REGISTRY_MODULE.RegistryValidationError, match="duplicate entity slug"):
        REGISTRY_MODULE.validate_registry(broken)

    broken = copy.deepcopy(registry)
    broken["entities"][0]["medallionSlug"] = "unreviewed-made-up-logo"
    with pytest.raises(REGISTRY_MODULE.RegistryValidationError, match="unknown medallion"):
        REGISTRY_MODULE.validate_registry(broken)


@pytest.mark.parametrize(
    ("event", "entity_id"),
    [
        ({"source_type": "dramteatr"}, "dramteatr39"),
        ({"source_url": "https://t.me/s/dramteatr39/42"}, "dramteatr39"),
        ({"sources": [{"source_type": "telegram", "username": "@dramteatr39"}]}, "dramteatr39"),
        ({"source_url": "https://vk.com/dramteatr39"}, "dramteatr39"),
        ({"source_url": "https://t.me/muztear39/42"}, "muzteatr39"),
        ({"source_url": "https://vk.com/muzteatr39"}, "muzteatr39"),
        ({"source_url": "https://vk.com/moyteatr_kld"}, "my-theatre-kaliningrad"),
        ({"source_url": "https://vk.com/gorodteatr39"}, "city-theatre-zheleznodorozhny"),
    ],
)
def test_exact_structured_parser_and_social_bindings_resolve_organizations(
    registry, event, entity_id
):
    result = REGISTRY_MODULE.resolve_event_memberships(event, registry)
    assert _ids(result, "organization_memberships") == [entity_id]
    assert _ids(result, "theatre_memberships") == [entity_id]
    assert result["venue_memberships"] == []
    assert _reason_codes(_membership(result, "theatre_memberships", entity_id)) == [
        "official_source"
    ]


def test_equal_and_subdomain_idna_domains_match_but_lookalikes_do_not(registry):
    exact = REGISTRY_MODULE.resolve_event_memberships(
        {"source_url": "https://театрэстрады39.рф/afisha/1"}, registry
    )
    subdomain = REGISTRY_MODULE.resolve_event_memberships(
        {"source_url": "https://tickets.домискусств39.рф/event/1"}, registry
    )
    structured_domain = REGISTRY_MODULE.resolve_event_memberships(
        {"sources": [{"source_domain": "events.домискусств39.рф"}]}, registry
    )
    punycode = REGISTRY_MODULE.resolve_event_memberships(
        {"source_url": "https://xn--39-6kcaud2faigcc4jwa.xn--p1ai/event/1"}, registry
    )
    lookalike = REGISTRY_MODULE.resolve_event_memberships(
        {"source_url": "https://театрэстрады39.рф.evil.example/event/1"}, registry
    )
    substring = REGISTRY_MODULE.resolve_event_memberships(
        {"source_url": "https://notdramteatr39.ru/event/1"}, registry
    )
    assert _ids(exact, "organization_memberships") == ["dom-iskusstv"]
    assert _ids(subdomain, "organization_memberships") == ["dom-iskusstv"]
    assert _ids(structured_domain, "organization_memberships") == ["dom-iskusstv"]
    assert _ids(punycode, "organization_memberships") == ["dom-iskusstv"]
    assert lookalike["organization_memberships"] == []
    assert substring["organization_memberships"] == []


def test_vk_group_id_is_exact_and_query_repost_or_lookalike_does_not_match(registry):
    wall = REGISTRY_MODULE.resolve_event_memberships(
        {"source_url": "https://vk.com/wall-20898960_123"}, registry
    )
    structured = REGISTRY_MODULE.resolve_event_memberships(
        {"sources": [{"platform": "vk", "owner_id": -20898960}]}, registry
    )
    arbitrary = REGISTRY_MODULE.resolve_event_memberships(
        {"source_url": "https://vk.com/wall-20898961_123"}, registry
    )
    query_repost = REGISTRY_MODULE.resolve_event_memberships(
        {"source_url": "https://vk.com/random_group?z=wall-20898960_123"}, registry
    )
    lookalike = REGISTRY_MODULE.resolve_event_memberships(
        {"source_url": "https://vk.com/koenigkukol390"}, registry
    )
    assert _ids(wall, "organization_memberships") == ["kaliningrad-puppet-theatre"]
    assert _ids(structured, "organization_memberships") == ["kaliningrad-puppet-theatre"]
    assert arbitrary["organization_memberships"] == []
    assert query_repost["organization_memberships"] == []
    assert lookalike["organization_memberships"] == []


def test_act_opus_offsite_source_is_organization_only_and_does_not_own_dom_molodezhi(registry):
    result = REGISTRY_MODULE.resolve_event_memberships(
        {
            "source_url": "https://actop.us/performances/offsite",
            "venue_name": "Дом молодёжи",
            "address": "Октябрьская 76",
            "city": "Калининград",
        },
        registry,
    )
    assert _ids(result, "organization_memberships") == ["act-opus"]
    assert _ids(result, "theatre_memberships") == ["act-opus"]
    assert result["venue_memberships"] == []


def test_guest_at_dramatic_home_keeps_different_organization_and_venue_memberships(registry):
    result = REGISTRY_MODULE.resolve_event_memberships(
        {
            "source_url": "https://vk.com/moyteatr_kld",
            "venue_name": "Драматический театр",
            "address": "Мира 4",
            "city": "Калининград",
        },
        registry,
    )
    assert _ids(result, "organization_memberships") == ["my-theatre-kaliningrad"]
    assert _ids(result, "venue_memberships") == ["dramteatr39"]
    assert _ids(result, "theatre_memberships") == [
        "dramteatr39",
        "my-theatre-kaliningrad",
    ]
    assert _reason_codes(
        _membership(result, "theatre_memberships", "dramteatr39")
    ) == ["venue"]
    assert _reason_codes(
        _membership(result, "theatre_memberships", "my-theatre-kaliningrad")
    ) == ["official_source"]


def test_same_entity_deduplicates_membership_but_preserves_all_exact_reasons(registry):
    result = REGISTRY_MODULE.resolve_event_memberships(
        {
            "source_type": "dramteatr",
            "source_urls": [
                "https://events.dramteatr39.ru/performance/1",
                "https://t.me/dramteatr39/1",
            ],
            "organizer_names": ["Калининградский областной драматический театр"],
            "venue_name": "Драматический театр",
            "address": "Мира 4",
            "city": "Калининград",
        },
        registry,
    )
    assert _ids(result, "organization_memberships") == ["dramteatr39"]
    assert _ids(result, "venue_memberships") == ["dramteatr39"]
    assert _ids(result, "theatre_memberships") == ["dramteatr39"]
    membership = result["theatre_memberships"][0]
    assert _reason_codes(membership) == [
        "official_source",
        "official_source",
        "official_source",
        "organizer",
        "venue",
    ]
    assert {reason["binding"] for reason in membership["reasons"]} >= {
        "parser:dramteatr",
        "telegram:dramteatr39",
        "domain:events.dramteatr39.ru",
    }


def test_yantar_hall_is_venue_only_and_source_never_creates_venue_membership(registry):
    source_only = REGISTRY_MODULE.resolve_event_memberships(
        {"source_type": "yantarhall", "source_url": "https://янтарьхолл.рф/afisha"},
        registry,
    )
    venue = REGISTRY_MODULE.resolve_event_memberships(
        {
            "venue_name": "Янтарь холл",
            "address": "Ленина 11",
            "city": "Светлогорск",
        },
        registry,
    )
    assert source_only["organization_memberships"] == []
    assert source_only["venue_memberships"] == []
    assert source_only["theatre_memberships"] == []
    assert _ids(venue, "venue_memberships") == ["yantar-hall"]
    assert venue["organization_memberships"] == []
    assert venue["theatre_memberships"] == []


def test_wrong_or_partial_canonical_tuple_fails_closed(registry):
    wrong_address = REGISTRY_MODULE.resolve_event_memberships(
        {
            "venue_name": "Драматический театр",
            "address": "Мира 40",
            "city": "Калининград",
        },
        registry,
    )
    partial_tuple = REGISTRY_MODULE.resolve_event_memberships(
        {"venue_name": "Драматический театр", "city": "Калининград"}, registry
    )
    approved_name_only = REGISTRY_MODULE.resolve_event_memberships(
        {"venue_name": "Драмтеатр"}, registry
    )
    assert wrong_address["venue_memberships"] == []
    assert partial_tuple["venue_memberships"] == []
    assert _ids(approved_name_only, "venue_memberships") == ["dramteatr39"]
    assert _reason_codes(approved_name_only["venue_memberships"][0]) == ["venue"]


@pytest.mark.parametrize(
    "event",
    [
        {"title": "Фестиваль у Янтарь-холла", "festival": "Театральный сезон"},
        {"title": "Ночь в кинотеатре", "topics": ["THEATRE"]},
        {"title": "Лекция в амфитеатре", "event_type": "спектакль"},
        {"organizer_names": ["Театральная гостиная Солёная ворона"]},
        {"organizer_names": ["Содружество актёров Николая Захарова"]},
        {"organizer_names": ["Драмтеатр на гастролях"]},
        {"source_url": "https://example.test/path/dramteatr39.ru/event"},
        {"source_url": "https://t.me/not_dramteatr39/1"},
    ],
)
def test_title_topic_festival_substring_and_reviewed_exclusions_fail_closed(registry, event):
    result = REGISTRY_MODULE.resolve_event_memberships(event, registry)
    assert result["organization_memberships"] == []
    assert result["venue_memberships"] == []
    assert result["theatre_memberships"] == []


def test_hash_and_resolution_order_are_deterministic(registry):
    reparsed = json.loads(
        json.dumps(registry, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    )
    assert REGISTRY_MODULE.registry_hash(registry) == REGISTRY_MODULE.registry_hash(reparsed)

    event = {
        "source_urls": [
            "https://vk.com/moyteatr_kld",
            "https://actop.us/performances/guest",
        ],
        "venue_name": "Драматический театр",
        "address": "Мира 4",
        "city": "Калининград",
    }
    first = REGISTRY_MODULE.resolve_event_memberships(event, registry)
    second = REGISTRY_MODULE.resolve_event_memberships(event, reparsed)
    assert first == second
    assert _ids(first, "theatre_memberships") == [
        "dramteatr39",
        "my-theatre-kaliningrad",
        "act-opus",
    ]
