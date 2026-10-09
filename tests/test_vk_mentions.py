"""Tests for VK mentions registry and MCP tools."""

import asyncio
import json
import pytest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from vk_mentions_registry import (
    VKMentionEntry,
    VKMentionsRegistry,
    VKMentionsRegistryError,
    VK_MENTIONS_CACHE_PATH,
    VK_MENTIONS_REGISTRY_URL,
    VK_MENTIONS_TTL_SECONDS,
    build_vk_mention_markup,
    escape_vk_mention_text,
    load_or_create_registry,
    parse_vk_mention_markup,
    refresh_registry,
    start_registry_refresh_scheduler,
)
from vk_mentions_mcp import (
    VKMentionSearchResult,
    VKMentionValidationResult,
    VKMentionsMCPTools,
    build_guide_vk_digest_text_with_mentions,
    build_mentions_for_text,
)


SAMPLE_REGISTRY_JSON = {
    "schemaVersion": "vk_mentions_registry_v1",
    "registryVersion": 1,
    "entries": [
        {
            "id": "lovekenig",
            "type": "community",
            "vkId": "241261191",
            "label": "Полюбить Калининград",
            "status": "verified",
            "aliases": ["lovekenig", "love kaliningrad"],
            "metadata": {"description": "Main city community"},
        },
        {
            "id": "popadin",
            "type": "user",
            "vkId": "123456789",
            "label": "Popadin",
            "status": "candidate",
            "aliases": [],
            "metadata": {},
        },
        {
            "id": "yantar-hall",
            "type": "community",
            "vkId": "987654321",
            "label": "Янтарь Холл",
            "status": "verified",
            "aliases": ["yantar", "янтарь"],
            "metadata": {"venue": True},
        },
        {
            "id": "koihm",
            "type": "community",
            "vkId": "555666777",
            "label": "KOIHM",
            "status": "candidate",
            "aliases": [],
            "metadata": {},
        },
    ],
}


class TestVKRegistryParsing:
    def test_parse_valid_registry(self):
        registry = VKMentionsRegistry.from_json(SAMPLE_REGISTRY_JSON)
        assert len(registry.entries) == 4
        assert len(registry.verified_entries) == 2
        assert "lovekenig" in registry.verified_entries
        assert "yantar-hall" in registry.verified_entries
        assert "popadin" not in registry.verified_entries
        assert "koihm" not in registry.verified_entries

    def test_parse_invalid_schema_version(self):
        bad_data = {**SAMPLE_REGISTRY_JSON, "schemaVersion": "wrong"}
        with pytest.raises(VKMentionsRegistryError, match="Invalid schema version"):
            VKMentionsRegistry.from_json(bad_data)

    def test_duplicate_id_raises(self):
        bad_data = {
            **SAMPLE_REGISTRY_JSON,
            "entries": SAMPLE_REGISTRY_JSON["entries"] + [SAMPLE_REGISTRY_JSON["entries"][0]],
        }
        with pytest.raises(VKMentionsRegistryError, match="Duplicate entry id"):
            VKMentionsRegistry.from_json(bad_data)

    def test_invalid_type_raises(self):
        bad_data = {
            **SAMPLE_REGISTRY_JSON,
            "entries": [{"id": "test", "type": "invalid", "vkId": "123", "label": "Test", "status": "verified"}],
        }
        with pytest.raises(VKMentionsRegistryError, match="type must be one of"):
            VKMentionsRegistry.from_json(bad_data)

    def test_non_numeric_vk_id_raises(self):
        bad_data = {
            **SAMPLE_REGISTRY_JSON,
            "entries": [{"id": "test", "type": "community", "vkId": "abc", "label": "Test", "status": "verified"}],
        }
        with pytest.raises(VKMentionsRegistryError, match="vkId must be numeric"):
            VKMentionsRegistry.from_json(bad_data)


class TestVKMentionEntry:
    def test_mention_markup_community(self):
        entry = VKMentionEntry(
            id="test",
            type="community",
            vk_id="241261191",
            label="Полюбить Калининград",
            status="verified",
        )
        assert entry.mention_markup() == "[club241261191|Полюбить Калининград]"

    def test_mention_markup_user(self):
        entry = VKMentionEntry(
            id="test",
            type="user",
            vk_id="123456789",
            label="Popadin",
            status="verified",
        )
        assert entry.mention_markup() == "[id123456789|Popadin]"

    def test_matches_query_by_label(self):
        entry = VKMentionEntry(
            id="test",
            type="community",
            vk_id="241261191",
            label="Полюбить Калининград",
            status="verified",
        )
        assert entry.matches_query("Полюбить Калининград")
        assert entry.matches_query("полюбить калининград")

    def test_matches_query_by_id(self):
        registry = VKMentionsRegistry.from_json(SAMPLE_REGISTRY_JSON)
        entry = registry.entries["lovekenig"]
        assert entry.matches_query("club241261191")
        assert entry.matches_query("CLUB241261191")

    def test_matches_query_by_alias(self):
        registry = VKMentionsRegistry.from_json(SAMPLE_REGISTRY_JSON)
        entry = registry.entries["lovekenig"]
        assert entry.matches_query("lovekenig")
        assert entry.matches_query("love kaliningrad")


class TestVKMentionsRegistry:
    def test_search_verified(self):
        registry = VKMentionsRegistry.from_json(SAMPLE_REGISTRY_JSON)
        results = registry.search_verified("Полюбить", limit=3)
        assert len(results) == 1
        assert results[0].id == "lovekenig"

    def test_search_verified_by_alias(self):
        registry = VKMentionsRegistry.from_json(SAMPLE_REGISTRY_JSON)
        results = registry.search_verified("lovekenig", limit=3)
        assert len(results) == 1
        assert results[0].id == "lovekenig"

    def test_search_verified_limit(self):
        registry = VKMentionsRegistry.from_json(SAMPLE_REGISTRY_JSON)
        results = registry.search_verified("Полюбить", limit=1)
        assert len(results) == 1
        assert results[0].id == "lovekenig"

    def test_get_verified_by_vk_id(self):
        registry = VKMentionsRegistry.from_json(SAMPLE_REGISTRY_JSON)
        entry = registry.get_verified_by_vk_id("241261191", "community")
        assert entry is not None
        assert entry.id == "lovekenig"

        entry = registry.get_verified_by_vk_id("241261191", "user")
        assert entry is None

    def test_all_verified(self):
        registry = VKMentionsRegistry.from_json(SAMPLE_REGISTRY_JSON)
        verified = registry.all_verified()
        assert len(verified) == 2

    def test_is_stale(self):
        import time
        registry = VKMentionsRegistry.from_json(SAMPLE_REGISTRY_JSON)
        registry.last_refresh = 0
        assert registry.is_stale()

        registry.last_refresh = time.time()
        assert not registry.is_stale()


class TestVKMarkupFunctions:
    def test_build_vk_mention_markup_community(self):
        markup = build_vk_mention_markup("241261191", "Полюбить Калининград", "community")
        assert markup == "[club241261191|Полюбить Калининград]"

    def test_build_vk_mention_markup_user(self):
        markup = build_vk_mention_markup("123456789", "Popadin", "user")
        assert markup == "[id123456789|Popadin]"

    def test_parse_vk_mention_markup_community(self):
        parsed = parse_vk_mention_markup("[club241261191|Полюбить Калининград]")
        assert parsed == ("club", "241261191", "Полюбить Калининград")

    def test_parse_vk_mention_markup_user(self):
        parsed = parse_vk_mention_markup("[id123456789|Popadin]")
        assert parsed == ("id", "123456789", "Popadin")

    def test_parse_vk_mention_markup_public(self):
        parsed = parse_vk_mention_markup("[public987654321|Test]")
        assert parsed == ("public", "987654321", "Test")

    def test_parse_invalid_markup(self):
        assert parse_vk_mention_markup("invalid") is None
        assert parse_vk_mention_markup("[club123]") is None
        assert parse_vk_mention_markup("club123|label") is None

    def test_escape_vk_mention_text(self):
        assert escape_vk_mention_text("Test|Label") == "Test\\|Label"
        assert escape_vk_mention_text("Test[Label]") == "Test\\[Label\\]"
        assert escape_vk_mention_text("Normal text") == "Normal text"


class TestVKMentionsMCPTools:
    @pytest.fixture
    def registry(self):
        return VKMentionsRegistry.from_json(SAMPLE_REGISTRY_JSON)

    @pytest.fixture
    def tools(self, registry):
        return VKMentionsMCPTools(registry=registry)

    @pytest.mark.asyncio
    async def test_search_mentions_verified_only(self, tools):
        result = await tools.search_mentions("Полюбить", limit=3, only_verified=True)
        assert isinstance(result, VKMentionSearchResult)
        assert len(result.entries) == 1
        assert result.entries[0].id == "lovekenig"
        assert result.query == "Полюбить"

    @pytest.mark.asyncio
    async def test_search_mentions_all(self, tools):
        result = await tools.search_mentions("Popadin", limit=10, only_verified=False)
        assert len(result.entries) == 1
        assert result.entries[0].id == "popadin"
        
        result = await tools.search_mentions("club", limit=10, only_verified=False)
        assert len(result.entries) == 0  # "club" doesn't partially match any label/alias

    @pytest.mark.asyncio
    async def test_validate_mention_verified(self, tools):
        result = await tools.validate_mention("241261191", "community", "Полюбить Калининград")
        assert isinstance(result, VKMentionValidationResult)
        assert result.valid is True
        assert result.entry is not None
        assert result.entry.id == "lovekenig"
        assert result.markup == "[club241261191|Полюбить Калининград]"

    @pytest.mark.asyncio
    async def test_validate_mention_label_mismatch(self, tools):
        result = await tools.validate_mention("241261191", "community", "Wrong Label")
        assert result.valid is False
        assert result.entry is not None
        assert result.entry.id == "lovekenig"
        assert "Label mismatch" in result.reason

    @pytest.mark.asyncio
    async def test_validate_mention_not_found(self, tools):
        result = await tools.validate_mention("999999999", "community")
        assert result.valid is False
        assert result.entry is None
        assert "No verified" in result.reason

    @pytest.mark.asyncio
    async def test_validate_mention_invalid_vk_id(self, tools):
        result = await tools.validate_mention("abc", "community")
        assert result.valid is False
        assert "Invalid VK ID" in result.reason

    @pytest.mark.asyncio
    async def test_validate_mention_markup_valid(self, tools):
        result = await tools.validate_mention_markup("[club241261191|Полюбить Калининград]")
        assert result.valid is True
        assert result.markup == "[club241261191|Полюбить Калининград]"

    @pytest.mark.asyncio
    async def test_validate_mention_markup_invalid(self, tools):
        result = await tools.validate_mention_markup("[club999999999|Unknown]")
        assert result.valid is False

    @pytest.mark.asyncio
    async def test_get_mention_markup(self, tools):
        markup = await tools.get_mention_markup("241261191", "community")
        assert markup == "[club241261191|Полюбить Калининград]"

        markup = await tools.get_mention_markup("999999999", "community")
        assert markup is None

    @pytest.mark.asyncio
    async def test_list_verified_mentions(self, tools):
        entries = await tools.list_verified_mentions(limit=10)
        assert len(entries) == 2


class TestBuildMentionsForText:
    @pytest.fixture
    def registry(self):
        return VKMentionsRegistry.from_json(SAMPLE_REGISTRY_JSON)

    @pytest.mark.asyncio
    async def test_build_mentions_for_text_basic(self, registry):
        text = "Экскурсия с гидом Иван Петров"
        enhanced, used = await build_mentions_for_text(
            text,
            guide_names=["Иван Петров"],
            registry=registry,
        )
        assert enhanced == text

    @pytest.mark.asyncio
    async def test_build_mentions_adds_markup(self, registry):
        text = "Экскурсия в Янтарь Холл"
        enhanced, used = await build_mentions_for_text(
            text,
            venue_names=["Янтарь Холл"],
            registry=registry,
        )
        assert "[club987654321|Янтарь Холл]" in enhanced
        assert len(used) == 1
        assert used[0].id == "yantar-hall"

    @pytest.mark.asyncio
    async def test_build_mentions_max_limit(self, registry):
        text = "Test"
        enhanced, used = await build_mentions_for_text(
            text,
            guide_names=["Полюбить Калининград", "Янтарь Холл", "KOIHM", "Popadin"],
            max_mentions=2,
            registry=registry,
        )
        assert len(used) <= 2

    @pytest.mark.asyncio
    async def test_build_mentions_excludes_self(self, registry):
        text = "Test"
        enhanced, used = await build_mentions_for_text(
            text,
            guide_names=["Полюбить Калининград"],
            exclude_self_vk_id="241261191",
            exclude_self_type="community",
            registry=registry,
        )
        assert "[club241261191|Полюбить Калининград]" not in enhanced
        assert len(used) == 0


class TestBuildGuideVKDigestTextWithMentions:
    @pytest.fixture
    def registry(self):
        return VKMentionsRegistry.from_json(SAMPLE_REGISTRY_JSON)

    @pytest.mark.asyncio
    async def test_enhances_digest_with_mentions(self, registry):
        base_text = """Новые экскурсии: 2 выхода, 15 октября и 20 октября

1. Прогулка по центру
🗓 15 октября, 14:00
📍 Калининград
Гид: Анна Петрова
Интересный маршрут по историческому центру
Запись: @guide_anna

----------

2. Янтарь Холл экскурсия
🗓 20 октября, 16:00
📍 Янтарь Холл
Организатор: Янтарь Холл
Уникальная экскурсия по залу

#экскурсии #Калининград #УхтыКалининград"""

        enhanced = await build_guide_vk_digest_text_with_mentions(
            base_text,
            [
                {"guide_names": ["Анна Петрова"], "organizer_names": [], "city": "Калининград", "meeting_point": "Центр"},
                {"guide_names": [], "organizer_names": ["Янтарь Холл"], "city": "Калининград", "meeting_point": "Янтарь Холл"},
            ],
            max_mentions=3,
            registry=registry,
        )

        assert "[club987654321|Янтарь Холл]" in enhanced

    @pytest.mark.asyncio
    async def test_does_not_add_duplicate_mentions(self, registry):
        base_text = "Test with Полюбить Калининград"
        enhanced = await build_guide_vk_digest_text_with_mentions(
            base_text,
            [
                {"guide_names": ["Полюбить Калининград"], "organizer_names": ["Полюбить Калининград"], "city": "Калининград"},
                {"guide_names": ["Полюбить Калининград"], "organizer_names": ["Полюбить Калининград"], "city": "Калининград"},
            ],
            max_mentions=3,
            registry=registry,
        )
        assert enhanced.count("[club241261191|Полюбить Калининград]") == 1


class TestRefreshRegistry:
    @pytest.mark.asyncio
    async def test_refresh_registry_from_cache(self, tmp_path, monkeypatch):
        cache_file = tmp_path / "test_cache.json"
        cache_file.write_text(json.dumps({
            **SAMPLE_REGISTRY_JSON,
            "refreshedAt": asyncio.get_event_loop().time(),
            "etag": "test-etag",
            "contentHash": "test-hash",
        }))

        monkeypatch.setattr("vk_mentions_registry.VK_MENTIONS_CACHE_PATH", cache_file)

        with patch("vk_mentions_registry.httpx.AsyncClient") as mock_client:
            mock_resp = MagicMock()
            mock_resp.status_code = 304
            mock_resp.headers = {"ETag": "test-etag"}
            mock_client.return_value.__aenter__.return_value.get = AsyncMock(return_value=mock_resp)

            registry = await load_or_create_registry()
            updated = await refresh_registry(registry)
            assert updated is False
            assert len(registry.entries) == 4


if __name__ == "__main__":
    pytest.main([__file__, "-v"])