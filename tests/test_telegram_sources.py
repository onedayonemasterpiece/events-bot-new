from __future__ import annotations

from telegram_sources import canonical_tg_sources, normalize_tg_username


def test_normalize_tg_username_accepts_public_channel_urls() -> None:
    assert normalize_tg_username("https://t.me/ecoklgd") == "ecoklgd"
    assert normalize_tg_username("https://t.me/ecoklgd/123?single") == "ecoklgd"
    assert normalize_tg_username("tg://resolve?domain=ecoklgd") == "ecoklgd"


def test_canonical_sources_include_ecoklgd() -> None:
    source = next(item for item in canonical_tg_sources() if item.username == "ecoklgd")

    assert source.trust_level == "medium"
    assert source.default_location is None
