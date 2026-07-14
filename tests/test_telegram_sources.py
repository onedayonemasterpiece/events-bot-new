from __future__ import annotations

from telegram_sources import (
    canonical_tg_sources,
    canonicalize_tg_url,
    normalize_tg_username,
    parse_tg_post_url,
)
from smart_update_identity import _normalize_url as normalize_identity_url


def test_normalize_tg_username_accepts_public_channel_urls() -> None:
    assert normalize_tg_username("https://t.me/ecoklgd") == "ecoklgd"
    assert normalize_tg_username("https://t.me/ecoklgd/123?single") == "ecoklgd"
    assert normalize_tg_username("https://telegram.me/ecodvor39") == "ecodvor39"
    assert normalize_tg_username("telegram.me/ecodvor39/926?single") == "ecodvor39"
    assert normalize_tg_username("tg://resolve?domain=ecoklgd") == "ecoklgd"


def test_telegram_me_urls_are_canonicalized_to_one_source_identity() -> None:
    assert (
        canonicalize_tg_url("http://www.telegram.me/ecodvor39/926?single")
        == "https://t.me/ecodvor39/926?single"
    )
    assert parse_tg_post_url("https://telegram.me/s/ecodvor39/927?single") == (
        "ecodvor39",
        927,
    )
    assert canonicalize_tg_url("https://example.com/ecodvor39/926") is None
    assert parse_tg_post_url("https://telegram.me/c/123/926") is None
    assert normalize_identity_url("https://telegram.me/ecodvor39/926") == normalize_identity_url(
        "https://t.me/ecodvor39/926"
    )


def test_canonical_sources_include_ecoklgd() -> None:
    source = next(item for item in canonical_tg_sources() if item.username == "ecoklgd")

    assert source.trust_level == "medium"
    assert source.default_location is None


def test_canonical_sources_include_official_ecodvor_channel() -> None:
    source = next(item for item in canonical_tg_sources() if item.username == "ecodvor39")

    assert source.trust_level == "high"
    assert source.default_location is None


def test_canonical_sources_include_kldscope_with_grounded_default_location() -> None:
    source = next(item for item in canonical_tg_sources() if item.username == "kldscope_news")

    assert source.trust_level == "high"
    assert source.default_location == (
        "КЛДскоп, Земельная 12, 1 этаж, кабинет 3, Калининград"
    )
