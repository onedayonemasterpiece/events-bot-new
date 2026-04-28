from guide_excursions.sources import canonical_guide_sources


def test_canonical_guide_sources_include_murnikova_channel() -> None:
    sources = canonical_guide_sources()
    by_username = {source.username: source for source in sources}

    assert "murnikovat" in by_username
    source = by_username["murnikovat"]
    assert source.profile_slug == "tatyana-murnikova"
    assert source.display_name == "Татьяна Мурникова"
    assert source.source_kind == "guide_personal"


def test_canonical_guide_sources_are_normalized_unique_and_sorted() -> None:
    sources = canonical_guide_sources()
    usernames = [source.username for source in sources]

    assert usernames == sorted(usernames)
    assert len(usernames) == len(set(usernames))
    assert all(username == username.lower() and "@" not in username for username in usernames)
