from source_parsing.telegram.handlers import (
    _extract_message_link_items,
    _infer_ticket_link_from_message_links,
    _parse_tg_source_url,
    _ticket_link_is_explicitly_non_admission,
)


def test_refines_broad_ticket_link_from_registration_entity() -> None:
    message = {
        "links": [
            {
                "url": "https://kgd80.ru/",
                "text": "80 историй о главном",
                "source": "entity",
            },
            {
                "url": "https://kgd80.ru/sobytiya/mirnaya-zhizn/?register=1",
                "text": "Бесплатно, по регистрации",
                "source": "entity",
            },
        ]
    }

    refined = _infer_ticket_link_from_message_links(
        _extract_message_link_items(message),
        current="https://kgd80.ru",
    )

    assert refined == "https://kgd80.ru/sobytiya/mirnaya-zhizn/?register=1"


def test_does_not_replace_broad_ticket_link_with_non_ticket_entity() -> None:
    message = {
        "links": [
            {
                "url": "https://kgd80.ru/sobytiya/about",
                "text": "80 историй о главном",
                "source": "entity",
            }
        ]
    }

    refined = _infer_ticket_link_from_message_links(
        _extract_message_link_items(message),
        current="https://kgd80.ru",
    )

    assert refined is None


def test_does_not_infer_vk_hashtag_search_as_ticket_link() -> None:
    refined = _infer_ticket_link_from_message_links(
        ["https://vk.com/search/statuses?q=%23%D0%BC%D1%83%D0%B7%D1%8B%D0%BA%D0%B0"],
        current=None,
    )

    assert refined is None


def test_does_not_infer_sole_unlabelled_external_url_as_ticket() -> None:
    refined = _infer_ticket_link_from_message_links(
        [{"url": "https://example.org/about", "text": "Подробнее"}],
        current=None,
    )

    # Generic details are not admission evidence on the server fallback path.
    assert refined is None


def test_ecodvor_tinkoff_support_link_is_not_ticket() -> None:
    links = [
        {
            "url": "https://www.tinkoff.ru/rm/shavarina.natalya1/03BKq67856",
            "text": "Поддержать Экодвор",
        }
    ]

    assert _infer_ticket_link_from_message_links(links, current=None) is None
    assert _ticket_link_is_explicitly_non_admission(links[0]["url"], links) is True


def test_explicit_registration_link_remains_ticket() -> None:
    refined = _infer_ticket_link_from_message_links(
        [{"url": "https://example.org/form", "text": "Регистрация на событие"}],
        current=None,
    )

    assert refined == "https://example.org/form"


def test_telegram_me_link_payload_is_canonicalized_before_source_dedup() -> None:
    items = _extract_message_link_items(
        {
            "links": [
                {"url": "https://telegram.me/ecodvor39/926", "text": "Источник"},
                {"url": "https://t.me/ecodvor39/926", "text": "Тот же источник"},
            ]
        }
    )

    assert [item["url"] for item in items] == ["https://t.me/ecodvor39/926"]
    assert _parse_tg_source_url("https://telegram.me/s/ecodvor39/927?single") == (
        "ecodvor39",
        927,
    )
