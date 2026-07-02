from source_parsing.telegram.handlers import (
    _extract_message_link_items,
    _infer_ticket_link_from_message_links,
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
