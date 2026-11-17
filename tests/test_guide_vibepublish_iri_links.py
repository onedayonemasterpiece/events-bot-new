from guide_excursions.vibepublish_client import html_content


def test_internationalized_digest_link_keeps_registration_and_query_identity():
    content = html_content(
        '<a href="https://студтуризм.рф/events/ZhbSOhuGmL?next=%2Fанкета&amp;mode=1#запись">Запись</a>'
    )
    assert content["paragraphs"] == [[{
        "kind": "link",
        "label": "Запись",
        "url": ("https://xn--d1agcrrehbhc.xn--p1ai/events/ZhbSOhuGmL"
                "?next=%2F%D0%B0%D0%BD%D0%BA%D0%B5%D1%82%D0%B0&mode=1"
                "#%D0%B7%D0%B0%D0%BF%D0%B8%D1%81%D1%8C"),
    }]]


def test_ascii_registration_link_is_copied_verbatim():
    url = "https://example.org/sign/%2f?next=a%2Fb%2BC"
    assert html_content(f'<a href="{url}">Запись</a>')["paragraphs"][0][0]["url"] == url
