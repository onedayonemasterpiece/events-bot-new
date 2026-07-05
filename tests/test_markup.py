import pytest

from markup import (
    simple_md_to_html,
    linkify_for_telegraph,
    linkify_phones_for_telegram_html,
    expose_links_for_vk,
    tel_href_for_phone_value,
)

def test_bold():
    assert simple_md_to_html('**bold** __bold__') == '<b>bold</b> <b>bold</b>'

def test_italic():
    assert simple_md_to_html('*it* _it_') == '<i>it</i> <i>it</i>'

def test_link():
    assert simple_md_to_html('see [site](https://example.com)') == 'see <a href="https://example.com">site</a>'

def test_header_and_newline():
    assert simple_md_to_html('# Title\ntext') == '<h1>Title</h1><br>text'

def test_emoji_preserved():
    assert simple_md_to_html('smile 😀') == 'smile 😀'

def test_no_italic_in_urls():
    url = 'https://example.com/image_2024_08.jpg'
    assert simple_md_to_html(url) == url

def test_link_with_underscore_url():
    assert (
        simple_md_to_html('[doc](https://site.com/a_b_c)')
        == '<a href="https://site.com/a_b_c">doc</a>'
    )

def test_plain_underscores_unchanged():
    assert simple_md_to_html('file_name') == 'file_name'

def test_linkify_plain_url():
    assert linkify_for_telegraph("https://example.com") == '<a href="https://example.com">https://example.com</a>'

def test_linkify_text_url():
    assert linkify_for_telegraph("Site (https://example.com)") == '<a href="https://example.com">Site</a>'

def test_linkify_markdown_link():
    assert linkify_for_telegraph("[site](https://example.com)") == '<a href="https://example.com">site</a>'


def test_linkify_vk_internal_link():
    assert (
        linkify_for_telegraph("[club9118984|Калининградском музее]")
        == '<a href="https://vk.com/club9118984">Калининградском музее</a>'
    )


def test_linkify_telegram_mention():
    assert (
        linkify_for_telegraph("@ruin_keepers_admin")
        == '<a href="https://t.me/ruin_keepers_admin">@ruin_keepers_admin</a>'
    )


def test_linkify_telegram_mention_inside_anchor_untouched():
    html = '<a href="https://example.com">@ruin_keepers_admin</a>'
    assert linkify_for_telegraph(html) == html


def test_linkify_telegram_mention_not_email():
    assert linkify_for_telegraph("info@example.com") == "info@example.com"

def test_expose_links_from_html():
    assert expose_links_for_vk('see <a href="https://example.com">site</a>') == 'see site (https://example.com)'

def test_expose_links_from_md():
    assert expose_links_for_vk('see [site](https://example.com)') == 'see site (https://example.com)'


def test_linkify_phone_with_country_code():
    """Phone with +7 country code becomes clickable tel: link."""
    assert (
        linkify_for_telegraph("+7 (495) 123-45-67")
        == '<a href="tel:+74951234567">+7 (495) 123-45-67</a>'
    )


def test_linkify_phone_with_8():
    """Phone with 8 prefix converts to +7 in tel: link."""
    assert (
        linkify_for_telegraph("8-800-555-35-35")
        == '<a href="tel:+78005553535">8-800-555-35-35</a>'
    )


def test_linkify_phone_local():
    """Local phone without country code gets +7 prefix."""
    assert (
        linkify_for_telegraph("(4012) 12-34-56")
        == '<a href="tel:+74012123456">(4012) 12-34-56</a>'
    )


def test_linkify_phone_inside_anchor_untouched():
    """Phone already inside a link is not modified."""
    html = '<a href="tel:+74951234567">+7 (495) 123-45-67</a>'
    assert linkify_for_telegraph(html) == html


def test_tel_href_for_phone_value_normalizes_russian_numbers():
    assert tel_href_for_phone_value("tel:+74012463635") == "tel:+74012463635"
    assert tel_href_for_phone_value("+7 (4012) 46-36-35") == "tel:+74012463635"
    assert tel_href_for_phone_value("8 (4012) 46-36-35") == "tel:+74012463635"


def test_linkify_phones_for_telegram_html_keeps_existing_links():
    html = (
        'Запись: +7 (4012) 46-36-35. '
        '<a href="https://example.com">+7 (999) 000-00-00</a>'
    )
    assert linkify_phones_for_telegram_html(html) == (
        'Запись: <a href="tel:+74012463635">+7 (4012) 46-36-35</a>. '
        '<a href="https://example.com">+7 (999) 000-00-00</a>'
    )


def test_linkify_phones_for_telegram_html_does_not_cross_numbered_list_lines():
    html = "1. Запись — +7 962 255-54-91\n2. Другая экскурсия"

    assert linkify_phones_for_telegram_html(html) == (
        '1. Запись — <a href="tel:+79622555491">+7 962 255-54-91</a>\n'
        "2. Другая экскурсия"
    )
