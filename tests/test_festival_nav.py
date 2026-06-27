import pytest

import main
from markup import FEST_NAV_START, FEST_NAV_END
from main import FOOTER_LINK_HTML

NAV_HTML = '<p>nav</p>'


def test_apply_festival_nav_insert_when_missing():
    html = '<p>start</p>'
    updated, changed, removed, replaced = main.apply_festival_nav(html, NAV_HTML)
    assert changed is True
    assert removed == 0
    assert replaced is False
    assert updated.startswith('<p>start</p>')
    assert updated.count(FEST_NAV_START) == 1
    assert '<!-- FEST_NAV_START -->' not in updated
    assert '<!-- FEST_NAV_END -->' not in updated
    assert '<!--FEST_NAV_START-->' not in updated
    assert '<!--FEST_NAV_END-->' not in updated
    assert '#near-festivals:hash:' in updated
    assert updated.endswith(FOOTER_LINK_HTML)


def test_apply_festival_nav_replace_existing():
    html = f'<p>start</p>{FEST_NAV_START}<p>old</p>{FEST_NAV_END}'
    updated, changed, removed, _ = main.apply_festival_nav(html, NAV_HTML)
    assert changed is True
    assert removed == 1
    assert NAV_HTML in updated
    assert '<p>old</p>' not in updated


def test_apply_festival_nav_idempotent():
    html = '<p>start</p>'
    first, changed, _, _ = main.apply_festival_nav(html, NAV_HTML)
    assert changed is True
    second, changed2, removed, replaced = main.apply_festival_nav(first, NAV_HTML)
    assert changed2 is False
    assert removed == 0
    assert replaced is False
    assert first == second


def test_apply_festival_nav_removes_legacy_heading():
    html = '<p>start</p><h3>Ближайшие фестивали</h3><p>old</p>'
    updated, changed, removed, _ = main.apply_festival_nav(html, NAV_HTML)
    assert changed is True
    assert removed == 1
    assert '<h3>Ближайшие фестивали</h3>' not in updated
    assert updated.count(FEST_NAV_START) == 1


def test_apply_festival_nav_removes_paragraph_strong_heading():
    html = '<p>start</p><p><strong>Ближайшие фестивали</strong></p><p>old</p>'
    updated, changed, removed, _ = main.apply_festival_nav(html, NAV_HTML)
    assert changed is True
    assert removed == 1
    assert 'Ближайшие фестивали' not in updated
    assert updated.count(FEST_NAV_START) == 1


def test_apply_festival_nav_rewrites_spaced_markers():
    html = '<p>start</p><!-- FEST_NAV_START --><p>old</p><!-- FEST_NAV_END -->'
    updated, changed, removed, replaced = main.apply_festival_nav(html, NAV_HTML)
    assert changed is True
    assert removed == 1
    assert replaced is True
    assert updated.count(FEST_NAV_START) == 1
    assert '<!-- FEST_NAV_START -->' not in updated
    assert '<!-- FEST_NAV_END -->' not in updated


def test_apply_festival_nav_rewrites_uppercase_markers():
    html = '<p>start</p><!--FEST_NAV_START--><p>old</p><!--FEST_NAV_END-->'
    updated, changed, removed, replaced = main.apply_festival_nav(html, NAV_HTML)
    assert changed is True
    assert removed == 1
    assert replaced is True
    assert updated.count(FEST_NAV_START) == 1
    assert '<!--FEST_NAV_START-->' not in updated
    assert '<!--FEST_NAV_END-->' not in updated


def test_apply_festival_nav_deduplicates_multiple_blocks():
    html = (
        f"<p>start</p>{FEST_NAV_START}<p>old</p>{FEST_NAV_END}"
        f"<p>mid</p>{FEST_NAV_START}<p>old2</p>{FEST_NAV_END}"
    )
    updated, changed, removed, _ = main.apply_festival_nav(html, NAV_HTML)
    assert changed is True
    assert removed == 2
    assert updated.count(FEST_NAV_START) == 1
    assert '<p>old</p>' not in updated
    assert '<p>old2</p>' not in updated


def test_apply_festival_nav_removes_heading_with_subheading():
    html = '<p>start</p><h3>Ближайшие фестивали</h3><h4>old</h4><p>end</p>'
    updated, changed, removed, _ = main.apply_festival_nav(html, NAV_HTML)
    assert changed is True
    assert removed == 1
    assert '<h3>Ближайшие фестивали</h3>' not in updated
    assert '<h4>old</h4>' not in updated
    assert updated.count(FEST_NAV_START) == 1


def test_apply_footer_link_idempotent():
    html = '<p>start</p>'
    first = main.apply_footer_link(html)
    second = main.apply_footer_link(first)
    assert first == second
    assert second.count('https://t.me/kenigevents') == 1
    assert second.count('https://t.me/kldevents') == 1
    assert second.count('https://vk.com/kenigeventsofficial') == 1
    assert second.count('https://vk.com/klgdevents') == 1
    assert second.count('https://vk.ru/im/channels/-239844596') == 1
    assert second.count(main.MAX_SOCIAL_URL) == 1
    assert '<p><b>Полюбить Калининград</b></p>' in second
    assert '<p>Telegram:' in second
    assert '<p>ВК:' in second
    assert '<p>Max:' in second


def test_apply_footer_link_replaces_legacy_single_link_footer():
    html = (
        '<p>start</p>'
        '<p>&#8203;</p>'
        '<p><a href="https://t.me/kenigevents">Полюбить Калининград Анонсы</a></p>'
        '<p>&#8203;</p>'
    )
    updated = main.apply_footer_link(html)
    assert updated.count('https://t.me/kenigevents') == 1
    assert 'Полюбить Калининград Анонсы</a></p>' not in updated
    assert updated.endswith(FOOTER_LINK_HTML)


def test_apply_footer_link_replaces_social_footer_without_max():
    html = (
        '<p>start</p>'
        '<p>&#8203;</p>'
        '<p><b>Полюбить Калининград</b></p>'
        '<p>Telegram: <a href="https://t.me/kenigevents">Анонсы</a> · '
        '<a href="https://t.me/kldevents">Афиша</a></p>'
        '<p>ВК: <a href="https://vk.com/kenigeventsofficial">Анонсы</a> · '
        '<a href="https://vk.com/klgdevents">Афиша</a> · '
        '<a href="https://vk.ru/im/channels/-239844596">канал Афиши</a></p>'
        '<p>&#8203;</p>'
    )
    updated = main.apply_footer_link(html)
    assert updated.count(main.MAX_SOCIAL_URL) == 1
    assert updated.count('<p><b>Полюбить Калининград</b></p>') == 1
    assert updated.endswith(FOOTER_LINK_HTML)

def test_apply_footer_link_replaces_telegraph_target_attrs_footer_without_max():
    html = (
        '<p>start</p>'
        '<p>\u200b</p>'
        '<p><b>Полюбить Калининград</b></p>'
        '<p>Telegram: <a href="https://t.me/kenigevents" target="_blank">Анонсы</a> · '
        '<a href="https://t.me/kldevents" target="_blank">Афиша</a></p>'
        '<p>ВК: <a href="https://vk.com/kenigeventsofficial" target="_blank">Анонсы</a> · '
        '<a href="https://vk.com/klgdevents" target="_blank">Афиша</a> · '
        '<a href="https://vk.ru/im/channels/-239844596" target="_blank">канал Афиши</a></p>'
        '<p>\u200b</p>'
    )
    updated = main.apply_footer_link(html)
    assert updated.count('<p><b>Полюбить Калининград</b></p>') == 1
    assert updated.count(main.MAX_SOCIAL_URL) == 1
    assert 'target="_blank">канал Афиши</a></p><p>\u200b</p><p>&#8203;</p>' not in updated
    assert updated.endswith(FOOTER_LINK_HTML)
