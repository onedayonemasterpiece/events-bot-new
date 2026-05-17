from markup import sanitize_for_vk


def test_sanitize_for_vk_strips_html_and_tg_emoji():
    src = (
        '<tg-emoji emoji-id="1"> 🗞 </tg-emoji> <i>Личный бренд на дзене</i>\n'
        'Приглашаем... <a href="https://forms.yandex.ru/u/68a392b2">регистрация</a><br>'
        '<i>Создаём своё имя...</i> <tg-spoiler>секрет</tg-spoiler>'
    )
    expected = (
        '🗞 _Личный бренд на дзене_\n'
        'Приглашаем... регистрация (https://forms.yandex.ru/u/68a392b2)\n'
        '_Создаём своё имя..._ секрет'
    )
    assert sanitize_for_vk(src) == expected


def test_sanitize_for_vk_removes_polubit_39_block():
    src = '📂 Полюбить 39\nhttps://t.me/addlist/foo\n\nОсновной текст'
    assert sanitize_for_vk(src) == 'Основной текст'


def test_sanitize_for_vk_removes_polubit_39_inline_link():
    src = "📂 Полюбить 39 (<a href='https://t.me/addlist/foo'>https://t.me/addlist/foo</a>)\nТекст"
    assert sanitize_for_vk(src) == 'Текст'


def test_sanitize_for_vk_renders_markdown_headings_as_plain_blocks():
    src = "### О событии\nТекст\n\n#### Что обсудим\n- первое"
    expected = "О СОБЫТИИ\n\nТекст\n\nЧТО ОБСУДИМ\n\n- первое"
    assert sanitize_for_vk(src) == expected


def test_sanitize_for_vk_renders_html_headings_as_plain_blocks():
    src = "<h3>О событии</h3><p>Текст</p><h4><b>Что обсудим</b></h4>Детали"
    expected = "О СОБЫТИИ\n\nТекст\n\nЧТО ОБСУДИМ\n\nДетали"
    assert sanitize_for_vk(src) == expected
