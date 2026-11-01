import ast
import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telethon.utils import parse_username

@pytest.mark.parametrize('path', [
    'guide_excursions/scanner.py',
    'kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py',
])
@pytest.mark.parametrize('username,expected', [
    ('+PrivateInviteToken', 'https://t.me/c/3167811186/290'),
    ('public_guide', 'https://t.me/public_guide/290'),
])
def test_scanners_preserve_public_links_and_use_private_post_links(path, username, expected):
    # Run the real scanner without importing the notebook's Kaggle setup.
    tree = ast.parse(Path(path).read_text())
    function = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == 'scan_source_posts')
    function.returns = None
    for arg in [*function.args.args, *function.args.kwonlyargs]:
        arg.annotation = None
    now = datetime.now(timezone.utc)
    entity = SimpleNamespace(id=3167811186, title='Guide')
    message = SimpleNamespace(id=290, date=now, action=None, grouped_id=None)
    client = AsyncMock()
    client.get_entity.return_value = entity
    client.get_messages.return_value = [message]
    client.return_value = SimpleNamespace(full_chat=SimpleNamespace(about=''))
    def collapse(messages, *, username, **kwargs):
        return SimpleNamespace(message_id=290, post_date=now, source_url=f'https://t.me/{username}/290')
    namespace = dict(datetime=datetime, timedelta=timedelta, timezone=timezone,
                     _utc_now=lambda: now, _collapse_group=collapse,
                     GuideScannedSourceMeta=SimpleNamespace,
                     functions=SimpleNamespace(channels=SimpleNamespace(GetFullChannelRequest=lambda **kw: None)))
    exec(compile(ast.Module(body=[function], type_ignores=[]), path, 'exec'), namespace)
    _, posts = asyncio.run(namespace['scan_source_posts'](client, username=username, limit=20, days_back=5))
    assert posts[0].source_url == expected
    client.get_entity.assert_awaited_once_with(f"https://t.me/{username}" if username.startswith("+") else username)
    resolved, is_invite = parse_username(client.get_entity.await_args.args[0])
    assert resolved is not None
    assert is_invite == username.startswith('+')
