import ast
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest


@pytest.mark.parametrize(
    "actual_owner,requested_owner,owner_type,expected",
    [
        (-211429559, 237389053, "group", "https://vk.com/wall-211429559_3892"),
        (41501067, 237389053, "group", "https://vk.com/wall41501067_3892"),
        (None, 237389053, "group", "https://vk.com/wall-237389053_3892"),
        (None, 41501067, "user", "https://vk.com/wall41501067_3892"),
    ],
)
def test_vk_post_link_uses_returned_owner(actual_owner, requested_owner, owner_type, expected):
    path = Path("kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py")
    tree = ast.parse(path.read_text())
    fn = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
              and node.name == "_vk_post_to_scanned_post")
    namespace = dict(Any=Any, datetime=datetime, timezone=timezone,
                     ScannedPost=SimpleNamespace,
                     _vk_post_text=lambda post: post["text"],
                     _vk_photo_media_refs=lambda post: [])
    exec(compile(ast.Module(body=[fn], type_ignores=[]), str(path), "exec"), namespace)
    post = {"id": 3892, "date": 1794510389, "text": "Экскурсии проекта 80 лет единства"}
    if actual_owner is not None:
        post["owner_id"] = actual_owner
    scanned = namespace[fn.name](post, group_id=requested_owner,
                                  username="kld80", owner_type=owner_type)
    assert scanned.source_url == expected
    assert scanned.message_id == 3892
    assert scanned.text == post["text"]
