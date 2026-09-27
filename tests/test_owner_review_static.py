from pathlib import Path

import pytest
from aiohttp import web

from owner_review_static import (
    OwnerReviewStaticError,
    register_owner_review_static,
    resolve_owner_review_file,
)


def _tree(tmp_path: Path) -> Path:
    root = tmp_path / "reviews"
    build = root / "preview-review-r9"
    (build / "poisk").mkdir(parents=True)
    (build / "__preview").mkdir()
    (build / "lab").mkdir()
    (build / "_review").mkdir()
    (build / "index.html").write_text("home", encoding="utf-8")
    (build / "poisk" / "index.html").write_text("search", encoding="utf-8")
    (build / "__preview" / "index.html").write_text("qa", encoding="utf-8")
    (build / "lab" / "index.html").write_text("lab", encoding="utf-8")
    (build / "_review" / "index.html").write_text("review", encoding="utf-8")
    return root


def test_resolve_owner_review_file_serves_normal_routes(tmp_path: Path) -> None:
    root = _tree(tmp_path)
    assert resolve_owner_review_file(root, "preview-review-r9", "").read_text() == "home"
    assert resolve_owner_review_file(root, "preview-review-r9", "poisk/").read_text() == "search"


@pytest.mark.parametrize("tail", ["__preview/", "lab/", "_review/", "../x", ".env"])
def test_resolve_owner_review_file_rejects_service_and_unsafe_paths(tmp_path: Path, tail: str) -> None:
    root = _tree(tmp_path)
    with pytest.raises(web.HTTPNotFound):
        resolve_owner_review_file(root, "preview-review-r9", tail)


def test_register_owner_review_static_is_disabled_by_default() -> None:
    app = web.Application()
    assert register_owner_review_static(app, {}) is False
    assert not list(app.router.routes())


def test_register_owner_review_static_requires_absolute_root() -> None:
    app = web.Application()
    with pytest.raises(OwnerReviewStaticError):
        register_owner_review_static(
            app,
            {"ENABLE_OWNER_REVIEW_STATIC": "1", "OWNER_REVIEW_STATIC_ROOT": "relative"},
        )


def test_register_owner_review_static_adds_only_preview_review_routes(tmp_path: Path) -> None:
    app = web.Application()
    assert register_owner_review_static(
        app,
        {
            "ENABLE_OWNER_REVIEW_STATIC": "1",
            "OWNER_REVIEW_STATIC_ROOT": str(tmp_path),
        },
    ) is True
    resources = [route.resource.canonical for route in app.router.routes()]
    assert "/{build_id}/" in resources
    assert "/{build_id}/{tail}" in resources
