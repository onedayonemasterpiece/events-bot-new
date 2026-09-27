"""Bounded static owner-review surface served from the Fly data volume.

This is a review-only fallback when Object Storage preview publication is
unavailable. It never serves production root and never exposes QA/service
routes from a preview artifact.
"""
from __future__ import annotations

import os
import re
from pathlib import Path, PurePosixPath
from typing import Mapping

from aiohttp import web

BUILD_RE = re.compile(r"preview-review-[A-Za-z0-9][A-Za-z0-9._-]*\Z")
SERVICE_ROOTS = frozenset({"__preview", "lab", "_review"})
DEFAULT_ROOT = "/data/owner_review_previews"


class OwnerReviewStaticError(RuntimeError):
    pass


def _enabled(env: Mapping[str, str]) -> bool:
    return str(env.get("ENABLE_OWNER_REVIEW_STATIC") or "").strip() == "1"


def _configured_root(env: Mapping[str, str]) -> Path:
    raw = str(env.get("OWNER_REVIEW_STATIC_ROOT") or DEFAULT_ROOT).strip()
    root = Path(raw)
    if not root.is_absolute():
        raise OwnerReviewStaticError("owner_review_root_must_be_absolute")
    return root


def resolve_owner_review_file(
    root: Path,
    build_id: str,
    tail: str,
) -> Path:
    if not BUILD_RE.fullmatch(str(build_id or "")):
        raise web.HTTPNotFound()

    raw_tail = str(tail or "").lstrip("/")
    parts = PurePosixPath(raw_tail).parts if raw_tail else ()
    if any(part in {"", ".", ".."} for part in parts):
        raise web.HTTPNotFound()
    if parts and parts[0] in SERVICE_ROOTS:
        raise web.HTTPNotFound()
    if any(part.startswith(".") for part in parts):
        raise web.HTTPNotFound()

    resolved_root = root.resolve()
    build_root = (resolved_root / build_id).resolve()
    if build_root.parent != resolved_root:
        raise web.HTTPNotFound()

    relative = Path(*parts) if parts else Path()
    candidate = (build_root / relative).resolve()
    if candidate != build_root and build_root not in candidate.parents:
        raise web.HTTPNotFound()
    if candidate.is_dir() or not parts:
        candidate = (candidate / "index.html").resolve()
    if candidate != build_root and build_root not in candidate.parents:
        raise web.HTTPNotFound()
    if not candidate.is_file():
        raise web.HTTPNotFound()
    return candidate


def _review_headers() -> dict[str, str]:
    return {
        "Cache-Control": "no-cache, max-age=0, must-revalidate",
        "Pragma": "no-cache",
        "X-Robots-Tag": "noindex, nofollow, noarchive",
        "X-Content-Type-Options": "nosniff",
        "Referrer-Policy": "no-referrer",
    }


def register_owner_review_static(
    app: web.Application,
    env: Mapping[str, str] = os.environ,
) -> bool:
    if not _enabled(env):
        return False
    root = _configured_root(env)

    async def handler(request: web.Request) -> web.StreamResponse:
        build_id = str(request.match_info.get("build_id") or "")
        tail = str(request.match_info.get("tail") or "")
        path = resolve_owner_review_file(root, build_id, tail)
        response = web.FileResponse(path)
        response.headers.update(_review_headers())
        return response

    pattern = r"{build_id:preview-review-[A-Za-z0-9][A-Za-z0-9._-]*}"
    app.router.add_get(f"/{pattern}/", handler, name="owner_review_root")
    app.router.add_get(f"/{pattern}/{{tail:.*}}", handler, name="owner_review_asset")
    return True
