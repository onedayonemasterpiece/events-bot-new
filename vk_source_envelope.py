"""Lossless-enough, secret-safe VK wall source envelopes.

The transport adapters in :mod:`main`, :mod:`main_part2`, and
:mod:`vk_auto_queue` all receive the same VK wall item shape.  This module is
the single pure normalization boundary for that shape.  It deliberately keeps
the sanitized provider item and a complete recursive attachment inventory;
media limits only select OCR candidates and never discard the inventory.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


VK_SOURCE_ENVELOPE_SCHEMA = "vk_source_envelope"
VK_SOURCE_ENVELOPE_VERSION = 1

# Request/auth/error material is not source evidence.  ``access_key`` is
# intentionally not in this set: VK returns it as attachment capability data
# and it can be required to replay the captured media.  It stays inside the
# protected raw packet and must never be copied into logs or LLM receipts.
_DENIED_KEYS = {
    "access_token",
    "authorization",
    "proxy_authorization",
    "api_key",
    "apikey",
    "client_secret",
    "token",
    "captcha_sid",
    "captcha_img",
    "captcha_key",
    "error",
    "error_data",
    "error_params",
    "request_params",
}

_VOLATILE_ITEM_KEYS = {
    "views",
    "likes",
    "comments",
    "reposts",
}


def _sanitize_url_secrets(value: str) -> str:
    """Remove credentials and secret-like query parameters from source URLs."""

    text = str(value or "")
    if not text.lower().startswith(("http://", "https://")):
        return text
    try:
        parsed = urlsplit(text)
    except ValueError:
        return ""
    hostname = parsed.hostname or ""
    if parsed.port:
        hostname = f"{hostname}:{parsed.port}"
    safe_query = []
    for key, item in parse_qsl(parsed.query, keep_blank_values=True):
        normalized = key.casefold().replace("-", "_")
        if normalized in _DENIED_KEYS or normalized in {"signature", "sig"}:
            continue
        safe_query.append((key, item))
    return urlunsplit(
        (parsed.scheme, hostname, parsed.path, urlencode(safe_query), "")
    )


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def sanitize_vk_source_value(value: Any) -> Any:
    """Return a JSON-safe recursive copy without transport secrets/errors."""

    if isinstance(value, Mapping):
        clean: dict[str, Any] = {}
        for raw_key, raw_value in value.items():
            key = str(raw_key)
            if key.casefold() in _DENIED_KEYS:
                continue
            clean[key] = sanitize_vk_source_value(raw_value)
        return clean
    if isinstance(value, (list, tuple)):
        return [sanitize_vk_source_value(item) for item in value]
    if isinstance(value, str):
        return _sanitize_url_secrets(value)
    if value is None or isinstance(value, (int, float, bool)):
        return value
    return str(value)


def _best_image_url(sizes: Any) -> str:
    if not isinstance(sizes, Sequence) or isinstance(
        sizes, (str, bytes, bytearray)
    ):
        return ""
    candidates = [item for item in sizes if isinstance(item, Mapping)]
    if not candidates:
        return ""

    def dimension(value: Any) -> int:
        try:
            return max(0, int(value or 0))
        except (TypeError, ValueError):
            return 0

    best = max(
        candidates,
        key=lambda item: (
            dimension(item.get("width")) * dimension(item.get("height")),
            dimension(item.get("width")),
        ),
    )
    return str(best.get("url") or best.get("src") or "").strip()


def _attachment_payload(
    attachment: Mapping[str, Any],
) -> tuple[str, Mapping[str, Any]]:
    attachment_type = str(attachment.get("type") or "unknown").strip() or "unknown"
    payload = attachment.get(attachment_type)
    if not isinstance(payload, Mapping):
        payload = {}
    return attachment_type, payload


def _semantic_attachment(
    attachment: Mapping[str, Any],
    *,
    preview_url: str,
) -> dict[str, Any]:
    """Stable semantic projection; excludes counters and capability keys."""

    attachment_type, payload = _attachment_payload(attachment)
    common = {
        key: payload.get(key)
        for key in ("owner_id", "id", "album_id", "post_id")
        if payload.get(key) is not None
    }
    semantic: dict[str, Any] = {"type": attachment_type, "ids": common}
    if attachment_type == "link":
        semantic["content"] = {
            key: payload.get(key)
            for key in ("url", "title", "description", "caption")
            if payload.get(key) not in (None, "")
        }
    elif attachment_type == "doc":
        semantic["content"] = {
            key: payload.get(key)
            for key in ("title", "ext", "size", "url", "type")
            if payload.get(key) not in (None, "")
        }
    elif attachment_type == "video":
        semantic["content"] = {
            key: payload.get(key)
            for key in ("title", "description", "duration", "player")
            if payload.get(key) not in (None, "")
        }
    elif attachment_type == "photo":
        semantic["content"] = {
            key: payload.get(key)
            for key in ("text", "date")
            if payload.get(key) not in (None, "")
        }
    else:
        # Unknown/nonvisual types can still contain event-bearing semantics
        # (for example poll questions/options). Keep their sanitized payload in
        # the revision projection, excluding volatile/capability fields.
        semantic["content"] = {
            key: value
            for key, value in sanitize_vk_source_value(payload).items()
            if key not in {"access_key", "likes", "views", "comments", "reposts"}
        }
    if preview_url:
        semantic["preview_url"] = preview_url
    return semantic


def _attachment_text(attachment: Mapping[str, Any]) -> str:
    """Render provider-supplied attachment semantics for the source LLM."""

    attachment_type, payload = _attachment_payload(attachment)
    fields: list[Any] = []
    if attachment_type == "link":
        fields = [
            payload.get("title"),
            payload.get("description"),
            payload.get("caption"),
            payload.get("url"),
        ]
    elif attachment_type == "doc":
        fields = [payload.get("title"), payload.get("url")]
    elif attachment_type == "video":
        fields = [payload.get("title"), payload.get("description"), payload.get("player")]
    elif attachment_type == "photo":
        fields = [payload.get("text")]
    elif attachment_type == "poll":
        fields = [payload.get("question")]
        answers = payload.get("answers")
        if isinstance(answers, Sequence) and not isinstance(
            answers, (str, bytes, bytearray)
        ):
            fields.extend(
                answer.get("text")
                for answer in answers
                if isinstance(answer, Mapping)
            )
    values: list[str] = []
    for value in fields:
        text = str(value or "").strip()
        if text and text not in values:
            values.append(text)
    return "\n".join(values)


def _attachment_preview(
    attachment: Mapping[str, Any],
) -> tuple[str, bool, str]:
    """Return ``(url, visual_expected, candidate_kind)``."""

    attachment_type, payload = _attachment_payload(attachment)
    if attachment_type == "photo":
        return _best_image_url(payload.get("sizes") or ()), True, "photo"
    if attachment_type == "link":
        photo = payload.get("photo")
        expected = isinstance(photo, Mapping)
        sizes = photo.get("sizes") if isinstance(photo, Mapping) else ()
        return _best_image_url(sizes or ()), expected, "link_preview"
    if attachment_type == "video":
        images = payload.get("first_frame") or payload.get("image") or ()
        expected = bool(images)
        return _best_image_url(images), expected, "video_preview"
    if attachment_type == "doc":
        preview = payload.get("preview")
        photo = preview.get("photo") if isinstance(preview, Mapping) else None
        expected = isinstance(photo, Mapping)
        sizes = photo.get("sizes") if isinstance(photo, Mapping) else ()
        return _best_image_url(sizes or ()), expected, "doc_preview"
    return "", False, ""


def _wall_url(owner_id: int, post_id: int, owner_type: str) -> str:
    signed = (
        int(owner_id)
        if str(owner_type).lower() == "user"
        else -abs(int(owner_id))
    )
    return f"https://vk.com/wall{signed}_{int(post_id)}"


def _node_metadata(node: Mapping[str, Any], path: str) -> dict[str, Any]:
    return {
        key: value
        for key, value in {
            "path": path,
            "owner_id": node.get("owner_id"),
            "from_id": node.get("from_id"),
            "id": node.get("id"),
            "date": node.get("date"),
            "edited": node.get("edited"),
            "post_type": node.get("post_type"),
        }.items()
        if value is not None
    }


def vk_source_semantic_projection(envelope: Mapping[str, Any]) -> dict[str, Any]:
    """Return the edit-sensitive, counter-insensitive revision projection."""

    return {
        "schema_version": int(envelope.get("schema_version") or 0),
        "text_segments": [
            {
                "path": item.get("path"),
                "role": item.get("role"),
                "text": item.get("text"),
            }
            for item in (envelope.get("text_segments") or ())
            if isinstance(item, Mapping)
        ],
        "revision_metadata": [
            {
                key: item.get(key)
                for key in (
                    "path",
                    "owner_id",
                    "from_id",
                    "id",
                    "date",
                    "edited",
                    "post_type",
                )
                if item.get(key) is not None
            }
            for item in (envelope.get("revision_metadata") or ())
            if isinstance(item, Mapping)
        ],
        "attachments": [
            {
                "path": item.get("path"),
                "semantic": item.get("semantic"),
            }
            for item in (envelope.get("attachment_inventory") or ())
            if isinstance(item, Mapping)
        ],
    }


def vk_source_semantic_revision_hash(envelope: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        _canonical_json(vk_source_semantic_projection(envelope)).encode("utf-8")
    ).hexdigest()


def is_vk_source_envelope(value: Any) -> bool:
    if (
        not isinstance(value, Mapping)
        or value.get("schema") != VK_SOURCE_ENVELOPE_SCHEMA
    ):
        return False
    try:
        return int(value.get("schema_version") or 0) == VK_SOURCE_ENVELOPE_VERSION
    except (TypeError, ValueError):
        return False


def vk_source_envelope_replayability(value: Any) -> str:
    if not is_vk_source_envelope(value):
        if isinstance(value, Mapping) and (
            str(value.get("text") or "").strip() or value.get("photos")
        ):
            return "replayable_legacy_incomplete"
        return "unavailable"
    completeness = value.get("completeness")
    try:
        post_id = int(value.get("post_id") or 0)
    except (TypeError, ValueError):
        post_id = 0
    structurally_complete = (
        post_id != 0
        and isinstance(value.get("raw_item"), Mapping)
        and isinstance(value.get("text_segments"), list)
        and isinstance(value.get("attachment_inventory"), list)
        and isinstance(value.get("all_media_candidates"), list)
    )
    if (
        structurally_complete
        and isinstance(completeness, Mapping)
        and bool(completeness.get("capture_complete"))
    ):
        return "replayable_lossless"
    return "replayable_legacy_incomplete"


def build_vk_source_envelope(
    raw_item: Mapping[str, Any],
    *,
    owner_id: int,
    owner_type: str = "group",
    post_id: int | None = None,
    source_url: str | None = None,
    media_limit: int | None = 12,
) -> dict[str, Any]:
    """Build the canonical VK envelope from one actual provider wall item."""

    sanitized = sanitize_vk_source_value(raw_item)
    if not isinstance(sanitized, dict):  # defensive; Mapping always yields dict
        sanitized = {}
    normalized_post_id = int(
        post_id
        if post_id is not None
        else sanitized.get("id") or sanitized.get("post_id") or 0
    )
    published_at = int(sanitized.get("date") or 0)
    normalized_owner_type = "user" if str(owner_type).lower() == "user" else "group"
    canonical_url = _sanitize_url_secrets(
        str(source_url or _wall_url(owner_id, normalized_post_id, normalized_owner_type))
    )

    text_segments: list[dict[str, Any]] = []
    revision_metadata: list[dict[str, Any]] = []
    attachment_inventory: list[dict[str, Any]] = []
    all_candidates: list[dict[str, Any]] = []
    unavailable_visual: list[dict[str, Any]] = []
    seen_candidate_urls: set[str] = set()

    def walk(node: Mapping[str, Any], path: str, *, role: str) -> None:
        revision_metadata.append(_node_metadata(node, path))
        text = node.get("text")
        if isinstance(text, str) and text.strip():
            text_segments.append({"path": path, "role": role, "text": text.strip()})

        attachments = node.get("attachments")
        if isinstance(attachments, Sequence) and not isinstance(
            attachments, (str, bytes, bytearray)
        ):
            for index, raw_attachment in enumerate(attachments):
                if not isinstance(raw_attachment, Mapping):
                    continue
                attachment = sanitize_vk_source_value(raw_attachment)
                if not isinstance(attachment, dict):
                    continue
                attachment_path = f"{path}.attachments[{index}]"
                preview_url, visual_expected, candidate_kind = _attachment_preview(
                    attachment
                )
                attachment_type, payload = _attachment_payload(attachment)
                ids = {
                    key: payload.get(key)
                    for key in (
                        "owner_id",
                        "id",
                        "album_id",
                        "post_id",
                        "access_key",
                    )
                    if payload.get(key) is not None
                }
                inventory_item = {
                    "path": attachment_path,
                    "type": attachment_type,
                    "ids": ids,
                    "semantic": _semantic_attachment(
                        attachment, preview_url=preview_url
                    ),
                    "raw": attachment,
                    "visual_expected": visual_expected,
                    "preview_url": preview_url or None,
                }
                attachment_inventory.append(inventory_item)
                attachment_text = _attachment_text(attachment)
                if attachment_text:
                    text_segments.append(
                        {
                            "path": attachment_path,
                            "role": "attachment",
                            "text": attachment_text,
                        }
                    )
                if preview_url:
                    candidate = {
                        "path": attachment_path,
                        "attachment_type": attachment_type,
                        "kind": candidate_kind,
                        "url": preview_url,
                    }
                    if preview_url not in seen_candidate_urls:
                        seen_candidate_urls.add(preview_url)
                        all_candidates.append(candidate)
                elif visual_expected:
                    unavailable_visual.append(
                        {
                            "path": attachment_path,
                            "attachment_type": attachment_type,
                            "reason": "preview_url_unavailable",
                        }
                    )

        copy_history = node.get("copy_history")
        if isinstance(copy_history, Sequence) and not isinstance(
            copy_history, (str, bytes, bytearray)
        ):
            for index, copied in enumerate(copy_history):
                if isinstance(copied, Mapping):
                    walk(copied, f"{path}.copy_history[{index}]", role="copy")

    walk(sanitized, "$", role="outer")

    normalized_limit = (
        len(all_candidates) if media_limit is None else max(0, int(media_limit))
    )
    selected_candidates = all_candidates[:normalized_limit]
    omitted_candidates = all_candidates[normalized_limit:]
    combined_parts: list[str] = []
    for index, segment in enumerate(text_segments):
        if index == 0 and segment["role"] == "outer":
            combined_parts.append(str(segment["text"]))
        else:
            label = "Вложение" if segment["role"] == "attachment" else "Репост"
            combined_parts.append(
                f"[{label} {segment['path']}]\n{segment['text']}"
            )
    combined_text = "\n\n".join(combined_parts).strip()

    metrics: dict[str, int] = {}
    for key in _VOLATILE_ITEM_KEYS:
        value = sanitized.get(key)
        count = value.get("count") if isinstance(value, Mapping) else None
        if isinstance(count, int) and count >= 0:
            metrics[key] = count

    envelope: dict[str, Any] = {
        "schema": VK_SOURCE_ENVELOPE_SCHEMA,
        "schema_version": VK_SOURCE_ENVELOPE_VERSION,
        "source_type": "vk",
        "owner_id": int(owner_id),
        "owner_type": normalized_owner_type,
        "post_id": normalized_post_id,
        "date": published_at,
        "published_at": published_at,
        "edited_at": sanitized.get("edited"),
        "source_url": canonical_url,
        "url": canonical_url,
        "raw_item": sanitized,
        "text_segments": text_segments,
        "revision_metadata": revision_metadata,
        "text": combined_text,
        # Compatibility projections used by existing discovery code.
        "attachments": sanitized.get("attachments") or [],
        "copy_history": sanitized.get("copy_history") or [],
        "photos": [str(item["url"]) for item in selected_candidates],
        "attachment_inventory": attachment_inventory,
        "all_media_candidates": all_candidates,
        "media_candidates": selected_candidates,
        "omitted_media_candidates": omitted_candidates,
        "unavailable_visual_attachments": unavailable_visual,
        "metrics": metrics,
        "views": metrics.get("views"),
        "likes": metrics.get("likes"),
        "counts": {
            "attachment_inventory_count": len(attachment_inventory),
            "visual_candidate_count": len(all_candidates),
            "selected_media_count": len(selected_candidates),
            "omitted_media_count": len(omitted_candidates),
            "unavailable_visual_count": len(unavailable_visual),
            "text_segment_count": len(text_segments),
        },
        "completeness": {
            "capture_complete": True,
            "raw_item_complete": True,
            "attachment_inventory_complete": True,
            "media_selection_truncated": bool(omitted_candidates),
            "unavailable_visual_count": len(unavailable_visual),
        },
    }
    return envelope


def vk_source_packet_hashes(envelope: Mapping[str, Any]) -> tuple[str, str]:
    """Return canonical full-payload and semantic-revision hashes."""

    payload_hash = hashlib.sha256(
        _canonical_json(envelope).encode("utf-8")
    ).hexdigest()
    return payload_hash, vk_source_semantic_revision_hash(envelope)


def vk_source_envelope_attachment_metadata(
    envelope: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the durable, replay-oriented attachment projection."""

    return {
        "schema": VK_SOURCE_ENVELOPE_SCHEMA,
        "schema_version": VK_SOURCE_ENVELOPE_VERSION,
        "photos": list(envelope.get("photos") or ()),
        "attachment_inventory": list(envelope.get("attachment_inventory") or ()),
        "all_media_candidates": list(envelope.get("all_media_candidates") or ()),
        "media_candidates": list(envelope.get("media_candidates") or ()),
        "omitted_media_candidates": list(envelope.get("omitted_media_candidates") or ()),
        "unavailable_visual_attachments": list(
            envelope.get("unavailable_visual_attachments") or ()
        ),
        "counts": dict(envelope.get("counts") or {}),
        "completeness": dict(envelope.get("completeness") or {}),
    }
