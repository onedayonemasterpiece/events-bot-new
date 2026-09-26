"""Lossless, backwards-compatible storage for large VK source JSON strings."""

from __future__ import annotations

import base64
import binascii
import zlib

_PREFIX = "zlib64:"


def encode_packet_json(raw_json: str) -> str:
    """Keep small JSON plain; compress large redundant VK envelopes."""
    if raw_json.startswith(_PREFIX):
        raise ValueError("packet JSON is already encoded")
    raw = raw_json.encode("utf-8")
    if len(raw) < 1024:
        return raw_json
    compressed = _PREFIX + base64.b64encode(zlib.compress(raw, level=6)).decode("ascii")
    return compressed if len(compressed) < len(raw_json) else raw_json


def decode_packet_json(stored: str) -> str:
    """Read both pre-migration plain rows and losslessly compressed rows."""
    if not stored.startswith(_PREFIX):
        return stored
    try:
        packed = base64.b64decode(stored[len(_PREFIX):], validate=True)
        return zlib.decompress(packed).decode("utf-8")
    except (binascii.Error, zlib.error, UnicodeDecodeError) as exc:
        raise ValueError("invalid compressed VK packet JSON") from exc
