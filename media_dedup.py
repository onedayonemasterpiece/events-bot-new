from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from io import BytesIO


@dataclass(frozen=True, slots=True)
class PreparedImage:
    dhash_hex: str
    webp_bytes: bytes


@dataclass(frozen=True, slots=True)
class ImageFingerprints:
    """Content fingerprints used by the event-media identity gate.

    ``dhash_hex`` intentionally matches the historical ``EventPoster.phash``
    value and the managed-storage ``p/dh16`` object key.  ``phash_hex`` is a
    real DCT perceptual hash and therefore has a separate name.
    """

    raw_sha256: str
    pixel_sha256: str
    dhash_hex: str
    phash_hex: str
    width: int
    height: int
    mime_type: str | None = None


def _normalized_rgb_image(image_bytes: bytes):
    if not image_bytes:
        return None
    try:
        from PIL import Image, ImageOps  # type: ignore
    except Exception:
        return None
    try:
        with Image.open(BytesIO(image_bytes)) as source:
            fmt = str(getattr(source, "format", "") or "").upper()
            image = ImageOps.exif_transpose(source).convert("RGB")
            image.load()
        mime = {
            "JPEG": "image/jpeg",
            "JPG": "image/jpeg",
            "PNG": "image/png",
            "WEBP": "image/webp",
            "GIF": "image/gif",
            "BMP": "image/bmp",
        }.get(fmt)
        return image, mime
    except Exception:
        return None


def _pixel_sha256_from_image(image) -> str:
    payload = (
        int(image.width).to_bytes(4, "big")
        + int(image.height).to_bytes(4, "big")
        + image.tobytes()
    )
    return hashlib.sha256(payload).hexdigest()


def _phash_hex_from_image(image, *, hash_size: int = 16, highfreq_factor: int = 2) -> str:
    """Return a small dependency-free DCT pHash.

    Pillow is already a runtime dependency; keeping the separable DCT here
    avoids adding NumPy/ImageHash only for a few event-local comparisons.
    """

    from PIL import Image  # type: ignore

    side = max(hash_size + 1, hash_size * highfreq_factor)
    resampling = getattr(Image, "Resampling", None)
    lanczos = resampling.LANCZOS if resampling else Image.LANCZOS
    pixels = list(image.convert("L").resize((side, side), lanczos).getdata())
    rows = [pixels[offset : offset + side] for offset in range(0, len(pixels), side)]
    cos_table = [
        [math.cos(math.pi * (2 * x + 1) * k / (2 * side)) for x in range(side)]
        for k in range(hash_size)
    ]
    row_dct = [
        [sum(float(row[x]) * cos_table[k][x] for x in range(side)) for k in range(hash_size)]
        for row in rows
    ]
    coeffs: list[float] = []
    for v in range(hash_size):
        cos_v = cos_table[v]
        for u in range(hash_size):
            coeffs.append(sum(row_dct[y][u] * cos_v[y] for y in range(side)))
    comparison = coeffs[1:] or coeffs
    ordered = sorted(comparison)
    median = ordered[len(ordered) // 2]
    value = 0
    for coefficient in coeffs:
        value = (value << 1) | int(coefficient > median)
    return f"{value:0{(hash_size * hash_size) // 4}x}"


def compute_image_fingerprints(image_bytes: bytes) -> ImageFingerprints | None:
    """Compute raw, normalized-pixel, dHash16 and DCT-pHash16 once."""

    normalized = _normalized_rgb_image(image_bytes)
    if normalized is None:
        return None
    image, mime_type = normalized
    try:
        return ImageFingerprints(
            raw_sha256=hashlib.sha256(image_bytes).hexdigest(),
            pixel_sha256=_pixel_sha256_from_image(image),
            dhash_hex=_dhash_hex_from_image(image, dhash_size=16),
            phash_hex=_phash_hex_from_image(image, hash_size=16),
            width=int(image.width),
            height=int(image.height),
            mime_type=mime_type,
        )
    except Exception:
        return None


def compute_global_ssim(left_bytes: bytes, right_bytes: bytes, *, side: int = 256) -> float | None:
    """Return a bounded grayscale SSIM signal for the LLM evidence bundle.

    This is deliberately only a recall/evidence signal, never a final verdict.
    """

    left = _normalized_rgb_image(left_bytes)
    right = _normalized_rgb_image(right_bytes)
    if left is None or right is None:
        return None
    try:
        from PIL import Image  # type: ignore

        resampling = getattr(Image, "Resampling", None)
        lanczos = resampling.LANCZOS if resampling else Image.LANCZOS
        a = list(left[0].convert("L").resize((side, side), lanczos).getdata())
        b = list(right[0].convert("L").resize((side, side), lanczos).getdata())
        n = len(a)
        if not n or n != len(b):
            return None
        mean_a = sum(a) / n
        mean_b = sum(b) / n
        denom = max(1, n - 1)
        var_a = sum((x - mean_a) ** 2 for x in a) / denom
        var_b = sum((x - mean_b) ** 2 for x in b) / denom
        covariance = sum((x - mean_a) * (y - mean_b) for x, y in zip(a, b)) / denom
        c1 = (0.01 * 255) ** 2
        c2 = (0.03 * 255) ** 2
        score = ((2 * mean_a * mean_b + c1) * (2 * covariance + c2)) / (
            (mean_a**2 + mean_b**2 + c1) * (var_a + var_b + c2)
        )
        return max(-1.0, min(1.0, float(score)))
    except Exception:
        return None


def _dhash_hex_from_image(im, *, dhash_size: int = 16) -> str:
    """Compute the perceptual dHash (hex) for an already-decoded PIL image.

    Single source of truth for both the Supabase/Yandex storage object key and the
    near-duplicate poster dedup. Keep this in sync with the `dh<size>` storage path
    scheme and the offline audit (`scripts/inspect/audit_media_dedup.py`).
    """
    from PIL import Image  # type: ignore

    resampling = getattr(Image, "Resampling", None)
    lanczos = resampling.LANCZOS if resampling else Image.LANCZOS

    # dHash (horizontal gradients) on grayscale, fixed size.
    gray = im.convert("L")
    small = gray.resize((dhash_size + 1, dhash_size), lanczos)
    get_flat = getattr(small, "get_flattened_data", None)
    pixels = list(get_flat() if callable(get_flat) else small.getdata())
    # Quantize to reduce sensitivity to minor resampling differences between resolutions.
    pixels = [p >> 3 for p in pixels]
    diff_bits: list[int] = []
    row_w = dhash_size + 1
    for row in range(dhash_size):
        off = row * row_w
        for col in range(dhash_size):
            left = pixels[off + col]
            right = pixels[off + col + 1]
            diff_bits.append(1 if left > right else 0)
    value = 0
    for bit in diff_bits:
        value = (value << 1) | bit
    width = (dhash_size * dhash_size) // 4
    return f"{value:0{width}x}"


def compute_dhash_hex(image_bytes: bytes, *, dhash_size: int = 16) -> str | None:
    """Perceptual dHash (hex) for raw image bytes, or None on failure.

    Value is identical to ``prepare_image_for_supabase(...).dhash_hex`` so a
    poster's near-dup phash matches its storage object key.
    """
    if not image_bytes:
        return None
    try:
        from PIL import Image, ImageOps  # type: ignore
    except Exception:
        return None
    try:
        with Image.open(BytesIO(image_bytes)) as im:
            im = ImageOps.exif_transpose(im)
            return _dhash_hex_from_image(im, dhash_size=dhash_size)
    except Exception:
        return None


def hamming_distance_hex(a: str | None, b: str | None) -> int:
    """Hamming distance between two equal-length hex hashes.

    Returns a large sentinel when either hash is missing or lengths differ, so
    callers treat the pair as "not a near-duplicate".
    """
    ha = (a or "").strip().lower()
    hb = (b or "").strip().lower()
    if not ha or not hb or len(ha) != len(hb):
        return 1 << 30
    try:
        return bin(int(ha, 16) ^ int(hb, 16)).count("1")
    except Exception:
        return 1 << 30


def prepare_image_for_supabase(
    image_bytes: bytes,
    *,
    dhash_size: int = 16,
    webp_quality: int = 82,
) -> PreparedImage | None:
    """Decode bytes once, compute perceptual dHash, and re-encode as WebP.

    This is used for cross-environment deduplication in Supabase Storage:
    - key by perceptual hash (stable across re-encodes / different resolutions)
    - store as WebP only
    """

    if not image_bytes:
        return None

    try:
        from PIL import Image, ImageOps  # type: ignore
    except Exception:
        return None

    try:
        with Image.open(BytesIO(image_bytes)) as im:
            im = ImageOps.exif_transpose(im)

            # dHash (horizontal gradients) on grayscale, fixed size.
            dhash_hex = _dhash_hex_from_image(im, dhash_size=dhash_size)

            # WebP encoding.
            out = BytesIO()
            if im.mode in {"RGBA", "LA"} or (
                im.mode == "P" and "transparency" in (im.info or {})
            ):
                im2 = im.convert("RGBA")
            else:
                im2 = im.convert("RGB")
            im2.save(
                out,
                format="WEBP",
                quality=int(webp_quality),
                method=6,
            )
            webp_bytes = out.getvalue()
            if not webp_bytes:
                return None
            return PreparedImage(dhash_hex=dhash_hex, webp_bytes=webp_bytes)
    except Exception:
        return None


def build_supabase_poster_object_path(
    dhash_hex: str,
    *,
    prefix: str = "p",
    dhash_size: int = 16,
) -> str:
    """Build a deterministic Storage path for a poster.

    Format:
      <prefix>/dh<dhash_size>/<first2>/<dhash>.webp
    """

    pfx = (prefix or "").strip().strip("/")
    if not pfx:
        pfx = "p"
    h = (dhash_hex or "").strip().lower()
    if not h:
        raise ValueError("dhash_hex is required")
    algo = f"dh{int(dhash_size)}"
    return f"{pfx}/{algo}/{h[:2]}/{h}.webp"
