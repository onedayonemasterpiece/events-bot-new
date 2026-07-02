from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO


@dataclass(frozen=True, slots=True)
class PreparedImage:
    dhash_hex: str
    webp_bytes: bytes


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
