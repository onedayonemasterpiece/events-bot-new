import re
from typing import Any, Iterable


KGD80_FESTIVAL_NAME = "80 историй о главном"


def normalize_campaign_anchor_text(value: Any) -> str:
    raw = str(value or "").casefold().replace("ё", "е")
    return " ".join(re.sub(r"[^0-9a-zа-я]+", " ", raw).split())


def contains_explicit_kgd80_anchor(values: Iterable[Any]) -> bool:
    """Recognise literal campaign evidence without inferring it from an anniversary."""

    text_parts: list[str] = []

    def collect(value: Any) -> None:
        if value is None or isinstance(value, bool):
            return
        if isinstance(value, dict):
            for nested in value.values():
                collect(nested)
            return
        if isinstance(value, (list, tuple, set)):
            for nested in value:
                collect(nested)
            return
        text_parts.append(str(value))

    for value in values:
        collect(value)
    raw = "\n".join(text_parts)
    normalized = normalize_campaign_anchor_text(raw)
    return (
        KGD80_FESTIVAL_NAME in normalized
        or bool(re.search(r"(?iu)(?:https?://)?(?:www\.)?kgd80\.ru(?:[/?#:\s]|$)", raw))
    )


def ground_kgd80_festival(
    festival: Any,
    *,
    source_evidence: Iterable[Any],
    curated_festival_series: Any = None,
) -> tuple[str | None, bool]:
    """Return the source-grounded festival value and whether KGD80 was dropped."""

    if festival is None or isinstance(festival, bool):
        return None, False
    value = str(festival).strip()
    if not value:
        return None, False
    if normalize_campaign_anchor_text(value) != KGD80_FESTIVAL_NAME:
        return value, False
    curated = normalize_campaign_anchor_text(curated_festival_series)
    if curated == KGD80_FESTIVAL_NAME:
        return value, False
    if contains_explicit_kgd80_anchor(source_evidence):
        return value, False
    return None, True
