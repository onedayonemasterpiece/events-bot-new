#!/usr/bin/env python3
"""Regenerate the checked-in KGD80 people catalog and all portrait avatars.

The script consumes the neighboring KGD80 checkout (or ``--kgd80-root``), so a
festival roster/media update is one reproducible sync, not per-event handwork.
New KenigEvents announcements are matched to this registry by Smart Update.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
import unicodedata
from pathlib import Path
from typing import Any

from PIL import Image


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_KGD80_ROOT = next(
    (
        candidate
        for candidate in (
            PROJECT_ROOT.parent / "kdg80",
            PROJECT_ROOT.parents[1] / "kdg80",
        )
        if candidate.exists()
    ),
    PROJECT_ROOT.parent / "kdg80",
)


def words(value: str) -> set[str]:
    value = unicodedata.normalize("NFKC", value).casefold().replace("ё", "е")
    return set(re.findall(r"[a-zа-я]+", value))


def slug(value: str) -> str:
    table = str.maketrans(
        {
            "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e",
            "ё": "e", "ж": "zh", "з": "z", "и": "i", "й": "y", "к": "k",
            "л": "l", "м": "m", "н": "n", "о": "o", "п": "p", "р": "r",
            "с": "s", "т": "t", "у": "u", "ф": "f", "х": "h", "ц": "ts",
            "ч": "ch", "ш": "sh", "щ": "sch", "ъ": "", "ы": "y", "ь": "",
            "э": "e", "ю": "yu", "я": "ya",
        }
    )
    value = unicodedata.normalize("NFKC", value).casefold().translate(table)
    return re.sub(r"[^a-z0-9]+", "-", value).strip("-")


def built_cards(root: Path) -> list[dict[str, Any]]:
    index_path = root / "site" / "dist" / "index.html"
    more_path = root / "site" / "dist" / "data" / "speakers-more.json"
    if not index_path.exists() or not more_path.exists():
        return []
    match = re.search(
        r'data-speaker-initial-cards="([^"]+)"',
        index_path.read_text(encoding="utf-8"),
    )
    if not match:
        return []
    first = json.loads(html.unescape(match.group(1)))
    more = json.loads(more_path.read_text(encoding="utf-8")).get("cards") or []
    return [*first, *more]


def regalia_names(root: Path) -> list[str]:
    path = root / "Исходные данные" / "regalii_spikerov_kratko_full.md"
    if not path.exists():
        return []
    return [
        match.group(1).strip()
        for match in re.finditer(
            r"^##\s+([А-ЯЁ][^\n]+)$",
            path.read_text(encoding="utf-8"),
            flags=re.MULTILINE,
        )
    ]


def master_affiliations(root: Path) -> list[tuple[str, str]]:
    path = root / "Исходные данные" / "festival_site_master_actual_v4.md"
    if not path.exists():
        return []
    return [
        (match.group(1).strip(), match.group(2).strip())
        for match in re.finditer(
            r"^-\s+([А-ЯЁ][^—\n]+?)\s+—\s+([^\n]+)$",
            path.read_text(encoding="utf-8"),
            flags=re.MULTILINE,
        )
    ]


def choose_name(
    manifest_name: str,
    *,
    roster_names: list[str],
    fallback_names: list[str],
) -> str:
    query = words(manifest_name)
    candidates = [
        name
        for name in [*roster_names, *fallback_names]
        if len(query & words(name)) >= 2
    ]
    candidates.sort(key=lambda value: (-len(words(value)), len(value), value))
    return candidates[0] if candidates else manifest_name


def write_avatar(source: Path, destination: Path) -> None:
    """Create a deterministic face/upper-body crop for the 64px medallion."""

    image = Image.open(source).convert("RGBA")
    alpha_box = image.getchannel("A").getbbox()
    if alpha_box:
        left, top, right, bottom = alpha_box
        subject_width = right - left
        subject_height = bottom - top
        # The KGD80 cards are transparent cut-outs. Their alpha bounds give a
        # reliable CPU-only subject box; focusing its upper 28% keeps the face
        # and upper torso legible without semantic/identity inference.
        crop_size = int(
            max(subject_width * 1.18, subject_height * 0.46, 260)
        )
        crop_size = min(crop_size, image.width, image.height)
        center_x = (left + right) / 2
        center_y = top + subject_height * 0.28
        crop_left = max(
            0,
            min(image.width - crop_size, int(center_x - crop_size / 2)),
        )
        crop_top = max(
            0,
            min(image.height - crop_size, int(center_y - crop_size / 2)),
        )
        image = image.crop(
            (
                crop_left,
                crop_top,
                crop_left + crop_size,
                crop_top + crop_size,
            )
        )
    image = image.resize((512, 512), Image.Resampling.LANCZOS)
    image.save(
        destination,
        "WEBP",
        quality=88,
        method=4,
        exact=True,
    )


def generate(kgd80_root: Path, output_root: Path) -> dict[str, Any]:
    manifest_path = kgd80_root / "site" / "src" / "data" / "media-manifest.json"
    roster_path = (
        kgd80_root
        / "registration"
        / "src"
        / "data"
        / "festival-event-speakers.json"
    )
    source_assets = kgd80_root / "site" / "public" / "generated" / "speakers"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))["speakers"]
    roster = json.loads(roster_path.read_text(encoding="utf-8"))
    roster_names = sorted({str(value) for value in roster.values() if str(value).strip()})
    cards = built_cards(kgd80_root)
    cards_by_file = {
        Path(str(card.get("image") or "")).name: card
        for card in cards
        if card.get("image")
    }
    fallback_names = regalia_names(kgd80_root)
    affiliations = master_affiliations(kgd80_root)
    people: list[dict[str, Any]] = []

    for manifest_name, raw_images in manifest.items():
        photo_files = [Path(str(value)).name for value in raw_images]
        primary_file = photo_files[0]
        card = cards_by_file.get(primary_file, {})
        name = str(card.get("name") or "").strip() or choose_name(
            str(manifest_name),
            roster_names=roster_names,
            fallback_names=fallback_names,
        )
        aliases = {name, str(manifest_name)}
        name_words = words(name)
        aliases.update(
            roster_name
            for roster_name in roster_names
            if len(name_words & words(roster_name)) >= 2
        )
        parts = name.split()
        if len(parts) >= 3:
            aliases.update({f"{parts[0]} {parts[-1]}", f"{parts[-1]} {parts[0]}"})
        for alias in list(aliases):
            alias_parts = alias.split()
            if len(alias_parts) == 2:
                aliases.add(f"{alias_parts[1]} {alias_parts[0]}")
        anchor = str(card.get("anchor") or "speakers")
        affiliation = str(card.get("affiliation") or "").strip()
        if not affiliation:
            matching_affiliations = [
                value
                for candidate_name, value in affiliations
                if len(words(name) & words(candidate_name)) >= 2
            ]
            affiliation = matching_affiliations[0] if matching_affiliations else ""
        people.append(
            {
                "artist_id": f"kgd80:{slug(name)}",
                "display_name": name,
                "aliases": sorted(aliases),
                "primary_domain": affiliation,
                "locality_status": "local_verified",
                "base_country_code": "RU",
                "base_region_code": "RU-KGD",
                "base_city": "Калининград",
                "photo_url": f"/assets/participants/{primary_file}",
                "photo_source_url": (
                    f"https://kgd80.ru/generated/speakers/{primary_file}"
                ),
                "photo_files": photo_files,
                "profile_url": f"https://kgd80.ru/#{anchor}",
                "credit_text": "80 историй о главном · kgd80.ru",
            }
        )

    # The registration roster is the identity source of truth. Most people
    # have a dedicated transparent portrait in media-manifest; keep the few
    # remaining speakers too and let the static UI use its initials fallback.
    for event_slug, roster_name in sorted(roster.items()):
        name = str(roster_name or "").strip()
        if not name:
            continue
        name_words = words(name)
        if any(len(name_words & words(person["display_name"])) >= 2 for person in people):
            continue
        parts = name.split()
        aliases = {name}
        if len(parts) >= 3:
            aliases.update({f"{parts[0]} {parts[-1]}", f"{parts[-1]} {parts[0]}"})
        matching_affiliations = [
            value
            for candidate_name, value in affiliations
            if len(name_words & words(candidate_name)) >= 2
        ]
        people.append(
            {
                "artist_id": f"kgd80:{slug(name)}",
                "display_name": name,
                "aliases": sorted(aliases),
                "primary_domain": matching_affiliations[0]
                if matching_affiliations
                else "",
                "locality_status": "local_verified",
                "base_country_code": "RU",
                "base_region_code": "RU-KGD",
                "base_city": "Калининград",
                "photo_url": None,
                "photo_source_url": None,
                "photo_files": [],
                "profile_url": f"https://kgd80.ru/#event-{event_slug}",
                "credit_text": None,
            }
        )

    people.sort(key=lambda item: item["display_name"])
    assigned_files = {
        filename
        for person in people
        for filename in person.get("photo_files") or []
    }
    # Keep alternate portraits that exist in the KGD80 media folder even when
    # the current manifest selects only one of them.
    for path in source_assets.glob("*.webp"):
        if path.name in assigned_files:
            continue
        prefix_match = next(
            (
                person
                for person in people
                if path.stem.startswith(
                    f"{Path(str(person['photo_url'])).stem}-"
                )
            ),
            None,
        )
        if prefix_match is not None:
            prefix_match["photo_files"].append(path.name)
            assigned_files.add(path.name)
            continue
        file_words = words(path.stem)
        candidates = sorted(
            people,
            key=lambda person: (
                -len(file_words & words(person["display_name"])),
                person["display_name"],
            ),
        )
        if candidates and len(file_words & words(candidates[0]["display_name"])) >= 2:
            candidates[0]["photo_files"].append(path.name)
            assigned_files.add(path.name)
    encoded = json.dumps(
        people,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    payload = {
        "schema_version": "kenigevents.kgd80_people.v1",
        "source_project": "https://kgd80.ru/",
        "source_revision": hashlib.sha256(encoded).hexdigest()[:16],
        "people": people,
    }
    catalog_path = output_root / "event_people" / "data" / "kgd80_people.json"
    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    catalog_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    output_assets = output_root / "site" / "public" / "assets" / "participants"
    output_assets.mkdir(parents=True, exist_ok=True)
    expected = {path.name for path in source_assets.glob("*.webp")}
    for path in source_assets.glob("*.webp"):
        write_avatar(path, output_assets / path.name)
    for path in output_assets.glob("*.webp"):
        if path.name not in expected:
            path.unlink()
    return {
        "people": len(people),
        "photos": len(expected),
        "source_revision": payload["source_revision"],
        "catalog": str(catalog_path),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--kgd80-root",
        type=Path,
        default=DEFAULT_KGD80_ROOT,
    )
    parser.add_argument("--output-root", type=Path, default=PROJECT_ROOT)
    args = parser.parse_args()
    print(
        json.dumps(
            generate(args.kgd80_root.resolve(), args.output_root.resolve()),
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
