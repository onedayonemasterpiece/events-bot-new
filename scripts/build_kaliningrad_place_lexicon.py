#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Iterable

FIELDS = [
    "place_id", "canonical_name", "place_type", "municipality", "district_or_okrug",
    "is_city", "is_settlement", "is_tourist_place", "is_nature_place", "is_historical_name",
    "aliases", "old_names", "latin_aliases", "common_misspellings", "geo_scope", "priority_tier",
    "ambiguity_level", "allowed_for_kaliningrad_scope", "requires_context", "reject_if_external_context",
    "source_url", "source_note",
]
AMBIGUOUS_CONTEXT_NAMES = {"Лесной", "Морское", "Рыбачий", "Приморье", "Русское", "Малиновка", "Куликово", "Покровское", "Дивное", "Ясное", "Северный", "Красное", "Весёлое", "Высокое", "Луговое", "Невское"}

CORE_REQUIRED = {
    "Калининград", "Багратионовск", "Балтийск", "Гвардейск", "Гурьевск", "Гусев", "Зеленоградск",
    "Краснознаменск", "Ладушкин", "Мамоново", "Неман", "Нестеров", "Озёрск", "Пионерский",
    "Полесск", "Правдинск", "Светлогорск", "Светлый", "Славск", "Советск", "Черняховск", "Янтарный",
    "Краснолесье", "Виштынецкое озеро", "Роминтенская пуща", "Куршская коса", "Балтийская коса",
}


def norm_name(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower().replace("ё", "е"))


def split_multi(value: str) -> list[str]:
    return [x.strip() for x in re.split(r"\s*;\s*", value or "") if x.strip()]


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return [dict(r) for r in csv.DictReader(f)]


def validate(rows: list[dict[str, str]]) -> None:
    ids: set[str] = set()
    canonical: set[str] = set()
    errors: list[str] = []
    for i, row in enumerate(rows, start=2):
        for field in FIELDS:
            row.setdefault(field, "")
        pid = row["place_id"].strip()
        name = row["canonical_name"].strip()
        if not pid:
            errors.append(f"row {i}: empty place_id")
        if not name:
            errors.append(f"row {i}: empty canonical_name")
        if pid in ids:
            errors.append(f"row {i}: duplicate place_id {pid}")
        ids.add(pid)
        n = norm_name(name)
        if n in canonical:
            errors.append(f"row {i}: duplicate canonical_name {name}")
        canonical.add(n)
        for col in ("aliases", "old_names", "latin_aliases", "common_misspellings"):
            if "," in (row.get(col) or ""):
                errors.append(f"row {i}: {col} must use semicolon separators, not commas")
        if (row.get("ambiguity_level") or "") == "high" and (row.get("requires_context") or "").lower() != "true":
            errors.append(f"row {i}: high ambiguity must require_context=true")
    missing = sorted(CORE_REQUIRED - {r.get("canonical_name", "").strip() for r in rows})
    if missing:
        errors.append("missing required core places: " + "; ".join(missing))
    # Old German names should remain aliases/features, not external-region evidence by themselves.
    for row in rows:
        if row.get("old_names") and (row.get("allowed_for_kaliningrad_scope") or "").lower() != "true":
            errors.append(f"old-name row must remain allowed-for-scope: {row.get('canonical_name')}")
    if errors:
        raise SystemExit("Lexicon validation failed:\n- " + "\n- ".join(errors))


def dedupe_rows(rows: Iterable[dict[str, str]]) -> list[dict[str, str]]:
    seen_ids: set[str] = set()
    out: list[dict[str, str]] = []
    for raw in rows:
        row = {field: str(raw.get(field, "") or "").strip() for field in FIELDS}
        if row.get("canonical_name") in AMBIGUOUS_CONTEXT_NAMES:
            row["requires_context"] = "true"
            row["ambiguity_level"] = "high"
        if not row["place_id"]:
            row["place_id"] = "kgd_place_" + re.sub(r"[^a-z0-9]+", "_", norm_name(row["canonical_name"]))[:80].strip("_")
        base = row["place_id"]
        suffix = 2
        while row["place_id"] in seen_ids:
            row["place_id"] = f"{base}_{suffix}"
            suffix += 1
        seen_ids.add(row["place_id"])
        out.append(row)
    return out


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)


def write_json(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": "v1",
        "row_count": len(rows),
        "normalization": "lowercase + ё/е + whitespace collapse; semicolon-separated aliases",
        "rows": rows,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate/export the Region Talk Kaliningrad place lexicon.")
    parser.add_argument("--input", default="docs/features/region-talk-channel/kaliningrad-place-lexicon-v1.csv")
    parser.add_argument("--docs-output", default="docs/features/region-talk-channel/kaliningrad-place-lexicon-v1.csv")
    parser.add_argument("--artifact-output", default="artifacts/region-talk/place-lexicon-latest.csv")
    parser.add_argument("--json-output", default="data/region-talk/place-lexicon-latest.json")
    args = parser.parse_args()
    rows = dedupe_rows(read_rows(Path(args.input)))
    validate(rows)
    write_csv(Path(args.docs_output), rows)
    write_csv(Path(args.artifact_output), rows)
    write_json(Path(args.json_output), rows)
    print(json.dumps({"ok": True, "rows": len(rows), "docs_output": args.docs_output, "json_output": args.json_output}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
