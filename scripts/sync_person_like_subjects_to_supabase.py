#!/usr/bin/env python3
"""Upsert the checked-in verified people as allowed Supabase like subjects."""

from __future__ import annotations

import argparse
import json
import os
import urllib.error
import urllib.request
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = PROJECT_ROOT / "event_people" / "data" / "kgd80_people.json"


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line or line.lstrip().startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def request_json(
    method: str,
    url: str,
    *,
    key: str,
    payload: object | None = None,
    prefer: str | None = None,
) -> object:
    body = (
        json.dumps(payload, ensure_ascii=False).encode("utf-8")
        if payload is not None
        else None
    )
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if prefer:
        headers["Prefer"] = prefer
    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:500]
        raise RuntimeError(f"Supabase HTTP {exc.code}: {detail}") from exc
    return json.loads(raw) if raw else {}


def sync(catalog_path: Path) -> dict[str, int | str]:
    load_dotenv(PROJECT_ROOT / ".env")
    load_dotenv(Path("/home/dev/projects/events-bot-new/.env"))
    url = os.getenv("PERSONALIZATION_SUPABASE_URL", "").rstrip("/")
    key = (
        os.getenv("PERSONALIZATION_SUPABASE_SECRET_KEY")
        or os.getenv("PERSONALIZATION_SUPABASE_SERVICE_ROLE_KEY")
        or ""
    )
    if not url or not key:
        raise RuntimeError("personalization Supabase URL/secret is missing")
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    revision = str(catalog["source_revision"])
    rows = [
        {
            "person_id": str(person["artist_id"]),
            "active": True,
            "source_revision": revision,
        }
        for person in catalog["people"]
        if str(person.get("artist_id") or "").strip()
    ]
    endpoint = (
        f"{url}/rest/v1/personalization_person_like_subject"
        "?on_conflict=person_id"
    )
    request_json(
        "POST",
        endpoint,
        key=key,
        payload=rows,
        prefer="resolution=merge-duplicates,return=minimal",
    )
    verified = request_json(
        "GET",
        (
            f"{url}/rest/v1/personalization_person_like_subject"
            f"?source_revision=eq.{revision}&select=person_id"
        ),
        key=key,
    )
    return {
        "catalog_people": len(rows),
        "verified_rows": len(verified) if isinstance(verified, list) else 0,
        "source_revision": revision,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", type=Path, default=CATALOG_PATH)
    args = parser.parse_args()
    print(json.dumps(sync(args.catalog.resolve()), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
