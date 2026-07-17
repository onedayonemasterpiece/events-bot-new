from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

from .schema import SCHEMA_VERSION, TIMEZONE, validate_provider_manifest


def fetch_source(url: str, *, timeout_seconds: int = 30) -> tuple[bytes, str]:
    request = Request(url, headers={"User-Agent": "KenigEventsTransportRefresh/1.0"})
    with urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310 - operator-configured HTTPS URL
        final_url = response.geturl()
        if not final_url.startswith("https://"):
            raise ValueError("transport source must remain HTTPS after redirects")
        return response.read(), final_url


def normalize_source_payload(provider: str, payload: bytes, *, source_url: str, fetched_at: datetime) -> dict[str, Any]:
    """Normalize provider adapter output into the common contract.

    Provider adapters may return the complete v1 object or a compact object with
    ``services`` and ``validity``. This boundary intentionally refuses HTML/PDF:
    a provider parser must produce explicit dated records rather than letting a
    generic scraper infer timetable semantics.
    """

    decoded = json.loads(payload.decode("utf-8"))
    if not isinstance(decoded, dict):
        raise ValueError("provider payload must be a JSON object")
    document_hash = hashlib.sha256(payload).hexdigest()
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "provider": provider,
        "snapshot_id": decoded.get("snapshot_id") or f"{provider}-{fetched_at.strftime('%Y%m%dT%H%M%SZ')}-{document_hash[:12]}",
        "fetched_at": fetched_at.astimezone(timezone.utc).isoformat(),
        "timezone": decoded.get("timezone") or TIMEZONE,
        "source": {
            "url": source_url,
            "fetched_at": fetched_at.astimezone(timezone.utc).isoformat(),
            "document_sha256": document_hash,
        },
        "validity": decoded.get("validity"),
        "services": decoded.get("services"),
    }
    return validate_provider_manifest(manifest, expected_provider=provider, now=fetched_at)


def run_provider_job(config: dict[str, Any], *, output_dir: str | Path, now: datetime | None = None) -> dict[str, Any]:
    provider = str(config.get("provider") or "")
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    payload_path = config.get("source_payload_path")
    if payload_path:
        path = Path(str(payload_path))
        payload, source_url = path.read_bytes(), str(config.get("source_url") or "https://fixture.invalid/provider.json")
    else:
        source_url = str(config.get("source_url") or "")
        payload, source_url = fetch_source(source_url, timeout_seconds=int(config.get("timeout_seconds") or 30))
    manifest = normalize_source_payload(provider, payload, source_url=source_url, fetched_at=now)
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    manifest_name = f"transport-{provider}-manifest.json"
    (output / manifest_name).write_text(json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    result = {
        "ok": True,
        "provider": provider,
        "snapshot_id": manifest["snapshot_id"],
        "snapshot_hash": manifest["snapshot_hash"],
        "content_hash": manifest["content_hash"],
        "service_count": len(manifest["services"]),
        "manifest": manifest_name,
        "finished_at": datetime.now(timezone.utc).isoformat(),
    }
    (output / "transport_provider_result.json").write_text(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return result
