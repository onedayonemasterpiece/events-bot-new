from __future__ import annotations

import asyncio
import hashlib
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Iterable
from urllib.parse import quote


_HEADING_RE = re.compile(r"^#\s+(.+?)\s*$", re.MULTILINE)
_FIELD_RE = re.compile(
    r"^(?:[-*]\s*)?(?P<name>status|статус|severity|impact|date|дата)\s*:\s*(?P<value>.+?)\s*$",
    re.IGNORECASE | re.MULTILINE,
)
_WORD_RE = re.compile(r"[0-9A-Za-zА-Яа-яЁё_-]+", re.UNICODE)


@dataclass(frozen=True, slots=True)
class IncidentDocument:
    document_id: str
    title: str
    relative_path: str
    url: str
    text: str
    metadata: dict[str, str]
    fingerprint: str
    modified_at: float


class IncidentIndex:
    """Bounded index of repository incident Markdown documents."""

    _DIRECTORIES = (
        "docs/reports/incidents",
        "docs/incidents",
    )

    def __init__(
        self,
        repository_root: str,
        *,
        repository_slug: str,
        repository_ref: str = "main",
        cache_ttl_seconds: int = 60,
        scan_byte_limit: int = 3 * 1024 * 1024,
        max_document_chars: int = 60_000,
    ) -> None:
        self.root = Path(repository_root).resolve()
        self.repository_slug = repository_slug
        self.repository_ref = repository_ref or "main"
        self.cache_ttl_seconds = max(1, int(cache_ttl_seconds))
        self.scan_byte_limit = max(64 * 1024, int(scan_byte_limit))
        self.max_document_chars = max(1000, int(max_document_chars))
        self._documents: tuple[IncidentDocument, ...] = ()
        self._loaded_at = 0.0
        self._lock = Lock()

    @staticmethod
    def _normalise_query(value: str) -> list[str]:
        return [item.casefold() for item in _WORD_RE.findall(value or "") if len(item) >= 2]

    def _github_url(self, relative_path: str) -> str:
        path = quote(relative_path, safe="/")
        ref = quote(self.repository_ref, safe="")
        return f"https://github.com/{self.repository_slug}/blob/{ref}/{path}"

    @staticmethod
    def _document_id(relative_path: str) -> str:
        digest = hashlib.sha256(relative_path.encode("utf-8")).hexdigest()[:24]
        return f"incident:{digest}"

    def _extract_metadata(self, text: str) -> dict[str, str]:
        metadata: dict[str, str] = {}
        for match in _FIELD_RE.finditer(text[:8000]):
            key = match.group("name").casefold()
            if key in {"статус"}:
                key = "status"
            elif key in {"дата"}:
                key = "date"
            metadata.setdefault(key, match.group("value").strip()[:300])
        return metadata

    def _read_document(self, path: Path) -> IncidentDocument | None:
        try:
            resolved = path.resolve(strict=True)
            resolved.relative_to(self.root)
            stat = resolved.stat()
        except (FileNotFoundError, OSError, ValueError):
            return None
        if not resolved.is_file() or stat.st_size <= 0 or stat.st_size > 512 * 1024:
            return None
        try:
            raw = resolved.read_bytes()
            text = raw.decode("utf-8")
        except (OSError, UnicodeDecodeError):
            return None
        relative = resolved.relative_to(self.root).as_posix()
        heading = _HEADING_RE.search(text)
        title = (heading.group(1).strip() if heading else resolved.stem.replace("-", " "))[:500]
        clipped = text[: self.max_document_chars]
        return IncidentDocument(
            document_id=self._document_id(relative),
            title=title,
            relative_path=relative,
            url=self._github_url(relative),
            text=clipped,
            metadata=self._extract_metadata(clipped),
            fingerprint=hashlib.sha256(raw).hexdigest(),
            modified_at=stat.st_mtime,
        )

    def _load_sync(self) -> tuple[IncidentDocument, ...]:
        now = time.monotonic()
        with self._lock:
            if self._documents and now - self._loaded_at < self.cache_ttl_seconds:
                return self._documents
            total = 0
            documents: list[IncidentDocument] = []
            for relative_dir in self._DIRECTORIES:
                directory = (self.root / relative_dir).resolve()
                try:
                    directory.relative_to(self.root)
                except ValueError:
                    continue
                if not directory.is_dir():
                    continue
                for path in sorted(directory.rglob("*.md")):
                    try:
                        size = path.stat().st_size
                    except OSError:
                        continue
                    if size <= 0 or total + size > self.scan_byte_limit:
                        continue
                    document = self._read_document(path)
                    if document is None:
                        continue
                    total += size
                    documents.append(document)
            documents.sort(key=lambda item: (item.modified_at, item.relative_path), reverse=True)
            self._documents = tuple(documents)
            self._loaded_at = now
            return self._documents

    async def documents(self) -> tuple[IncidentDocument, ...]:
        return await asyncio.to_thread(self._load_sync)

    async def search(self, query: str, *, limit: int = 10) -> list[IncidentDocument]:
        tokens = self._normalise_query(query)
        if not tokens:
            return list((await self.documents())[: max(1, min(limit, 25))])
        scored: list[tuple[int, IncidentDocument]] = []
        for document in await self.documents():
            title = document.title.casefold()
            path = document.relative_path.casefold()
            body = document.text.casefold()
            score = 0
            for token in tokens:
                if token in title:
                    score += 12
                if token in path:
                    score += 8
                occurrences = body.count(token)
                score += min(occurrences, 8)
            if score:
                scored.append((score, document))
        scored.sort(key=lambda item: (item[0], item[1].modified_at), reverse=True)
        return [item[1] for item in scored[: max(1, min(limit, 25))]]

    async def get(self, document_id: str) -> IncidentDocument | None:
        if not document_id.startswith("incident:"):
            return None
        for document in await self.documents():
            if document.document_id == document_id:
                return document
        return None
