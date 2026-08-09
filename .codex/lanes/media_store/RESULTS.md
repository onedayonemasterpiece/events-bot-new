# media_store lane results

## Scope

- Lane ID: `media_store`
- Requirement IDs: `MEDIA-01` secure owner-bound image ingress; `MEDIA-02` SSRF-safe network boundary with injectable resolver/fetcher; `MEDIA-03` immutable filesystem persistence and bounded SQLite manifest; `MEDIA-04` verified open/expiry cleanup lifecycle; `MEDIA-05` adversarial regression suite.
- Writable paths used: `private_events_mcp_media.py`, `tests/test_private_events_mcp_media_store.py`, `.codex/lanes/media_store/RESULTS.md`.
- Forbidden paths: all existing files and all other paths. None were edited.

## Outcome / evidence

Implemented `SecureMediaAssetStore`, structurally compatible with `AssetIngestor.ingest(file, *, owner_binding, max_bytes, expires_at)` and the exact provisional `VerifiedAsset` fields. The integration contract is imported lazily. Opaque refs use `ing_...`; returned digests use `sha256:<64 lowercase hex>` while the internal manifest stores the bare digest.

Security properties covered:

- HTTPS-only, port 443, no userinfo/fragments/control characters/backslashes/IP literals/legacy numeric hosts.
- Required exact or explicit `*.suffix` host allowlist; fresh DNS resolution on every ingress; all answers must be public; aiohttp's resolver is pinned to the validated answer set.
- Redirects disabled/rejected; no auth, cookie, or referrer headers; proxy/environment trust and decompression disabled.
- Streamed body with per-request and service byte limits plus a hard wall-clock timeout.
- Private `0700` root, atomically created `0600` key/manifest/temp files, atomic non-overwriting hard-link publication, final `0400`/single-link assets, directory/file fsync, and `O_NOFOLLOW` verified reads.
- Pillow content sniff + full `verify()` for JPEG/PNG/WebP, declared/HTTP MIME consistency, dimension/pixel caps, role allow matrix. MP4/video is explicitly rejected.
- Exact lowercase SHA-256 owner binding; only keyed owner/file-id fingerprints are persisted. URL, raw file ID, and file name are never persisted.
- Per-asset default 30 MiB, aggregate retained-store default 128 MiB enforced under an immediate SQLite transaction, configured TTL default 1 hour and hard maximum 24 hours.
- Every open/verify rechecks ownership, expiry, regular-file/link shape, byte length, and SHA-256 before returning a handle. Cleanup removes expired rows and assets without following symlinks.

## Commands run

- `python3 -m py_compile private_events_mcp_media.py tests/test_private_events_mcp_media_store.py` — passed.
- `uvx ruff format private_events_mcp_media.py tests/test_private_events_mcp_media_store.py` — passed / no pending formatting changes.
- `uvx ruff check private_events_mcp_media.py tests/test_private_events_mcp_media_store.py` — passed.
- `uv run --with pytest --with Pillow python -m pytest --noconftest -q tests/test_private_events_mcp_media_store.py` — **49 passed**.
- Initial repository-wide pytest bootstrap attempts were not usable in this clean worktree: system Python lacked pytest, and a temporary minimal pytest/Pillow environment loaded the repository `tests/conftest.py`, which imports unavailable `aiogram`. `--noconftest` was then used because this self-contained lane does not depend on global fixtures.

## Risks / intentional limits

- Production ingestion is image-only. A bounded MP4 atom parser plus trustworthy video-track/codec validation was not safely feasible in this bounded lane. Signature-only `ftyp` acceptance would fake validation, so videos remain fail-closed until a real validator is integrated.
- The SQLite manifest and HMAC key depend on the configured media root being on a local filesystem with normal POSIX atomic link/fsync/permission semantics. Deployments must not place it on a filesystem that weakens those guarantees.
- The store supplies constructor configuration and does not read environment variables itself; the core integration owns env mapping and cleanup scheduling.

## Git

- Base SHA: `80f7bc6c31125abba67575dc94d0fa2b730db247`
- Implementation head SHA: `1033cacfd65ca439875c146dccef6ce3274f436b` (the following evidence-only commit adds this results file; final branch SHA is reported in the parent handoff).
- Branch: `agent/mcp-media-store`
- Changed files:
  - `private_events_mcp_media.py`
  - `tests/test_private_events_mcp_media_store.py`
  - `.codex/lanes/media_store/RESULTS.md`
