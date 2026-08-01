# VISUAL-CODE results

- Lane: `VISUAL-CODE`
- Requirements: `R05`, `R06`
- Base SHA: `66ce2a5ae2c175bae3aa2f968e7785089b731dc8`
- Implementation head SHA: `9484ac5c`
- Scope: ImageDiagnostic article/source-media association, durable materialization contract, media-first presentation recommendation, tests and canonical visual/publishing docs.

## Delivered

- HTTP-first bounded article candidate extraction from JSON-LD Article types, `article`/`main` figure/picture/image, lightboxes, `og:image`, `twitter:image` and `image_src`.
- Per-candidate evidence: URL, canonical referrer, DOM/metadata role, alt, caption, dimensions, association reason/matched terms; obvious logo/avatar/ad/tracker/related/tiny assets are rejected before scoring.
- Selective VLM association contract binds external-article selection to title/summary and untrusted HTTP/DOM evidence, rejects unrelated site assets, and returns best plus ranked ordinals.
- VLM best/ranked ordinals now reorder `selected_media_ids`; selection no longer silently uses frame 1 / `scored[0]`. Bounded social selection defaults to at most 6 assets.
- Durable per-frame materialization ledger: reviewed SHA-256, source ref, exact Telegram message / VK wall attachment / article page+DOM evidence refetch locator and fingerprint. The ordered selected subset has its own JSON payload and fingerprint.
- Media-first recommendation fields:
  - `article_single_source_image`
  - `source_media_hero`
  - `source_media_carousel`
  - `system_link_preview` only as missing/unaccepted-media fallback
  - `browser_materialization_pending` for unresolved JS-only pages
  - `source_attribution_required=true`
  - `presentation_media_policy=editorial_source_media_with_prominent_attribution`
  - `visual_asset_rights_status=not_independently_verified` (no ownership/license overclaim)
- JS-only/blocked pages produce one-page bounded `browser_materialization_request_json` and durable queue status `needs_browser_materialization`; ImageDiagnostic skips repeat static-page hot loops.
- Docs record P0 unchanged source hero/carousel with prominent attribution, native preview fallback, and permission-sensitive Bento later.

## Notifier/orchestrator integration dependency

This lane did not edit forbidden notifier/orchestrator files. Their follow-up contract is exact:

1. Consume `presentation_recommendation`, `presentation_max_assets`, ordered `selected_media_ids`, and `selected_media_materialization_json`.
2. Reacquire each selected asset with its `refetch_locator`; compare `reviewed_content_sha256` when stable bytes are available. On mismatch, route back to visual review; never substitute another frame.
3. Article delivery uses exactly one `selected_primary_media_id` and requires `image_vlm_article_association_supported=true`.
4. Social carousel uses the ordered selected materialization, normally 3–6 images; hero otherwise.
5. Always render prominent author/source attribution plus original post/article URL.
6. A bounded local/orchestrated Playwright consumer must claim `needs_browser_materialization`, use only the request's canonical page/selectors/limits, and return direct refs/evidence. Until resolved, do not publish a random/system preview.

## Validation

Commands:

```text
uv run --with-requirements requirements.txt --with openpyxl pytest -q tests/test_region_talk_image_diagnostic.py
# 54 passed in 2.03s

uv run --with-requirements requirements.txt --with openpyxl pytest -q tests/test_region_talk*.py
# 639 passed in 34.88s

git diff --check
# clean
```

## Changed files

- `kaggle/RegionTalkImageDiagnostic/region_talk_image_diagnostic.py`
- `tests/test_region_talk_image_diagnostic.py`
- `docs/features/region-talk-channel/image-postcardness.md`
- `docs/features/region-talk-channel/telegram-vk-publishing.md`
- `CHANGELOG.md`
- `.codex/lanes/visual-code/RESULTS.md`

## Risks / blockers

- The ImageDiagnostic handoff exists, but the forbidden-scope notifier/refetcher and bounded Playwright consumer must be integrated by their owning lane before production can actually attach these durable media assets.
- Public source-media transport is explicitly attribution-bound and does not claim asset ownership or a reusable license; modified Bento/cards remain a separate permission-sensitive phase.
- No deployment or production mutation was performed.
