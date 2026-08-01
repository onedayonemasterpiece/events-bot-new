# Telegram and VK publishing contracts

Status: daily diversity-aware selection plan implemented; public Telegram/VK
API publisher remains disabled. No channel/community creation or target-channel
publishing is performed by the planner.

The plan is publication-readiness gated: a terminal candidate without current
v9 two-paragraph Russian copy, attribution, grounded support and an exact media
materialization manifest cannot occupy a future public slot. Both articles and
social posts pass the staged LLM-first writer; retained article intake is
evidence, not a finished-copy projection.

The target Telegram channel is [**«Калининград с первого
взгляда»**](https://t.me/kalinigrad_visit) (`@kalinigrad_visit`). **«О
Калининграде говорят»** remains the product/editorial working name.

Operator-chat acknowledgement is bound to a fingerprint of the exact completed
draft, not only to the candidate URL. Legacy rows marked `sent_to_chat` before
the readiness gate are therefore delivered once again after grounded backfill,
and a materially regenerated draft is not hidden by the older delivery ledger.
The versioned delivery key remains idempotent for retries of the same draft.

The durable selection input is `publication_schedule_item`: exactly one
external article and one Telegram/VK social post per day. Future unlocked slots
are recalculated after every autonomous discovery session against actual
published history. Telegram and VK are two delivery targets for the same daily
content pair, not four independent selections.

## Source media and attribution policy

Region Talk intentionally republishes the selected source image, album or
video as a native media-first post and prominently links both the source and
the exact original. This is not a generic `rights_policy` gate. The media gate
answers product-integrity questions instead: is the asset actually associated
with the material, is it usable, and is the reviewed order exactly the order
that will be published? Missing materializable bytes/refs blocks that review
revision; genuinely absent usable media is recorded as a link-preview fallback.

## Media-first payload contract

ImageDiagnostic persists the transport recommendation consumed by the future
notifier/publisher:

- `presentation_recommendation=article_single_source_image` — use exactly the
  VLM-selected article-associated `selected_primary_media_id`;
- `source_media_hero` — one accepted source image;
- `source_media_carousel` — ordered `selected_media_ids`, capped by
  `presentation_max_assets` (currently 6; prefer 3–6 when available);
- `system_link_preview` — terminal fallback when associated source media is
  genuinely missing, unextractable, unsafe or unaccepted; a legacy
  `rights_policy=link_only` value is not sufficient;
- `browser_materialization_pending` — do not publish yet: the article exposed
  no safe static image evidence and its one-page bounded Playwright request is
  still unresolved;
- `source_attribution_required=true` and
  `presentation_media_policy=editorial_source_media_with_prominent_attribution`
  are mandatory checks, not display hints.

For article rows the visual decision additionally carries
`image_vlm_article_association_supported=true`, its reason, best ordinal and
the HTTP/DOM evidence in `web_image_used_evidence_json`. The publisher must
never substitute `scored[0]` when the VLM-ranked selection exists. Local
Kaggle paths are ephemeral. `selected_media_materialization_json` supplies the
ordered durable media ID, reviewed SHA-256, source ref and platform/page
refetch locator; `selected_media_materialization_fingerprint` binds that exact
selection. The notifier must reacquire the asset, verify the reviewed digest
where the platform still provides identical bytes, and otherwise send it back
through visual review rather than silently substitute another image.
The notifier resolves this exact manifest and sends the media being reviewed;
the bounded Playwright consumer handles JS-only article pages before the next
ImageDiagnostic pass. Materialization preserves `reviewed_content_sha256`, the
per-item fingerprint and the refetch locator. Telethon delivery verifies every
available reviewed SHA against the downloaded bytes before sending; a mismatch
returns the revision to review instead of publishing substituted media. Bot API
URL delivery fails closed for hash-bound revisions because it cannot inspect
the bytes Telegram will fetch.

For a native Telegram album, the exact source-post URL is itself a valid
materialization locator even when the upstream ledger has not expanded the
group into individual message IDs. The notifier resolves the anchor
`grouped_id`. A reviewed ordered `selected_media_ids` sequence is preferred
and survives the image-ledger → publication-manifest boundary unchanged. If an
older row has only a source-album locator, the notifier keeps Telegram's
original order and deterministically takes at most the first six frames; fewer
than three still fails closed. A single source-album locator therefore means
“resolve this exact group and apply the bounded fallback”, not “publish a
one-image album” or “send all ten platform frames”.
External article source attestations participate in the same live-fingerprint
merge as Telegram/VK sources so the autonomous orchestrator sees article
backfill work instead of leaving it visible only to the notifier.
The v8 backfill also fills missing presentation fields from the newest exact-URL
durable `image_queue_item`. This narrow evidence join carries either an already
extracted publisher hero or the reviewed ordered social selection into the
review manifest, but never overwrites an existing media selection, publication
verdict or editorial copy.

### Product format priority

1. **P0:** unchanged source hero/carousel with strong attribution and original
   link. This minimizes reader friction and preserves the source's visual
   evidence.
2. **Fallback:** native system link preview, only if a suitable source asset
   cannot be carried.
3. **Later:** branded Bento/card when it adds comparison or narrative value
   and a derivative visual is intentionally wanted. Do not make a decorative
   card merely to replace a stronger original photograph.

## Telegram target

Target: future Telegram channel **«О Калининграде говорят»**. Bot must be channel admin with send message/photo rights.

Modes:

1. `article_hero` / `social_hero` — one associated source image with the
   complete caption.
2. `social_album` — 3–6 ordered source images; one atomic album caption.
3. `social_video` — the source video and the same atomic caption.
4. `link_preview_fallback` — only when no usable source media exists, with a
   durable fallback reason.

Caption contract:

- exactly two editorial Russian paragraphs;
- paragraph 1 introduces the external source/optic and an honest bridge;
- paragraph 2 attributes one or two concrete observations in the third person
  and gives a reason to open the original;
- 550–900 visible characters in total;
- bold linked `Источник` and `Оригинал` lines;
- the caption and ordered media remain one review/publication revision.

Operational contract:

- dedupe by original post URL and `candidate_id`;
- idempotency key `region_talk:{candidate_id}:telegram`;
- retry on transient API errors; no infinite retry;
- store Telegram API response in `region_talk_publication_log`;
- document rollback/edit/delete limitations.

The future Telegram publisher may use MTProto instead of Bot API when custom
premium emoji entities are editorially needed. Only
`TELEGRAM_AUTH_BUNDLE_DISCOVERY1` / `DISCOVERY2` are eligible for that Region
Talk path, and only after the mapped Kaggle notebook is verified idle and the
per-bundle local lease is acquired. Generic `TELEGRAM_SESSION` and
`TELEGRAM_AUTH_BUNDLE_E2E` remain Codex/manual-E2E inputs and are never passed
to the functional publisher. Plain text remains the fallback; missing premium
emoji capability must not block a publication. This is a transport contract,
not an enabled target-channel publisher.

## VK target

Target: future VK community for **«О Калининграде говорят»**.

Preferred mode: unchanged selected source hero/carousel with prominent
attribution. Prepared cards are a later, permission-sensitive format rather
than the default.

VK image/card requirements:

- 1080×1350 preferred for mobile feed; 1080×1080 acceptable variant;
- source photo or generated publication card;
- round source avatar overlay;
- readable source title/handle/short link label;
- contrast plate/shadow/outline for dark and light backgrounds;
- safe margins, no obstruction of main visual subject;
- overlay must not imply source endorsement;
- use short handle on image; full URL in post text.

Example text style:

```text
О Калининграде говорят

[Источник] показал(а) Калининградскую область как маршрут с морем, дюнами и старой архитектурой.

В публикации отмечают:
— [positive смысл];
— [neutral/useful смысл];
— [optional concern].

Источник: [source title]
Оригинал: [link]
Это наше краткое summary публикации.
```

## VK API validation risk

Planned image path:

```text
photos.getWallUploadServer(group_id)
  → upload image to upload_url
  → photos.saveWallPhoto(group_id, photo, server, hash)
  → wall.post(owner_id=-group_id, from_group=1, attachments=photo...)
```

A recent `vk-api-schema` issue reports `photos.getWallUploadServer` failing with Community Token (`error_code=27`) while text-only `wall.post` works. Therefore VK image publishing must have a separate dry-run validation before production.

Mitigations if community token is insufficient:

- limited user-token operational policy;
- manual token rotation and redacted logs;
- separate VK publisher dry-run;
- fallback to text+link;
- fallback to Telegram-only until VK image upload is validated.

VK source-text restoration for the draft writer is a read-only operation and
already uses exact `wall.getById` through the established service/user token
priority. It neither grants nor implies permission to call `wall.post`.
