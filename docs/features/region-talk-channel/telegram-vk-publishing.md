# Telegram and VK publishing contracts

Status: daily diversity-aware selection plan implemented; public Telegram/VK
API publisher remains disabled. No channel/community creation or target-channel
publishing is performed by the planner.

The plan is publication-readiness gated: a terminal candidate without complete
Telegram/VK copy, attribution and grounded support cannot occupy a future
public slot. Evidence-backed legacy external-article copy can be projected from
the already validated research contract; social copy remains LLM-first and has
no heuristic fallback.

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

## Rights and attribution policy

The product default is **media-first**, not a bare link preview. A selected
source image/album may be transported unchanged into the editorial
recommendation when the original author/source is prominent and the original
URL is present. This is an attribution-bound editorial-use policy, not a claim
that Region Talk owns the file or that the asset has a reusable license.

`rights_policy` values:

- `unknown` — unchanged selected source media may be transported with prominent
  attribution; modified cards/Bento remain blocked.
- `link_only` — publish summary + original link/native preview only; do not
  transport the image as a standalone asset.
- `forward_allowed` — use platform-native forward/repost when technically/policy allowed.
- `media_reuse_allowed` — can render branded cards using source media.
- `blocked` — do not publish.

Every public post must include prominent source attribution and the original
link. `blocked` and `link_only` remain hard transport constraints. Unknown
asset rights are persisted honestly as `not_independently_verified`; that does
not silently convert them into either an owned asset or a Bento input.

## Media-first payload contract

ImageDiagnostic persists the transport recommendation consumed by the future
notifier/publisher:

- `presentation_recommendation=article_single_source_image` — use exactly the
  VLM-selected article-associated `selected_primary_media_id`;
- `source_media_hero` — one accepted source image;
- `source_media_carousel` — ordered `selected_media_ids`, capped by
  `presentation_max_assets` (currently 6; prefer 3–6 when available);
- `system_link_preview` — fallback when media is missing, unextractable,
  unsafe, unaccepted or explicitly link-only;
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
Implementing that resolver/notifier and the bounded Playwright request consumer
is outside ImageDiagnostic ownership.

### Product format priority

1. **P0:** unchanged source hero/carousel with strong attribution and original
   link. This minimizes reader friction and preserves the source's visual
   evidence.
2. **Fallback:** native system link preview, only if a suitable source asset
   cannot be carried.
3. **Later:** branded Bento/card when it adds comparison or narrative value
   and the exact asset is cleared for transformation. Do not make a decorative
   card merely to replace a stronger original photograph.

## Telegram target

Target: future Telegram channel **«О Калининграде говорят»**. Bot must be channel admin with send message/photo rights.

Modes:

1. `sendPhoto` — one selected strong image; caption carries summary.
2. `sendMediaGroup` — 2–10 images as album; first media caption may carry main text.
3. `sendMessage` fallback — native source preview when `rights_policy=link_only`
   or media is missing/unsafe/unconfirmed.

Caption style:

- concise;
- no full copy-paste;
- source attribution;
- original link;
- optional “Что отметили” block.

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
