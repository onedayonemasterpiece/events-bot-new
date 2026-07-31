# Telegram and VK publishing contracts

Status: daily diversity-aware selection plan implemented; public Telegram/VK
API publisher remains disabled. No channel/community creation or target-channel
publishing is performed by the planner.

The durable selection input is `publication_schedule_item`: exactly one
external article and one Telegram/VK social post per day. Future unlocked slots
are recalculated after every autonomous discovery session against actual
published history. Telegram and VK are two delivery targets for the same daily
content pair, not four independent selections.

## Rights and attribution policy

`rights_policy` values:

- `unknown` — can appear in discovery/favorites report; autopublish with modified images blocked.
- `link_only` — publish summary + original link only; do not reuse images as standalone assets.
- `forward_allowed` — use platform-native forward/repost when technically/policy allowed.
- `media_reuse_allowed` — can render branded cards using source media.
- `blocked` — do not publish.

Every public post must include source attribution and original link. Unknown rights block autopublish with images.

## Telegram target

Target: future Telegram channel **«О Калининграде говорят»**. Bot must be channel admin with send message/photo rights.

Modes:

1. `sendPhoto` — one selected strong image; caption carries summary.
2. `sendMediaGroup` — 2–10 images as album; first media caption may carry main text.
3. `sendMessage` fallback — source link only when `rights_policy=link_only` or media unsafe.

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

## VK target

Target: future VK community for **«О Калининграде говорят»**.

Preferred mode: carousel-like wall post with multiple prepared image cards.

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
