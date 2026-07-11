# Static-site contract — event-comment-feedback

Status: draft. The public site consumes only static JSON/HTML artifacts.

## Manifest placement decision

Current site data flow uses:

- `site/src/data/preview-events.json` for build-time event cards/pages;
- `site/src/data/preview-related.json` and `/data/discovery/<event_id>.json` for event-detail related manifests;
- `site/src/lib/types.ts` / `site/src/lib/events.ts` as typed build-time adapters.

MVP recommendation: **separate compact build-time manifest**:

```text
site/src/data/comment-feedback.json
```

Why:

- avoids bloating every `PreviewEvent` row;
- allows Astro to render the block into static HTML with no browser fetch;
- mirrors `preview-related.json` as a feature-specific manifest;
- keeps raw `preview-events.json` stable for event facts/cards;
- can later be mirrored to `site/public/data/comment-feedback.json` for debug/preview or split into per-event files if it grows.

Avoid as MVP default:

- `PreviewEvent.comment_feedback`: couples public discussion state to the main event object and increases fixture size;
- per-event `site/public/data/comment-feedback/{event_id}.json`: useful later for large state but unnecessary for 1–5 items/event and would push rendering toward client fetch if not also imported at build time.

## Public JSON contract

```json
{
  "schema_version": "event-comment-feedback-v1",
  "generated_at": "2026-07-04T12:00:00Z",
  "phrase_bank_version": "event-comment-feedback-phrase-bank-v1",
  "events": {
    "1842": {
      "event_id": 1842,
      "status": "published",
      "comments_seen_count": 37,
      "comments_used_count": 14,
      "sources_count": 2,
      "updated_at": "2026-07-04T12:00:00Z",
      "items": [
        {
          "phrase_id": "anticipation_high",
          "tone": "positive",
          "signal_type": "anticipation",
          "icon": "smile_green",
          "public_sentence": "В комментариях отмечают, что это очень ожидаемое мероприятие",
          "evidence_count": 6,
          "unique_authors_count": 5,
          "sources_count": 2,
          "confidence": 0.86,
          "verification_method": "vector_strict_v1",
          "rotation_weight": 1.4
        }
      ]
    }
  }
}
```

`verification_method` may remain in debug/preview data. Production UI should not expose technical verification labels to users.

## Public export allowlist

Allowed item fields:

- `phrase_id`, `tone`, `signal_type`, `icon`, `public_sentence`;
- aggregate counts: `evidence_count`, `unique_authors_count`, `sources_count`;
- confidence/weight if used by renderer only;
- `updated_at` and status at event level.

Forbidden public fields:

- raw comments, representative snippets, quote text;
- author names, user ids, avatars, profile links;
- individual comment URLs;
- raw platform payloads;
- YDB keys beyond `event_id`/public `phrase_id`;
- verifier raw responses/reasons;
- private source fingerprints.

## Site UI contract

Block heading:

```text
Что видно по обсуждению
```

Subtitle/disclosure:

```text
Сводка открытых комментариев из источников события. Имена и прямые цитаты не публикуются.
```

Card:

```text
🙂
В комментариях отмечают, что это очень ожидаемое мероприятие

На основе 6 комментариев из 2 источников
```

Do not call the block “Отзывы”, “Рейтинг”, “Отзывы посетителей” or “Оценка события”.

## Icon/tone semantics

| Visual | Icon id | Tone | Meaning |
|---|---|---|---|
| Green smiling face | `smile_green` | `positive` | Positive interest, anticipation, trust, love for artist, intent to attend. |
| Gray neutral face | `neutral_gray` | `neutral` | Practical interest, questions, clarifications. |
| Red sad face | `sad_red` | `concern` | Frustration, barrier, doubt or problem; not necessarily “event is bad”. |

Red can be a popularity signal, e.g. “В комментариях расстраиваются, что билеты быстро закончились”. Do not let red cards dominate or appear first if strong positive/neutral context exists.

Do not rely only on color: include emoji/icon + text + accessible label.

## Carousel behavior

- Mobile: 1 card at a time.
- Desktop: 2–3 cards when space allows.
- Prev/next buttons and dots.
- Autoplay off by default; if enabled later, it must be slow, pause on interaction and respect `prefers-reduced-motion`.
- Max 1–5 cards per event; MVP preferably 1–3.
- Hide block entirely if no sufficiently confirmed items.
- Keyboard accessible controls, visible focus, semantic region label.

Default ordering:

1. anticipation/interest;
2. artists/program/organizers;
3. tickets/demand;
4. practical questions;
5. frustration/barriers.

Red cards should not be first when positive/neutral context exists and should be capped to avoid a negative-dominant block.

## Rendering fallback

- Manifest missing: block hidden; page remains valid.
- Event id missing or status not `published`: block hidden.
- `items.length === 0`: block hidden.
- JSON schema invalid: build/check should fail in preview; production runtime should not crash page.
- Stale status: either hide or show only previously approved non-stale items; never fetch live data in browser.

## Suggested TypeScript contracts

Implementation follow-up can add to `site/src/lib/types.ts`:

```ts
export type CommentFeedbackTone = 'positive' | 'neutral' | 'concern';
export type CommentFeedbackIcon = 'smile_green' | 'neutral_gray' | 'sad_red';

export interface EventCommentFeedbackItem {
  phrase_id: string;
  tone: CommentFeedbackTone;
  signal_type: string;
  icon: CommentFeedbackIcon;
  public_sentence: string;
  evidence_count: number;
  unique_authors_count: number;
  sources_count: number;
  confidence?: number;
  verification_method?: 'vector_strict_v1' | 'llm_verified_v1' | 'manual_review_v1';
  rotation_weight?: number;
}

export interface EventCommentFeedbackEntry {
  event_id: number;
  status: 'published' | 'suppressed' | 'stale' | 'error';
  comments_seen_count: number;
  comments_used_count: number;
  sources_count: number;
  updated_at: string;
  items: EventCommentFeedbackItem[];
}

export interface EventCommentFeedbackManifest {
  schema_version: 'event-comment-feedback-v1';
  generated_at: string;
  phrase_bank_version: string;
  events: Record<string, EventCommentFeedbackEntry>;
}
```

## SEO/GEO notes

The block can be rendered as visible HTML because it is useful event context, but it must be careful:

- Keep disclosure text close to the block.
- Do not place raw comments in HTML/JSON-LD.
- Do not add these phrases to event JSON-LD as canonical event facts.
- Do not use generated phrases as `og:description` until QA proves no overclaim.
- If snippets become noisy, mark controls/metadata `data-nosnippet`; keep the main cautious public sentence visible.
