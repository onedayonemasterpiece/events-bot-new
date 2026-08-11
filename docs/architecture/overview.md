# Architecture

## Static personal announcements data boundary

The static-site release uses four deliberately separate owners:

- Fly SQLite — canonical events/sources/lifecycle/publication state;
- personalization Supabase/Postgres — identity, current user profile, consent, favorites/subscriptions and email control plane;
- YDB — service-only analytics/history and independent comment-feedback sidecar;
- Object Storage/CDN — generated public/personal artifacts.

Canonical decision: [personalization data ownership](personalization-data-ownership.md).

Email edge services do not change that data boundary: SpaceWeb is the retained
human/inbound mailbox, a read-only Yandex IMAP collector handles the production
automation copy while Mail Trigger is direct-canary-only, and Postbox is the
transactional transport. NotiSend owns personal recommendations plus the narrow
reviewed repeat/test Auth route; one Supabase admission set enforces its shared
200-unique-recipient ceiling. The operational contract is [email infrastructure
and delivery](../operations/email-delivery.md).

The bot is built with **aiogram 3** and runs on Fly.io using a webhook.

- **Web Server** – aiohttp application that receives updates on `/webhook`.
- **Bot Framework** – aiogram Dispatcher handles commands and callback queries.
- **Database** – SQLite accessed through SQLModel and `aiosqlite`. The default
  path is `/data/db.sqlite` mounted from a Fly volume.
- **Deployment** – Docker container on Fly.io with volume `data` attached to
  `/data`.

## Raw-first, typed source ingestion

Configured Telegram/VK/source-parser inputs share a provider-neutral semantic
boundary. Transport-specific collectors may enforce source configuration,
pagination, crawl horizon, byte limits, and quota admission, but must persist the
raw carrier revision before any semantic decision. Keyword, date, historical,
cancellation, and `event_ts_hint` detections are neutral hints only; they cannot
drop a fetched carrier, confirm no-event, or veto a positive child.

```text
configured source
  -> durable immutable raw source packet/revision
  -> attachment/OCR EvidenceManifest
  -> automatic SourceParseDecision
  -> optional conditional contradiction verification
  -> Smart Update for each event child + typed lifecycle actions
  -> closed automatic outcome or durable due retry
```

The evidence manifest binds the raw-text hash/length and the complete attachment
inventory to available, included, omitted, unavailable, and truncated OCR/media
evidence. A negative semantic conclusion is forbidden while evidence is
incomplete. Empty, malformed, truncated, timed-out, quota-limited, or otherwise
technical responses are retryable rather than semantic no-event. Legacy payloads
without a typed decision are accepted only through a fail-closed compatibility
adapter: grounded positive children may continue, but an empty legacy array is
never terminal evidence.

`SourceParseDecision` has a closed set of dispositions:

- `EVENTS_FOUND`;
- `LIFECYCLE_ONLY`;
- `MIXED`;
- `CONFIRMED_NO_EVENT`;
- `RETRY_REQUIRED`.

Only a complete, structurally valid decision can confirm no-event. Conditional
verification is reserved for explicit contradictions or incomplete coverage; it
is not an unconditional second semantic pass. Smart Update returns closed typed
child outcomes and separates accepted event identity from any diagnostic ID.
Downstream publication/page jobs run only for accepted outcomes. Provider,
schema, persistence, quota, timeout, or unresolved-action failures retain typed
retry metadata and remain due; there is no terminal technical-failure state.

Durable crawl continuations are consumed automatically. Their lifecycle is
`pending/retry due -> leased/running -> persist every raw packet in the page ->
advance or done`. A crash, expired lease, or typed provider/backpressure failure
returns the continuation to a bounded due retry. Source cursors advance only
after every fetched in-horizon packet is durable. Manual review and legacy inbox
screens are diagnostic/admin surfaces, not required transitions in this state
machine. The detailed VK contract is
[`../features/vk-auto-queue/README.md`](../features/vk-auto-queue/README.md); the
Telegram producer/consumer contract is
[`../features/telegram-monitoring/README.md`](../features/telegram-monitoring/README.md).

The MVP includes moderator registration, timezone setting and simple event
creation (`/addevent` and `/addevent_raw`). For each event the bot creates a
Telegraph page containing the original announcement text. When the event comes
from a registered announcement channel the title on that page links to the
source post. `/events` shows upcoming events by day (with links to these pages)
and allows deletion or editing through inline buttons. A
helper `python main.py test_telegraph` checks Telegraph access and creates a
Telegraph token automatically if needed.

Each event stores optional ticket information (`ticket_price_min`, `ticket_price_max`, `ticket_link`) together with the cached vk.cc short link (`vk_ticket_short_url`) and the associated stats key (`vk_ticket_short_key`). If the event was forwarded from a channel, the link to that post is saved in `source_post_url`.
Free events are marked with `is_free`. Telegraph pages are stored with both URL and path so they can be updated when the event description changes. If a message includes images (under 5&nbsp;MB each), they are uploaded to Catbox and embedded at the start of the source page. Under the cover the bot renders a «Быстрые факты» block summarizing key fields: the event date and time (or the closing date for ongoing exhibitions), the normalized location, ticket information with a registration or ticket link when available, and a separate `✅ Пушкинская карта` line when `pushkin_card=true`. Each line is omitted when the underlying data is missing so moderators can see which facts are optional.
Month pages list upcoming events. When their content exceeds about 64&nbsp;kB the bot creates a second page and links to it from the first.
Events also keep `event_type` (one of eight categories: спектакль, выставка, концерт, ярмарка, лекция, встреча, мастер-класс, кинопоказ) and an `emoji` suggested by the LLM. Multi-day events store `end_date` and appear with "Открытие" or "Закрытие" on the respective days. `/exhibitions` lists active exhibitions.
`pushkin_card` marks events that accept the Пушкинская карта.
`ics_url` stores a link to a calendar file uploaded to Supabase. Moderators can generate or remove this file when editing an event. Calendar files are named `event-<id>-YYYY-MM-DD.ics` and include a link back to the event page.
When present the link is inserted into the Telegraph source page below the title image so readers can quickly add the event to their phone calendar.
If a text describes several events at once the LLM returns an array of event objects and the bot creates separate entries and Telegraph pages for each of them.
Channels where the bot is admin are tracked in the `channel` table. Use `/setchannel` to choose an admin channel and mark it as an announcement source. The `/channels` command lists all admin channels and shows which ones are registered.
- `../reference/locations.md` – list of standard venues used when parsing events;
  the reference context is appended to the configured structured model request
  so events use consistent `location_name` values.

## Poster OCR pipeline

- Poster media is uploaded to Catbox once per unique image; the resulting bytes feed both the Telegraph page and the OCR stage.
- `poster_ocr.recognize_posters` caches results in `PosterOcrCache` by hash, detail level and model so retries reuse the stored text and token counts.
- Daily usage is tracked in the `OcrUsage` table and compared against the 10 000 000-token budget. Cached entries keep working, while new, uncached OCR requests are blocked until the quota resets.
- Recognized text is saved in `EventPoster` rows and injected into the downstream
  typed LLM pipeline together with the operator draft.

## Video Announce pipeline

The video announce feature generates promotional video clips with event highlights:

- **Session management** — `VideoAnnounceSession` and `VideoAnnounceItem` track selection state, rendering status and publication history.
- **Candidate selection** — `selection.py` ranks events by topic relevance, date proximity and manual boost (`🎬` counter). LLM generates the intro text.
- **Pattern preview** — `pattern_preview.py` renders client-side PNG previews of three intro patterns (`STICKER`, `RISING`, `COMPACT`) without Kaggle.
- **Payload generation** — `payload_as_json()` produces the JSON for the Kaggle kernel, including `cities`, `date`, `pattern`, and scene data.
- **Kaggle rendering** — the kernel (`kaggle/VideoAfisha/video_afisha.ipynb`) downloads assets, renders frames with MoviePy, and uploads the final video.
- **Publication** — once complete, the video is sent to the test or main channel; events are marked as published and their counters decremented.
