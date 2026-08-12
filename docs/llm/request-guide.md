# 4o Request Guide

This document describes how the bot communicates with model **4o**.

## LLM-first policy (applies to all LLM providers)

For configured-source ingestion, every fetched in-horizon carrier revision must
be an exact successful replay, receive a complete-evidence typed semantic LLM
decision, or remain durably retryable. Keyword/date/history/past/too-far/
cancellation/giveaway/title/venue regexes are hints or contradiction triggers
only. They cannot produce `rejected`, `failed`, `skipped`, `silent`, delete an
LLM child, or confirm no-event. `CONFIRMED_NO_EVENT` requires a completed, valid
structured response and complete `EvidenceManifest`. Cost, latency and TPM are
handled by durable admission/backpressure and never by removing source evidence.
Normal path is one primary parse; a second call is conditional on a closed
contradiction reason and reuses an existing configured model.

## Google AI request admission (all agents and runtimes)

Every API-key-authenticated Gemini, Gemma or Antigravity provider attempt must
use the dedicated shared ledger and the versioned
`google_ai_project_model_atomic_v1` contract. The required order is
`reserve → mark_sent → provider → finalize`; a reservation without the exact
contract and non-empty `quota_scope` must fail before the secret is read.

This applies equally to Codex/local diagnostics, Fly, Kaggle and Supabase Edge
Functions. Raw `urllib`/`requests`, direct provider SDK clients and manual
dangerous overrides are prohibited. Unavailable limiter configuration is an
observable fail-closed result, never permission to send through a process-local
counter. Before release run
`python3 scripts/inspect/audit_google_ai_provider_paths.py` and require both
`allowlisted_debt=0` and `unapproved=0`.

Provider quotas are scoped by Google Cloud project/model. Different local key
aliases are not assumed to be independent: unmapped keys share one conservative
`quota_scope` until an operator-verified key→project inventory permits a split.

Prompts and few-shot material must be domain-generic. Do not embed names,
franchises, organizers or narrative facts from a production incident as a
positive example in a reusable prompt: a later fallback can copy them into an
unrelated event. Validate generated public fields against the current
source/OCR bundle in a separate, small LLM contract and fail closed when the
evidence is not verbatim/grounded.

If a change affects the *meaning* or perceived quality of event data, prefer
doing it **inside the LLM** (prompt rules in `docs/llm/prompts.md`, provider
prompts such as `kaggle/TelegramMonitor/telegram_monitor.py`, or Smart Update
LLM passes). This includes not only text fields (`title`, `description`,
`search_digest`), but also semantic extraction/classification choices such as
`is_free`, ticket availability/status, work-hours-vs-event decisions, venue/title
meaning, and duplicate/match judgments.

Deterministic code is allowed as *supporting plumbing*:

- sanitizers / escaping (HTML/Markdown safety, whitespace, URL cleanup);
- canonicalization / normalization (venues, dates, phone masking);
- narrow output-consistency guardrails (region filters, safety checks, JSON
  cleanup, contradiction checks that do not decide event meaning on their own);
- hints passed *to the LLM input* to steer it (without rewriting the resulting text).

For timetable-like social posts, structural date/time counting may only route a
small LLM screen. The semantic stage must assign the role of the date/time
evidence (for example `occurrence`, `work_hours`, `ticket_valid_until`,
`deadline`, `historical`) before extraction. A ticket-validity date or
visitor/cash-desk hours must never be promoted to event logistics by a regex.

For vector-first quality workflows, embeddings provide recall only: nearest
events and incident prototypes select context for the LLM verifier, but vector
similarity is not source evidence and cannot approve, merge, repair or publish a
row. Missing vector/LLM/source coverage is `indeterminate` and fails closed.

Event age rating follows the same boundary with an extra quota invariant:
source-native declared values need no LLM, while text/OCR `age_decision` may be
returned only inside an already-required Smart Update JSON request
(`SMART_UPDATE_EVENT_AGE_LLM_MODE=piggyback_only`). The feature must not open a
separate per-event request. CPU BGE retrieval cannot become a rating by nearest
prototype; an assessed candidate needs the approved calibrated dual-head gate
defined in `docs/features/event-age-rating/README.md`.

Deterministic code must not replace an LLM-owned semantic decision with broad
keyword logic. For example, do not decide that an event is free merely because no
price was found, do not convert library/museum date lists into work-hours skips
without prompt-owned classification, and do not merge/split events by a title
regex when the user-visible meaning is ambiguous.

Guide-specific hard rule:
- for `guide excursions monitoring`, semantic decisions in `trail_scout.screen.*`,
  `trail_scout.*extract*`, and adjacent stage routing must stay LLM-owned;
  regex/keyword shortcuts are not an acceptable replacement for post meaning
  classification, region-fit judgment, or announce-vs-reportage decisions.

Avoid deterministic “editorial” rewrites (e.g. renaming, adding semantic prefixes,
rewriting sentences) unless it’s a narrowly scoped safety invariant and it’s
explicitly documented as an exception.

Separate high-volume event processing from final public-copy generation. Model
routing may optimize extraction, matching, grounding and other internal
processing contracts independently, but must not silently change the approved
writer for a public surface. In particular, the `tg_event_publish` intro uses
Gemini Lite first and must not inherit a Gemma fallback chain. If Lite is
unavailable or its answer fails validation, the only permitted writer fallback
is strict `gpt-4o` behind a persisted hard budget of at most 100 requests per
UTC day. Deterministic narrative construction and cross-model fallbacks to
Gemma/`gpt-4o-mini` are forbidden; when both approved writers are unavailable,
publication fails closed.

Для внутренних facts/grounding стадий Smart Update действует отдельная узкая
политика доступности: primary `gemini-3.1-flash-lite`, затем
`gemini-3.5-flash-lite`, затем `gemma-4-31b-it`. Она передаётся per-call только
для facts-labels (включая `create_bundle_grounding`) и не расширяет writer,
публикационные или иные LLM-пути. Каждый переход заново проходит shared
reserve; provider `429` сначала закрывает весь фактический
`quota_scope/model`, поэтому другой key того же Cloud project не получает
повторный send.

Documented deterministic support is limited to syntax/transport validation,
reference normalization, exact replay identity, shortlist recall and objective
schema contradictions. Any ambiguous semantic consequence is decided by the LLM
or becomes retry. In particular, deterministic code must not collapse
multi-session children, convert a positive source child to product exclusion,
or suppress it from public fanout.

Requests are sent as HTTP `POST` to the URL stored in the environment variable
`FOUR_O_URL` (defaults to `https://api.openai.com/v1/chat/completions`). The
header `Authorization: Bearer <FOUR_O_TOKEN>` is added. Set these values via Fly
secrets.

Payload:
```json
{
  "model": "gpt-4o",
  "messages": [
    {"role": "system", "content": "<contents of PROMPTS.md>"},
    {"role": "user", "content": "Today is YYYY-MM-DD. <original event text>"}
  ]
}
```

When a post is forwarded from a channel or imported from a VK group, its title
is appended to the user message on a new line. This helps the model infer the
venue when it is omitted in the text.

If `../reference/locations.md` exists, its lines are appended to the system prompt as a
list of known venues. This helps the model normalise `location_name` to a
standard form.

If `../reference/holidays.md` exists, a "Known holidays" list is appended with the
canonical names of seasonal festivals, their alias hints and short
descriptions. The model should rely on these entries when filling the
`festival` field so that holiday-related events converge on the same canonical
records even when the source text uses synonyms.

When the database stores festival metadata, the system prompt receives an extra
JSON payload with canonical `festival_names` and normalised
`festival_alias_pairs`. Each pair is `[alias_norm, festival_index]` where
`alias_norm` is computed with the same rules as `norm(text)` (casefold, trim,
strip quotes, remove the leading words «фестиваль»/«международный»/«областной»/
«городской», collapse whitespace). These pairs let the parser map alternative
spellings to the correct festival so new events attach to existing records
instead of creating duplicates.

The response must be JSON with the fields listed in `prompts.md`. When the
text describes multiple events, return an array of such objects. Theatre
announcements that share one date but list several start times (формулировки
вроде «начало в 12:00 и 17:00») must therefore yield several objects that reuse
the same date and differ only in `time` and other time-specific fields.
The prefix "Today is YYYY-MM-DD." helps the model infer the correct year for
dates that omit it and lets the model ignore any events scheduled before today.
When a post is forwarded from a Telegram channel or imported from a VK group,
the channel or group title is added before the announcement text as
`Channel: <name>.` so the model can guess the venue.
Edit this file or `prompts.md` to fine‑tune the request details.

The command `/ask4o <text>` sends an arbitrary user message to the same
endpoint and returns the assistant reply. It is intended for quick diagnostics
and available only to the superadmin.

## Digest intro example

To compose the introductory phrase for the lecture digest the bot sends a
single-message chat completion describing the number of lectures, the horizon
(``недели``/``двух недель``) and up to three topic hints. The response is plain
text with 1–2 short sentences:

```
POST $FOUR_O_URL
{ "model": "gpt-4o", "messages": [{"role": "user", "content": "..."}] }
```

Response:

```
{"choices": [{"message": {"content": "Подобрали для вас ..."}}]}
```

When a new event might duplicate an existing one (same date/time/city but
slightly different title or venue), the bot sends both versions to 4o asking if
they describe the same event. The model replies with JSON
`{"duplicate": true|false, "title": "", "short_description": ""}`. If
`duplicate` is true the returned title and description replace the stored event
fields.

Festival pages also rely on 4o. To craft a festival blurb the bot sends the
previous description (if any) together with the full text of up to five recent
announcements and a fact sheet summarising период, город, длительность, число
событий, ключевые сюжеты и площадки. The prompt asks the model to write like a
culture journalist, stick strictly to the supplied facts, and return один абзац
без списков, эмодзи и выдуманных подробностей. The final text is capped at 350
characters, so the LLM keeps only the essentials and avoids clichés. Only
information lifted from the provided materials may appear in the summary.
The model also returns `festival_full` alongside `festival` so the bot can store
the edition name separately.
If the description contains a date range like "с 27 августа по 6 сентября 2025",
these dates define the festival period. When no range is present the period is
calculated from the events added to the festival.

## Logging

OpenAI usage resets daily at 00:00 UTC. The `four_o.usage` log records each
request with its token count and the remaining budget as defined by
`FOUR_O_DAILY_TOKEN_LIMIT` (1,000,000 tokens by default). Grafana dashboards can
filter by the `four_o.usage` key to visualise daily token spend.

Direct `gpt-4o` calls also have a model-specific hard guard:
`FOUR_O_GPT4O_DAILY_TOKEN_LIMIT` defaults to `950000`. Before sending a
`gpt-4o` request, the bot reads today's persisted `token_usage` total for
`gpt-4o`/`gpt-4o-2024-08-06` and adds a conservative estimate for the next
request. If that would exceed the cap, the request is sent to
`FOUR_O_GPT4O_FALLBACK_MODEL` (`gpt-4o-mini` by default) and logs
`four_o.budget_fallback`. The fallback model is not counted against this
model-specific `gpt-4o` cap.
