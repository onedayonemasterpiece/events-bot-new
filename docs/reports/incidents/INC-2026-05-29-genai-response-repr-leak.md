# INC-2026-05-29-genai-response-repr-leak Raw provider SDK response leaked into public posts

Status: mitigated
Severity: sev2
Service: Smart Update description generation / public Telegraph + VK `klgdevents` event posts
Opened: 2026-05-29
Closed: —
Owners: Codex / events-bot maintainers
Related incidents: `INC-2026-05-17-future-event-quality-regressions`
Related docs: `docs/features/smart-event-update/README.md`, `docs/llm/request-guide.md`, `google_ai/client.py`, `media_dedup.py`

## Summary

The public VK post `vk.com/wall-231920894_1613` ("Спорт — Дворец спорта «Янтарный»", 2026-05-30) was published with its body set to the **stringified `repr` of a google-genai `GenerateContentResponse` object** instead of a description — including the model's chain-of-thought, token counts and HTTP-response wrapper:

```
sdk_http_response=HttpResponse(headers=) candidates=[Candidate(content=Content(parts=[Part(
text="""* Task: Edit/Rewrite a Markdown event announcement. ... 1. Festival""", thought=True )],
role='model'), finish_reason=, index=0 )] model_version='gemma-4-31b-it' ...
usage_metadata=GenerateContentResponseUsageMetadata(prompt_token_count=2562, thoughts_token_count=1897 ...) parsed=None
```

A production DB scan found **16 active future events** with the same dump in `event.description`, with Telegraph slugs spanning 2026-05-07 → 2026-05-28 — i.e. the regression had been publishing garbage for ~3 weeks. The same post also carried two near-duplicate poster images (a separate, long-standing gap surfaced by the same report).

## User / Business Impact

- Public VK `klgdevents` posts and Telegraph pages showed an internal SDK object dump (model thoughts, token counts, http headers) instead of an event description — directly user-visible nonsense on the public afisha.
- 16 active future events affected; several dated 2026-05-29/30 (live at detection).
- Erodes trust in `/daily`, Telegraph, and VK output.

## Detection

- Operator reported the bad post (`wall-231920894_1613`) and two near-identical images on 2026-05-29.
- Confirmed by read-only `wall.getById` executed **from the prod machine** (the VK user tokens are IP-locked) and a `event.description` signature scan of `/data/db.sqlite`.
- Observability gap: the failure was **silent** — `ProviderError`/empty-response markers were absent from 24h of runtime logs while `gemma-4-31b` served 3244 calls/day.

## Timeline

- 2026-05-07 … 2026-05-28 — 16 future events created/updated with the SDK-repr dump as `description` (Telegraph slug dates).
- 2026-05-29 — operator report; prod `wall.getById` + DB scan confirm 16 affected events; this record opened; hotfix prepared on `hotfix/2026-05-29-vk-genai-repr-leak` off `origin/main`.

## Root Cause

1. **Primary (text leak).** `google_ai/client.py` `_call_provider._extract_text` had a `return str(resp).strip()` "last resort" branch. When the model returned **only a thought-channel part and no answer part** (Gemma 4 spent its whole `max_output_tokens` budget "thinking" — `thoughts_token_count=1897`, truncated mid-sentence at "1. Festival"), the part-based extractor correctly skipped the thought part, `resp.text` was empty, and the last-resort `str(resp)` dumped the entire response object as if it were model output.
2. **Silent + fallback-defeating.** The non-empty `str(resp)` string defeated the existing `if not response_text: raise ProviderError("empty_response")` guard immediately below it, and it short-circuited the existing **Gemma→GPT-4o fallback** in `_ask_gemma_text_unbounded` (which only triggers when Gemma yields empty/raises). So a recoverable case produced published garbage with no error logged.
3. **Image near-duplicates (contributing, separate).** Live poster dedup was exact-match only: `sha256` of bytes + exact URL string. The perceptual hash (`phash`/dh16) was computed only as a storage object-key (exact match) and was `None` on the VK intake path; no Hamming-distance near-duplicate comparison existed in the live Smart Update path (only offline in `scripts/afishathumb/prepare_slot.py` and `scripts/inspect/audit_media_dedup.py`). Round-1 only deduped the current `candidate.posters` batch, but `_apply_posters` still appended the selected managed-storage poster to existing `Event.photo_urls` without comparing it to legacy site/CDN thumbnails already persisted on the event. This left pairs such as qTickets CDN thumb + managed-storage dh16 poster in the DB; `sync_vk_source_post` then uploaded both as distinct VK attachments.
4. **Generic title recovery too strict (contributing, separate).** The first title-recovery prompt asked only for a formal "own title as the organizer named it" and required `НЕТ` if that exact kind of name was absent. That is correct for anti-hallucination, but too narrow for афиша sources where the grounded attendee-facing title is a festival+performer, central artwork/programme, holiday/theme, or event concept rather than an official branded name. Examples left behind after the first repair pass: `Pianissimo: Илья Папоян`, `Розовый натюрморт`, `День защиты детей в Юности`.
5. **VK title-only edits were skipped (contributing, separate).** `job_sync_vk_source_post` used `content_hash(description/source_text)` as its idempotency guard, while the actual VK wall text also contains `event.title`, date and place metadata. A title-only repair could update DB/Telegraph but skip `wall.edit` for an existing managed `klgdevents` post because the body hash had not changed.

## Contributing Factors

- Extraction "be lenient, never return empty" instinct (`str(resp)`) is actively dangerous for objects whose `repr` is plausible-looking text.
- No alert/metric on empty/thought-only provider responses.
- `phash` existed in the schema and offline tooling but was never wired into the live publish path.

## Fix

- **Core:** `_extract_text` no longer stringifies the response; it returns `""` so the `empty_response` `ProviderError` fires and the existing retry/4o fallback runs. The empty case now also logs `google_ai.empty_response` with `finish_reasons` / `thought_only` / `thoughts_token_count` (closes the observability gap).
- **Defense-in-depth:** `markup.looks_like_genai_response_dump` detector (≥2 SDK-internal markers); `_sanitize_description_output` drops a dump outright (covers Telegraph/daily/VK), `sanitize_for_vk` returns `""` on a dump, and `job_sync_vk_source_post` falls back to `source_text` if `event.description` looks like a dump.
- **Images:** real `phash` (dh16, shared with the storage key) is now threaded into poster candidates incl. the VK path; `_apply_posters` collapses near-duplicate posters by Hamming distance (`SMART_UPDATE_POSTER_NEAR_DUP_HAMMING`, default 20 for the 256-bit hash) keeping the highest-quality survivor, then normalizes the full persisted `Event.photo_urls` set by perceptual hash so legacy CDN thumbnails are replaced by the preferred managed/poster URL instead of surviving into VK attachments. `sync_vk_source_post` also has a final near-duplicate guard before upload.
- **Titles:** generic `"<event_type> — <venue>"` placeholders still run the strict formal-name recovery first. If that returns no usable title, Smart Update now runs a second LLM-first public-heading recovery pass that may build a short title from grounded source/OCR/fact material (festival+performer, central artwork/programme, holiday/theme). Acceptance was tightened so every meaningful token in the recovered title must be grounded in the same corpus, generic venue placeholders are still rejected, and the title-recovery labels route through the stable facts-stage model instead of repeatedly burning Gemma retries on thought-only truncation.
- **VK title-only sync:** the managed VK post idempotency hash now includes `event.title` plus body text, so title repairs re-edit existing `klgdevents` posts instead of being skipped.
- **VK formatting/post ids:** `sanitize_for_vk` now breaks inline Markdown `###` headings into VK-friendly plain heading blocks before `wall.post`/`wall.edit`; `post_to_vk` resolves postponed posts through the user actor (group actor cannot call `wall.get`), and `sync_vk_source_post` lazy-resolves stale stored `postponed_id` URLs to the real wall id if `wall.getById` for the stored id is empty.
- **Cleanup:** the 16 affected events are regenerated with `/rebuild_event <id> --regen-desc` (fact-first description rebuild via the fixed pipeline) and re-rendered to Telegraph + re-synced to VK.

## Automation Contract

### Treat as regression guard when

- Editing `google_ai/client.py` response/text extraction or the empty-response guard.
- Editing Smart Update description generation, `_sanitize_description_output`, `sanitize_for_vk`, or the VK source-post publish boundary.
- Editing poster dedup / `media_dedup` / `_apply_posters`.
- Editing generic-title recovery or title grounding acceptance.
- Editing `vk_source_hash` / VK event-post idempotency.

### Affected surfaces

- code: `google_ai/client.py`, `markup.py`, `smart_event_update.py`, `main.py` (`job_sync_vk_source_post`), `poster_media.py`, `vk_intake.py`, `media_dedup.py`.
- env/config: `SMART_UPDATE_POSTER_NEAR_DUP_HAMMING`.
- release: Fly app `events-bot-new-wngqia` via `flyctl deploy`.
- external: VK `klgdevents` group, Telegraph.

### Mandatory checks before closure or deploy

- `tests/test_google_ai_client.py::test_thought_only_response_raises_instead_of_leaking_sdk_repr`
- `tests/test_genai_dump_and_poster_dedup.py`
- `tests/test_vk_source.py::test_sync_vk_source_post_dedupes_near_duplicate_photos`
- No `event.description` on prod matches the SDK-repr signature after cleanup.
- Reported VK posts / postponed posts (`wall-231920894_1733`, `wall-231920894_1710`, `wall-231920894_1715`) no longer have duplicate image attachments after data repair/resync, or are explicitly documented if VK edit windows block historical repair.
- Active future events should not remain with generic `"<event_type> — <venue>"` titles when source/OCR/facts contain grounded attendee-facing title material; if recovery returns `НЕТ`, the source must be manually reviewed and documented.

## Follow-ups

- Consider disabling thinking / raising the description-stage output budget for Gemma so thought-only truncation does not even reach the fallback.
- Backfill `EventPoster.phash` for existing VK-path rows so near-dup dedup and the offline audit cover historical posters.
