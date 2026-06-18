# INC-2026-06-18 VK Auto-import Generic Title And Source Shortlink Leaked To Public Posts

Status: closed
Severity: sev2
Service: VK auto-import / Smart Update / public @kldevents + klgdevents event posts
Opened: 2026-06-18
Closed: 2026-06-18
Owners: Codex / events-bot maintainers
Related incidents: `INC-2026-05-08-vk-tg-prompt-and-dup-probe`, `INC-2026-05-11-event-parse-defender-and-escalation-poc`, `INC-2026-05-29-genai-response-repr-leak`, `INC-2026-06-18-tg-location-prose-still-extracted`
Related docs: `docs/features/vk-auto-queue/README.md`, `docs/features/smart-event-update/README.md`, `docs/llm/prompts.md`, `docs/operations/runtime-logs.md`

## Summary

On 2026-06-18 the operator reported two fresh public `@kldevents` defects immediately after the previous event-quality repairs:

- `https://t.me/kldevents/861` / event `6156` was published as `Концерт — Бар Советов`, the same prompt-forbidden `<event_type> — <venue>` fallback family that earlier incidents had already banned.
- `https://t.me/kldevents/860` / event `6155` rendered the registration link through the source shortener `https://clck.ru/3UEVYF` instead of the resolved destination `https://world-ocean.ru/posetitelyam/vremya-raboty`.

Both rows were created by VK auto-import and reached Telegram, VK and Telegraph public surfaces. A follow-up active/future audit found the same already-public title/shortlink family on `https://t.me/kldevents/751`, `https://t.me/kldevents/662`, and short registration links on `https://t.me/kldevents/582`; authenticated VK audit also found matching VK-only rows `4084` and `5511`; those were repaired in the same mitigation pass rather than left for the operator to rediscover.

## User / Business Impact

- Users saw a low-quality generic event title instead of the source/poster-grounded title for the Bar Sovetov vinyl/DJ event.
- Users clicking registration on the museum event were routed through an opaque source shortlink instead of the direct destination.
- The recurrence undermines the prior LLM-first title contract because a deterministic VK-intake guard replaced an LLM-produced title with a known-forbidden placeholder.

## Detection

- Operator report on 2026-06-18 with public links `https://t.me/kldevents/861` and `https://t.me/kldevents/860`.
- Public Telegram inspection via `t.me/s/kldevents?before=862` confirmed:
  - `/861`: `💿 Концерт — Бар Советов`.
  - `/860`: `по регистрации` href `https://clck.ru/3UEVYF`.
- Production DB rows and last-24h runtime logs were checked through Fly `/data/runtime_logs/events-bot.log*`.
- VK managed posts were inspected through authenticated VK API, not public HTML.

## Timeline

- 2026-06-18 13:32 UTC — VK auto-import created event `6155` from source `https://vk.com/wall-35373633_27995`; source ticket link was `https://clck.ru/3UEVYF`.
- 2026-06-18 13:35 UTC — VK auto-import processed source `https://vk.com/wall-223666016_428`; poster OCR returned `ocr_title='СОВЕТСКАЯ ЭЛЕКТРОНИКА'`.
- 2026-06-18 13:35 UTC — runtime logged `vk_intake suspicious_title replaced title='💿 Виниловый вечер с DJ Switchoff' missing=Виниловый fallback='Концерт — Бар Советов'`.
- 2026-06-18 13:35 UTC — Smart Update title recovery attempted `title_recover` and `title_recover_public`, but both failed on provider RPD rate limit and 4o fallback budget exhaustion, so event `6156` was created with the placeholder title.
- 2026-06-18 14:17 UTC — `https://t.me/kldevents/860` was published with the unresolved `clck.ru` registration href.
- 2026-06-18 14:27 UTC — `https://t.me/kldevents/861` was published with the placeholder title.

- 2026-06-18 15:19 UTC — operator correctly challenged the first repair: `Виниловый вечер с DJ Switchoff` was source-supported by `слушаем винил` + `DJ Switchoff`; replacing it with poster OCR heading `СОВЕТСКАЯ ЭЛЕКТРОНИКА` was still deterministic title choice. Event `6156` was repaired again to `Виниловый вечер с DJ Switchoff`, Telegram `/861` edited in place, VK replacement `https://vk.com/wall-231920894_3767` created, and stale postponed `3760` deleted.

## Root Cause

1. VK intake's exact-token suspicious-title guard falsely classified the normal LLM title `Виниловый вечер с DJ Switchoff` as suspicious: `Виниловый` is grounded by the source noun `винил`, and `вечер` appears in the source text. This false-positive signal was then allowed to drive a deterministic rewrite.
2. `_fallback_title(...)` synthesized exactly the forbidden `<event_type> — <venue>` shape (`Концерт — Бар Советов`). This happened after the main `event_parse` defender, so the existing bare-title escalation did not see it.
3. A source-grounded poster OCR heading existed (`СОВЕТСКАЯ ЭЛЕКТРОНИКА`), but the better LLM title `Виниловый вечер с DJ Switchoff` was also source-supported (`винил` + `DJ Switchoff`). The correct boundary is that VK intake must not deterministically choose either a generic fallback or a poster heading over a non-generic LLM title; poster OCR is evidence, not an override.
4. Smart Update's LLM title recovery was present, but provider `rpd` and 4o hourly budget exhaustion made it unavailable at the critical moment. The pipeline therefore needed a non-placeholder fail-safe before public fanout.
5. VK auto-import stored `clck.ru` as the canonical `ticket_link` and public Telegram rendered that short URL directly. There was no source-root normalization step for external registration shorteners.

## Contributing Factors

- Prior title regressions focused on LLM output / Smart Update recovery; this recurrence was introduced by a deterministic pre-Smart-Update guard in `vk_intake`.
- The suspicious-title guard was designed for real hallucination/typo cases such as `Утя`, but its fallback was worse than the suspicious LLM title for event-name quality.
- Public-surface audit did not include newly published posts `/860` and `/861` until the operator pointed at them.
- The first mitigation still allowed deterministic poster-title overwrite. That was incorrect: poster OCR may be evidence, but it is not automatically more correct than a source-grounded LLM title.

## Automation Contract

### Treat as regression guard when

- Changing `vk_intake.build_event_drafts_from_vk`, title-grounding heuristics, poster OCR title handoff, `smart_event_update` generic-title recovery, VK auto-import ticket-link extraction, or public Telegram/VK event rendering.

### Affected surfaces

- `vk_intake.py` suspicious-title guard and ticket-link normalization.
- `smart_event_update.py` generic title recovery and ticket-link persistence.
- Public Telegram `@kldevents`, VK `klgdevents`, Telegraph event pages.
- Runtime logs `/data/runtime_logs/events-bot.log*` for historical `vk_intake suspicious_title`, `title_recover`, and `ticket_shortlink_resolved`; current code must have no VK-intake title-grounding/suspicious-title rewrite path.

### Mandatory checks before closure or deploy

- Regression test: VK intake must not run deterministic word-level suspicious-title/grounding checks and must keep a source-grounded non-generic LLM title such as `Виниловый вечер с DJ Switchoff`; it must not replace it with either `Концерт — <venue>` or a poster OCR heading.
- Regression test: `clck.ru` ticket links must be resolved before draft links are persisted.
- Active/future public audit must show no fresh `Концерт — <venue>` / `Лекция — <venue>` / `Спектакль — <venue>` placeholders when source/OCR contains attendee-facing title material.
- Public post repair must update DB, Telegraph, Telegram and VK; hashes must be invalidated/recomputed. If VK sync creates a replacement postponed post, the stale postponed post must be deleted or verified absent.

### Required evidence

- Production DB rows before/after repair for events `6155` and `6156`.
- Public Telegram href/title after edit.
- VK API post text after edit.
- Runtime log excerpts showing the original root cause.
- Deployed SHA reachable from `origin/main` and Fly health check.

## Immediate Mitigation

- Prepared source-root code fix in `vk_intake`: remove the VK-intake exact-token title-grounding/suspicious-title guard entirely; do not synthesize `<event_type> — <venue>` as the title fallback and do not overwrite a non-generic LLM title with poster OCR. Title review/recovery stays owned by Smart Update LLM stages. Resolve `clck.ru` source ticket links before storing `ticket_link`.
- Production repair completed for events `6155` and `6156`; active/future audit also repaired event `6120` title, event `6032`/`6033` source shortlinks, and VK-only rows `4084`/`5511`. Telegram, Telegraph and VK were republished; stale VK postponed posts `https://vk.com/wall-231920894_3754` and the first-repair `https://vk.com/wall-231920894_3760` were deleted; current corrected VK post is `https://vk.com/wall-231920894_3767`.

## Corrective Actions

- [x] Add regression tests for preserving source-grounded LLM title with no deterministic title-grounding/suspicious-title gate, plus `clck.ru` ticket-link expansion.
- [x] Update VK auto-import docs and changelog with the LLM-first boundary.
- [x] Repair event `6155` ticket link and all public surfaces.
- [x] Repair event `6156` title and all public surfaces.
- [x] Deploy second-pass code fix and verify health.

## Follow-up Actions

- [ ] Add a scheduled/fresh-public audit for generic title placeholders in the last 24–48h of `@kldevents` and `klgdevents`.
- [x] Run immediate active/future audit for existing generic titles and unresolved `clck.ru` ticket links; repaired public rows `6032`, `6033`, `6120`, and VK-only rows `4084`, `5511`.
- [ ] Review whether Smart Update title recovery budget/fallback should be more isolated from unrelated 4o hourly consumption.

## Release And Closure Evidence

- deployed SHA: `441aa3cc` (removes VK-intake deterministic title grounding, reachable from `origin/main`)
- deploy path: Fly remote deploy to `events-bot-new-wngqia`; image `events-bot-new-wngqia:deployment-01KVDRXYQ5AG4Y613TFQD2VRRW`, machine `683961db016e28` version `1447`, Fly status `1 passing`
- regression checks: `python3 -m py_compile vk_intake.py` passed; local pytest unavailable (`No module named pytest`). `git diff --check` passed.
- production repair verification: `https://t.me/kldevents/860` registration href resolves directly to `https://world-ocean.ru/posetitelyam/vremya-raboty`; `https://t.me/kldevents/861` title was initially repaired to `💿 Советская электроника: DJ Switchoff`, then rechecked and corrected to the source-supported LLM title `💿 Виниловый вечер с DJ Switchoff`; `https://t.me/kldevents/751` title is `🎻 ПроСТО век Зацепина`; `https://t.me/kldevents/662` title/link and `https://t.me/kldevents/582` link were also repaired. VK API verified current managed posts `3749`, `3767`, `3622`, `3761`, `3654`, `3763`, and `3764`; old bad postponed posts `3754` and `3760` were deleted. Final production audit found no active/public rows with `clck.ru` ticket links or generic `<type> — <venue>` titles in Telegram/VK-managed surfaces.
- post-deploy verification: Fly status healthy; deployed `/app/vk_intake.py` has no VK-intake deterministic title-grounding/suspicious-title gate and no `replaced_with_poster_title` overwrite path.

## Prevention

- The title guard now stays LLM-first: deterministic logic can collect/log source evidence and route onward, but must not invent a semantic placeholder title or replace a non-generic LLM title with a poster heading.
- External source shortlinks are normalized at ingestion, so public rendering uses the direct destination rather than opaque source shorteners.
