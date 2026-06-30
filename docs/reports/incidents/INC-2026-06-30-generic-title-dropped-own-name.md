# INC-2026-06-30 Generic Category Title Dropped Event Own Name

Status: open
Severity: sev2
Service: Telegram Monitoring / Smart Update / public @kldevents + klgdevents + Telegraph event posts
Opened: 2026-06-30
Closed: —
Owners: Codex / events-bot maintainers
Related incidents: `INC-2026-06-18-vk-title-shortlink-public-regression`, `INC-2026-05-11-bar-bastion-stochastic-title-fallback-and-semantic-dup`, `INC-2026-06-24-future-event-date-default-venue-regressions`, `INC-2026-06-29-qtickets-structured-facts-lost`
Related docs: `docs/features/smart-event-update/README.md`, `docs/llm/request-guide.md`, `docs/operations/incident-management.md`

## Summary

On 2026-06-30 the operator reported public Telegram post `https://t.me/kldevents/1630` for event `6508`: the title was `Городской фестиваль`, while the actual source headline and poster OCR contained the attendee-facing name `Городской фестиваль «ВЕЛОДЕНЬ»` / `ВЕЛОДЕНЬ`.

The same generic title propagated to the production DB, Telegraph page `https://telegra.ph/Gorodskoj-festival-06-29`, and managed VK URL stored as `https://vk.com/wall-231920894_5115`.

## User / Business Impact

- Public event cards did not name the actual event, reducing recognizability and trust.
- Telegraph body was effectively title-only/generic despite a rich source post.
- The incident repeats the title-quality class where a pipeline stage can preserve or publish a weaker title after source/LLM evidence contains a better grounded name.

## Detection

- Operator report with public link `https://t.me/kldevents/1630`.
- Telethon inspection confirmed the public post title line was `Городской фестиваль`, while the body/source contained `«ВЕЛОДЕНЬ»`.
- Production DB inspection confirmed event `6508` had `event.title='Городской фестиваль'`, generic `description`/`short_description`, source text with `Городской фестиваль «ВЕЛОДЕНЬ»`, and poster OCR title `ВЕЛОДЕНЬ / 12 ИЮЛЯ`.
- Runtime logs showed Smart Update started with `title=Городской фестиваль`; no LLM title recovery ran because the existing recovery detector only covered `<event_type> — <venue>` placeholders.

## Timeline

- 2026-06-29 23:01 UTC — Telegram source `https://t.me/kulturnaya_chaika/7913` was imported; Smart Update log shows `title=Городской фестиваль` before creation.
- 2026-06-29 23:01 UTC — event `6508` and Telegraph page `https://telegra.ph/Gorodskoj-festival-06-29` were created with the generic title/body.
- 2026-06-30 05:00 UTC — public `@kldevents/1630` was published with the generic title.
- 2026-06-30 — operator reported the public title defect; incident opened and replay fixture added.

## Root Cause

1. Upstream Telegram extraction passed a category title (`Городской фестиваль`) even though the source headline had the own name `«ВЕЛОДЕНЬ»` and poster OCR had `ВЕЛОДЕНЬ / 12 ИЮЛЯ`.
2. Smart Update had an LLM-first title recovery path, but it was routed only for explicit `<event_type> — <venue>` placeholders. A generic category title with a grounded quoted/poster own-name was considered acceptable because token grounding found `городской` in the source.
3. The create-path title guard therefore blocked neither the weak title nor the title-only description fallback; the weak title reached DB and every public fanout surface.

## Contributing Factors

- Prior title incidents focused on forbidden `type — venue` placeholders and deterministic VK intake rewrites; this shape was a more subtle “category + generic qualifier” title.
- `_meaningful_title_tokens()` treated `городской` as a meaningful grounded token, so the weak-title override guard did not recognize that no distinctive event name remained.
- Public-fresh audit did not flag a rich source whose final Telegraph/body stayed title-only.

## Automation Contract

### Treat as regression guard when

- Changing Smart Update create title handling, `_is_candidate_title_weak_for_llm_override`, title recovery prompts, Telegram/VK intake title handoff, poster OCR title evidence, or public Telegram/VK/Telegraph event rendering.

### Affected surfaces

- `smart_event_update.py` weak-title routing and LLM title recovery.
- `source_parsing/telegram/handlers.py` candidate title handoff from Telegram Monitoring.
- Production `event`, `event_source`, `eventposter`, `event_source_fact`, public Telegraph, `@kldevents`, and `klgdevents` posts for event `6508`.

### Mandatory checks before closure or deploy

- Unit tests must cover `Городской фестиваль` + source `Городской фестиваль «ВЕЛОДЕНЬ»` / OCR `ВЕЛОДЕНЬ` routing to LLM recovery.
- Negative controls must show a genuinely generic festival source with no quoted/OCR own name is not forced into recovery, and an already distinctive title containing `ВЕЛОДЕНЬ` is not touched.
- Replay fixture `tests/replays/INC-2026-06-30-generic-title-dropped-own-name/kulturnaya_chaika_7913.json` must pass through Smart Update create boundary and save a title containing `ВЕЛОДЕНЬ`.
- Public repair must verify DB, Telegraph, Telegram `@kldevents/1630`, and managed VK if accessible through VK API.
- `/healthz` must be OK after deploy/repair.

### Required evidence

- Production DB rows before/after repair for event `6508`.
- Telethon source/public post artifacts and runtime log excerpts.
- Test output for focused title recovery tests and Smart Update replay.
- Deployed SHA reachable from `origin/main` before closure.

## Immediate Mitigation

- Added an LLM-first routing guard: generic category titles that have no distinctive own-name tokens and whose source/OCR contains a quoted/poster own-name are routed to existing LLM title recovery.
- No deterministic rewrite to `ВЕЛОДЕНЬ` was added; deterministic code only detects the high-risk shape and asks the LLM. The recovered title is accepted only after source/OCR/fact grounding validation.
- Added focused tests and a replay fixture for the incident source shape.

## Corrective Actions

- [x] Extend Smart Update weak-title routing beyond `<event_type> — <venue>` to source-grounded category-title own-name loss.
- [x] Update Smart Update docs and changelog.
- [x] Add unit/Smart Update replay tests with negative controls.
- [ ] Repair event `6508` canonical DB row and public Telegram/VK/Telegraph surfaces.
- [ ] Deploy from a clean SHA and back-merge to `origin/main`.

## Follow-up Actions

- [ ] Add a fresh-public audit that flags active/current/future events where final title/body are category-only while source/OCR contains a quoted or poster own-name.
- [ ] Review Telegram Monitoring title extraction prompt for preserving quoted own names in single-event headlines.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

The prevention keeps the LLM-first boundary: deterministic code only routes high-risk weak titles to LLM recovery and validates grounding; it does not choose a semantic title by regex/OCR. Negative controls prevent the guard from becoming a blanket rewrite for all generic festival/category titles.
