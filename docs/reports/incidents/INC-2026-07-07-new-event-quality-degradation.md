# INC-2026-07-07 New event quality degradation after vector identity rollout

Status: mitigated
Severity: sev1
Service: `@kldevents` event import/publication, VK auto-import, Smart Update
Opened: 2026-07-07
Closed: —
Owners: events-bot
Related incidents: `INC-2026-06-18-tg-location-prose-still-extracted`, `INC-2026-06-24-future-event-date-default-venue-regressions`, `INC-2026-07-03-current-import-vector-vk-publication`, `INC-2026-05-30-active-duplicate-events-recall-gate`, `INC-2026-06-29-kgd80-ticket-location-drift`
Related docs: `docs/features/smart-event-update/README.md`, `docs/llm/request-guide.md`, `docs/operations/incident-management.md`

## Summary

7 July 2026 свежие карточки `@kldevents` показали массовую деградацию качества: неверные площадки, не-публичная ссылка `tg://user`, расписание вместо title, неверная дата из prose-context и нерелевантные картинки из общего VK-поста. Векторный identity gate сработал как дедуп-слой, но не был quality gate: `allow_create` был корректен для identity, однако неподтверждённые поля LLM-кандидата прошли дальше в public fanout.

## User / Business Impact

- Пользователи видели неверные места у `https://t.me/kldevents/2063`, `https://t.me/kldevents/2062` и более раннего sibling `2014`.
- В `https://t.me/kldevents/2057` альбом содержал 3 нерелевантные картинки из соседнего события того же VK-поста.
- `https://t.me/kldevents/2052` получил title `пятница 22:00` и неверную площадку.
- Дополнительный аудит нашёл `https://t.me/kldevents/2055`: дата/время взяты из prose-контекста (`10 июля`) вместо структурной строки `31 июля, начало 21:00`.

## Detection

Инцидент замечен пользователем по публичным Telegram URL. Runtime file mirror был включён (`ENABLE_RUNTIME_FILE_LOGGING=1`, `/data/runtime_logs`) и дал логи Smart Update/vector identity/fact rejection. Артефакты расследования сохранены локально в `artifacts/codex/20260707-new-event-degradation/`.

## Timeline

- 2026-07-07 07:16 UTC — снят production probe по постам 2052/2057/2062/2063.
- 2026-07-07 07:18 UTC — подтверждено, что `wall-148784347_6843` содержит смешанные фото: первые 3 — двор/концерт, последние 3 — «Ужин в музее».
- 2026-07-07 07:19 UTC — runtime logs показали `smart_update.fact_rejected reason=ungrounded_sensitive_fact` для `Калининград Сити Джаз Клуб`, но canonical `location_name` всё равно был опубликован.
- 2026-07-07 07:21 UTC — свежий аудит выявил дополнительный дефект `2055` / event `6721`.
- 2026-07-07 07:24 UTC — production DB repaired with backups and requeued public fanout.
- 2026-07-07 07:29 UTC — media rows converted to stable static URLs after VK source URL 404 on retry.
- 2026-07-07 07:30 UTC — replacement Telegram post for event `6709` published as `https://t.me/c/3954607218/2066`; old wrong album messages 2057–2060 had been deleted.

## Root Cause

1. **Vector-first boundary was identity-only.** `SMART_UPDATE_IDENTITY_GATE=enforce` compared candidate vectors to existing events and allowed creates because nearest vectors were not same identity. It did not validate source grounding of candidate fields.
2. **LLM candidate quality was allowed to fail open.** VK LLM extraction emitted unsupported venue (`Калининград Сити Джаз Клуб`), weak schedule title (`пятница 22:00`) and wrong date from prose context; pre-create did not fail closed.
3. **Fact rejection did not scrub canonical fields.** Smart Update logged ungrounded location as rejected fact, but `event.location_name`/`location_address` survived to Telegram/VK.
4. **VK mixed-media fallback was too broad.** For multi-event VK posts, when OCR could not confidently assign images to child events, raw source gallery fallback attached unrelated photos.
5. **VK contact mention normalized to Telegram internal link.** A VK mention `[id9648720|...]` became `tg://user?id=9648720`, unusable as a public VK-source registration link.
6. **Source default/location enrichment gap.** `moyteatr_kld` had group address in VK (`Больничная, 24`, `2 этаж`) but production source metadata had no safe default, making venue hallucination more likely.

## Contributing Factors

- Multi-event same-source posts create several children with shared `source_url`, so per-child media relevance matters.
- Several older incident contracts already required source-grounded location/media guards, but VK raw-photo fallback and vector identity gate were not covered by one combined regression test.

## Automation Contract

### Treat as regression guard when

- Changing VK intake LLM prompt, `build_event_drafts_from_vk`, `_build_smart_update_posters`, Smart Update create-path quality gates, vector identity gate, ticket-link normalization, or source-media rehydration.
- Enabling/changing vector search before Smart Update create/merge decisions.

### Affected surfaces

- `vk_intake.py` VK LLM prompt and post-LLM guardrails.
- Smart Update vector identity/create path.
- Telegram/VK/Telegraph event fanout and joboutbox retries.
- `vk_source` default location metadata.

### Mandatory checks before closure or deploy

- Unit tests for unsupported VK venue clearing, VK `tg://user` conversion, schedule-fragment title fail-closed, structured footer date conflict, and mixed multi-event media fallback.
- Runtime-log evidence that vector identity decisions are not treated as quality approval.
- Public verification for repaired Telegram/VK/Telegraph posts.
- Release governance: fix commit reachable from `origin/main`, clean deploy worktree, production deploy evidence.

### Required evidence

- Production repair backups: `codex_backup_20260707_new_event_degradation_*` and `codex_backup_20260707_new_event_degradation_media_retry`.
- Investigation artifacts: `artifacts/codex/20260707-new-event-degradation/*.json`.
- Test run: `python -m pytest tests/test_vk_intake_quality_guardrails.py tests/test_telegraph_side_effects.py -q`.
- Deployed SHA and post-deploy smoke before status moves to closed.

## Immediate Mitigation

- Repaired canonical rows `6701`, `6702`, `6703`, `6707`, `6708`, `6709`, `6716`, `6717`, `6721` from source-grounded data.
- Set `moyteatr_kld` source default location to `Мой театр, Больничная 24, Калининград`.
- Replaced VK-source registration links with `https://vk.com/id9648720` for the Moy Teatr events.
- Deleted wrong Telegram albums for `6707` and `6709`; requeued/republished corrected fanout.
- Converted «Ужин в музее» media to stable static copies of the relevant dinner images.

## Corrective Actions

- Tightened VK LLM prompt: structured `📅 DD месяц, начало HH:MM` wins over contextual prose date; schedule fragments must not be titles; ungrounded venues must stay empty.
- Added VK fail-closed guardrails for unsupported venue facts, schedule-fragment titles, structured-footer date conflicts, public VK contact links, and raw-gallery fallback in mixed multi-event posts.
- Added regression tests for all guards.

## Follow-up Actions

- [ ] Close only after production deploy SHA is reachable from `origin/main` and post-deploy VK/TG smoke confirms no recurrence.
- [ ] Add a periodic fresh-event quality audit that samples new `@kldevents` posts for unsupported venue/title/date/media shapes.
- [ ] Backfill/verify default locations for high-volume VK sources where VK group address exists but `vk_source.default_location` is empty.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: local tests passing; production repair in progress/monitoring
- post-deploy verification: pending

## Prevention

Vector recall remains a dedup/identity signal only. New quality gates are LLM-first prompt constraints plus narrow deterministic fail-closed support: unsupported facts are removed/rejected before public creation, and ambiguous media from multi-event sources is dropped instead of copied broadly.
