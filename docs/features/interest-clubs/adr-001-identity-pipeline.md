# ADR-001: identity, evidence и publication boundary клубов

- **Status:** Accepted for implementation RC; production rollout gated
- **Date:** 2026-07-17
- **Decision owners:** product owner + events-bot maintainer
- **Evidence:** PR #54 / `e1a14afc`, [catalog audit](../../reports/interest-clubs-catalog-audit-2026-07-17.md)

## Контекст

Исследование нашло реальный candidate pool, но exact name/source retrieval недостаточен для новых встреч, а source-only lane загрязнён. Existing vector sidecar не покрывает исторический corpus достаточно для честного model comparison. Controlled Gemma verifier достиг нулевых unsafe false positives на 24 hard negatives, тогда как Lite дал четыре. Нужна отдельная identity поверх canonical events без смешения с linked occurrences, festivals и venue/organizer series.

## Решение

1. **Canonical ownership.** Fly SQLite хранит versioned club identity и many-to-many event relation. Existing canonical event остаётся единственным источником event facts. Static export — disposable projection; Supabase может обслуживать существующий provider limiter/telemetry, но не владеет club identity.
2. **Evidence-first relation.** Curated identity anchors и retrieval только создают bounded candidate. Positive relation требует `gemma-4-31b-it` verdict `yes`, exact supporting quote и версии packet/policy/model. Provider/quote/ambiguity failures fail closed; Lite не positive fallback.
3. **Public boundary.** Публикуются только owner-approved confirmed identities и accepted grounded relations, прошедшие freshness/boundary checks. Last-good manifest сохраняется при pipeline failure.
4. **Identity operations.** Immutable id; stable slug; aliases/redirect tombstones; reviewer-only merge/split/slug change. Co-hosting — несколько отдельно доказанных relations, не merge identities.
5. **Time and boundaries.** Distinct meeting date — evidence unit. Source duplicates и linked siblings collapse; festival containment само по себе relation не создаёт.
6. **Lifecycle.** Active ≤90 дней либо есть future meeting; dormant 91–365 дней без future; archived >365 дней или explicit closure. Dormant/archive скрыты из актуальной выдачи, но контролируемая история и redirects остаются.
7. **Privacy/storage.** Храним минимум provenance, не участников/членство/inferred audience. Raw/replay/embedding artifacts имеют retention и могут быть пересобраны.

## Почему не альтернативы

- **Regex/keywords:** semantic false merges (`клубника`, nightclub, generic topics) и нарушение LLM-first policy.
- **Source/name anchors как verdict:** пропускают rename/new clubs и смешивают несколько identities одного source.
- **Unreviewed embedding clusters:** similarity — retrieval, не доказательство; measured historical comparator пока отсутствует.
- **Festival/linked identity reuse:** ломает предметные границы и завышает recurrence.
- **Отдельная Supabase canonical DB:** создаёт dual-write/ownership drift без продуктовой необходимости.

## Последствия

Плюсы: fail-closed precision, auditability, устойчивые URL, безопасный rollback и независимость event semantics. Цена: review queue, false negatives/deferred cases, отдельные identity migrations и необходимость owner-approved gold/replay до production.

## Обязательные regression contracts

- `INC-2026-05-05-smart-update-gemma3-fallback-hallucination`: никакого broad/Lite fallback для positive relation;
- relation не меняет canonical event fields или linked/festival identity;
- merge/split/slug mutation не происходит без audit record;
- любой public relation воспроизводим по event/source ids, packet hash, quote, policy/model version;
- public build при ошибке остаётся на last-good manifest.

Изменение порогов freshness, positive model/fallback, canonical DB или автоматизация merge/split требует новой ADR либо superseding amendment.
