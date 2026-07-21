# agy Gemini Pro consultation — 2026-07-21

> **Consultant:** `agy` model `Gemini 3.1 Pro (High)` / resolved CLI model
> `gemini-3.1-pro-high`.
> **Result:** completed, exit `0`, 2026-07-21 20:29:15–20:31:20 UTC.
> **Prompt:** [external-research-brief.md](external-research-brief.md).
> **Local evidence (not committed):**
> `artifacts/codex/easter-eggs-product-20260721/gemini-invocation.txt`,
> `gemini-3.1-pro-high.raw.md`, `kenigevents_easter_eggs_research.gemini.md`.
> **Verdict:** consultant returned `NARROW`, confidence `85%`.

## Что подтвердил консультант

1. Механика должна быть конечной культурной коллекцией, а не бесконечным
   engagement engine.
2. Главный риск — cannibalization пути к событию/билету; completion и time-on-site
   не являются продуктовой ценностью.
3. Нужны явные `hide/safety pause`, reduced-motion/keyboard/screen-reader paths,
   admin kill switch и раздельная партнёрская модерация.
4. Призовая версия, leaderboard, промокоды и partner network не входят в MVP.
5. Эксперимент требует control, A/A, SRM, novelty-aware duration и проверки
   основной конверсии.
6. Common placement улучшает обсуждаемость, но создаёт spoiler/fatigue risk.

Эти выводы уже отражены в [product-analysis.md](product-analysis.md) и feature
contract.

## Что не принято без дополнительного доказательства

Consultant output — input в решение, а не authority. Следующие предложения
**отклонены или оставлены гипотезами**.

| Предложение Gemini | Disposition | Причина |
|---|---|---|
| kill при core conversion `-2%` | не принято как число | нет baseline, variance, MDE, economics и exact outcome definition; threshold принимает owner до эксперимента |
| kill при `empty hunt sessions >15% DAU` | не принято как число/деноминатор | DAU и session classification могут сами быть vanity/noisy; нужен journey-level metric и baseline |
| `1 новая/неделю`, `≤5 active`, `1 find/session`, cooldown `24h` | research hypotheses | числа не обоснованы traffic/UX evidence; в spec остаётся configurable cadence/caps |
| universal common placement | не принято как default | сообщение `519` описывает реальный trade-off; MVP recommendation остаётся stable `cohort`, common — отдельный experiment/campaign mode |
| grey `EXPIRED_UNFOUND`/«утерянная» | отклонено | создаёт FOMO/punishment; earned progress сохраняется, cultural archive открывается после campaign |
| hunt в 404/no-results как первый MVP slot | только prototype candidate | error/recovery surfaces критичны; пасхалка может помешать восстановлению основной задачи |
| checkbox «передаю права» решает UGC/IP | отклонено как достаточный gate | checkbox не доказывает авторство/полномочия и не заменяет договор/rights review |
| prize future автоматически требует `115-ФЗ`, Госуслуги/телефон и `18+` | отклонено | это неподтверждённое и частично неверно маршрутизированное legal утверждение; exact promo/lottery/advertising/tax/privacy regime определяет юрист по механике |
| suggested historical objects/facts | не приняты в curated set | отсутствует provenance/fact/IP/safety review; это ideation only |
| evidence strength `High` для examples без direct primary source | понижено до supplementary | часть таблицы опирается на generic blogs/cases или не даёт прямой проверяемой ссылки |
| предполагаемый social-share rate | не принято как факт | нет источника и product-specific baseline; share остаётся secondary/diagnostic |

## Что изменилось после review

- Сохранён `NARROW` verdict и усилена формулировка о non-inferiority основного
  event journey.
- Все numeric cadence/kill values трактуются как параметры исследования, а не
  готовые правила.
- `communal|cohort|personal` остаются разными campaign modes; выбор не закрыт
  одним внешним мнением.
- Legal section не содержит предположений о конкретной авторизации/возрасте и
  требует отдельного owner/legal gate.
- Curated set нельзя собирать из непроверенных примеров консультанта.
- Full raw report не коммитится: в канонике сохраняется проверяемый synthesis и
  disposition каждого существенного спорного вывода.

## Итоговый consultant gate

Консультация закрывает только **critical product-review input** для discovery.
Она не закрывает external market research, legal, UX prototype, architecture,
analytics feasibility или owner acceptance. Перед immutable implementation RC
нужен повторный independent review уже по точному прототипу и измерительному
контракту.
