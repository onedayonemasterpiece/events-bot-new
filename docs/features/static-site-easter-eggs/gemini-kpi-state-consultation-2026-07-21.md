# Gemini Pro: критика KPI, стабильного размещения и motion

> **Дата:** 2026-07-21
>
> **Внешний консультант:** agy `Gemini 3.1 Pro (High)`
>
> **Provider verdict:** `ACCEPT WITH CHANGES`
>
> **Итог владельца продукта:** принять выборочно; контракт обновлён, но не все
> советы консультанта приняты.

## Receipt

- Prompt: [gemini-kpi-state-followup-brief-2026-07-21.md](gemini-kpi-state-followup-brief-2026-07-21.md)
- Команда:
  `/home/dev/.local/bin/a-gemini --print-timeout 15m < docs/features/static-site-easter-eggs/gemini-kpi-state-followup-brief-2026-07-21.md`
- Successful run: `2026-07-21T21:03:48Z` → `2026-07-21T21:05:47Z`, exit `0`,
  transcript `9a82f61c-317a-462f-9380-930fe4ee897e`.
- Wrapper default: `ANTIGRAVITY_GEMINI_MODEL:-Gemini 3.1 Pro (High)`.
- Agy log зафиксировал `model="Gemini 3.1 Pro (High)"` и передачу model override
  backend. Ответ завершён без provider error.
- Локальные некоммитируемые материалы:
  `artifacts/codex/easter-eggs-kpi-state-20260721/stdout.md`,
  `stderr.log`, `invocation.txt`, `model-evidence.txt`. Предшествующий вызов
  wrapper с `--help` дал `a-agy-model: empty prompt`; он отмечен в receipt как
  failed help probe и не является consultant run.

Это валидная консультация Pro-class по проектной policy, а не Flash/Lite probe.

## Что сказал консультант

Gemini поддержал found echo, разделение dislike/report/hide, запрет reroll и
релокацию только по safety-причине. Он потребовал:

- сделать ITT `assigned_eligible` главным denominator;
- не исключать ушедших пользователей через `reached K`;
- отделить explicit hint от proactive hint;
- не использовать постоянную viewport/viewability телеметрию;
- не считать privacy suppression `k≥20` достаточным размером выборки;
- не обещать полную анонимность, юридический compliance или prize eligibility без
  отдельных audit/terms/anti-fraud.

Одновременно Gemini предложил one-shot entrance-анимацию, убрать точный
`placement_id` из аналитики, удалить все difficulty target bands и заменить их
только uplift core CTA.

## Критическое disposition

| Recommendation | Решение | Почему |
|---|---|---|
| Found echo остаётся до expiry, затем история живёт в коллекции | **Принято** | Сохраняет пространственное подтверждение без repeat claim и постоянного beacon. |
| Dislike, report и hide независимы; dislike не скрывает и не reroll | **Принято** | Feedback не должен быть скрытой командой смены difficulty/location. |
| Stable semantic zone с fallback вместо DOM index | **Принято и усилено** | Assignment теперь фиксирует versioned `placement_bundle` и все mobile/desktop/accessibility anchors сразу. Fallback не выбирается заново при открытии устройства. |
| Основной difficulty denominator — ITT `assigned_eligible` | **Принято** | Иначе non-delivery и dropout могут искусственно повысить find rate. Delivery остаётся диагностикой рядом. |
| Убрать постоянный viewport/viewability event | **Принято** | Default строится из assignment/hint/find/feedback и максимум одного optional delivery summary; scroll/intersection ticks не хранятся. |
| Time-bounded KPI вместо основного `@K` | **Принято частично** | Primary стал `unassisted_discovery_within_W`; `W` pre-register. `@K` оставлен лишь пилотной usability-диагностикой без survivor exclusion. |
| Удалить все difficulty target bands | **Отклонено** | Пользователь явно требует целевые KPI и проверку «не слишком легко». Bands сохранены только как canary hypotheses до baseline/A/A/MDE, не как доказанные нормы или SLO. |
| Заменить difficulty KPI только uplift core CTA | **Отклонено** | CTA uplift измеряет бизнес-эффект кампании, но не отвечает, стала ли находка слишком простой. Оба слоя нужны рядом, с non-inferiority guardrails. |
| Убрать `placement_id` из агрегатов | **Отклонено** | Это уничтожит прямо запрошенную диагностику по местам. Остаётся allowlisted semantic low-cardinality placement/version с suppression; raw URL/DOM/координат нет. |
| One-shot entrance при первом viewport | **Отклонено для default** | Автоматическая entrance-анимация превращает секрет в beacon и требует viewport orchestration. Default static; finite halo возможен только как явная ступень подсказки. |
| `k≥20` заменить sequential testing | **Уточнено** | `k≥20` может быть privacy suppression candidate, но никогда не inference gate. Power/MDE/interval plan задаётся отдельно; метод выбирается до пилота. |
| TTL purge из YDB/Supabase за 30 дней | **Уточнено** | TTL обязателен для временных analytics summaries, но durable assignment/progress в Supabase живёт по account/collection retention policy, иначе ломается «не убегает». |
| Kill switch SLA `≤5 минут` | **Не принято как обещание** | Нужен проверяемый kill-switch gate, но численный SLA нельзя утверждать без runtime design, owner и rehearsal evidence. |

## Итоговые продуктовые решения

1. **Не убегает:** назначается не DOM-координата, а неизменяемый semantic
   placement bundle с заранее заданными anchors для всех поддерживаемых путей.
2. **После находки:** один confirmation, затем статичный
   `✓ Найдено — открыть историю` в прежнем месте до expiry или явного hide.
3. **Не понравилось:** оценка и Undo; отдельно hide; отдельно safety/fact/report.
   Обычный dislike не снимает объект и не меняет призовые шансы.
4. **Движение:** до находки static; hover/focus — короткий design-system
   transition; finite halo — только после hint; после find motion отсутствует.
5. **Статистика:** ITT по назначенным + delivery diagnostic; rollups по
   low-cardinality semantic placement, interaction type и story domain; без GPS,
   raw URL, IP в payload и viewport firehose.
6. **Сложность:** onboarding легче; standard/hard имеют предварительные низкие
   unassisted bands, но release-решение требует baseline, confidence intervals,
   accessibility parity, frustration и core CTA non-inferiority.

## Что остаётся доказать до implementation

- Baseline и MDE: проценты сложности сейчас не эмпирическая норма.
- Эквивалентность placement bundles на mobile/desktop/keyboard/screen reader.
- Consent, retention и deletion policy; HMAC означает de-identified, не
  «абсолютно anonymous».
- Anti-fraud и legal terms до материальных призов.
- Kill-switch propagation и operational SLA на rehearsal, а не в предположении.
