# Prompt для agy Gemini: KPI, стабильное размещение и motion пасхалок

## PROMPT START

Ты — критический продуктовый консультант Gemini Pro. Проведи adversarial review
нового контракта пасхалок KenigEvents. Не переписывай исходный текст и не предлагай
код. Нужны ясные решения и критика, особенно если предложенные числа создают
невидимую, несправедливую или легко оптимизируемую механику.

Repository/branch:
https://github.com/onedayonemasterpiece/events-bot-new/tree/feature/static-site-easter-eggs-product-analysis-20260721

Обязательные документы:

1. Feature home:
https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/static-site-easter-eggs-product-analysis-20260721/docs/features/static-site-easter-eggs/README.md
2. Critical product analysis:
https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/static-site-easter-eggs-product-analysis-20260721/docs/features/static-site-easter-eggs/product-analysis.md
3. Новый draft measurement/state contract находится в локальном worktree:
`docs/features/static-site-easter-eggs/measurement-and-state-contract.md`.
Если GitHub ещё не показывает этот файл, прочитай его локально из current project.
4. Data ownership:
https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/static-site-easter-eggs-product-analysis-20260721/docs/architecture/personalization-data-ownership.md
5. Design system:
https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/static-site-easter-eggs-product-analysis-20260721/docs/features/static-site-pages/design-system/README.md

### Решения, которые нужно проверить

- Stable assignment: user/device получает `placement_id/version` один раз; reload,
  visit, hint, dislike и card reorder не перемещают пасхалку. Только audited
  safety relocation.
- После find target остаётся в том же месте как static found echo до expiry;
  пользователь может явно скрыть marker, но progress сохраняется.
- `dislike` не меняет progress/placement/eligibility; hide и report отдельны.
- Default pre-find token статичен; finite halo только как hint; found echo не
  анимируется.
- Browser компактизирует события в opportunity summary; Supabase хранит durable
  progress, YDB — de-identified TTL analytics, core SQLite — definitions/control.
- Difficulty targets:
  - first/onboarding `unassisted_discovery@2 = 55–75%`;
  - standard `unassisted_discovery@3 = 15–35%`;
  - hard `unassisted_discovery@3 = 5–20%`;
  - assisted by expiry: `70–90%`, `30–55%`, `15–35%` соответственно;
  - placement gap `≤15pp`, accessibility-path gap `≤10pp`;
  - negative quality thresholds provisional.

### Обязательные вопросы

1. Действительно ли found echo до expiry лучше immediate disappearance и permanent
   full card? Укажи trade-offs и точный recommended default.
2. Может ли dislike когда-либо скрывать объект автоматически? Раздели dislike,
   report и hide.
3. Достаточно ли stable hash + persistence, чтобы обещать «не убегает» на anonymous,
   login merge, mobile/desktop и изменяемом каталоге?
4. Какие forced relocation cases допустимы и как не нарушить fairness?
5. Motion: static vs halo vs pulse. Как сохранить азарт, но не сделать target
   баннером или accessibility trap?
6. Критикуй каждый KPI band. Не принимай числа без оснований. Какие denominators,
   censoring, hint attribution и minimum sample нужны?
7. Как агрегировать по placement locations и interaction/story types без high
   cardinality, re-identification и ложного ranking малых cells?
8. Можно ли выбросить ещё больше telemetry? Назови минимально достаточные fields и
   retention.
9. Какие метрики легче всего gaming/manipulation партнёром, редактором или
   scheduler и как закрыть loopholes?
10. Сформулируй пять обязательных release acceptance rules.

### Формат ответа

1. Verdict `accept | accept with changes | reject`.
2. Таблица `decision → agree/disagree → correction → evidence/reason`.
3. KPI critique с предложенными provisional bands или честным отказом от чисел.
4. Minimal ecological event/rollup schema.
5. Final state/motion matrix.
6. Exact diff recommendations к `measurement-and-state-contract.md`.
7. Unsupported/legal/privacy claims, которые нельзя включать.

Не использовать Flash/Lite/Gemma как consultant. Отвечай по-русски. Все внешние
факты снабжай прямыми ссылками; отделяй evidence от product judgment.

## PROMPT END
