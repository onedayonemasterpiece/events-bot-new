# Как обновлять launch-readiness dashboard

Этот каталог — единый живой контур для фокус-группы и публичного запуска
**1 сентября 2026 года**.

## Что редактируется

Редактируется только [`checklist.toml`](checklist.toml).

Файлы [`README.md`](README.md), [`CHECKLIST.md`](CHECKLIST.md) и
[`KANBAN.md`](KANBAN.md) генерируются и не правятся вручную:

```bash
python3 scripts/generate_launch_readiness.py
python3 scripts/generate_launch_readiness.py --check
```

## Рекомендуемая периодичность

- до 24 августа — каждые 2–3 дня;
- 25–31 августа — ежедневно;
- немедленно после изменения любого P0-блокера, owner gate, production incident,
  публичного документа или target release identity;
- 1 сентября — после каждого D0 gate и после публикации;
- D+1, D+3, D+7 и D+10 — отдельные зафиксированные срезы.

Следующая плановая сверка начального среза: **7 августа 2026 года**.

## Пять минут на обновление

1. Взять свежий `origin/main`, список открытых PR и terminal evidence.
2. Изменить только реально затронутые строки в `checklist.toml`:
   `status`, `stage`, `evidence`, `source`, `next_action`, `target`, `blocked_by`.
3. Обновить `meta.updated_at`, `meta.next_review` и при необходимости verdict.
4. Запустить генератор.
5. Просмотреть diff сводки, детального checklist и kanban.
6. Запустить `--check`.
7. В PR перечислить изменившиеся IDs и основания перехода статуса.

Не нужно перечитывать и переписывать все 242 пункта на каждом цикле. Обновляются
только изменившиеся deliverables; генератор сам пересчитывает укрупнённую картину.

## Минимальный handoff каждого обновления

В описании PR или итоговом комментарии указываются: изменившиеся IDs, прежний и
новый статус, ссылка на terminal evidence, точный target SHA/release identity и
оставшиеся блокеры. Формулировка «реализовано» без указания уровня evidence не
считается обновлением readiness.

## Статусы

| Status | Когда использовать |
|---|---|
| `DONE` | Требуемый deliverable завершён на заявленном уровне evidence |
| `IN_PROGRESS` | Есть активная работа, но acceptance ещё не закрыт |
| `BLOCKED` | Есть конкретная зависимость/дефект, не позволяющие продолжить или принять |
| `NOT_STARTED` | Работа ещё не начата |
| `OWNER_GATE` | Нужен выбор/утверждение владельца продукта, релиза, дизайна или юриста |
| `VERIFY` | Код или прежнее evidence есть, но актуальный target/main/live ещё не доказан |
| `DEFERRED` | Явно исключено из текущего релиза; допустимо только для P1/P2 |

`PARTIAL` намеренно не используется: он плохо отвечает на вопрос, что именно
нужно сделать дальше. Неполный пункт получает `IN_PROGRESS`, `VERIFY` или
`BLOCKED` и обязательный `next_action`.

## Уровни evidence

| Level | Доказательство |
|---|---|
| `E0` | Проверяемого evidence нет |
| `E1` | Исследование, спецификация или принятое решение |
| `E2` | Код, unit или contract tests |
| `E3` | Интеграция, реальная сборка или browser test |
| `E4` | Hosted target, immutable candidate, emulator/simulator/device |
| `E5` | Production, live round-trip, rollback или soak |

Открытый PR сам по себе обычно не выше `E2`. Merge не делает пункт production-ready.
Старый candidate не закрывает проверку нового `main`. Для юридических пунктов
`DONE` означает не только текст, но и соответствие фактического UI/data flow плюс
правовую проверку.

## Stages

`research → decision → design → development → integration → qa → live → ready`

Stage показывает, где сейчас находится deliverable. Он не заменяет status:
например, `live + VERIFY` означает, что требуется production evidence, а
`design + BLOCKED` — что дизайн нельзя завершить до решения зависимости.

## Как работать с PR и evidence

В `source` указывается канонический документ, PR, run/receipt или incident.
Статус меняется только после проверки:

- PR открыт: обычно `IN_PROGRESS`;
- PR merged, code/unit green: обычно `E2`, иногда `E3`;
- current main candidate прошёл browser/hosted target: `E4`;
- production round-trip/soak/rollback подтверждён: `E5`;
- workflow зелёный, но production probe был skipped: не PASS;
- owner посмотрел исторический preview: не заменяет актуальный exact-main candidate.

## Как отражать UI

Для каждой существенной surface должны существовать отдельные пункты:

1. product/design decision;
2. approved reference;
3. implementation;
4. responsive/accessibility QA;
5. hosted/current-main evidence;
6. production acceptance, если она нужна.

Так утверждённый макет не смешивается с собранным UI, а собранный UI — с
фактически выпущенным.

## Как отражать юридические документы

Публичные документы ведутся как launch deliverables, а не как приложение к
документации. Минимально отслеживаются:

- сведения об операторе и контакте;
- политика обработки персональных данных;
- отдельные purpose-specific согласия;
- отдельное согласие на информационные/рекламные сообщения;
- cookies/localStorage/analytics notice;
- пользовательское соглашение;
- условия фокус-группы и правила розыгрыша;
- localization/cross-border/data-flow audit;
- retention/deletion/data-subject requests;
- incident procedure и legal sign-off.

Финальные формулировки должен проверить квалифицированный юрист по фактической
архитектуре и пользовательским потокам.

## Почему сначала Markdown/TOML, а не отдельный сервис

Базовый контур полностью бесплатен:

- версия и история изменений находятся в Git;
- GitHub отображает Markdown tasklists и Mermaid;
- generator/check работает стандартным Python;
- workflow запускается только при изменениях этого контура или вручную;
- нет платного SaaS и второго источника правды.

После двух успешных циклов обновления можно создать бесплатный GitHub Project с
представлениями board/table/roadmap. Рекомендуемые поля:

- `Status`
- `Priority`
- `Phase`
- `Stage`
- `Evidence`
- `Target`
- `Owner`
- `Blocked by`
- `Release`

До появления автоматической синхронизации Project остаётся представлением, а
`checklist.toml` — источником правды. Не нужно вручную вести две расходящиеся
доски.

## Стандартизация после пилота

После D+10 или раньше, если два цикла прошли без проблем:

1. вынести генератор в универсальный `scripts/release_readiness.py`;
2. добавить release slug/date параметрами;
3. утвердить общие enums и owner roles;
4. при необходимости генерировать GitHub Issues/Project items;
5. сохранять immutable release snapshots для ретроспективы;
6. не автоматизировать вывод `DONE` только по состоянию PR/CI.
