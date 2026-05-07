# Incident Management

Канонический процесс для production incidents, их формализации, regression discipline и closure gate.

## Автоматический запуск incident workflow

- Достаточно указать конкретный incident ID (`INC-*`), чтобы агент автоматически перешёл в incident workflow.
- Даже без готового `INC-*`, production-недоступность или user-visible деградация автоматически запускает incident workflow: `/healthz` timeout/not ready, Fly proxy `/webhook` errors, бот не отвечает на `/start` или другие базовые команды, critical scheduled slot сорван/завис.
- При таком упоминании агент обязан открыть:
  - `docs/reports/incidents/README.md`
  - канонический incident record по ID
  - `docs/operations/release-governance.md`
- Incident record становится обязательным regression contract для всей задачи.

## Цель

- быстро и предсказуемо стабилизировать production;
- не закрывать инцидент “на глаз”, если регресс может вернуться;
- держать incident records в таком виде, чтобы по одному ID можно было восстановить:
  - impact;
  - root cause;
  - affected surfaces;
  - mandatory checks;
  - release evidence;
  - follow-up actions.

## Severity

- `sev0` — полная недоступность или критический бизнес-stop для production.
- `sev1` — заметная деградация ключевой production-функции, customer-visible impact или потеря канонического product contract.
- `sev2` — частичная деградация, обходной путь есть, но риск повторения/накопления высокий.
- `sev3` — локальный или маломасштабный дефект без немедленного широкого impact, но требующий формального record при production relevance.

## Обязательный workflow

### 1. Triage

- найти или создать канонический incident record в `docs/reports/incidents/`;
- зафиксировать incident ID, статус, severity и service scope;
- определить affected surfaces: код, env, deploy path, внешние зависимости, smoke path, мониторинг.

### 2. Evidence Collection

- собрать production evidence из логов, runtime state, Fly config/status, БД/`ops_run`, Kaggle outputs или других канонических источников;
- не заменять evidence догадками;
- если evidence недостаточно, инцидент нельзя считать локализованным.

### 3. Containment And Fix

- минимизировать blast radius;
- incident-related change держать как можно уже по scope;
- если нужен emergency deploy, соблюдать `docs/operations/release-governance.md`.

### 4. Regression Contract

Для каждого incident record должны быть явно перечислены:

- `Treat as regression guard when`
- `Mandatory checks before closure or deploy`
- `Required evidence`

Если этих секций нет или они слабые, incident record нужно усилить до завершения задачи.

### 5. Closure Gate

Incident-related задача не считается завершённой, пока не выполнены все пункты:

1. root cause и contributing factors зафиксированы;
2. корректирующее изменение попало в код;
3. выполнены mandatory regression checks из incident record;
4. собран release evidence;
5. deployed SHA достижим из `origin/main`, если fix уже ушёл в production;
6. заведены follow-up actions, если нужны долгосрочные изменения;
7. docs/README/changelog синхронизированы, если поведение или процесс поменялись.

Дополнительный обязательный gate для source-import / Smart Update quality incidents:

- сохранить минимальные сырые source artifacts, которые воспроизводят сбой, в `tests/replays/<incident-id>/` или в incident-linked fixture;
- прогнать эти artifacts через тот же production import boundary, который сломался (`Telegram Monitoring` server import, `VK auto-import`, `/parse`, linked-source import и т.п.), дальше обязательно через `smart_event_update.py`, на prod snapshot copy или shadow DB;
- зафиксировать pre/post DB diff или проверочный query output: какие кандидаты были созданы/смёржены/пропущены, какие public rows стали active/merged/skipped;
- если replay не даёт целевой результат incident record, closure запрещён: агент обязан продолжить итерацию prompt/code/guard изменений и повторять replay до прохождения, либо явно оформить внешний blocker; production data repair может быть частью mitigation, но не заменяет passing replay для prevention;
- добавить хотя бы один negative/opposite control, чтобы guard не превратился в blanket skip валидных будущих событий;
- считать prompt-only diff, unit tests, локальный вызов extractor без Smart Update и ручной SQL-аудит недостаточными для closure, если replay не выполнен или blocker не оформлен как follow-up.

## Incident Control Block

Для каждой incident-related задачи агент должен быстро собрать рабочий блок:

- `Incident ID`
- `Current status`
- `Affected surfaces`
- `Target behavior`
- `Mandatory checks`
- `Release evidence to collect`
- `Follow-up actions`

Этот блок может быть не отдельным файлом, но должен явно присутствовать в reasoning и итоговом отчёте.

## Что должен делать агент автоматически

Если пользователь указал конкретный incident ID:

1. открыть incident record;
2. извлечь affected surfaces и обязательные проверки;
3. ограничить решение incident-relevant scope, если пользователь не попросил шире;
4. не завершать задачу без incident-specific regression checks;
5. в финальном ответе явно перечислить:
   - incident ID;
   - что было исправлено;
   - какие regression checks выполнены;
   - какое release evidence собрано;
   - какие follow-up actions остались.

Если пользователь меняет production path, который затрагивает already-known incident surface:

1. поднять релевантный incident record самостоятельно;
2. выполнить его regression checks;
3. сообщить об этом в финальном отчёте.

## Как оформлять новый incident

- использовать `docs/reports/incidents/TEMPLATE.md`;
- file name: `INC-YYYY-MM-DD-short-slug.md`;
- один production event — один incident record;
- повторение на новой дате оформлять новым `INC-*` с ссылкой на прошлые инциденты в `Related incidents`.

## Minimum Quality Bar

- blameless разбор;
- чёткий timeline;
- root cause, а не только symptom;
- actions в формате actionable/specific/bounded;
- обязательные regression checks против повторного инцидента;
- release discipline без branch drift и без “починили только в проде”.
