# Floating Islands — текущая исполняемая итерация

2026-09-05. PR #638, `work/floating-islands-owner-preview-20260905`. Draft: не merge-ready, не production, не полный FI-P1/A=S=P. Владелец контракта — существующий DS #47, pattern.detached-chrome-control-islands, composition v1.3.

## Сохранённые решения владельца

Один цельный нижний остров; будущее оформление ближе к macOS, не отдельные иконки-острова. Отклонённая тяжёлая desktop skin удалена, прежний dock остаётся. Брендовое меню не сжимается/не переставляется/не меняет states от соседней компоновки. Exact Free использует canonical medallion без дублирующего floating-названия, при сохранённых H1/accessible name/asset-error fallback. Richer scopes сохраняют qualifiers. Города используют тот же fieldset/checkbox/filter/storage owner.

## Исправление inline fallback и подтверждение доступа

Ретроспективы idea-hub о GitHub access/preflight прочитаны. Schemas и push permission подтвердились; одна повторная попытка **того же create_tree** с прежде заблокированной CSS правкой успешно принята. Получен и прочитан обратно commit ef1336070391502b411d2b72d888990410c5f913. Причина прежнего safety-status отказа неизвестна; это не доказательство, что все MCP заработали. Developer plugins установлены, но их callable namespaces отсутствуют.

Первый повторный run33969596843 подтвердил 23 source checks и обычные browser проверки, однако выявил неполноту прежней подготовленной правки: высота controls и rail освобождена, но actual mobile `.ke-listing-discovery-rail__inner` всё ещё наследовал `grid-template-rows:48px` из design-system.css. Поэтому прямоугольник рисовался поверх последующих карточек, и реальный checkbox click падал. Не увеличиваем z-index и не используем forced click: это неверная высота layout, не приоритет слоя.

Следующее адресное исправление сохраняет natural height на **всех** уровнях: compact control owner — block/auto-height, expanded-inline rail — static/auto-height, его inner grid — `grid-template-rows:auto; grid-auto-rows:auto`. Исходная закрытая rail, бренд, нижний dock и shared foundations не меняются. Браузерный regression дополнен actual containment: низ открытого inline-прямоугольника должен входить в выделенную rail область. Существующие click/selection/outside-dismiss/Escape/cleanup assertions не удалены и не ослаблены.

## Evidence

Новый terminal verdict берётся из exact commit/run и JSON артефактов existing floating-islands-diagnostic.yml. На момент записи исходников следующего прогона ещё нет; ничего не названо PASS. Старый23-source/partial-browser результат не переносится на successor автоматически. Снимки в native-popover и Popover-unavailable initial states сохраняются отдельно, как и parent/panel rects в малом viewport.

Это настоящий Astro через existing local:focused на July23 fixtures, не свежий production corpus, не actual OS-клавиатура и не public Kaggle preview. Adapter не добавляет network/Auth/product writes/telemetry. Задача не меняет production/root/current/ICS/foundations/STATUS.

## Открытая интеграция

Current trunk имеет пересекающуюся top-band/menu implementation. Нельзя механически совместить controllers или вернуть отменённую adaptive бренд-геометрию. Единый верхний ряд, canonical family/impact/scenario registration, current-corpus Kaggle preview/native P и root Unreleased integration остаются незавершёнными. Этот пакет закрывает конкретный regression, не всю нормализацию или A=S=P.
