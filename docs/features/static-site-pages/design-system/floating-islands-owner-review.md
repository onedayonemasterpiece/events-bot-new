# Floating Islands — текущая исполняемая итерация

2026-09-05. PR #638, `work/floating-islands-owner-preview-20260905`. Draft: не merge-ready, не production, не полный FI-P1/A=S=P. Актуальный contract: существующий DS #47, `pattern.detached-chrome-control-islands`, composition v1.3.

## Решения владельца, которые сохраняются

Нижний остров — **одна цельная поверхность**, будущее оформление ближе к macOS, не отдельные острова-иконки. Тяжёлый desktop вариант отклонён и снят в be4a15d1; прежний desktop/mobile вид сохраняется. Новая skin не внедряется этим исправлением.

Брендовое меню «Полюбить Калининград Анонсы» не сжимается, не переставляется и не меняет states из-за соседних островов. Exact Free допускает один canonical medallion вместо дублирующего floating-названия, но сохраняет H1/accessible name и текстовый fallback при asset failure. Richer scopes сохраняют qualifiers. Выбор городов использует прежние fieldset/checkbox/filter/storage owners.

## Исправление ранее заблокированного случая

Исходный30a0c977 / run33966645022 имел реальный failed case: без native Popover API номинально inline-прямоугольник оставался второй колонкой horizontal controls, а sticky rail перекрывал H1. Неудачный клик не форсировался и тест не удалялся.

Точечное CSS исправление: compact controls становятся block/100%/auto-height; **только при `data-fi-city-placement=inline`** соответствующий rail становится static/top:auto/auto-height, его inner container получает natural height. Обычная rail, бренд и lower dock не меняются. Выполнено добавление source regression в existing test file; прежний реальный browser outside-dismiss case остаётся без ослабления.

Ретроспектива доступа прочитана: `idea-hub/docs/github-access-retrospective-2026-08-27.md` и recurrence. GitHub read/permissions/schemas подтверждены; тот же create_tree с прежней CSS правкой при одном повторе теперь принят. Не использован альтернативный маршрут для обхода прежнего отказа. Отсутствие методов developer MCP остаётся отдельной проблемой: установленные my-data-hub/browser/Penpot не опубликованы среди callable namespaces этого окна.

## Проверки и граница результата

Старые 22 tests/частичный browser PASS не переносятся на новое исправление. Текущий source count — 23 checks с новой regression; status устанавливается по terminal existing `floating-islands-diagnostic.yml`, actual generated Popular/Free и артефактам. На момент записи source-документа новый прогон ещё не завершён; exact SHA/run/terminal verdict публикуются в PR #638. Отдельные результаты обычных проверок и recovery обязательно читаются из JSON, не выводятся из зелёного workflow.

Уже работающие проверки сохраняют original fieldset/selection/focus, repeated open/close, cleanup, viewport recovery, missing image fallback и прежний lower dock. Fixture corpus — July23; resize не native OSK. No network/product-write/auth side effects из adapter. Shared foundations/STATUS, production/root/current/ICS и действующая integration ветка не меняются.

## Незавершённая интеграция

Current trunk имеет пересекающуюся top-band/menu implementation: нельзя механически совмещать controllers или вернуть отменённое adaptive branded menu. Остаются единая верхняя композиция с защищённым брендом, canonical family/impact/scenario registration, current-corpus Kaggle preview и native P. Temporary diagnostic не второй publisher и не готовая deployment lane. Root Unreleased changelog и полный source-bound S/P пакет должны быть согласованы при интеграции; эта bounded repair не объявляет их завершёнными.
