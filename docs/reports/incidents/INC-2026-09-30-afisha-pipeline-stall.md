# INC-2026-09-30 Афиша: остановка автоматических публикаций

Status: mitigating
Severity: sev1
Service: Telegram event publishing, VK import, Telegram Monitoring
Opened: 2026-09-30
Closed: —
Related: `INC-2026-09-19-tg-monitoring-runtime-starvation.md`

## Impact and evidence

После исправлений локаций в `@kldevents` не выходили новые автоматические события. Production `joboutbox.id=90896` (event 9407) 13 раз завершился ошибкой public writer. Логи 2026-09-30 16:06 UTC показывают: Lite вернул текст, отклонённый проверкой цитаты; строгий 4o fallback вернул HTTP 400 на JSON-схему с `enum` исходных цитат. `ops_run.id=10029` Telegram Monitoring завершился тайм-аутом Kaggle с нулём просмотренных источников. Последний VK import `ops_run.id=10091` обработал 12 записей, создал 0 событий, 9 закончил технической ошибкой проверки LLM.

## Root cause and mitigation

Автоматическую публикацию блокировал несовместимый со строгим OpenAI structured-output `enum` с фрагментами исходного текста в схеме резервного автора. Публикация одного поста вручную не восстанавливает конвейер. Исправление удаляет `enum` только из схемы 4o; ответ по-прежнему проверяется на дословную цитату из организаторского источника. Telegram Monitoring и VK import требуют отдельного восстановления после этого узкого фикса.

После релиза `34bb4a784` автоматический retry `joboutbox.id=90901` подтвердил второй блокер: модель вернула текст, который нельзя подтвердить общей текстовой подписью к афише, хотя событие было извлечено из изображения. Задание снова завершилось `strict 4o fallback returned invalid text`. Для таких принятых карточек с исходным организаторским текстом добавлен вариант публикации без повествовательного вступления: только уже сохранённые фактические поля карточки. При отсутствии исходного текста отказ остаётся.

## Regression contract and closure

- Trigger: изменение Telegram public writer, его схемы/fallback, очереди публикаций или мониторинга.
- Mandatory: тест strict fallback с цитатой, содержащей кавычки; проверка реального автоматического retry без ручного поста; readback поста и задания; отдельная проверка Telegram Monitoring и VK import с созданным событием.
- Release evidence: PR #710, SHA `34bb4a7844ab6e48fc9db0d4aee7c7fcfea5dd9c`, Fly health passed; live 4o schema canary passed. Automatic retry job 90901 failed on a separate grounding gate, prompting the factual-shell fix.
- [ ] Deploy и автоматический retry публикации.
- [ ] Восстановить Telegram Monitoring и VK import; подтвердить новые события, а не только запуск задач.

## 30 September follow-up

- PR #711 deployed at `cb6882a1fa406eb2d48bd4623d0ab06cb15e21c4`;
  automatic outbox job 90901 completed and published event 9408 as
  `https://t.me/kldevents/4406`. This verifies the publication stage only.
- At 18:42 UTC, Telegram Kaggle run
  `catchup-tg-monitoring-9f03bf6d0dd94e4d886e80abbb4a9907` still reported
  `alive`, source 24/57, 319 messages scanned after the host ops run 10029 had
  timed out. No second S22 scan was started.
- VK auto-import run 10091 created zero events; 9/12 inbox rows failed
  technically. Runtime logs show the opted-in Gemini 3.6 reserve was blocked
  by daily quota and Gemini 2.5 was also blocked, while the two-attempt cap
  prevented reaching the ordinary model chain. The four-per-hour 4o fallback
  budget was then exhausted. This is mechanical provider routing; venue and
  occurrence semantic gates remain required.
- Corrective patch lets local quota admission failures pass through the model
  chain without consuming the cap on actual provider sends. It requires live
  import and publication verification after deploy before incident closure.
