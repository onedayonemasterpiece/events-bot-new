# Global product decisions for the static personal announcements release

> Status: release-level product decisions originally accepted in the planning thread on 2026-07-11; retention and numerical quality thresholds still require their own evidence/approval.
> Documentation correction prepared 2026-09-05 in PR #587: personalization is a service function described in the User Agreement, not a separate user opt-in. The internal start-of-profile record is not an additional user action. This clarifies the existing research and the owner's correction; it is not a new legal conclusion or a deployed change.
> Integration with current voice/island work: [release-integration.md](release-integration.md).

## Working decisions

1. **Three recommendations means exactly three events in the email.** A visual hero may be one of those three; it is not a fourth event. The personal page may contain a larger ranked set.
2. **Calendar save and favorite are one durable saved-event state.** Exporting an ICS is a side effect/action, not a separate user-owned entity.
3. **Calendar/favorite never implies email consent.** Transactional follow mail and recommendation mail each retain their explicit purpose-specific consent/eligibility rules.
4. **Supabase/Postgres is the current durable user/profile and email control-plane owner.** YDB is analytics/history and comment-feedback sidecar only. The existing localization/data-flow gate can block new remote-profile rollout; a store migration requires an explicit ownership revision, not a silent parallel YDB profile.
5. **Profile linking is idempotent.** Login/logout cannot silently fork two durable profiles; reset/unlink/delete remain explicit operations.
6. **Static fallback is mandatory.** Auth, personalization, telemetry, YDB, search provider or email failure must not make public event pages unusable.
7. **All F1–F17 are required for the first public release/presentation.** Technical canaries may be staged, but no capability may be removed from the release scope or presented as a future beta merely to make a smaller release green. Historical stage labels are not current proof of implementation.
8. **Verified-email login supports both a code and a one-click link.** Both complete one Supabase Auth verification flow and recover the same identity.
9. **Personal pages use a forwardable public secret link.** Anyone who has the high-entropy URL may open it without authentication. The page is `noindex` and contains no raw profile/private identity data; the token remains revocable/rotatable under retention policy.
10. **Anonymous-to-authenticated profile linking is automatic and intelligent for an eligible existing profile.** No extra merge-confirmation or personalization-enable dialog is required after login. The merge is idempotent, deduplicates saved/action state, preserves explicit user actions over inferred interests, decays/conflict-checks inferred signals, shows the result and keeps reset/unlink available. It does not create an interest profile from mere prior browsing or grant unrelated purpose consents.
11. **Email provider routing is explicit.** SpaceWeb owns and retains the human/inbound mailbox. A read-only Yandex IMAP collector handles the production automation copy without changing `Seen`; Yandex Mail Trigger remains a direct technical canary. Postbox sends transactional mail. NotiSend sends personal recommendations and the narrow returning/repeated/fixed-test Auth route. Supabase remains the consent/suppression/admission authority; YDB remains analytics-only.
12. **NotiSend has a shared hard launch ceiling of 200 unique recipients.** Recommendation consent remains independently capped and fail-closed; provider limits do not authorize an extra recipient or a recommendation fallback to Postbox. Auth assigns over-capacity new recipients to Postbox before dispatch.
13. **Пасхалки — отдельный post-release campaign format, а не engagement North Star.** Они используют общий promo control plane, но требуют first-class egg subject/progress ledger вместо fake event ids. Первый пилот — конечная non-prize коллекция с добровольными подсказками/share, отдельным блоком в `Моё`, feedback/partner intake, admin kill switch и holdout по downstream event value; до product/legal/privacy/a11y/IP/anti-abuse acceptance production implementation запрещена.
14. **Персонализация — часть сервиса на условиях Пользовательского соглашения; отдельно включать её не требуется.** Не вводить обязательные кнопку, экран, checkbox, consent-token или команду «Включить персонализацию». Обычные предусмотренные действия учитываются по существующему договору функции без дополнительного подтверждения. Выбор «Для меня» — режим выдачи, а не обязательный предварительный допуск ко всей персонализации.

## 2026-09-05: персонализация по соглашению, без отдельного включения

Источники: ручные [требования персонализации](../static-site-pages/personalizaion/requirements.md), [исследование договорной модели](../static-site-pages/personalizaion/legal-repsonalization.md), [целевой blueprint, §9.3](../static-site-pages/personalizaion/personalization-to-be.md#93-activation-отдельные-consents-reset-и-delete), а также уточнение владельца в текущем обсуждении. Исходные исследования и ручные требования не переписываются задним числом.

**Было в неоднозначной формулировке:** отдельные слова `activation`, `activated profile`, `activation gate` и «включение» могли быть прочитаны как ещё один обязательный шаг пользователя перед работающими рекомендациями. Ранее устранённый термин `personalization-consent` также не должен возвращаться под новым названием.

**Стало:** персонализация описана в Пользовательском соглашении и работает в рамках обычного использования соответствующих функций сервиса. После предусмотренного функционального действия — например, изменения интересов или лайка — система сама учитывает его и выполняет допустимое начало/обновление профиля. Второй клик, переход в настройки, принятие отдельного персонализационного consent либо отдельный вызов `enable_personalization` не требуется. После явного скрытия сохраняется существующее undo-поведение.

### Техническое начало профиля не является отдельным пользовательским действием

В исходном исследовании момент начала серверного профиля связан с первым содержательным действием, предусмотренным соглашением, а не с произвольным посещением страницы. Это ограничение состава и момента обработки данных, **не интерфейсный opt-in**. Оно не отменяется уточнением владельца об отсутствии отдельного включателя.

Существующие `personalization_started_at`, `activation_action` и версии документов, если они нужны модели данных, описывают внутренний факт. Их запись выполняется в обработке обычного действия и не становится условием «сначала включите рекомендации». Не переименовывать БД ради терминологии и не создавать новый отдельный consent ledger для основной персонализации.

В существующем blueprint примеры таких действий — `interest_profile_change`, like, `personal_feed_enabled`, `not_interested` после undo-window. `personal_feed_enabled` — один из допустимых сценариев, а не обязательный предшественник остальных. До первых достаточных сигналов показывается общая/контекстная выдача; это cold start, не пользовательский отказ и не состояние, требующее отдельной кнопки включения. Простые scroll/impression/open, закрытие уведомления и сам факт login не превращаются в разрешение хранить любую историю.

Обычная поисковая фраза по-прежнему не равна постоянному интересу: «с ребёнком сегодня» не становится долгосрочной характеристикой только из-за отсутствия отдельного переключателя. Текущий запрос остаётся рабочим контекстом. Сохранение явных интересов и другие допустимые сигналы используют существующие доменные действия.

Информация о рекомендациях и ссылки на Пользовательское соглашение, Политику и Правила остаются доступны. Информационный toast не ждёт `Согласен` или `Понятно` для применения уже допустимого действия. Сброс рекомендаций и удаление данных остаются явными доступными командами, а не доказательством необходимости предварительного opt-in.

### Независимые цели не смешиваются с договором персонализации

Раздельность `product_analytics`, исследовательской фокус-группы, email, push и других communications остаётся по их собственным действующим контрактам. Отсутствие отдельного включателя основной персонализации не объединяет эти цели и не означает разрешения на произвольный raw clickstream. Необязательная аналитика не должна управлять доступностью основной функции.

Отказ от optional analytics не блокирует лайк, скрытие или допустимое обновление рекомендаций. Согласие на аналитику не создаёт профиль задним числом из накопленного browsing history. Не копировать эту семантику в несколько конкурирующих документов: голосовой поиск, release integration и постановка островов используют настоящий раздел как общее определение терминов.

Автоматическое связывание при login работает с допустимым существующим профилем и проверенной identity без новой церемонии включения. Требования размещения, безопасности, минимизации и data-flow сохраняются; договорная модель проекта не объявляется доказанной юридической пригодностью любого инфраструктурного размещения.

### Проверки отсутствия отдельного включения

Это уточнение ожидаемого поведения в существующем реестре тестов, не новый test framework и не выполненные проверки:

- пользователь не нажимал «Включить персонализацию» и имеет `product_analytics=off` → ставит допустимый лайк/меняет интересы → действие учитывается без дополнительной формы; optional analytics writes остаются нулевыми;
- принятие обычного доменного действия атомарно и идемпотентно фиксирует необходимое техническое начало профиля; не нужен второй запрос включения и повтор не создаёт второй профиль;
- открытие «Для меня» не является обязательным шагом перед персонализацией других допустимых поверхностей; cold start не показывает блокирующий opt-in;
- информационное уведомление можно не закрывать; это не задерживает работу рекомендаций и не собирает новое согласие;
- reset/delete и отмена pending hide сохраняют существующий смысл; поздний ответ не восстанавливает удалённый профиль, а отсутствие включателя не разрешает бессрочное хранение raw истории.

## Consequences

- Email-only users are authenticated identities, not a parallel anonymous-subscription account model.
- The code and link cannot create two accounts or consume each other incorrectly; replay/attempt/TTL limits apply to the shared verification transaction.
- `noindex` is discovery control, not access control. A forwarded personal-page URL intentionally grants read access to its holder.
- Personal-page artifacts must exclude email, account id, raw/inferred profile internals, hidden scores and sensitive history. The explicit forwardable personal-page contract does not make private voice conversation history publicly readable.
- Login links an eligible existing anonymous profile without another merge or enablement ceremony. It does not silently persist pre-profile behavior outside the accepted contract; purpose-specific permissions are not inferred from login or linking.
- All ten release workstreams in the original readiness checklist remain obligations until explicitly reconciled by current release evidence. New voice/island work does not replace them with a UI-only acceptance claim.
- The public recommendation-email launch can reach fewer than 200 users during canary or when provider seed/service contacts reduce usable plan capacity, but it can never exceed the shared admitted ceiling without a later explicit product and infrastructure decision.
- A strong action's primary acknowledgement, asynchronous analytics delivery and later profile materialization are distinct. Failure of the analytics sidecar does not undo the primary action; a proxy response does not by itself prove either downstream outcome.
- Calendar chronology, thematic eligibility, global exact hide/undo, query-over-profile priority and non-jumping visible content remain owned by the personalization blueprint and apply to the voice answer timeline as well.

## Product decisions still required

### Retention

Product/legal owners must approve retention for current profile state, contractual-start evidence and independent purpose consents, raw telemetry, delivery events, suppressions and personal pages. Suppression evidence must outlive normal profile deletion enough to prevent accidental resend. Conversation history and browser outbox lifetime are separate from profile horizons and analytical session definitions; no feature silently overwrites all of them with one TTL.

### Event-quality stability window

Define the required canary duration and numerical “almost no defects” thresholds for duplicates, wrong location and wrong date/time. Smart Update remains prevention owner; release monitoring supplies the evidence. Historical checkmarks do not establish the current stability window.
