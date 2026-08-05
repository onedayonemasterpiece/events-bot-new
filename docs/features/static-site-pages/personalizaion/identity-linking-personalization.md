# Identity linking, logout и конфликтная склейка персонализации

> **Статус:** предварительный обязательный методологический contract для P13N-03/P13N-04 и тестов P13N-06.  
> **Дата среза:** 2026-08-04.  
> **Цель:** не потерять анонимные сигналы при авторизации, не смешивать разные аккаунты, не превращать разные устройства в разные органические личности и не дать старому локальному профилю перезаписать уже существующий durable account profile.

## 1. Базовые сущности

| Сущность | Смысл | Где живёт |
|---|---|---|
| `device_instance` | Текущий браузер/устройство, доказанный credential | browser + approved primary store proof |
| `anonymous_subject` | Анонимный профиль до авторизации | локальный state, затем primary store только после activation/gate |
| `account_subject` | Авторизованный пользователь email/Yandex/Supabase Auth | primary store |
| `activation_epoch` | Версия старта/сброса персонализации | browser + primary store |
| `profile_revision` | Immutable materialized snapshot/projection | primary store + browser projection |
| `identity_link` | Событие склейки anonymous→account или auth method→account | primary store audit |
| `merge_decision` | Результат сравнения профилей | primary store audit + sanitized evidence |

Browser не имеет права сам назначать `account_subject` или переносить чужой
профиль. Он передаёт только idempotent link request, а server определяет
ownership из Auth/session/device proof.

## 2. Главные инварианты

1. Авторизация сама по себе не является activation event.
2. Logout не удаляет durable profile; reset/delete — отдельные команды.
3. Account switch не наследует профиль предыдущего account.
4. Authenticated explicit state выигрывает конфликт с anonymous/device state.
5. Exact hides объединяются как union, кроме более позднего explicit restore.
6. Raw browsing history не переносится при link; переносится compact current
   state, bounded strong actions и compatible projection evidence.
7. Долгосрочные интересы не становятся радикально разными по устройствам без
   явного evidence; device-specific поведение остаётся session/short overlay.
8. Если anonymous profile сильно конфликтует с существующим account profile,
   он не перезаписывает account автоматически.
9. Pending/ambiguous outbox старого subject не replay-ится в новый subject.
10. Любая склейка воспроизводима: есть merge input hashes, conflict score,
    decision code, resulting revision и rollback/reconcile path.

## 3. Profile conflict score

Перед merge server строит compact comparison:

```text
conflict_score = max(
  exact_state_conflict_rate,
  top_facet_opposition_score,
  constraint_opposition_score,
  rank_overlap_loss_score,
  account_confidence_advantage
)
```

Стартовые компоненты:

| Компонент | Пример |
|---|---|
| `exact_state_conflict_rate` | anonymous liked event, account hidden same event/family |
| `top_facet_opposition_score` | anonymous сильный rock, account устойчиво suppress rock |
| `constraint_opposition_score` | anonymous дорогие вечерние события, account устойчиво free/daytime |
| `rank_overlap_loss_score` | top-30 account сильно ухудшается после naïve merge |
| `account_confidence_advantage` | account имеет long/mid evidence, anonymous только short/session |

Числа не фиксируются из воздуха: thresholds калибруются offline на fixtures и
longitudinal E2E. До этого статусы:

- `low_conflict` — безопасная склейка;
- `medium_conflict` — account wins + anonymous как low-weight short overlay;
- `high_conflict` — account wins, anonymous помещается в quarantine/context
  suggestion, нужен явный пользовательский выбор для полноценного учёта.

## 4. Merge policies

### 4.1. Anonymous → новый account без durable profile

```text
decision = import_anonymous_current_state
```

Перенести:

- activation evidence, если legal/localization gate допускает;
- exact states;
- likes/saves/current preferences;
- bounded recent strong actions;
- compact projection seed.

Не переносить:

- raw page views;
- raw clickstream;
- session id;
- campaign implicit evidence;
- sensitive facets;
- expired events.

### 4.2. Anonymous → существующий account, low conflict

```text
decision = merge_into_account_revision
```

Правила:

- authenticated exact state wins on conflicts;
- exact hides union;
- likes/saves merge idempotently;
- short evidence may increase compatible facets;
- long horizon не меняется от anonymous short/session сразу;
- materializer выпускает новую revision с provenance.

### 4.3. Anonymous → существующий account, medium/high conflict

```text
decision = account_wins_with_device_overlay | quarantine_anonymous_context
```

Поведение:

- account profile остаётся основным;
- anonymous interests не перезаписывают mid/long facets;
- compatible exact actions may merge;
- incompatible facets попадают в temporary device/session overlay с short TTL;
- UI может позже предложить: `Учитывать интересы с этого устройства?`, но это
  отдельный product/legal flow и не P13N-00/01;
- model report показывает conflict evidence и прогноз влияния.

Пример: пользователь долго ходил анонимно по детским событиям на семейном
компьютере, а затем вошёл в личный account, где устойчивый интерес — научпоп и
выставки. Семейный anonymous context не должен радикально переписать account.
Он может быть предложен как контекст `семейные выходные`, но не как новый
long-term taste.

### 4.4. Один account на разных устройствах

После login каждое устройство:

1. очищает несовместимую anonymous projection;
2. получает account projection через ETag/refresh;
3. применяет только local session overlay сверху;
4. отправляет strong actions в тот же account subject;
5. не создаёт отдельный long-term профиль устройства.

Если устройство имеет специфическое поведение, materializer видит его как
contextual/session evidence, а не как новую органическую личность. Отдельные
персоны/режимы `семья`, `работа`, `турист` возможны позже только как явная
product feature.

### 4.5. Logout

Logout выполняет:

- stop remote flush for current account;
- clear account projection and account-specific local overlay;
- rotate `device_session`;
- preserve only non-personal interface settings;
- keep a local `logout_epoch` barrier;
- leave durable account profile intact server-side.

После logout пользователь может продолжить anonymous browsing, но это новый
anonymous context. Он не должен содержать account profile и не должен потом
случайно перейти в другой account.

### 4.6. Login в другой account после logout

```text
previous_account_epoch != new_account_epoch
```

Правила:

- не переносить projection/overlay предыдущего account;
- pending outbox старого account не replay-ить;
- local anonymous actions, совершённые **после logout_epoch**, могут стать
  кандидатами на link к новому account;
- если logout не успел получить ACK, следующий login сначала делает reconcile
  старого epoch без записи в новый account;
- UI не должен показывать старые лайки/скрытия как состояние нового account.

### 4.7. Email и Yandex как методы одного account

Если Supabase/Auth или другой утверждённый identity owner доказывает, что email и
Yandex принадлежат одному subject, добавляется `auth_method_link`, а не новый
profile merge.

Если появляются два account_subject, нужна отдельная account-merge процедура:

- fresh re-auth;
- explicit owner action;
- conflict score;
- audit;
- rollback/purge rules.

Автоматически сливать два существующих account нельзя.

## 5. State machine

```text
anonymous_inactive
  → anonymous_active
  → login_started
  → authenticated_pending_link
  → link_evaluating_conflict
  → linked_low_conflict | linked_account_wins | linked_quarantined
  → account_active
  → logout_pending
  → anonymous_after_logout
  → login_different_account_guarded
```

Любой network/backend fault оставляет state в последнем безопасном состоянии:

- UI не показывает `linked`, пока нет ACK;
- actions сохраняются в bounded outbox только с subject/epoch binding;
- ambiguous link result требует `/state` reconcile;
- repeated link is idempotent.

## 6. E2E сценарии identity/link/logout

### I0. Anonymous → new account

```gherkin
Scenario: Новый пользователь забирает анонимные действия после авторизации
  Given anonymous profile has likes, hides and saves
  When user logs in with email or Yandex for the first time
  Then account profile contains compatible explicit current state
  And raw browsing history is not copied
  And new projection revision is issued
  And served pages reflect linked state after refresh
```

### I1. Anonymous → existing compatible account

```gherkin
Scenario: Совместимый anonymous profile усиливает account without overwriting
  Given account has mid evidence for science
  And anonymous device has recent likes for science lectures
  When link is accepted
  Then science short/mid confidence increases
  And exact account hides remain hidden
  And no long-term facet is created from anonymous-only short evidence
```

### I2. Anonymous → existing conflicting account

```gherkin
Scenario: Конфликтный anonymous profile не перезаписывает account
  Given account has long evidence for exhibitions and science
  And anonymous device has strong short evidence for loud concerts
  When user logs in
  Then merge decision is account_wins_with_device_overlay or quarantine
  And account long facets remain unchanged
  And concert interest may appear only as short device overlay
  And report contains conflict score and decision code
```

### I3. Same account on two devices

```gherkin
Scenario: Два устройства получают одну durable projection и разные session overlays
  Given device A and device B are logged into the same account
  When device A likes science and device B browses family events
  Then both strong actions are accepted under one account subject
  And materializer coalesces them into one revision
  And device-specific behavior stays session/short until repeated evidence
```

### I4. Logout then login different account

```gherkin
Scenario: Новый account не наследует state старого account после logout
  Given browser is logged into account A with hidden events
  When user logs out and logs into account B
  Then account A projection and overlay are absent from UI
  And pending account A outbox is not replayed into account B
  And account B receives only its own server projection
```

### I5. Pending outbox during logout

```gherkin
Scenario: Ambiguous pending action cannot cross identity boundary
  Given account A has a pending idempotent action in outbox
  When user logs out before ACK and later logs into account B
  Then action remains bound to account A epoch
  And account B receives no replay
  And reconcile of account A uses idempotency key or expires safely
```

### I6. Reset before link

```gherkin
Scenario: Reset epoch blocks stale anonymous merge
  Given anonymous profile has long local history
  When user resets personalization before login
  And then logs in
  Then pre-reset anonymous evidence is not merged
  And only post-reset actions can be candidates for link
```

## 7. Evidence artifacts

```text
identity-link-report.json
merge-input-hashes.json
merge-decision.json
conflict-score.json
subject-epoch-timeline.ndjson
outbox-epoch-binding.ndjson
projection-before-after.json
account-switch-dom-proof.json
```

`merge-decision.json` содержит:

```json
{
  "schema_version": "p13n-merge-decision-v1",
  "decision": "account_wins_with_device_overlay",
  "conflict_level": "medium",
  "anonymous_evidence_age_days": 21,
  "account_profile_revision": 12,
  "result_profile_revision": 13,
  "merged_exact_states": 8,
  "quarantined_facets": 3,
  "raw_history_copied": false
}
```

## 8. Product UI guidance

По умолчанию link должен быть low-friction. Но при high conflict нельзя молча
перезаписать account. Допустимые UX-варианты для будущего research:

- тихо сохранить account и учитывать device context только в текущей сессии;
- показать неблокирующее сообщение: `Похоже, на этом устройстве выбирали другие
  события. Хотите учесть это в рекомендациях?`;
- предложить режим/контекст `семья`, `работа`, `турист`, но только после
  отдельного product/legal design.

На MVP достаточно безопасного `account wins + device short overlay`, без нового
обязательного вопроса.

## 9. Hard NO-GO

- login different account inherits previous account state;
- old account outbox replays into new account;
- anonymous conflicting profile overwrites account mid/long facets silently;
- logout deletes durable account profile;
- authorization alone starts personalization;
- raw browsing history copied during link;
- sensitive facets merged;
- account/device conflict has no audit evidence;
- merge result cannot be reproduced from sanitized hashes;
- two devices create two durable personalities for one account without explicit
  user-facing profile/role model.

## 10. Open decisions

- exact threshold values for low/medium/high conflict;
- whether to expose high-conflict prompt in focus cohort;
- whether future product needs explicit multi-context profiles;
- inactivity retention after 365 days;
- final primary-store/localization contour.

These decisions do not block Wave 0–2, but block production account-link rollout.
