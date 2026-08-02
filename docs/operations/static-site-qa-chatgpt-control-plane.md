# Запуск автотестов статического сайта из ChatGPT

> **Статус:** стратегический, канонический companion к
> [`static-site-autotest-strategy.md`](static-site-autotest-strategy.md).
> **Цель:** любой реализованный сценарий можно безопасно запустить и разобрать из
> ChatGPT без запуска Codex и без изменения кода.
> **Реестр сценариев:**
> [`../testing/static-site-autotest-scenarios.v1.yml`](../testing/static-site-autotest-scenarios.v1.yml).

## 1. Почему одного `workflow_dispatch` недостаточно

`workflow_dispatch` остаётся удобным входом из GitHub UI, но наличие такого
workflow само по себе не гарантирует, что текущая ChatGPT GitHub-интеграция умеет
создать новый run. Нельзя строить operational contract на возможностях одного
конкретного клиента или обещать запуск, когда connector умеет только читать,
комментировать или перезапускать уже существующие jobs.

Гарантированный project-level control plane должен использовать стандартную
операцию, доступную как человеку, так и GitHub connector: **комментарий в одном
каноническом control issue**. GitHub Actions принимает этот комментарий, строго
валидирует его и запускает тот же reusable scenario runner, что и ручной
`workflow_dispatch`.

До реализации command gateway текущий статус честный:

- ChatGPT может анализировать PR/runs/jobs и безопасные artifacts;
- ChatGPT может комментировать issue/PR и, при наличии разрешения connector,
  повторять failed jobs;
- новый static-site QA run из ChatGPT **не считается подготовленным**, если для
  него существует только GitHub UI `workflow_dispatch`.

## 2. Целевая схема

```text
ChatGPT or human
  -> comment in canonical Static Site QA Control issue
  -> issue_comment workflow
  -> permission + command + scenario-registry validation
  -> reusable browser/android/ios workflow
  -> protected Environment approval where required
  -> sanitized evidence artifact + job summary
  -> bot comment with run URL, exact SHA, status and artifact name
  -> ChatGPT reads run/jobs/artifact and performs analysis
```

Это control plane, а не новый test framework. Бизнес-сценарий и platform adapter
остаются теми же, что при ручном запуске.

## 3. Канонический command format

Команда однострочная и не содержит secrets:

```text
/qa run scenario=focus.otp.browser_tab platform=android target_url=https://kenigevents.ru/... expected_repo_sha=<40-hex> mode=blocking
```

Допустимые операции первой версии:

```text
/qa run scenario=<registry-id> platform=browser|android|ios|all target_url=<https-url> expected_repo_sha=<40-hex> mode=blocking|advisory
/qa status run_id=<github-run-id>
/qa rerun-failed run_id=<github-run-id>
```

`/qa rerun-failed` разрешён только для существующего run и сохраняет его exact
scenario/target/SHA contract. Для real OTP он не должен автоматически повторять
job после возможного side effect; повтор разрешается только если evidence
однозначно доказывает, что OTP issue ещё не произошёл.

## 4. Безопасность command gateway

Workflow обязан fail-closed проверить всё до checkout/test side effects:

1. комментарий находится в exact canonical control issue;
2. issue имеет служебный label, например `static-site-qa-control`;
3. actor имеет repository permission `write`, `maintain` или `admin`;
4. command parser не использует `eval`, shell interpolation или произвольные
   workflow inputs;
5. `scenario` существует в checked-in registry;
6. requested platform разрешена сценарием;
7. requested mode не ослабляет registry blocking policy;
8. `target_url` проходит scenario-specific origin/path allowlist;
9. `expected_repo_sha` — exact 40-character SHA и доступен из trusted ref;
10. branch/ref не задаётся комментарием: runner использует trusted default branch
    либо exact allowlisted release SHA;
11. fresh-user mode, destructive reset, paid device cloud и production writes
    запрещены обычной командой и требуют отдельного protected input/approval;
12. real OTP job всё равно входит в Environment `external-e2e` и общую
    concurrency-группу;
13. comment body, target и outputs очищаются от bearer URLs и PII до публикации.

`pull_request_target` с исполнением кода из fork запрещён. Если command gateway
работает из issue/PR comment, он checkout только trusted repository code и не
исполняет изменённый код внешнего PR до отдельной проверки exact SHA/policy.

## 5. Управление стоимостью

Default command без явной platform запускает только `browser`.

- `android` и `ios` разрешены, только если registry содержит эту platform;
- `all` для дорогих L2 jobs требует release/blocking scenario либо явного
  owner-approved override;
- full catalog на эмуляторах запрещён;
- advisory run может завершать текущий диалог статусом `STARTED_BACKGROUND`, но
  workflow обязан позднее опубликовать terminal result;
- real OTP не запускается schedule/nightly и использует fixed identity;
- duplicate command с тем же scenario/platform/target/SHA в активной concurrency
  группе возвращает ссылку на существующий run вместо второго запуска.

## 6. Ответы control plane

После принятия команды workflow-комментарий должен содержать:

- `ACCEPTED` или `REJECTED`;
- normalized scenario/platform/mode;
- exact repository SHA;
- sanitized target origin/path;
- GitHub run URL/ID;
- ожидаемое имя artifact;
- blocking/advisory policy;
- причину rejection без secrets.

После завершения отдельный terminal comment или check summary содержит:

- `PASS`, `FAIL` или `BLOCKED`;
- first failed step/failure domain;
- artifact name и retention;
- redaction result;
- для background run — связь с исходным `STARTED_BACKGROUND` request.

Комментарий не заменяет artifact. `qa-summary.json` остаётся machine-readable
source for ChatGPT analysis.

## 7. OTP-specific contract

Первый сценарий, подключаемый к control plane, —
`focus.otp.browser_tab`.

- `browser`, `android`, `ios` и `all` используют один shared journey;
- `all` выполняется последовательно при одном mailbox;
- target — опубликованный HTTPS onboarding URL;
- expected full repo SHA обязателен;
- запуск требует Environment review;
- email/OTP/IMAP secret никогда не попадают в issue comment;
- video/trace/HAR запрещены;
- command acknowledgement не означает OTP PASS;
- отсутствие dedicated mailbox/reconciliation/hook readiness даёт
  `BLOCKED_INFRASTRUCTURE`, а не автоматический обход.

## 8. Implementation boundary

Command gateway должен быть добавлен вместе с первым реальным Android/iOS OTP
vertical slice либо отдельным маленьким PR до его приёмки. Не откладывать его до
общего all-pages framework: иначе мобильный сценарий существует, но исходное
требование «запускать из ChatGPT без Codex» остаётся незакрытым.

Минимальные файлы целевого решения:

```text
.github/workflows/static-site-qa-command.yml
.github/workflows/external-focus-email-otp.yml   # reusable/manual entry
scripts or site/e2e control command parser/tests
qa control issue with fixed ID/label
```

Точный issue ID появляется только в implementation PR/setup receipt и затем
фиксируется в workflow/документации. Не создавать control issue раньше
работающего listener, чтобы не формировать ложную кнопку запуска.

## 9. Acceptance

Control plane считается готовым только если:

- [ ] ChatGPT connector публикует safe `/qa run` comment без Codex;
- [ ] authorised request создаёт один exact workflow run;
- [ ] unauthorised/malformed request rejected до side effects;
- [ ] browser smoke можно запустить и получить terminal artifact;
- [ ] protected OTP request доходит до Environment approval, а не обходит его;
- [ ] duplicate active request deduplicated;
- [ ] run acknowledgement и terminal result возвращаются в control issue;
- [ ] ChatGPT может по run ID найти jobs/artifact и сформировать независимый
      отчёт;
- [ ] docs/registry/agent instructions указывают этот control plane как
      обязательный способ connector-independent запуска.
