# Repository Workflow

Канонический workflow для параллельной разработки, production-fix, deploy и безопасной работы без потери незавершённого кода.

## Почему это нужно

- В этом репозитории параллельно идут несколько потоков работы.
- Если вести feature-разработку в грязном `main` checkout и потом пытаться быстро вытащить отдельный fix в prod, легко получить один из инцидентов:
  - незакоммиченная работа “исчезает” после branch-switch/reset/rebase;
  - prod-fix собирается не от той базы и случайно откатывает чужие изменения;
  - deploy блокируется, потому что единственный локальный checkout смешал несколько задач;
  - long-running feature живёт только локально и становится невоспроизводимой.

## Внешняя база

- Git `worktree` позволяет держать несколько рабочих деревьев для одного репозитория и одновременно иметь checkout нескольких веток: [git-worktree](https://git-scm.com/docs/git-worktree).
- Git `fetch` обновляет remote-tracking refs и должен выполняться перед решением, от какой базы делать fix/merge: [git-fetch](https://git-scm.com/docs/git-fetch).
- Git `switch -c` создаёт новую ветку транзакционно перед переключением; если переключение небезопасно, ветка не будет частично “перекинута”: [git-switch](https://git-scm.com/docs/git-switch).
- Git `reflog` хранит локальную историю движений ссылок и нужен как страховка при аварийном восстановлении: [git-reflog](https://git-scm.com/docs/git-reflog).
- GitHub рекомендует изолировать работу по веткам, а protected branches позволяют требовать статус-чеков и актуальную базу перед merge: [About branches](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/about-branches), [About protected branches](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/about-protected-branches).

## Базовая модель

- `origin/main` — единственный production baseline.
- Один поток работы = одна именованная ветка + один отдельный worktree.
- Обычная работа по настройкам, UX, расписаниям, отчётам, документации и
  плановым исправлениям начинается в `feature/<topic>` или, если это короткий
  неаварийный багфикс, в `fix/<topic>`.
- `hotfix/<topic>` используется только для активного production incident /
  emergency-fix, где нужна быстрая доставка исправления в prod.
- Локальный checkout не должен одновременно быть:
  - местом для многодневной feature-разработки;
  - площадкой для emergency prod-fix;
  - staging-зоной для нескольких несвязанных задач.

## Обязательные правила

### 1. Главный checkout не используется как свалка

- Локальный `main` checkout должен быть либо clean, либо быстро возвращаемым к clean-state.
- Если в текущем checkout уже есть незавершённая работа, её нельзя оставлять “без адреса” на `main`.

### 2. Любая незавершённая работа сначала получает имя

- Если в dirty checkout уже лежит новая работа, первым действием нужно:
  - создать для неё именованную ветку;
  - сделать WIP commit(ы), если работа больше не укладывается в минутный локальный эксперимент;
  - запушить эту ветку в `origin`, если работа длится дольше одной сессии или становится базой для другой задачи.
- Repo policy: не считать `stash` единственным местом хранения многодневной работы. Для долгоживущего состояния нужны commit + branch + origin.

### 3. Новый поток работы всегда начинается в отдельном worktree

- Для новой задачи создаётся linked worktree.
- Для плановой задачи базой по умолчанию служит актуальный `origin/main` и
  branch `feature/<topic>`.
- Для production/hotfix-задачи базой по умолчанию служит актуальный `origin/main`
  и короткая branch `hotfix/<topic>`.
- Для feature-задачи базой служит либо актуальный `origin/main`, либо уже существующая **запушенная** integration branch, если задача реально зависит от ещё не влитой работы.
- Если работа началась как incident/hotfix, но после mitigation превратилась в
  плановую настройку или улучшение, продолжение нужно переносить в новый
  `feature/<topic>` worktree от свежего `origin/main`, а не переиспользовать
  старую hotfix-ветку.

### Размер рабочих копий и завершение потока

- Продолжение того же потока использует его существующий worktree: не создавай
  новую копию на каждое замечание ревью.
- Перед новой копией проверяй свободное место и размер checkout: tracked SQLite,
  артефакты и зависимости могут стоить намного больше самих Git refs. Для
  source-only работы используй подходящий sparse checkout; проверь, что он не
  подтянул крупные файлы корня (cone mode сам по себе этого не гарантирует).
- После интеграции работника проверь чистоту и достижимость либо эквивалентность
  его изменений в запушенной ветке. Сохрани уникальные доказательства проверок,
  затем удали завершённый worktree через `git worktree remove`.
- Не удаляй активные shared dependencies, пользовательские записи/очереди/Auth,
  уникальные артефакты или DB snapshots как «кэш» без отдельного основания.
  Удаление названий веток почти не освобождает место; историю не чистим ради ГБ.

### 4. Нельзя делать prod-fix от локальной “призрачной” базы

- Если продовый fix зависит от незамёрженной feature-работы, недостаточно “взять файлы из грязного checkout”.
- Сначала нужно сделать базу воспроизводимой:
  - либо довести нужную base branch до push в `origin`;
  - либо backport-ить минимальный fix прямо на `origin/main`, если это возможно без той feature.
- Только после этого можно создавать отдельный fix-branch/worktree от явной базы.

### 5. Deploy только из clean worktree

- Deployable checkout обязан быть clean.
- Deployable branch обязан иметь понятную базу:
  - обычно `origin/main`;
  - для emergency hotfix — короткая ветка от актуального `origin/main`, уже запушенная в `origin`.
- Dirty feature-worktree не считается нормальной площадкой для deploy.

## Практический workflow

### Сценарий A: начинается новая обычная feature

1. `git fetch origin --prune`
2. Создать linked worktree от `origin/main`
3. В этом worktree создать branch вида `feature/<topic>`
4. Работать только в нём
5. Коммитить и пушить по мере появления durable-state
6. Вливать в `main` через PR

Пример:

```bash
git fetch origin --prune
git worktree add ../events-bot-new_feature-x -b feature/feature-x origin/main
```

### Сценарий B: текущий checkout уже грязный, но это ценная работа

1. Не делать reset и не пытаться “быстро освободить main”
2. Сразу привязать работу к именованной ветке
3. Зафиксировать хотя бы WIP snapshot
4. Запушить ветку в `origin`
5. Только после этого создавать новый worktree под следующую задачу

Безопасный минимум:

```bash
git switch -c feature/<topic>
git add <relevant-files>
git commit -m "WIP: <topic>"
git push -u origin feature/<topic>
```

### Сценарий C: нужен production fix, пока параллельно живут другие feature

1. `git fetch origin --prune`
2. Проверить, зависит ли fix только от `origin/main`
3. Если да:
   - создать linked worktree от `origin/main`
   - сделать там короткую ветку `hotfix/<topic>`
4. Если нет:
   - сначала довести зависимую base branch до `origin`
   - затем создать отдельный fix/worktree от этой **запушенной** base branch
   - отдельно решить, можно ли это мержить в `main` целиком или нужен backport
5. Прогнать таргетные тесты именно в clean worktree
6. Commit/push/PR
7. Deploy только из этого clean worktree или после merge в `main`

### Сценарий C2: после incident нужна плановая донастройка

1. Считать emergency/hotfix поток завершённым после deploy/mitigation и
   фиксации evidence.
2. `git fetch origin --prune`
3. Создать новый linked worktree от актуального `origin/main`
4. Создать branch вида `feature/<topic>`
5. Переносить только нужные изменения через обычный diff/commit, без
   продолжения работы в старом `hotfix/*`

### Сценарий D: задача уже готова и закрыта

1. Убедиться, что ветка запушена и слита/больше не нужна
2. Удалить linked worktree через `git worktree remove`
3. При необходимости почистить stale metadata через `git worktree prune`

## Prod-safe правила для этого репозитория

- Любой deploy-значимый fix должен быть достижим из `origin/main`.
- Нельзя закрывать инцидент, если fix остался только:
  - в локальном worktree;
  - в незапушенной ветке;
  - в side-ветке без плана back-merge в `main`.
- Перед production deploy обязательно:
  - `git fetch origin --prune`
  - `git branch --show-current`
  - `git status --short`
  - сверка diff именно по релевантным файлам
  - подтверждение, что deploy checkout clean
  - подтверждение, что база актуальна относительно `origin/main`

## Recovery

- Если что-то “пропало”, сначала смотреть `git reflog`, а не делать новые рискованные операции.
- Если случайно удалён linked worktree руками, административные следы можно безопасно прибрать через `git worktree prune`; штатное удаление — `git worktree remove`.
- Если работа существует только локально и не привязана к ветке, это уже инцидент workflow, а не “обычная рабочая грязь”.

## Repo Contract

- По умолчанию:
  - одна задача = одна ветка;
  - одна активная ветка = один worktree;
  - deploy/fix work не делается в том же checkout, где лежит незавершённая параллельная feature.
- Если агент видит грязный checkout и при этом пользователь просит довести конкретную задачу до prod, агент должен:
  - не останавливаться на констатации проблемы;
  - сначала выяснить, какая работа уже лежит в dirty checkout и привязана ли она к branch/origin;
  - затем выбрать безопасную изоляцию: named branch, WIP push, linked worktree, hotfix branch;
  - только после этого продолжать prod rollout.
