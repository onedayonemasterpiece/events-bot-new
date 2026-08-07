# Penpot review flow for the design system

> **Status:** operational delivery published; product/design review of the current visual baseline remains a human sign-off step.
> **Plugin:** LoveKGD Runtime Review — exact AS-IS prototype 003.
> **Canonical UI source:** `events-bot-new` Git runtime, not Penpot.

## Install the reviewed plugin revision

Open a Penpot file, open the Plugin Manager and install this immutable manifest URL:

```text
https://cdn.jsdelivr.net/gh/onedayonemasterpiece/lovekgd-design-system@2d917d1e39dbcac5ee9e88bcc6dd9f988e4b688c/prototypes/penpot-as-is-runtime-003/dist/manifest.json
```

Use the immutable URL for a sign-off session. The moving live URL is reserved for routine currentness checks:

```text
https://cdn.jsdelivr.net/gh/onedayonemasterpiece/lovekgd-design-system@penpot-as-is-live/prototypes/penpot-as-is-runtime-003/dist/manifest.json
```

The published manifest requests only:

- `content:read`;
- `content:write`;
- `comment:read`.

It does not request comment write access and does not send comments to GitHub or a product database.

## First synchronization

1. Open the plugin in the target Penpot file.
2. Press **«Проверить актуальность»**.
3. Confirm that the panel shows the exact catalog revision, catalog SHA, runtime SHA, page count, artifact count and downloaded bytes.
4. Review the proposed plan. A new file should report nine pages to create and the current artifact set to add.
5. Press **«Синхронизировать именованные страницы»**.
6. After completion, reopen the plugin and run the check once more. The expected steady state is `CURRENT` with no pending managed changes.

The initial catalog contains:

- exact runtime sections for foundations, actions, fields, states and registry;
- real product-component implementations rendered by the existing Astro lab;
- actual built public page archetypes for home, today, tomorrow, weekend, popular, collections, festivals and an event detail page;
- desktop and mobile evidence;
- no synthetic “NOT APPROVED DESIGN” load fixtures in the managed design-system pages.

## Page responsibilities

| Penpot page | Responsibility |
|---|---|
| `00 — System map` | source/revision orientation and runtime catalog intro |
| `20 — Foundations` | observed tokens, typography, spacing, shape, elevation and brand lockup |
| `30 — Core UI` | primitive actions, fields and state surfaces |
| `40 — Announcements components` | product components rendered from their real Astro implementations |
| `60 — Page archetypes` | full public-route evidence at desktop and mobile widths |
| `70 — AS-IS registry` | observed component/version/status registry |
| `80 — Candidate review` | proposed alternatives; never treated as current runtime |
| `90 — Review archive` | superseded visual states retained because they have decision evidence |
| `99 — Technical tests` | prior load/smoke fixtures and other non-product evidence |

## Commenting contract

Use native Penpot comments on the managed board that contains the problem. A useful comment states the observed issue and expected decision without inventing implementation details, for example:

```text
На mobile карточке дата конкурирует с названием. Нужны два варианта и проверка на длинном русском заголовке; current AS-IS не менять до выбора.
```

Do not place one comment over a large canvas when it concerns one component or route. The board attachment is part of the deterministic provenance.

### Build an implementation prompt

1. Leave the relevant comment threads unresolved.
2. Select one managed board to scope the result, or leave no managed board selected to collect all unresolved comments in the current mirror.
3. Press **«Собрать промпт по комментариям»**.
4. Copy the generated prompt.

The generated prompt contains exact Git and Penpot context. It explicitly requires a candidate preview separate from AS-IS and prohibits production promotion without owner sign-off.

## Update behavior

When `events-bot-new` changes, GitHub Actions rebuilds the Astro site, captures the same selectors/routes at the new exact SHA and publishes a new catalog.

The plugin compares each managed `elementId`, content hash, target page and target slot:

| Situation | Action |
|---|---|
| same content and metadata | `noop` |
| same content, changed metadata | metadata refresh |
| changed content, no comments | replace current board |
| changed content, comments present | archive old board as review evidence, add new current board |
| target page/slot changed, no comments | relocate/move |
| target page/slot changed, comments present | preserve old review snapshot, create current board in the new target |
| element removed, no comments | remove managed board |
| element removed, comments present | preserve review evidence |

No foreign or manually created board is deleted by the managed sync.

## Failure semantics

The plugin reports one of four states:

- `CHECKING` — catalog and artifacts are being loaded/verified;
- `CURRENT` — the managed Penpot mirror matches the catalog;
- `STALE` — a deterministic change plan exists;
- `SYNC FAILED` — no successful current switch was made.

A sync must fail closed when:

- the live ref or catalog cannot be downloaded;
- catalog schema or source SHA is invalid;
- an artifact hash or byte length differs;
- a Penpot media upload fails;
- staging cannot be completed;
- post-commit verification finds missing, duplicate, misplaced or wrong-hash current boards.

Staged boards are removed on failure. Existing current boards are restored when commit verification fails.

## Candidate-to-release flow

```text
AS-IS board + native comments
→ deterministic implementation prompt
→ candidate implementation in a separate Git branch/lab route
→ candidate screenshot on `80 — Candidate review`
→ product-owner decision
→ accepted code and tests in events-bot-new
→ exact runtime capture at the accepted SHA
→ managed AS-IS refresh
→ prior commented state retained on `90 — Review archive`
```

A comment resolution is evidence that the discussion is closed; it is not by itself proof that production contains the accepted solution. Production evidence remains the exact `events-bot-new` commit and its runtime capture.

## Acceptance evidence for prototype 003

- workflow run: `31155694854` — all capture, validation and publication steps passed;
- published plugin commit: `2d917d1e39dbcac5ee9e88bcc6dd9f988e4b688c`;
- plugin UI commit pinned from the plugin: `13fa03d0105fc66c7bb0ebc33cd20a55aed74805`;
- source runtime: `c6a679dbbb3bbd65eb096becbd5976e7ccd67a26`;
- 9 pages, 46 artifacts, desktop/mobile coverage, 0 capture errors;
- native comment-read permission and prompt construction are validated by the publication contract;
- final environment acceptance is: install the immutable manifest, synchronize a real Penpot file, add one native comment, rebuild the prompt, then confirm a second check reports `CURRENT`.
