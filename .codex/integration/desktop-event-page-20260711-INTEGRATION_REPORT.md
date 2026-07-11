# Desktop event-page variants — integration report

Implementation commit: `869bd0b7`.

## Requirement closure

| ID | Status | Evidence |
|---|---|---|
| R01 | Done | free/no-registration calendar primary; paid/unknown label `Билеты` |
| R02 | Done for design gate | all six lab options use one owner per fact/action; default desktop awaits selection |
| R03 | Done | onboarding defaults off; adaptive/calendar-first/forced QA modes |
| R04 | Done | no calendar `+`; 29px computed trial icons in 48px+ controls |
| R05 | Done | event 6510 exporter override and 390/1440 `contain` QA |
| R06 | Done | H1/H2/H3 implemented; Gemini Pro specification completed |
| R07 | Partial | P1/P2/P3 implemented; requested independent Opus review blocked by quota/login |
| R08 | Done | 390px Playwright, no page overflow; production mobile hierarchy not redesigned |
| R09 | Done | public noindex preview and real-event stress pages published and verified |

## Integration decision

- Accept code/docs/test changes on `integration/event-page-desktop-variants-20260711`.
- Do not promote H1/P1 or any other desktop option to default until the user selects one.
- Keep consultant blocker explicit instead of fabricating model consensus.

## Release evidence

- Public lab: `https://kenigevents.ru/preview-20260711t-desktop-event-layouts/lab/event-desktop/`
- Public build: `https://kenigevents.ru/preview-20260711t-desktop-event-layouts/__preview/`
- Public and local Playwright: 4/4 scenarios each.
- `check:preview`: passed on the fresh 312-event fixture.
