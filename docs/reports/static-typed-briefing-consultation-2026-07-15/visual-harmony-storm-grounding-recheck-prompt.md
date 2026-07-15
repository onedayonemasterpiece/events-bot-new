# Focused storm-chain grounding and visual recheck

Это обязательный recheck после независимого merge review. Предыдущий visual
`LAB PUBLISH PASS` не закрывает grounding: reviewer справедливо обнаружил, что
две прежние «лекции» имели прошедшие `display_date` (3 апреля и 3 июля), а их
широкий `end_date` ошибочно использовался как доказательство будущего
occurrence. Тот уже опубликованный, но никому не отправленный preview
superseded и не должен приниматься.

Проверь новую двухэкранную lab-цепочку по оригиналам:

Каталог:
`/home/dev/projects/events-bot-new-typed-briefing-followup-20260715-integration/artifacts/codex/static-typed-briefing-followup-20260715/grounding-recheck/`

Premise:
- `storm_weekend_demo-1366x768.png`
- `storm_weekend_demo-1440x900.png`
- `storm_weekend_demo-320x568.png`
- `storm_weekend_demo-390x844.png`

Future lecture:
- `storm_lecture_science_demo-1366x768.png`
- `storm_lecture_science_demo-1440x900.png`
- `storm_lecture_science_demo-320x568.png`
- `storm_lecture_science_demo-390x844.png`

Real mobile transition/cursor recording:
- `storm-grounded-chain-390x844.webm`

## New contract and facts

1. Premise copy is explicitly conditional: `Если прогнозируют шторм — может,
   в уют?`. It does not assert a live/current-weekend forecast.
2. It links to and automatically continues to one genuinely future event:
   event `5803`, `Суперспособности: выдумка и реальность`,
   `starts_at=2026-07-24T18:30:00+02:00`, indoor venue `Арт-пространство Заря`.
3. Second screen: `Шоу-лекция: Суперспособности.` Both the named fragment and
   CTA open canonical event `5803`.
4. The chain stops after screen 2. Public Next appears; terminal underscore
   makes three cycles and disappears. This follows the documented fail-closed
   rule: with one eligible future event use two screens, never invent a second.
5. The production backlog remains stricter: an attributed fresh forecast and
   each recommendation must overlap that forecast interval. This lab is a
   conditional visual/mechanics fixture, not a weather claim.
6. Scenario deck is now 18 + fallback. Automated geometry still requires
   1–3 lines at 320/375/390/1440, hero <=50vh, categories and feed entry visible.

## Answer format

1. Grounding verdict: does this remove the stale/current-weather deception?
2. Visual/copy verdict for both screens at desktop and mobile; note any true
   blocker, especially awkward wraps, cursor/underline collision, or CTA/Next
   hierarchy.
3. Motion verdict from WebM: pending cursor accurately promises screen 2 and
   terminal cursor retires rather than blinking forever.
4. State whether the earlier full-width seam/bottom-anchor acceptance remains
   applicable (layout code is unchanged) and whether its storm-specific part is
   safely superseded by this focused recheck.
5. Final line exactly `LAB PUBLISH PASS` or `LAB PUBLISH FAIL`.

No `PASS WITH CONDITIONS` if a publish blocker remains. Пиши по-русски, строго.
