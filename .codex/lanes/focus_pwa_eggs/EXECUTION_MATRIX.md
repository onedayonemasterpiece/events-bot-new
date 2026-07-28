# Focus-group PWA and Easter-egg extension — execution matrix

| ID | Requirement | Area | Dependencies | Conflict risk | Primary lane | Parallelizable | Done when |
|---|---|---|---|---|---|---|---|
| R13 | Center `docs/reference/PWA-icon.png` at the top of the join screen, mobile-first | onboarding UI | — | low | pwa-membership | yes | the invite page uses the exact reference asset with responsive, accessible presentation |
| R14 | Offer PWA installation and launch the installed app at the focus-group secret home | PWA | R13 | high | pwa-membership | yes | a focus-specific manifest/start URL and honest browser-controlled install/open states are represented and tested |
| R15 | If email/Yandex identity is not confirmed, show onboarding but allow the user to continue without it | optional identity | R14 | high | pwa-membership | yes | anonymous continuation and verified-choice states are distinct; neither is misrepresented as authorization |
| R16 | Focus membership survives first/subsequent PWA launches for the whole period and is not erased by personalization reset | state ownership | R14/R15 | high | pwa-membership | yes | focus state and personalization use independent keys; reset and expiry contracts are tested |
| R17 | Reward is two theatre tickets | reward copy | — | medium | egg-program | yes | product contract and UI consistently say one prize: two theatre tickets |
| R18 | Winner is determined by Easter-egg collection breadth plus NPS participation, likes/dislikes, text page feedback and Search use | scoring/research integrity | R17 | high | egg-program | yes | a bounded, auditable breadth score is designed without rewarding positive sentiment or NPS value |
| R19 | Design site-wide Easter-egg placements that exercise varied functions on mobile and desktop | discovery architecture | R18 | high | egg-program | yes | versioned placement matrix covers page families, device/accessibility alternatives, prerequisites and fail-closed states |
| R20 | Support conditional eggs between list objects, including after the third calendar save only when the list has three items | placement rules | R19 | medium | egg-prototype | after egg-program | a prototype collection/placement surface demonstrates locked, eligible, found and unavailable states |
| R21 | Preserve prototype-only boundary, docs/changelog, regression tests and pushed integration branch | integration | R13–R20 | medium | integration | after all | full local QA and incident regression checks pass; closure report is updated and branch is pushed |

## Dependency graph

```text
R13 → R14 → R15 → R16
R17 → R18 → R19 → R20
R13–R20 → R21
```

## Interpretation decisions

- “Membership” in this branch is a durable **prototype participation marker**,
  not server authorization. Production continuity must come from verified
  membership and recovery; the UI must not promise localStorage durability
  against browser-data deletion.
- The prize criterion rewards **breadth of participation**, not positive NPS,
  likes over dislikes, comment length or sentiment. Otherwise the focus-group
  measurements become biased. Tie handling and anti-abuse remain explicit
  production/legal gates.
- PWA installation remains browser-controlled. The page can offer installation
  and explain how to open the installed app, but cannot launch a newly installed
  app programmatically on every platform.
