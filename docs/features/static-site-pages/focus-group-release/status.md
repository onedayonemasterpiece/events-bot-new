# Статус release-контура фокус-группы

> **Срез:** 2026-08-05.
> Статус документа не доказывает runtime.

| Capability | Current truth | Target |
|---|---|---|
| invite/QR opens site | частично реализовано в candidate tracks | PASS на exact final SHA |
| browse without login | product decision accepted | browser acceptance required |
| visible disabled feedback pre-auth | target | implementation/evidence required |
| silent anonymous Auth | forbidden | zero occurrences |
| anonymous server feedback | forbidden | zero writes |
| email auth return | partial shared Auth | exact context return evidence |
| Yandex auth return | partial shared Auth | exact context return evidence |
| authenticated page score | partial/prototype | revision-aware terminal receipt |
| service NPS | prototype/design | separate SOR + browser evidence |
| text/screenshot | partial | component receipt/no-loss matrix |
| diagnostics attachment | design/partial | redacted component receipt |
| `/profil/` | staged docs | implementation slice pending |
| seven-artifact collection | accepted inventory | exact placement/progress evidence |
| prize rules | stale old model invalid | owner-approved rebaseline required |

## Explicit blockers

- old anonymous-session flow in any runtime/test path;
- 12-artifact/10-threshold text in current candidate;
- false success on feedback;
- no safe return after Auth;
- no page/service revision binding;
- no terminal cross-browser evidence.
