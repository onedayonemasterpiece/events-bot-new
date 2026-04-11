# INC-YYYY-MM-DD-short-slug Title

Status: open | mitigated | monitoring | closed
Severity: sev0 | sev1 | sev2 | sev3
Service: <primary production surface>
Opened: YYYY-MM-DD
Closed: YYYY-MM-DD or `—`
Owners: <service owner / incident owner / comms owner as needed>
Related incidents: `INC-*`, если есть
Related docs: <feature docs / runbooks / release governance / monitoring docs>

## Summary

Кратко: что сломалось, где проявилось, почему это production incident.

## User / Business Impact

- кто пострадал;
- как именно проявлялся дефект;
- почему это важно для эксплуатации / продукта.

## Detection

- как инцидент был замечен;
- какие сигналы сработали или не сработали;
- какие gaps в observability были обнаружены.

## Timeline

- timestamped chronology с timezone;
- lead-up, detection, escalations, mitigations, restoration.

## Root Cause

1. ...
2. ...
3. ...

## Contributing Factors

- ...

## Automation Contract

### Treat as regression guard when

- какие изменения автоматически должны поднимать этот incident record как regression-check.

### Affected surfaces

- code paths
- env/config
- release path
- external systems
- smoke paths / alerts / health checks

### Mandatory checks before closure or deploy

- тесты;
- smoke;
- config checks;
- logs/evidence checks;
- release-governance checks.

### Required evidence

- deployed SHA;
- ссылки на тесты/smoke;
- ссылки на логи/артефакты/скриншоты;
- confirmation, что fix reachable from `origin/main`.

## Immediate Mitigation

- что было сделано для быстрой стабилизации.

## Corrective Actions

- что изменено в коде/конфиге/процессе для устранения root cause.

## Follow-up Actions

- [ ] owner / due date / tracking issue / action
- [ ] owner / due date / tracking issue / action

## Release And Closure Evidence

- deployed SHA:
- deploy path:
- regression checks:
- post-deploy verification:

## Prevention

- какие guardrails, tests, alerts, process changes или docs были добавлены против повторения инцидента.
