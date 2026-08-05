# LLM-first выявление необычных событий

> **Статус:** принятый semantic contract; public mode требует shadow evaluation
> и precision gate.

## Что классифицируется

Цель — найти конкретный source-grounded отличительный факт. Это не синоним
качества, популярности, известности, sold-out или рекламного слова
«уникальный».

Публичные уровни:

```text
unusual_public        # редкость поддержана достаточным baseline
distinctive_fact_only # факт интересен, но редкость не доказана
ordinary
indeterminate
```

При слабом baseline система может назвать факт, но не заявлять «редкое/самое
необычное».

## Pipeline

### U0 — eligibility/source bundle

Только canonical active/future event. Bundle содержит versioned source
fragments/OCR, event facts, provenance, lifecycle и duplicate identity.

### U1 — semantic signature extraction

LLM извлекает:

- public format;
- participation mechanics;
- narrow themes;
- unusual place/access;
- explicit seasonal/natural window;
- participant geography claims;
- one-off structure;
- exact evidence fragment IDs and uncertainty.

Marketing adjectives не становятся facts. World knowledge без текущего source
bundle запрещён.

### U2 — deterministic baseline

Система считает по versioned taxonomy:

- rolling 365/730-day catalog windows;
- season-aware peers;
- deduplicated occurrence/program families;
- peer count, matching signature count, occurrence days, venues, coverage;
- nearest semantic neighbours only as recall/diagnostic.

Vector distance не доказывает новизну. Недостаточный sample/coverage означает
`baseline_sufficient=false`.

### U3 — grounded adjudication

Отдельный LLM получает U1, U2 и source fragments. `unusual_public` допустим,
только когда:

1. есть explicit decisive fact;
2. evidence IDs разрешаются в current source bundle;
3. baseline достаточен именно для этого signature;
4. public fact проходит entailment validator;
5. event остаётся active/future, decision не истёк.

Иначе — `distinctive_fact_only`, `ordinary` или `indeterminate`.

### U4 — writer/validators

Offline writer получает только approved fact и claim level. Validators fail
closed при:

- missing/mismatched evidence;
- `единственный/самый/впервые/уникальный` без разрешённого claim;
- подмене редкого формата на «лучшее событие»;
- invalid link, stale/cancelled lifecycle;
- viewport/copy budget violation.

## Negative controls

Сами по себе не доказывают unusual:

- marketing superlatives;
- обычный concert/show/exhibition/lecture/walk;
- celebrity, comments, likes, sales, sold-out;
- первое появление в нашем source;
- редкое слово/опечатка/необычная картинка без text evidence;
- короткая несезонная history;
- duplicate cards одной программы;
- model world knowledge.

## Storage and lifecycle

Хранится compact decision:

```text
event_id / source_revision_hash
signature_version / baseline_version
explicit evidence IDs/hashes
decision / decisive dimensions
public_fact / claim_allowed
model+prompt version / confidence / expires_at
```

Raw source copy и prompt transcript не дублируются в event row. Historical
training/evaluation artifacts имеют отдельный bounded retention.

## Acceptance

- [ ] Labeled real-event eval отделяет signature extraction, baseline и public
  claim.
- [ ] Precision gate для `unusual_public` проходит; false superlative = 0.
- [ ] Weak baseline produces `distinctive_fact_only`, not fabricated rarity.
- [ ] Lifecycle/source change invalidates stale decision.
- [ ] Public copy reproduces evidence and contains no new facts.
- [ ] Shadow output and exact-SHA evidence reviewed before public enablement.
