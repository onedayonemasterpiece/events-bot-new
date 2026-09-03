const GOLDEN_ACTION_ORIGIN = 'https://example.invalid';
const GOLDEN_ACTION_PREFIX = '/kenigevents-golden';

function hasRegistration(spec) {
  const value = [spec.admission?.kind, spec.admission?.label].filter(Boolean).join(' ').toLowerCase();
  return /registration|регистрац|зарегистр/u.test(value);
}

function hasBooking(spec) {
  const value = [spec.admission?.kind, spec.admission?.label].filter(Boolean).join(' ').toLowerCase();
  return /phone|телефон|запис|коммент/u.test(value);
}

export function goldenActionHref(spec) {
  const lifecycleStatus = spec.lifecycle_status || 'active';
  if (lifecycleStatus === 'cancelled' || spec.admission?.kind === 'free') return null;
  if (spec.admission?.kind === 'phone') return 'tel:+74012000000';
  if (!['ticket', 'registration', 'source'].includes(spec.admission?.kind)) return null;
  return `${GOLDEN_ACTION_ORIGIN}${GOLDEN_ACTION_PREFIX}/${spec.admission.kind}/${spec.id}`;
}

export function goldenExpectedActionLabel(spec) {
  if (spec.admission?.kind === 'source') return 'Открыть пост организатора';
  if (spec.admission?.kind === 'free') return 'Источник события';
  if (spec.admission?.kind === 'registration') return 'Зарегистрироваться';
  return spec.admission?.label || 'Условия уточняются';
}

export function goldenExpectedAdmissionLabel(spec) {
  const admission = spec.admission || {};
  if (admission.is_free) {
    if (hasRegistration(spec)) return 'Бесплатно · регистрация';
    if (hasBooking(spec)) return 'Бесплатно · по записи';
    return 'Бесплатно · вход свободный';
  }
  if (admission.price_label) return admission.price_label;
  if (admission.kind === 'phone') return 'Запись по телефону';
  if (admission.kind === 'ticket') return 'Билеты';
  return spec.status_label || admission.label || 'Условия уточняются';
}

export function applyGoldenActionFixtures(previewData, corpus) {
  const specs = new Map(corpus.events.map((event) => [Number(event.id), event]));
  return {
    ...previewData,
    events: previewData.events.map((event) => {
      const spec = specs.get(Number(event.id));
      if (!spec) return event;
      const href = goldenActionHref(spec);
      const sourceUrl = spec.admission.kind === 'source' ? href : null;
      return {
        ...event,
        ticket: {
          ...event.ticket,
          href,
        },
        source_url: sourceUrl,
        source_urls: sourceUrl ? [sourceUrl] : [],
      };
    }),
  };
}

export function goldenActionContract(corpus) {
  return corpus.events.map((event) => ({
    event_id:Number(event.id),
    slug:event.slug,
    kind:event.admission.kind,
    lifecycle_status:event.lifecycle_status || 'active',
    href:goldenActionHref(event),
    expected_action_label:goldenExpectedActionLabel(event),
    expected_admission_label:goldenExpectedAdmissionLabel(event),
  }));
}
