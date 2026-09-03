const GOLDEN_ACTION_ORIGIN = 'https://example.invalid';
const GOLDEN_ACTION_PREFIX = '/kenigevents-golden';

export function goldenActionHref(spec) {
  const lifecycleStatus = spec.lifecycle_status || 'active';
  if (lifecycleStatus === 'cancelled' || spec.admission?.is_free || spec.admission?.kind === 'free') return null;
  if (spec.admission?.kind === 'phone') return 'tel:+74012000000';
  if (!['ticket', 'registration', 'source'].includes(spec.admission?.kind)) return null;
  return `${GOLDEN_ACTION_ORIGIN}${GOLDEN_ACTION_PREFIX}/${spec.admission.kind}/${spec.id}`;
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
  }));
}
