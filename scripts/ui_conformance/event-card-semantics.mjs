const OBSOLETE_ADMISSION_LABELS = new Set(['условия уточняются']);
const SOLD_PATTERN = /sold|unavailable|not[_\s-]?available|нет\s+бил|законч|распрод/u;
const REGISTRATION_PATTERN = /регистрац|registration|зарегистр/u;
const BOOKING_PATTERN = /запис|phone|телефон|коммент/u;
const TICKET_PATTERN = /билет|ticket|sale|available|продаж/u;

function normalizedText(value) {
  return String(value || '').normalize('NFKC').replace(/ё/giu, 'е').replace(/\s+/gu, ' ').trim().toLocaleLowerCase('ru-RU');
}

function semanticToken(value) {
  return normalizedText(value)
    .replace(/[«»"'`´’‘.,!?()[\]{}:;—–_/\\]+/gu, '-')
    .replace(/\s+/gu, '-')
    .replace(/-+/gu, '-')
    .replace(/^-|-$/gu, '');
}

/** Event-type vocabulary is content, never a Penpot component-variant axis. */
export function resolveEventTypeSemantic(rawValue, { labelOverride = null } = {}) {
  const raw = normalizedText(rawValue);
  if (!raw) return { semantic_value:'event', raw_value:null, label:labelOverride || 'событие', label_source:labelOverride ? 'explicit-override' : 'fallback', component_variant:'default' };
  return {
    semantic_value: semanticToken(raw),
    raw_value: raw,
    label: labelOverride || raw,
    label_source: labelOverride ? 'explicit-override' : 'normalized-source',
    component_variant: 'default',
  };
}

const CURRENCY_SUFFIXES = [
  { pattern:/\s*(?:₽|руб(?:\.|лей|ля|ль)?)\s*$/iu, code:'RUB' },
  { pattern:/\s*(?:€|EUR)\s*$/iu, code:'EUR' },
  { pattern:/\s*(?:\$|USD)\s*$/iu, code:'USD' },
  { pattern:/\s*(?:£|GBP)\s*$/iu, code:'GBP' },
  { pattern:/\s*(?:₸|KZT)\s*$/iu, code:'KZT' },
];

function parsePositiveInteger(value) {
  const normalized = String(value || '').replace(/[\s\u00a0]/gu, '');
  return /^\d+$/u.test(normalized) ? Number(normalized) : null;
}

export function parseAdmissionPrice(label, { defaultCurrency = 'RUB' } = {}) {
  const sourceLabel = String(label || '').normalize('NFKC').replace(/\s+/gu, ' ').trim();
  if (!sourceLabel) return { valid:false, reason:'missing-price-label', label:null, amount_min:null, amount_max:null, currency:defaultCurrency, currency_source:'default' };
  let numeric = sourceLabel; let currency = defaultCurrency; let currencySource = 'default';
  for (const row of CURRENCY_SUFFIXES) {
    if (!row.pattern.test(numeric)) continue;
    numeric = numeric.replace(row.pattern, '').trim(); currency = row.code; currencySource = 'label'; break;
  }
  const match = /^(?:от\s+)?([\d\s\u00a0]+)(?:\s*[–—-]\s*([\d\s\u00a0]+))?$/iu.exec(numeric);
  if (!match) return { valid:false, reason:'unparseable-price', label:sourceLabel, amount_min:null, amount_max:null, currency, currency_source:currencySource };
  const amountMin = parsePositiveInteger(match[1]); const amountMax = parsePositiveInteger(match[2] || match[1]);
  if (!Number.isFinite(amountMin) || !Number.isFinite(amountMax) || amountMin <= 0 || amountMax <= 0) {
    return { valid:false, reason:'non-positive-price', label:sourceLabel, amount_min:amountMin, amount_max:amountMax, currency, currency_source:currencySource };
  }
  if (amountMax < amountMin) return { valid:false, reason:'descending-price-range', label:sourceLabel, amount_min:amountMin, amount_max:amountMax, currency, currency_source:currencySource };
  return { valid:true, reason:null, label:sourceLabel, amount_min:amountMin, amount_max:amountMax, currency, currency_source:currencySource, amount_kind:amountMin === amountMax ? 'single' : 'range' };
}

function formatStructuredPrice(amountMin, amountMax, currency) {
  return `${amountMin}${amountMax === amountMin ? '' : `–${amountMax}`} ${currency}`;
}

function structuredAdmissionPrice(ticket, { defaultCurrency }) {
  const rawMin = ticket.amount_min ?? ticket.price_min;
  const rawMax = ticket.amount_max ?? ticket.price_max ?? rawMin;
  if (rawMin === undefined || rawMin === null || rawMin === '') return null;
  const amountMin = Number(rawMin); const amountMax = Number(rawMax);
  const currency = String(ticket.currency || defaultCurrency).trim().toUpperCase();
  const currencySource = ticket.currency ? 'field' : 'default';
  const label = String(ticket.price_label || '').trim() || (Number.isFinite(amountMin) && Number.isFinite(amountMax) ? formatStructuredPrice(amountMin, amountMax, currency) : null);
  if (!Number.isSafeInteger(amountMin) || !Number.isSafeInteger(amountMax)) return { valid:false, reason:'non-integer-price', label:null, amount_min:amountMin, amount_max:amountMax, currency, currency_source:currencySource };
  if (!/^[A-Z]{3}$/u.test(currency)) return { valid:false, reason:'invalid-currency', label, amount_min:amountMin, amount_max:amountMax, currency, currency_source:currencySource };
  if (amountMin <= 0 || amountMax <= 0) return { valid:false, reason:'non-positive-price', label, amount_min:amountMin, amount_max:amountMax, currency, currency_source:currencySource };
  if (amountMax < amountMin) return { valid:false, reason:'descending-price-range', label, amount_min:amountMin, amount_max:amountMax, currency, currency_source:currencySource };
  return { valid:true, reason:null, label, amount_min:amountMin, amount_max:amountMax, currency, currency_source:currencySource, amount_kind:amountMin === amountMax ? 'single' : 'range' };
}

function eventStatusText(event) {
  return normalizedText([event.ticket?.status, event.ticket?.label, event.status_label, event.ticket?.note].filter(Boolean).join(' '));
}

export function resolveAdmissionSemantic(event, { defaultCurrency = 'RUB' } = {}) {
  const ticket = event.ticket || {}; const statusText = eventStatusText(event); const rawLabel = String(ticket.price_label || event.status_label || ticket.label || '').trim() || null;
  if (SOLD_PATTERN.test(statusText)) return { state:'sold_out', visible:true, label:'Билеты закончились', price:null, anomaly:null, component_variant:'default' };
  const structuredPrice = structuredAdmissionPrice(ticket, { defaultCurrency });
  if (structuredPrice || ticket.price_label) {
    const price = structuredPrice || parseAdmissionPrice(ticket.price_label, { defaultCurrency:ticket.currency || defaultCurrency });
    if (!price.valid) return { state:'invalid_price', visible:false, label:null, price, anomaly:price.reason, component_variant:'default' };
    return { state:'priced', visible:true, label:price.label, price, anomaly:null, component_variant:'default' };
  }
  if (ticket.is_free) {
    if (REGISTRATION_PATTERN.test(statusText)) return { state:'free_registration', visible:true, label:'Бесплатно · регистрация', price:null, anomaly:null, component_variant:'default' };
    if (BOOKING_PATTERN.test(statusText)) return { state:'free_booking', visible:true, label:'Бесплатно · по записи', price:null, anomaly:null, component_variant:'default' };
    return { state:'free_entry', visible:true, label:'Бесплатно · вход свободный', price:null, anomaly:null, component_variant:'default' };
  }
  if (ticket.kind === 'phone' || BOOKING_PATTERN.test(statusText)) return { state:'phone_booking', visible:true, label:'Запись по телефону', price:null, anomaly:null, component_variant:'default' };
  if (ticket.kind === 'registration' || REGISTRATION_PATTERN.test(statusText)) return { state:'registration_required', visible:true, label:'Регистрация', price:null, anomaly:null, component_variant:'default' };
  if (ticket.kind === 'ticket' || TICKET_PATTERN.test(statusText)) return { state:'ticketed', visible:true, label:'Билеты', price:null, anomaly:null, component_variant:'default' };
  const obsolete = OBSOLETE_ADMISSION_LABELS.has(normalizedText(rawLabel));
  return { state:'unspecified', visible:false, label:null, price:null, anomaly:obsolete ? 'obsolete-unspecified-label-hidden' : 'unspecified-admission-hidden', component_variant:'default' };
}

export function resolveAdmissionCta(event, admission) {
  const ticket = event.ticket || {}; const href = String(ticket.href || '').trim() || null;
  if (admission.state === 'sold_out') return { semantic_action:'purchase', present:true, enabled:false, label:'Билеты закончились', href:null, component_variant:'default' };
  if (ticket.kind === 'phone') return { semantic_action:'call', present:Boolean(href), enabled:Boolean(href), label:'Позвонить организатору', href, component_variant:'default' };
  if (ticket.kind === 'registration') return { semantic_action:'register', present:Boolean(href), enabled:Boolean(href), label:'Зарегистрироваться', href, component_variant:'default' };
  if (ticket.kind === 'source') return { semantic_action:'open_source', present:Boolean(href), enabled:Boolean(href), label:'Открыть пост организатора', href, component_variant:'default' };
  if (ticket.kind === 'ticket' && href) return { semantic_action:'purchase', present:true, enabled:true, label:'Купить билет', href, component_variant:'default' };
  return { semantic_action:'none', present:false, enabled:false, label:null, href:null, component_variant:'default' };
}

function nonNegativeCount(value) {
  const count = Number(value ?? 0);
  return Number.isSafeInteger(count) && count >= 0 ? count : 0;
}

export function resolveSocialProof(event) {
  const likes = nonNegativeCount(event.likes_count); const shares = nonNegativeCount(event.shares_count);
  return {
    like: { component_id:'event.social-proof.like', metric:'like', count:likes, visible:likes > 0, count_label:likes > 0 ? String(likes) : '', component_variant:'default' },
    share: { component_id:'event.social-proof.share', metric:'share', count:shares, visible:shares > 0, count_label:shares > 0 ? String(shares) : '', component_variant:'default' },
  };
}

export function resolveEventCardActions(event, { calendarEligible = false } = {}) {
  return {
    not_interested: { semantic_action:'dismiss_recommendation', present:true, label:'Не интересно', component_variant:'default' },
    calendar: { semantic_action:'calendar_add', present:Boolean(calendarEligible), label:calendarEligible ? 'В календарь' : null, component_variant:'default' },
    share: { semantic_action:'share', present:true, label:'Поделиться', component_variant:'default' },
    like: { semantic_action:'like', present:true, label:null, component_variant:'default' },
  };
}

export function resolveEventCardSemantics(event, options = {}) {
  const eventType = resolveEventTypeSemantic(event.event_type, { labelOverride:options.eventTypeLabelOverride || null });
  const admission = resolveAdmissionSemantic(event, { defaultCurrency:options.defaultCurrency || 'RUB' });
  return {
    event_type: eventType,
    admission,
    admission_cta: resolveAdmissionCta(event, admission),
    actions: resolveEventCardActions(event, { calendarEligible:Boolean(options.calendarEligible) }),
    social_proof: resolveSocialProof(event),
    anomalies: [admission.anomaly].filter(Boolean),
  };
}
