import { readLocalSavedEventInputs } from './favorites.mjs';

function normalizedId(value) {
  const id = String(value ?? '').trim();
  return /^\d+$/u.test(id) && Number(id) > 0 ? id : null;
}

export function localSavedEventState(storage, source, value) {
  const id = normalizedId(value);
  if (!id) return false;
  const local = readLocalSavedEventInputs(storage);
  return source === 'calendar'
    ? local.calendarIds.includes(id)
    : local.likedEventIds.includes(id);
}

export function buildSavedEventReconciliationPlan(storage) {
  const local = readLocalSavedEventInputs(storage);
  const seen = new Set();
  const plan = [];
  for (const value of local.calendarIds) {
    const eventId = normalizedId(value);
    if (!eventId || seen.has(`calendar:${eventId}`)) continue;
    seen.add(`calendar:${eventId}`);
    plan.push({ eventId: Number(eventId), source: 'calendar', saved: true });
  }
  for (const value of local.likedEventIds) {
    const eventId = normalizedId(value);
    if (!eventId || seen.has(`favorite:${eventId}`)) continue;
    seen.add(`favorite:${eventId}`);
    plan.push({ eventId: Number(eventId), source: 'favorite', saved: true });
  }
  return plan;
}

export function savedEventReconciliationSignature(plan) {
  const canonical = (Array.isArray(plan) ? plan : [])
    .map((item) => `${item.source}:${item.eventId}:${item.saved ? 1 : 0}`)
    .join('|');
  let hash = 2166136261;
  for (let index = 0; index < canonical.length; index += 1) {
    hash ^= canonical.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return `v2:${(hash >>> 0).toString(36)}:${Array.isArray(plan) ? plan.length : 0}`;
}
