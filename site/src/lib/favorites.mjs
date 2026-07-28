export const LOCAL_PROFILE_KEY = 'ke_personalization_profile';
export const LOCAL_CALENDAR_KEY = 'ke_calendar_saved_v1';

function eventId(value) {
  const normalized = String(value ?? '').trim();
  return /^\d+$/u.test(normalized) && Number(normalized) > 0 ? normalized : null;
}

function timestamp(value) {
  const parsed = Date.parse(String(value || ''));
  return Number.isFinite(parsed) ? parsed : 0;
}

function safeJson(storage, key) {
  try {
    const parsed = JSON.parse(storage?.getItem?.(key) || 'null');
    return parsed && typeof parsed === 'object' ? parsed : null;
  } catch {
    return null;
  }
}

export function readLocalSavedEventInputs(storage) {
  const calendarState = safeJson(storage, LOCAL_CALENDAR_KEY);
  const profile = safeJson(storage, LOCAL_PROFILE_KEY);
  const calendarIds = calendarState?.v === 1 && calendarState.e && typeof calendarState.e === 'object'
    ? Object.keys(calendarState.e).map(eventId).filter(Boolean).reverse()
    : [];
  const likedEventIds = profile?.consent_ok === true && Array.isArray(profile.liked_event_ids)
    ? profile.liked_event_ids.map(eventId).filter(Boolean).reverse()
    : [];
  return { calendarIds, likedEventIds };
}

export function mergeSavedEventRefs({
  remoteRows = [],
  calendarIds = [],
  likedEventIds = [],
} = {}) {
  const byId = new Map();
  let order = 0;
  const touch = (value, source, actionAt = '') => {
    const id = eventId(value);
    if (!id) return;
    const existing = byId.get(id) || {
      eventId: id,
      calendarSaved: false,
      favoriteSaved: false,
      likedLocally: false,
      calendarAt: '',
      favoriteAt: '',
      firstOrder: order++,
    };
    if (source === 'calendar') {
      existing.calendarSaved = true;
      if (timestamp(actionAt) > timestamp(existing.calendarAt)) existing.calendarAt = actionAt;
    } else if (source === 'favorite') {
      existing.favoriteSaved = true;
      if (timestamp(actionAt) > timestamp(existing.favoriteAt)) existing.favoriteAt = actionAt;
    } else if (source === 'like') {
      existing.likedLocally = true;
    }
    byId.set(id, existing);
  };

  for (const row of remoteRows) {
    if (row?.calendar_saved) touch(row.event_id, 'calendar', row.calendar_added_at);
    if (row?.favorite_saved) touch(row.event_id, 'favorite', row.favorite_added_at);
  }
  for (const id of calendarIds) touch(id, 'calendar');
  for (const id of likedEventIds) touch(id, 'like');

  return [...byId.values()]
    .map((item) => ({
      ...item,
      sourcePriority: item.calendarSaved ? 0 : 1,
      source: item.calendarSaved ? 'calendar' : (item.favoriteSaved ? 'favorite' : 'like'),
      sortAt: Math.max(timestamp(item.calendarAt), timestamp(item.favoriteAt)),
    }))
    .sort((left, right) => (
      left.sourcePriority - right.sourcePriority
      || right.sortAt - left.sortAt
      || left.firstOrder - right.firstOrder
      || Number(left.eventId) - Number(right.eventId)
    ));
}

function catalogItems(payload) {
  if (Array.isArray(payload)) return payload;
  return Array.isArray(payload?.related_static) ? payload.related_static : [];
}

function catalogEventId(item) {
  return eventId(item?.event_id ?? item?.id ?? item?.candidate?.event_id ?? item?.candidate?.id);
}

function catalogDate(item) {
  const candidate = item?.candidate || item || {};
  return String(candidate.date || candidate.start_date || candidate.display?.start_date || '').slice(0, 10);
}

export function joinFutureSavedEvents(payload, savedRefs, currentDate) {
  const byId = new Map(catalogItems(payload).map((item) => [catalogEventId(item), item]));
  return (Array.isArray(savedRefs) ? savedRefs : [])
    .map((saved) => ({ saved, item: byId.get(eventId(saved?.eventId)) }))
    .filter(({ item }) => Boolean(item))
    .filter(({ item }) => {
      const date = catalogDate(item);
      return /^\d{4}-\d{2}-\d{2}$/u.test(date) && date >= currentDate;
    });
}
