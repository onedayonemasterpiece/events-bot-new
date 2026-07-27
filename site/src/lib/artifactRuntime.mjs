export const AMBER_ARTIFACT_ID = 'amber_cosmonaut';
export const AMBER_ARTIFACT_PUBLIC_ID = 'amber-cosmonaut';
export const AMBER_ARTIFACT_COLLECTION_ID = 'kaliningrad_artifacts_v1';
export const AMBER_ARTIFACT_PLACEMENT = 'weekend.rail.tail.v1';
export const ARTIFACT_COLLECTION_STORAGE_KEY = 'ke_artifact_collection_v1';
export const LEGACY_AMBER_STORAGE_KEY = 'ke_amber_artifact_prototype_v1:tail';

export const ARTIFACT_COLLECTION_SLOTS = Object.freeze([
  {
    id: AMBER_ARTIFACT_ID,
    title: 'Янтарный космонавт',
    hint: 'Первый след прячется у самого края одного события на выходные.',
  },
  {
    id: 'future_maritime',
    hint: 'Что-то морское ещё ждёт своего маршрута.',
  },
  {
    id: 'future_nature',
    hint: 'Следующая история будет связана с природой области.',
  },
  {
    id: 'future_city',
    hint: 'Один городской символ появится в будущей главе.',
  },
  {
    id: 'future_taste',
    hint: 'Последняя ячейка пока хранит вкусный секрет.',
  },
]);

function emptyCollectionState() {
  return {
    schemaVersion: 1,
    collectionId: AMBER_ARTIFACT_COLLECTION_ID,
    artifacts: {},
  };
}

function normalizeFoundRecord(value) {
  if (!value || typeof value !== 'object' || value.status !== 'found') return null;
  const eventId = value.eventId == null ? null : Number(value.eventId);
  return {
    status: 'found',
    foundAt: typeof value.foundAt === 'string' ? value.foundAt : null,
    eventId: Number.isSafeInteger(eventId) && eventId > 0 ? eventId : null,
    placement: typeof value.placement === 'string' && value.placement
      ? value.placement
      : AMBER_ARTIFACT_PLACEMENT,
  };
}

export function normalizeArtifactCollectionState(value) {
  const state = emptyCollectionState();
  if (!value || typeof value !== 'object') return state;
  const found = normalizeFoundRecord(
    value.artifacts?.[AMBER_ARTIFACT_ID] || value.artifacts?.[AMBER_ARTIFACT_PUBLIC_ID],
  );
  if (found) state.artifacts[AMBER_ARTIFACT_ID] = found;
  return state;
}

export function readArtifactCollection(storage = globalThis.localStorage, now = () => new Date()) {
  let state = emptyCollectionState();
  try {
    const raw = storage?.getItem(ARTIFACT_COLLECTION_STORAGE_KEY);
    if (raw) state = normalizeArtifactCollectionState(JSON.parse(raw));
  } catch {}

  if (state.artifacts[AMBER_ARTIFACT_ID]?.status === 'found') return state;

  try {
    if (storage?.getItem(LEGACY_AMBER_STORAGE_KEY) !== 'found') return state;
    state.artifacts[AMBER_ARTIFACT_ID] = {
      status: 'found',
      foundAt: now().toISOString(),
      eventId: null,
      placement: AMBER_ARTIFACT_PLACEMENT,
    };
    storage?.setItem(ARTIFACT_COLLECTION_STORAGE_KEY, JSON.stringify(state));
  } catch {}
  return state;
}

export function collectAmberArtifact({
  storage = globalThis.localStorage,
  eventId = null,
  placement = AMBER_ARTIFACT_PLACEMENT,
  now = () => new Date(),
} = {}) {
  const state = readArtifactCollection(storage, now);
  if (state.artifacts[AMBER_ARTIFACT_ID]?.status === 'found') {
    return { state, collected: false };
  }
  state.artifacts[AMBER_ARTIFACT_ID] = {
    status: 'found',
    foundAt: now().toISOString(),
    eventId: Number.isSafeInteger(Number(eventId)) && Number(eventId) > 0 ? Number(eventId) : null,
    placement,
  };
  try {
    storage?.setItem(ARTIFACT_COLLECTION_STORAGE_KEY, JSON.stringify(state));
  } catch {}
  return { state, collected: true };
}

export function hasAmberArtifact(state) {
  return state?.artifacts?.[AMBER_ARTIFACT_ID]?.status === 'found';
}

export function isAmberArtifactResearchEnabled(siteMode, flag) {
  return siteMode !== 'production' && flag === 'tail';
}

export function stableArtifactHash(value) {
  let hash = 0x811c9dc5;
  for (const character of String(value)) {
    hash ^= character.codePointAt(0);
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0;
}

export function selectAmberArtifactEventId(events, { seed, start, end }) {
  const candidates = [...new Map((events || [])
    .filter((event) => Number.isSafeInteger(Number(event?.id)) && Number(event.id) > 0)
    .filter((event) => typeof event?.title === 'string' && event.title.trim())
    .filter((event) => event.start_date === start || event.start_date === end)
    .map((event) => [Number(event.id), event])).values()]
    .sort((left, right) => Number(left.id) - Number(right.id));
  if (!candidates.length) return null;
  const index = stableArtifactHash(`${AMBER_ARTIFACT_ID}:assignment-v1:${String(seed)}`) % candidates.length;
  return Number(candidates[index].id);
}
