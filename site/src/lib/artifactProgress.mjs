export const AMBER_ARTIFACT_ID = 'amber-cosmonaut';
export const LEGACY_AMBER_ARTIFACT_ID = 'amber_cosmonaut';
export const FIRST_ARTIFACT_COLLECTION_ID = 'signs-of-kaliningrad-001';
export const AMBER_ARTIFACT_PLACEMENT = 'weekend.rail.tail.v1';
export const ARTIFACT_COLLECTION_STORAGE_KEY = 'ke_artifact_collection_v1';
export const LEGACY_AMBER_STORAGE_KEY = 'ke_amber_artifact_prototype_v1:tail';

function emptyCollectionState() {
  return {
    schemaVersion: 1,
    collectionId: FIRST_ARTIFACT_COLLECTION_ID,
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
    value.artifacts?.[AMBER_ARTIFACT_ID] || value.artifacts?.[LEGACY_AMBER_ARTIFACT_ID],
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
