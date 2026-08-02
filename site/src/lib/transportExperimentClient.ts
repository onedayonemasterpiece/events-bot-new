import {
  TRANSPORT_EXPERIMENT_CONFIG_HASH,
  TRANSPORT_EXPERIMENT_KEY,
  TRANSPORT_EXPERIMENT_VERSION,
  assignTransportExperimentVariant,
  isTransportExperimentVariant,
  isUuid,
  transportExperimentEligible,
  type TransportExperimentMode,
  type TransportExperimentVariant,
} from './transportExperiment';
import { getIdempotentOutbox } from './idempotentOutbox.ts';
import { getResilientDataClient } from './resilientDataClient.ts';

const SUBJECT_KEY = 'ke_experiment_subject_v1';
const PROFILE_KEY = 'ke_personalization_profile';
const QA_OVERRIDE_KEY = `ke_experiment_qa_override:${TRANSPORT_EXPERIMENT_KEY}:${TRANSPORT_EXPERIMENT_VERSION}`;
const ASSIGNMENT_KEY = `ke_experiment_assignment:${TRANSPORT_EXPERIMENT_KEY}:${TRANSPORT_EXPERIMENT_VERSION}`;
const initialized = new WeakSet<HTMLElement>();

interface ClientState {
  subjectId: string;
  variant: TransportExperimentVariant;
  bucket: number;
  qaOverride: boolean;
  exposurePromise: Promise<boolean> | null;
}

const states = new WeakMap<HTMLElement, ClientState>();
let clickListenerInstalled = false;

function storageGet(storage: Storage, key: string): string | null {
  try { return storage.getItem(key); } catch { return null; }
}

function storageSet(storage: Storage, key: string, value: string): boolean {
  try { storage.setItem(key, value); return true; } catch { return false; }
}

function uuid(): string | null {
  try { return globalThis.crypto?.randomUUID?.() || null; } catch { return null; }
}

function experimentSubject(): string | null {
  const stored = storageGet(localStorage, SUBJECT_KEY);
  if (isUuid(stored)) return stored;
  const created = uuid();
  return created && storageSet(localStorage, SUBJECT_KEY, created) ? created : null;
}

function automationActor(): boolean {
  const ua = String(navigator.userAgent || '').toLocaleLowerCase('en-US');
  return Boolean(navigator.webdriver || /(bot|crawler|spider|playwright|lighthouse|headlesschrome)/u.test(ua));
}

function qaOverride(mode: TransportExperimentMode): TransportExperimentVariant | null {
  if (mode !== 'qa' && mode !== 'focus_group') return null;
  const query = new URLSearchParams(location.search).get('ke-exp-transport');
  if (isTransportExperimentVariant(query)) {
    storageSet(sessionStorage, QA_OVERRIDE_KEY, query);
    return query;
  }
  const stored = storageGet(sessionStorage, QA_OVERRIDE_KEY);
  return isTransportExperimentVariant(stored) ? stored : null;
}

function readAssignment(): { variant: TransportExperimentVariant; bucket: number } | null {
  try {
    const parsed = JSON.parse(storageGet(localStorage, ASSIGNMENT_KEY) || 'null');
    if (parsed?.config_hash !== TRANSPORT_EXPERIMENT_CONFIG_HASH) return null;
    if (!isTransportExperimentVariant(parsed?.variant)) return null;
    if (!Number.isInteger(parsed?.bucket) || parsed.bucket < 0 || parsed.bucket > 9999) return null;
    return { variant: parsed.variant, bucket: parsed.bucket };
  } catch { return null; }
}

function writeAssignment(value: { variant: TransportExperimentVariant; bucket: number }): void {
  storageSet(localStorage, ASSIGNMENT_KEY, JSON.stringify({
    ...value,
    config_hash: TRANSPORT_EXPERIMENT_CONFIG_HASH,
    algorithm: 'sha256-u32be-bucket-10000-v1',
    assigned_at: new Date().toISOString(),
  }));
}

function consentedProfile(): { anon_id: string; session_id: string; consent_version: string } | null {
  try {
    const profile = JSON.parse(storageGet(localStorage, PROFILE_KEY) || 'null');
    if (profile?.consent_ok !== true || !isUuid(profile?.anon_id) || !isUuid(profile?.session_id)) return null;
    return {
      anon_id: profile.anon_id,
      session_id: profile.session_id,
      consent_version: String(profile.consent_version || 'local-explicit-v1').slice(0, 80),
    };
  } catch { return null; }
}

function viewportClass(): 'mobile' | 'tablet' | 'desktop' {
  if (innerWidth < 768) return 'mobile';
  if (innerWidth < 1024) return 'tablet';
  return 'desktop';
}

function moveNextDeparture(treatment: HTMLElement, allowPastFallback = false): boolean {
  const trips = Array.from(treatment.querySelectorAll<HTMLElement>('[data-transport-c-trip]'));
  const nowWithReserve = Date.now() + 10 * 60 * 1000;
  const upcoming = trips.filter((trip) => {
    const time = Date.parse(trip.dataset.departureAt || '');
    const past = !Number.isFinite(time) || time <= nowWithReserve;
    trip.dataset.transportPast = past ? 'true' : 'false';
    return !past;
  });
  if (!upcoming.length && !allowPastFallback) return false;
  const slot = treatment.querySelector<HTMLElement>('[data-transport-next-slot]');
  if (!slot) return false;
  if (!upcoming.length && allowPastFallback) trips.forEach((trip) => { trip.dataset.transportPast = 'false'; });
  const next = upcoming[0] || trips[0];
  if (!next) return false;
  if (allowPastFallback) next.dataset.transportPast = 'false';
  next.dataset.transportNext = 'true';
  slot.append(next);
  return true;
}

function renderVariant(root: HTMLElement, variant: TransportExperimentVariant, forced: boolean): HTMLElement | null {
  const treatmentSet = root.querySelector<HTMLElement>('[data-transport-treatment-set]');
  const treatment = root.querySelector<HTMLElement>(`[data-transport-treatment="${variant}"]`);
  const baseline = root.querySelector<HTMLElement>('[data-transport-baseline]');
  if (!treatmentSet || !treatment || !baseline) return null;
  if (variant === 'next_departure_queue_v1' && !moveNextDeparture(treatment, forced) && !forced) return null;
  treatmentSet.hidden = false;
  root.querySelectorAll<HTMLElement>('[data-transport-treatment]').forEach((node) => { node.hidden = node !== treatment; });
  treatment.hidden = false;
  // The accepted departure-board arm is also the resilient no-JS/ineligible
  // fallback. Do not hide it when assignment selects that same arm.
  baseline.hidden = treatment !== baseline;
  root.dataset.assignedVariant = variant;
  root.dataset.renderedVariant = variant;
  return treatment;
}

function debugBadge(root: HTMLElement, state: ClientState): void {
  if (new URLSearchParams(location.search).get('ke-exp-debug') !== '1') return;
  const output = document.createElement('output');
  output.dataset.nosnippet = '';
  output.textContent = `${state.variant} · ${state.bucket}${state.qaOverride ? ' · QA' : ''}`;
  output.style.cssText = 'display:block;margin:.3rem 0;color:#78685c;font:700 11px/1.2 system-ui';
  root.prepend(output);
}

async function ingest(root: HTMLElement, state: ClientState, eventKind: string, metadata: Record<string, unknown> = {}): Promise<boolean> {
  const mode = root.dataset.experimentMode as TransportExperimentMode;
  if ((mode !== 'focus_group' && mode !== 'live') || state.qaOverride || automationActor()) return false;
  const profile = consentedProfile();
  const url = String(root.dataset.supabaseUrl || '').replace(/\/+$/u, '');
  const relayUrl = String(root.dataset.supabaseRelayUrl || '').replace(/\/+$/u, '');
  const key = String(root.dataset.supabaseKey || '');
  const clientEventId = uuid();
  if (!profile || !url || !key || !clientEventId) return false;
  const payload = {
    experiment_key: TRANSPORT_EXPERIMENT_KEY,
    experiment_version: TRANSPORT_EXPERIMENT_VERSION,
    experiment_subject_id: state.subjectId,
    anon_id: profile.anon_id,
    session_id: profile.session_id,
    client_event_id: clientEventId,
    event_id: Number(root.dataset.eventId || 0),
    assigned_variant: state.variant,
    rendered_variant: root.dataset.renderedVariant || state.variant,
    assignment_bucket: state.bucket,
    event_kind: eventKind,
    occurred_at: new Date().toISOString(),
    viewport_class: viewportClass(),
    release_id: String(root.dataset.releaseId || '').slice(0, 160),
    config_hash: TRANSPORT_EXPERIMENT_CONFIG_HASH,
    transport_snapshot_hash: String(root.dataset.transportSnapshotHash || '').slice(0, 128),
    consent_version: profile.consent_version,
    metadata,
  };
  const send = async (nextPayload: typeof payload): Promise<boolean> => {
    try {
      const response = await getResilientDataClient({
        directUrl: url,
        relayUrl,
        publishableKey: key,
      }).request(`${url}/rest/v1/rpc/ingest_transport_experiment_event_v1`, {
      method: 'POST',
      headers: { apikey: key, Authorization: `Bearer ${key}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ p_payload: nextPayload }),
      keepalive: true,
      });
      return response.ok;
    } catch { return false; }
  };
  if (await send(payload)) return true;
  await getIdempotentOutbox().enqueue({
    id:`transport:${clientEventId}`,
    channel:'transport-experiment-v1',
    payload,
  });
  return false;
}

async function flushTransportOutbox(root: HTMLElement): Promise<void> {
  const url = String(root.dataset.supabaseUrl || '').replace(/\/+$/u, '');
  const relayUrl = String(root.dataset.supabaseRelayUrl || '').replace(/\/+$/u, '');
  const key = String(root.dataset.supabaseKey || '');
  if (!url || !key) return;
  const client = getResilientDataClient({ directUrl:url, relayUrl, publishableKey:key });
  await getIdempotentOutbox().flush(async (entry) => {
    if (entry.channel !== 'transport-experiment-v1') return 'skip';
    try {
      const response = await client.request(`${url}/rest/v1/rpc/ingest_transport_experiment_event_v1`, {
        method:'POST',
        headers:{ apikey:key, Authorization:`Bearer ${key}`, 'Content-Type':'application/json' },
        body:JSON.stringify({ p_payload:entry.payload }),
        keepalive:true,
      });
      if (response.ok || response.status === 409) return 'sent';
      return response.status >= 400 && response.status < 500 ? 'drop' : 'retry';
    } catch { return 'retry'; }
  });
}

function observeExposure(root: HTMLElement, treatment: HTMLElement, state: ClientState): void {
  if (!('IntersectionObserver' in window)) return;
  let timer = 0;
  let settled = false;
  const observer = new IntersectionObserver((entries) => {
    const entry = entries[0];
    if (settled) return;
    if (entry?.intersectionRatio >= 0.5 && document.visibilityState === 'visible') {
      if (!timer) timer = window.setTimeout(() => {
        timer = 0;
        if (document.visibilityState !== 'visible' || settled) return;
        settled = true;
        state.exposurePromise = ingest(root, state, 'valid_exposure', {
          trip_count: treatment.querySelectorAll('[data-transport-trip-id]').length,
        });
        observer.disconnect();
      }, 1000);
    } else if (timer) {
      clearTimeout(timer);
      timer = 0;
    }
  }, { threshold: [0, 0.5, 1] });
  observer.observe(treatment);
}

async function initRoot(root: HTMLElement): Promise<void> {
  if (initialized.has(root)) return;
  initialized.add(root);
  const mode = root.dataset.experimentMode as TransportExperimentMode;
  if (mode === 'off') return;
  void flushTransportOutbox(root);
  const forcedVariant = qaOverride(mode);
  // Automation may render an explicitly forced QA arm for deterministic visual
  // acceptance, but never receives a normal assignment or trusted telemetry.
  if (automationActor() && !forcedVariant) return;
  const departures = Array.from(root.querySelectorAll<HTMLElement>('[data-transport-baseline] [data-departure-at]'))
    .map((node) => node.dataset.departureAt || '');
  if (!forcedVariant && !transportExperimentEligible(departures)) {
    root.dataset.experimentIneligible = 'schedule_or_time';
    return;
  }
  const subjectId = experimentSubject();
  if (!subjectId) return;
  const saved = readAssignment();
  const allocation = forcedVariant
    ? { variant: forcedVariant, bucket: -1 }
    : saved || await assignTransportExperimentVariant(subjectId);
  if (!allocation) return;
  if (!forcedVariant && !saved) writeAssignment(allocation);
  const treatment = renderVariant(root, allocation.variant, Boolean(forcedVariant));
  if (!treatment) {
    root.dataset.experimentIneligible = 'treatment_unavailable';
    return;
  }
  const state: ClientState = {
    subjectId,
    variant: allocation.variant,
    bucket: allocation.bucket,
    qaOverride: Boolean(forcedVariant),
    exposurePromise: null,
  };
  states.set(root, state);
  root.dataset.qaOverride = state.qaOverride ? 'true' : 'false';
  debugBadge(root, state);
  observeExposure(root, treatment, state);
}

function installClickListener(): void {
  if (clickListenerInstalled) return;
  clickListenerInstalled = true;
  document.addEventListener('click', (event) => {
    const action = (event.target as Element | null)?.closest<HTMLElement>('[data-transport-action]');
    const shell = action?.closest<HTMLElement>('[data-kaup-transport]');
    const root = shell?.querySelector<HTMLElement>('[data-transport-experiment]');
    if (!action || !root) return;
    const state = states.get(root);
    if (!state || state.qaOverride || !state.exposurePromise) return;
    void state.exposurePromise.then((accepted) => {
      if (!accepted) return false;
      return ingest(root, state, String(action.dataset.transportAction || ''), {
        trip_id: action.dataset.transportTripId || null,
      });
    });
  }, { capture: true });
}

export function initTransportTimetableExperiments(): void {
  installClickListener();
  document.querySelectorAll<HTMLElement>('[data-transport-experiment]').forEach((root) => { void initRoot(root); });
}
