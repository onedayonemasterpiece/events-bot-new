import type { StaticSiteAuthSnapshot } from './staticSiteAuth';
import { setDurableSavedEvent } from './savedEvents';
import {
  buildSavedEventReconciliationPlan,
  localSavedEventState,
  savedEventReconciliationSignature,
} from './savedEventRuntimeCore.mjs';

const INSTALL_KEY = '__KENIGEVENTS_SAVED_EVENT_RUNTIME_V1__';
const RECONCILIATION_KEY = 'ke_saved_event_reconciliation_v1';
const ACTION_SETTLE_MS = 30_000;

declare global {
  interface Window {
    __KENIGEVENTS_SAVED_EVENT_RUNTIME_V1__?: boolean;
  }
}

function readReconciliationMarkers(): Record<string, string> {
  try {
    const parsed = JSON.parse(window.localStorage.getItem(RECONCILIATION_KEY) || '{}');
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

function writeReconciliationMarker(userId: string, signature: string) {
  try {
    const markers = readReconciliationMarkers();
    markers[userId] = signature;
    window.localStorage.setItem(RECONCILIATION_KEY, JSON.stringify(
      Object.fromEntries(Object.entries(markers).slice(-8)),
    ));
  } catch {
    // The RPC writes remain authoritative; a later page may retry idempotently.
  }
}

export function installSavedEventRuntime(auth: {
  client: Parameters<typeof setDurableSavedEvent>[0];
  subscribe: (subscriber: (snapshot: StaticSiteAuthSnapshot) => void) => () => void;
}) {
  if (window[INSTALL_KEY]) return;
  window[INSTALL_KEY] = true;
  let signedIn = false;
  let signedInUserId = '';
  let reconciliationInFlight: Promise<void> | null = null;
  const pending = new Map<string, { initial: boolean; startedAt: number; timer: number }>();
  const sentState = new Map<string, boolean>();

  const dispatchSavedState = (name: string, detail: Record<string, unknown>) => {
    window.dispatchEvent(new CustomEvent(name, { detail }));
  };

  const persist = async (eventId: number, source: 'calendar' | 'favorite', saved: boolean) => {
    if (!signedIn) return;
    if (reconciliationInFlight) await reconciliationInFlight.catch(() => {});
    if (!signedIn) return;
    const key = `${source}:${eventId}`;
    if (sentState.get(key) === saved) return;
    await setDurableSavedEvent(auth.client, eventId, source, saved);
    sentState.set(key, saved);
    dispatchSavedState('kenigevents:saved-event-state', { eventId, source, saved });
  };

  const settleAfterLocalCommit = (
    eventId: number,
    source: 'calendar' | 'favorite',
    initial: boolean,
  ) => {
    const key = `${source}:${eventId}`;
    const previous = pending.get(key);
    if (previous) window.clearTimeout(previous.timer);
    const startedAt = Date.now();
    const check = () => {
      const next = localSavedEventState(window.localStorage, source, eventId);
      if (next !== initial) {
        pending.delete(key);
        void persist(eventId, source, next).catch(() => {
          dispatchSavedState('kenigevents:saved-event-state-error', { eventId, source, saved: next });
        });
        return;
      }
      if (Date.now() - startedAt >= ACTION_SETTLE_MS) {
        pending.delete(key);
        return;
      }
      const active = pending.get(key);
      if (active) active.timer = window.setTimeout(check, 120);
    };
    const timer = window.setTimeout(check, 0);
    pending.set(key, { initial, startedAt, timer });
  };

  const reconcileLocalState = async (userId: string) => {
    const plan = buildSavedEventReconciliationPlan(window.localStorage);
    const signature = savedEventReconciliationSignature(plan);
    if (readReconciliationMarkers()[userId] === signature) return;
    for (const item of plan) {
      await setDurableSavedEvent(auth.client, item.eventId, item.source, item.saved);
      sentState.set(`${item.source}:${item.eventId}`, true);
    }
    writeReconciliationMarker(userId, signature);
    dispatchSavedState('kenigevents:saved-event-reconciliation-complete', {
      count: plan.length,
    });
  };

  auth.subscribe((snapshot) => {
    signedIn = snapshot.status === 'signed_in' && Boolean(snapshot.user);
    signedInUserId = signedIn ? String(snapshot.user?.id || '') : '';
    if (signedInUserId) {
      if (!reconciliationInFlight) {
        reconciliationInFlight = reconcileLocalState(signedInUserId)
          .catch(() => {
            dispatchSavedState('kenigevents:saved-event-state-error', {
              source: 'reconciliation',
            });
          })
          .finally(() => {
            reconciliationInFlight = null;
          });
      }
    } else {
      sentState.clear();
    }
  });

  document.addEventListener('click', (event) => {
    if (!signedIn || !(event.target instanceof Element)) return;
    const control = event.target.closest<HTMLElement>('[data-calendar-action], [data-saved-event-action]');
    const likeControl = event.target.closest<HTMLElement>('[data-feedback-action="like"]');
    if (!control && !likeControl) return;
    const target = control || likeControl;
    if (!target) return;
    const source = likeControl ? 'favorite' : (target.dataset.savedEventSource === 'favorite' ? 'favorite' : 'calendar');
    const rawId = target.dataset.eventId || target.dataset.calendarEventId || target.dataset.savedEventId || '';
    const eventId = Number(rawId);
    if (!Number.isSafeInteger(eventId) || eventId <= 0) return;
    const initial = localSavedEventState(window.localStorage, source, eventId);
    settleAfterLocalCommit(eventId, source, initial);
  }, { capture: true });

  window.addEventListener('kenigevents:local-saved-event-change', (event) => {
    if (!signedIn || !(event instanceof CustomEvent)) return;
    const eventId = Number(event.detail?.eventId);
    const source = event.detail?.source === 'favorite' ? 'favorite' : 'calendar';
    const saved = event.detail?.saved === true;
    if (!Number.isSafeInteger(eventId) || eventId <= 0) return;
    void persist(eventId, source, saved).catch(() => {
      dispatchSavedState('kenigevents:saved-event-state-error', { eventId, source, saved });
    });
  });
}
