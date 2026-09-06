import {
  homeCandidateFamily, homeCandidateId, homeHiddenIds, parseHomeProfile,
  rankHomeCandidates, reconcileHomeOrder,
} from './homeFeed.mjs';
import { packRelatedCardRows } from './relatedCardLayout.mjs';
import { LEGACY_PROFILE_STORAGE_KEY_V1 } from './personalization/legacy/profile-v1';

type CardHost = {
  hiddenIds: () => string[];
  register: (grid: HTMLElement, candidates: unknown[], context: Record<string,string>) => void;
};
type HomeWindow = Window & {
  KenigEventsCreateEventCard?: (item: unknown, variant?: string, layout?: unknown) => HTMLElement | null;
  KenigEventsSearchCardHost?: CardHost;
  KenigEventsHomeCardHost?: { sync: (reason?: string) => void };
  applyFeedbackState?: (options?: Record<string,unknown>) => Promise<void>;
};

export function bindHomeFeeds() {
  const win = window as HomeWindow;
  for (const root of document.querySelectorAll<HTMLElement>('[data-home-cold-start-feed]')) {
    if (root.dataset.homeFeedBound) continue;
    const grid = root.querySelector<HTMLElement>('[data-home-feed-grid]');
    const source = root.querySelector('[data-home-feed-candidates]');
    if (!grid || !source) continue;
    let pool;
    try { pool = JSON.parse(source.textContent || ''); } catch { continue; }
    if (pool.v !== 1 || !Array.isArray(pool.candidates)) continue;
    root.dataset.homeFeedBound = 'true';
    const candidates = pool.candidates;
    const byId = new Map<string,any>(candidates.map((item: any) => [homeCandidateId(item), item]));
    const readProfile = () => {
      try { return parseHomeProfile(localStorage.getItem(LEGACY_PROFILE_STORAGE_KEY_V1)); } catch { return null; }
    };
    const cards = () => Array.from(grid.querySelectorAll<HTMLElement>(':scope > [data-home-feed-item]'));
    let order = cards().map((card) => card.dataset.eventId!);
    let locked = 0;
    let personalized = false;
    let syncing = false;
    let ready = false;
    let snapshotKey = '';
    let owner = '';
    let framePending = false;
    // Source identity prevents a stale saved order from admitting a changed snapshot.
    const sourceIdentity = `${pool.currentDate}:${candidates.map(homeCandidateId).join(',')}`;
    const hash = (value: string) => {
      let result = 2166136261;
      for (let i = 0; i < value.length; i++) result = Math.imul(result ^ value.charCodeAt(i), 16777619);
      return (result >>> 0).toString(16);
    };
    const setOwner = (profile: any) => {
      const next = profile?.anon_id || 'general';
      if (next === owner) return false;
      owner = next;
      snapshotKey = `ke_home_feed_v2:${location.pathname}:${owner}:${hash(sourceIdentity)}`;
      return true;
    };
    const save = () => {
      if (!ready) return;
      try {
        sessionStorage.setItem(snapshotKey, JSON.stringify({v:2, order, personalized, at:Date.now()}));
      } catch { /* Static feed and local actions stay usable with denied session storage. */ }
    };
    const markObserved = () => {
      for (const [index, id] of order.entries()) {
        const card = grid.querySelector<HTMLElement>(`[data-home-feed-item][data-event-id="${CSS.escape(id)}"]`);
        if (card && !card.hidden && card.getBoundingClientRect().top < innerHeight) locked = Math.max(locked,index + 1);
      }
    };
    setOwner(readProfile());
    let restored = false;
    try {
      const saved = JSON.parse(sessionStorage.getItem(snapshotKey) || 'null');
      if (saved?.v === 2 && Date.now() - saved.at < 6 * 3600_000 && Array.isArray(saved.order)
          && saved.order.length <= candidates.length && saved.order.every((id:unknown) => typeof id === 'string' && byId.has(id))) {
        order = [...new Set<string>(saved.order)]; locked = order.length;
        personalized = saved.personalized === true; restored = true;
      }
    } catch { /* No saved state is necessary for the no-JS/first-visit fallback. */ }
    if (!restored) markObserved();

    const sync = () => {
      if (syncing || !ready || !win.KenigEventsCreateEventCard || !win.KenigEventsSearchCardHost) return;
      syncing = true;
      try {
        const profile = readProfile();
        if (setOwner(profile)) {
          // Never reuse another owner's personal order; keep only already observed cards in place.
          personalized = false;
        }
        markObserved();
        const plan = rankHomeCandidates(candidates, profile);
        const hidden = homeHiddenIds(profile);
        for (const id of win.KenigEventsSearchCardHost.hiddenIds()) hidden.add(String(id));
        const next = reconcileHomeOrder({ previous:order, locked, ranked:plan.items, candidates, hidden });
        if (plan.personalized && next.order.slice(locked).length) personalized = true;
        if (!profile || !plan.personalized) personalized = false;
        const existing = new Map(cards().map((card) => [card.dataset.eventId!,card]));
        // Same shared framing owner as SSR; no home image fields, cropping or layout filter.
        const visibleCandidates = next.order.filter((id:string) => !next.hidden.includes(id)).map((id:string) => byId.get(id));
        const framing = new Map(packRelatedCardRows(visibleCandidates, {
          limit:30, rowSize:3, mediaTreatment:'hybrid', presentation:'flow', preserveOrder:true,
        }).map((entry:any) => [homeCandidateId(entry.item),entry.layout]));
        const created = new Map<string,HTMLElement>();
        for (const id of next.order) {
          if (existing.has(id)) continue;
          const card = win.KenigEventsCreateEventCard({candidate:byId.get(id),event_id:Number(id)}, 'split-actions', framing.get(id));
          if (!card) { root.dataset.homeFeedMode = 'static_fallback'; return; }
          card.dataset.homeFeedItem = ''; card.classList.add('home-feed__item');
          created.set(id,card);
        }
        let previous: HTMLElement | null = null;
        for (const [index,id] of next.order.entries()) {
          const card = existing.get(id) || created.get(id)!;
          card.hidden = next.hidden.includes(id);
          card.dataset.homeFeedRank = String(index);
          // Never detach/recreate observed cards. Only reconcile a changed unseen suffix.
          if (card.parentElement !== grid || card.previousElementSibling !== previous) {
            grid.insertBefore(card, previous ? previous.nextElementSibling : grid.firstElementChild);
          }
          previous = card;
        }
        for (const [id,card] of existing) if (!next.order.includes(id)) card.remove();
        order = next.order; locked = next.locked;
        const ordered = [...order.map((id) => byId.get(id)), ...candidates.filter((candidate:any) => !order.includes(homeCandidateId(candidate)))];
        win.KenigEventsSearchCardHost.register(grid, ordered, {
          servedListId:`home:${hash(sourceIdentity)}:${hash(order.join(','))}`,
          servedListHash:hash(order.join(',')), algorithmId:personalized ? 'legacy-personal-feed-v1' : 'legacy-static-diversity-v1',
          sectionId:'home-feed', surface:'home_feed',
        });
        const mode = personalized ? 'personalized' : 'general';
        root.dataset.homeFeedMode = mode;
        root.dataset.homeStablePrefix = 'true';
        root.dataset.dsState = `${mode} ${next.visible ? 'populated' : 'empty'}`;
        const status = root.querySelector('[data-home-feed-status]');
        if (status) status.textContent = personalized ? 'С учётом ваших интересов' : 'Общая подборка';
        const empty = root.querySelector<HTMLElement>('[data-home-feed-empty]');
        if (empty) empty.hidden = next.visible > 0;
        markObserved(); save();
      } finally { syncing = false; }
    };
    win.KenigEventsHomeCardHost = {sync};
    // Wait for the common EventCard template/action owner, never a second materializer.
    let attempts = 0;
    const initialize = () => {
      if (!win.KenigEventsCreateEventCard || !win.KenigEventsSearchCardHost) {
        if (++attempts < 180) requestAnimationFrame(initialize);
        return;
      }
      ready = true; sync();
      void win.applyFeedbackState?.({skipDiscoveryHydration:true});
      const returning = history.state?.keHomeFeed;
      if (restored && returning?.key === snapshotKey && Number.isFinite(returning.scrollY)) {
        requestAnimationFrame(() => scrollTo({top:returning.scrollY,behavior:'instant'}));
      }
    };
    initialize();
    addEventListener('scroll', () => {
      if (framePending) return;
      framePending = true;
      requestAnimationFrame(() => {framePending = false; markObserved();});
    }, {passive:true});
    document.addEventListener('click', (event) => {
      if ((event.target as Element)?.closest?.('[data-home-feed-item]')) markObserved();
    }, {capture:true});
    addEventListener('storage', (event) => {
      if (!event.key || event.key === LEGACY_PROFILE_STORAGE_KEY_V1) sync();
    });
    addEventListener('pagehide', () => {
      save();
      try { history.replaceState({...history.state,keHomeFeed:{key:snapshotKey,scrollY}},''); } catch {}
    });
    addEventListener('pageshow', (event) => {if (event.persisted) sync();});
  }
}
