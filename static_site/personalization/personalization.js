(function attachKenigEventsPersonalization(global) {
  'use strict';

  const DEFAULT_STORAGE_KEY = 'ke_personalization_profile';
  const PROFILE_VERSION = 'anon-profile-v1';
  const FEATURE_SCHEMA_VERSION = 'event-detail-related-v1';
  const TAXONOMY_VERSION = 'event-taxonomy-v1';
  const SURFACE_EVENT_DETAIL_RELATED = 'event_detail_related';
  const STATIC_ALGORITHM_ID = 'static_related_v1';
  const LOCAL_ALGORITHM_ID = 'local_related_rerank_v1';
  const LOCAL_FALLBACK_ALGORITHM_ID = 'local_related_rerank_v1_fallback';
  const DEFAULT_SERVED_LIST_DEDUPE_MS = 30 * 60 * 1000;

  const DEFAULT_WEIGHTS = {
    staticContext: 0.80,
    profileAffinity: 0.10,
    priceMatch: 0.04,
    timeMatch: 0.03,
    exploration: 0.02,
    negativeInterest: 0.55,
    fatigue: 0.18,
    soldOut: 0.20,
  };

  function safeJsonParse(raw, fallback) {
    if (!raw) return fallback;
    try {
      return JSON.parse(raw);
    } catch (_) {
      return fallback;
    }
  }

  function getSafeStorage(win, explicitStorage) {
    if (explicitStorage) return explicitStorage;
    try {
      return win && win.localStorage ? win.localStorage : null;
    } catch (_) {
      return null;
    }
  }

  function safeStorageGet(storage, key) {
    try {
      return storage ? storage.getItem(key) : null;
    } catch (_) {
      return null;
    }
  }

  function safeStorageSet(storage, key, value) {
    try {
      if (!storage) return false;
      storage.setItem(key, value);
      return true;
    } catch (_) {
      return false;
    }
  }

  function safeStorageRemove(storage, key) {
    try {
      if (storage) storage.removeItem(key);
      return true;
    } catch (_) {
      return false;
    }
  }

  function viewportClass(width) {
    if (width < 768) return 'mobile';
    if (width < 1024) return 'tablet';
    return 'desktop';
  }

  function relatedPresentationMode(width) {
    return viewportClass(width) === 'mobile' ? 'vertical_related' : 'grid_related';
  }

  function relatedLayoutMode() {
    return 'module';
  }

  function toArray(value) {
    return Array.isArray(value) ? value : [];
  }

  function asId(value) {
    if (value == null) return null;
    return String(value);
  }

  function candidateId(candidate) {
    return candidate && (candidate.event_id != null ? candidate.event_id : candidate.id);
  }

  function currentEventId(manifest) {
    if (!manifest) return null;
    if (manifest.current_event && manifest.current_event.event_id != null) return manifest.current_event.event_id;
    return manifest.event_id != null ? manifest.event_id : null;
  }

  function candidatePositiveMatchingTags(candidate) {
    if (!candidate) return [];
    const values = [];
    for (const key of ['category', 'event_type']) {
      if (candidate[key]) values.push(String(candidate[key]));
    }
    for (const key of ['tags', 'audience_tags', 'format_tags', 'time_tags', 'price_tags']) {
      for (const item of toArray(candidate[key])) values.push(String(item));
    }
    for (const reason of toArray(candidate.reason_codes)) {
      const text = String(reason);
      if (text.startsWith('tag:')) values.push(text.slice(4));
      if (text.startsWith('same_category:')) values.push(text.slice('same_category:'.length));
    }
    return Array.from(new Set(values.filter(Boolean)));
  }

  function candidateExclusionTags(candidate) {
    return Array.from(new Set(toArray(candidate && candidate.audience_exclusion_tags).map(String).filter(Boolean)));
  }

  // Backward helper name: returns only matching/interest tags, not exclusions.
  function candidateTags(candidate) {
    return candidatePositiveMatchingTags(candidate);
  }

  function profileHasConsent(profile) {
    return Boolean(profile && profile.consent_ok === true);
  }

  function isCompatibleProfile(profile, featureSchemaVersion, taxonomyVersion) {
    if (!profileHasConsent(profile)) return false;
    const expectedFeatureSchema = featureSchemaVersion || FEATURE_SCHEMA_VERSION;
    const expectedTaxonomy = taxonomyVersion || TAXONOMY_VERSION;
    if (profile.profile_version !== PROFILE_VERSION) return false;
    if (profile.feature_schema_version !== expectedFeatureSchema) return false;
    if (profile.taxonomy_version !== expectedTaxonomy) return false;
    // Legacy field is intentionally rejected. Migrate/reset before scoring.
    if (Object.prototype.hasOwnProperty.call(profile, 'negative_tags')) return false;
    return true;
  }

  function readProfile(storage, storageKey) {
    return safeJsonParse(safeStorageGet(storage, storageKey), null);
  }

  function writeProfile(storage, storageKey, profile) {
    return safeStorageSet(storage, storageKey, JSON.stringify(profile));
  }

  function mapScore(map, key) {
    if (!map || key == null) return 0;
    const value = map[key];
    const num = Number(value || 0);
    return Number.isFinite(num) ? num : 0;
  }

  function collectWeightedProfileMap(profile, fieldName) {
    const result = Object.assign({}, profile && profile[fieldName] ? profile[fieldName] : {});
    const horizons = [
      ['session', 1.0],
      ['short', 0.65],
      ['mid', 0.35],
      ['long', 0.20],
    ];
    for (const [horizon, weight] of horizons) {
      const section = profile && profile[horizon];
      if (!section || !section[fieldName]) continue;
      for (const [key, value] of Object.entries(section[fieldName])) {
        result[key] = mapScore(result, key) + Number(value || 0) * weight;
      }
    }
    return result;
  }

  function staticCandidateScore(candidate) {
    const raw = candidate && (candidate.static_score != null ? candidate.static_score : candidate.base_similarity);
    const score = Number(raw || 0);
    if (!Number.isFinite(score)) return 0;
    return Math.max(0, Math.min(1, score));
  }

  function isCancelledLike(candidate) {
    const lifecycle = String((candidate && candidate.lifecycle_status) || '').toLowerCase();
    const status = String((candidate && candidate.status) || (candidate && candidate.ticket_status) || '').toLowerCase();
    return lifecycle === 'cancelled' || lifecycle === 'postponed' || lifecycle === 'duplicate' || lifecycle === 'merged' || status === 'cancelled' || status === 'postponed';
  }

  function hiddenSet(profile) {
    return new Set(toArray(profile && profile.hidden_event_ids).map(asId));
  }

  function isEligibleCandidate(candidate, manifest, profile) {
    if (!candidate) return false;
    const id = candidateId(candidate);
    if (id == null) return false;
    if (asId(id) === asId(currentEventId(manifest))) return false;
    if (isCancelledLike(candidate)) return false;
    if (profileHasConsent(profile) && hiddenSet(profile).has(asId(id))) return false;
    return true;
  }

  function tagAffinity(candidate, profile) {
    const positive = collectWeightedProfileMap(profile || {}, 'positive_tags');
    const positiveCategories = collectWeightedProfileMap(profile || {}, 'positive_categories');
    let sum = 0;
    for (const tag of candidatePositiveMatchingTags(candidate)) sum += mapScore(positive, tag);
    if (candidate && candidate.category) sum += mapScore(positiveCategories, candidate.category);
    return Math.min(1.5, sum / 2);
  }

  function negativeInterestPenalty(candidate, profile) {
    const negative = collectWeightedProfileMap(profile || {}, 'negative_interest_tags');
    let penalty = 0;
    for (const tag of candidatePositiveMatchingTags(candidate)) penalty += Math.max(0, mapScore(negative, tag));
    return Math.min(1.5, penalty);
  }

  function priceMatch(candidate, profile) {
    if (!profile || !profile.price_preferences) return 0;
    if (profile.price_preferences.prefer_free && candidate && candidate.is_free) return 1;
    return 0;
  }

  function timeMatch(candidate, profile) {
    const prefs = collectWeightedProfileMap(profile || {}, 'positive_time_tags');
    let score = 0;
    for (const tag of toArray(candidate && candidate.time_tags)) score += mapScore(prefs, tag);
    return Math.min(1, score);
  }

  function fatiguePenalty(candidate, profile) {
    if (!profile) return 0;
    const id = asId(candidateId(candidate));
    const venue = asId(candidate && (candidate.venue_id || candidate.location_name));
    let penalty = 0;
    if (new Set(toArray(profile.seen_event_ids).map(asId)).has(id)) penalty += 0.7;
    if (venue && new Set(toArray(profile.seen_venue_ids).map(asId)).has(venue)) penalty += 0.35;
    return Math.min(1, penalty);
  }

  function scoreRelatedCandidate(candidate, manifest, profile, options) {
    const weights = Object.assign({}, DEFAULT_WEIGHTS, options && options.weights ? options.weights : {});
    const base = staticCandidateScore(candidate);
    if (!profileHasConsent(profile)) {
      return {
        score: base,
        base_similarity: base,
        reason_codes: toArray(candidate && candidate.reason_codes).slice(),
      };
    }
    const affinity = tagAffinity(candidate, profile);
    const negative = negativeInterestPenalty(candidate, profile);
    const fatigue = fatiguePenalty(candidate, profile);
    const price = priceMatch(candidate, profile);
    const time = timeMatch(candidate, profile);
    const soldOut = String((candidate && (candidate.status || candidate.ticket_status)) || '').toLowerCase() === 'sold_out' ? 1 : 0;
    const exploration = candidate && candidate.exploration_candidate ? 1 : 0;
    const score =
      weights.staticContext * base +
      weights.profileAffinity * affinity +
      weights.priceMatch * price +
      weights.timeMatch * time +
      weights.exploration * exploration -
      weights.negativeInterest * negative -
      weights.fatigue * fatigue -
      weights.soldOut * soldOut;
    const reasonCodes = toArray(candidate && candidate.reason_codes).slice();
    if (affinity > 0) reasonCodes.push('profile:positive_affinity');
    if (negative > 0) reasonCodes.push('profile:negative_interest_penalty');
    if (fatigue > 0) reasonCodes.push('profile:fatigue_penalty');
    if (price > 0) reasonCodes.push('profile:price_match');
    return {
      score,
      base_similarity: base,
      reason_codes: Array.from(new Set(reasonCodes)),
    };
  }

  function applyDiversity(sorted, options) {
    const opts = options || {};
    const maxSameCategory = opts.maxSameCategory == null ? 3 : Number(opts.maxSameCategory);
    const maxSameVenue = opts.maxSameVenue == null ? 2 : Number(opts.maxSameVenue);
    const result = [];
    const postponed = [];
    const categoryCounts = new Map();
    const venueCounts = new Map();
    for (const item of sorted) {
      const candidate = item.candidate;
      const category = candidate.category || candidate.event_type || 'unknown';
      const venue = candidate.venue_id || candidate.location_name || 'unknown';
      const catCount = categoryCounts.get(category) || 0;
      const venueCount = venueCounts.get(venue) || 0;
      if (catCount >= maxSameCategory || venueCount >= maxSameVenue) {
        postponed.push(Object.assign({}, item, { diversity_postponed: true }));
        continue;
      }
      result.push(item);
      categoryCounts.set(category, catCount + 1);
      venueCounts.set(venue, venueCount + 1);
    }
    return result.concat(postponed);
  }

  function rankEventDetailRelated(manifest, profile, options) {
    const opts = options || {};
    const candidates = toArray(manifest && (manifest.related_static || manifest.candidates));
    const activeProfile = isCompatibleProfile(profile, manifest && manifest.feature_schema_version, manifest && manifest.taxonomy_version) ? profile : null;
    const scored = [];
    for (const candidate of candidates) {
      if (!isEligibleCandidate(candidate, manifest, activeProfile)) continue;
      const scoreInfo = scoreRelatedCandidate(candidate, manifest, activeProfile, opts);
      scored.push({ candidate, score: scoreInfo.score, base_similarity: scoreInfo.base_similarity, reason_codes: scoreInfo.reason_codes });
    }
    scored.sort((left, right) => {
      const byScore = right.score - left.score;
      if (byScore !== 0) return byScore;
      const byBase = right.base_similarity - left.base_similarity;
      if (byBase !== 0) return byBase;
      return Number(candidateId(left.candidate) || 0) - Number(candidateId(right.candidate) || 0);
    });
    return applyDiversity(scored, opts).map((item, index) => ({
      event_id: candidateId(item.candidate),
      candidate: item.candidate,
      rank: index,
      personal_score: Number(item.score.toFixed(4)),
      base_similarity: Number(item.base_similarity.toFixed(4)),
      reason_codes: item.reason_codes,
      diversity_postponed: Boolean(item.diversity_postponed),
    }));
  }

  function randomId(prefix) {
    const cryptoObj = (global.crypto || {});
    if (cryptoObj.randomUUID) return `${prefix}-${cryptoObj.randomUUID()}`;
    return `${prefix}-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  }

  function stableHash(value) {
    const text = typeof value === 'string' ? value : JSON.stringify(value);
    let hash = 2166136261;
    for (let index = 0; index < text.length; index += 1) {
      hash ^= text.charCodeAt(index);
      hash = Math.imul(hash, 16777619);
    }
    return (hash >>> 0).toString(16);
  }

  function createServedListHash(ranked, manifest, context, profile) {
    return stableHash({
      surface: SURFACE_EVENT_DETAIL_RELATED,
      current_event_id: currentEventId(manifest),
      algorithm_id: context.algorithmId,
      profile_version: profile && profile.profile_version,
      feature_schema_version: profile && profile.feature_schema_version,
      taxonomy_version: profile && profile.taxonomy_version,
      event_ids: ranked.map((item) => item.event_id),
    });
  }

  function createServedListSummary(ranked, manifest, context) {
    const servedListId = context.servedListId || randomId('served');
    return {
      event_kind: 'served_list_summary',
      served_list_id: servedListId,
      served_list_hash: context.servedListHash || null,
      anon_id: context.anonId || null,
      session_id: context.sessionId || null,
      surface: SURFACE_EVENT_DETAIL_RELATED,
      viewport_class: context.viewportClass,
      layout_mode: context.layoutMode,
      presentation_mode: context.presentationMode,
      current_event_id: currentEventId(manifest),
      algorithm_id: context.algorithmId,
      shown: ranked.map((item) => ({
        event_id: item.event_id,
        rank: item.rank,
        base_similarity: item.base_similarity,
        personal_score: item.personal_score,
        reason_codes: item.reason_codes.slice(0, 8),
      })),
    };
  }

  function createStrongAction(kind, item, context, extra) {
    return Object.assign({
      event_kind: kind,
      event_id: item && item.event_id != null ? item.event_id : null,
      rank: item && item.rank != null ? item.rank : null,
      served_list_id: context.servedListId || null,
      served_list_hash: context.servedListHash || null,
      surface: SURFACE_EVENT_DETAIL_RELATED,
      viewport_class: context.viewportClass,
      layout_mode: context.layoutMode,
      presentation_mode: context.presentationMode,
      algorithm_id: context.algorithmId,
    }, extra || {});
  }

  function createSessionSummary(profile, actionCounts, context) {
    return {
      event_kind: 'session_summary',
      client_summary_id: randomId('summary'),
      anon_id: profile && profile.anon_id || null,
      session_id: profile && profile.session_id || context.sessionId || null,
      surface: SURFACE_EVENT_DETAIL_RELATED,
      viewport_class: context.viewportClass,
      layout_mode: context.layoutMode,
      presentation_mode: context.presentationMode,
      algorithm_id: context.algorithmId,
      event_counts: Object.assign({}, actionCounts || {}),
      positive_tag_delta: {},
      negative_interest_tag_delta: {},
      strong_event_ids: {},
    };
  }

  function createEmptyProfile(seed) {
    return Object.assign({
      consent_ok: true,
      profile_version: PROFILE_VERSION,
      feature_schema_version: FEATURE_SCHEMA_VERSION,
      taxonomy_version: TAXONOMY_VERSION,
      anon_id: randomId('anon'),
      session_id: randomId('session'),
      positive_tags: {},
      negative_interest_tags: {},
      hidden_event_ids: [],
      seen_event_ids: [],
      seen_venue_ids: [],
      price_preferences: { prefer_free: false },
      updated_at: new Date().toISOString(),
    }, seed || {});
  }

  function createEventDetailRelatedController(options) {
    const opts = options || {};
    const doc = opts.document || global.document;
    const win = opts.window || global.window || global;
    if (!doc) throw new Error('document is required');
    const storage = getSafeStorage(win, opts.storage);
    const storageKey = opts.storageKey || DEFAULT_STORAGE_KEY;
    const manifest = opts.manifest || win.__relatedManifest || { related_static: [] };
    const container = typeof opts.container === 'string' ? doc.querySelector(opts.container) : opts.container;
    const status = typeof opts.status === 'string' ? doc.querySelector(opts.status) : opts.status;
    const consentButton = typeof opts.consentButton === 'string' ? doc.querySelector(opts.consentButton) : opts.consentButton;
    const resetButton = typeof opts.resetButton === 'string' ? doc.querySelector(opts.resetButton) : opts.resetButton;
    const telemetrySink = typeof opts.telemetrySink === 'function' ? opts.telemetrySink : function noop() {};
    const servedListDedupeMs = opts.servedListDedupeMs == null ? DEFAULT_SERVED_LIST_DEDUPE_MS : Number(opts.servedListDedupeMs);
    const actionCounts = {};
    const servedListByHash = new Map();
    let lastRenderedViewportClass = null;
    let resizeTimer = null;

    function profile() {
      return readProfile(storage, storageKey);
    }

    function baseContext(algorithmId) {
      const width = Number(win.innerWidth || 1024);
      const currentProfile = profile();
      return {
        viewportClass: viewportClass(width),
        layoutMode: relatedLayoutMode(width),
        presentationMode: relatedPresentationMode(width),
        algorithmId,
        anonId: currentProfile && currentProfile.anon_id,
        sessionId: currentProfile && currentProfile.session_id,
        servedListId: null,
        servedListHash: null,
      };
    }

    function servedListState(hash) {
      const now = Date.now();
      const existing = servedListByHash.get(hash);
      if (existing && now - existing.emittedAt <= servedListDedupeMs) {
        return { servedListId: existing.servedListId, shouldEmit: false };
      }
      const next = { servedListId: randomId('served'), emittedAt: now };
      servedListByHash.set(hash, next);
      return { servedListId: next.servedListId, shouldEmit: true };
    }

    function renderCard(item, ctx) {
      const event = item.candidate;
      const card = doc.createElement('article');
      card.className = 'related-card';
      card.dataset.eventId = String(item.event_id);
      card.dataset.rank = String(item.rank);
      card.dataset.score = String(item.personal_score);
      const title = doc.createElement('h3');
      title.textContent = event.title || 'Событие';
      const meta = doc.createElement('p');
      meta.className = 'meta';
      meta.textContent = [event.city, event.date || event.start_at, event.location_name].filter(Boolean).join(' · ');
      const reasons = doc.createElement('p');
      reasons.className = 'reasons';
      reasons.textContent = toArray(item.reason_codes).slice(0, 4).join(', ');
      const details = doc.createElement('button');
      details.type = 'button';
      details.className = 'details';
      details.textContent = 'Подробнее';
      details.addEventListener('click', () => {
        actionCounts.related_card_click = (actionCounts.related_card_click || 0) + 1;
        telemetrySink(createStrongAction('related_card_click', item, ctx));
      });
      const hide = doc.createElement('button');
      hide.type = 'button';
      hide.className = 'hide';
      hide.textContent = 'Не интересно';
      hide.addEventListener('click', () => {
        const current = profile() || {};
        const hidden = Array.from(new Set(toArray(current.hidden_event_ids).map(asId).concat([asId(item.event_id)])));
        const persisted = writeProfile(storage, storageKey, Object.assign({}, current, { hidden_event_ids: hidden, updated_at: new Date().toISOString() }));
        if (persisted) {
          actionCounts.hide_event = (actionCounts.hide_event || 0) + 1;
          telemetrySink(createStrongAction('hide_event', item, ctx));
        }
        render();
      });
      card.append(title, meta, reasons, details, hide);
      return card;
    }

    function render() {
      if (!container) return [];
      const currentProfile = profile();
      const hasLocalProfile = isCompatibleProfile(currentProfile, manifest.feature_schema_version, manifest.taxonomy_version);
      let algorithmId = hasLocalProfile ? LOCAL_ALGORITHM_ID : STATIC_ALGORITHM_ID;
      if (hasLocalProfile && opts.backendAvailable === false) algorithmId = LOCAL_FALLBACK_ALGORITHM_ID;
      const ctxBase = baseContext(algorithmId);
      const rankedAll = rankEventDetailRelated(manifest, hasLocalProfile ? currentProfile : null, opts.rankOptions || {});
      const defaultLimit = ctxBase.viewportClass === 'mobile' ? 6 : 4;
      const limit = opts.limit == null ? defaultLimit : Number(opts.limit);
      const ranked = rankedAll.slice(0, limit);
      let ctx = ctxBase;
      let shouldEmitServedList = false;
      if (hasLocalProfile) {
        const servedListHash = createServedListHash(ranked, manifest, ctxBase, currentProfile);
        const state = servedListState(servedListHash);
        shouldEmitServedList = state.shouldEmit;
        ctx = Object.assign({}, ctxBase, { servedListId: state.servedListId, servedListHash });
      }
      container.className = `related related--${ctx.presentationMode}`;
      container.dataset.surface = SURFACE_EVENT_DETAIL_RELATED;
      container.dataset.algorithmId = algorithmId;
      container.dataset.viewportClass = ctx.viewportClass;
      container.dataset.layoutMode = ctx.layoutMode;
      container.dataset.presentationMode = ctx.presentationMode;
      lastRenderedViewportClass = ctx.viewportClass;
      if (status) {
        status.textContent = !hasLocalProfile
          ? 'static related fallback'
          : (opts.backendAvailable === false ? 'personalized local fallback: telemetry endpoint unavailable' : 'personalized related');
      }
      container.replaceChildren();
      for (const item of ranked) container.appendChild(renderCard(item, ctx));
      if (hasLocalProfile && shouldEmitServedList) {
        const summary = createServedListSummary(ranked, manifest, ctx);
        telemetrySink(summary);
        if (opts.backendAvailable === false) telemetrySink(createStrongAction('recommendation_fallback_used', null, ctx));
      }
      return ranked;
    }

    function acceptConsent(seedProfile) {
      writeProfile(storage, storageKey, createEmptyProfile(seedProfile || opts.defaultProfile || win.__seedProfile));
      render();
    }

    function resetPersonalization() {
      safeStorageRemove(storage, storageKey);
      render();
    }

    if (consentButton) consentButton.addEventListener('click', () => acceptConsent(win.__seedProfile || opts.defaultProfile));
    if (resetButton) resetButton.addEventListener('click', resetPersonalization);
    function handleResize() {
      if (resizeTimer) clearTimeout(resizeTimer);
      resizeTimer = setTimeout(() => {
        const nextViewportClass = viewportClass(Number(win.innerWidth || 1024));
        if (nextViewportClass !== lastRenderedViewportClass) render();
      }, Number(opts.resizeDebounceMs == null ? 200 : opts.resizeDebounceMs));
    }

    if (win.addEventListener) win.addEventListener('resize', handleResize);

    return {
      render,
      acceptConsent,
      resetPersonalization,
      readProfile: profile,
      rank: () => rankEventDetailRelated(manifest, profile(), opts.rankOptions || {}),
      createSessionSummary: () => {
        const current = profile();
        const algorithmId = isCompatibleProfile(current, manifest.feature_schema_version, manifest.taxonomy_version) ? LOCAL_ALGORITHM_ID : STATIC_ALGORITHM_ID;
        return createSessionSummary(current, actionCounts, baseContext(algorithmId));
      },
    };
  }

  global.KenigEventsPersonalization = {
    DEFAULT_STORAGE_KEY,
    PROFILE_VERSION,
    FEATURE_SCHEMA_VERSION,
    TAXONOMY_VERSION,
    STATIC_ALGORITHM_ID,
    LOCAL_ALGORITHM_ID,
    viewportClass,
    relatedPresentationMode,
    relatedLayoutMode,
    candidatePositiveMatchingTags,
    candidateExclusionTags,
    candidateTags,
    isCompatibleProfile,
    scoreRelatedCandidate,
    rankEventDetailRelated,
    createServedListSummary,
    createStrongAction,
    createSessionSummary,
    createEmptyProfile,
    createEventDetailRelatedController,
    createController: createEventDetailRelatedController,
  };
})(typeof window !== 'undefined' ? window : globalThis);
