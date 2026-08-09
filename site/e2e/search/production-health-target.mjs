const ACCEPTED_SOURCE = 'current_accepted_pointer';
const SECRET_REVIEW_PATH = /^\/_review\/[A-Za-z0-9_-]{43}\/poisk\/$/u;
const SECRET_PREVIEW_PATH = /^\/preview-[a-z0-9][a-z0-9-]{5,}\/poisk\/$/iu;
const SHA40 = /^[0-9a-f]{40}$/u;

export const ACCEPTED_TARGET_RESOLVER_SOURCE = ACCEPTED_SOURCE;

export function redactAcceptedTargetUrl(value) {
  let parsed;
  try {
    parsed = new URL(String(value || ''));
  } catch {
    throw new Error('search_health_target_url_invalid');
  }
  if (parsed.username || parsed.password || parsed.search || parsed.hash) {
    throw new Error('search_health_target_url_invalid');
  }
  const redactedPath = parsed.pathname
    .replace(/^\/_review\/[A-Za-z0-9_-]{43}\//u, '/_review/<redacted>/')
    .replace(/^\/preview-[a-z0-9][a-z0-9-]{5,}\//iu, '/preview-<redacted>/');
  if (redactedPath === parsed.pathname) throw new Error('search_health_target_not_secret');
  return `${parsed.origin}${redactedPath}`;
}

class NormalizedAcceptedTarget {
  #navigationUrl;

  constructor({ navigationUrl, targetRepoSha, acceptedReleaseId, generationIds }) {
    this.source = ACCEPTED_SOURCE;
    this.target_repo_sha = targetRepoSha;
    this.accepted_release_id = acceptedReleaseId;
    this.generation_ids = Object.freeze({ ...generationIds });
    this.target_url_redacted = redactAcceptedTargetUrl(navigationUrl);
    this.#navigationUrl = navigationUrl;
    Object.freeze(this);
  }

  navigationUrl() {
    return this.#navigationUrl;
  }

  toDisplayJSON() {
    return {
      source: this.source,
      target_url: this.target_url_redacted,
      target_repo_sha: this.target_repo_sha,
      accepted_release_id: this.accepted_release_id,
      // Generation identifiers are observability context only. They are not
      // used to accept, route, or compare a target.
      generation_ids: { ...this.generation_ids },
    };
  }

  toJSON() {
    return this.toDisplayJSON();
  }
}

const normalizedGenerationIds = (input) => {
  const source = input?.generation_ids && typeof input.generation_ids === 'object'
    ? input.generation_ids
    : {};
  return Object.fromEntries(Object.entries(source)
    .filter(([key, value]) => /^[a-z][a-z0-9_]{0,47}$/u.test(key) && typeof value === 'string')
    .map(([key, value]) => [key, value.slice(0, 96)]));
};

/**
 * Normalize only the authoritative current-accepted pointer. A latest Kaggle
 * job, a generated snapshot, and the public /poisk/ URL are not fallback
 * sources and therefore cannot pass this boundary.
 */
export function normalizeAcceptedTargetResolverResult(input) {
  if (!input || typeof input !== 'object' || input.source !== ACCEPTED_SOURCE) {
    throw new Error('search_health_target_source_invalid');
  }
  if (input.latest_kaggle_job != null || input.public_poisk_fallback != null || input.fallback_url != null) {
    throw new Error('search_health_target_fallback_forbidden');
  }
  const navigationUrl = String(input.target_url || '');
  let parsed;
  try {
    parsed = new URL(navigationUrl);
  } catch {
    throw new Error('search_health_target_url_invalid');
  }
  if (
    parsed.protocol !== 'https:'
    || parsed.username || parsed.password || parsed.search || parsed.hash
    || (!SECRET_REVIEW_PATH.test(parsed.pathname) && !SECRET_PREVIEW_PATH.test(parsed.pathname))
  ) {
    throw new Error('search_health_target_url_invalid');
  }
  const targetRepoSha = String(input.target_repo_sha || '').trim().toLowerCase();
  if (!SHA40.test(targetRepoSha)) throw new Error('search_health_target_repo_sha_invalid');
  const acceptedReleaseId = String(input.accepted_release_id || '').trim();
  if (!/^[A-Za-z0-9][A-Za-z0-9._:-]{0,95}$/u.test(acceptedReleaseId)) {
    throw new Error('search_health_accepted_release_id_invalid');
  }

  // checkout_repo_sha is intentionally neither required nor compared. The
  // checked-out planner code and the accepted deployed target are independent.
  return new NormalizedAcceptedTarget({
    navigationUrl: parsed.href,
    targetRepoSha,
    acceptedReleaseId,
    generationIds: normalizedGenerationIds(input),
  });
}

const samePinnedTarget = (left, right) => (
  left.navigationUrl() === right.navigationUrl()
  && left.target_repo_sha === right.target_repo_sha
  && left.accepted_release_id === right.accepted_release_id
);

export function assessAcceptedTargetSupersession(pinned, observed) {
  if (!(pinned instanceof NormalizedAcceptedTarget) || !(observed instanceof NormalizedAcceptedTarget)) {
    throw new Error('search_health_target_not_normalized');
  }
  const targetSuperseded = !samePinnedTarget(pinned, observed);
  return Object.freeze({
    target_superseded: targetSuperseded,
    retry_allowed: false,
    product_failure: false,
    product_incident: false,
  });
}

/**
 * One run pins the first resolver value. A later pointer observation may mark
 * that immutable pin superseded, but it never mutates the pin or authorizes a
 * retry after Search dispatch.
 */
export function createAcceptedTargetRun(resolver) {
  if (typeof resolver !== 'function') throw new Error('search_health_target_resolver_missing');
  let pinnedPromise;
  let resolverCalls = 0;

  const resolveNormalized = async () => {
    resolverCalls += 1;
    return normalizeAcceptedTargetResolverResult(await resolver());
  };

  return Object.freeze({
    pin() {
      pinnedPromise ??= resolveNormalized();
      return pinnedPromise;
    },
    async observeSupersession() {
      const pinned = await this.pin();
      const observed = await resolveNormalized();
      return assessAcceptedTargetSupersession(pinned, observed);
    },
    resolverCallCount() {
      return resolverCalls;
    },
  });
}
