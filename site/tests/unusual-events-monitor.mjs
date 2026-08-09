import { mkdir, readFile, rename, writeFile } from 'node:fs/promises';
import { basename, dirname, join, resolve } from 'node:path';

const MAX_CARDS = 30;
const MAX_LINKS = 30;
const MAX_DIAGNOSTICS = 20;
const MAX_TEXT = 240;
const SHA256 = /^[a-f0-9]{64}$/u;
const REPO_SHA = /^[a-f0-9]{40}$/u;
const ISO_DATE = /^\d{4}-\d{2}-\d{2}$/u;
const PROVIDER_PATTERN = /(?:generativelanguage\.googleapis\.com|api\.openai\.com|api\.anthropic\.com|huggingface\.co|inference-api|\/(?:functions\/v1\/)?(?:event-search|semantic|embeddings?|unusual-(?:score|rank))\b|\bbge-m3\b|\bgemini\b)/iu;

export const UNUSUAL_BROWSER_VIEWPORTS = Object.freeze([
  Object.freeze({ name: 'mobile', width: 390, height: 844 }),
  Object.freeze({ name: 'desktop', width: 1728, height: 900 }),
]);

const nonEmpty = (value, name, maximum = 256) => {
  const result = typeof value === 'string' ? value.trim() : '';
  if (!result || result.length > maximum) throw new Error(`health_${name}_invalid`);
  return result;
};
const boundedInteger = (value, name, maximum = MAX_CARDS) => {
  if (!Number.isSafeInteger(value) || value < 0 || value > maximum) {
    throw new Error(`health_${name}_invalid`);
  }
  return value;
};
const identity = (value, name) => {
  if (!['string', 'number'].includes(typeof value)) throw new Error(`health_${name}_invalid`);
  const result = String(value).trim();
  if (!result || result.length > 128) throw new Error(`health_${name}_invalid`);
  return result;
};
const arrayField = (value, name) => {
  if (!Array.isArray(value) || value.length > MAX_CARDS) throw new Error(`health_${name}_invalid`);
  return value;
};
const unique = (items) => new Set(items).size === items.length;

function selectedRow(row, index) {
  if (!row || typeof row !== 'object') throw new Error(`health_selected_${index}_invalid`);
  const eventId = identity(row.event_id ?? row.eventId, `selected_${index}_event_id`);
  const conceptId = identity(row.concept_id ?? row.conceptId, `selected_${index}_concept_id`);
  const title = nonEmpty(row.title, `selected_${index}_title`, 500);
  const href = nonEmpty(row.path || row.url || row.href, `selected_${index}_path`, 2048);
  let path;
  try { path = new URL(href, 'https://health.invalid/').pathname; }
  catch { throw new Error(`health_selected_${index}_path_invalid`); }
  if (!path.startsWith('/') || path.includes('/../')) throw new Error(`health_selected_${index}_path_invalid`);
  const startDate = row.start_date == null ? null : String(row.start_date).trim();
  const endDate = row.end_date == null ? null : String(row.end_date).trim();
  if (startDate && !ISO_DATE.test(startDate)) throw new Error(`health_selected_${index}_start_date_invalid`);
  if (endDate && !ISO_DATE.test(endDate)) throw new Error(`health_selected_${index}_end_date_invalid`);
  return Object.freeze({
    event_id: eventId,
    concept_id: conceptId,
    title,
    path,
    start_date: startDate,
    end_date: endDate,
    family: row.family == null ? null : String(row.family).slice(0, 128),
    image_required: row.image_required === true,
  });
}

export function normalizeUnusualHealth(raw) {
  if (!raw || typeof raw !== 'object' || raw.schema_version !== 'unusual-events-health-v1') {
    throw new Error('health_schema_version_invalid');
  }
  const publication = raw.publication;
  const feed = raw.feed || publication?.feed; // old pre-release receipts nested this once
  const contracts = raw.contracts || {};
  if (!publication || typeof publication !== 'object') throw new Error('health_publication_invalid');
  if (!feed || typeof feed !== 'object') throw new Error('health_feed_invalid');
  if (typeof publication.expected !== 'boolean') throw new Error('health_publication_expected_invalid');
  if (typeof publication.indexable !== 'boolean') throw new Error('health_publication_indexable_invalid');
  const manifestSha256 = nonEmpty(publication.manifest_sha256, 'manifest_sha256', 64).toLowerCase();
  if (!SHA256.test(manifestSha256)) throw new Error('health_manifest_sha256_invalid');
  const selectedCount = boundedInteger(feed.selected_count, 'selected_count');
  const targetCount = boundedInteger(feed.target_count, 'target_count');
  const minimumPublishCount = boundedInteger(feed.minimum_publish_count, 'minimum_publish_count');
  if (minimumPublishCount > targetCount || selectedCount > targetCount) {
    throw new Error('health_feed_count_contract_invalid');
  }
  const selected = arrayField(feed.selected, 'selected').map(selectedRow);
  const eventIds = arrayField(feed.visible_event_ids, 'visible_event_ids').map((value, index) => identity(value, `visible_event_ids_${index}`));
  const conceptIds = arrayField(feed.visible_concept_ids, 'visible_concept_ids').map((value, index) => identity(value, `visible_concept_ids_${index}`));
  if (selected.length !== selectedCount || eventIds.length !== selectedCount || conceptIds.length !== selectedCount) {
    throw new Error('health_selected_count_parity_invalid');
  }
  if (!unique(eventIds) || !unique(conceptIds)) throw new Error('health_visible_ids_duplicate');
  if (selected.some((row, index) => row.event_id !== eventIds[index] || row.concept_id !== conceptIds[index])) {
    throw new Error('health_selected_order_invalid');
  }
  const visibleOutputSha256 = nonEmpty(contracts.visible_output_sha256, 'visible_output_sha256', 64).toLowerCase();
  if (!SHA256.test(visibleOutputSha256)) throw new Error('health_visible_output_sha256_invalid');
  const repoSha = nonEmpty(raw.repo_sha, 'repo_sha', 40).toLowerCase();
  if (!REPO_SHA.test(repoSha)) throw new Error('health_repo_sha_invalid');
  const canonicalPath = String(publication.canonical_path || '/neobychnoe/').trim();
  if (!/^\/[A-Za-z0-9._~/-]+\/?$/u.test(canonicalPath)
    || canonicalPath.includes('..') || /\/_review\//u.test(canonicalPath) || canonicalPath.length > 256) {
    throw new Error('health_canonical_path_invalid');
  }
  const contentReadiness = String(raw.content_readiness || raw.status?.content_readiness || 'UNKNOWN').trim().toUpperCase();
  const healthStatus = String(raw.health_status || raw.status?.health_status || 'UNKNOWN').trim().toUpperCase();
  return Object.freeze({
    schema_version: raw.schema_version,
    run_id: nonEmpty(raw.run_id, 'run_id', 128),
    repo_sha: repoSha,
    health_status: healthStatus.slice(0, 64),
    content_readiness: contentReadiness.slice(0, 64),
    as_of_date: ISO_DATE.test(String(raw.as_of_date || '')) ? String(raw.as_of_date) : new Date().toISOString().slice(0, 10),
    publication: Object.freeze({
      expected: publication.expected,
      indexable: publication.indexable,
      manifest_sha256: manifestSha256,
      canonical_path: canonicalPath.endsWith('/') ? canonicalPath : `${canonicalPath}/`,
    }),
    feed: Object.freeze({
      selected_count: selectedCount,
      target_count: targetCount,
      minimum_publish_count: minimumPublishCount,
      visible_event_ids: Object.freeze(eventIds),
      visible_concept_ids: Object.freeze(conceptIds),
      selected: Object.freeze(selected),
    }),
    contracts: Object.freeze({ visible_output_sha256: visibleOutputSha256 }),
  });
}

export function isSemanticProviderUrl(value) {
  return PROVIDER_PATTERN.test(String(value || ''));
}

export function redactCandidateSecrets(value, secrets = []) {
  let result = String(value ?? '');
  for (const secret of secrets.filter(Boolean).sort((a, b) => b.length - a.length)) {
    result = result.split(secret).join('[REDACTED_CANDIDATE]');
  }
  result = result
    .replace(/\/_review\/[A-Za-z0-9_-]{16,}(?=\/|\?|#|$)/gu, '/_review/[REDACTED_CANDIDATE]')
    .replace(/([?&](?:token|key|signature|sig|auth|bearer)=)[^&#\s]+/giu, '$1[REDACTED]')
    .replace(/Bearer\s+[A-Za-z0-9._~+\/-]+=*/giu, 'Bearer [REDACTED]');
  return result.slice(0, MAX_TEXT);
}

export function resolveUnusualTarget(env, canonicalPath) {
  const raw = String(env.UNUSUAL_EVENTS_BASE_URL || env.UNUSUAL_EVENTS_CANDIDATE_URL || '').trim();
  if (!raw) throw new Error('missing_unusual_events_base_url');
  let target;
  try { target = new URL(raw); }
  catch { throw new Error('unusual_events_base_url_invalid'); }
  if (!['http:', 'https:'].includes(target.protocol) || target.username || target.password) {
    throw new Error('unusual_events_base_url_invalid');
  }
  const normalizedCanonical = canonicalPath.replace(/^\/+|\/+$/gu, '');
  const normalizedPath = target.pathname.replace(/\/+$/gu, '');
  if (!normalizedPath.endsWith(`/${normalizedCanonical}`)) {
    target.pathname = `${normalizedPath}/${normalizedCanonical}/`.replace(/\/{2,}/gu, '/');
  } else {
    target.pathname = `${normalizedPath}/`;
  }
  return Object.freeze({
    url: target.toString(),
    secrets: Object.freeze([target.toString(), raw, target.origin + normalizedPath, ...normalizedPath.split('/').filter((part) => part.length >= 16)]),
  });
}

const failure = (failures, code, details = null) => {
  if (failures.length >= MAX_DIAGNOSTICS) return;
  failures.push(details == null ? { code } : { code, details });
};
const safeText = (value, secrets) => redactCandidateSecrets(value, secrets);
const pathMatches = (actual, expected) => {
  try {
    const actualPath = new URL(actual).pathname.replace(/\/+$/u, '');
    const expectedPath = new URL(expected, 'https://health.invalid/').pathname.replace(/\/+$/u, '');
    return actualPath === expectedPath || actualPath.endsWith(expectedPath);
  } catch { return false; }
};
const duplicateValues = (values) => [...new Set(values.filter((value, index) => values.indexOf(value) !== index))].slice(0, MAX_DIAGNOSTICS);

async function inspectCards(page, expected, asOfDate, failures) {
  const locator = page.locator('[data-unusual-card]');
  const count = await locator.count();
  const cards = [];
  for (let index = 0; index < Math.min(count, MAX_CARDS); index += 1) {
    const card = locator.nth(index);
    await card.scrollIntoViewIfNeeded().catch(() => {});
    const observed = await card.evaluate(async (node) => {
      const eventCard = node.querySelector('[data-event-card]');
      const titleLink = node.querySelector('[data-card-title]');
      const image = node.querySelector('[data-card-image]');
      if (image && !image.complete) {
        await new Promise((resolveWait) => {
          const timer = setTimeout(resolveWait, 5000);
          image.addEventListener('load', () => { clearTimeout(timer); resolveWait(); }, { once:true });
          image.addEventListener('error', () => { clearTimeout(timer); resolveWait(); }, { once:true });
        });
      }
      if (image?.complete && image.naturalWidth > 0) await image.decode().catch(() => {});
      const fallback = node.querySelector('[data-card-image-fallback],.event-card__fallback');
      const fallbackStyle = fallback ? getComputedStyle(fallback) : null;
      const rect = node.getBoundingClientRect();
      return {
        event_id: String(node.getAttribute('data-unusual-event-id') || eventCard?.getAttribute('data-event-id') || ''),
        concept_id: String(node.getAttribute('data-unusual-concept-id') || ''),
        title: String(titleLink?.textContent || '').trim(),
        href: String(titleLink?.href || eventCard?.getAttribute('data-card-href') || ''),
        required_data: Boolean(eventCard && titleLink && titleLink.getAttribute('href')),
        image_present: Boolean(image),
        image_decoded: Boolean(image?.complete && image.naturalWidth > 0 && image.naturalHeight > 0),
        fallback_visible: Boolean(fallback && fallbackStyle?.display !== 'none' && fallbackStyle?.visibility !== 'hidden'),
        horizontal_overflow: Math.max(0, -rect.left, rect.right - document.documentElement.clientWidth),
      };
    });
    const expectedRow = expected[index];
    const receiptCard = {
      event_id: observed.event_id,
      concept_id: observed.concept_id,
      required_data: observed.required_data,
      image_state: observed.image_decoded ? 'decoded' : (observed.fallback_visible ? 'fallback' : 'broken'),
    };
    cards.push(receiptCard);
    if (!expectedRow) continue;
    if (!observed.required_data) failure(failures, 'card_required_data_missing', { index });
    if (observed.title !== expectedRow.title) failure(failures, 'card_title_mismatch', { index });
    if (!pathMatches(observed.href, expectedRow.path)) failure(failures, 'card_href_mismatch', { index });
    const activeThrough = expectedRow.end_date || expectedRow.start_date;
    if (activeThrough && activeThrough < asOfDate) failure(failures, 'card_date_expired', { index, active_through: activeThrough });
    if (expectedRow.image_required && !observed.image_decoded) failure(failures, 'card_required_image_not_decoded', { index });
    if (!observed.image_decoded && !observed.fallback_visible) failure(failures, 'card_image_or_fallback_missing', { index });
    if (observed.horizontal_overflow > 1) failure(failures, 'card_horizontal_overflow', { index });
  }
  return cards;
}

async function checkLinks(context, cards, failures) {
  const uniqueLinks = [...new Set(cards.map((card) => card.href).filter(Boolean))].slice(0, MAX_LINKS);
  const results = [];
  for (let index = 0; index < uniqueLinks.length; index += 1) {
    try {
      const response = await context.request.get(uniqueLinks[index], { failOnStatusCode:false, maxRedirects:5, timeout:10_000 });
      results.push({ index, status:response.status(), ok:response.ok() });
      if (!response.ok()) failure(failures, 'event_link_http_error', { index, status:response.status() });
    } catch {
      results.push({ index, status:null, ok:false });
      failure(failures, 'event_link_request_failed', { index });
    }
  }
  return { checked_count:results.length, truncated:cards.length > MAX_LINKS, results };
}

async function inspectViewport({ browser, viewport, health, target, screenshotDir }) {
  const context = await browser.newContext({ viewport:{ width:viewport.width, height:viewport.height }, reducedMotion:'reduce' });
  const page = await context.newPage();
  const failures = [];
  const diagnostics = { console_errors:[], page_errors:[], request_errors:[], semantic_provider_calls:[] };
  const observe = (bucket, value) => {
    if (bucket.length < MAX_DIAGNOSTICS) bucket.push(safeText(value, target.secrets));
  };
  page.on('console', (message) => { if (message.type() === 'error') observe(diagnostics.console_errors, message.text()); });
  page.on('pageerror', (error) => observe(diagnostics.page_errors, error?.message || error));
  page.on('request', (request) => {
    if (isSemanticProviderUrl(request.url())) observe(diagnostics.semantic_provider_calls, `${request.method()} provider-request`);
  });
  page.on('requestfailed', (request) => observe(diagnostics.request_errors, `${request.resourceType()}:${request.failure()?.errorText || 'failed'}`));
  page.on('response', (response) => {
    if (response.status() >= 400) observe(diagnostics.request_errors, `${response.request().resourceType()}:http-${response.status()}`);
  });

  let httpStatus = null;
  let domState = 'unknown';
  let shellError = false;
  let cards = [];
  let links = { checked_count:0, truncated:false, results:[] };
  let overflowPx = null;
  let canonical = { present:false, path_matches:false, index_contract:false };
  const screenshotName = `unusual-events-${viewport.name}-${viewport.width}x${viewport.height}.png`;
  try {
    const response = await page.goto(target.url, { waitUntil:'domcontentloaded', timeout:30_000 });
    httpStatus = response?.status() ?? null;
    if (!response || response.status() !== 200) failure(failures, 'page_http_status', { status:httpStatus });
    await page.locator('[data-unusual-feed="ready"],[data-unusual-feed-ready="true"],[data-unusual-feed="empty"],[data-unusual-feed-empty]').first().waitFor({ state:'attached', timeout:10_000 }).catch(() => {});
    shellError = await page.evaluate(() => {
      if (document.querySelector('[data-error-page],[data-page-error],[data-not-found],main[data-status="error"]')) return true;
      const heading = document.querySelector('main h1,h1')?.textContent?.trim() || '';
      return /^(?:404|500|error|ошибка|страница не найдена)(?:\s|:|$)/iu.test(heading);
    });
    if (shellError) failure(failures, 'page_error_shell');
    const ready = await page.locator('[data-unusual-feed="ready"],[data-unusual-feed-ready="true"]').count() > 0;
    const empty = await page.locator('[data-unusual-feed="empty"],[data-unusual-feed-empty]').count() > 0;
    domState = ready && !empty ? 'ready' : (empty && !ready ? 'empty' : 'invalid');
    if (domState === 'invalid') failure(failures, 'feed_state_invalid');

    cards = await inspectCards(page, health.feed.selected, health.as_of_date, failures);
    const eventIds = cards.map((card) => card.event_id);
    const conceptIds = cards.map((card) => card.concept_id);
    if (cards.length !== health.feed.selected_count) failure(failures, 'selected_count_mismatch', { expected:health.feed.selected_count, actual:cards.length });
    if (JSON.stringify(eventIds) !== JSON.stringify(health.feed.visible_event_ids)) failure(failures, 'ordered_event_ids_mismatch');
    if (JSON.stringify(conceptIds) !== JSON.stringify(health.feed.visible_concept_ids)) failure(failures, 'ordered_concept_ids_mismatch');
    if (duplicateValues(eventIds).length) failure(failures, 'duplicate_event_ids');
    if (duplicateValues(conceptIds).length) failure(failures, 'duplicate_concept_ids');
    if (health.publication.expected && health.content_readiness === 'READY') {
      if (domState !== 'ready') failure(failures, 'ready_feed_missing');
      if (cards.length < health.feed.minimum_publish_count) failure(failures, 'minimum_publish_count_not_met');
    } else if (domState !== 'empty' || cards.length !== 0) {
      failure(failures, 'blocked_feed_not_empty');
    }

    const canonicalObserved = await page.evaluate(() => ({
      href:document.querySelector('link[rel="canonical"]')?.href || '',
      robots:document.querySelector('meta[name="robots"]')?.content || '',
    }));
    const noindex = /(?:^|[,\s])(?:noindex|none)(?:$|[,\s])/iu.test(canonicalObserved.robots);
    canonical = {
      present:Boolean(canonicalObserved.href),
      path_matches:pathMatches(canonicalObserved.href, health.publication.canonical_path),
      index_contract:health.publication.indexable ? !noindex : noindex,
    };
    if (!canonical.present || !canonical.path_matches) failure(failures, 'canonical_contract_failed');
    if (!canonical.index_contract) failure(failures, 'index_contract_failed');
    overflowPx = await page.evaluate(() => Math.max(0, document.documentElement.scrollWidth - document.documentElement.clientWidth));
    if (overflowPx > 1) failure(failures, 'page_horizontal_overflow', { pixels:Math.ceil(overflowPx) });
    links = await checkLinks(context, await page.locator('[data-unusual-card] [data-card-title]').evaluateAll((nodes) => nodes.map((node) => ({ href:node.href }))), failures);
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(100);
  } catch (error) {
    failure(failures, 'browser_verification_exception', { message:safeText(error?.message || error, target.secrets) });
  } finally {
    await page.screenshot({ path:join(screenshotDir, screenshotName), fullPage:true }).catch((error) => {
      failure(failures, 'screenshot_failed', { message:safeText(error?.message || error, target.secrets) });
    });
    await context.close().catch(() => {});
  }
  if (diagnostics.console_errors.length) failure(failures, 'console_errors', { count:diagnostics.console_errors.length });
  if (diagnostics.page_errors.length) failure(failures, 'page_errors', { count:diagnostics.page_errors.length });
  if (diagnostics.request_errors.length) failure(failures, 'request_errors', { count:diagnostics.request_errors.length });
  if (diagnostics.semantic_provider_calls.length) failure(failures, 'semantic_provider_calls', { count:diagnostics.semantic_provider_calls.length });
  return {
    viewport,
    screenshot:basename(screenshotName),
    http_status:httpStatus,
    error_shell:shellError,
    dom_state:domState,
    card_count:cards.length,
    visible_event_ids:cards.map((card) => card.event_id),
    visible_concept_ids:cards.map((card) => card.concept_id),
    cards:cards.map(({ event_id, concept_id, required_data, image_state }) => ({ event_id, concept_id, required_data, image_state })),
    links,
    canonical,
    horizontal_overflow_px:overflowPx,
    diagnostics,
    passed:failures.length === 0,
    failures,
  };
}

export function buildUnusualBrowserReceipt(health, viewports, startedAt, completedAt) {
  const boundedViewports = viewports.slice(0, UNUSUAL_BROWSER_VIEWPORTS.length).map((entry) => ({
    ...entry,
    visible_event_ids:(entry.visible_event_ids || []).slice(0, MAX_CARDS),
    visible_concept_ids:(entry.visible_concept_ids || []).slice(0, MAX_CARDS),
    cards:(entry.cards || []).slice(0, MAX_CARDS),
    links:{
      ...(entry.links || {}),
      results:(entry.links?.results || []).slice(0, MAX_LINKS),
    },
    diagnostics:Object.fromEntries(Object.entries(entry.diagnostics || {}).map(([key, values]) => (
      [key, Array.isArray(values) ? values.slice(0, MAX_DIAGNOSTICS) : []]
    ))),
    failures:(entry.failures || []).slice(0, MAX_DIAGNOSTICS),
  }));
  const browserMechanicsPassed = boundedViewports.length === UNUSUAL_BROWSER_VIEWPORTS.length
    && boundedViewports.every((entry) => entry.passed);
  // WATCH is an accepted non-incident state (for example a feed between the
  // publication minimum and target). It must still prove exact browser parity.
  const upstreamHealthy = ['HEALTHY', 'WATCH', 'READY', 'PASS', 'PASSED', 'OK'].includes(health.health_status);
  const readyContract = health.publication.expected
    && upstreamHealthy
    && health.content_readiness === 'READY'
    && health.feed.selected_count >= health.feed.minimum_publish_count;
  const pageManifestMatch = readyContract && browserMechanicsPassed && boundedViewports.every((entry) => (
    entry.dom_state === 'ready'
    && entry.card_count === health.feed.selected_count
    && JSON.stringify(entry.visible_event_ids) === JSON.stringify(health.feed.visible_event_ids)
    && JSON.stringify(entry.visible_concept_ids) === JSON.stringify(health.feed.visible_concept_ids)
  ));
  return {
    schema_version:'unusual-events-browser-receipt-v1',
    started_at:startedAt,
    completed_at:completedAt,
    run_id:health.run_id,
    repo_sha:health.repo_sha,
    health_status:health.health_status,
    content_readiness:health.content_readiness,
    publication:{ ...health.publication },
    feed:{
      selected_count:health.feed.selected_count,
      target_count:health.feed.target_count,
      minimum_publish_count:health.feed.minimum_publish_count,
      visible_event_ids:[...health.feed.visible_event_ids],
      visible_concept_ids:[...health.feed.visible_concept_ids],
    },
    contracts:{ visible_output_sha256:health.contracts.visible_output_sha256 },
    browser_mechanics_passed:browserMechanicsPassed,
    page_manifest_match:pageManifestMatch,
    status:pageManifestMatch ? 'READY' : (browserMechanicsPassed ? 'BLOCKED' : 'FAILED'),
    viewports:boundedViewports,
  };
}

async function atomicJson(path, value) {
  await mkdir(dirname(path), { recursive:true });
  const temporary = `${path}.tmp-${process.pid}`;
  await writeFile(temporary, `${JSON.stringify(value, null, 2)}\n`, { encoding:'utf8', mode:0o600 });
  await rename(temporary, path);
}

export async function runUnusualEventsBrowserMonitor({ env = process.env, playwright } = {}) {
  const healthFile = String(env.UNUSUAL_EVENTS_HEALTH_FILE || '').trim();
  const screenshotDir = String(env.UNUSUAL_EVENTS_SCREENSHOT_DIR || '').trim();
  const receiptPath = String(env.UNUSUAL_EVENTS_BROWSER_RECEIPT || '').trim();
  if (!healthFile) throw new Error('missing_unusual_events_health_file');
  if (!screenshotDir) throw new Error('missing_unusual_events_screenshot_dir');
  if (!receiptPath) throw new Error('missing_unusual_events_browser_receipt');
  const health = normalizeUnusualHealth(JSON.parse(await readFile(resolve(healthFile), 'utf8')));
  const target = resolveUnusualTarget(env, health.publication.canonical_path);
  await mkdir(resolve(screenshotDir), { recursive:true });
  const startedAt = new Date().toISOString();
  let browser;
  let viewports = [];
  try {
    const runtime = playwright || await import('playwright');
    browser = await runtime.chromium.launch({ headless:true });
    for (const viewport of UNUSUAL_BROWSER_VIEWPORTS) {
      viewports.push(await inspectViewport({ browser, viewport, health, target, screenshotDir:resolve(screenshotDir) }));
    }
  } catch (error) {
    viewports = UNUSUAL_BROWSER_VIEWPORTS.map((viewport) => ({
      viewport,
      screenshot:`unusual-events-${viewport.name}-${viewport.width}x${viewport.height}.png`,
      http_status:null,
      error_shell:false,
      dom_state:'unknown',
      card_count:0,
      visible_event_ids:[],
      visible_concept_ids:[],
      cards:[],
      links:{ checked_count:0, truncated:false, results:[] },
      canonical:{ present:false, path_matches:false, index_contract:false },
      horizontal_overflow_px:null,
      diagnostics:{ console_errors:[], page_errors:[], request_errors:[], semantic_provider_calls:[] },
      passed:false,
      failures:[{ code:'browser_launch_failed', details:{ message:safeText(error?.message || error, target.secrets) } }],
    }));
  } finally {
    await browser?.close().catch(() => {});
  }
  const receipt = buildUnusualBrowserReceipt(health, viewports, startedAt, new Date().toISOString());
  await atomicJson(resolve(receiptPath), receipt);
  const safeSummary = {
    schema_version:receipt.schema_version,
    run_id:receipt.run_id,
    status:receipt.status,
    page_manifest_match:receipt.page_manifest_match,
    browser_mechanics_passed:receipt.browser_mechanics_passed,
    receipt:basename(receiptPath),
  };
  process.stdout.write(`${JSON.stringify(safeSummary)}\n`);
  if (!receipt.browser_mechanics_passed || (health.content_readiness === 'READY' && !receipt.page_manifest_match)) {
    const error = new Error('unusual_events_browser_verification_failed');
    error.receipt = receipt;
    throw error;
  }
  return receipt;
}
