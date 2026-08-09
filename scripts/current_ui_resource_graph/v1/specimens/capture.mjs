import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { createReadStream, existsSync, mkdirSync, readFileSync, statSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { dirname, extname, join, normalize, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { inflateSync } from 'node:zlib';
import { buildSpecimenRegistry } from './registry.mjs';
import { assertEvidencePacket, assertSpecimenRegistry, stableHash } from './validate.mjs';
import { capturePlaywrightStablePair } from '../evidence.mjs';

const sha = (value) => createHash('sha256').update(value).digest('hex');
const MIME = { '.html': 'text/html; charset=utf-8', '.css': 'text/css', '.js': 'text/javascript', '.svg': 'image/svg+xml', '.webp': 'image/webp', '.png': 'image/png', '.jpg': 'image/jpeg' };

export function startSpecimenServer({ dist, host = '127.0.0.1', port = 0 }) {
  const root = resolve(dist);
  if (!existsSync(root)) throw new Error('Built specimen dist is missing');
  const server = createServer((request, response) => {
    const pathname = decodeURIComponent(new URL(request.url || '/', `http://${host}`).pathname);
    const clean = normalize(pathname).replace(/^(?:\.\.(?:\/|\\|$))+/u, '').replace(/^[/\\]+/u, '');
    let target = resolve(root, clean);
    if (!target.startsWith(`${root}/`) && target !== root) { response.writeHead(403).end(); return; }
    if (existsSync(target) && statSync(target).isDirectory()) target = join(target, 'index.html');
    if (!existsSync(target) && !extname(target) && existsSync(`${target}.html`)) target = `${target}.html`;
    if (!existsSync(target) || !statSync(target).isFile()) { response.writeHead(404).end('not found'); return; }
    response.setHeader('content-type', MIME[extname(target)] || 'application/octet-stream');
    createReadStream(target).pipe(response);
  });
  return new Promise((accept, reject) => {
    server.once('error', reject);
    server.listen(port, host, () => {
      const address = server.address();
      accept({ baseUrl: `http://${host}:${address.port}`, close: () => new Promise((done, fail) => server.close((error) => error ? fail(error) : done())) });
    });
  });
}

function paeth(a, b, c) {
  const p = a + b - c; const pa = Math.abs(p - a); const pb = Math.abs(p - b); const pc = Math.abs(p - c);
  return pa <= pb && pa <= pc ? a : pb <= pc ? b : c;
}
export function decodePngRgb(buffer) {
  if (!buffer.subarray(0, 8).equals(Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]))) throw new Error('Not a PNG screenshot');
  let offset = 8; let width; let height; let depth; let colorType; let interlace; const chunks = [];
  while (offset < buffer.length) {
    const length = buffer.readUInt32BE(offset); const type = buffer.toString('ascii', offset + 4, offset + 8); const data = buffer.subarray(offset + 8, offset + 8 + length); offset += 12 + length;
    if (type === 'IHDR') { width = data.readUInt32BE(0); height = data.readUInt32BE(4); depth = data[8]; colorType = data[9]; interlace = data[12]; }
    if (type === 'IDAT') chunks.push(data);
    if (type === 'IEND') break;
  }
  if (depth !== 8 || interlace !== 0 || ![0, 2, 4, 6].includes(colorType)) throw new Error('Unsupported PNG format for deterministic dHash');
  const channels = ({ 0: 1, 2: 3, 4: 2, 6: 4 })[colorType]; const stride = width * channels;
  const raw = inflateSync(Buffer.concat(chunks)); const pixels = Buffer.alloc(stride * height); let source = 0;
  for (let y = 0; y < height; y += 1) {
    const filter = raw[source++]; const row = pixels.subarray(y * stride, (y + 1) * stride); const previous = y ? pixels.subarray((y - 1) * stride, y * stride) : null;
    for (let x = 0; x < stride; x += 1) {
      const value = raw[source++]; const left = x >= channels ? row[x - channels] : 0; const up = previous ? previous[x] : 0; const upLeft = previous && x >= channels ? previous[x - channels] : 0;
      row[x] = (value + (filter === 0 ? 0 : filter === 1 ? left : filter === 2 ? up : filter === 3 ? Math.floor((left + up) / 2) : filter === 4 ? paeth(left, up, upLeft) : (() => { throw new Error('Invalid PNG filter'); })())) & 255;
    }
  }
  return { width, height, channels, pixels };
}

export function pngDifferenceHash(buffer) {
  const { width, height, channels, pixels } = decodePngRgb(buffer); const luminance = [];
  for (let y = 0; y < 8; y += 1) for (let x = 0; x < 9; x += 1) {
    const px = Math.min(width - 1, Math.floor((x + 0.5) * width / 9)); const py = Math.min(height - 1, Math.floor((y + 0.5) * height / 8)); const index = py * width * channels + px * channels;
    const r = pixels[index]; const g = channels === 1 || channels === 2 ? r : pixels[index + 1]; const b = channels === 1 || channels === 2 ? r : pixels[index + 2];
    luminance.push(299 * r + 587 * g + 114 * b);
  }
  let bits = 0n;
  for (let y = 0; y < 8; y += 1) for (let x = 0; x < 8; x += 1) bits = (bits << 1n) | BigInt(luminance[y * 9 + x] > luminance[y * 9 + x + 1]);
  return bits.toString(16).padStart(16, '0');
}

export function safeCapturedValue(value, maximum = 512) {
  const text = String(value ?? '');
  if (text.length <= maximum && !/(?:https?:\/\/|\/_review\/|authorization|bearer\s|password|access[_-]?token|api[_-]?key|sb_(?:secret|publishable))/iu.test(text)) return text;
  return { redacted: true, length: text.length, sha256: sha(text) };
}

export async function collectBoundedElementFacts(locator, partSelectors) {
  const facts = await locator.evaluate((node, parts) => {
    const style = getComputedStyle(node); const rect = node.getBoundingClientRect(); const before = getComputedStyle(node, '::before'); const after = getComputedStyle(node, '::after');
    const allowed = /^(?:data-|aria-|role$|open$|hidden$|disabled$|tabindex$)/u;
    const retained = [...node.attributes].filter((item) => allowed.test(item.name));
    const sensitiveAttributeNames = retained.filter((item) => /(?:url|uri|href|src|endpoint|authorization|secret|token|key)/iu.test(item.name)).map((item) => item.name).sort();
    const attributes = Object.fromEntries(retained.filter((item) => !sensitiveAttributeNames.includes(item.name)).slice(0, 40).map((item) => [item.name, item.value]));
    const cssVariables = Object.fromEntries([...style].filter((name) => name.startsWith('--')).sort().slice(0, 80).map((name) => [name, style.getPropertyValue(name).trim()]));
    const computedNames = ['display','visibility','opacity','position','color','backgroundColor','fontFamily','fontSize','fontWeight','lineHeight','padding','margin','gap','borderRadius','objectFit','overflow'];
    const computed = Object.fromEntries(computedNames.map((name) => [name, style[name]]));
    const matchedRules = [];
    const visitRules = (rules, sheetIndex, context = []) => {
      for (let ruleIndex = 0; ruleIndex < rules.length && matchedRules.length < 80; ruleIndex += 1) {
        const rule = rules[ruleIndex];
        if (rule.selectorText) {
          let matches = false; try { matches = node.matches(rule.selectorText); } catch { matches = false; }
          if (matches) matchedRules.push({ stylesheet_index: sheetIndex, rule_index: ruleIndex,
            selector: rule.selectorText.slice(0, 512), context,
            declarations: [...rule.style].slice(0, 80).map((property) => ({ property, value: rule.style.getPropertyValue(property).trim(), priority: rule.style.getPropertyPriority(property) })) });
        } else if (rule.cssRules) {
          const label = rule.conditionText || rule.media?.mediaText || rule.name || rule.constructor?.name || 'group-rule';
          visitRules(rule.cssRules, sheetIndex, [...context, String(label).slice(0, 256)]);
        }
      }
    };
    [...document.styleSheets].forEach((sheet, sheetIndex) => { try { visitRules(sheet.cssRules, sheetIndex); } catch {} });
    const loadedFonts = document.fonts ? [...document.fonts].slice(0, 80).map((face) => ({
      family: face.family, style: face.style, weight: face.weight, stretch: face.stretch, status: face.status,
    })) : [];
    const media = [...node.querySelectorAll('img,picture,video,svg')].slice(0, 12).map((item) => ({ tag: item.tagName.toLowerCase(), width: item.getBoundingClientRect().width, height: item.getBoundingClientRect().height, natural_width: item.naturalWidth ?? null, natural_height: item.naturalHeight ?? null, source_sha_input: item.currentSrc || item.getAttribute('src') || '', object_fit: getComputedStyle(item).objectFit }));
    return {
      tag: node.tagName.toLowerCase(), classes: [...node.classList].slice(0, 30), attributes, redacted_attribute_names: sensitiveAttributeNames,
      text_length: (node.textContent || '').length, text_sha_input: node.textContent || '', child_count: node.childElementCount,
      geometry: { x: rect.x, y: rect.y, width: rect.width, height: rect.height }, computed, css_variables: cssVariables,
      pseudo: { before: { content: before.content, display: before.display }, after: { content: after.content, display: after.display } },
      state: { focused: document.activeElement === node || node.contains(document.activeElement), hidden: node.hidden, open: 'open' in node ? Boolean(node.open) : null, disabled: 'disabled' in node ? Boolean(node.disabled) : null },
      media, parts: parts.map((selector) => ({ selector, count: node.querySelectorAll(selector).length })),
      cascade: { matched_rules: matchedRules, provenance: 'browser-cssom-matched-rules-compiled-source-line-unavailable' }, loaded_fonts: loadedFonts,
      media_queries: { reduced_motion: matchMedia('(prefers-reduced-motion: reduce)').matches, narrow_420: matchMedia('(max-width:420px)').matches, narrow_720: matchMedia('(max-width:720px)').matches },
    };
  }, partSelectors);
  facts.text_sha256 = sha(facts.text_sha_input); delete facts.text_sha_input;
  for (const media of facts.media) { media.source_sha256 = sha(media.source_sha_input); delete media.source_sha_input; }
  const sanitizeTree = (value) => Array.isArray(value) ? value.map(sanitizeTree) : value && typeof value === 'object'
    ? Object.fromEntries(Object.entries(value).map(([key, child]) => [key, sanitizeTree(child)])) : typeof value === 'string' ? safeCapturedValue(value) : value;
  return sanitizeTree(facts);
}

export function loadPinnedPlaywrightImageComparator(nodeModules, mimeType = 'image/png') {
  const requireFromPlaywright = createRequire(join(resolve(nodeModules), 'playwright', 'package.json'));
  const coreBundlePath = requireFromPlaywright.resolve('playwright-core/lib/utilsBundle').replace(/utilsBundle\.js$/u, 'coreBundle.js');
  return requireFromPlaywright(coreBundlePath).utils.getComparator(mimeType);
}

export async function captureStableLocatorPng({ locator, path, imageComparator, label = 'Element screenshot' }) {
  if (typeof imageComparator !== 'function') throw new Error('PNG capture requires the pinned Playwright image comparator');
  await locator.scrollIntoViewIfNeeded();
  const layoutStable = await locator.evaluate(async (node) => {
    const fingerprint = () => {
      const rect = node.getBoundingClientRect(); const style = getComputedStyle(node);
      return JSON.stringify([style.display, style.visibility, Math.round(rect.x), Math.round(rect.y), Math.round(rect.width), Math.round(rect.height), node.childElementCount]);
    };
    let previous = ''; let stable = 0;
    for (let frame = 0; frame < 120; frame += 1) {
      await new Promise((done) => requestAnimationFrame(done)); const current = fingerprint();
      if (current === previous) stable += 1; else stable = 0;
      if (stable >= 5) return true;
      previous = current;
    }
    return false;
  });
  if (!layoutStable) throw new Error(`${label} layout did not stabilize`);
  const screenshotOptions = { type: 'png', animations: 'disabled', caret: 'hide', scale: 'css' };
  const stablePair = await capturePlaywrightStablePair({
    capture: () => locator.screenshot(screenshotOptions), comparator: imageComparator, label,
  });
  const first = stablePair.first; const second = stablePair.accepted;
  mkdirSync(dirname(resolve(path)), { recursive: true }); writeFileSync(path, second);
  const firstDhash = pngDifferenceHash(first); const secondDhash = pngDifferenceHash(second);
  return {
    bytes: second.length, sha256: sha(second), dhash: secondDhash, first_sha256: sha(first), first_dhash: firstDhash,
    exact_stable: first.equals(second), perceptually_stable: true,
    stability_attempts: stablePair.attempts, comparator: stablePair.comparator,
  };
}

async function applyAction(page, action) {
  if (action.kind === 'focus') await page.locator(action.selector).focus();
  else if (action.kind === 'click') await page.locator(action.selector).click();
  else if (action.kind === 'toggle-open') await page.locator(action.selector).evaluate((element) => { element.open = true; });
  else throw new Error(`Unsupported specimen action: ${action.kind}`);
}

async function captureStep({ page, row, outputDir, step, telemetry, imageComparator }) {
  const locator = page.locator(row.root_selector); await locator.waitFor({ state: 'visible' });
  for (const selector of row.expected_markers) if (await page.locator(selector).count() === 0) throw new Error(`Expected marker missing for ${row.id}: ${selector}`);
  for (const selector of row.expected_absent_markers || []) if (await page.locator(selector).count() !== 0) throw new Error(`Expected absent marker rendered for ${row.id}: ${selector}`);
  await page.evaluate(() => document.fonts?.ready);
  const name = `${row.id}-${step}.png`; const screenshot = await captureStableLocatorPng({ locator, path: join(outputDir, 'component-screenshots', name), imageComparator, label: `Controlled specimen ${row.id}/${step}` });
  let aria = null; try { aria = safeCapturedValue(await locator.ariaSnapshot({ timeout: 3000 }), 6000); } catch (error) { aria = { unavailable: true, error_class: error.constructor?.name || 'Error' }; }
  const facts = await collectBoundedElementFacts(locator, row.part_selectors);
  const packet = {
    schema_version: row.schema_version, id: `specimen-observation.${row.id}.${step}`, specimen_id: row.id, plan_id: row.id,
    capsule_ids: [...row.capsule_ids],
    trace_kind: row.trace_kind, state_equivalence: row.state_equivalence, production_state_claimed: false,
    source_paths: [...row.source_paths], consumer_paths: [...row.consumer_paths], fixture_ref: row.fixture_ref, fixture_delta_fields: Object.keys(row.fixture_delta).sort(),
    component_presence: row.component_presence || 'expected-present', expected_absent_markers: [...(row.expected_absent_markers || [])],
    environment: row.environment, viewport: row.viewport, container: row.container, step, evidence_status: 'captured-not-reviewed',
    proof_label: 'controlled-specimen-browser-element', dom: { tag: facts.tag, classes: facts.classes, attributes: facts.attributes, redacted_attribute_names: facts.redacted_attribute_names, child_count: facts.child_count, text_length: facts.text_length, text_sha256: facts.text_sha256, full_html_retained: false },
    accessibility: { aria_snapshot: aria, ...facts.state }, computed: facts.computed, geometry: facts.geometry, css_variables: facts.css_variables,
    pseudo: facts.pseudo, parts: facts.parts, media: facts.media, media_queries: facts.media_queries,
    cascade: facts.cascade, loaded_fonts: facts.loaded_fonts,
    screenshot: { path: `component-screenshots/${name}`, ...screenshot },
    console: { counts: telemetry.consoleCounts, message_text_retained: false, message_hashes: telemetry.consoleHashes.slice(0, 20) },
    network: { counts_by_resource_type: telemetry.resourceCounts, response_status_counts: telemetry.statusCounts, failed_count: telemetry.failed, raw_urls_retained: false },
    review_status: 'pending-human-visual-review', normalization_allowed: false,
  };
  assertEvidencePacket(packet); return packet;
}

export async function captureControlledSpecimens({ browser, baseUrl, outputDir, imageComparator, registry = buildSpecimenRegistry() }) {
  assertSpecimenRegistry(registry); const observations = [];
  for (const row of registry.controlled_specimens) {
    const context = await browser.newContext({ viewport: row.viewport, reducedMotion: row.environment.reduced_motion ? 'reduce' : 'no-preference' });
    for (const action of row.before_navigation || []) if (action.kind === 'seed-amber-found') await context.addInitScript(({ eventId }) => {
      localStorage.setItem('ke_artifact_collection_v1', JSON.stringify({ schemaVersion: 1, collectionId: 'kaliningrad_artifacts_v1', artifacts: { amber_cosmonaut: { status: 'found', foundAt: '2026-08-08T00:00:00.000Z', eventId, placement: 'weekend.rail.tail.v1' } } }));
    }, { eventId: action.event_id });
    const page = await context.newPage(); const telemetry = { consoleCounts: {}, consoleHashes: [], resourceCounts: {}, statusCounts: {}, failed: 0 };
    page.on('console', (message) => { telemetry.consoleCounts[message.type()] = (telemetry.consoleCounts[message.type()] || 0) + 1; telemetry.consoleHashes.push(sha(message.text())); });
    page.on('request', (request) => { const type = request.resourceType(); telemetry.resourceCounts[type] = (telemetry.resourceCounts[type] || 0) + 1; });
    page.on('response', (response) => { const status = String(response.status()); telemetry.statusCounts[status] = (telemetry.statusCounts[status] || 0) + 1; });
    page.on('requestfailed', () => { telemetry.failed += 1; });
    await page.goto(`${baseUrl}/specimens/${row.id}/`, { waitUntil: 'networkidle' });
    if ((row.capture_steps || []).includes('before-action')) observations.push(await captureStep({ page, row, outputDir, step: 'before-action', telemetry, imageComparator }));
    for (const action of row.actions) await applyAction(page, action);
    observations.push(await captureStep({ page, row, outputDir, step: row.actions.length ? 'after-action' : 'baseline', telemetry, imageComparator }));
    await context.close();
  }
  writeFileSync(join(outputDir, 'specimen-observations.jsonl'), observations.map((row) => JSON.stringify(row)).join('\n') + '\n');
  return observations;
}

export async function captureWithExactPlaywright({ nodeModules, dist, outputDir, registry = buildSpecimenRegistry() }) {
  const modulePath = join(resolve(nodeModules), 'playwright/index.mjs');
  if (!existsSync(modulePath)) throw new Error('Exact Playwright entrypoint missing');
  const { chromium } = await import(pathToFileURL(modulePath).href); const server = await startSpecimenServer({ dist });
  const imageComparator = loadPinnedPlaywrightImageComparator(nodeModules, 'image/png');
  const browser = await chromium.launch({ headless: true });
  try { return await captureControlledSpecimens({ browser, baseUrl: server.baseUrl, outputDir, imageComparator, registry }); }
  finally { await browser.close(); await server.close(); }
}

export function observationDigest(rows) { return stableHash(rows); }
