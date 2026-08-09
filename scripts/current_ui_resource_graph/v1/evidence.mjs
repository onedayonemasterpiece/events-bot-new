import { createHash } from 'node:crypto';
import { mkdirSync, statSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

export const COMPONENT_EVIDENCE_SCREENSHOT_RESERVATION = 1024 * 1024;
export const COMPONENT_EVIDENCE_LIMIT_PER_PAGE = 3;
export const COMPONENT_BREAKPOINT_CONTEXTS = Object.freeze([390, 420, 540, 700, 720, 1728]);

function sha(value) { return createHash('sha256').update(value).digest('hex'); }

export function sanitizeEvidenceString(value, maxLength = 160) {
  if (value === null || value === undefined) return null;
  const text = String(value);
  if (text.length <= maxLength && !/(?:https?:\/\/|authorization|bearer|token|secret|password|sb_publishable|_review\/)/iu.test(text)) return text;
  return { redacted: true, sha256: sha(text), length: text.length };
}

export function assertSafeComponentEvidence(record) {
  const serialized = JSON.stringify(record);
  if (/(?:https?:\/\/|authorization|bearer|sb_publishable|_review\/)/iu.test(serialized)) throw new Error('Unsafe component evidence contains a URL or credential-shaped value');
  const visit = (value) => {
    if (!value || typeof value !== 'object') return;
    for (const [key, child] of Object.entries(value)) {
      if (/^(?:html|outerhtml|innerhtml)$/iu.test(key)) throw new Error('Full HTML is forbidden in component evidence');
      if (!['data-action-family', 'data-action-layout'].includes(key) && /(?:key|token|secret|url|uri|endpoint|relay|authorization|href|src|action)/iu.test(key)
        && key !== 'redacted_attribute_names' && typeof child === 'string') throw new Error(`Sensitive nested evidence field is forbidden: ${key}`);
      visit(child);
    }
  };
  visit(record);
  if (!['exact-candidate-browser-element', 'controlled-specimen-browser-element', 'public-root-browser-element'].includes(record.proof_label)) throw new Error('Unknown component evidence proof label');
}

export async function captureComponentScopedEvidence({
  page, pageFamily, routeHash, viewport, outputDir, budget, sharp, plane = 'latest_checked_kaggle_candidate',
}) {
  if (typeof sharp !== 'function') throw new Error('Component evidence requires the pinned image decoder');
  const componentSelectors = [
    '[data-event-transport-schedule]', '[data-event-bus-schedule]', '[data-kaup-transport]',
    '.event-token-layout[data-medallion-layout]', '[data-focus-egg-artifact]', '[data-artifact-collection]',
    '[data-artifact-collection-unavailable]', '[data-amber-artifact]', '[data-desktop-action-panel]',
    '[data-media-frame]', '[data-authorized-event-search]', '[data-favorites-surface]',
    '.event-card', '.listing-event-card', '[data-mobile-listing-row]', '[data-desktop-clean-event]',
  ];
  const selector = componentSelectors.join(',');
  const candidates = await page.locator(selector).evaluateAll((nodes) => nodes.filter((node) => {
    const rect = node.getBoundingClientRect(); const style = getComputedStyle(node);
    return rect.width > 0 && rect.height > 0 && style.display !== 'none' && style.visibility !== 'hidden';
  }).slice(0, 4).map((node) => {
    const rect = node.getBoundingClientRect(); const style = getComputedStyle(node);
    const retainedAttributes = [...node.attributes]
      .filter((item) => /^(?:data-|aria-|role$|hidden$|open$|disabled$)/u.test(item.name));
    const safeActionStateAttributes = new Set(['data-action-family', 'data-action-layout']);
    const sensitiveAttributeNames = retainedAttributes.filter((item) => !safeActionStateAttributes.has(item.name) && /(?:key|token|secret|url|uri|endpoint|relay|authorization|href|src|action)/iu.test(item.name)).map((item) => item.name).sort();
    const attributes = Object.fromEntries(retainedAttributes
      .filter((item) => !sensitiveAttributeNames.includes(item.name))
      .slice(0, 32).map((item) => [item.name, item.value]));
    const variables = {};
    for (const name of [...style].filter((item) => item.startsWith('--')).sort().slice(0, 64)) variables[name] = style.getPropertyValue(name).trim();
    const focusable = node.matches('button,a,input,select,textarea,[tabindex]') || Boolean(node.querySelector('button,a,input,select,textarea,[tabindex]'));
    const binding = [
      ['[data-event-transport-schedule]', 'src/components/EventTransportSchedule.astro'],
      ['[data-event-bus-schedule]', 'src/components/EventBusTransportSchedule.astro'],
      ['[data-kaup-transport]', 'src/components/KaupTransportSchedule.astro'],
      ['.event-token-layout[data-medallion-layout]', 'src/components/EventTokenMedallions.astro'],
      ['[data-focus-egg-artifact]', 'src/components/FocusEggArtifact.astro'],
      ['[data-artifact-collection]', 'src/components/artifacts/ArtifactCollection.astro'],
      ['[data-amber-artifact]', 'src/components/listings/AmberRailArtifact.astro'],
      ['[data-desktop-action-panel]', 'src/components/DesktopEventActionPanel.astro'],
      ['.event-card', 'src/components/EventCard.astro'],
      ['.listing-event-card', 'src/components/listings/ListingEventCard.astro'],
    ].find(([match]) => node.matches(match))?.[1] || null;
    return {
      matched_index: nodes.indexOf(node), tag: node.tagName.toLowerCase(), id: node.id || null,
      classes: [...node.classList].slice(0, 24), attributes, redacted_attribute_names: sensitiveAttributeNames,
      binding,
      geometry: { x: Math.round(rect.x), y: Math.round(rect.y), width: Math.round(rect.width), height: Math.round(rect.height) },
      computed: {
        display: style.display, visibility: style.visibility, opacity: style.opacity, color: style.color,
        background_color: style.backgroundColor, font_family: style.fontFamily, font_size: style.fontSize,
        font_weight: style.fontWeight, line_height: style.lineHeight, padding: style.padding, margin: style.margin,
        gap: style.gap, border_radius: style.borderRadius, object_fit: style.objectFit,
      },
      css_variables: variables,
      accessibility: {
        role: node.getAttribute('role') || null, aria_label: node.getAttribute('aria-label') || null,
        aria_expanded: node.getAttribute('aria-expanded'), aria_hidden: node.getAttribute('aria-hidden'),
        aria_disabled: node.getAttribute('aria-disabled'), disabled: 'disabled' in node ? Boolean(node.disabled) : null,
      },
      font_status: { requested: `${style.fontWeight} ${style.fontSize} ${style.fontFamily}`, loaded: document.fonts?.check(`${style.fontWeight} ${style.fontSize} ${style.fontFamily}`) ?? null },
      state: { hidden: node.hidden, open: 'open' in node ? Boolean(node.open) : null, focusable, focused: document.activeElement === node },
    };
  }));
  const dir = join(outputDir, 'component-screenshots'); mkdirSync(dir, { recursive: true });
  const observations = [];
  for (let order = 0; order < candidates.slice(0, COMPONENT_EVIDENCE_LIMIT_PER_PAGE).length; order += 1) {
    const candidate = candidates[order];
    const locator = page.locator(selector).nth(candidate.matched_index);
    const filename = `component-${routeHash.slice(0, 12)}-${viewport.width}x${viewport.height}-${order}.jpg`;
    const path = join(dir, filename);
    const buffer = await locator.screenshot({ type: 'jpeg', quality: 60, animations: 'disabled', caret: 'hide', scale: 'css' });
    const confirm = await locator.screenshot({ type: 'jpeg', quality: 60, animations: 'disabled', caret: 'hide', scale: 'css' });
    const differenceHash = async (bytes) => {
      const { data } = await sharp(bytes).greyscale().resize(9, 8, { fit: 'fill', kernel: 'lanczos3' }).raw().toBuffer({ resolveWithObject: true });
      let bits = '';
      for (let y = 0; y < 8; y += 1) for (let x = 0; x < 8; x += 1) bits += data[y * 9 + x] > data[y * 9 + x + 1] ? '1' : '0';
      return BigInt(`0b${bits}`).toString(16).padStart(16, '0');
    };
    const firstDhash = await differenceHash(buffer); const confirmDhash = await differenceHash(confirm);
    if (firstDhash !== confirmDhash) throw new Error(`Component screenshot failed perceptual two-frame stability contract: ${filename}`);
    if (buffer.length > COMPONENT_EVIDENCE_SCREENSHOT_RESERVATION) throw new Error(`Component screenshot exceeds deterministic byte reservation: ${filename}`);
    writeFileSync(path, buffer); budget.claim(buffer.length, `component-screenshots/${filename}`);
    const clean = JSON.parse(JSON.stringify(candidate, (_key, value) => typeof value === 'string' ? sanitizeEvidenceString(value) : value));
    const record = {
      id: `component-evidence.${sha(`${routeHash}\0${viewport.width}\0${order}`).slice(0, 16)}`,
      plane, page_family: pageFamily, route_hash: routeHash, viewport,
      breakpoint_context: COMPONENT_BREAKPOINT_CONTEXTS.includes(viewport.width) ? 'named-evidence-width' : 'page-evidence-width',
      selector_evidence: { id_sha256: candidate.id ? sha(candidate.id) : null, tag: candidate.tag, classes: clean.classes },
      dom_summary: { attributes: clean.attributes, redacted_attribute_names: clean.redacted_attribute_names, child_count_not_retained: true, full_html_retained: false },
      geometry: clean.geometry, computed: clean.computed, css_variables: clean.css_variables,
      accessibility: clean.accessibility, font_status: clean.font_status, state: clean.state,
      screenshot_path: `component-screenshots/${filename}`, screenshot_bytes: statSync(path).size, screenshot_sha256: sha(buffer),
      screenshot_confirm_sha256: sha(confirm), screenshot_perceptual_dhash_64: firstDhash,
      screenshot_confirm_perceptual_dhash_64: confirmDhash, screenshot_exact_stable: buffer.equals(confirm), screenshot_perceptually_stable: true,
      component_binding: clean.binding, binding_status: clean.binding ? 'exact-runtime-marker-to-source-binding' : 'requires-source-or-specimen-marker-reconciliation',
      override_source: 'computed-cascade-observed-source-unresolved',
      proof_label: plane === 'current_root_prelaunch' ? 'public-root-browser-element' : 'exact-candidate-browser-element',
    };
    assertSafeComponentEvidence(record); observations.push(record);
  }
  return observations;
}
