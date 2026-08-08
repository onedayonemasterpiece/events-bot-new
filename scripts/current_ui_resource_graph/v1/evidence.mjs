import { createHash } from 'node:crypto';
import { mkdirSync, statSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

export const COMPONENT_EVIDENCE_SCREENSHOT_RESERVATION = 128 * 1024;
export const COMPONENT_EVIDENCE_LIMIT_PER_PAGE = 1;
export const COMPONENT_BREAKPOINT_CONTEXTS = Object.freeze([390, 420, 540, 700, 720, 1728]);

function sha(value) { return createHash('sha256').update(value).digest('hex'); }

export function sanitizeEvidenceString(value, maxLength = 160) {
  if (value === null || value === undefined) return null;
  const text = String(value);
  if (text.length <= maxLength && !/(?:https?:\/\/|authorization|bearer|token|secret|password|_review\/)/iu.test(text)) return text;
  return { redacted: true, sha256: sha(text), length: text.length };
}

export function assertSafeComponentEvidence(record) {
  const serialized = JSON.stringify(record);
  if (/(?:https?:\/\/|authorization|bearer|_review\/)/iu.test(serialized)) throw new Error('Unsafe component evidence contains a URL or credential-shaped value');
  if ('html' in record || 'outerHTML' in record || 'innerHTML' in record) throw new Error('Full HTML is forbidden in component evidence');
  if (!['exact-candidate-browser-element', 'controlled-specimen-browser-element', 'public-root-browser-element'].includes(record.proof_label)) throw new Error('Unknown component evidence proof label');
}

export async function captureComponentScopedEvidence({
  page, pageFamily, routeHash, viewport, outputDir, budget, plane = 'latest_checked_kaggle_candidate',
}) {
  const candidates = await page.locator([
    '[data-desktop-clean-event]', '[data-desktop-action-panel]', '[data-focus-egg-artifact]',
    '[data-transport-layout]', '[data-event-token-medallions]', 'header', 'main > section',
    '[class*="event-card"]', '[class*="listing"]', '[class*="medallion"]', '[class*="transport"]',
  ].join(',')).evaluateAll((nodes) => nodes.filter((node) => {
    const rect = node.getBoundingClientRect(); const style = getComputedStyle(node);
    return rect.width > 0 && rect.height > 0 && style.display !== 'none' && style.visibility !== 'hidden';
  }).slice(0, 4).map((node) => {
    const rect = node.getBoundingClientRect(); const style = getComputedStyle(node);
    const attributes = Object.fromEntries([...node.attributes]
      .filter((item) => /^(?:data-|aria-|role$|hidden$|open$|disabled$)/u.test(item.name))
      .slice(0, 32).map((item) => [item.name, item.value]));
    const variables = {};
    for (const name of [...style].filter((item) => item.startsWith('--')).sort().slice(0, 64)) variables[name] = style.getPropertyValue(name).trim();
    const focusable = node.matches('button,a,input,select,textarea,[tabindex]') || Boolean(node.querySelector('button,a,input,select,textarea,[tabindex]'));
    return {
      matched_index: nodes.indexOf(node), tag: node.tagName.toLowerCase(), id: node.id || null,
      classes: [...node.classList].slice(0, 24), attributes,
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
      state: { hidden: node.hidden, open: 'open' in node ? Boolean(node.open) : null, focusable, focused: document.activeElement === node },
    };
  }));
  const dir = join(outputDir, 'component-screenshots'); mkdirSync(dir, { recursive: true });
  const observations = [];
  for (let order = 0; order < candidates.slice(0, COMPONENT_EVIDENCE_LIMIT_PER_PAGE).length; order += 1) {
    const candidate = candidates[order];
    const locator = page.locator([
      '[data-desktop-clean-event]', '[data-desktop-action-panel]', '[data-focus-egg-artifact]',
      '[data-transport-layout]', '[data-event-token-medallions]', 'header', 'main > section',
      '[class*="event-card"]', '[class*="listing"]', '[class*="medallion"]', '[class*="transport"]',
    ].join(',')).nth(candidate.matched_index);
    const filename = `component-${routeHash.slice(0, 12)}-${viewport.width}x${viewport.height}-${order}.jpg`;
    const path = join(dir, filename);
    const buffer = await locator.screenshot({ type: 'jpeg', quality: 60, animations: 'disabled', caret: 'hide', scale: 'css' });
    if (buffer.length > COMPONENT_EVIDENCE_SCREENSHOT_RESERVATION) throw new Error(`Component screenshot exceeds deterministic byte reservation: ${filename}`);
    writeFileSync(path, buffer); budget.claim(COMPONENT_EVIDENCE_SCREENSHOT_RESERVATION, `component-screenshots/${filename}`);
    const clean = JSON.parse(JSON.stringify(candidate, (_key, value) => typeof value === 'string' ? sanitizeEvidenceString(value) : value));
    const record = {
      id: `component-evidence.${sha(`${routeHash}\0${viewport.width}\0${order}`).slice(0, 16)}`,
      plane, page_family: pageFamily, route_hash: routeHash, viewport,
      breakpoint_context: COMPONENT_BREAKPOINT_CONTEXTS.includes(viewport.width) ? 'named-evidence-width' : 'page-evidence-width',
      selector_evidence: { id_sha256: candidate.id ? sha(candidate.id) : null, tag: candidate.tag, classes: clean.classes },
      dom_summary: { attributes: clean.attributes, child_count_not_retained: true, full_html_retained: false },
      geometry: clean.geometry, computed: clean.computed, css_variables: clean.css_variables,
      accessibility: clean.accessibility, state: clean.state,
      screenshot_path: `component-screenshots/${filename}`, screenshot_bytes: statSync(path).size,
      component_binding: null, binding_status: 'requires-source-or-specimen-marker-reconciliation',
      override_source: 'computed-cascade-observed-source-unresolved',
      proof_label: plane === 'current_root_prelaunch' ? 'public-root-browser-element' : 'exact-candidate-browser-element',
    };
    assertSafeComponentEvidence(record); observations.push(record);
  }
  return observations;
}
