import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('occurrence navigation consumes the established event-detail occurrence roles', async () => {
  const [occurrenceNav, foundations] = await Promise.all([
    read('src/components/EventOccurrenceNav.astro'),
    read('src/components/design-system/event-detail-foundations.css'),
  ]);

  const requiredTokens = [
    '--ke-color-occurrence-muted',
    '--ke-color-occurrence-summary',
    '--ke-color-occurrence-link',
    '--ke-color-occurrence-link-hover',
    '--ke-color-occurrence-date',
    '--ke-color-occurrence-mobile-surface',
    '--ke-color-occurrence-chip-surface',
    '--ke-color-occurrence-chip-hover-surface',
    '--ke-color-occurrence-chip-current',
    '--ke-color-occurrence-chip-border',
    '--ke-elevation-occurrence-current',
    '--ke-occurrence-chip-radius',
    '--ke-occurrence-mobile-panel-radius',
    '--ke-color-event-detail-border',
    '--ke-color-event-detail-border-soft',
  ];

  for (const token of requiredTokens) {
    assert.match(foundations, new RegExp(`${token.replaceAll('-', '\\-')}:`), `${token} remains defined by event-detail foundations`);
    assert.match(occurrenceNav, new RegExp(`var\\(${token.replaceAll('-', '\\-')}\\)`), `${token} is consumed by EventOccurrenceNav`);
  }

  assert.doesNotMatch(
    occurrenceNav,
    /#(?:7b6b5e|75665b|8d3417|b54d22|352820|fffaf2|fff3e8)\b|rgba\((?:121,\s*48,\s*20|141,\s*52,\s*23),\s*(?:\.12|\.18|\.2|\.28)\)/iu,
    'occurrence paint roles must not return to component-local literals',
  );
  assert.doesNotMatch(occurrenceNav, /border-radius:\s*(?:10px|999px)/u,
    'occurrence panel and chip radii remain token-owned');
});
