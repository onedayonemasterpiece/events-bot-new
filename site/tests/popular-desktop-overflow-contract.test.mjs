import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const styles = await readFile(new URL('../src/styles/design-system.css', import.meta.url), 'utf8');
const previewCheck = await readFile(new URL('../scripts/check-preview.mjs', import.meta.url), 'utf8');

test('desktop Popular keeps one intrinsic-width row and owns its horizontal overflow', () => {
  const base = styles.match(/\.ke-popular-behavior__row\s*\{([^}]*)\}/u)?.[1] || '';
  assert.match(base, /display:\s*flex/u);
  assert.match(base, /flex-wrap:\s*nowrap/u);
  assert.match(base, /overflow-x:\s*auto/u,
    'wide real-data shelves must scroll inside the row instead of widening the document');
  assert.doesNotMatch(styles, /(?:html|body)[^{]*\{[^}]*overflow-x:\s*(?:hidden|clip)/u,
    'document clipping would mask the ownership defect');
});

test('fresh real data may truthfully omit evidence shelves without reordering survivors', () => {
  assert.match(previewCheck, /expectedPopularDesktopReasonOrder = \['fast_growth', 'multi_source', 'discussed', 'frequently_shared', 'score_fallback'\]/u);
  assert.doesNotMatch(previewCheck, /popularDesktopReasons\.length < 3/u);
  assert.match(previewCheck, /expectedIndex <= previousIndex/u);
  assert.match(previewCheck, /count < 3 \|\| count > 5/u);
});
