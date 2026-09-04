import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

async function read(relativePath) {
  return readFile(path.join(siteRoot, relativePath), 'utf8');
}

function lastStyle(source) {
  const start = source.lastIndexOf('<style>');
  const end = source.lastIndexOf('</style>');
  assert.ok(start >= 0 && end > start, 'expected a terminal style block');
  return source.slice(start + '<style>'.length, end);
}

function stripComments(source) {
  return source.replace(/\/\*[\s\S]*?\*\//gu, '');
}

test('DesktopEventActionPanel is the single desktop action anatomy and CSS owner', async () => {
  const page = await read('src/components/DesktopEventPage.astro');
  const panel = await read('src/components/DesktopEventActionPanel.astro');
  const pageStyle = stripComments(lastStyle(page));
  const panelStyle = stripComments(lastStyle(panel));

  assert.equal((page.match(/<DesktopEventActionPanel\b/gu) || []).length, 3);
  assert.equal((page.match(/state=\{mediaPolicy\}/gu) || []).length, 3);
  assert.match(page, /variant="editorial-side" state=\{mediaPolicy\}/u);
  assert.match(page, /variant="split-inline" state=\{mediaPolicy\}/u);
  assert.match(page, /variant=\{candidate === 'editorial' \? 'editorial-flow' : 'split-flow'\} state=\{mediaPolicy\}/u);
  assert.doesNotMatch(page, /className="desktop-prototype__action--/u);
  assert.doesNotMatch(page, /<DesktopEventActionPanel[^>]+\bfamily=/u);

  assert.match(panel, /type DesktopActionVariant = 'editorial-side' \| 'split-inline' \| 'editorial-flow' \| 'split-flow';/u);
  assert.match(panel, /type DesktopActionState = 'non-ocr' \| 'ocr';/u);
  assert.match(panel, /data-action-variant=\{variant\}/u);
  assert.match(panel, /data-action-state=\{state\}/u);
  assert.match(panel, /data-action-family=\{family\}/u);
  assert.doesNotMatch(panel, /className\?: string/u);
  assert.doesNotMatch(panel, /family\?: 'split' \| 'editorial'/u);

  for (const selector of [
    'desktop-prototype__action',
    'desktop-prototype__primary-action',
    'desktop-prototype__icon-action',
    'desktop-prototype__action-row',
  ]) {
    assert.doesNotMatch(pageStyle, new RegExp(`\\.${selector}(?:--|(?=[\\s>.:#\\[]))`, 'u'));
    assert.match(panelStyle, new RegExp(selector, 'u'));
  }
  assert.match(panelStyle, /data-desktop-action-panel/u);
  assert.match(panelStyle, /data-action-variant="split-inline"/u);
  assert.match(
    panelStyle,
    /data-action-variant="editorial-side"[^}]*grid-column:auto !important;[^}]*grid-row:auto !important;/u,
    'the Editorial side panel must reset obsolete hero-grid placement inside its bounded side owner',
  );
});

test('desktop action controls keep accepted targets and central icon roles', async () => {
  const panel = await read('src/components/DesktopEventActionPanel.astro');
  const panelStyle = stripComments(lastStyle(panel));

  assert.match(panelStyle, /:global\(\.desktop-prototype__primary-action\) \{[\s\S]*?min-height: 56px;/u);
  assert.match(panelStyle, /:global\(\.desktop-prototype__icon-action\) \{[\s\S]*?min-width: 52px;[\s\S]*?min-height: 52px;/u);
  assert.match(panelStyle, /desktop-prototype__icon-action\)[^{]*\{[\s\S]*?min-width:56px;[\s\S]*?min-height:56px;/u);
  assert.equal((panelStyle.match(/var\(--ke-icon-size-action\)/gu) || []).length, 4);
  assert.doesNotMatch(panelStyle, /desktop-prototype__primary-action \.icon\)[^{]*\{[^}]*1\.5rem/u);
  assert.doesNotMatch(panelStyle, /desktop-prototype__icon-action \.icon\)[^{]*\{[^}]*29px/u);
});
