import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const source = await readFile(new URL('../src/pages/fokus-gruppa/index.astro', import.meta.url), 'utf8');

test('focus programme route owns one foundation-bound composition', () => {
  assert.match(source, /product-contour-foundations\.css/u);
  assert.match(source, /data-ds-family="FocusGroupProgrammeSurface"/u);
  assert.match(source, /data-ds-version="1"/u);
  assert.match(source, /data-ds-variant="research-programme"/u);
  assert.match(source, /data-ds-state="available"/u);
  assert.match(source, /data-ke-foundation-consumer="focus-programme-route"/u);
});

test('focus programme route uses canonical navigation identity and button roots', () => {
  assert.match(source, /import Button from '\.\.\/\.\.\/components\/design-system\/Button\.astro'/u);
  assert.match(source, /import SemanticIcon from '\.\.\/\.\.\/components\/design-system\/SemanticIcon\.astro'/u);
  assert.match(source, /<SemanticIcon name="arrow-left" role="inline" \/>/u);
  assert.match(source, /<Button variant="primary" size="large" href="#focus-invite-share">/u);
  assert.match(source, /<Button variant="secondary" href=\{withBase\('\/fokus-gruppa\/zavershenie\/'\)\}>/u);
  assert.doesNotMatch(source, /← Стартовый экран/u);
});

test('focus programme route keeps research boundaries and normalized visible surfaces', () => {
  assert.match(source, /Capacity, отзыв согласия и active membership/u);
  assert.match(source, /FocusGroupThankYou/u);
  assert.match(source, /FocusGroupInviteShare/u);
  for (const token of [
    '--ke-color-background-page',
    '--ke-color-focus-brand-marker',
    '--ke-focus-panel-radius',
    '--ke-color-focus-panel-surface',
    '--ke-color-status-info-background',
    '--ke-color-status-info-foreground',
    '--ke-color-focus-ring-accent',
  ]) assert.match(source, new RegExp(token, 'u'));
  assert.doesNotMatch(source, /background:\s*#fff|background:\s*#fff7e7|color:\s*#98401f|background:\s*#e7f2f7/u,
    'route visible palette is owned by foundations');
});
