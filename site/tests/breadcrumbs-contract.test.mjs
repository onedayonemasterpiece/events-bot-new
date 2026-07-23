import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, '..');
const component = fs.readFileSync(path.join(root, 'src/components/Breadcrumbs.astro'), 'utf8');
const helper = fs.readFileSync(path.join(root, 'src/lib/breadcrumbs.ts'), 'utf8');

test('breadcrumb component exposes ordered semantic desktop hierarchy', () => {
  assert.match(component, /<nav[\s\S]*aria-label="Хлебные крошки"/u);
  assert.match(component, /<ol>/u);
  assert.match(component, /<li aria-current="page">/u);
  assert.match(component, /min-height:\s*44px/u);
});

test('responsive mobile pattern is one named parent link, not browser history', () => {
  assert.match(component, /data-product-parent-link/u);
  assert.match(component, /aria-label="К родительскому разделу"/u);
  assert.doesNotMatch(component, /history\.(?:back|go)/u);
  assert.match(component, /@media \(max-width:\s*1023px\)[\s\S]*product-parent-link--responsive \{ display: block; \}/u);
});

test('event hierarchy only points to materialized parent pages', () => {
  assert.match(helper, /label:'Афиша', href:siteHomeHref\(\)/u);
  assert.match(helper, /isExhibitionLikeEvent\(event\)/u);
  assert.match(helper, /label:'Выставки', href:withBase\('\/vystavki\/'\)/u);
  assert.doesNotMatch(helper, /referrer|document\.referrer|history\./u);
});

