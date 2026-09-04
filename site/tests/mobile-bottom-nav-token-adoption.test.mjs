import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('mobile bottom navigation consumes its established shell roles', async () => {
  const [nav, shell] = await Promise.all([
    read('src/components/MobileBottomNav.astro'),
    read('src/components/design-system/shell-foundations.css'),
  ]);

  for (const token of [
    '--ke-color-mobile-nav-surface',
    '--ke-color-mobile-nav-border',
    '--ke-color-mobile-nav-inactive',
    '--ke-color-mobile-nav-active',
    '--ke-color-mobile-nav-active-icon-surface',
    '--ke-elevation-mobile-nav',
    '--ke-mobile-nav-icon-size',
    '--ke-mobile-nav-icon-container-width',
    '--ke-mobile-nav-icon-container-height',
    '--ke-mobile-nav-icon-container-radius',
  ]) {
    assert.match(shell, new RegExp(`${token}:`, 'u'), `${token} must remain shell-owned`);
    assert.match(nav, new RegExp(`var\\(${token}\\)`, 'u'), `${token} must be consumed by the navigation`);
  }

  assert.doesNotMatch(nav, /rgba\(121,48,20,\.13\)|#fffdf8|0 -9px 24px rgba\(72,45,25,\.07\)|#766b62|#221a14|rgba\(34,26,20,\.08\)/iu);
  assert.doesNotMatch(nav, /\.mobile-bottom-nav__icon\s*\{[^}]*\b(?:width:38px|height:28px|border-radius:10px)/su);
});
