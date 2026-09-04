import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('mobile toast consumes its established shell roles', async () => {
  const [toast, shell] = await Promise.all([
    read('src/components/MobileToastRegion.astro'),
    read('src/components/design-system/shell-foundations.css'),
  ]);

  for (const token of [
    '--ke-color-toast-surface',
    '--ke-color-toast-text',
    '--ke-color-toast-action',
    '--ke-color-toast-border',
    '--ke-color-toast-success-border',
    '--ke-color-toast-error-border',
    '--ke-color-toast-progress',
    '--ke-elevation-toast',
    '--ke-toast-radius',
    '--ke-toast-control-size',
    '--ke-toast-close-icon-size',
  ]) {
    assert.match(shell, new RegExp(`${token}:`, 'u'), `${token} must remain shell-owned`);
    assert.match(toast, new RegExp(`var\\(${token}\\)`, 'u'), `${token} must be consumed by the toast`);
  }

  assert.doesNotMatch(toast, /rgba\(121,48,20,\.18\)|#fffdf8|#221a14|0 14px 34px rgba\(72,45,25,\.18\)|rgba\(38,120,72,\.32\)|rgba\(164,59,47,\.36\)|#793014|#a54821/iu);
  assert.doesNotMatch(toast, /\.mobile-toast__action,.mobile-toast__close\s*\{[^}]*\b(?:min-width:44px|min-height:44px)/su);
});
