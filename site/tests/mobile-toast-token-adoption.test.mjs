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
  }

  assert.match(toast, /data-app-lower-surface="notification"/u);
  for (const token of ['--ke-color-toast-action', '--ke-color-toast-progress', '--ke-toast-control-size', '--ke-toast-close-icon-size']) {
    assert.match(toast, new RegExp(`var\\(${token}\\)`, 'u'), `${token} remains a local control/timing consumer`);
  }
  assert.doesNotMatch(toast, /border:1px solid var\(--ke-color-toast-border\)|box-shadow:var\(--ke-elevation-toast\)/u,
    'lower notification paint is owned by the shared shell');
  assert.doesNotMatch(toast, /\.mobile-toast__action,.mobile-toast__close\s*\{[^}]*\b(?:min-width:44px|min-height:44px)/su);
});
