import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

function assertConsumes(source, root, tokens) {
  for (const token of tokens) {
    assert.match(source, new RegExp(`var\\(${token.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&')}\\)`, 'u'), `${root} must consume ${token}`);
  }
}

test('production footer, service-share, and PWA roots consume their canonical visible-role tokens', async () => {
  const [footer, share, pwa] = await Promise.all([
    read('src/components/SiteFooter.astro'),
    read('src/components/ServiceShareAction.astro'),
    read('src/components/PwaInstallAction.astro'),
  ]);

  assertConsumes(footer, 'SiteFooter', [
    '--ke-color-footer-surface',
    '--ke-color-footer-share-surface',
    '--ke-color-footer-control-surface',
    '--ke-elevation-footer-share',
  ]);
  assertConsumes(share, 'ServiceShareAction', [
    '--ke-color-service-share-border',
    '--ke-color-service-share-success-surface',
    '--ke-color-service-share-error-surface',
    '--ke-elevation-service-share-shortcut',
  ]);
  assertConsumes(pwa, 'PwaInstallAction', [
    '--ke-pwa-install-surface',
    '--ke-color-pwa-install-button-surface',
    '--ke-color-pwa-install-presentation-surface',
    '--ke-elevation-pwa-install-presentation',
  ]);

  for (const [source, root, rawPaint] of [
    [footer, 'SiteFooter', /#25211e|#fff8ee|#9b3f1d|#f8ecdd|rgba\(255,255,255,\.045\)/u],
    [share, 'ServiceShareAction', /#3f8a5b|#2f7449|#a85645|#8b4537|rgba\(121, 48, 20, \.48\)/u],
    [pwa, 'PwaInstallAction', /#25211e|#fff8ee|#cdbfb4|#d8c9bd|rgba\(255,255,255,\.055\)/u],
  ]) {
    assert.doesNotMatch(source, rawPaint, `${root} must not retain a tokenized visible paint literal`);
  }
});
