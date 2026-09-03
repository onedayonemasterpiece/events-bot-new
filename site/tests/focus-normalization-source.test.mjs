import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

const consumers = {
  feedback: 'src/components/FocusGroupFeedback.astro',
  share: 'src/components/FocusGroupInviteShare.astro',
  pwa: 'src/components/FocusPwaInstallAction.astro',
  badge: 'src/components/FocusLabBadge.astro',
  lab: 'src/components/FocusGroupLabPanel.astro',
};

test('normalized focus consumers load the one product-contour foundation registry', async () => {
  const sources = await Promise.all(Object.values(consumers).map(read));
  for (const source of sources) {
    assert.match(source, /import '\.\/design-system\/product-contour-foundations\.css'/u);
  }
});

test('focus dialog close controls use the canonical control-size semantic icon', async () => {
  const [feedback, lab] = await Promise.all([read(consumers.feedback), read(consumers.lab)]);
  for (const source of [feedback, lab]) {
    assert.match(source, /import SemanticIcon from '\.\/design-system\/SemanticIcon\.astro'/u);
    assert.match(source, /<SemanticIcon name="close" role="control" \/>/u);
    assert.doesNotMatch(source, />×</u);
  }
  assert.equal((lab.match(/<SemanticIcon name="close" role="control" \/>/gu) || []).length, 2);
});

test('focus feedback, share and lab surfaces consume central panel, input, focus and elevation roles', async () => {
  const [feedback, share, lab] = await Promise.all([
    read(consumers.feedback),
    read(consumers.share),
    read(consumers.lab),
  ]);

  for (const source of [feedback, share]) {
    for (const token of [
      '--ke-color-focus-panel-surface',
      '--ke-focus-panel-radius',
      '--ke-elevation-focus-panel',
      '--ke-color-border-input',
    ]) assert.match(source, new RegExp(token, 'u'));
  }
  for (const token of [
    '--ke-color-border-panel-warm',
    '--ke-color-border-score',
    '--ke-color-focus-control-selected-accent',
    '--ke-elevation-focus-lab',
    '--ke-elevation-focus-dialog',
    '--ke-focus-dialog-lab-max',
  ]) assert.match(lab, new RegExp(token, 'u'));

  assert.doesNotMatch(feedback, /background:\s*#d7f0ec|background:\s*#f8e5d8|border:\s*1px solid #cdbfad/u);
  assert.doesNotMatch(share, /border:\s*1px solid #cdbfad|background:\s*#fffdf8/u);
  assert.doesNotMatch(lab, /border:\s*1px solid #dbc9b5|border:\s*1px solid #d8c8b5|border:\s*1px solid #cdbfad/u);
});

test('focus PWA and lab badge consume central brand and surface roles', async () => {
  const [pwa, badge] = await Promise.all([read(consumers.pwa), read(consumers.badge)]);
  for (const token of [
    '--ke-color-focus-panel-surface',
    '--ke-elevation-focus-pwa',
    '--ke-color-status-success-foreground',
  ]) assert.match(pwa, new RegExp(token, 'u'));
  for (const token of [
    '--ke-color-focus-brand-marker-surface',
    '--ke-color-focus-brand-marker-border',
    '--ke-focus-lab-icon-size',
    '--ke-focus-lab-icon-size-hero',
  ]) assert.match(badge, new RegExp(token, 'u'));
  assert.match(badge, /data-ds-family="FocusLabBadge"/u);
  assert.match(badge, /data-ds-version="1"/u);
});

test('data-hook and hidden/download controls stay explicit compatibility consumers until Button supports passthrough attributes', async () => {
  const [feedback, share, pwa, lab] = await Promise.all([
    read(consumers.feedback),
    read(consumers.share),
    read(consumers.pwa),
    read(consumers.lab),
  ]);
  assert.match(feedback, /data-ke-button-compat="runtime-hook" data-feedback-open/u);
  assert.match(share, /data-ke-button-compat="runtime-hook-download"[\s\S]*data-focus-share-qr-download/u);
  assert.match(pwa, /data-ke-button-compat="runtime-hook-hidden"[\s\S]*data-pwa-install-button[\s\S]*hidden/u);
  assert.match(lab, /data-ke-button-compat="runtime-hook" data-focus-issue-submit/u);
});
