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
  intake: 'src/components/FocusGroupInviteIntake.astro',
  connectivity: 'src/components/FocusConnectivityDiagnostic.astro',
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

test('focus invite intake consumes the central focus roles and canonical success identity', async () => {
  const intake = await read(consumers.intake);

  assert.match(intake, /import Button from '\.\/design-system\/Button\.astro'/u);
  assert.match(intake, /import SemanticIcon from '\.\/design-system\/SemanticIcon\.astro'/u);
  assert.match(intake, /data-ds-family="FocusGroupInviteIntake"/u);
  assert.match(intake, /data-ds-version="1"/u);
  assert.match(intake, /data-ds-variant="pwa-membership"/u);
  assert.match(intake, /data-ds-state="pending"/u);
  assert.match(intake, /data-ke-foundation-consumer="focus-invite-intake"/u);
  assert.match(intake, /root\.dataset\.dsState = stage/u);
  assert.match(intake, /root\.dataset\.dsState = 'error'/u);
  assert.match(intake, /<SemanticIcon name="check" role="feature" \/>/u);
  assert.doesNotMatch(intake, />✓</u);
  assert.match(intake, /<Button variant="primary" href=\{programmeHref\}>/u);
  assert.match(intake, /<Button variant="primary" size="large" href=\{homeHref\}>/u);

  for (const token of [
    '--ke-focus-intake-container',
    '--ke-focus-brand-image-size',
    '--ke-elevation-focus-brand',
    '--ke-color-focus-sheet-surface',
    '--ke-elevation-focus-card',
    '--ke-color-border-prize',
    '--ke-color-focus-consent-surface',
    '--ke-focus-otp-cell-size',
    '--ke-focus-intake-check-icon-size',
    '--ke-focus-intake-spinner-size',
  ]) assert.match(intake, new RegExp(token, 'u'));

  assert.doesNotMatch(
    intake,
    /width:\s*5\.75rem|background:\s*#fffdf8|border:\s*1px solid #ecd29a|background:\s*#fff4d6|background:\s*#f8f2e9|background:\s*#e6f4ef|background:\s*#d7f0ec|border:\s*1px solid #cdbfad/u,
    'intake no longer owns foundation palette, image size or field geometry',
  );
});

test('connectivity diagnostic consumes its complete product-theme cluster without changing probe topology', async () => {
  const source = await read(consumers.connectivity);

  assert.match(source, /data-ds-family="FocusConnectivityDiagnostic"/u);
  assert.match(source, /data-ds-version="1"/u);
  assert.match(source, /data-ds-variant="resilient-route-probe"/u);
  assert.match(source, /data-ds-state="idle"/u);
  assert.match(source, /data-ke-foundation-consumer="focus-connectivity"/u);
  assert.match(source, /root\.dataset\.dsState = 'checking'/u);
  assert.match(source, /root\.dataset\.dsState = failed === 0 \? 'complete' : 'degraded'/u);
  assert.match(source, /Promise\.all/u);
  assert.match(source, /selectionPromise = transport \? transport\.selectRoute\(true\)/u);
  assert.match(source, /attempts: 1, timeoutMs: 5_000/u);
  assert.match(source, /probeTimeoutMs: 3_500/u);
  assert.match(source, /safeRequestTimeoutMs: 3_500/u);

  for (const token of [
    '--ke-color-connectivity-text',
    '--ke-connectivity-card-max',
    '--ke-color-connectivity-card-border',
    '--ke-elevation-connectivity-card',
    '--ke-connectivity-action-min-height',
    '--ke-elevation-connectivity-action',
    '--ke-color-connectivity-result-surface',
    '--ke-color-connectivity-success',
    '--ke-color-connectivity-error',
    '--ke-color-connectivity-input-border',
    '--ke-color-connectivity-copy-surface',
    '--ke-connectivity-card-radius-mobile',
  ]) assert.match(source, new RegExp(token, 'u'));

  assert.doesNotMatch(
    source,
    /color:\s*#33261e|border:\s*1px solid #e3d2bd|background:\s*#fffdf8|box-shadow:\s*0 16px 40px|background:\s*#a74523|background:\s*#f5eee5|color:\s*#1f744e|color:\s*#b02c2c|border:\s*1px solid #d9c5b4/u,
    'connectivity surface no longer owns the central product-theme palette or elevation',
  );
});

test('data-hook and hidden/download controls stay explicit compatibility consumers until Button supports passthrough attributes', async () => {
  const [feedback, share, pwa, lab, intake, connectivity] = await Promise.all([
    read(consumers.feedback),
    read(consumers.share),
    read(consumers.pwa),
    read(consumers.lab),
    read(consumers.intake),
    read(consumers.connectivity),
  ]);
  assert.match(feedback, /data-ke-button-compat="runtime-hook" data-feedback-open/u);
  assert.match(share, /data-ke-button-compat="runtime-hook-download"[\s\S]*data-focus-share-qr-download/u);
  assert.match(pwa, /data-ke-button-compat="runtime-hook-hidden"[\s\S]*data-pwa-install-button[\s\S]*hidden/u);
  assert.match(lab, /data-ke-button-compat="runtime-hook" data-focus-issue-submit/u);
  assert.match(intake, /data-ke-button-compat="runtime-hook" data-focus-email-open/u);
  assert.match(intake, /data-ke-button-compat="runtime-hook" data-focus-account-continue/u);
  assert.match(connectivity, /data-ke-button-compat="runtime-hook" data-connectivity-run/u);
  assert.match(connectivity, /data-ke-button-compat="runtime-hook" data-connectivity-copy-button/u);
});
