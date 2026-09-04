import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

test('SiteFooter consumes its established continuity roles instead of parallel literals', async () => {
  const source = await readFile(path.join(siteRoot, 'src/components/SiteFooter.astro'), 'utf8');
  const tokens = [
    '--ke-footer-padding-top', '--ke-footer-padding-bottom', '--ke-footer-stack-gap',
    '--ke-footer-share-min-height', '--ke-footer-share-gap', '--ke-footer-share-padding-top',
    '--ke-footer-share-padding-right', '--ke-footer-share-padding-bottom', '--ke-footer-share-padding-left',
    '--ke-footer-share-border-left', '--ke-footer-share-radius', '--ke-footer-share-heading-gap',
    '--ke-footer-share-heading-size', '--ke-footer-share-heading-line', '--ke-footer-share-heading-letter',
    '--ke-footer-share-heading-weight', '--ke-footer-share-strong-weight', '--ke-footer-share-wordmark-height',
    '--ke-footer-share-actions-max', '--ke-footer-share-action-basis', '--ke-footer-share-action-min-height',
    '--ke-footer-share-action-padding-inline', '--ke-footer-main-brand-min', '--ke-footer-main-brand-fr',
    '--ke-footer-main-column-min', '--ke-footer-main-gap', '--ke-footer-brand-gap',
    '--ke-footer-brand-link-max', '--ke-footer-lockup-gap', '--ke-footer-endorsement-size',
    '--ke-footer-endorsement-weight', '--ke-footer-endorsement-letter', '--ke-footer-wordmark-max',
    '--ke-footer-brand-copy-max', '--ke-footer-brand-copy-size', '--ke-footer-brand-copy-line',
    '--ke-footer-contact-weight', '--ke-footer-column-gap', '--ke-footer-column-heading-margin-bottom',
    '--ke-footer-column-heading-size', '--ke-footer-column-heading-weight', '--ke-footer-column-heading-letter',
    '--ke-footer-column-link-size', '--ke-footer-column-link-weight', '--ke-footer-column-link-line',
    '--ke-footer-link-underline-offset', '--ke-footer-notification-size', '--ke-footer-notification-margin-left',
    '--ke-footer-notification-ring-size', '--ke-footer-document-gap', '--ke-footer-document-note-size',
    '--ke-footer-document-note-weight', '--ke-footer-document-copy-margin', '--ke-footer-document-copy-size',
    '--ke-footer-document-copy-line', '--ke-footer-bottom-gap', '--ke-footer-bottom-padding-top',
    '--ke-footer-social-gap', '--ke-footer-social-control-gap', '--ke-footer-social-padding-block',
    '--ke-footer-social-padding-inline', '--ke-footer-social-label-size', '--ke-footer-social-label-weight',
    '--ke-footer-social-meta-size', '--ke-footer-utility-gap', '--ke-footer-utility-size',
    '--ke-footer-tablet-share-gap', '--ke-footer-mobile-padding-top', '--ke-footer-mobile-padding-bottom',
    '--ke-footer-mobile-stack-gap', '--ke-footer-mobile-share-padding', '--ke-footer-mobile-share-border-left',
    '--ke-footer-mobile-share-radius', '--ke-footer-mobile-share-heading-size',
    '--ke-footer-mobile-share-wordmark-height', '--ke-footer-mobile-main-row-gap',
    '--ke-footer-mobile-main-column-gap',
  ];
  for (const token of tokens) assert.match(source, new RegExp(`var\\(${token}\\)`), `${token} must be consumed`);

  assert.doesNotMatch(source, /font-size:\s*(?:\.68rem|\.72rem|\.9rem)/u);
  assert.doesNotMatch(source, /gap:\s*(?:\.28em|\.32rem|\.56rem)/u);
  assert.doesNotMatch(source, /width:\s*(?:220|240)px/u);
});
