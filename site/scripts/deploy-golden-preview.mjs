const buildId = String(process.env.PREVIEW_BUILD_ID || '').trim();
if (!/^preview-golden-[a-zA-Z0-9._-]+$/u.test(buildId) || buildId.includes('/')) {
  throw new Error('Golden deployment requires an exact PREVIEW_BUILD_ID beginning with preview-golden-');
}
if (process.env.KENIGEVENTS_SITE_DEPLOY_DRY_RUN) {
  console.log(`Golden deploy dry run requested for ${buildId}`);
}
process.env.KENIGEVENTS_SITE_REQUIRE_PUBLIC_VERIFY = '1';
await import('./deploy-preview-yc.mjs');
