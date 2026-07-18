async page => {
  const rawBaseUrl = page.url();
  const baseUrl = rawBaseUrl.endsWith('/') ? rawBaseUrl : `${rawBaseUrl}/`;
  const fixtures = [
    [5755, 'zoykina-kvartira-kaliningrad-5755'],
    [6408, 'spektakl-sobaka-na-sene-kaliningrad-6408'],
    [4783, 'myuzikl-alye-parusa-kaliningrad-4783'],
  ];
  const eventUrl = slug => `${baseUrl}sobytiya/${slug}/`;
  const results = [];
  const expect = (condition, message) => {
    if (!condition) throw new Error(message);
  };

  await page.setViewportSize({ width: 1440, height: 900 });
  await page.emulateMedia({ reducedMotion: 'reduce' });
  for (const [eventId, slug] of fixtures) {
    await page.goto(eventUrl(slug), { waitUntil: 'domcontentloaded' });
    const root = page.locator('[data-desktop-clean-event]');
    const media = root.locator('[data-media-frame]').first();
    const hero = root.locator('[data-clean-hero-image]');
    const count = await root.locator('[data-closed-hero-status]').evaluate(node => Number(node.textContent?.match(/из (\d+)/u)?.[1] || 0));
    expect(count > 1, `${eventId}: closed hero must have multiple images`);
    const beforeSrc = await hero.getAttribute('src');
    await media.hover();
    const beforeScroll = await page.evaluate(() => window.scrollY);
    await page.keyboard.press('ArrowRight');
    await page.waitForFunction(() => document.querySelector('[data-desktop-clean-event]')?.getAttribute('data-active-hero-gallery-index') === '1');
    await page.waitForFunction(src => document.querySelector('[data-clean-hero-image]')?.getAttribute('src') !== src, beforeSrc);
    expect(await page.evaluate(() => window.scrollY) === beforeScroll, `${eventId}: ArrowRight must not scroll while the hero is hovered`);
    expect((await root.locator('[data-closed-hero-status]').textContent())?.includes(`из ${count}`), `${eventId}: live status must expose the image count`);

    const indexAfterArrow = await root.getAttribute('data-active-hero-gallery-index');
    await page.keyboard.press('Control+ArrowRight');
    expect(await root.getAttribute('data-active-hero-gallery-index') === indexAfterArrow, `${eventId}: modified arrow must be ignored`);
    await page.evaluate(() => {
      const input = document.createElement('input');
      input.setAttribute('data-gallery-gate-input', '');
      input.style.position = 'fixed';
      input.style.inset = '0 auto auto 0';
      document.body.append(input);
      input.focus();
    });
    await page.keyboard.press('ArrowRight');
    expect(await root.getAttribute('data-active-hero-gallery-index') === indexAfterArrow, `${eventId}: input arrow must be ignored`);
    await page.locator('[data-gallery-gate-input]').evaluate(node => node.remove());
    results.push({ eventId, closedHeroArrow: 'pass' });
  }

  await page.emulateMedia({ reducedMotion: 'reduce' });
  await page.goto(eventUrl(fixtures[0][1]), { waitUntil: 'domcontentloaded' });
  const desktopOpener = page.locator('.desktop-prototype__media-opener[data-hero-gallery-open]').first();
  const desktopGalleryId = await desktopOpener.getAttribute('data-hero-gallery-open');
  const desktopGallery = page.locator(`#${desktopGalleryId}`);
  await desktopOpener.click();
  await desktopGallery.waitFor({ state: 'visible' });
  expect(await desktopGallery.getAttribute('data-auto-advance') === 'false', 'reduced motion must disable timed gallery advance');
  expect(await desktopGallery.getAttribute('data-auto-pan') === null, 'reduced motion must disable timed gallery pan');
  await desktopGallery.locator('[data-hero-gallery-slide][aria-hidden="false"] .hero-gallery__image').click({ position: { x: 24, y: 180 } });
  await desktopGallery.waitFor({ state: 'hidden' });
  expect(await desktopGallery.isHidden(), 'desktop image click must close the gallery');

  await desktopOpener.click();
  await desktopGallery.waitFor({ state: 'visible' });
  const desktopCta = desktopGallery.locator('[data-gallery-slide-kind="cta"] a[href]');
  expect(await desktopCta.getAttribute('tabindex') === '-1', 'inactive CTA must not be tabbable');
  const desktopSlides = await desktopGallery.locator('[data-hero-gallery-slide]').count();
  for (let step = 1; step < desktopSlides; step += 1) await desktopGallery.locator('[data-hero-gallery-next]').click();
  await page.waitForFunction(id => document.getElementById(id)?.querySelector('[data-gallery-slide-kind="cta"]')?.getAttribute('aria-hidden') === 'false', desktopGalleryId);
  expect(await desktopCta.getAttribute('tabindex') !== '-1', 'active CTA must restore tabbability');
  const desktopBeforeCta = page.url();
  await desktopCta.click();
  await page.waitForURL(url => url.href !== desktopBeforeCta);
  expect(page.url() !== desktopBeforeCta, 'desktop CTA must navigate');

  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto(eventUrl(fixtures[1][1]), { waitUntil: 'domcontentloaded' });
  const mobileOpener = page.locator('.event-hero__visual[data-hero-gallery-open]').first();
  const mobileGalleryId = await mobileOpener.getAttribute('data-hero-gallery-open');
  const mobileGallery = page.locator(`#${mobileGalleryId}`);
  await mobileOpener.click();
  await mobileGallery.waitFor({ state: 'visible' });
  const mobileSlides = await mobileGallery.locator('[data-hero-gallery-slide]').count();
  for (let step = 1; step < mobileSlides; step += 1) await mobileGallery.locator('[data-hero-gallery-next]').click();
  await page.waitForFunction(id => document.getElementById(id)?.querySelector('[data-gallery-slide-kind="cta"]')?.getAttribute('aria-hidden') === 'false', mobileGalleryId);
  const mobileCta = mobileGallery.locator('[data-gallery-slide-kind="cta"] a[href]');
  const mobileBeforeCta = page.url();
  await mobileCta.click();
  await page.waitForURL(url => url.href !== mobileBeforeCta);
  expect(page.url() !== mobileBeforeCta, 'mobile CTA must navigate');
  results.push({ desktopStandardGallery: 'pass', mobileStandardGallery: 'pass' });
  return results;
}
