import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

function count(source, pattern) {
  return (source.match(pattern) || []).length;
}

test('MobileBottomNav delegates visible glyphs to canonical semantic/asset owners and the control role', async () => {
  const [nav, semantic, icon, foundations] = await Promise.all([
    read('src/components/MobileBottomNav.astro'),
    read('src/components/design-system/SemanticIcon.astro'),
    read('src/components/Icon.astro'),
    read('src/components/design-system/foundations.ts'),
  ]);

  assert.match(nav, /import SemanticIcon from '\.\/design-system\/SemanticIcon\.astro'/u);
  assert.match(nav, /import \{ BASE_PATH, withBase \} from '\.\.\/lib\/events'/u);
  assert.match(nav, /withBase\('\/assets\/icons\/reference4-v8\/search-thin\.svg'\)/u);
  assert.match(nav, /icon: 'ticket'/u);
  assert.match(nav, /icon: 'calendar'/u);
  assert.match(nav, /icon: 'search'/u);
  assert.match(nav, /icon: 'personal'/u);
  assert.equal(count(nav, /<SemanticIcon\b/gu), 3);
  assert.match(nav, /<SemanticIcon name="ticket" role="control" \/>/u);
  assert.match(nav, /<SemanticIcon name="calendar" role="control" \/>/u);
  assert.match(nav, /<SemanticIcon name="spark" role="control" \/>/u);
  assert.doesNotMatch(nav, /<SemanticIcon[^>]+name=\{/u, 'navigation icon identities must stay statically inspectable');
  assert.match(nav, /data-ke-icon-name="search"/u);
  assert.match(nav, /data-ke-icon-asset="reference4-v8\/search-thin\.svg"/u);
  assert.match(nav, /class="mobile-bottom-nav__asset-icon ke-icon-role ke-icon-role--control ke-icon-contract--four-role-v1 ke-icon-size-owner--foundations"/u);
  assert.match(nav, /data-ke-icon-role="control"/u);
  assert.match(nav, /data-ke-icon-size-owner="foundations\.css"/u);

  assert.doesNotMatch(nav, /<svg\b/u, 'bottom navigation must not own inline SVG geometry');
  assert.doesNotMatch(nav, /\b21px\b/u, 'bottom navigation must not own a fifth icon size');
  assert.doesNotMatch(nav, /\.mobile-bottom-nav\s+svg\s*\{/u);
  assert.doesNotMatch(nav, /\.ke-icon-role[^}]*\b(?:width|height)\s*:/u);

  assert.match(nav, /aria-label="Навигация по афише"/u);
  assert.equal(count(nav, /aria-current=\{item\.key === current/gu), 1);
  for (const [label, route] of [
    ['Афиша', '/populyarnoe/'],
    ['Даты', '/segodnya/'],
    ['Поиск', '/poisk/'],
    ['Для меня', '/dlya-menya/'],
  ]) {
    assert.ok(nav.includes(`label: '${label}'`));
    assert.ok(nav.includes(`mobileDiscoveryHref('${route}'`));
  }
  assert.match(nav, /\.mobile-bottom-nav a \{[\s\S]*min-height:var\(--mobile-nav-h\)/u);
  assert.match(nav, /\.mobile-bottom-nav__icon \{ width:var\(--ke-mobile-nav-icon-container-width\); height:var\(--ke-mobile-nav-icon-container-height\);/u);
  assert.match(nav, /\.mobile-bottom-nav \.ke-icon-role \{ --ke-icon-size:var\(--ke-mobile-nav-icon-size\); \}/u);

  assert.match(semantic, /'ke-icon-contract--four-role-v1'/u);
  assert.match(semantic, /'ke-icon-size-owner--foundations'/u);
  assert.match(semantic, /data-ke-icon-role=\{role\}/u);
  assert.match(semantic, /<Icon[\s\S]+dataIconName=\{name\}/u,
    'SemanticIcon must forward its machine-readable identity through the shared Icon renderer');
  assert.match(semantic, /<Icon[\s\S]+dataIconRole=\{role\}/u,
    'SemanticIcon must forward its canonical size role through the shared Icon renderer');
  assert.match(icon, /data-ke-icon-name=\{dataIconName\}/u);
  assert.match(icon, /data-ke-icon-role=\{dataIconRole\}/u);
  assert.match(icon, /data-ke-icon-contract=\{dataIconContract\}/u);
  assert.match(icon, /data-ke-icon-size-owner=\{dataIconSizeOwner\}/u);
  assert.match(foundations, /inline: 16,[\s\S]*control: 20,[\s\S]*action: 24,[\s\S]*feature: 32,/u);
  assert.equal(count(foundations, /^\s*(?:inline|control|action|feature): \d+,?$/gmu), 4);
});

test('ServiceShareAction uses canonical action/status icons without changing behavior or accessibility', async () => {
  const [share, foundations] = await Promise.all([
    read('src/components/ServiceShareAction.astro'),
    read('src/components/design-system/foundations.ts'),
  ]);

  assert.match(share, /import SemanticIcon from '\.\/design-system\/SemanticIcon\.astro'/u);
  assert.doesNotMatch(share, /<svg\b/u, 'service share must not own local SVG copies');
  assert.doesNotMatch(share, /\b1\.15rem\b/u, 'service share must use a central icon role');
  assert.doesNotMatch(share, /linkIconUrl|service-share-icon-url|service-share-action__icon--mask/u);
  assert.doesNotMatch(share, /\.service-share-action__icon\s*\{[^}]*\b(?:width|height)\s*:/su);

  assert.equal(count(share, /<SemanticIcon\b/gu), 6);
  for (const name of ['share', 'image', 'link']) {
    assert.equal(count(share, new RegExp(`<SemanticIcon name="${name}" role="control" \\/>`, 'gu')), 1);
  }
  assert.equal(count(share, /<SemanticIcon name="check" role="control" \/>/gu), 3);
  assert.equal(count(share, /data-service-share-icon-default/gu), 3);
  assert.equal(count(share, /data-service-share-icon-success/gu), 3);

  for (const canonical of [
    ["'action.share'", "name: 'share'"],
    ["'action.gallery'", "name: 'image'"],
    ["'action.link'", "name: 'link'"],
    ["'status.success'", "name: 'check'"],
  ]) {
    const start = foundations.indexOf(canonical[0]);
    assert.notEqual(start, -1, `${canonical[0]} must be canonical`);
    assert.ok(foundations.slice(start, start + 140).includes(canonical[1]), `${canonical[0]} must map to ${canonical[1]}`);
  }

  assert.equal(count(share, /data-service-share-button/gu), 3);
  for (const intent of ['mobile', 'image', 'text']) {
    assert.equal(count(share, new RegExp(`data-service-share-intent="${intent}"`, 'gu')), 1);
  }
  for (const label of [
    'Поделиться анонсами KenigEvents',
    'Скопировать карточку KenigEvents',
    'Скопировать текст и ссылку KenigEvents',
  ]) {
    assert.equal(count(share, new RegExp(`aria-label="${label}"`, 'gu')), 1);
  }
  assert.equal(count(share, /aria-describedby=\{statusId\}/gu), 3);
  assert.equal(count(share, /aria-busy="false"/gu), 3);
  assert.match(share, /\.service-share-action__button \{[\s\S]*min-width: 44px;[\s\S]*min-height: 44px;/u);
  assert.match(share, /data-service-share-status[\s\S]*role="status"[\s\S]*aria-live="polite"[\s\S]*aria-atomic="true"/u);
  assert.equal(count(share, /href="https:\/\/kenigevents\.ru\/"/gu), 2);
  assert.match(share, /data-service-share-fallback/u);
  assert.match(share, /<noscript>/u);
  assert.match(share, /data-service-shortcut-badge[^>]*aria-hidden="true">P<\/kbd>/u);
  assert.match(share, /data-service-shortcut-badge[^>]*aria-hidden="true">S<\/kbd>/u);
  assert.match(share, /import \{ hydrateServiceShareActions \} from '\.\.\/lib\/service-share\/controller\.js'/u);
  assert.match(share, /hydrateServiceShareActions\(\);/u);

  assert.match(share, /\.service-share-action__icon--success \{ display: none; \}/u);
  assert.match(share, /data-service-share-state="success"[\s\S]*\.service-share-action__icon--default \{ display: none; \}/u);
  assert.match(share, /data-service-share-state="success"[\s\S]*\.service-share-action__icon--success \{ display: inline-flex; \}/u);
  assert.match(share, /@media \(min-width: 768px\)[\s\S]*__button--mobile \{ display: none; \}[\s\S]*__button--desktop \{ display: inline-flex; \}/u);
  assert.match(share, /@media \(prefers-reduced-motion: reduce\)[\s\S]*transition: none/u);
});

test('the bounded cluster contains no raw inline SVG owner in either product component', async () => {
  const [nav, share] = await Promise.all([
    read('src/components/MobileBottomNav.astro'),
    read('src/components/ServiceShareAction.astro'),
  ]);
  assert.equal(count(`${nav}\n${share}`, /<svg\b/gu), 0);
  assert.equal(count(`${nav}\n${share}`, /<path\b/gu), 0);
});
