import { execFileSync } from 'node:child_process';

const requestedRef = process.argv[2] || 'HEAD';
const ref = execFileSync('git', ['rev-parse', requestedRef], { encoding: 'utf8' }).trim();
const paths = execFileSync('git', ['ls-tree', '-r', '--name-only', ref], { encoding: 'utf8' })
  .split(/\r?\n/u)
  .filter(Boolean);

const show = (path) => execFileSync('git', ['show', `${ref}:${path}`], {
  encoding: 'utf8',
  maxBuffer: 16 * 1024 * 1024,
});
const styleText = (path, source) => {
  if (path.endsWith('.css')) return source;
  if (!path.endsWith('.astro')) return '';
  return [...source.matchAll(/<style\b[^>]*>([\s\S]*?)<\/style>/giu)]
    .map((match) => match[1])
    .join('\n');
};
const sourcePaths = paths.filter((path) => (
  path.startsWith('site/src/')
  && /\.(?:astro|css|ts|mjs|json)$/u.test(path)
));
const consumerPaths = sourcePaths.filter((path) => (
  /^(?:site\/src\/(?:components|pages|layouts)\/)/u.test(path)
  && !path.startsWith('site/src/components/design-system/')
  && path !== 'site/src/components/Icon.astro'
  && path !== 'site/src/components/SocialIcon.astro'
  && !path.startsWith('site/src/components/brand/')
));
const centralPaths = sourcePaths.filter((path) => (
  path === 'site/src/styles/design-system.css'
  || path.startsWith('site/src/components/design-system/')
  || path === 'site/src/components/Icon.astro'
  || path === 'site/src/components/SocialIcon.astro'
  || path.startsWith('site/src/components/brand/')
));

const rawColorPattern = /#[0-9a-f]{3,8}\b|(?:rgba?|hsla?)\([^)]*\)/giu;
const rawLengthPattern = /(?:width|height|font-size|border-radius|gap|padding(?:-[a-z]+)?|margin(?:-[a-z]+)?):\s*(-?(?:\d*\.)?\d+(?:px|rem|em))(?![\w-])/giu;
const fontFamilyPattern = /font-family:\s*([^;]+);/giu;
const iconDeclarationPattern = /[^\n{}]*icon[^\n{}]*\{[^{}]*(?:width|height):\s*(-?(?:\d*\.)?\d+(?:px|rem|em))[^{}]*\}/giu;

const rawColorFiles = [];
const rawLengthFiles = [];
const localIconSizeFiles = [];
const fontFamilyFiles = [];
const inlineSvgFiles = [];
const colorCounts = new Map();

for (const path of consumerPaths) {
  const source = show(path);
  const styles = styleText(path, source);
  const colors = [...styles.matchAll(rawColorPattern)].map((match) => match[0].toLowerCase());
  const lengths = [...styles.matchAll(rawLengthPattern)].map((match) => match[1].toLowerCase());
  const iconSizes = [...styles.matchAll(iconDeclarationPattern)].map((match) => match[1].toLowerCase());
  const fontFamilies = [...styles.matchAll(fontFamilyPattern)]
    .map((match) => match[1].trim())
    .filter((value) => !value.includes('var('));
  const inlineSvgCount = path.endsWith('.astro') ? (source.match(/<svg\b/giu) || []).length : 0;

  if (colors.length) {
    rawColorFiles.push({ path, count: colors.length, values: [...new Set(colors)].sort() });
    for (const color of colors) colorCounts.set(color, (colorCounts.get(color) || 0) + 1);
  }
  if (lengths.length) rawLengthFiles.push({ path, count: lengths.length, values: [...new Set(lengths)].sort() });
  if (iconSizes.length) localIconSizeFiles.push({ path, count: iconSizes.length, values: [...new Set(iconSizes)].sort() });
  if (fontFamilies.length) fontFamilyFiles.push({ path, values: [...new Set(fontFamilies)].sort() });
  if (inlineSvgCount) inlineSvgFiles.push({ path, count: inlineSvgCount });
}

const centralSources = Object.fromEntries(centralPaths.map((path) => [path, show(path)]));
const legacyCss = centralSources['site/src/styles/design-system.css'] || '';
const foundationsTs = centralSources['site/src/components/design-system/foundations.ts'] || '';
const foundationCss = Object.entries(centralSources)
  .filter(([path]) => path.endsWith('.css'))
  .map(([, source]) => source)
  .join('\n');
const roleBlock = foundationsTs.match(/export const ICON_SIZE_ROLES = \{([\s\S]*?)\} as const;/u)?.[1] || '';
const iconRoles = Object.fromEntries(
  [...roleBlock.matchAll(/^\s*([a-z-]+):\s*(\d+),?\s*$/gmu)]
    .map((match) => [match[1], Number(match[2])]),
);
const fontDeclaration = legacyCss.match(/--ke-font-sans:\s*([^;]+);/u)?.[1]?.trim() || null;
const fontAssets = paths.filter((path) => /(?:^|\/)(?:fonts?|assets\/[^/]*fonts?)(?:\/|$)|\.(?:woff2?|ttf|otf)$/iu.test(path));
const typographyAuthorityPath = 'site/src/components/design-system/f0-typography-authority.v1.json';
const typographyAuthority = paths.includes(typographyAuthorityPath)
  ? JSON.parse(show(typographyAuthorityPath))
  : null;
const activeFontAssetPaths = new Set(typographyAuthority?.delivery?.authoritative_asset_paths || []);
const fontAssetInventory = {
  classification: 'repository_inventory_not_implicit_UI_authority',
  total_count: fontAssets.length,
  active_authority_paths: fontAssets.filter((path) => activeFontAssetPaths.has(path)),
  inactive_inventory_paths: fontAssets.filter((path) => !activeFontAssetPaths.has(path)),
};
const registeredFontBypassPaths = new Set(
  typographyAuthority?.actual_consumer_migration_bindings?.map((binding) => binding.path) || [],
);
const unregisteredFontFamilyConsumers = typographyAuthority
  ? fontFamilyFiles.filter((item) => (
      item.values.some((value) => /\bInter\b/u.test(value))
      && !registeredFontBypassPaths.has(item.path)
    ))
  : fontFamilyFiles.filter((item) => item.values.some((value) => /\bInter\b/u.test(value)));
const typeRoles = [...foundationCss.matchAll(/--ke-type-([a-z0-9-]+):/giu)].map((match) => match[1]);
const centralContainers = [...foundationCss.matchAll(/--ke-container-([a-z0-9-]+):/giu)].map((match) => match[1]);
const centralBreakpoints = [...foundationCss.matchAll(/--ke-breakpoint-([a-z0-9-]+):/giu)].map((match) => match[1]);
const canonicalActions = [...foundationsTs.matchAll(/^\s*'([^']+)':\s*\{/gmu)].map((match) => match[1]);
const obsoleteLegacyOwners = [...legacyCss.matchAll(/(?:^|\n)\.ke-(badge|field|state-panel)(?:--|__|\s|\{|:)/gu)].map((match) => match[1]);

const report = {
  schema: 'kenigevents.f0-candidate-consumer-census.v1',
  requested_ref: requestedRef,
  resolved_ref: ref,
  source_file_count: sourcePaths.length,
  consumer_file_count: consumerPaths.length,
  central_file_count: centralPaths.length,
  checklist_evidence: {
    11: {
      font_declaration: fontDeclaration,
      font_authority: typographyAuthority ? {
        status: typographyAuthority.status,
        primary_family: typographyAuthority.family.primary,
        css_stack: typographyAuthority.family.css_stack,
        primitive_owner: typographyAuthority.family.primitive_owner,
        semantic_owner: typographyAuthority.family.semantic_owner,
        owner_approval_claimed: typographyAuthority.family.owner_approval_claimed,
        delivery_mode: typographyAuthority.delivery.mode,
        pm0_state: typographyAuthority.pm0_item_11_state,
      } : {
        status: 'MISSING_FROM_REF',
        path: typographyAuthorityPath,
      },
      font_assets_are_authority: false,
      font_assets: fontAssets,
      font_asset_inventory: fontAssetInventory,
      registered_consumer_migration_bindings: typographyAuthority?.actual_consumer_migration_bindings || [],
      unregistered_named_family_consumers: unregisteredFontFamilyConsumers,
      weight_role_count: new Set([...foundationCss.matchAll(/--ke-type-[a-z0-9-]+-weight:/giu)].map((match) => match[0])).size,
    },
    12: {
      type_roles: [...new Set(typeRoles)].sort(),
      raw_font_family_consumers: fontFamilyFiles,
    },
    13: {
      consumer_files_with_raw_length_values: rawLengthFiles.length,
      top_files: rawLengthFiles.sort((left, right) => right.count - left.count || left.path.localeCompare(right.path)).slice(0, 40),
    },
    14: {
      central_containers: [...new Set(centralContainers)].sort(),
      central_breakpoints: [...new Set(centralBreakpoints)].sort(),
    },
    15: {
      canonical_component_owner_present: ['Badge', 'Field', 'StatePanel'].every((name) => foundationsTs.includes(name) || foundationCss.includes(`ke-foundation-${name.replace(/[A-Z]/gu, (letter) => `-${letter.toLowerCase()}`).replace(/^-/, '')}`)),
      obsolete_legacy_owner_matches: obsoleteLegacyOwners,
    },
    16: {
      consumer_files_with_raw_colors: rawColorFiles.length,
      occurrence_count: rawColorFiles.reduce((sum, item) => sum + item.count, 0),
      top_values: [...colorCounts.entries()].sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0])).slice(0, 50),
      top_files: rawColorFiles.sort((left, right) => right.count - left.count || left.path.localeCompare(right.path)).slice(0, 50),
    },
    17: {
      canonical_action_count: canonicalActions.length,
      canonical_actions: canonicalActions,
      consumer_inline_svg_files: inlineSvgFiles,
    },
    18: {
      icon_roles: iconRoles,
      consumer_files_with_raw_icon_sizes: localIconSizeFiles,
    },
    19: {
      brand_root_files: paths.filter((path) => path.startsWith('site/src/components/brand/')),
      social_root_files: paths.filter((path) => /(?:^|\/)SocialIcon\.astro$/u.test(path)),
      medallion_root_files: paths.filter((path) => /Medallion[^/]*\.astro$/u.test(path)),
    },
  },
};

process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
