/**
 * F0 foundations contract for icon sizing and canonical SVG identity.
 *
 * Values are mirrored by foundations.css. Keep exactly four size roles.
 */
export const ICON_SIZE_ROLES = {
  inline: 16,
  control: 20,
  action: 24,
  feature: 32,
} as const;

export type IconSizeRole = keyof typeof ICON_SIZE_ROLES;

export const UI_ICON_NAMES = [
  'heart',
  'comment',
  'calendar',
  'dislike',
  'share',
  'pin',
  'ticket',
  'info',
  'spark',
  'lock',
  'image',
  'phone',
  'copy',
  'check',
  'bus',
  'car',
  'walk',
  'install',
] as const;

export type UiIconName = (typeof UI_ICON_NAMES)[number];

export const LOCAL_SEMANTIC_ICON_NAMES = [
  'trend-up',
  'close',
  'arrow-left',
  'arrow-right',
  'chevron-down',
  'link',
] as const;
export type LocalSemanticIconName = (typeof LOCAL_SEMANTIC_ICON_NAMES)[number];

export const SEMANTIC_ICON_NAMES = [...UI_ICON_NAMES, ...LOCAL_SEMANTIC_ICON_NAMES] as const;
export type SemanticIconName = (typeof SEMANTIC_ICON_NAMES)[number];

export const SOCIAL_ICON_NAMES = ['telegram', 'vk', 'max'] as const;
export type SocialIconName = (typeof SOCIAL_ICON_NAMES)[number];

/**
 * One canonical visible SVG identity per semantic action.
 * Existing actions continue to render through ../Icon.astro. Semantic-only
 * navigation/status glyphs are centralized in SemanticIcon.astro.
 */
export const CANONICAL_SVG_BY_ACTION = {
  'feedback.like': { component: '../Icon.astro', name: 'heart' },
  'feedback.comment': { component: '../Icon.astro', name: 'comment' },
  'feedback.not-interested': { component: '../Icon.astro', name: 'dislike' },
  'action.calendar': { component: '../Icon.astro', name: 'calendar' },
  'action.share': { component: '../Icon.astro', name: 'share' },
  'action.link': { component: './SemanticIcon.astro', name: 'link' },
  'action.location': { component: '../Icon.astro', name: 'pin' },
  'action.ticket': { component: '../Icon.astro', name: 'ticket' },
  'action.information': { component: '../Icon.astro', name: 'info' },
  'action.highlight': { component: '../Icon.astro', name: 'spark' },
  'action.locked': { component: '../Icon.astro', name: 'lock' },
  'action.gallery': { component: '../Icon.astro', name: 'image' },
  'action.phone': { component: '../Icon.astro', name: 'phone' },
  'action.copy': { component: '../Icon.astro', name: 'copy' },
  'action.install': { component: '../Icon.astro', name: 'install' },
  'action.close': { component: './SemanticIcon.astro', name: 'close' },
  'action.disclosure': { component: './SemanticIcon.astro', name: 'chevron-down' },
  'navigation.previous': { component: './SemanticIcon.astro', name: 'arrow-left' },
  'navigation.next': { component: './SemanticIcon.astro', name: 'arrow-right' },
  'status.success': { component: '../Icon.astro', name: 'check' },
  'transport.bus': { component: '../Icon.astro', name: 'bus' },
  'transport.car': { component: '../Icon.astro', name: 'car' },
  'transport.walk': { component: '../Icon.astro', name: 'walk' },
  'listing.trend': { component: './SemanticIcon.astro', name: 'trend-up' },
} as const satisfies Record<string, { component: string; name: SemanticIconName }>;

export const CANONICAL_SOCIAL_SVG_BY_SERVICE = {
  telegram: { component: '../SocialIcon.astro', name: 'telegram' },
  vk: { component: '../SocialIcon.astro', name: 'vk' },
  max: { component: '../SocialIcon.astro', name: 'max' },
} as const satisfies Record<SocialIconName, { component: string; name: SocialIconName }>;
