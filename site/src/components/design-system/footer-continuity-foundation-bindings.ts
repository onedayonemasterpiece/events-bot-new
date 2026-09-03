export type FooterContinuityKind = 'color' | 'type' | 'spacing' | 'shape' | 'elevation' | 'icon';

export interface FooterContinuityBinding {
  consumer: 'site/src/components/SiteFooter.astro';
  sourceHead: string;
  owner: 'A0';
  kind: FooterContinuityKind;
  current: string;
  replacement: string;
  browserExpectation: string;
  negativeControl: string;
}

/** Current-head A0 handoff for the public footer. */
export const FOOTER_CONTINUITY_BINDINGS = [
  {
    consumer: 'site/src/components/SiteFooter.astro',
    sourceHead: '170cd7e7408101359ae450667c8c46ac08927ff5', owner: 'A0', kind: 'color',
    current: '#25211e inverse shell; footer text/copy/muted/contact hierarchy; warm share panel; social alpha states; unusual dot',
    replacement: 'var(--ke-color-footer-*); var(--ke-color-unusual-notification/-ring)',
    browserExpectation: 'inverse footer, warm share callout, document notes and social controls keep current contrast',
    negativeControl: 'official social-service SVG fills remain brand asset data',
  },
  {
    consumer: 'site/src/components/SiteFooter.astro',
    sourceHead: '170cd7e7408101359ae450667c8c46ac08927ff5', owner: 'A0', kind: 'spacing',
    current: 'local outer stack, share callout, four-column main, brand/columns/documents/bottom/social/utility and mobile spacing',
    replacement: 'var(--ke-footer-*)',
    browserExpectation: 'desktop four-column, tablet three-column and mobile two-column layouts retain current rhythm',
    negativeControl: 'link order, document disabled state and footer runtime hooks remain unchanged',
  },
  {
    consumer: 'site/src/components/SiteFooter.astro',
    sourceHead: '170cd7e7408101359ae450667c8c46ac08927ff5', owner: 'A0', kind: 'type',
    current: 'local share heading, endorsement, brand copy, column heading/link, document, social and utility typography',
    replacement: 'var(--ke-footer-*-size/weight/line/letter)',
    browserExpectation: 'share prompt, navigation columns and utility copy wrap at the same points',
    negativeControl: 'AnnouncementsWordmark remains SVG brand media',
  },
  {
    consumer: 'site/src/components/SiteFooter.astro',
    sourceHead: '170cd7e7408101359ae450667c8c46ac08927ff5', owner: 'A0', kind: 'shape',
    current: '20px/18px share radius; 5px/4px left accent; pill social controls; 7px unusual marker',
    replacement: 'var(--ke-footer-share-*); var(--ke-footer-mobile-share-*); var(--ke-footer-notification-*)',
    browserExpectation: 'callout silhouette and notification marker remain unchanged',
    negativeControl: 'social controls continue using the canonical pill shape',
  },
  {
    consumer: 'site/src/components/SiteFooter.astro',
    sourceHead: '170cd7e7408101359ae450667c8c46ac08927ff5', owner: 'A0', kind: 'elevation',
    current: 'share 0 16px 38px rgba(0,0,0,.16)',
    replacement: 'var(--ke-elevation-footer-share)',
    browserExpectation: 'warm share callout remains the only raised footer surface',
    negativeControl: 'footer shell and social controls remain flat',
  },
  {
    consumer: 'site/src/components/SiteFooter.astro',
    sourceHead: '170cd7e7408101359ae450667c8c46ac08927ff5', owner: 'A0', kind: 'icon',
    current: 'social-icon width/height 1.15rem',
    replacement: 'SocialIcon role="control"; var(--ke-footer-social-icon-size) -> control=20px',
    browserExpectation: 'Telegram/VK/MAX identities retain one consistent 20px footprint',
    negativeControl: 'do not alter official service fills and do not create a fifth icon role',
  },
] as const satisfies readonly FooterContinuityBinding[];
