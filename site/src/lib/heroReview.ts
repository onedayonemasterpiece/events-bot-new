export type HeroReviewComposition = 'poster-billboard' | 'poster-attached-card' | 'photo-cinematic-sheet' | 'photo-parallax-sheet' | 'compact-ticketing';

export interface HeroReviewCase {
  caseId: string;
  eventId: number;
  composition: HeroReviewComposition;
  variant: string;
  note: string;
}

export const HERO_REVIEW_CASES: HeroReviewCase[] = [
  {
    caseId: '5878-poster-billboard',
    eventId: 5878,
    composition: 'poster-billboard',
    variant: '5878 · Poster Billboard',
    note: 'Same-event OCR/poster baseline: image must reach both mobile edges; the decision sheet starts below the uncut poster.',
  },
  {
    caseId: '5878-poster-attached-card',
    eventId: 5878,
    composition: 'poster-attached-card',
    variant: '5878 · Poster Attached Card',
    note: 'Same event, stronger attached sheet; used to decide whether the overlap feels premium without covering poster text.',
  },
  {
    caseId: '5878-compact-ticketing',
    eventId: 5878,
    composition: 'compact-ticketing',
    variant: '5878 · Compact Ticketing',
    note: 'Same event fallback/control: less spectacle, more transaction clarity.',
  },
  {
    caseId: '6322-photo-cinematic-sheet',
    eventId: 6322,
    composition: 'photo-cinematic-sheet',
    variant: '6322 · Photo Cinematic Sheet',
    note: 'Same-event visual_only baseline: cover image may crop because the source is not an OCR poster.',
  },
  {
    caseId: '6322-photo-parallax-sheet',
    eventId: 6322,
    composition: 'photo-parallax-sheet',
    variant: '6322 · Photo Parallax Sheet',
    note: 'Same event motion experiment: stronger premium parallax + gentle zoom; not default while under review.',
  },
  {
    caseId: '6322-compact-ticketing',
    eventId: 6322,
    composition: 'compact-ticketing',
    variant: '6322 · Compact Ticketing',
    note: 'Same visual_only event in compact transactional mode for contrast.',
  },
  {
    caseId: '4913-photo-cinematic-sheet',
    eventId: 4913,
    composition: 'photo-cinematic-sheet',
    variant: '4913 · Photo Cinematic Sheet',
    note: 'Wide visual_only source: tests whether cover gives a stronger emotional hero than preserving the wide frame.',
  },
  {
    caseId: '4913-poster-attached-card',
    eventId: 4913,
    composition: 'poster-attached-card',
    variant: '4913 · Poster Attached Card',
    note: 'Wide source under poster-like composition: useful negative/control case for choosing defaults.',
  },
];

export const HERO_REVIEW_EVENT_GROUPS = [5878, 6322, 4913] as const;
