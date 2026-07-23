import gameVibesCover from '../assets/clubs/source/game-vibes-event-2897.webp';
import type { ImageMetadata } from 'astro';

export interface InterestClubCover {
  image: ImageMetadata;
  alt: string;
  sourceEventId: number;
  sourcePostUrl: string;
}

/**
 * Reviewed, source-grounded covers only.
 *
 * The current source images for `klub-issledovateley-neyronok` are generated
 * announcement illustrations, not trustworthy documentary photographs, so
 * that card deliberately uses the deterministic visual fallback.
 */
const INTEREST_CLUB_COVERS: Readonly<Record<string, InterestClubCover>> = {
  'game-vibes': {
    image: gameVibesCover,
    alt: 'Фигуры и кубик на поле настольной игры сообщества Game Vibes',
    sourceEventId: 2897,
    sourcePostUrl: 'https://t.me/signalkld/9929',
  },
};

export function getInterestClubCover(slug: string): InterestClubCover | undefined {
  return INTEREST_CLUB_COVERS[slug];
}
