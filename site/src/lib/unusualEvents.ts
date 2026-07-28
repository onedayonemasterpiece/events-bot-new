import unusualData from '../data/unusual-events.json';
import { getCurrentDate, getEvents } from './events';
import type { PreviewEvent } from './types';
import { resolveUnusualFeed as resolveFeed } from './unusualManifest.mjs';

export interface UnusualFeedItem {
  conceptId: string;
  tier: string;
  score: number;
  confidence: number;
  families: string[];
  reasonCodes: string[];
  firstPublishedAt: string | null;
  notifyEligible: boolean;
  event: PreviewEvent;
}

export interface UnusualFeed {
  approved: boolean;
  status: string;
  buildId: string | null;
  generatedAt: string | null;
  baselineAt: string | null;
  items: UnusualFeedItem[];
  unreadCandidates: Array<{ conceptId: string; firstPublishedAt: string }>;
}

export function resolveUnusualFeed(raw: unknown, catalog: PreviewEvent[], today: string): UnusualFeed {
  return resolveFeed(raw, catalog, today) as UnusualFeed;
}

export function getUnusualFeed(): UnusualFeed {
  return resolveUnusualFeed(unusualData, getEvents(), getCurrentDate());
}
