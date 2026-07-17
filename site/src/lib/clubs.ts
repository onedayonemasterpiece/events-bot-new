import clubsData from '../data/interest-clubs.json';
import { absoluteUrl, withBase } from './events';
import type { InterestClub, InterestClubsData, InterestClubMeeting } from './types';

export const INTEREST_CLUBS_SCHEMA_VERSION = 'interest-clubs-static-v1' as const;
export const INTEREST_CLUBS_PUBLIC_ENABLED = ['1', 'true', 'yes', 'on'].includes(
  String(import.meta.env.PUBLIC_INTEREST_CLUBS_ENABLED || '').trim().toLowerCase(),
);

const raw = clubsData as Partial<InterestClubsData>;
const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}$/u;
const SLUG_RE = /^[a-z0-9]+(?:-[a-z0-9]+)*$/u;

function isNullableText(value: unknown): value is string | null {
  return value === null || typeof value === 'string';
}

function validMeeting(value: InterestClubMeeting): boolean {
  return Number.isInteger(value?.event_id)
    && typeof value?.title === 'string'
    && Boolean(value.title.trim())
    && ISO_DATE_RE.test(value.start_date)
    && isNullableText(value.start_time)
    && isNullableText(value.display_time)
    && isNullableText(value.city)
    && isNullableText(value.venue_name)
    && isNullableText(value.event_path)
    && isNullableText(value.source_url);
}

function validClub(value: InterestClub): boolean {
  const activity = value?.activity;
  return Number.isInteger(value?.id)
    && SLUG_RE.test(value?.slug || '')
    && Boolean(value?.name?.trim())
    && Boolean(value?.topic?.trim())
    && isNullableText(value?.description)
    && isNullableText(value?.city)
    && isNullableText(value?.typical_venue)
    && Number.isInteger(activity?.meeting_count)
    && Number.isInteger(activity?.distinct_date_count)
    && Number.isInteger(activity?.future_meeting_count)
    && ISO_DATE_RE.test(activity?.first_observed_date || '')
    && ISO_DATE_RE.test(activity?.last_observed_date || '')
    && Array.isArray(value?.future_meetings)
    && value.future_meetings.every(validMeeting)
    && value.future_meetings.length === activity.future_meeting_count;
}

function validProjection(value: Partial<InterestClubsData>): value is InterestClubsData {
  return value.schema_version === INTEREST_CLUBS_SCHEMA_VERSION
    && value.projection_version === 1
    && typeof value.generated_at === 'string'
    && ISO_DATE_RE.test(value.current_date || '')
    && typeof value.source === 'string'
    && Array.isArray(value.clubs);
}

const projection: InterestClubsData = validProjection(raw)
  ? { ...raw, clubs: raw.clubs.filter(validClub) }
  : {
      schema_version: INTEREST_CLUBS_SCHEMA_VERSION,
      projection_version: 1,
      generated_at: '',
      current_date: '1970-01-01',
      source: 'invalid-contract-fallback',
      clubs: [],
    };

export function getInterestClubs(): InterestClub[] {
  if (!INTEREST_CLUBS_PUBLIC_ENABLED) return [];
  return [...projection.clubs].sort((left, right) => (
    Number(right.activity.future_meeting_count > 0) - Number(left.activity.future_meeting_count > 0)
    || right.activity.last_observed_date.localeCompare(left.activity.last_observed_date)
    || left.name.localeCompare(right.name, 'ru')
  ));
}

export function getInterestClubBySlug(slug: string): InterestClub | undefined {
  return projection.clubs.find((club) => club.slug === slug);
}

export function interestClubPath(club: Pick<InterestClub, 'slug'>): string {
  return `/kluby-po-interesam/${club.slug}/`;
}

export function interestClubHref(club: Pick<InterestClub, 'slug'>): string {
  return withBase(interestClubPath(club));
}

export function interestClubAbsoluteUrl(club: Pick<InterestClub, 'slug'>): string {
  return absoluteUrl(interestClubPath(club));
}

export function interestClubMeetingHref(meeting: InterestClubMeeting): string | null {
  return meeting.event_path ? withBase(meeting.event_path) : meeting.source_url;
}

export function getInterestClubsCurrentDate(): string {
  return projection.current_date;
}
