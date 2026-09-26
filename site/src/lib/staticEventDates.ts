import {
  buildEventDateAvailability,
  eventDateManifest,
  type EventDateAvailability,
  type EventDateManifest,
} from './eventDateAvailability';
import {
  getCurrentDate,
  getAvailableWeekendRanges,
  getWeekendRange,
  getEvents,
  getPreviewBuild,
  isExhibitionLikeEvent,
} from './events';

/**
 * Keep route generation, mobile cells, the public manifest and Today runtime
 * guard on the exact same date-listing inventory.
 */
export function getStaticEventDateAvailability(): EventDateAvailability {
  const dateListingEvents = getEvents().filter((event) => !isExhibitionLikeEvent(event));
  return buildEventDateAvailability(dateListingEvents, getCurrentDate());
}

export function getStaticEventDateManifest(): EventDateManifest {
  return eventDateManifest(
    getStaticEventDateAvailability(),
    getPreviewBuild().generated_at,
  );
}

/** The immutable build shares one calendar model across all page consumers. */
const dateAccessoryModels = new Map<string, ReturnType<typeof buildDateAccessoryModel>>();
export function getStaticDateAccessoryModel(today:string,selectedDate:string,current:'today'|'tomorrow'|'weekend'|'date',weekendEnd='') {
  const key=[today,selectedDate,current,weekendEnd].join(':');
  if (!dateAccessoryModels.has(key)) dateAccessoryModels.set(key,buildDateAccessoryModel(today,selectedDate,current,weekendEnd));
  return dateAccessoryModels.get(key)!;
}
function buildDateAccessoryModel(today:string,selectedDate:string,current:'today'|'tomorrow'|'weekend'|'date',weekendEnd:string) {
const DAY = 86_400_000;
const parseDate = (value: string) => new Date(`${value}T12:00:00Z`);
const isoDate = (date: Date) => date.toISOString().slice(0, 10);
const tomorrow = isoDate(new Date(parseDate(today).getTime() + DAY));
const currentWeekendStart = getWeekendRange().start;
const availableWeekendStarts = new Set(getAvailableWeekendRanges().map((range) => range.start));
const availability = getStaticEventDateAvailability();
// On Sunday the current weekend starts one day before the build reference.
// Keep that selected Saturday in the rail/sheet instead of producing a
// calendar with no aria-current date.
const railDates = selectedDate < availability.today
  ? [selectedDate, ...availability.allDates]
  : availability.allDates;
// Full calendar months keep their conventional grid; past/empty dates have no link.
const firstCalendarDate = `${railDates[0].slice(0, 7)}-01`;
const calendarDates = Array.from({ length:Math.round((parseDate(availability.horizonEnd).getTime() - parseDate(firstCalendarDate).getTime()) / DAY) + 1 }, (_, index) => isoDate(new Date(parseDate(firstCalendarDate).getTime() + index * DAY)));
const weekdayFormatter = new Intl.DateTimeFormat('ru-RU', { weekday:'short', timeZone:'Europe/Kaliningrad' });
const monthFormatter = new Intl.DateTimeFormat('ru-RU', { month:'short', timeZone:'Europe/Kaliningrad' });
const monthLongFormatter = new Intl.DateTimeFormat('ru-RU', { month:'long', year:'numeric', timeZone:'Europe/Kaliningrad' });
const dates = calendarDates.map((iso) => {
  const date = parseDate(iso);
  const weekday = weekdayFormatter.format(date).replace('.', '').toLocaleUpperCase('ru-RU');
  const day = date.getUTCDate();
  const month = monthFormatter.format(date).replace('.', '').toLocaleUpperCase('ru-RU');
  const monthLong = monthLongFormatter.format(date);
  const monthKey = iso.slice(0, 7);
  const isSaturday = date.getUTCDay() === 6;
  const isSunday = date.getUTCDay() === 0;
  const nextDate = new Date(date.getTime() + DAY);
  const hasEvents = availability.availableDates.has(iso);
  const weekendAvailable = isSaturday && availableWeekendStarts.has(iso);
  const destinationAvailable = hasEvents || weekendAvailable;
  const href: string | null = !destinationAvailable
    ? null
    : iso === today
      ? '/segodnya/'
      : iso === tomorrow
        ? '/zavtra/'
        : weekendAvailable
          ? (iso === currentWeekendStart ? '/vyhodnye/' : `/vyhodnye/${iso}/`)
          : `/date-${iso}/`;
  return {
    iso,
    weekday,
    day,
    month,
    monthLong,
    monthKey,
    href,
    calendarHref: hasEvents ? (iso === today ? '/segodnya/' : iso === tomorrow ? '/zavtra/' : `/date-${iso}/`) : null,
    hasEvents,
    isSaturday,
    isSunday,
    isWeekendRange: weekendAvailable,
    rangeLabel: weekendAvailable ? `${day}–${nextDate.getUTCDate()}` : String(day),
    selected: iso === selectedDate,
  };
});
const monthGroups = [...new Set(dates.map((item) => item.monthKey))].map((monthKey) => {
  const items = dates.filter((item) => item.monthKey === monthKey);
  // Calendar months start at day 1; the compact rail still starts at the reference day.
  const firstVisibleIso = items[0]?.iso || `${monthKey}-01`;
  const firstWeekday = (parseDate(firstVisibleIso).getUTCDay() + 6) % 7;
  return { monthKey, label: items[0]?.monthLong || monthKey, firstWeekday, items };
});
const selectedMonth = monthGroups.some((month) => month.monthKey === selectedDate.slice(0, 7))
  ? selectedDate.slice(0, 7)
  : monthGroups[0]?.monthKey;
const selectedLabel = current === 'weekend' && weekendEnd
  ? `${selectedDate} — ${weekendEnd}`
  : selectedDate;
return {dates,monthGroups,selectedMonth,selectedLabel,railDates,availability};
}
