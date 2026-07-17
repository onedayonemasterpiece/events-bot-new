export interface PreferredBusBoardingConfig {
  stop_name: string;
  locality_label: string;
  offset_from_terminal_minutes: number;
  time_is_estimated: boolean;
  map_url: string;
}

export interface ResolvedBusBoarding {
  terminalDeparture: string;
  terminalDepartureMinutes: number;
  boardingDeparture: string;
  boardingDepartureMinutes: number;
  boardingStop: string;
  boardingLocalityLabel: string;
  boardingMapUrl: string;
  boardingTimeEstimated: boolean;
  offsetFromTerminalMinutes: number;
  remainingRideMinutes: number;
  destinationArrivalMinutes: number;
}

export function busClockToMinutes(value: string | null | undefined): number | null {
  const match = /^(\d{2}):(\d{2})$/u.exec(String(value || ''));
  if (!match) return null;
  const hours = Number(match[1]);
  const minutes = Number(match[2]);
  if (hours > 23 || minutes > 59) return null;
  return hours * 60 + minutes;
}

export function busMinutesToClock(value: number): string {
  const normalized = ((value % (24 * 60)) + (24 * 60)) % (24 * 60);
  return `${String(Math.floor(normalized / 60)).padStart(2, '0')}:${String(normalized % 60).padStart(2, '0')}`;
}

export function resolveBusBoarding(input: {
  terminalDeparture: string;
  terminalStop: string;
  terminalRideEstimateMinutes: number;
  preferredBoarding?: PreferredBusBoardingConfig | null;
}): ResolvedBusBoarding | null {
  const terminalDepartureMinutes = busClockToMinutes(input.terminalDeparture);
  if (terminalDepartureMinutes === null) return null;

  const terminalRideEstimateMinutes = Number(input.terminalRideEstimateMinutes);
  if (!Number.isFinite(terminalRideEstimateMinutes) || terminalRideEstimateMinutes < 0) return null;

  const preferred = input.preferredBoarding;
  const offsetFromTerminalMinutes = preferred ? Number(preferred.offset_from_terminal_minutes) : 0;
  if (
    !Number.isFinite(offsetFromTerminalMinutes)
    || offsetFromTerminalMinutes < 0
    || offsetFromTerminalMinutes > terminalRideEstimateMinutes
  ) return null;

  const boardingDepartureMinutes = terminalDepartureMinutes + offsetFromTerminalMinutes;
  const remainingRideMinutes = terminalRideEstimateMinutes - offsetFromTerminalMinutes;

  return {
    terminalDeparture:input.terminalDeparture,
    terminalDepartureMinutes,
    boardingDeparture:busMinutesToClock(boardingDepartureMinutes),
    boardingDepartureMinutes,
    boardingStop:preferred?.stop_name || input.terminalStop,
    boardingLocalityLabel:preferred?.locality_label || '',
    boardingMapUrl:preferred?.map_url || '',
    boardingTimeEstimated:Boolean(preferred?.time_is_estimated),
    offsetFromTerminalMinutes,
    remainingRideMinutes,
    // Keep the destination estimate invariant when the UI moves boarding from
    // the terminal to a downstream stop: (terminal + full ride) equals
    // (preferred boarding + remaining ride).
    destinationArrivalMinutes:boardingDepartureMinutes + remainingRideMinutes,
  };
}
