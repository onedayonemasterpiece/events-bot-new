import type { PersonalizationRuntimeMode } from './contract.ts';

export interface PersonalizationRuntimeModeResolution {
  mode: PersonalizationRuntimeMode;
  diagnostic: 'p13n_mode.explicit' | 'p13n_mode.default' | 'p13n_mode.invalid_off';
}

export function resolvePersonalizationRuntimeMode(
  requested: unknown,
  siteMode: unknown,
): PersonalizationRuntimeModeResolution {
  const normalized = String(requested || '').trim().toLowerCase();
  if (normalized === 'off' || normalized === 'characterize' || normalized === 'local-shadow') {
    return { mode: normalized, diagnostic: 'p13n_mode.explicit' };
  }
  if (normalized) return { mode: 'off', diagnostic: 'p13n_mode.invalid_off' };
  return String(siteMode || '').trim().toLowerCase() === 'production'
    ? { mode: 'off', diagnostic: 'p13n_mode.default' }
    : { mode: 'characterize', diagnostic: 'p13n_mode.default' };
}
