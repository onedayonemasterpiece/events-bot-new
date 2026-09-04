import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('weather date context consumes its established cross-route roles', async () => {
  const [weather, foundations] = await Promise.all([
    read('src/components/WeatherDateContext.astro'),
    read('src/components/design-system/event-detail-foundations.css'),
  ]);

  for (const token of [
    '--ke-color-weather-surface',
    '--ke-color-weather-mobile-surface',
    '--ke-color-weather-text',
    '--ke-color-weather-muted',
    '--ke-color-weather-accent',
    '--ke-color-weather-border',
    '--ke-weather-location-icon-size',
    '--ke-weather-inline-icon-size',
    '--ke-weather-radius',
    '--ke-weather-radius-compact',
    '--ke-weather-min-height',
    '--ke-weather-min-height-compact',
    '--ke-weather-min-height-mobile',
  ]) {
    assert.match(foundations, new RegExp(`${token}:`, 'u'), `${token} must remain foundation-owned`);
    assert.match(weather, new RegExp(`var\\(${token}\\)`, 'u'), `${token} must be consumed by WeatherDateContext`);
  }

  assert.doesNotMatch(weather, /#276b73|rgba\(226,\s*239,\s*239,\s*\.58\)|\b(?:min-height:\s*(?:74|108|116)px|border-radius:\s*(?:12|16)px)\b/iu);
  assert.doesNotMatch(weather, /\.weather-date-context__icon\s*\{[^}]*\b(?:width:\s*24px|height:\s*24px|flex:\s*0 0 24px)/su);
  assert.doesNotMatch(weather, /\.weather-date-context__water \.weather-date-context__icon\s*\{[^}]*\b(?:width:\s*15px|height:\s*15px|flex-basis:\s*15px)/su);
});
