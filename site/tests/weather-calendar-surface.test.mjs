import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { test } from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('weather surface is default-off, same-origin and mounted only on date contexts', async () => {
  const [component, dateSurface, weekendSurface, mobileSurface, runtime] = await Promise.all([
    read('src/components/WeatherDateContext.astro'),
    read('src/components/listings/DateListingSurface.astro'),
    read('src/components/listings/WeekendListingSurface.astro'),
    read('src/components/listings/MobileListingRailSurface.astro'),
    read('src/lib/weatherCalendarRuntime.ts'),
  ]);
  assert.match(component, /PUBLIC_WEATHER_CALENDAR_ENABLED \|\| '0'/u);
  assert.match(component, /withBase\('\/data\/weather\/v1\/current\.json'\)/u);
  assert.match(component, /data-weather-route-kind/u);
  assert.match(dateSurface, /<WeatherDateContext date=\{date\}/u);
  assert.match(dateSurface, /weather=\{\{ date, dateLabel, routeKind: kind \}\}/u);
  assert.equal((weekendSurface.match(/<WeatherDateContext date=\{day\.date\}/gu) || []).length, 1);
  assert.match(weekendSurface, /routeKind:'weekend'/u);
  assert.match(mobileSurface, /section\.weather \? <WeatherDateContext/u);
  assert.match(runtime, /url\.origin !== page\.origin/u);
  assert.match(runtime, /snapshot_integrity_mismatch/u);
  assert.doesNotMatch(runtime, /api\.open-meteo\.com|marine-api\.open-meteo\.com/u);
});

test('weather SVGRepo assets form one adapted CC0 outline family', async () => {
  const manifest = JSON.parse(await read('public/assets/weather/manifest.json'));
  assert.equal(manifest.license, 'CC0 License');
  assert.match(manifest.family, /Weather And Forecast Icons/u);
  assert.deepEqual(Object.keys(manifest.icons).sort(), [
    'clear', 'cloud', 'fog', 'heavy-rain', 'rain', 'showers', 'snow', 'thunderstorm', 'water-temperature',
  ]);
  const svgs = await Promise.all(Object.keys(manifest.icons).map((name) => read(`public/assets/weather/${name}.svg`)));
  for (const svg of svgs) {
    assert.match(svg, /viewBox="0 0 32 32"/u);
    assert.match(svg, /width="24" height="24"/u);
    assert.match(svg, /stroke:currentColor/u);
    assert.doesNotMatch(svg, /#000000/u);
  }
});
