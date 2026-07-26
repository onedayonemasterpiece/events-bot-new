import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import { stripTypeScriptTypes } from 'node:module';
import test from 'node:test';

const resolverUrl = new URL('./listingVenueMedallions.ts', import.meta.url);
const eventResolverUrl = new URL('./eventMedallions.ts', import.meta.url);
const organizerManifestUrl = new URL('../data/organizerMedallions.json', import.meta.url);
const festivalManifestUrl = new URL('../data/festivalMedallions.json', import.meta.url);

const [resolverSource, organizers, festivals] = await Promise.all([
  fs.readFile(resolverUrl, 'utf8'),
  fs.readFile(organizerManifestUrl, 'utf8'),
  fs.readFile(festivalManifestUrl, 'utf8'),
]);
const executableResolver = stripTypeScriptTypes(
  resolverSource
    .replace("import organizerMedallions from '../data/organizerMedallions.json';", `const organizerMedallions = ${organizers};`)
    .replace("import festivalMedallions from '../data/festivalMedallions.json';", `const festivalMedallions = ${festivals};`)
    .replace("from './eventMedallions';", `from '${eventResolverUrl.href}';`)
    .replaceAll(/import type .*?;\n/gu, ''),
  { mode:'transform' },
);
const { getListingIdentityMedallions } = await import(
  `data:text/javascript;base64,${Buffer.from(executableResolver).toString('base64')}`
);

const july26RuinKeepersEvent = {
  id:7018,
  title:'Воскресник в Озёрске',
  start_date:'2026-07-26',
  venue_name:'центр «Крупорушка»',
  address:'Черняховского 10',
  city:'Озёрск',
  festival:null,
  description_html:'Команда «Хранителей руин» приглашает принять участие.',
};

test('event 7018 gets the accepted Ruin Keepers listing medallion from an exact curated binding', () => {
  const medallions = getListingIdentityMedallions(july26RuinKeepersEvent);
  assert.deepEqual(medallions.map(({ slug }) => slug), ['ruin-keepers']);
  assert.deepEqual(medallions[0].evidence, {
    field:'event_id',
    value:'7018',
    match:'curated_event',
  });
  assert.equal(medallions[0].avatarUrl, '/assets/organizers/ruin-keepers.webp');
});

test('Ruin Keepers prose and the unrelated Kruporushka venue do not widen listing matching', () => {
  const medallions = getListingIdentityMedallions({
    ...july26RuinKeepersEvent,
    id:7019,
  });
  assert.deepEqual(medallions, []);
});
